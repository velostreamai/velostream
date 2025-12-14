//! Query execution engine
//!
//! Integrates with existing Velostream components:
//! - SqlValidator for parsing
//! - QueryAnalyzer for source/sink extraction
//! - StreamJobServer for execution

use super::capture::{CaptureConfig, SinkCapture};
use super::config_override::ConfigOverrides;
use super::error::{TestHarnessError, TestHarnessResult};
use super::file_io::{FileSinkFactory, FileSourceFactory};
use super::generator::SchemaDataGenerator;
use super::infra::TestHarnessInfra;
use super::schema::SchemaRegistry;
use super::spec::{
    FileFormat, InputConfig, OutputConfig, QueryTest, SinkType, SourceType, TimeSimulationConfig,
};
use super::stress::MemoryTracker;
use crate::velostream::kafka::kafka_fast_producer::PolledProducer;
use crate::velostream::server::config::StreamJobServerConfig;
use crate::velostream::server::stream_job_server::{JobStatus, StreamJobServer};
use crate::velostream::sql::execution::types::FieldValue;
use rdkafka::producer::BaseRecord;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// Type alias for source/sink extraction results: (name, optional_topic)
type SourceSinkEntry = (String, Option<String>);
/// Type alias for source/sink extraction: (sources, sinks)
type SourcesAndSinks = (Vec<SourceSinkEntry>, Vec<SourceSinkEntry>);

/// Query execution context
pub struct QueryExecutor {
    /// Test infrastructure
    infra: TestHarnessInfra,

    /// StreamJobServer for executing SQL queries
    server: Option<StreamJobServer>,

    /// Timeout per query
    timeout: Duration,

    /// Captured outputs from previous queries
    outputs: HashMap<String, CapturedOutput>,

    /// Config overrides for testing
    overrides: Option<ConfigOverrides>,

    /// Schema registry for data generation
    schema_registry: SchemaRegistry,

    /// Data generator
    generator: SchemaDataGenerator,

    /// Parsed queries from SQL file (query_name -> ParsedQuery)
    parsed_queries: HashMap<String, ParsedQuery>,

    /// Parsed queries in SQL file order (preserves statement order from source file)
    parsed_queries_ordered: Vec<ParsedQuery>,

    /// Global source name -> topic name mapping (aggregated from all parsed queries)
    /// Used to resolve the actual Kafka topic when publishing test data
    source_topics: HashMap<String, String>,
}

/// Captured output from a query execution
#[derive(Debug, Clone)]
pub struct CapturedOutput {
    /// Query name
    pub query_name: String,

    /// Sink name
    pub sink_name: String,

    /// Kafka topic that was captured (if applicable)
    pub topic: Option<String>,

    /// Captured records (value payload)
    pub records: Vec<HashMap<String, FieldValue>>,

    /// Captured message keys (one per record, in same order as records)
    /// String representation of the Kafka message key
    pub message_keys: Vec<Option<String>>,

    /// Execution time in milliseconds
    pub execution_time_ms: u64,

    /// Any warnings generated
    pub warnings: Vec<String>,

    /// Peak memory usage in bytes during execution
    pub memory_peak_bytes: Option<u64>,

    /// Memory growth in bytes during execution
    pub memory_growth_bytes: Option<i64>,
}

/// Result of query execution
#[derive(Debug)]
pub struct ExecutionResult {
    /// Query name
    pub query_name: String,

    /// Whether execution succeeded
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,

    /// Captured outputs
    pub outputs: Vec<CapturedOutput>,

    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl QueryExecutor {
    /// Create new executor with infrastructure
    pub fn new(infra: TestHarnessInfra) -> Self {
        Self {
            infra,
            server: None,
            timeout: Duration::from_secs(30),
            outputs: HashMap::new(),
            overrides: None,
            schema_registry: SchemaRegistry::new(),
            generator: SchemaDataGenerator::new(None),
            parsed_queries: HashMap::new(),
            parsed_queries_ordered: Vec::new(),
            source_topics: HashMap::new(),
        }
    }

    /// Initialize StreamJobServer for actual SQL execution
    ///
    /// Without calling this, queries will only publish data but not execute SQL.
    /// Call this after infrastructure is started to enable full end-to-end testing.
    ///
    /// # Arguments
    /// * `base_dir` - Optional base directory for resolving relative config file paths in SQL
    ///   (e.g., `../../configs/common_kafka_source.yaml`). Pass the parent directory
    ///   of the SQL file being executed.
    pub async fn with_server(
        mut self,
        base_dir: Option<impl AsRef<Path>>,
    ) -> TestHarnessResult<Self> {
        let bootstrap_servers =
            self.infra
                .bootstrap_servers()
                .ok_or_else(|| TestHarnessError::ConfigError {
                    message: "Bootstrap servers not available. Start infrastructure first."
                        .to_string(),
                })?;

        let run_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let group_id =
            crate::velostream::datasource::kafka::config_helpers::generate_test_harness_group_id(
                &run_id, None,
            );
        let mut config =
            StreamJobServerConfig::new(bootstrap_servers.to_string(), group_id).with_max_jobs(10);

        // Set base_dir for resolving relative config file paths in SQL
        if let Some(dir) = base_dir {
            config = config.with_base_dir(dir.as_ref());
        }

        self.server = Some(StreamJobServer::with_config(config));
        log::info!(
            "StreamJobServer initialized with bootstrap servers: {}",
            bootstrap_servers
        );

        Ok(self)
    }

    /// Set query timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set config overrides
    pub fn with_overrides(mut self, overrides: ConfigOverrides) -> Self {
        self.overrides = Some(overrides);
        self
    }

    /// Set schema registry
    pub fn with_schema_registry(mut self, registry: SchemaRegistry) -> Self {
        self.schema_registry = registry;
        self
    }

    /// Set data generator seed
    pub fn with_generator_seed(mut self, seed: u64) -> Self {
        self.generator = SchemaDataGenerator::new(Some(seed));
        self
    }

    /// List all jobs from the StreamJobServer
    ///
    /// Returns job summaries for all deployed jobs, including those that are
    /// running, stopped, or completed.
    pub async fn list_server_jobs(
        &self,
    ) -> Vec<crate::velostream::server::stream_job_server::JobSummary> {
        if let Some(ref server) = self.server {
            server.list_jobs().await
        } else {
            Vec::new()
        }
    }

    /// Stop the executor and cleanup infrastructure
    ///
    /// This method MUST be called before program exit to ensure proper cleanup
    /// of testcontainers. Failure to call this will leave orphaned Docker
    /// containers running.
    pub async fn stop(&mut self) -> TestHarnessResult<()> {
        log::info!("Stopping QueryExecutor and cleaning up infrastructure...");

        // Drop the server (jobs will stop naturally)
        self.server = None;

        // Stop infrastructure (cleans up testcontainers)
        self.infra.stop().await?;

        log::info!("QueryExecutor stopped and infrastructure cleaned up");
        Ok(())
    }

    /// Load and parse a SQL file without executing
    /// This populates the parsed_queries so sink topic info is available
    pub fn load_sql_file(&mut self, sql_file: impl AsRef<Path>) -> TestHarnessResult<()> {
        let sql_file = sql_file.as_ref();

        // Read SQL file
        let sql_content =
            std::fs::read_to_string(sql_file).map_err(|e| TestHarnessError::IoError {
                message: e.to_string(),
                path: sql_file.display().to_string(),
            })?;

        // Parse SQL to extract queries
        let parsed_queries = self.parse_sql(&sql_content, sql_file)?;

        // Store queries in order (preserves SQL file statement order)
        self.parsed_queries_ordered = parsed_queries.clone();

        // Store parsed queries and build global source_topics mapping
        for parsed in parsed_queries.iter() {
            self.parsed_queries
                .insert(parsed.name.clone(), parsed.clone());

            // Aggregate all source name -> topic mappings
            for (source_name, topic) in &parsed.source_topics {
                log::debug!(
                    "Mapping source '{}' -> topic '{}' (from query '{}')",
                    source_name,
                    topic,
                    parsed.name
                );
                self.source_topics
                    .insert(source_name.clone(), topic.clone());
            }
        }

        log::info!(
            "Loaded {} queries from SQL file: {:?}",
            self.parsed_queries.len(),
            self.parsed_queries.keys().collect::<Vec<_>>()
        );
        log::info!("Source -> topic mappings: {:?}", self.source_topics);

        Ok(())
    }

    /// Execute a SQL file
    pub async fn execute_file(
        &mut self,
        sql_file: impl AsRef<Path>,
        queries: &[&QueryTest],
    ) -> TestHarnessResult<Vec<ExecutionResult>> {
        let sql_file = sql_file.as_ref();

        // Load and parse SQL file if not already done
        if self.parsed_queries.is_empty() {
            self.load_sql_file(sql_file)?;
        }

        let mut results = Vec::new();

        for query_test in queries {
            // Execute query
            let result = self.execute_query(query_test).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Execute a single query
    pub async fn execute_query(&mut self, query: &QueryTest) -> TestHarnessResult<ExecutionResult> {
        let start = std::time::Instant::now();
        let memory_tracker = MemoryTracker::new(true);

        log::info!("Executing query: {}", query.name);

        // Step 1: Generate and publish input data
        for input in &query.inputs {
            self.publish_input_data(input).await?;
            memory_tracker.sample();
        }

        // Step 2: Execute the query via StreamJobServer (if available)
        // Get the parsed query to access sink topic
        // First try exact match by query name, then fall back to first parsed query
        // (for test specs that test the same SQL query with different inputs)
        log::debug!(
            "Looking for query '{}' in {} parsed queries: {:?}",
            query.name,
            self.parsed_queries.len(),
            self.parsed_queries.keys().collect::<Vec<_>>()
        );

        let parsed_query = self
            .parsed_queries
            .get(&query.name)
            .or_else(|| self.parsed_queries.values().next());

        log::debug!(
            "Found parsed_query: {:?}, sink_topic: {:?}",
            parsed_query.map(|pq| &pq.name),
            parsed_query.and_then(|pq| pq.sink_topic.as_ref())
        );

        // Determine output topic: use sink topic from SQL config, or fall back to query name
        // (the query name IS the sink name in CREATE STREAM statements)
        let output_topic = parsed_query
            .and_then(|pq| pq.sink_topic.clone())
            .unwrap_or_else(|| query.name.clone());

        log::debug!("Output topic for query '{}': {}", query.name, output_topic);

        if let Some(ref server) = self.server {
            // Get the SQL text for this query
            let sql_text = parsed_query
                .map(|pq| pq.query_text.clone())
                .ok_or_else(|| TestHarnessError::ExecutionError {
                    message: format!(
                        "SQL text for query '{}' not found. Call execute_file() first.",
                        query.name
                    ),
                    query_name: query.name.clone(),
                    source: None,
                })?;

            log::info!("Deploying query '{}' via StreamJobServer", query.name);

            // Apply config overrides to SQL (e.g., topic prefixes) before deployment
            let sql_text_with_overrides = if let Some(ref overrides) = self.overrides {
                let modified = overrides.apply_to_sql_properties(&sql_text);
                if modified != sql_text {
                    log::info!(
                        "Applied topic/config overrides to SQL for query '{}'",
                        query.name
                    );
                }
                modified
            } else {
                sql_text.clone()
            };

            // Deploy the job
            server
                .deploy_job(
                    query.name.clone(),
                    "1.0.0".to_string(),
                    sql_text_with_overrides,
                    output_topic.clone(),
                    None,
                    None,
                )
                .await
                .map_err(|e| TestHarnessError::ExecutionError {
                    message: format!("Failed to deploy job: {}", e),
                    query_name: query.name.clone(),
                    source: Some(e.to_string()),
                })?;

            // Wait for job to process data
            self.wait_for_job_completion(&query.name, self.timeout)
                .await?;
        } else {
            // No server configured - just wait for external processing
            log::warn!(
                "No StreamJobServer configured. Waiting for external processing. \
                 Call with_server() to enable SQL execution."
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        // Step 3: Capture outputs from sink (Kafka topic or file)
        memory_tracker.sample();
        let mut captured_outputs = Vec::new();

        // Check if output is configured as a file sink
        let file_sink_config =
            query
                .output
                .as_ref()
                .map(|o| &o.sink_type)
                .and_then(|st| match st {
                    SinkType::File { path, format } => Some((path.clone(), format.clone())),
                    SinkType::Kafka { .. } => None,
                });

        if let Some((file_path, file_format)) = file_sink_config {
            // Capture from file sink
            log::info!(
                "Capturing output from file sink: {} (format: {:?})",
                file_path,
                file_format
            );

            match self
                .capture_file_output(&query.name, &file_path, &file_format)
                .await
            {
                Ok(mut output) => {
                    memory_tracker.sample();
                    output.memory_peak_bytes = memory_tracker.peak_memory();
                    if let (Some(start_mem), Some(current_mem)) = (
                        memory_tracker.start_memory(),
                        memory_tracker.current_memory(),
                    ) {
                        output.memory_growth_bytes = Some(current_mem as i64 - start_mem as i64);
                    }
                    captured_outputs.push(output);
                }
                Err(e) => {
                    log::warn!("Failed to capture from file '{}': {}", file_path, e);
                }
            }
        } else if let Some(bootstrap_servers) = self.infra.bootstrap_servers() {
            // Capture from Kafka topic (default)
            let capture = SinkCapture::new(bootstrap_servers).with_config(CaptureConfig {
                timeout: self.timeout,
                min_records: 0,
                max_records: 100_000,
                idle_timeout: Duration::from_secs(3),
            });

            match capture.capture_topic(&output_topic, &query.name).await {
                Ok(mut output) => {
                    memory_tracker.sample();
                    // Populate memory metrics from the tracker
                    output.memory_peak_bytes = memory_tracker.peak_memory();
                    if let (Some(start_mem), Some(current_mem)) = (
                        memory_tracker.start_memory(),
                        memory_tracker.current_memory(),
                    ) {
                        output.memory_growth_bytes = Some(current_mem as i64 - start_mem as i64);
                    }
                    captured_outputs.push(output);
                }
                Err(e) => {
                    log::warn!("Failed to capture from topic '{}': {}", output_topic, e);
                }
            }
        }

        // Step 4: Cleanup - stop the job
        if let Some(ref server) = self.server {
            if let Err(e) = server.stop_job(&query.name).await {
                log::warn!("Failed to stop job '{}': {}", query.name, e);
            }
        }

        let execution_time_ms = start.elapsed().as_millis() as u64;

        // Store outputs for chaining
        for output in &captured_outputs {
            self.outputs.insert(query.name.clone(), output.clone());
        }

        Ok(ExecutionResult {
            query_name: query.name.clone(),
            success: true,
            error: None,
            outputs: captured_outputs,
            execution_time_ms,
        })
    }

    /// Wait for a job to complete processing
    async fn wait_for_job_completion(
        &self,
        job_name: &str,
        timeout: Duration,
    ) -> TestHarnessResult<()> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(100);

        log::debug!(
            "Waiting for job '{}' to complete (timeout: {:?})",
            job_name,
            timeout
        );

        while start.elapsed() < timeout {
            if let Some(ref server) = self.server {
                if let Some(status) = server.get_job_status(job_name).await {
                    match status.status {
                        JobStatus::Failed(ref msg) => {
                            return Err(TestHarnessError::ExecutionError {
                                message: format!("Job failed: {}", msg),
                                query_name: job_name.to_string(),
                                source: None,
                            });
                        }
                        JobStatus::Stopped => {
                            log::debug!("Job '{}' stopped", job_name);
                            return Ok(());
                        }
                        JobStatus::Running => {
                            // Check if we've processed enough records (heuristic)
                            if status.stats.records_processed > 0 {
                                // Give it a bit more time to finish processing
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                log::debug!(
                                    "Job '{}' processed {} records",
                                    job_name,
                                    status.stats.records_processed
                                );
                                return Ok(());
                            }
                        }
                        _ => {}
                    }
                }
            }
            tokio::time::sleep(poll_interval).await;
        }

        // Timeout reached - return success but log warning
        log::warn!(
            "Job '{}' did not complete within timeout ({:?}), proceeding with capture",
            job_name,
            timeout
        );
        Ok(())
    }

    /// Publish input data for a query
    async fn publish_input_data(&mut self, input: &InputConfig) -> TestHarnessResult<()> {
        log::info!(
            "Publishing input data for source: {} (schema: {:?}, source_type: {:?})",
            input.source,
            input.schema,
            input.source_type
        );

        // Check if we should use previous query output
        if let Some(ref previous_query) = input.from_previous {
            return self
                .publish_from_previous(&input.source, previous_query)
                .await;
        }

        // Check if input is from a file source
        if let Some(SourceType::File { path, format, .. }) = &input.source_type {
            return self
                .load_from_file_source(&input.source, path, format)
                .await;
        }

        // Check for shorthand data_file configuration (convenience for file sources)
        if let Some(ref data_file) = input.data_file {
            // Infer format from file extension
            let format = infer_file_format(data_file);
            return self
                .load_from_file_source(&input.source, data_file, &format)
                .await;
        }

        // Generate data from schema (default Kafka source behavior)
        let schema_name = input.schema.as_deref().unwrap_or(&input.source);
        let schema = self.schema_registry.get(schema_name).ok_or_else(|| {
            TestHarnessError::SchemaParseError {
                message: format!("Schema '{}' not found in registry", schema_name),
                file: schema_name.to_string(),
            }
        })?;

        let record_count = input.records.unwrap_or(schema.record_count);

        // Log detailed schema configuration
        let source_info = schema
            .source_path
            .as_ref()
            .map(|p| format!(" ← {}", p))
            .unwrap_or_default();
        log::info!(
            "📊 Data generation config for '{}' (schema: '{}'{}):",
            input.source,
            schema.name,
            source_info
        );
        log::info!("   Records: {}", record_count);
        log::info!(
            "   Fields: {}",
            schema
                .fields
                .iter()
                .map(|f| format!("{}:{:?}", f.name, f.field_type))
                .collect::<Vec<_>>()
                .join(", ")
        );
        // Log distributions if any
        for field in &schema.fields {
            if let Some(ref dist) = field.constraints.distribution {
                log::info!("   Distribution [{}]: {:?}", field.name, dist);
            }
            if let Some(ref enum_vals) = field.constraints.enum_values {
                log::info!(
                    "   Enum [{}]: {:?}{}",
                    field.name,
                    enum_vals.values,
                    if enum_vals.weights.is_some() {
                        " (weighted)"
                    } else {
                        ""
                    }
                );
            }
        }
        // Determine key_field: InputConfig overrides Schema
        let key_field: Option<String> =
            input.key_field.clone().or_else(|| schema.key_field.clone());
        if let Some(ref kf) = key_field {
            log::info!("   Kafka message key: field '{}'", kf);
        } else {
            log::info!("   Kafka message key: None");
        }

        // Configure time simulation on generator if specified
        if let Some(ref time_sim) = input.time_simulation {
            log::info!(
                "Configuring time simulation: start_time={:?}, time_scale={}, events_per_second={:?}",
                time_sim.start_time,
                time_sim.time_scale,
                time_sim.events_per_second
            );
            self.generator.set_time_simulation(time_sim, record_count)?;
        } else {
            // Clear any previous time simulation state
            self.generator.clear_time_simulation();
        }

        let records = self.generator.generate(schema, record_count)?;

        // Resolve topic: use source_topics mapping from SQL analysis
        // This maps stream name (e.g., "in_market_data_stream") to actual topic (e.g., "in_market_data")
        let topic = self
            .source_topics
            .get(&input.source)
            .cloned()
            .unwrap_or_else(|| input.source.clone());

        log::info!(
            "Publishing to topic '{}' (source: '{}', resolved from SQL: {})",
            topic,
            input.source,
            self.source_topics.contains_key(&input.source)
        );

        // Use rate-controlled publishing if events_per_second is configured
        if let Some(ref time_sim) = input.time_simulation {
            if time_sim.events_per_second.is_some() {
                self.publish_records_with_rate(&topic, &records, time_sim, key_field.as_deref())
                    .await?;
                log::info!(
                    "Published {} records to topic '{}' with rate control",
                    records.len(),
                    topic
                );
                return Ok(());
            }
        }

        // Default: publish all records as fast as possible
        self.publish_records(&topic, &records, key_field.as_deref())
            .await?;

        log::info!("Published {} records to topic '{}'", records.len(), topic);

        Ok(())
    }

    /// Load input data from a file source and publish to Kafka
    async fn load_from_file_source(
        &self,
        source: &str,
        path: &str,
        format: &FileFormat,
    ) -> TestHarnessResult<()> {
        log::info!(
            "Loading input data from file source: {} (path: {}, format: {:?})",
            source,
            path,
            format
        );

        // Resolve the file path (relative to current directory)
        let file_path = Path::new(path);
        let full_path = if file_path.is_absolute() {
            file_path.to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_default().join(file_path)
        };

        // Load records from file
        let stream_records = FileSourceFactory::load_records(&full_path, format)?;

        log::info!(
            "Loaded {} records from file '{}'",
            stream_records.len(),
            full_path.display()
        );

        // Convert StreamRecord to HashMap<String, FieldValue> for publishing
        let records: Vec<HashMap<String, FieldValue>> =
            stream_records.into_iter().map(|sr| sr.fields).collect();

        // Resolve topic: use source_topics mapping from SQL analysis
        let topic = self
            .source_topics
            .get(source)
            .cloned()
            .unwrap_or_else(|| source.to_string());

        log::info!(
            "Publishing {} records from file to topic '{}' (source: '{}')",
            records.len(),
            topic,
            source
        );

        // Publish records to Kafka (no key field for file sources)
        self.publish_records(&topic, &records, None).await?;

        log::info!(
            "Published {} records from file '{}' to topic '{}'",
            records.len(),
            path,
            topic
        );

        Ok(())
    }

    /// Capture output from a file sink
    async fn capture_file_output(
        &self,
        query_name: &str,
        path: &str,
        format: &FileFormat,
    ) -> TestHarnessResult<CapturedOutput> {
        let start = std::time::Instant::now();

        log::info!(
            "Capturing output from file sink: {} (format: {:?})",
            path,
            format
        );

        // Resolve the file path
        let file_path = Path::new(path);
        let full_path = if file_path.is_absolute() {
            file_path.to_path_buf()
        } else {
            // Try to resolve relative to temp directory if available
            self.infra
                .temp_file_path(path)
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_default().join(file_path))
        };

        // Wait for file to exist (with timeout)
        let wait_timeout = Duration::from_secs(10);
        let wait_start = std::time::Instant::now();
        while !full_path.exists() && wait_start.elapsed() < wait_timeout {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if !full_path.exists() {
            return Err(TestHarnessError::CaptureError {
                message: format!("File not found after {:?} timeout", wait_timeout),
                sink_name: path.to_string(),
                source: None,
            });
        }

        // Load records from file
        let stream_records = FileSinkFactory::read_output(&full_path, format)?;

        log::info!(
            "Captured {} records from file '{}'",
            stream_records.len(),
            full_path.display()
        );

        // Convert StreamRecord to HashMap<String, FieldValue>
        let records: Vec<HashMap<String, FieldValue>> =
            stream_records.into_iter().map(|sr| sr.fields).collect();

        let record_count = records.len();
        let execution_time_ms = start.elapsed().as_millis() as u64;

        Ok(CapturedOutput {
            query_name: query_name.to_string(),
            sink_name: path.to_string(),
            topic: None, // File sink, not Kafka
            records,
            message_keys: vec![None; record_count], // Files don't have message keys
            execution_time_ms,
            warnings: Vec::new(),
            memory_peak_bytes: None,
            memory_growth_bytes: None,
        })
    }

    /// Publish records from previous query output
    async fn publish_from_previous(
        &self,
        source: &str,
        previous_query: &str,
    ) -> TestHarnessResult<()> {
        // Find previous output by query name
        let previous_output = self.outputs.get(previous_query);

        if let Some(output) = previous_output {
            log::info!(
                "Using {} records from previous query '{}'",
                output.records.len(),
                output.query_name
            );

            // Resolve topic: use source_topics mapping from SQL analysis
            // This maps stream name (e.g., "in_market_data_stream") to actual topic (e.g., "in_market_data")
            let topic = self
                .source_topics
                .get(source)
                .cloned()
                .unwrap_or_else(|| source.to_string());

            log::info!(
                "Publishing from previous to topic '{}' (source: '{}', resolved from SQL: {})",
                topic,
                source,
                self.source_topics.contains_key(source)
            );

            // Publish records (no key field for from_previous - uses original message keys if any)
            self.publish_records(&topic, &output.records, None).await?;
        } else {
            log::warn!(
                "No previous output found for query '{}', skipping",
                previous_query
            );
        }

        Ok(())
    }

    /// Publish records to a Kafka topic
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name
    /// * `records` - Records to publish
    /// * `key_field` - Optional field name to use as Kafka message key
    async fn publish_records(
        &self,
        topic: &str,
        records: &[HashMap<String, FieldValue>],
        key_field: Option<&str>,
    ) -> TestHarnessResult<()> {
        // Use high-throughput async producer with non-blocking sends
        let mut producer = self.infra.create_async_producer()?;
        let start_time = std::time::Instant::now();

        for record in records {
            // Serialize to JSON
            let json_value = field_values_to_json(record);
            let payload = serde_json::to_string(&json_value).map_err(|e| {
                TestHarnessError::GeneratorError {
                    message: format!("Failed to serialize record: {}", e),
                    schema: "unknown".to_string(),
                }
            })?;

            // Extract event_time from record and set as Kafka message timestamp
            // This ensures _TIMESTAMP (Kafka message time) matches the event_time in the data
            // which is critical for windowed queries to work correctly
            let kafka_timestamp = Self::extract_event_time_ms(record);

            // Extract key field value if specified
            let key_value: Option<String> = key_field.and_then(|kf| {
                record.get(kf).map(|v| match v {
                    FieldValue::String(s) => s.clone(),
                    other => format!("{:?}", other),
                })
            });

            // Non-blocking send - the poll thread handles delivery callbacks
            let mut base_record = BaseRecord::<str, [u8]>::to(topic).payload(payload.as_bytes());
            if let Some(ts) = kafka_timestamp {
                base_record = base_record.timestamp(ts);
            }
            // Set the key if we have one - need to keep key_value alive
            let base_record = if let Some(ref key) = key_value {
                base_record.key(key.as_str())
            } else {
                base_record
            };
            if let Err((e, _)) = producer.send(base_record) {
                return Err(TestHarnessError::ExecutionError {
                    message: format!("Failed to queue message for topic '{}': {}", topic, e),
                    query_name: "publish".to_string(),
                    source: Some(e.to_string()),
                });
            }
        }

        let queue_time = start_time.elapsed();

        // Flush to ensure all messages are delivered
        producer
            .flush(Duration::from_secs(30))
            .map_err(|e| TestHarnessError::ExecutionError {
                message: format!("Failed to flush producer: {}", e),
                query_name: "publish".to_string(),
                source: Some(e.to_string()),
            })?;

        let total_time = start_time.elapsed();
        let rate = records.len() as f64 / total_time.as_secs_f64();
        log::info!(
            "High-throughput publishing complete: {} records in {:?} ({:.1}/sec, queue: {:?}, flush: {:?})",
            records.len(),
            total_time,
            rate,
            queue_time,
            total_time - queue_time
        );

        Ok(())
    }

    /// Publish records to Kafka with simulated timestamps
    ///
    /// This method publishes records as fast as possible. The `events_per_second`
    /// setting affects the **simulated timestamps** in the records (set by the generator),
    /// NOT the actual publishing rate. This is a time simulation, not real-time rate limiting.
    ///
    /// Uses AsyncPolledProducer for high-throughput non-blocking sends.
    ///
    /// # Arguments
    /// * `topic` - Kafka topic name
    /// * `records` - Records to publish
    /// * `config` - Time simulation configuration
    /// * `key_field` - Optional field name to use as Kafka message key
    async fn publish_records_with_rate(
        &self,
        topic: &str,
        records: &[HashMap<String, FieldValue>],
        config: &TimeSimulationConfig,
        key_field: Option<&str>,
    ) -> TestHarnessResult<()> {
        // Use high-throughput async producer
        let mut producer = self.infra.create_async_producer()?;
        let simulated_rate = config.events_per_second.unwrap_or(f64::MAX);

        log::info!(
            "Time-simulation publishing: {} records with simulated rate {:.1} events/sec (no actual delay - timestamps are pre-computed)",
            records.len(),
            simulated_rate,
        );

        let start_time = std::time::Instant::now();
        let mut records_sent = 0;

        // Publish all records as fast as possible - timestamps are already simulated in the records
        for record in records {
            // Serialize to JSON
            let json_value = field_values_to_json(record);
            let payload = serde_json::to_string(&json_value).map_err(|e| {
                TestHarnessError::GeneratorError {
                    message: format!("Failed to serialize record: {}", e),
                    schema: "unknown".to_string(),
                }
            })?;

            // Extract event_time from record and set as Kafka message timestamp
            // This ensures _TIMESTAMP (Kafka message time) matches the simulated event_time
            // which is critical for windowed queries to work correctly with time simulation
            let kafka_timestamp = Self::extract_event_time_ms(record);

            // Extract key field value if specified
            let key_value: Option<String> = key_field.and_then(|kf| {
                record.get(kf).map(|v| match v {
                    FieldValue::String(s) => s.clone(),
                    other => format!("{:?}", other),
                })
            });

            // Non-blocking send - the poll thread handles delivery callbacks
            let mut base_record = BaseRecord::<str, [u8]>::to(topic).payload(payload.as_bytes());
            if let Some(ts) = kafka_timestamp {
                base_record = base_record.timestamp(ts);
            }
            // Set the key if we have one
            let base_record = if let Some(ref key) = key_value {
                base_record.key(key.as_str())
            } else {
                base_record
            };
            if let Err((e, _)) = producer.send(base_record) {
                return Err(TestHarnessError::ExecutionError {
                    message: format!("Failed to queue message for topic '{}': {}", topic, e),
                    query_name: "publish".to_string(),
                    source: Some(e.to_string()),
                });
            }

            records_sent += 1;

            // Log progress every 10000 records
            if records_sent % 10000 == 0 {
                let elapsed = start_time.elapsed();
                let actual_rate = records_sent as f64 / elapsed.as_secs_f64();
                log::debug!(
                    "Progress: {}/{} records ({:.1}%), publishing at {:.1}/sec",
                    records_sent,
                    records.len(),
                    (records_sent as f64 / records.len() as f64) * 100.0,
                    actual_rate
                );
            }
        }

        // Flush to ensure all messages are delivered
        producer
            .flush(Duration::from_secs(30))
            .map_err(|e| TestHarnessError::ExecutionError {
                message: format!("Failed to flush producer: {}", e),
                query_name: "publish".to_string(),
                source: Some(e.to_string()),
            })?;

        let elapsed = start_time.elapsed();
        let actual_rate = records.len() as f64 / elapsed.as_secs_f64();
        log::info!(
            "Time-simulation publishing complete: {} records in {:?} (actual: {:.1}/sec, simulated: {:.1}/sec)",
            records.len(),
            elapsed,
            actual_rate,
            simulated_rate
        );

        Ok(())
    }

    /// Get captured output from previous query
    pub fn get_output(&self, query_name: &str) -> Option<&CapturedOutput> {
        self.outputs.get(query_name)
    }

    /// Parse SQL file to extract query definitions
    fn parse_sql(&self, sql_content: &str, sql_file: &Path) -> TestHarnessResult<Vec<ParsedQuery>> {
        use crate::velostream::sql::validator::SqlValidator;

        // Use SQL file's directory as base for resolving relative config_file paths
        // This allows SQL files to use paths like '../configs/source.yaml' correctly
        // Canonicalize to get absolute path, so relative paths resolve correctly
        let sql_dir = sql_file
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .canonicalize()
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
        let validator = SqlValidator::with_base_dir(&sql_dir);
        let result = validator.validate_sql_content(sql_content);

        if !result.is_valid {
            let errors: Vec<_> = result
                .query_results
                .iter()
                .filter(|q| !q.is_valid)
                .flat_map(|q| q.parsing_errors.iter())
                .map(|e| e.message.clone())
                .collect();

            return Err(TestHarnessError::SqlParseError {
                message: errors.join("; "),
                file: sql_file.display().to_string(),
                line: None,
            });
        }

        // Extract query names and sink info from parsed results
        let queries: Vec<ParsedQuery> = result
            .query_results
            .into_iter()
            .filter_map(|q| {
                // Extract CREATE STREAM/TABLE name from query text
                extract_stream_name(&q.query_text).map(|name| {
                    // Extract source names from DataSourceRequirement structs
                    let sources: Vec<String> =
                        q.sources_found.iter().map(|s| s.name.clone()).collect();

                    // Extract sink names from DataSinkRequirement structs
                    let sinks: Vec<String> = q.sinks_found.iter().map(|s| s.name.clone()).collect();

                    // Get the sink topic from the first sink's properties
                    // The properties HashMap contains "topic" key from YAML config normalization
                    let sink_topic = q
                        .sinks_found
                        .first()
                        .and_then(|sink| sink.properties.get("topic").cloned());

                    // Build source name -> topic mapping from YAML configs
                    // This allows the test harness to publish to the correct Kafka topic
                    let source_topics: std::collections::HashMap<String, String> = q
                        .sources_found
                        .iter()
                        .filter_map(|src| {
                            src.properties.get("topic").map(|topic| {
                                log::debug!(
                                    "Source '{}' maps to topic '{}'",
                                    src.name,
                                    topic
                                );
                                (src.name.clone(), topic.clone())
                            })
                        })
                        .collect();

                    log::debug!(
                        "Parsed query '{}': sources={:?}, sinks={:?}, sink_topic={:?}, source_topics={:?}",
                        name,
                        sources,
                        sinks,
                        sink_topic,
                        source_topics
                    );

                    ParsedQuery {
                        name,
                        query_text: q.query_text,
                        sources,
                        sinks,
                        sink_topic,
                        source_topics,
                    }
                })
            })
            .collect();

        Ok(queries)
    }

    /// Store captured output
    pub fn store_output(&mut self, output: CapturedOutput) {
        self.outputs.insert(output.query_name.clone(), output);
    }

    /// Get infrastructure reference
    pub fn infra(&self) -> &TestHarnessInfra {
        &self.infra
    }

    /// Get mutable infrastructure reference
    pub fn infra_mut(&mut self) -> &mut TestHarnessInfra {
        &mut self.infra
    }

    /// Get all captured outputs
    pub fn all_outputs(&self) -> &HashMap<String, CapturedOutput> {
        &self.outputs
    }

    /// Get parsed queries
    pub fn parsed_queries(&self) -> &HashMap<String, ParsedQuery> {
        &self.parsed_queries
    }

    /// Get parsed queries in SQL file order (preserves statement order)
    pub fn parsed_queries_ordered(&self) -> &[ParsedQuery] {
        &self.parsed_queries_ordered
    }

    /// Extract event_time from a record and convert to milliseconds since epoch.
    ///
    /// Looks for common timestamp field names: event_time, timestamp, time, created_at.
    /// Returns None if no timestamp field is found (Kafka will use current time).
    ///
    /// This is critical for time simulation - we need to set the Kafka message timestamp
    /// to match the simulated event_time so that windows work correctly.
    fn extract_event_time_ms(record: &HashMap<String, FieldValue>) -> Option<i64> {
        // Try common timestamp field names in priority order
        const TIMESTAMP_FIELDS: &[&str] = &["event_time", "timestamp", "time", "created_at"];

        for field_name in TIMESTAMP_FIELDS {
            if let Some(value) = record.get(*field_name) {
                match value {
                    FieldValue::Timestamp(dt) => {
                        return Some(dt.and_utc().timestamp_millis());
                    }
                    FieldValue::Integer(ms) => {
                        // Assume milliseconds since epoch
                        return Some(*ms);
                    }
                    _ => continue,
                }
            }
        }

        None
    }
}

/// Parsed query information
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Query name (from CREATE STREAM/TABLE)
    pub name: String,

    /// Original query text
    pub query_text: String,

    /// Source names
    pub sources: Vec<String>,

    /// Sink names
    pub sinks: Vec<String>,

    /// Sink topic name (from SQL config, e.g., 'sink_name.topic' = 'topic_name')
    pub sink_topic: Option<String>,

    /// Source name to topic mapping (from YAML configs via SQL analysis)
    /// Key: source name (e.g., "in_market_data_stream")
    /// Value: actual Kafka topic (e.g., "in_market_data")
    pub source_topics: std::collections::HashMap<String, String>,
}

/// Extract stream/table name from CREATE STREAM or CREATE TABLE statement
pub fn extract_stream_name(query: &str) -> Option<String> {
    let query_upper = query.to_uppercase();

    // Look for CREATE STREAM name
    if let Some(pos) = query_upper.find("CREATE STREAM") {
        let after = &query[pos + "CREATE STREAM".len()..];
        let trimmed = after.trim_start();

        // Extract identifier (first word)
        let name: String = trimmed
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();

        if !name.is_empty() {
            return Some(name);
        }
    }

    // Look for CREATE TABLE name (CTAS)
    if let Some(pos) = query_upper.find("CREATE TABLE") {
        let after = &query[pos + "CREATE TABLE".len()..];
        let trimmed = after.trim_start();

        // Extract identifier (first word)
        let name: String = trimmed
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();

        if !name.is_empty() {
            return Some(name);
        }
    }

    None
}

/// Parse WITH block properties from SQL text
/// Returns HashMap of property key -> value (without quotes)
pub fn parse_with_properties(sql: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();

    // Find WITH ( ... ) block - case insensitive
    let sql_upper = sql.to_uppercase();
    if let Some(with_pos) = sql_upper.rfind("WITH") {
        let after_with = &sql[with_pos + 4..];
        if let Some(paren_start) = after_with.find('(') {
            let content = &after_with[paren_start + 1..];
            // Find matching closing paren
            if let Some(paren_end) = content.rfind(')') {
                let props_str = &content[..paren_end];
                // Parse 'key' = 'value' pairs
                for pair in props_str.split(',') {
                    let pair = pair.trim();
                    if let Some(eq_pos) = pair.find('=') {
                        let key = pair[..eq_pos].trim().trim_matches('\'').trim_matches('"');
                        let value = pair[eq_pos + 1..]
                            .trim()
                            .trim_matches('\'')
                            .trim_matches('"');
                        if !key.is_empty() {
                            props.insert(key.to_string(), value.to_string());
                        }
                    }
                }
            }
        }
    }
    props
}

/// Extract source and sink info from WITH properties
/// Returns (sources: Vec<(name, topic)>, sinks: Vec<(name, topic)>)
pub fn extract_sources_and_sinks(props: &HashMap<String, String>) -> SourcesAndSinks {
    let mut sources: HashMap<String, Option<String>> = HashMap::new();
    let mut sinks: HashMap<String, Option<String>> = HashMap::new();

    for (key, value) in props {
        // Parse keys like 'source_name.type' = 'kafka_source' or 'sink_name.topic' = 'topic_name'
        if let Some(dot_pos) = key.find('.') {
            let name = &key[..dot_pos];
            let prop = &key[dot_pos + 1..];

            match prop {
                "type" => {
                    if value == "kafka_source" {
                        sources.entry(name.to_string()).or_insert(None);
                    } else if value == "kafka_sink" {
                        sinks.entry(name.to_string()).or_insert(None);
                    }
                }
                "topic" => {
                    // Check if this is a source or sink and set the topic
                    if sources.contains_key(name) {
                        sources.insert(name.to_string(), Some(value.clone()));
                    } else if sinks.contains_key(name) {
                        sinks.insert(name.to_string(), Some(value.clone()));
                    } else {
                        // We don't know the type yet, check later
                        // For now, store in a temp and resolve after
                    }
                }
                _ => {}
            }
        }
    }

    // Second pass: assign topics to sources/sinks that were defined after their topic
    for (key, value) in props {
        if let Some(dot_pos) = key.find('.') {
            let name = &key[..dot_pos];
            let prop = &key[dot_pos + 1..];

            if prop == "topic" {
                if let Some(topic_opt) = sources.get_mut(name) {
                    if topic_opt.is_none() {
                        *topic_opt = Some(value.clone());
                    }
                }
                if let Some(topic_opt) = sinks.get_mut(name) {
                    if topic_opt.is_none() {
                        *topic_opt = Some(value.clone());
                    }
                }
            }
        }
    }

    let sources_vec: Vec<_> = sources.into_iter().collect();
    let sinks_vec: Vec<_> = sinks.into_iter().collect();

    (sources_vec, sinks_vec)
}

/// Convert FieldValue map to JSON value
fn field_values_to_json(record: &HashMap<String, FieldValue>) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for (key, value) in record {
        let json_value = field_value_to_json(value);
        map.insert(key.clone(), json_value);
    }

    serde_json::Value::Object(map)
}

/// Convert single FieldValue to JSON value
fn field_value_to_json(value: &FieldValue) -> serde_json::Value {
    match value {
        FieldValue::Null => serde_json::Value::Null,
        FieldValue::Boolean(b) => serde_json::Value::Bool(*b),
        FieldValue::Integer(i) => serde_json::json!(*i),
        FieldValue::Float(f) => serde_json::json!(*f),
        FieldValue::String(s) => serde_json::Value::String(s.clone()),
        FieldValue::Date(d) => serde_json::Value::String(d.format("%Y-%m-%d").to_string()),
        FieldValue::Timestamp(ts) => {
            serde_json::Value::String(ts.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
        }
        FieldValue::ScaledInteger(value, scale) => {
            // Convert to decimal representation
            let divisor = 10_i64.pow(*scale as u32) as f64;
            let decimal = *value as f64 / divisor;
            serde_json::json!(decimal)
        }
        FieldValue::Decimal(d) => {
            // Handle Decimal type as string for precision
            serde_json::Value::String(d.to_string())
        }
        FieldValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(field_value_to_json).collect())
        }
        FieldValue::Map(m) | FieldValue::Struct(m) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in m {
                obj.insert(k.clone(), field_value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        FieldValue::Interval { value, unit } => {
            serde_json::json!({
                "value": value,
                "unit": format!("{:?}", unit)
            })
        }
    }
}

/// Infer file format from file extension
fn infer_file_format(path: &str) -> FileFormat {
    let lower = path.to_lowercase();
    if lower.ends_with(".csv") {
        FileFormat::Csv
    } else if lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
        FileFormat::JsonLines
    } else if lower.ends_with(".json") {
        FileFormat::Json
    } else {
        // Default to JSON Lines for unknown extensions
        log::warn!(
            "Unknown file extension for '{}', defaulting to JSON Lines format",
            path
        );
        FileFormat::JsonLines
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_file_format_csv() {
        assert!(matches!(infer_file_format("data.csv"), FileFormat::Csv));
        assert!(matches!(infer_file_format("data.CSV"), FileFormat::Csv));
        assert!(matches!(
            infer_file_format("/path/to/data.csv"),
            FileFormat::Csv
        ));
    }

    #[test]
    fn test_infer_file_format_json() {
        assert!(matches!(infer_file_format("data.json"), FileFormat::Json));
        assert!(matches!(infer_file_format("data.JSON"), FileFormat::Json));
    }

    #[test]
    fn test_infer_file_format_jsonl() {
        assert!(matches!(
            infer_file_format("data.jsonl"),
            FileFormat::JsonLines
        ));
        assert!(matches!(
            infer_file_format("data.ndjson"),
            FileFormat::JsonLines
        ));
        assert!(matches!(
            infer_file_format("data.NDJSON"),
            FileFormat::JsonLines
        ));
    }

    #[test]
    fn test_infer_file_format_unknown() {
        // Unknown extensions default to JSON Lines
        assert!(matches!(
            infer_file_format("data.txt"),
            FileFormat::JsonLines
        ));
        assert!(matches!(
            infer_file_format("data.parquet"),
            FileFormat::JsonLines
        ));
    }

    #[test]
    fn test_extract_stream_name() {
        assert_eq!(
            extract_stream_name("CREATE STREAM my_stream AS SELECT * FROM source"),
            Some("my_stream".to_string())
        );

        assert_eq!(
            extract_stream_name("CREATE STREAM enriched_data AS SELECT a, b FROM source"),
            Some("enriched_data".to_string())
        );

        assert_eq!(extract_stream_name("SELECT * FROM source"), None);
    }

    #[test]
    fn test_field_values_to_json() {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(42));
        record.insert("name".to_string(), FieldValue::String("test".to_string()));
        record.insert("active".to_string(), FieldValue::Boolean(true));

        let json = field_values_to_json(&record);

        assert!(json.is_object());
        assert_eq!(json["id"], 42);
        assert_eq!(json["name"], "test");
        assert_eq!(json["active"], true);
    }
}
