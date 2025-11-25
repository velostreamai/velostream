//! Query execution engine
//!
//! Integrates with existing Velostream components:
//! - SqlValidator for parsing
//! - QueryAnalyzer for source/sink extraction
//! - StreamJobServer for execution

use super::capture::{CaptureConfig, SinkCapture};
use super::config_override::ConfigOverrides;
use super::error::{TestHarnessError, TestHarnessResult};
use super::generator::SchemaDataGenerator;
use super::infra::TestHarnessInfra;
use super::schema::SchemaRegistry;
use super::spec::{InputConfig, QueryTest};
use crate::velostream::sql::execution::types::FieldValue;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// Query execution context
pub struct QueryExecutor {
    /// Test infrastructure
    infra: TestHarnessInfra,

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
}

/// Captured output from a query execution
#[derive(Debug, Clone)]
pub struct CapturedOutput {
    /// Query name
    pub query_name: String,

    /// Sink name
    pub sink_name: String,

    /// Captured records
    pub records: Vec<HashMap<String, FieldValue>>,

    /// Execution time in milliseconds
    pub execution_time_ms: u64,

    /// Any warnings generated
    pub warnings: Vec<String>,
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
            timeout: Duration::from_secs(30),
            outputs: HashMap::new(),
            overrides: None,
            schema_registry: SchemaRegistry::new(),
            generator: SchemaDataGenerator::new(None),
        }
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

    /// Execute a SQL file
    pub async fn execute_file(
        &mut self,
        sql_file: impl AsRef<Path>,
        queries: &[&QueryTest],
    ) -> TestHarnessResult<Vec<ExecutionResult>> {
        let sql_file = sql_file.as_ref();

        // Read SQL file
        let sql_content =
            std::fs::read_to_string(sql_file).map_err(|e| TestHarnessError::IoError {
                message: e.to_string(),
                path: sql_file.display().to_string(),
            })?;

        // Parse SQL to extract queries
        let parsed_queries = self.parse_sql(&sql_content, sql_file)?;

        let mut results = Vec::new();

        for query_test in queries {
            // Find matching parsed query
            let _parsed = parsed_queries
                .iter()
                .find(|q| q.name == query_test.name)
                .ok_or_else(|| TestHarnessError::ExecutionError {
                    message: format!("Query '{}' not found in SQL file", query_test.name),
                    query_name: query_test.name.clone(),
                    source: None,
                })?;

            // Execute query
            let result = self.execute_query(query_test).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Execute a single query
    pub async fn execute_query(&mut self, query: &QueryTest) -> TestHarnessResult<ExecutionResult> {
        let start = std::time::Instant::now();

        log::info!("Executing query: {}", query.name);

        // Step 1: Generate and publish input data
        for input in &query.inputs {
            self.publish_input_data(input).await?;
        }

        // Step 2: Execute the query (placeholder - actual execution via StreamJobServer)
        // In the current implementation, we rely on the test infrastructure
        // For full integration, we would deploy via StreamJobServer.deploy_job()

        // Step 3: Wait for processing and capture outputs
        let mut captured_outputs = Vec::new();

        // Wait a bit for processing
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Capture from sinks based on query configuration
        if let Some(ref bootstrap_servers) = self.infra.bootstrap_servers() {
            let capture = SinkCapture::new(bootstrap_servers).with_config(CaptureConfig {
                timeout: self.timeout,
                min_records: 0,
                max_records: 100_000,
                idle_timeout: Duration::from_secs(3),
            });

            // Capture from output topic (convention: {query_name}_output)
            let output_topic = if let Some(ref overrides) = self.overrides {
                overrides.override_topic(&format!("{}_output", query.name))
            } else {
                format!("{}_output", query.name)
            };

            match capture.capture_topic(&output_topic, &query.name).await {
                Ok(output) => {
                    captured_outputs.push(output);
                }
                Err(e) => {
                    log::warn!("Failed to capture from topic '{}': {}", output_topic, e);
                }
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

    /// Publish input data for a query
    async fn publish_input_data(&mut self, input: &InputConfig) -> TestHarnessResult<()> {
        log::info!(
            "Publishing input data for source: {} (schema: {:?})",
            input.source,
            input.schema
        );

        // Check if we should use previous query output
        if let Some(ref previous_query) = input.from_previous {
            return self
                .publish_from_previous(&input.source, previous_query)
                .await;
        }

        // Generate data from schema
        let schema_name = input.schema.as_deref().unwrap_or(&input.source);
        let schema = self.schema_registry.get(schema_name).ok_or_else(|| {
            TestHarnessError::SchemaParseError {
                message: format!("Schema '{}' not found in registry", schema_name),
                file: schema_name.to_string(),
            }
        })?;

        let record_count = input.records.unwrap_or(schema.record_count);
        let records = self.generator.generate(schema, record_count)?;

        // Publish to Kafka topic
        let topic = if let Some(ref overrides) = self.overrides {
            overrides.override_topic(&input.source)
        } else {
            input.source.clone()
        };

        self.publish_records(&topic, &records).await?;

        log::info!("Published {} records to topic '{}'", records.len(), topic);

        Ok(())
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

            let topic = if let Some(ref overrides) = self.overrides {
                overrides.override_topic(source)
            } else {
                source.to_string()
            };

            self.publish_records(&topic, &output.records).await?;
        } else {
            log::warn!(
                "No previous output found for query '{}', skipping",
                previous_query
            );
        }

        Ok(())
    }

    /// Publish records to a Kafka topic
    async fn publish_records(
        &self,
        topic: &str,
        records: &[HashMap<String, FieldValue>],
    ) -> TestHarnessResult<()> {
        let producer = self.infra.create_producer()?;

        for record in records {
            // Serialize to JSON
            let json_value = field_values_to_json(record);
            let payload = serde_json::to_string(&json_value).map_err(|e| {
                TestHarnessError::GeneratorError {
                    message: format!("Failed to serialize record: {}", e),
                    schema: "unknown".to_string(),
                }
            })?;

            // Publish to Kafka
            let delivery_result = producer
                .send(
                    FutureRecord::<(), _>::to(topic).payload(&payload),
                    Duration::from_secs(5),
                )
                .await;

            if let Err((e, _)) = delivery_result {
                return Err(TestHarnessError::ExecutionError {
                    message: format!("Failed to publish to topic '{}': {}", topic, e),
                    query_name: "publish".to_string(),
                    source: Some(e.to_string()),
                });
            }
        }

        // Flush to ensure all messages are sent
        producer
            .flush(Duration::from_secs(10))
            .map_err(|e| TestHarnessError::ExecutionError {
                message: format!("Failed to flush producer: {}", e),
                query_name: "publish".to_string(),
                source: Some(e.to_string()),
            })?;

        Ok(())
    }

    /// Get captured output from previous query
    pub fn get_output(&self, query_name: &str) -> Option<&CapturedOutput> {
        self.outputs.get(query_name)
    }

    /// Parse SQL file to extract query definitions
    fn parse_sql(&self, sql_content: &str, sql_file: &Path) -> TestHarnessResult<Vec<ParsedQuery>> {
        use crate::velostream::sql::validator::SqlValidator;

        let validator = SqlValidator::new();
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

        // Extract query names from parsed results
        let queries: Vec<ParsedQuery> = result
            .query_results
            .into_iter()
            .filter_map(|q| {
                // Extract CREATE STREAM name from query text
                extract_stream_name(&q.query_text).map(|name| ParsedQuery {
                    name,
                    query_text: q.query_text,
                    sources: Vec::new(), // TODO: Extract from QueryAnalyzer
                    sinks: Vec::new(),   // TODO: Extract from QueryAnalyzer
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
}

/// Parsed query information
#[derive(Debug)]
pub struct ParsedQuery {
    /// Query name (from CREATE STREAM)
    pub name: String,

    /// Original query text
    pub query_text: String,

    /// Source names
    pub sources: Vec<String>,

    /// Sink names
    pub sinks: Vec<String>,
}

/// Extract stream name from CREATE STREAM statement
fn extract_stream_name(query: &str) -> Option<String> {
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

    None
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

#[cfg(test)]
mod tests {
    use super::*;

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
