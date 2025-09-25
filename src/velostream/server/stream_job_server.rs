//! StreamJobServer - Concurrent streaming SQL job execution
//!
//! Production-ready streaming SQL engine that can execute multiple concurrent
//! SQL jobs with full isolation. Uses pluggable datasources instead of
//! hardcoded Kafka-only processing.

use crate::velostream::datasource::DataWriter;
use crate::velostream::server::processors::{
    create_multi_sink_writers, create_multi_source_readers, FailureStrategy, JobProcessingConfig,
    SimpleJobProcessor, TransactionalJobProcessor,
};
use crate::velostream::server::table_registry::{TableRegistry, TableRegistryConfig, TableMetadata as TableStatsInfo};
use crate::velostream::sql::{
    ast::StreamingQuery, config::with_clause_parser::WithClauseParser,
    execution::performance::PerformanceMonitor, query_analyzer::QueryAnalyzer, SqlApplication,
    SqlError, SqlValidator, StreamExecutionEngine, StreamingSqlParser,
};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;


#[derive(Clone)]
pub struct StreamJobServer {
    jobs: Arc<RwLock<HashMap<String, RunningJob>>>,
    base_group_id: String,
    max_jobs: usize,
    job_counter: Arc<Mutex<u64>>,
    performance_monitor: Option<Arc<PerformanceMonitor>>,
    /// Shared table registry for managing CTAS-created tables
    table_registry: TableRegistry,
}

#[derive(Debug)]
pub struct RunningJob {
    pub name: String,
    pub version: String,
    pub query: String,
    pub topic: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub execution_handle: JoinHandle<()>,
    pub shutdown_sender: mpsc::Sender<()>,
    pub metrics: JobMetrics,
}

#[derive(Clone, Debug, serde::Serialize)]
pub enum JobStatus {
    Starting,
    Running,
    Paused,
    Stopped,
    Failed(String),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct JobMetrics {
    pub records_processed: u64,
    pub records_per_second: f64,
    pub last_record_time: Option<chrono::DateTime<chrono::Utc>>,
    pub errors: u64,
    pub memory_usage_mb: f64,
}

impl Default for JobMetrics {
    fn default() -> Self {
        Self {
            records_processed: 0,
            records_per_second: 0.0,
            last_record_time: None,
            errors: 0,
            memory_usage_mb: 0.0,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct JobSummary {
    pub name: String,
    pub version: String,
    pub topic: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metrics: JobMetrics,
}

impl StreamJobServer {
    pub fn new(_brokers: String, base_group_id: String, max_jobs: usize) -> Self {
        Self::new_with_monitoring(_brokers, base_group_id, max_jobs, false)
    }

    pub fn new_with_monitoring(
        _brokers: String,
        base_group_id: String,
        max_jobs: usize,
        enable_monitoring: bool,
    ) -> Self {
        let performance_monitor = if enable_monitoring {
            let monitor = Arc::new(PerformanceMonitor::new());
            info!("Performance monitoring enabled for StreamJobServer");
            Some(monitor)
        } else {
            None
        };

        let table_registry_config = TableRegistryConfig {
            max_tables: 100,
            enable_ttl: false,
            ttl_duration_secs: None,
            kafka_brokers: "localhost:9092".to_string(),
            base_group_id: base_group_id.clone(),
        };

        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            base_group_id,
            max_jobs,
            job_counter: Arc::new(Mutex::new(0)),
            performance_monitor,
            table_registry: TableRegistry::with_config(table_registry_config),
        }
    }

    /// Get performance metrics (if monitoring is enabled)
    pub fn get_performance_metrics(&self) -> Option<String> {
        self.performance_monitor
            .as_ref()
            .map(|monitor| monitor.export_prometheus_metrics())
    }

    /// Check if performance monitoring is enabled
    pub fn has_performance_monitoring(&self) -> bool {
        self.performance_monitor.is_some()
    }

    /// Get performance health status
    pub fn get_health_status(&self) -> Option<String> {
        self.performance_monitor.as_ref().map(|monitor| {
            let health = monitor.health_check();
            serde_json::to_string_pretty(&serde_json::json!({
                "status": format!("{:?}", health.status),
                "issues": health.issues,
                "warnings": health.warnings,
                "metrics": monitor.get_current_metrics(),
                "job_count": self.jobs.try_read().map(|jobs| jobs.len()).unwrap_or(0)
            }))
            .unwrap_or_else(|_| "Error serializing health status".to_string())
        })
    }

    /// Get detailed performance report
    pub fn get_performance_report(&self) -> Option<String> {
        self.performance_monitor.as_ref().map(|monitor| {
            format!(
                "{}\n\n=== Job Information ===\n{}",
                monitor.get_performance_report(),
                self.get_job_summary()
            )
        })
    }

    /// Get summary of all jobs
    fn get_job_summary(&self) -> String {
        if let Ok(jobs) = self.jobs.try_read() {
            if jobs.is_empty() {
                "No active jobs".to_string()
            } else {
                let mut summary = format!("Active Jobs: {}\n", jobs.len());
                for (name, job) in jobs.iter() {
                    summary.push_str(&format!(
                        "  - {}: {:?} (records: {}, rps: {:.1})\n",
                        name,
                        job.status,
                        job.metrics.records_processed,
                        job.metrics.records_per_second
                    ));
                }
                summary
            }
        } else {
            "Unable to read job information".to_string()
        }
    }

    pub async fn deploy_job(
        &self,
        name: String,
        version: String,
        query: String,
        topic: String,
    ) -> Result<(), SqlError> {
        info!(
            "Deploying job '{}' version '{}' on topic '{}': {}",
            name, version, topic, query
        );

        // Check if we're at max capacity
        let jobs = self.jobs.read().await;
        if jobs.len() >= self.max_jobs {
            return Err(SqlError::ExecutionError {
                message: format!("Maximum jobs limit reached ({})", self.max_jobs),
                query: None,
            });
        }

        // Check if job already exists
        if jobs.contains_key(&name) {
            return Err(SqlError::ExecutionError {
                message: format!("Job '{}' already exists", name),
                query: None,
            });
        }
        drop(jobs);

        // Input validation
        if name.trim().is_empty() {
            return Err(SqlError::ExecutionError {
                message: "job name cannot be empty".to_string(),
                query: None,
            });
        }

        if version.trim().is_empty() {
            return Err(SqlError::ExecutionError {
                message: "job version cannot be empty".to_string(),
                query: None,
            });
        }

        if query.trim().is_empty() {
            return Err(SqlError::ExecutionError {
                message: "query cannot be empty".to_string(),
                query: None,
            });
        }

        if topic.trim().is_empty() {
            return Err(SqlError::ExecutionError {
                message: "topic cannot be empty".to_string(),
                query: None,
            });
        }

        // Parse and validate the query
        let parser = StreamingSqlParser::new();
        let parsed_query = parser.parse(&query)?;

        // Extract table dependencies and ensure they exist
        let required_tables = TableRegistry::extract_table_dependencies(&parsed_query);
        if !required_tables.is_empty() {
            info!("Job '{}' requires tables: {:?}", name, required_tables);

            // Check that all required tables exist in the registry
            let mut missing_tables = Vec::new();

            for table_name in &required_tables {
                if !self.table_registry.exists(table_name).await {
                    missing_tables.push(table_name.clone());
                }
            }

            if !missing_tables.is_empty() {
                return Err(SqlError::ExecutionError {
                    message: format!(
                        "Job '{}' cannot be deployed: missing required tables: {:?}\n\
                        Create them first using: CREATE TABLE <name> AS SELECT ...",
                        name, missing_tables
                    ),
                    query: Some(query),
                });
            }

            info!("Job '{}': All required tables exist in registry", name);
        } else {
            info!("Job '{}' has no table dependencies", name);
        }

        // Analyze query to determine required resources
        let analyzer = QueryAnalyzer::new(self.base_group_id.clone());
        let analysis = analyzer.analyze(&parsed_query)?;

        // Extract batch configuration from WITH clauses
        let batch_config = Self::extract_batch_config_from_query(&parsed_query)?;

        // Generate unique consumer group ID
        let mut counter = self.job_counter.lock().await;
        *counter += 1;
        let _group_id = format!("{}-job-{}-{}", self.base_group_id, name, *counter);
        drop(counter);

        // Create shutdown channel
        let (shutdown_sender, shutdown_receiver) = mpsc::channel(1);

        // Create execution engine for this job with query-driven format
        let (output_sender, mut output_receiver) = mpsc::unbounded_channel();
        let mut execution_engine = StreamExecutionEngine::new(output_sender);

        // Enable performance monitoring for this job if available
        if let Some(monitor) = &self.performance_monitor {
            execution_engine.set_performance_monitor(Some(Arc::clone(monitor)));
        }

        // Inject shared tables into execution context if any required
        if !required_tables.is_empty() {
            let table_registry = self.table_registry.clone();
            let required_tables_clone = required_tables.clone();

            execution_engine.context_customizer = Some(Arc::new(move |context| {
                // Use blocking task to get tables
                let tables_to_inject = tokio::task::block_in_place(|| {
                    let runtime = tokio::runtime::Handle::current();
                    runtime.block_on(async {
                        let mut tables = HashMap::new();
                        for table_name in &required_tables_clone {
                            if let Ok(table) = table_registry.get_table(table_name).await {
                                tables.insert(table_name.clone(), table);
                                info!("Injected table '{}' into job execution context", table_name);
                            }
                        }
                        tables
                    })
                });

                for (table_name, table) in tables_to_inject {
                    context.load_reference_table(&table_name, table);
                }
            }));

            info!(
                "Job '{}': Configured table injection for {} tables",
                name,
                required_tables.len()
            );
        }

        let execution_engine = Arc::new(tokio::sync::Mutex::new(execution_engine));

        // Clone data for the job task
        let job_name = name.clone();
        let topic_clone = topic.clone();
        let batch_config_clone = batch_config.clone();

        // Spawn output handler task to consume from SQL engine output channel
        let output_job_name = job_name.clone();
        tokio::spawn(async move {
            debug!(
                "Job '{}': Starting output handler task for SQL engine output channel",
                output_job_name
            );
            let mut stdout_writer = crate::velostream::datasource::StdoutWriter::new_pretty();

            while let Some(record) = output_receiver.recv().await {
                debug!(
                    "Job '{}': SQL engine output channel received record, writing to stdout",
                    output_job_name
                );
                if let Err(e) = stdout_writer.write(record).await {
                    warn!(
                        "Job '{}': Failed to write SQL engine output to stdout: {:?}",
                        output_job_name, e
                    );
                }
            }
            debug!(
                "Job '{}': Output handler task completed (channel closed)",
                output_job_name
            );
        });

        // Spawn the job execution task using modern datasource approach
        let execution_handle = tokio::spawn(async move {
            info!(
                "Starting job '{}' execution task with {} sources and {} sinks",
                job_name,
                analysis.required_sources.len(),
                analysis.required_sinks.len()
            );

            // Use multi-source processing for all jobs (handles single-source as special case)
            match create_multi_source_readers(
                &analysis.required_sources,
                &topic_clone,
                &job_name,
                &batch_config_clone,
            )
            .await
            {
                Ok(readers) => {
                    info!(
                        "Job '{}' successfully created {} data sources",
                        job_name,
                        readers.len()
                    );

                    // Create all sinks
                    match create_multi_sink_writers(
                        &analysis.required_sinks,
                        &job_name,
                        &batch_config_clone,
                    )
                    .await
                    {
                        Ok(mut writers) => {
                            info!(
                                "Job '{}' successfully created {} data sinks",
                                job_name,
                                writers.len()
                            );

                            // Add stdout as fallback if no sinks were created
                            if writers.is_empty() {
                                info!(
                                    "Job '{}' no sinks created, adding stdout as default",
                                    job_name
                                );
                                writers.insert(
                                    "stdout_default".to_string(),
                                    Box::new(
                                        crate::velostream::datasource::StdoutWriter::new_pretty(),
                                    ) as Box<dyn DataWriter>,
                                );
                            }

                            // Determine processing mode and create appropriate processor
                            let config = Self::extract_job_config_from_query(&parsed_query);
                            let use_transactions = config.use_transactions;

                            info!(
                                "Job '{}' processing configuration: use_transactions={}, failure_strategy={:?}, max_batch_size={}, batch_timeout={}ms, max_retries={}, retry_backoff={}ms, log_progress={}",
                                job_name,
                                config.use_transactions,
                                config.failure_strategy,
                                config.max_batch_size,
                                config.batch_timeout.as_millis(),
                                config.max_retries,
                                config.retry_backoff.as_millis(),
                                config.log_progress
                            );

                            if use_transactions {
                                info!("Job '{}' using transactional processor for multi-source processing", job_name);
                                let processor = TransactionalJobProcessor::new(config);

                                match processor
                                    .process_multi_job(
                                        readers,
                                        writers,
                                        execution_engine,
                                        parsed_query,
                                        job_name.clone(),
                                        shutdown_receiver,
                                    )
                                    .await
                                {
                                    Ok(stats) => {
                                        info!(
                                            "Job '{}' completed successfully (transactional): {:?}",
                                            job_name, stats
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "Job '{}' failed (transactional): {:?}",
                                            job_name, e
                                        );
                                    }
                                }
                            } else {
                                info!(
                                    "Job '{}' using simple processor for multi-source processing",
                                    job_name
                                );
                                let processor = SimpleJobProcessor::new(config);

                                match processor
                                    .process_multi_job(
                                        readers,
                                        writers,
                                        execution_engine,
                                        parsed_query,
                                        job_name.clone(),
                                        shutdown_receiver,
                                    )
                                    .await
                                {
                                    Ok(stats) => {
                                        info!(
                                            "Job '{}' completed successfully (simple): {:?}",
                                            job_name, stats
                                        );
                                    }
                                    Err(e) => {
                                        error!("Job '{}' failed (simple): {:?}", job_name, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Job '{}' failed to create sinks: {}, using stdout only",
                                job_name, e
                            );

                            // Create stdout-only writers map for fallback
                            let mut fallback_writers = std::collections::HashMap::new();
                            fallback_writers.insert(
                                "stdout_fallback".to_string(),
                                Box::new(crate::velostream::datasource::StdoutWriter::new_pretty())
                                    as Box<dyn DataWriter>,
                            );

                            // Still proceed with processing using simple processor
                            let config = Self::extract_job_config_from_query(&parsed_query);
                            let processor = SimpleJobProcessor::new(config);

                            match processor
                                .process_multi_job(
                                    readers,
                                    fallback_writers,
                                    execution_engine,
                                    parsed_query,
                                    job_name.clone(),
                                    shutdown_receiver,
                                )
                                .await
                            {
                                Ok(stats) => {
                                    info!(
                                        "Job '{}' completed successfully (fallback): {:?}",
                                        job_name, stats
                                    );
                                }
                                Err(e) => {
                                    error!("Job '{}' failed (fallback): {:?}", job_name, e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Job '{}' failed to create data sources: {}", job_name, e);
                }
            }
        });

        // Create the job record
        let job = RunningJob {
            name: name.clone(),
            version: version.clone(),
            query: query.clone(),
            topic: topic.clone(),
            status: JobStatus::Running,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            execution_handle,
            shutdown_sender,
            metrics: JobMetrics::default(),
        };

        // Store the job
        let mut jobs = self.jobs.write().await;
        jobs.insert(name.clone(), job);
        drop(jobs);

        info!(
            "Successfully deployed job '{}' version '{}' on topic '{}'",
            name, version, topic
        );
        Ok(())
    }

    /// Create a shared table via CREATE TABLE AS SELECT
    /// This table will be available for all future SQL jobs to reference
    pub async fn create_table(&self, ctas_query: String) -> Result<String, SqlError> {
        self.table_registry.create_table(ctas_query).await
    }

    /// Get list of all available tables
    pub async fn list_tables(&self) -> Vec<String> {
        self.table_registry.list_tables().await
    }

    /// Check if a table exists in the registry
    pub async fn table_exists(&self, table_name: &str) -> bool {
        self.table_registry.exists(table_name).await
    }

    /// Get a reference to a table (for internal use)
    pub async fn get_table(
        &self,
        table_name: &str,
    ) -> Result<Arc<dyn crate::velostream::table::SqlQueryable + Send + Sync>, SqlError> {
        self.table_registry.get_table(table_name).await
    }

    /// Drop a table and stop its background population job
    pub async fn drop_table(&self, table_name: &str) -> Result<(), SqlError> {
        self.table_registry.drop_table(table_name).await
    }

    /// Get statistics about all tables
    pub async fn get_table_stats(&self) -> HashMap<String, TableStatsInfo> {
        self.table_registry.get_all_table_stats().await
    }

    /// Clean up inactive tables based on TTL
    pub async fn cleanup_inactive_tables(&self) -> Result<Vec<String>, SqlError> {
        self.table_registry.cleanup_inactive_tables().await
    }

    /// Get health status of all tables
    pub async fn get_tables_health(&self) -> HashMap<String, String> {
        let health_reports = self.table_registry.get_health_status().await;
        let mut health = HashMap::new();

        for report in health_reports {
            let status_str = format!("{:?}", report.status);
            health.insert(report.table_name, status_str);
        }

        health
    }

    pub async fn stop_job(&self, name: &str) -> Result<(), SqlError> {
        let mut jobs = self.jobs.write().await;

        if let Some(job) = jobs.remove(name) {
            info!("Stopping job '{}'", name);

            // Send shutdown signal
            if let Err(e) = job.shutdown_sender.try_send(()) {
                warn!("Failed to send shutdown signal to job '{}': {:?}", name, e);
            }

            // Abort the execution task
            job.execution_handle.abort();

            info!("Successfully stopped job '{}'", name);
            Ok(())
        } else {
            Err(SqlError::ExecutionError {
                message: format!("Job '{}' not found", name),
                query: None,
            })
        }
    }

    pub async fn pause_job(&self, name: &str) -> Result<(), SqlError> {
        let mut jobs = self.jobs.write().await;
        if let Some(job) = jobs.get_mut(name) {
            // Send shutdown signal to pause consumption
            if let Err(e) = job.shutdown_sender.try_send(()) {
                warn!("Failed to send pause signal to job '{}': {:?}", name, e);
            }

            job.status = JobStatus::Paused;
            job.updated_at = chrono::Utc::now();
            info!("Paused job '{}'", name);
            Ok(())
        } else {
            Err(SqlError::ExecutionError {
                message: format!("Job '{}' not found", name),
                query: None,
            })
        }
    }

    pub async fn list_jobs(&self) -> Vec<JobSummary> {
        let jobs = self.jobs.read().await;
        jobs.values()
            .map(|job| JobSummary {
                name: job.name.clone(),
                version: job.version.clone(),
                topic: job.topic.clone(),
                status: job.status.clone(),
                created_at: job.created_at,
                metrics: job.metrics.clone(),
            })
            .collect()
    }

    pub async fn get_job_status(&self, name: &str) -> Option<JobSummary> {
        let jobs = self.jobs.read().await;
        jobs.get(name).map(|job| JobSummary {
            name: job.name.clone(),
            version: job.version.clone(),
            topic: job.topic.clone(),
            status: job.status.clone(),
            created_at: job.created_at,
            metrics: job.metrics.clone(),
        })
    }

    /// Deploy multiple jobs from a SQL application
    pub async fn deploy_sql_application(
        &self,
        app: SqlApplication,
        default_topic: Option<String>,
    ) -> Result<Vec<String>, SqlError> {
        self.deploy_sql_application_with_filename(app, default_topic, None)
            .await
    }

    pub async fn deploy_sql_application_with_filename(
        &self,
        app: SqlApplication,
        default_topic: Option<String>,
        source_filename: Option<String>,
    ) -> Result<Vec<String>, SqlError> {
        info!(
            "Deploying SQL application '{}' version '{}'",
            app.metadata.name, app.metadata.version
        );

        // Pre-deployment SQL validation to prevent runtime failures
        info!("Validating SQL application before deployment...");
        let validator = SqlValidator::new();

        // Reconstruct the SQL content from the application statements for validation
        let sql_content = app
            .statements
            .iter()
            .map(|stmt| stmt.sql.clone())
            .collect::<Vec<String>>()
            .join(";\n");

        let validation_result = validator.validate_sql_content(&sql_content);

        if !validation_result.is_valid {
            error!(
                "SQL validation failed for application '{}':",
                app.metadata.name
            );
            for query_result in &validation_result.query_results {
                if !query_result.parsing_errors.is_empty()
                    || !query_result.configuration_errors.is_empty()
                {
                    error!(
                        "Query {} (line {}): {}",
                        query_result.query_index + 1,
                        query_result.start_line,
                        query_result
                            .query_text
                            .chars()
                            .take(100)
                            .collect::<String>()
                    );

                    for error in &query_result.parsing_errors {
                        error!(
                            "  Parsing Error: {} (line: {:?})",
                            error.message, error.line
                        );
                    }

                    for error in &query_result.configuration_errors {
                        error!(
                            "  Configuration Error: {} (line: {:?})",
                            error.message, error.line
                        );
                    }
                }
            }

            for global_error in &validation_result.global_errors {
                error!("  Global Error: {}", global_error);
            }

            for missing_config in &validation_result
                .configuration_summary
                .missing_configurations
            {
                error!("  Configuration Issue: {}", missing_config);
            }

            return Err(SqlError::parse_error(
                format!(
                    "SQL validation failed for application '{}'. Found {} invalid queries out of {} total queries. Deployment aborted to prevent runtime failures.",
                    app.metadata.name,
                    validation_result.total_queries - validation_result.valid_queries,
                    validation_result.total_queries
                ),
                None
            ));
        }

        info!(
            "SQL validation passed: {}/{} queries validated successfully",
            validation_result.valid_queries, validation_result.total_queries
        );

        let mut deployed_jobs = Vec::new();

        // Deploy statements in order
        for stmt in &app.statements {
            match stmt.statement_type {
                crate::velostream::sql::app_parser::StatementType::StartJob
                | crate::velostream::sql::app_parser::StatementType::DeployJob
                | crate::velostream::sql::app_parser::StatementType::Select
                | crate::velostream::sql::app_parser::StatementType::CreateStream
                | crate::velostream::sql::app_parser::StatementType::CreateTable => {
                    // Extract job name from the SQL statement
                    let job_name = if let Some(name) = &stmt.name {
                        name.clone()
                    } else {
                        // Generate compact, meaningful job name: filename_snippet_timestamp_id
                        let file_prefix = source_filename
                            .as_ref()
                            .and_then(|path| {
                                std::path::Path::new(path)
                                    .file_stem()
                                    .and_then(|s| s.to_str())
                            })
                            .map(|name| {
                                // Take first few chars of filename, clean it up
                                name.chars()
                                    .filter(|c| c.is_alphanumeric() || *c == '_')
                                    .take(8)
                                    .collect::<String>()
                            })
                            .unwrap_or_else(|| "app".to_string());

                        let sql_snippet = Self::extract_sql_snippet(&stmt.sql);
                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs()
                            % 100000; // Last 5 digits for compactness
                        format!(
                            "{}_{}_{}_{:02}",
                            file_prefix, sql_snippet, timestamp, stmt.order
                        )
                    };

                    // Determine topic from statement dependencies or use default
                    let topic = if !stmt.dependencies.is_empty() {
                        stmt.dependencies[0].clone()
                    } else if let Some(ref default) = default_topic {
                        default.clone()
                    } else {
                        format!("processed_data_{}", job_name) // Auto-generate topic for SELECT statements
                    };

                    // Deploy the job - fail entire deployment if any single job fails
                    match self
                        .deploy_job(
                            job_name.clone(),
                            app.metadata.version.clone(),
                            stmt.sql.clone(),
                            topic,
                        )
                        .await
                    {
                        Ok(()) => {
                            info!("Successfully deployed job '{}' from application", job_name);
                            deployed_jobs.push(job_name);
                        }
                        Err(e) => {
                            error!(
                                "Failed to deploy job '{}' from application '{}': {:?}",
                                job_name, app.metadata.name, e
                            );

                            // CLEANUP: Stop any jobs that were already deployed to prevent partial state
                            if !deployed_jobs.is_empty() {
                                error!("Cleaning up {} already-deployed jobs to prevent partial deployment state", deployed_jobs.len());
                                for cleanup_job in &deployed_jobs {
                                    if let Err(cleanup_err) = self.stop_job(cleanup_job).await {
                                        warn!(
                                            "Failed to cleanup job '{}' during rollback: {:?}",
                                            cleanup_job, cleanup_err
                                        );
                                    } else {
                                        info!(
                                            "Successfully cleaned up job '{}' during rollback",
                                            cleanup_job
                                        );
                                    }
                                }
                            }

                            // ABORT ENTIRE DEPLOYMENT - any single job failure should stop everything
                            return Err(SqlError::execution_error(
                                format!(
                                    "Deployment of application '{}' aborted: Job '{}' failed to deploy: {}. {} previously deployed jobs were stopped to prevent partial deployment state.",
                                    app.metadata.name, job_name, e, deployed_jobs.len()
                                ),
                                Some(stmt.sql.clone())
                            ));
                        }
                    }
                }
                _ => {
                    info!("Skipping non-job statement: {:?}", stmt.statement_type);
                }
            }
        }

        info!(
            "Successfully deployed {} jobs from SQL application '{}'",
            deployed_jobs.len(),
            app.metadata.name
        );
        Ok(deployed_jobs)
    }

    /// Extract a meaningful snippet from SQL for job naming
    /// Examples:
    /// - "CREATE STREAM raw_transactions AS SELECT..." -> "stream_raw_transactions"
    /// - "CREATE TABLE merchant_analytics AS SELECT..." -> "table_merchant_analytics"  
    /// - "CREATE SINK high_value_export WITH..." -> "sink_high_value_export"
    /// - "SELECT customer_id, amount FROM transactions" -> "sel_customer_transactions"
    /// - "SELECT COUNT(*) FROM fraud_alerts WHERE..." -> "sel_count_fraud_alerts"
    fn extract_sql_snippet(sql: &str) -> String {
        // Clean and normalize the SQL
        let sql_clean = sql
            .to_lowercase()
            .replace(['\n', '\r', '\t'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        // Ensure it's a valid identifier (alphanumeric + underscore)
        sql_clean
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>()
            .get(..30) // Increase limit to 30 chars for more descriptive names
            .unwrap_or("job")
            .to_string()
    }

    /// Extract job processing configuration from query properties
    fn extract_job_config_from_query(query: &StreamingQuery) -> JobProcessingConfig {
        let properties = Self::get_query_properties(query);

        let mut config = JobProcessingConfig::default();

        // Extract use_transactions
        if let Some(use_tx) = properties.get("use_transactions") {
            config.use_transactions = use_tx.to_lowercase() == "true";
        }

        // Extract failure_strategy
        if let Some(strategy) = properties.get("failure_strategy") {
            config.failure_strategy = match strategy.as_str() {
                "RetryWithBackoff" => FailureStrategy::RetryWithBackoff,
                "LogAndContinue" => FailureStrategy::LogAndContinue,
                "FailBatch" => FailureStrategy::FailBatch,
                "SendToDLQ" => FailureStrategy::SendToDLQ,
                _ => FailureStrategy::LogAndContinue, // Default
            };
        }

        // Extract retry_backoff (milliseconds)
        if let Some(backoff) = properties.get("retry_backoff") {
            if let Ok(ms) = backoff.parse::<u64>() {
                config.retry_backoff = Duration::from_millis(ms);
            }
        }

        // Extract max_retries
        if let Some(retries) = properties.get("max_retries") {
            if let Ok(max_retries) = retries.parse::<u32>() {
                config.max_retries = max_retries;
            }
        }

        // Extract max_batch_size
        if let Some(batch_size) = properties.get("max_batch_size") {
            if let Ok(size) = batch_size.parse::<usize>() {
                config.max_batch_size = size;
            }
        }

        // Extract batch_timeout (milliseconds)
        if let Some(timeout) = properties.get("batch_timeout") {
            if let Ok(ms) = timeout.parse::<u64>() {
                config.batch_timeout = Duration::from_millis(ms);
            }
        }

        config
    }

    /// Extract properties from different query types
    fn get_query_properties(query: &StreamingQuery) -> HashMap<String, String> {
        match query {
            StreamingQuery::CreateStream { properties, .. } => properties.clone(),
            StreamingQuery::CreateTable { properties, .. } => properties.clone(),
            StreamingQuery::StartJob { properties, .. } => properties.clone(),
            StreamingQuery::CreateStreamInto { properties, .. } => {
                properties.clone().into_legacy_format()
            }
            StreamingQuery::CreateTableInto { properties, .. } => {
                properties.clone().into_legacy_format()
            }
            _ => HashMap::new(),
        }
    }

    /// Extract batch configuration from SQL WITH clauses
    fn extract_batch_config_from_query(
        query: &StreamingQuery,
    ) -> Result<Option<crate::velostream::datasource::BatchConfig>, SqlError> {
        let properties = Self::get_query_properties(query);

        if properties.is_empty() {
            return Ok(None);
        }

        // Convert properties to WITH clause format
        let with_clause = properties
            .iter()
            .map(|(k, v)| format!("'{}' = '{}'", k, v))
            .collect::<Vec<_>>()
            .join(",\n    ");

        let with_clause_text = format!("WITH (\n    {}\n)", with_clause);

        info!(
            "Parsing WITH clause for batch configuration: {}",
            with_clause_text
        );

        // Parse WITH clause using our batch configuration parser
        let parser = WithClauseParser::new();
        match parser.parse_with_clause(&with_clause_text) {
            Ok(config) => {
                if config.batch_config.is_some() {
                    info!("Successfully extracted batch configuration from WITH clause");
                    Ok(config.batch_config)
                } else {
                    debug!("No batch configuration found in WITH clause");
                    Ok(None)
                }
            }
            Err(e) => {
                warn!("Failed to parse WITH clause for batch configuration: {}", e);
                // Don't fail the job, just log and continue without batch config
                Ok(None)
            }
        }
    }

}
