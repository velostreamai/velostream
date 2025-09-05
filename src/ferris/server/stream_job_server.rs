//! StreamJobServer - Concurrent streaming SQL job execution
//!
//! Production-ready streaming SQL engine that can execute multiple concurrent
//! SQL jobs with full isolation. Uses pluggable datasources instead of
//! hardcoded Kafka-only processing.

use crate::ferris::datasource::DataWriter;
use crate::ferris::server::processors::{
    create_datasource_reader, create_datasource_writer, process_datasource_records, DataSinkConfig,
    DataSourceConfig, FailureStrategy, JobProcessingConfig,
};
use crate::ferris::sql::{
    ast::StreamingQuery, execution::performance::PerformanceMonitor, query_analyzer::QueryAnalyzer,
    SqlApplication, SqlError, StreamExecutionEngine, StreamingSqlParser,
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

        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            base_group_id,
            max_jobs,
            job_counter: Arc::new(Mutex::new(0)),
            performance_monitor,
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
                        "  - {}: {} (records: {}, rps: {:.1})\n",
                        name,
                        format!("{:?}", job.status),
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

        // Analyze query to determine required resources
        let analyzer = QueryAnalyzer::new(self.base_group_id.clone());
        let analysis = analyzer.analyze(&parsed_query)?;

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

        let execution_engine = Arc::new(tokio::sync::Mutex::new(execution_engine));

        // Clone data for the job task
        let job_name = name.clone();
        let topic_clone = topic.clone();

        // Spawn output handler task to consume from SQL engine output channel
        let output_job_name = job_name.clone();
        tokio::spawn(async move {
            debug!(
                "Job '{}': Starting output handler task for SQL engine output channel",
                output_job_name
            );
            let mut stdout_writer = crate::ferris::datasource::StdoutWriter::new_pretty();

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
            info!("Starting job '{}' execution task", job_name);

            // Create datasource and process records using helper functions
            if let Some(requirement) = analysis.required_sources.first() {
                let datasource_config = DataSourceConfig {
                    requirement: requirement.clone(),
                    default_topic: topic_clone,
                    job_name: job_name.clone(),
                };

                match create_datasource_reader(&datasource_config).await {
                    Ok(reader) => {
                        info!("Job '{}' successfully created datasource reader", job_name);

                        // Create writer if sink is specified in SQL
                        let writer = if let Some(sink_requirement) = analysis.required_sinks.first()
                        {
                            let sink_config = DataSinkConfig {
                                requirement: sink_requirement.clone(),
                                job_name: job_name.clone(),
                            };
                            match create_datasource_writer(&sink_config).await {
                                Ok(writer) => {
                                    info!(
                                        "Job '{}' successfully created datasink writer",
                                        job_name
                                    );
                                    Some(writer)
                                }
                                Err(e) => {
                                    warn!("Job '{}' failed to create sink writer: {}, defaulting to stdout", job_name, e);
                                    Some(Box::new(
                                        crate::ferris::datasource::StdoutWriter::new_pretty(),
                                    )
                                        as Box<dyn DataWriter>)
                                }
                            }
                        } else {
                            warn!(
                                "Job '{}': No sink specified, defaulting to stdout.",
                                job_name
                            );
                            Some(
                                Box::new(crate::ferris::datasource::StdoutWriter::new_pretty())
                                    as Box<dyn DataWriter>,
                            )
                        };

                        let job_name_clone = job_name.clone();
                        // Extract job processing configuration from query properties
                        let config = Self::extract_job_config_from_query(&parsed_query);
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
                        match process_datasource_records(
                            reader,
                            writer,
                            execution_engine,
                            parsed_query,
                            job_name_clone,
                            shutdown_receiver,
                            config,
                        )
                        .await
                        {
                            Ok(stats) => {
                                info!("Job '{}' completed successfully: {:?}", job_name, stats);
                            }
                            Err(e) => {
                                error!("Job '{}' processing failed: {:?}", job_name, e);
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Job '{}' failed to create datasource reader: {}",
                            job_name, e
                        );
                    }
                }
            } else {
                error!(
                    "No supported datasource found in query analysis for job '{}'",
                    job_name
                );
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

        let mut deployed_jobs = Vec::new();

        // Deploy statements in order
        for stmt in &app.statements {
            match stmt.statement_type {
                crate::ferris::sql::app_parser::StatementType::StartJob
                | crate::ferris::sql::app_parser::StatementType::DeployJob
                | crate::ferris::sql::app_parser::StatementType::Select
                | crate::ferris::sql::app_parser::StatementType::CreateStream
                | crate::ferris::sql::app_parser::StatementType::CreateTable => {
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

                    // Deploy the job
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
                            warn!(
                                "Failed to deploy job '{}' from application: {:?}",
                                job_name, e
                            );
                            // Continue with other jobs - don't fail the entire application
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
            _ => HashMap::new(),
        }
    }
}
