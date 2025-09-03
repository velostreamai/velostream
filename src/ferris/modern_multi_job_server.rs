//! Modern Multi-Job SQL Server
//!
//! This is the modern implementation that uses pluggable datasources
//! instead of hardcoded Kafka-only processing.

use crate::ferris::sql::{
    execution::performance::PerformanceMonitor,
    multi_job_common::{
        create_datasource_reader, process_datasource_records, DataSourceConfig, JobProcessingConfig,
    },
    query_analyzer::QueryAnalyzer,
    SqlApplication, SqlError, StreamExecutionEngine, StreamingSqlParser,
};
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct MultiJobSqlServer {
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

impl MultiJobSqlServer {
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
            info!("Performance monitoring enabled for multi-job SQL server");
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
        let (output_sender, _output_receiver) = mpsc::unbounded_channel();
        let mut execution_engine = StreamExecutionEngine::new(output_sender);

        // Enable performance monitoring for this job if available
        if let Some(monitor) = &self.performance_monitor {
            execution_engine.set_performance_monitor(Some(Arc::clone(monitor)));
        }

        let execution_engine = Arc::new(tokio::sync::Mutex::new(execution_engine));

        // Clone data for the job task
        let job_name = name.clone();
        let topic_clone = topic.clone();

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
                        let job_name_clone = job_name.clone();
                        // Use default configuration for simple processing
                        let config = JobProcessingConfig::default();
                        match process_datasource_records(
                            reader,
                            None, // No sink writer for basic server
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
                | crate::ferris::sql::app_parser::StatementType::Select => {
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
    /// - "SELECT customer_id, amount FROM transactions" -> "sel_customer_transactions"
    /// - "SELECT COUNT(*) FROM fraud_alerts" -> "sel_count_fraud_alerts"  
    /// - "SELECT AVG(amount) AS avg_amt FROM sales" -> "sel_avg_sales"
    fn extract_sql_snippet(sql: &str) -> String {
        // Clean and normalize the SQL
        let sql_clean = sql
            .to_lowercase()
            .replace(['\n', '\r', '\t'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");

        // Extract key components
        let mut parts = Vec::new();

        // Extract query type (SELECT, INSERT, etc.)
        if sql_clean.starts_with("select") {
            parts.push("sel");

            // Try to extract meaningful fields or functions
            if let Some(select_part) = sql_clean.strip_prefix("select ") {
                if let Some(from_pos) = select_part.find(" from ") {
                    let select_fields = &select_part[..from_pos];

                    // Look for aggregate functions
                    if select_fields.contains("count(") {
                        parts.push("count");
                    } else if select_fields.contains("sum(") {
                        parts.push("sum");
                    } else if select_fields.contains("avg(") {
                        parts.push("avg");
                    } else if select_fields.contains("max(") {
                        parts.push("max");
                    } else if select_fields.contains("min(") {
                        parts.push("min");
                    } else if select_fields.contains("*") {
                        parts.push("all");
                    } else {
                        // Extract first significant field name
                        let first_field = select_fields
                            .split(',')
                            .next()
                            .unwrap_or("")
                            .trim()
                            .split_whitespace()
                            .next()
                            .unwrap_or("data");
                        parts.push(&first_field[..first_field.len().min(8)]);
                    }
                }

                // Extract table name
                if let Some(from_part) = select_part
                    .strip_prefix(&select_part[..select_part.find(" from ").unwrap_or(0)])
                {
                    if let Some(table_start) = from_part.strip_prefix(" from ") {
                        let table_name = table_start.split_whitespace().next().unwrap_or("table");
                        parts.push(&table_name[..table_name.len().min(12)]);
                    }
                }
            }
        } else {
            // For non-SELECT statements, use first word + generic identifier
            let first_word = sql_clean.split_whitespace().next().unwrap_or("query");
            parts.push(&first_word[..first_word.len().min(6)]);
            parts.push("stmt");
        }

        // Join parts with underscores, ensuring valid identifier
        let result = parts.join("_");

        // Ensure it's a valid identifier (alphanumeric + underscore)
        result
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>()
            .get(..20) // Limit to 20 chars
            .unwrap_or("job")
            .to_string()
    }
}
