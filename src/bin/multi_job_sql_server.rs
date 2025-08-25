use clap::{Parser, Subcommand};
use ferrisstreams::ferris::kafka::{JsonSerializer, KafkaConsumer};
use ferrisstreams::ferris::serialization::{InternalValue, JsonFormat};
use ferrisstreams::ferris::sql::{
    execution::performance::PerformanceMonitor, FieldValue, SqlApplication, SqlApplicationParser,
    SqlError, StreamExecutionEngine, StreamingSqlParser,
};
use log::{error, info, warn};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;

#[derive(Parser)]
#[command(name = "ferris-sql-multi")]
#[command(about = "FerrisStreams Multi-Job SQL Server - Execute multiple SQL jobs concurrently")]
#[command(version = "1.0.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the multi-job SQL server
    Server {
        /// Kafka broker addresses
        #[arg(long, default_value = "localhost:9092")]
        brokers: String,

        /// Server port for SQL commands
        #[arg(long, default_value = "8080")]
        port: u16,

        /// Base consumer group ID (jobs will append suffixes)
        #[arg(long, default_value = "ferris-sql-server")]
        group_id: String,

        /// Maximum concurrent jobs
        #[arg(long, default_value = "10")]
        max_jobs: usize,

        /// Enable performance monitoring
        #[arg(long)]
        enable_metrics: bool,

        /// Metrics endpoint port (if different from server port)
        #[arg(long)]
        metrics_port: Option<u16>,
    },
    /// Deploy a SQL application from a .sql file
    DeployApp {
        /// Path to the .sql application file
        #[arg(long)]
        file: String,

        /// Kafka broker addresses
        #[arg(long, default_value = "localhost:9092")]
        brokers: String,

        /// Base consumer group ID
        #[arg(long, default_value = "ferris-sql-app")]
        group_id: String,

        /// Default topic for statements without explicit topics
        #[arg(long)]
        default_topic: Option<String>,

        /// Don't monitor jobs after deployment (exit immediately)
        #[arg(long, default_value = "false")]
        no_monitor: bool,
    },
}

#[derive(Clone)]
pub struct MultiJobSqlServer {
    jobs: Arc<RwLock<HashMap<String, RunningJob>>>,
    brokers: String,
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
    pub shutdown_sender: mpsc::UnboundedSender<()>,
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

impl MultiJobSqlServer {
    pub fn new(brokers: String, base_group_id: String, max_jobs: usize) -> Self {
        Self::new_with_monitoring(brokers, base_group_id, max_jobs, false)
    }

    pub fn new_with_monitoring(
        brokers: String,
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
            brokers,
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

        // Parse and validate the query
        let parser = StreamingSqlParser::new();
        let parsed_query = parser.parse(&query)?;

        // Generate unique consumer group ID
        let mut counter = self.job_counter.lock().await;
        *counter += 1;
        let group_id = format!("{}-job-{}-{}", self.base_group_id, name, *counter);
        drop(counter);

        // Create shutdown channel
        let (shutdown_sender, mut shutdown_receiver) = mpsc::unbounded_channel();

        // Create execution engine for this job
        let (output_sender, _output_receiver) = mpsc::unbounded_channel();
        let mut execution_engine = StreamExecutionEngine::new(output_sender, Arc::new(JsonFormat));

        // Enable performance monitoring for this job if available
        if let Some(monitor) = &self.performance_monitor {
            execution_engine.set_performance_monitor(Some(Arc::clone(monitor)));
        }

        let execution_engine = Arc::new(tokio::sync::Mutex::new(execution_engine));

        // Clone data for the job task
        let brokers = self.brokers.clone();
        let job_name = name.clone();
        let topic_clone = topic.clone();
        let _query_clone = query.clone();

        // Spawn the job execution task
        let execution_handle = tokio::spawn(async move {
            info!("Starting job '{}' execution task", job_name);

            // Create Kafka consumer for this job
            let consumer = match KafkaConsumer::<String, Value, JsonSerializer, JsonSerializer>::new(
                &brokers,
                &group_id,
                JsonSerializer,
                JsonSerializer,
            ) {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to create consumer for job '{}': {:?}", job_name, e);
                    return;
                }
            };

            if let Err(e) = consumer.subscribe(&[&topic_clone]) {
                error!(
                    "Failed to subscribe to topic '{}' for job '{}': {:?}",
                    topic_clone, job_name, e
                );
                return;
            }

            info!("Job '{}' consuming from topic '{}'", job_name, topic_clone);

            let mut records_processed = 0u64;
            let start_time = std::time::Instant::now();

            loop {
                // Check for shutdown signal
                if shutdown_receiver.try_recv().is_ok() {
                    info!("Job '{}' received shutdown signal", job_name);
                    break;
                }

                // Poll for messages
                match consumer.poll(Duration::from_millis(1000)).await {
                    Ok(message) => {
                        records_processed += 1;

                        // Convert Kafka message to format expected by execution engine
                        let headers = message.headers().clone();
                        let mut header_map = HashMap::new();
                        for (key, value) in headers.iter() {
                            if let Some(val) = value {
                                header_map.insert(key.clone(), val.clone());
                            }
                        }

                        // Extract fields from JSON value
                        let mut fields = HashMap::new();
                        if let Some(json_obj) = message.value().as_object() {
                            for (key, value) in json_obj {
                                let field_value = match value {
                                    Value::String(s) => FieldValue::String(s.clone()),
                                    Value::Number(n) => {
                                        if let Some(i) = n.as_i64() {
                                            FieldValue::Integer(i)
                                        } else if let Some(f) = n.as_f64() {
                                            FieldValue::Float(f)
                                        } else {
                                            FieldValue::String(n.to_string())
                                        }
                                    }
                                    Value::Bool(b) => FieldValue::Boolean(*b),
                                    Value::Null => FieldValue::Null,
                                    _ => FieldValue::String(value.to_string()),
                                };
                                fields.insert(key.clone(), field_value);
                            }
                        }

                        // Convert FieldValue to InternalValue for execution engine
                        let mut record_json = HashMap::new();
                        for (key, field_value) in &fields {
                            let internal_value = match field_value {
                                FieldValue::String(s) => InternalValue::String(s.clone()),
                                FieldValue::Integer(i) => InternalValue::Integer(*i),
                                FieldValue::Float(f) => InternalValue::Number(*f),
                                FieldValue::Boolean(b) => InternalValue::Boolean(*b),
                                FieldValue::Null => InternalValue::Null,
                                FieldValue::ScaledInteger(value, scale) => {
                                    InternalValue::ScaledNumber(*value, *scale)
                                }
                                FieldValue::Date(d) => {
                                    InternalValue::String(d.format("%Y-%m-%d").to_string())
                                }
                                FieldValue::Timestamp(ts) => InternalValue::String(
                                    ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
                                ),
                                FieldValue::Interval { value, unit } => {
                                    InternalValue::String(format!("INTERVAL {} {:?}", value, unit))
                                }
                                FieldValue::Decimal(dec) => InternalValue::String(dec.to_string()),
                                FieldValue::Array(arr) => {
                                    let internal_arr: Vec<InternalValue> = arr
                                        .iter()
                                        .map(|item| match item {
                                            FieldValue::Integer(i) => InternalValue::Integer(*i),
                                            FieldValue::Float(f) => InternalValue::Number(*f),
                                            FieldValue::String(s) => {
                                                InternalValue::String(s.clone())
                                            }
                                            FieldValue::Boolean(b) => InternalValue::Boolean(*b),
                                            FieldValue::Null => InternalValue::Null,
                                            _ => InternalValue::String(format!("{:?}", item)),
                                        })
                                        .collect();
                                    InternalValue::Array(internal_arr)
                                }
                                FieldValue::Map(map) => {
                                    let internal_map: HashMap<String, InternalValue> = map
                                        .iter()
                                        .map(|(k, v)| {
                                            (
                                                k.clone(),
                                                match v {
                                                    FieldValue::Integer(i) => {
                                                        InternalValue::Integer(*i)
                                                    }
                                                    FieldValue::Float(f) => {
                                                        InternalValue::Number(*f)
                                                    }
                                                    FieldValue::String(s) => {
                                                        InternalValue::String(s.clone())
                                                    }
                                                    FieldValue::Boolean(b) => {
                                                        InternalValue::Boolean(*b)
                                                    }
                                                    FieldValue::Null => InternalValue::Null,
                                                    _ => InternalValue::String(format!("{:?}", v)),
                                                },
                                            )
                                        })
                                        .collect();
                                    InternalValue::Object(internal_map)
                                }
                                FieldValue::Struct(fields) => {
                                    let internal_map: HashMap<String, InternalValue> = fields
                                        .iter()
                                        .map(|(k, v)| {
                                            (
                                                k.clone(),
                                                match v {
                                                    FieldValue::Integer(i) => {
                                                        InternalValue::Integer(*i)
                                                    }
                                                    FieldValue::Float(f) => {
                                                        InternalValue::Number(*f)
                                                    }
                                                    FieldValue::String(s) => {
                                                        InternalValue::String(s.clone())
                                                    }
                                                    FieldValue::Boolean(b) => {
                                                        InternalValue::Boolean(*b)
                                                    }
                                                    FieldValue::Null => InternalValue::Null,
                                                    _ => InternalValue::String(format!("{:?}", v)),
                                                },
                                            )
                                        })
                                        .collect();
                                    InternalValue::Object(internal_map)
                                }
                            };
                            record_json.insert(key.clone(), internal_value);
                        }

                        // Execute the query
                        let mut engine = execution_engine.lock().await;
                        if let Err(e) = engine
                            .execute_with_headers(&parsed_query, record_json, header_map)
                            .await
                        {
                            error!("Job '{}' failed to process record: {:?}", job_name, e);
                        }
                        drop(engine);

                        // Log progress every 1000 records
                        if records_processed % 1000 == 0 {
                            let elapsed = start_time.elapsed().as_secs_f64();
                            let rps = records_processed as f64 / elapsed;
                            info!(
                                "Job '{}': processed {} records ({:.2} records/sec)",
                                job_name, records_processed, rps
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Job '{}' no messages available: {:?}", job_name, e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }

            info!(
                "Job '{}' execution task completed. Processed {} records",
                job_name, records_processed
            );
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
            if let Err(e) = job.shutdown_sender.send(()) {
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
            if let Err(e) = job.shutdown_sender.send(()) {
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
        info!(
            "Deploying SQL application '{}' version '{}'",
            app.metadata.name, app.metadata.version
        );

        let mut deployed_jobs = Vec::new();

        // Deploy statements in order
        for stmt in &app.statements {
            match stmt.statement_type {
                ferrisstreams::ferris::sql::app_parser::StatementType::StartJob
                | ferrisstreams::ferris::sql::app_parser::StatementType::DeployJob => {
                    // Extract job name from the SQL statement
                    let job_name = if let Some(name) = &stmt.name {
                        name.clone()
                    } else {
                        format!("{}_stmt_{}", app.metadata.name, stmt.order)
                    };

                    // Determine topic from statement dependencies or use default
                    let topic = if !stmt.dependencies.is_empty() {
                        stmt.dependencies[0].clone()
                    } else if let Some(ref default) = default_topic {
                        default.clone()
                    } else {
                        return Err(SqlError::ExecutionError {
                            message: format!(
                                "No topic specified for job '{}' and no default topic provided",
                                job_name
                            ),
                            query: None,
                        });
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

async fn start_multi_job_server(
    brokers: String,
    port: u16,
    group_id: String,
    max_jobs: usize,
    enable_metrics: bool,
    metrics_port: Option<u16>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Starting FerrisStreams Multi-Job SQL Server on port {}",
        port
    );
    info!("Max concurrent jobs: {}", max_jobs);

    let server = MultiJobSqlServer::new_with_monitoring(
        brokers.clone(),
        group_id.clone(),
        max_jobs,
        enable_metrics,
    );

    if enable_metrics {
        let metrics_port = metrics_port.unwrap_or(port + 1000); // Default to main port + 1000
        info!(
            "Performance monitoring enabled - metrics available on port {}",
            metrics_port
        );

        // Start metrics server (reuse the same one from sql_server.rs)
        let server_clone = server.clone();
        tokio::spawn(async move {
            start_metrics_server_multi(server_clone, metrics_port)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to start metrics server: {}", e);
                });
        });
    }

    info!("Multi-job SQL server ready - no jobs deployed");
    info!("Use 'deploy-app' command or HTTP API to deploy SQL applications");

    // Status monitoring loop
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;

        let jobs = server.list_jobs().await;
        info!("Active jobs: {}", jobs.len());

        for job in &jobs {
            info!(
                "  Job '{}' ({}): {:?} - {} records processed",
                job.name, job.topic, job.status, job.metrics.records_processed
            );
        }

        if jobs.is_empty() {
            info!("No active jobs. Server will continue running...");
        }
    }
}

/// Start a simple HTTP server for metrics endpoints (multi-job version)
async fn start_metrics_server_multi(
    server: MultiJobSqlServer,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("Multi-job metrics server listening on port {}", port);

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                info!("Multi-job metrics request from: {}", addr);
                let server = server.clone();

                tokio::spawn(async move {
                    let mut buffer = [0; 1024];
                    loop {
                        match stream.try_read(&mut buffer) {
                            Ok(0) => return,
                            Ok(n) => {
                                let request = String::from_utf8_lossy(&buffer[0..n]);
                                let response =
                                    handle_multi_metrics_request(request.as_ref(), &server).await;

                                if let Err(e) = stream.try_write(response.as_bytes()) {
                                    error!("Failed to write response: {}", e);
                                }
                                return;
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                tokio::task::yield_now().await;
                                continue;
                            }
                            Err(e) => {
                                error!("Failed to read from socket: {}", e);
                                return;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

/// Handle HTTP metrics requests for multi-job server
async fn handle_multi_metrics_request(request: &str, server: &MultiJobSqlServer) -> String {
    // Parse the request path
    let path = if let Some(first_line) = request.lines().next() {
        if let Some(path_part) = first_line.split_whitespace().nth(1) {
            path_part
        } else {
            "/"
        }
    } else {
        "/"
    };

    match path {
        "/metrics" => {
            // Prometheus metrics endpoint
            if let Some(metrics) = server.get_performance_metrics() {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                    metrics.len(),
                    metrics
                )
            } else {
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\nMetrics not enabled".to_string()
            }
        }
        "/health" => {
            // Health check endpoint
            if let Some(health) = server.get_health_status() {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    health.len(),
                    health
                )
            } else {
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\nHealth monitoring not enabled".to_string()
            }
        }
        "/report" => {
            // Detailed performance report
            if let Some(report) = server.get_performance_report() {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                    report.len(),
                    report
                )
            } else {
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\nPerformance monitoring not enabled".to_string()
            }
        }
        "/jobs" => {
            // Job status endpoint
            let jobs = server.list_jobs().await;
            let job_info = serde_json::to_string_pretty(&jobs)
                .unwrap_or_else(|_| "Error serializing job information".to_string());

            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                job_info.len(),
                job_info
            )
        }
        "/" => {
            // Root endpoint with available endpoints
            let response_body = if server.performance_monitor.is_some() {
                r#"{
    "service": "ferris-sql-multi-server",
    "status": "running",
    "endpoints": {
        "/metrics": "Prometheus metrics export",
        "/health": "System health status (JSON)",
        "/report": "Detailed performance report (text)",
        "/jobs": "List all running jobs (JSON)"
    },
    "monitoring": "enabled"
}"#
            } else {
                r#"{
    "service": "ferris-sql-multi-server", 
    "status": "running",
    "endpoints": {
        "/jobs": "List all running jobs (JSON)"
    },
    "monitoring": "disabled",
    "note": "Use --enable-metrics to enable performance monitoring"
}"#
            };

            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                response_body.len(),
                response_body
            )
        }
        _ => "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\nEndpoint not found"
            .to_string(),
    }
}

#[tokio::main]
async fn main() -> ferrisstreams::ferris::error::FerrisResult<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            brokers,
            port,
            group_id,
            max_jobs,
            enable_metrics,
            metrics_port,
        } => {
            start_multi_job_server(
                brokers,
                port,
                group_id,
                max_jobs,
                enable_metrics,
                metrics_port,
            )
            .await?;
        }
        Commands::DeployApp {
            file,
            brokers,
            group_id,
            default_topic,
            no_monitor,
        } => {
            deploy_sql_application_from_file(file, brokers, group_id, default_topic, no_monitor)
                .await?;
        }
    }

    Ok(())
}

async fn deploy_sql_application_from_file(
    file_path: String,
    brokers: String,
    group_id: String,
    default_topic: Option<String>,
    no_monitor: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting deployment from file: {}", file_path);

    // Read the SQL application file
    println!("Reading SQL file: {}", file_path);
    let content = fs::read_to_string(&file_path).map_err(|e| {
        let error_msg = format!("Failed to read SQL file '{}': {}", file_path, e);
        println!("ERROR: {}", error_msg);
        eprintln!("ERROR: {}", error_msg);
        e
    })?;
    println!("Successfully read {} bytes from file", content.len());

    // Parse the SQL application
    println!("Parsing SQL application...");
    let app_parser = SqlApplicationParser::new();
    let app = app_parser.parse_application(&content).map_err(|e| {
        let error_msg = format!("Failed to parse SQL application: {}", e);
        println!("ERROR: {}", error_msg);
        eprintln!("ERROR: {}", error_msg);
        e
    })?;
    println!(
        "Successfully parsed application: {} v{} with {} statements",
        app.metadata.name,
        app.metadata.version,
        app.statements.len()
    );

    // Keep logging for server operations, but rely on println! for deployment output

    // Create a temporary server instance for deployment
    println!(
        "Creating multi-job server with brokers: {}, group_id: {}",
        brokers, group_id
    );
    let server = MultiJobSqlServer::new(brokers, group_id, 100); // High limit for app deployment

    // Deploy the application
    println!(
        "Deploying application with {} statements...",
        app.statements.len()
    );
    let deployed_jobs = server
        .deploy_sql_application(app.clone(), default_topic)
        .await
        .map_err(|e| {
            let error_msg = format!("Failed to deploy SQL application: {}", e);
            println!("ERROR: {}", error_msg);
            eprintln!("ERROR: {}", error_msg);
            e
        })?;
    println!(
        "Deployment completed. Jobs deployed: {}",
        deployed_jobs.len()
    );

    // Output results to stdout for integration tests
    println!("SQL application deployment completed!");
    println!(
        "Application: {} v{}",
        app.metadata.name, app.metadata.version
    );
    println!("Deployed {} jobs: {:?}", deployed_jobs.len(), deployed_jobs);

    if let Some(description) = &app.metadata.description {
        println!("Description: {}", description);
    }

    if let Some(author) = &app.metadata.author {
        println!("Author: {}", author);
    }

    if no_monitor {
        println!("Deployment completed. Jobs are running in the background.");
        return Ok(());
    }

    // Keep the jobs running
    info!("Jobs are now running. Use Ctrl+C to stop.");

    // Status monitoring loop for the deployed application
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;

        let jobs = server.list_jobs().await;
        info!(
            "Application '{}' - Active jobs: {}",
            app.metadata.name,
            jobs.len()
        );

        for job in &jobs {
            info!(
                "  Job '{}' ({}): {:?} - {} records processed",
                job.name, job.topic, job.status, job.metrics.records_processed
            );
        }

        if jobs.is_empty() {
            info!(
                "No active jobs remaining for application '{}'",
                app.metadata.name
            );
            break;
        }
    }

    Ok(())
}
