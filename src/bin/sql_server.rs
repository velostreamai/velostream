use clap::{Parser, Subcommand};
use ferrisstreams::ferris::{
    error::FerrisResult,
    kafka::{JsonSerializer, KafkaConsumer},
    serialization::{InternalValue, JsonFormat},
    sql::{
        FieldValue, SqlError, StreamExecutionEngine, StreamRecord, StreamingSqlParser,
        execution::performance::PerformanceMonitor,
    },
};
use log::{error, info, warn};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio::sync::{RwLock, mpsc};

#[derive(Parser)]
#[command(name = "ferris-sql")]
#[command(about = "FerrisStreams SQL Server - Execute SQL queries on Kafka streams")]
#[command(version = "1.0.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the SQL server
    Server {
        /// Kafka broker addresses
        #[arg(long, default_value = "localhost:9092")]
        brokers: String,

        /// Server port for SQL commands
        #[arg(long, default_value = "8080")]
        port: u16,

        /// Consumer group ID
        #[arg(long, default_value = "ferris-sql-server")]
        group_id: String,

        /// Enable performance monitoring
        #[arg(long)]
        enable_metrics: bool,

        /// Metrics endpoint port (if different from server port)
        #[arg(long)]
        metrics_port: Option<u16>,
    },
    /// Execute a single SQL query
    Execute {
        /// SQL query to execute
        #[arg(long)]
        query: String,

        /// Kafka broker addresses
        #[arg(long, default_value = "localhost:9092")]
        brokers: String,

        /// Input topic name
        #[arg(long)]
        topic: String,

        /// Consumer group ID
        #[arg(long, default_value = "ferris-sql-client")]
        group_id: String,

        /// Maximum number of records to process
        #[arg(long)]
        limit: Option<usize>,
    },
}

#[derive(Clone)]
pub struct SqlJobManager {
    jobs: Arc<RwLock<HashMap<String, ActiveJob>>>,
    execution_engine: Arc<StreamExecutionEngine>,
    performance_monitor: Option<Arc<PerformanceMonitor>>,
}

#[derive(Clone)]
pub struct ActiveJob {
    pub name: String,
    pub version: String,
    pub query: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
pub enum JobStatus {
    Running,
    Paused,
    Stopped,
    Failed(String),
}

impl SqlJobManager {
    pub fn new() -> Self {
        Self::new_with_monitoring(false)
    }

    pub fn new_with_monitoring(enable_monitoring: bool) -> Self {
        let (output_sender, _output_receiver) = mpsc::unbounded_channel();

        // Create serialization format (JSON by default)
        let serialization_format = Arc::new(JsonFormat);
        let mut execution_engine = StreamExecutionEngine::new(output_sender, serialization_format);

        // Set up performance monitoring if enabled
        let performance_monitor = if enable_monitoring {
            let monitor = Arc::new(PerformanceMonitor::new());
            execution_engine.set_performance_monitor(Some(Arc::clone(&monitor)));
            info!("Performance monitoring enabled for SQL server");
            Some(monitor)
        } else {
            None
        };

        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            execution_engine: Arc::new(execution_engine),
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
                "metrics": monitor.get_current_metrics()
            }))
            .unwrap_or_else(|_| "Error serializing health status".to_string())
        })
    }

    /// Get detailed performance report
    pub fn get_performance_report(&self) -> Option<String> {
        self.performance_monitor
            .as_ref()
            .map(|monitor| monitor.get_performance_report())
    }

    pub async fn deploy_job(
        &self,
        name: String,
        version: String,
        query: String,
    ) -> Result<(), SqlError> {
        info!("Deploying job '{}' version '{}': {}", name, version, query);

        // Parse and validate the query
        let parser = StreamingSqlParser::new();
        let _parsed_query = parser.parse(&query)?;

        // Create the job record
        let job = ActiveJob {
            name: name.clone(),
            version: version.clone(),
            query: query.clone(),
            status: JobStatus::Running,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        // Store the job
        let mut jobs = self.jobs.write().await;
        jobs.insert(name.clone(), job);

        info!("Successfully deployed job '{}' version '{}'", name, version);
        Ok(())
    }

    pub async fn pause_job(&self, name: &str) -> Result<(), SqlError> {
        let mut jobs = self.jobs.write().await;
        if let Some(job) = jobs.get_mut(name) {
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

    pub async fn resume_job(&self, name: &str) -> Result<(), SqlError> {
        let mut jobs = self.jobs.write().await;
        if let Some(job) = jobs.get_mut(name) {
            job.status = JobStatus::Running;
            job.updated_at = chrono::Utc::now();
            info!("Resumed job '{}'", name);
            Ok(())
        } else {
            Err(SqlError::ExecutionError {
                message: format!("Job '{}' not found", name),
                query: None,
            })
        }
    }

    pub async fn stop_job(&self, name: &str) -> Result<(), SqlError> {
        let mut jobs = self.jobs.write().await;
        if let Some(_job) = jobs.remove(name) {
            info!("Stopped and removed job '{}'", name);
            Ok(())
        } else {
            Err(SqlError::ExecutionError {
                message: format!("Job '{}' not found", name),
                query: None,
            })
        }
    }

    pub async fn list_jobs(&self) -> Vec<ActiveJob> {
        let jobs = self.jobs.read().await;
        jobs.values().cloned().collect()
    }

    pub async fn get_job_status(&self, name: &str) -> Option<ActiveJob> {
        let jobs = self.jobs.read().await;
        jobs.get(name).cloned()
    }
}

async fn execute_sql_query(
    query: String,
    brokers: String,
    topic: String,
    group_id: String,
    limit: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Executing SQL query: {}", query);

    // Parse the query
    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(&query)?;
    info!("Successfully parsed SQL query");

    // Create execution engine
    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();

    // Create serialization format (JSON by default)
    let serialization_format = Arc::new(JsonFormat);
    let execution_engine = Arc::new(tokio::sync::Mutex::new(StreamExecutionEngine::new(
        output_sender,
        serialization_format,
    )));

    // Create Kafka consumer
    let consumer = KafkaConsumer::<String, Value, JsonSerializer, JsonSerializer>::new(
        &brokers,
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )?;
    consumer.subscribe(&[&topic])?;

    info!("Connected to Kafka, consuming from topic '{}'", topic);

    let mut processed_count = 0;
    let max_records = limit.unwrap_or(usize::MAX);

    // Process records
    let engine_clone = Arc::clone(&execution_engine);
    tokio::spawn(async move {
        loop {
            if processed_count >= max_records {
                break;
            }

            match consumer.poll(Duration::from_millis(1000)).await {
                Ok(message) => {
                    processed_count += 1;

                    // Convert Kafka message to StreamRecord
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

                    let stream_record = StreamRecord {
                        fields,
                        timestamp: message.timestamp().unwrap_or(0),
                        offset: message.offset(),
                        partition: message.partition(),
                        headers: header_map,
                    };

                    // Convert FieldValue to InternalValue for execution engine
                    let mut record_internal = HashMap::new();
                    for (key, field_value) in &stream_record.fields {
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
                            FieldValue::Decimal(dec) => InternalValue::String(dec.to_string()),
                            FieldValue::Array(arr) => {
                                let internal_arr: Vec<InternalValue> = arr
                                    .iter()
                                    .map(|item| match item {
                                        FieldValue::Integer(i) => InternalValue::Integer(*i),
                                        FieldValue::Float(f) => InternalValue::Number(*f),
                                        FieldValue::String(s) => InternalValue::String(s.clone()),
                                        FieldValue::Boolean(b) => InternalValue::Boolean(*b),
                                        FieldValue::Null => InternalValue::Null,
                                        _ => InternalValue::String(format!("{:?}", item)),
                                    })
                                    .collect();
                                InternalValue::Array(internal_arr)
                            }
                            FieldValue::Map(map) => {
                                let internal_obj: HashMap<String, InternalValue> = map
                                    .iter()
                                    .map(|(k, v)| {
                                        (
                                            k.clone(),
                                            match v {
                                                FieldValue::Integer(i) => {
                                                    InternalValue::Integer(*i)
                                                }
                                                FieldValue::Float(f) => InternalValue::Number(*f),
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
                                InternalValue::Object(internal_obj)
                            }
                            FieldValue::Struct(fields) => {
                                let internal_obj: HashMap<String, InternalValue> = fields
                                    .iter()
                                    .map(|(k, v)| {
                                        (
                                            k.clone(),
                                            match v {
                                                FieldValue::Integer(i) => {
                                                    InternalValue::Integer(*i)
                                                }
                                                FieldValue::Float(f) => InternalValue::Number(*f),
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
                                InternalValue::Object(internal_obj)
                            }
                            FieldValue::Interval { value, unit } => {
                                InternalValue::String(format!("INTERVAL {} {:?}", value, unit))
                            }
                        };
                        record_internal.insert(key.clone(), internal_value);
                    }

                    let mut engine = engine_clone.lock().await;
                    if let Err(e) = engine
                        .execute_with_headers(&parsed_query, record_internal, stream_record.headers)
                        .await
                    {
                        error!("Failed to process record: {:?}", e);
                    }
                }
                Err(e) => {
                    warn!("No messages available: {:?}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        info!("Processed {} records", processed_count);
    });

    // Print results
    let mut result_count = 0;
    while let Some(result) = output_receiver.recv().await {
        result_count += 1;
        println!("Result {}: {:?}", result_count, result);

        if let Some(limit) = limit {
            if result_count >= limit {
                break;
            }
        }
    }

    Ok(())
}

async fn start_sql_server(
    brokers: String,
    port: u16,
    group_id: String,
    enable_metrics: bool,
    metrics_port: Option<u16>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting FerrisStreams SQL Server on port {}", port);

    let job_manager = SqlJobManager::new_with_monitoring(enable_metrics);

    if enable_metrics {
        let metrics_port = metrics_port.unwrap_or(port + 1000); // Default to main port + 1000
        info!(
            "Performance monitoring enabled - metrics available on port {}",
            metrics_port
        );

        // Start metrics server
        let job_manager_clone = job_manager.clone();
        tokio::spawn(async move {
            start_metrics_server(job_manager_clone, metrics_port)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to start metrics server: {}", e);
                });
        });
    }

    // In a real implementation, you would:
    // 1. Start an HTTP server (using axum, warp, or actix-web)
    // 2. Expose REST endpoints for SQL operations
    // 3. Handle WebSocket connections for real-time query results
    // 4. Implement authentication and authorization

    // For now, let's create a simple demonstration
    info!("SQL Server started successfully!");
    info!("Brokers: {}", brokers);
    info!("Consumer Group: {}", group_id);

    // Deploy financial trading volume spike analysis
    let query = "SELECT symbol, volume, price, timestamp as spike_time FROM market_data WHERE volume > 500000";
    job_manager
        .deploy_job(
            "volume_spike_analysis".to_string(),
            "1.0.0".to_string(),
            query.to_string(),
        )
        .await?;

    let jobs = job_manager.list_jobs().await;
    info!("Active jobs: {}", jobs.len());

    // Keep the server running
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        info!(
            "SQL Server is running... (jobs: {})",
            job_manager.list_jobs().await.len()
        );
    }
}

/// Start a simple HTTP server for metrics endpoints
async fn start_metrics_server(
    job_manager: SqlJobManager,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("Metrics server listening on port {}", port);

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                info!("Metrics request from: {}", addr);
                let job_manager = job_manager.clone();

                tokio::spawn(async move {
                    let mut buffer = [0; 1024];
                    loop {
                        match stream.try_read(&mut buffer) {
                            Ok(0) => return,
                            Ok(n) => {
                                let request = String::from_utf8_lossy(&buffer[0..n]);
                                let response =
                                    handle_metrics_request(request.as_ref(), &job_manager).await;

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

/// Handle HTTP metrics requests
async fn handle_metrics_request(request: &str, job_manager: &SqlJobManager) -> String {
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
            if let Some(metrics) = job_manager.get_performance_metrics() {
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
            if let Some(health) = job_manager.get_health_status() {
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
            if let Some(report) = job_manager.get_performance_report() {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                    report.len(),
                    report
                )
            } else {
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\nPerformance monitoring not enabled".to_string()
            }
        }
        "/" => {
            // Root endpoint with available metrics endpoints
            let response_body = if job_manager.performance_monitor.is_some() {
                r#"{
    "service": "ferris-sql-server",
    "status": "running",
    "endpoints": {
        "/metrics": "Prometheus metrics export",
        "/health": "System health status (JSON)",
        "/report": "Detailed performance report (text)"
    },
    "monitoring": "enabled"
}"#
            } else {
                r#"{
    "service": "ferris-sql-server", 
    "status": "running",
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
async fn main() -> FerrisResult<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            brokers,
            port,
            group_id,
            enable_metrics,
            metrics_port,
        } => {
            start_sql_server(brokers, port, group_id, enable_metrics, metrics_port).await?;
        }
        Commands::Execute {
            query,
            brokers,
            topic,
            group_id,
            limit,
        } => {
            execute_sql_query(query, brokers, topic, group_id, limit).await?;
        }
    }

    Ok(())
}
