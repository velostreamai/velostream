use clap::{Parser, Subcommand};
use ferrisstreams::ferris::kafka::{JsonSerializer, KafkaConsumer};
use ferrisstreams::ferris::sql::{
    FieldValue, SqlApplication, SqlApplicationParser, SqlError, StreamExecutionEngine,
    StreamingSqlParser,
};
use log::{error, info, warn};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, mpsc};
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
    },
}

#[derive(Clone)]
pub struct MultiJobSqlServer {
    jobs: Arc<RwLock<HashMap<String, RunningJob>>>,
    brokers: String,
    base_group_id: String,
    max_jobs: usize,
    job_counter: Arc<Mutex<u64>>,
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

#[derive(Clone, Debug)]
pub enum JobStatus {
    Starting,
    Running,
    Paused,
    Stopped,
    Failed(String),
}

#[derive(Debug, Clone)]
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
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            brokers,
            base_group_id,
            max_jobs,
            job_counter: Arc::new(Mutex::new(0)),
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
        let execution_engine = Arc::new(tokio::sync::Mutex::new(StreamExecutionEngine::new(
            output_sender,
        )));

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

                        // Convert FieldValue back to JSON for execution engine
                        let mut record_json = HashMap::new();
                        for (key, field_value) in &fields {
                            let json_value = match field_value {
                                FieldValue::String(s) => serde_json::Value::String(s.clone()),
                                FieldValue::Integer(i) => {
                                    serde_json::Value::Number(serde_json::Number::from(*i))
                                }
                                FieldValue::Float(f) => serde_json::Value::Number(
                                    serde_json::Number::from_f64(*f).unwrap_or(0.into()),
                                ),
                                FieldValue::Boolean(b) => serde_json::Value::Bool(*b),
                                FieldValue::Null => serde_json::Value::Null,
                                FieldValue::Array(arr) => {
                                    let json_arr: Vec<serde_json::Value> = arr
                                        .iter()
                                        .map(|item| match item {
                                            FieldValue::Integer(i) => serde_json::Value::Number(
                                                serde_json::Number::from(*i),
                                            ),
                                            FieldValue::Float(f) => serde_json::Value::Number(
                                                serde_json::Number::from_f64(*f)
                                                    .unwrap_or(0.into()),
                                            ),
                                            FieldValue::String(s) => {
                                                serde_json::Value::String(s.clone())
                                            }
                                            FieldValue::Boolean(b) => serde_json::Value::Bool(*b),
                                            FieldValue::Null => serde_json::Value::Null,
                                            _ => serde_json::Value::String(format!("{:?}", item)),
                                        })
                                        .collect();
                                    serde_json::Value::Array(json_arr)
                                }
                                FieldValue::Map(map) => {
                                    let json_obj: serde_json::Map<String, serde_json::Value> = map
                                        .iter()
                                        .map(|(k, v)| {
                                            (
                                                k.clone(),
                                                match v {
                                                    FieldValue::Integer(i) => {
                                                        serde_json::Value::Number(
                                                            serde_json::Number::from(*i),
                                                        )
                                                    }
                                                    FieldValue::Float(f) => {
                                                        serde_json::Value::Number(
                                                            serde_json::Number::from_f64(*f)
                                                                .unwrap_or(0.into()),
                                                        )
                                                    }
                                                    FieldValue::String(s) => {
                                                        serde_json::Value::String(s.clone())
                                                    }
                                                    FieldValue::Boolean(b) => {
                                                        serde_json::Value::Bool(*b)
                                                    }
                                                    FieldValue::Null => serde_json::Value::Null,
                                                    _ => serde_json::Value::String(format!(
                                                        "{:?}",
                                                        v
                                                    )),
                                                },
                                            )
                                        })
                                        .collect();
                                    serde_json::Value::Object(json_obj)
                                }
                                FieldValue::Struct(fields) => {
                                    let json_obj: serde_json::Map<String, serde_json::Value> =
                                        fields
                                            .iter()
                                            .map(|(k, v)| {
                                                (
                                                    k.clone(),
                                                    match v {
                                                        FieldValue::Integer(i) => {
                                                            serde_json::Value::Number(
                                                                serde_json::Number::from(*i),
                                                            )
                                                        }
                                                        FieldValue::Float(f) => {
                                                            serde_json::Value::Number(
                                                                serde_json::Number::from_f64(*f)
                                                                    .unwrap_or(0.into()),
                                                            )
                                                        }
                                                        FieldValue::String(s) => {
                                                            serde_json::Value::String(s.clone())
                                                        }
                                                        FieldValue::Boolean(b) => {
                                                            serde_json::Value::Bool(*b)
                                                        }
                                                        FieldValue::Null => serde_json::Value::Null,
                                                        _ => serde_json::Value::String(format!(
                                                            "{:?}",
                                                            v
                                                        )),
                                                    },
                                                )
                                            })
                                            .collect();
                                    serde_json::Value::Object(json_obj)
                                }
                            };
                            record_json.insert(key.clone(), json_value);
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

#[derive(Debug, Clone)]
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
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Starting FerrisStreams Multi-Job SQL Server on port {}",
        port
    );
    info!("Max concurrent jobs: {}", max_jobs);

    let server = MultiJobSqlServer::new(brokers.clone(), group_id.clone(), max_jobs);

    // Example: Deploy some test jobs
    info!("Deploying example jobs...");

    // Job 1: High-value orders
    let _ = server
        .deploy_job(
            "high_value_orders".to_string(),
            "1.0.0".to_string(),
            "SELECT customer_id, amount FROM orders WHERE amount > 1000".to_string(),
            "orders".to_string(),
        )
        .await;

    // Job 2: User activity tracking
    let _ = server.deploy_job(
        "user_activity".to_string(),
        "1.0.0".to_string(),
        "SELECT JSON_VALUE(payload, '$.user_id') as user_id, JSON_VALUE(payload, '$.action') as action FROM events".to_string(),
        "user_events".to_string(),
    ).await;

    // Job 3: Error monitoring
    let _ = server
        .deploy_job(
            "error_monitor".to_string(),
            "1.0.0".to_string(),
            "SELECT * FROM logs WHERE level = 'ERROR'".to_string(),
            "application_logs".to_string(),
        )
        .await;

    info!("Example jobs deployed successfully");

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
        } => {
            start_multi_job_server(brokers, port, group_id, max_jobs).await?;
        }
        Commands::DeployApp {
            file,
            brokers,
            group_id,
            default_topic,
        } => {
            deploy_sql_application_from_file(file, brokers, group_id, default_topic).await?;
        }
    }

    Ok(())
}

async fn deploy_sql_application_from_file(
    file_path: String,
    brokers: String,
    group_id: String,
    default_topic: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Deploying SQL application from file: {}", file_path);

    // Read the SQL application file
    let content = fs::read_to_string(&file_path)?;

    // Parse the SQL application
    let app_parser = SqlApplicationParser::new();
    let app = app_parser.parse_application(&content)?;

    info!(
        "Parsed SQL application '{}' version '{}' with {} statements",
        app.metadata.name,
        app.metadata.version,
        app.statements.len()
    );

    // Create a temporary server instance for deployment
    let server = MultiJobSqlServer::new(brokers, group_id, 100); // High limit for app deployment

    // Deploy the application
    let deployed_jobs = server
        .deploy_sql_application(app.clone(), default_topic)
        .await?;

    info!("SQL application deployment completed!");
    info!(
        "Application: {} v{}",
        app.metadata.name, app.metadata.version
    );
    info!("Deployed {} jobs: {:?}", deployed_jobs.len(), deployed_jobs);

    if let Some(description) = &app.metadata.description {
        info!("Description: {}", description);
    }

    if let Some(author) = &app.metadata.author {
        info!("Author: {}", author);
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
