//! Multi-Job SQL Server Core Library
//!
//! This module contains the core MultiJobSqlServer implementation that can be
//! used both as a library and in the binary application.

use crate::ferris::{
    serialization::SerializationFormat,
    sql::{
        FieldValue, SqlApplicationParser, SqlError, StreamExecutionEngine, StreamRecord,
        StreamingQuery, StreamingSqlParser,
    },
};
use log::{error, info, warn};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;

/// Multi-Job SQL Server that can run multiple SQL jobs concurrently
#[derive(Clone)]
pub struct MultiJobSqlServer {
    jobs: Arc<RwLock<HashMap<String, RunningJob>>>,
    brokers: String,
    base_group_id: String,
    max_jobs: usize,
    job_counter: Arc<Mutex<u64>>,
}

/// Represents a running SQL job with all its metadata and execution context
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

/// Job execution status
#[derive(Clone, Debug, PartialEq)]
pub enum JobStatus {
    Starting,
    Running,
    Stopped,
    Failed(String),
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Starting => write!(f, "Starting"),
            JobStatus::Running => write!(f, "Running"),
            JobStatus::Stopped => write!(f, "Stopped"),
            JobStatus::Failed(err) => write!(f, "Failed: {}", err),
        }
    }
}

/// Job performance metrics
#[derive(Debug, Clone)]
pub struct JobMetrics {
    pub records_processed: u64,
    pub records_per_second: f64,
    pub errors_count: u64,
    pub last_processed: Option<chrono::DateTime<chrono::Utc>>,
}

impl Default for JobMetrics {
    fn default() -> Self {
        Self {
            records_processed: 0,
            records_per_second: 0.0,
            errors_count: 0,
            last_processed: None,
        }
    }
}

/// Summary information about a job for API responses
#[derive(Debug, Clone)]
pub struct JobSummary {
    pub name: String,
    pub version: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub metrics: JobMetrics,
}

impl MultiJobSqlServer {
    /// Create a new MultiJobSqlServer instance
    pub fn new(brokers: String, base_group_id: String, max_jobs: usize) -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            brokers,
            base_group_id,
            max_jobs,
            job_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Deploy a new SQL job
    pub async fn deploy_job(
        &self,
        name: String,
        version: String,
        query: String,
        topic: String,
    ) -> Result<(), SqlError> {
        // Input validation
        if name.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Job name cannot be empty".to_string(),
                query: None,
            });
        }

        if version.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Job version cannot be empty".to_string(),
                query: None,
            });
        }

        if query.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Job query cannot be empty".to_string(),
                query: Some(query),
            });
        }

        if topic.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Job topic cannot be empty".to_string(),
                query: None,
            });
        }

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
        let _parsed_query = parser.parse(&query)?;

        // Generate unique consumer group ID
        let mut counter = self.job_counter.lock().await;
        *counter += 1;
        let _group_id = format!("{}-job-{}-{}", self.base_group_id, name, *counter);
        drop(counter);

        // Create shutdown channel
        let (shutdown_sender, mut shutdown_receiver) = mpsc::unbounded_channel();

        // Create real job execution task
        let job_name_clone = name.clone();
        let query_clone = query.clone();
        let brokers_clone = self.brokers.clone();
        let execution_handle = tokio::spawn(async move {
            info!("Starting real job execution for '{}'", job_name_clone);

            // Parse the SQL query to extract input and output topics
            let parser = SqlApplicationParser::new();
            let sql_app = match parser.parse_application(&query_clone) {
                Ok(app) => app,
                Err(e) => {
                    error!("Failed to parse SQL for job '{}': {:?}", job_name_clone, e);
                    return;
                }
            };

            // Extract the first statement (assuming single query for now)
            let statement = match sql_app.statements.first() {
                Some(stmt) => stmt,
                None => {
                    error!("No SQL statements found for job '{}'", job_name_clone);
                    return;
                }
            };

            // Extract input topic from SQL FROM clause (primary) or properties (fallback)
            let input_topic = extract_input_topic_from_sql(&statement.sql)
                .or_else(|| {
                    // Fallback to properties if FROM clause parsing fails
                    statement.properties.get("input.topic").cloned()
                })
                .unwrap_or_else(|| "default_input".to_string());

            let output_topic = statement
                .properties
                .get("output.topic")
                .cloned()
                .unwrap_or_else(|| "default_output".to_string());

            // Create output channel for SQL results with unbounded capacity
            let (result_sender, mut result_receiver) = tokio::sync::mpsc::unbounded_channel::<
                HashMap<String, crate::ferris::serialization::InternalValue>,
            >();
            info!(
                "Job '{}' created output channel for SQL results",
                job_name_clone
            );

            // Create serialization format (JSON by default)
            let serialization_format =
                std::sync::Arc::new(crate::ferris::serialization::JsonFormat);

            // Create and configure StreamExecutionEngine
            let mut execution_engine =
                StreamExecutionEngine::new(result_sender, serialization_format);
            info!("Job '{}' created StreamExecutionEngine", job_name_clone);

            // Parse the SQL into a StreamingQuery for execution
            let streaming_query = match parse_sql_to_streaming_query(&statement.sql) {
                Ok(query) => query,
                Err(e) => {
                    error!("Failed to parse SQL for job '{}': {:?}", job_name_clone, e);
                    return;
                }
            };
            info!(
                "Job '{}' consuming from '{}', producing to '{}'",
                job_name_clone, input_topic, output_topic
            );

            // Create Kafka consumer for input
            let consumer =
                match create_kafka_consumer(&brokers_clone, &job_name_clone, &input_topic).await {
                    Ok(consumer) => consumer,
                    Err(e) => {
                        error!(
                            "Failed to create consumer for job '{}': {:?}",
                            job_name_clone, e
                        );
                        return;
                    }
                };

            // Create Kafka producer for output
            let producer = match create_kafka_producer(&brokers_clone).await {
                Ok(producer) => producer,
                Err(e) => {
                    error!(
                        "Failed to create producer for job '{}': {:?}",
                        job_name_clone, e
                    );
                    return;
                }
            };

            // Main processing loop - fixed channel communication
            loop {
                tokio::select! {
                    _ = shutdown_receiver.recv() => {
                        info!("Job '{}' received shutdown signal", job_name_clone);
                        break;
                    }

                    // Handle SQL execution results - prioritize this to prevent channel blocking
                    Some(result) = result_receiver.recv() => {
                        info!("Job '{}' received SQL result with {} fields", job_name_clone, result.len());
                        // Use serialization format to convert results to bytes for Kafka output
                        let serialization_format = std::sync::Arc::new(crate::ferris::serialization::JsonFormat);

                        // Convert InternalValue to FieldValue map
                        let mut field_result = HashMap::new();
                        for (key, internal_value) in result {
                            field_result.insert(key, internal_to_field_value(&internal_value));
                        }

                        // Serialize using the pluggable format
                        match serialization_format.serialize_record(&field_result) {
                            Ok(serialized_bytes) => {
                                if let Err(e) = send_bytes_to_kafka(&producer, &output_topic, serialized_bytes).await {
                                    error!("Job '{}' failed to send result to Kafka: {:?}", job_name_clone, e);
                                } else {
                                    info!("Job '{}' successfully sent result to topic '{}'", job_name_clone, output_topic);
                                }
                            },
                            Err(e) => {
                                error!("Job '{}' failed to serialize result: {:?}", job_name_clone, e);
                            }
                        }
                    }

                    // Handle incoming Kafka messages
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        // Poll for messages from Kafka
                        match tokio::time::timeout(
                            Duration::from_millis(100),
                            async {
                                use futures::StreamExt;
                                let mut stream = consumer.stream();
                                stream.next().await
                            }
                        ).await {
                            Ok(Some(Ok(message))) => {
                                // Process the message through SQL execution engine
                                if let Err(e) = process_kafka_message(&mut execution_engine, &message, &streaming_query).await {
                                    error!("Job '{}' failed to process record: {:?}", job_name_clone, e);
                                }
                            }
                            Ok(Some(Err(e))) => {
                                error!("Job '{}' consumer error: {:?}", job_name_clone, e);
                            }
                            Ok(None) | Err(_) => {
                                // No message available or timeout - this is normal, continue polling
                            }
                        }
                    }
                }
            }

            info!(
                "Job '{}' execution completed - cleaning up channels",
                job_name_clone
            );
            drop(result_receiver); // Explicitly drop receiver to clean up
        });

        // Create and store the running job
        let now = chrono::Utc::now();
        let running_job = RunningJob {
            name: name.clone(),
            version,
            query,
            topic,
            status: JobStatus::Starting,
            created_at: now,
            updated_at: now,
            execution_handle,
            shutdown_sender,
            metrics: JobMetrics::default(),
        };

        // Insert the job
        let mut jobs = self.jobs.write().await;
        jobs.insert(name.clone(), running_job);
        drop(jobs);

        // Update status to running after a brief delay (simulate startup)
        let jobs_clone = self.jobs.clone();
        let name_clone = name.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut jobs = jobs_clone.write().await;
            if let Some(job) = jobs.get_mut(&name_clone) {
                job.status = JobStatus::Running;
                job.updated_at = chrono::Utc::now();
            }
        });

        info!("Successfully deployed job '{}'", name);
        Ok(())
    }

    /// Stop a running job
    pub async fn stop_job(&self, name: &str) -> Result<(), SqlError> {
        info!("Stopping job '{}'", name);

        let mut jobs = self.jobs.write().await;

        if let Some(mut job) = jobs.remove(name) {
            // Send shutdown signal
            if job.shutdown_sender.send(()).is_err() {
                warn!("Failed to send shutdown signal to job '{}'", name);
            }

            // Abort the execution task
            job.execution_handle.abort();

            // Update status
            job.status = JobStatus::Stopped;
            job.updated_at = chrono::Utc::now();

            info!("Successfully stopped job '{}'", name);
            Ok(())
        } else {
            Err(SqlError::ExecutionError {
                message: format!("Job '{}' not found", name),
                query: None,
            })
        }
    }

    // Note: pause_job functionality has been removed as it was not properly implemented
    // Future implementation should include proper job control channels and execution loop modifications

    /// List all jobs
    pub async fn list_jobs(&self) -> Vec<JobSummary> {
        let jobs = self.jobs.read().await;

        jobs.values()
            .map(|job| JobSummary {
                name: job.name.clone(),
                version: job.version.clone(),
                status: job.status.clone(),
                created_at: job.created_at,
                updated_at: job.updated_at,
                metrics: job.metrics.clone(),
            })
            .collect()
    }

    /// Get status of a specific job
    pub async fn get_job_status(&self, name: &str) -> Option<JobSummary> {
        let jobs = self.jobs.read().await;

        jobs.get(name).map(|job| JobSummary {
            name: job.name.clone(),
            version: job.version.clone(),
            status: job.status.clone(),
            created_at: job.created_at,
            updated_at: job.updated_at,
            metrics: job.metrics.clone(),
        })
    }

    /// Deploy a complete SQL application with multiple jobs
    pub async fn deploy_sql_application(
        &self,
        sql_content: &str,
        default_topic: Option<String>,
    ) -> Result<usize, SqlError> {
        info!("Deploying SQL application");

        let parser = SqlApplicationParser::new();
        let app = parser.parse_application(sql_content)?;

        info!(
            "Parsed SQL application '{}' version '{}' with {} jobs",
            app.metadata.name,
            app.metadata.version,
            app.resources.jobs.len()
        );

        let mut deployed_count = 0;
        let mut failed_jobs = Vec::new();

        for job_name in &app.resources.jobs {
            // Extract the actual SQL query for this job from the application statements
            let job_sql = match app.statements.iter().find(|stmt| {
                matches!(
                    stmt.statement_type,
                    crate::ferris::sql::app_parser::StatementType::StartJob
                        | crate::ferris::sql::app_parser::StatementType::DeployJob
                ) && stmt.name.as_ref() == Some(job_name)
            }) {
                Some(stmt) => stmt.sql.clone(),
                None => {
                    // Fallback to a basic query if job not found in statements
                    warn!(
                        "Job '{}' not found in application statements, using default query",
                        job_name
                    );
                    format!(
                        "SELECT * FROM {}",
                        default_topic.as_deref().unwrap_or("default_topic")
                    )
                }
            };

            // Extract topic from the SQL or use default
            let topic = self
                .extract_topic_from_sql(&job_sql)
                .or_else(|| default_topic.as_deref().map(|s| s.to_string()))
                .unwrap_or_else(|| "default_topic".to_string());

            match self
                .deploy_job(
                    job_name.clone(),
                    app.metadata.version.clone(),
                    job_sql,
                    topic,
                )
                .await
            {
                Ok(_) => {
                    deployed_count += 1;
                    info!("Successfully deployed job '{}'", job_name);
                }
                Err(e) => {
                    error!("Failed to deploy job '{}': {}", job_name, e);
                    failed_jobs.push(job_name.clone());
                }
            }
        }

        if !failed_jobs.is_empty() {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "Failed to deploy {} jobs: {:?}",
                    failed_jobs.len(),
                    failed_jobs
                ),
                query: None,
            });
        }

        info!(
            "Successfully deployed SQL application with {} jobs",
            deployed_count
        );

        Ok(deployed_count)
    }

    /// Get current job count
    pub async fn job_count(&self) -> usize {
        let jobs = self.jobs.read().await;
        jobs.len()
    }

    /// Get maximum jobs limit
    pub fn max_jobs(&self) -> usize {
        self.max_jobs
    }

    /// Get brokers configuration
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// Get base group ID
    pub fn base_group_id(&self) -> &str {
        &self.base_group_id
    }

    /// Extract topic name from SQL query
    fn extract_topic_from_sql(&self, sql: &str) -> Option<String> {
        let upper_sql = sql.to_uppercase();

        // Look for FROM clause and extract topic/table name
        if let Some(from_pos) = upper_sql.find(" FROM ") {
            let after_from = &sql[from_pos + 6..];
            let words: Vec<&str> = after_from.split_whitespace().collect();
            if !words.is_empty() {
                let topic_name = words[0].trim_end_matches(',').trim_end_matches(';').trim();
                return Some(topic_name.to_string());
            }
        }

        // Look for INSERT INTO clause and extract target table
        if let Some(insert_pos) = upper_sql.find("INSERT INTO ") {
            let after_insert = &sql[insert_pos + 12..];
            let words: Vec<&str> = after_insert.split_whitespace().collect();
            if !words.is_empty() {
                let table_name = words[0].trim_end_matches('(').trim();
                return Some(table_name.to_string());
            }
        }

        None
    }
}

// Helper functions for real job execution

/// Create a Kafka consumer for the given topic
async fn create_kafka_consumer(
    brokers: &str,
    group_id: &str,
    topic: &str,
) -> Result<StreamConsumer, SqlError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .create()
        .map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to create consumer: {}", e),
            query: None,
        })?;

    consumer
        .subscribe(&[topic])
        .map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to subscribe to topic '{}': {}", topic, e),
            query: None,
        })?;

    info!(
        "Created Kafka consumer for topic '{}' with group '{}'",
        topic, group_id
    );
    Ok(consumer)
}

/// Create a Kafka producer
async fn create_kafka_producer(brokers: &str) -> Result<FutureProducer, SqlError> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to create producer: {}", e),
            query: None,
        })?;

    info!("Created Kafka producer");
    Ok(producer)
}

/// Process a Kafka message through the SQL execution engine
async fn process_kafka_message(
    execution_engine: &mut StreamExecutionEngine,
    message: &rdkafka::message::BorrowedMessage<'_>,
    query: &crate::ferris::sql::StreamingQuery,
) -> Result<(), SqlError> {
    // Extract message payload
    let payload = match message.payload() {
        Some(p) => p,
        None => {
            warn!("Received message with no payload");
            return Ok(());
        }
    };

    // Parse JSON payload into StreamRecord
    let json_value: Value =
        serde_json::from_slice(payload).map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to parse message payload as JSON: {}", e),
            query: None,
        })?;

    // Convert serde_json::Value to HashMap<String, InternalValue>
    let record_map: HashMap<String, crate::ferris::serialization::InternalValue> = match json_value
    {
        Value::Object(map) => {
            let mut internal_map = HashMap::new();
            for (key, value) in map {
                let internal_value = json_to_internal(&value)?;
                internal_map.insert(key, internal_value);
            }
            internal_map
        }
        _ => {
            return Err(SqlError::ExecutionError {
                message: "Expected JSON object for record processing".to_string(),
                query: None,
            });
        }
    };

    // Execute the SQL query on this record
    // This will process the record through the SQL pipeline and generate results
    execution_engine.execute(query, record_map).await
}

/// Convert JSON value to StreamRecord format
#[allow(dead_code)]
fn json_to_stream_record(json: Value) -> Result<StreamRecord, SqlError> {
    let mut fields = HashMap::new();

    match json {
        Value::Object(map) => {
            for (key, value) in map {
                let field_value = match value {
                    Value::String(s) => FieldValue::String(s),
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            FieldValue::Integer(i)
                        } else if let Some(f) = n.as_f64() {
                            FieldValue::Float(f)
                        } else {
                            FieldValue::String(n.to_string())
                        }
                    }
                    Value::Bool(b) => FieldValue::String(b.to_string()), // Convert bool to string for now
                    Value::Null => FieldValue::Null,
                    other => FieldValue::String(other.to_string()),
                };
                fields.insert(key, field_value);
            }
        }
        _ => {
            return Err(SqlError::ExecutionError {
                message: "Expected JSON object for stream record".to_string(),
                query: None,
            });
        }
    }

    Ok(StreamRecord {
        fields,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
    })
}

/// Send serialized bytes to Kafka topic
async fn send_bytes_to_kafka(
    producer: &FutureProducer,
    topic: &str,
    bytes: Vec<u8>,
) -> Result<(), SqlError> {
    let record = FutureRecord::to(topic).payload(&bytes).key("result");

    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to send message to Kafka: {:?}", e.0),
            query: None,
        })?;

    Ok(())
}

/// Parse SQL string into a StreamingQuery
fn parse_sql_to_streaming_query(sql: &str) -> Result<StreamingQuery, SqlError> {
    let parser = StreamingSqlParser::new();
    parser.parse(sql)
}

/// Extract input topic from SQL FROM clause
fn extract_input_topic_from_sql(sql: &str) -> Option<String> {
    // Extract table/topic name from FROM clause
    // Handles: "SELECT ... FROM topic_name", "FROM schema.table", "FROM table AS alias"
    let sql_upper = sql.to_uppercase();

    if let Some(from_pos) = sql_upper.find("FROM ") {
        let after_from = &sql[from_pos + 5..]; // Skip "FROM "

        // Find the table name (stop at whitespace, comma, or keywords)
        let table_part: String = after_from
            .chars()
            .skip_while(|c| c.is_whitespace())
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
            .collect();

        if !table_part.is_empty() {
            // Handle schema.table format - take just the table name
            let topic_name = if let Some(dot_pos) = table_part.rfind('.') {
                table_part[dot_pos + 1..].to_string()
            } else {
                table_part
            };

            return Some(topic_name);
        }
    }

    None
}

/// Convert serde_json::Value to InternalValue
fn json_to_internal(
    json_value: &serde_json::Value,
) -> Result<crate::ferris::serialization::InternalValue, SqlError> {
    use crate::ferris::serialization::InternalValue;

    match json_value {
        serde_json::Value::String(s) => Ok(InternalValue::String(s.clone())),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(InternalValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(InternalValue::Number(f))
            } else {
                Ok(InternalValue::String(n.to_string()))
            }
        }
        serde_json::Value::Bool(b) => Ok(InternalValue::Boolean(*b)),
        serde_json::Value::Null => Ok(InternalValue::Null),
        serde_json::Value::Array(arr) => {
            let internal_arr: Result<Vec<_>, _> = arr.iter().map(json_to_internal).collect();
            Ok(InternalValue::Array(internal_arr?))
        }
        serde_json::Value::Object(obj) => {
            let mut internal_map = HashMap::new();
            for (k, v) in obj {
                internal_map.insert(k.clone(), json_to_internal(v)?);
            }
            Ok(InternalValue::Object(internal_map))
        }
    }
}

/// Convert InternalValue to FieldValue
fn internal_to_field_value(
    internal_value: &crate::ferris::serialization::InternalValue,
) -> FieldValue {
    use crate::ferris::serialization::InternalValue;

    match internal_value {
        InternalValue::String(s) => FieldValue::String(s.clone()),
        InternalValue::Number(n) => FieldValue::Float(*n),
        InternalValue::Integer(i) => FieldValue::Integer(*i),
        InternalValue::Boolean(b) => FieldValue::Boolean(*b),
        InternalValue::Null => FieldValue::Null,
        InternalValue::ScaledNumber(value, scale) => FieldValue::ScaledInteger(*value, *scale),
        InternalValue::Array(arr) => {
            let field_arr: Vec<FieldValue> = arr.iter().map(internal_to_field_value).collect();
            FieldValue::Array(field_arr)
        }
        InternalValue::Object(obj) => {
            let mut field_map = HashMap::new();
            for (k, v) in obj {
                field_map.insert(k.clone(), internal_to_field_value(v));
            }
            FieldValue::Map(field_map)
        }
    }
}
