//! Sink output capture
//!
//! Captures output from query execution:
//! - Kafka topic consumption
//! - File sink reading
//! - Record deserialization

use super::error::{TestHarnessError, TestHarnessResult};
use super::executor::CapturedOutput;
use crate::velostream::sql::execution::types::FieldValue;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::message::Message;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// Sink capture configuration
#[derive(Debug, Clone)]
pub struct CaptureConfig {
    /// Maximum time to wait for messages
    pub timeout: Duration,

    /// Minimum records to capture before returning
    pub min_records: usize,

    /// Maximum records to capture
    pub max_records: usize,

    /// Wait time after last message before considering capture complete
    pub idle_timeout: Duration,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            min_records: 1,
            max_records: 100_000,
            idle_timeout: Duration::from_secs(5),
        }
    }
}

/// Captures output from sinks
pub struct SinkCapture {
    /// Capture configuration
    config: CaptureConfig,

    /// Bootstrap servers for Kafka
    bootstrap_servers: String,

    /// Consumer group ID prefix
    group_id_prefix: String,
}

impl SinkCapture {
    /// Create new capture with bootstrap servers
    pub fn new(bootstrap_servers: &str) -> Self {
        Self {
            config: CaptureConfig::default(),
            bootstrap_servers: bootstrap_servers.to_string(),
            group_id_prefix: "test_harness_capture".to_string(),
        }
    }

    /// Set capture configuration
    pub fn with_config(mut self, config: CaptureConfig) -> Self {
        self.config = config;
        self
    }

    /// Set consumer group ID prefix
    pub fn with_group_id_prefix(mut self, prefix: &str) -> Self {
        self.group_id_prefix = prefix.to_string();
        self
    }

    /// Capture output from Kafka topic
    pub async fn capture_topic(
        &self,
        topic: &str,
        query_name: &str,
    ) -> TestHarnessResult<CapturedOutput> {
        let start = std::time::Instant::now();

        log::info!(
            "Capturing from topic: {} (timeout: {:?})",
            topic,
            self.config.timeout
        );

        // Generate unique consumer group ID
        let group_id = format!(
            "{}_{}_{}",
            self.group_id_prefix,
            topic,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        );

        // Create Kafka consumer
        let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", &group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", "10000")
            .set("fetch.wait.max.ms", "100")
            .create()
            .map_err(|e| TestHarnessError::CaptureError {
                message: format!("Failed to create Kafka consumer: {}", e),
                sink_name: topic.to_string(),
                source: Some(e.to_string()),
            })?;

        // Subscribe to topic
        consumer
            .subscribe(&[topic])
            .map_err(|e| TestHarnessError::CaptureError {
                message: format!("Failed to subscribe to topic '{}': {}", topic, e),
                sink_name: topic.to_string(),
                source: Some(e.to_string()),
            })?;

        log::debug!("Subscribed to topic: {} with group: {}", topic, group_id);

        let mut records = Vec::new();
        let mut warnings = Vec::new();
        let mut last_message_time = std::time::Instant::now();

        // Create message stream
        let mut stream = consumer.stream();

        loop {
            // Check overall timeout
            if start.elapsed() >= self.config.timeout {
                log::debug!("Overall timeout reached after {:?}", start.elapsed());
                break;
            }

            // Check idle timeout (no messages for a while)
            if records.len() >= self.config.min_records
                && last_message_time.elapsed() >= self.config.idle_timeout
            {
                log::debug!(
                    "Idle timeout reached after {:?} with {} records",
                    last_message_time.elapsed(),
                    records.len()
                );
                break;
            }

            // Check max records
            if records.len() >= self.config.max_records {
                log::debug!("Max records ({}) reached", self.config.max_records);
                break;
            }

            // Poll for next message with short timeout
            let poll_timeout = Duration::from_millis(100);
            match tokio::time::timeout(poll_timeout, stream.next()).await {
                Ok(Some(result)) => match result {
                    Ok(borrowed_message) => {
                        last_message_time = std::time::Instant::now();

                        // Get message payload
                        if let Some(payload) = borrowed_message.payload() {
                            match self.deserialize_message(payload) {
                                Ok(record) => {
                                    records.push(record);
                                }
                                Err(e) => {
                                    warnings.push(format!(
                                        "Failed to deserialize message at offset {}: {}",
                                        borrowed_message.offset(),
                                        e
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warnings.push(format!("Error consuming message: {}", e));
                    }
                },
                Ok(None) => {
                    // Stream ended
                    break;
                }
                Err(_) => {
                    // Poll timeout - continue loop
                }
            }
        }

        let execution_time_ms = start.elapsed().as_millis() as u64;

        log::info!(
            "Captured {} records from topic '{}' in {}ms",
            records.len(),
            topic,
            execution_time_ms
        );

        Ok(CapturedOutput {
            query_name: query_name.to_string(),
            sink_name: topic.to_string(),
            records,
            execution_time_ms,
            warnings,
        })
    }

    /// Deserialize a message payload to FieldValue map
    fn deserialize_message(
        &self,
        payload: &[u8],
    ) -> TestHarnessResult<HashMap<String, FieldValue>> {
        // Try JSON deserialization first
        let content = std::str::from_utf8(payload).map_err(|e| TestHarnessError::CaptureError {
            message: format!("Invalid UTF-8 in message: {}", e),
            sink_name: "kafka".to_string(),
            source: Some(e.to_string()),
        })?;

        let json: serde_json::Value =
            serde_json::from_str(content).map_err(|e| TestHarnessError::CaptureError {
                message: format!("Failed to parse JSON: {}", e),
                sink_name: "kafka".to_string(),
                source: Some(e.to_string()),
            })?;

        json_to_field_values(&json)
    }

    /// Capture output from file sink
    pub async fn capture_file(
        &self,
        path: impl AsRef<Path>,
        query_name: &str,
    ) -> TestHarnessResult<CapturedOutput> {
        let path = path.as_ref();
        let start = std::time::Instant::now();

        log::info!("Capturing from file: {}", path.display());

        // Read file content
        let content =
            std::fs::read_to_string(path).map_err(|e| TestHarnessError::CaptureError {
                message: e.to_string(),
                sink_name: path.display().to_string(),
                source: None,
            })?;

        // Parse JSONL format
        let records = self.parse_jsonl(&content)?;
        let execution_time_ms = start.elapsed().as_millis() as u64;

        Ok(CapturedOutput {
            query_name: query_name.to_string(),
            sink_name: path.display().to_string(),
            records,
            execution_time_ms,
            warnings: Vec::new(),
        })
    }

    /// Parse JSONL (JSON Lines) format
    fn parse_jsonl(&self, content: &str) -> TestHarnessResult<Vec<HashMap<String, FieldValue>>> {
        let mut records = Vec::new();

        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let json: serde_json::Value =
                serde_json::from_str(line).map_err(|e| TestHarnessError::CaptureError {
                    message: format!("Line {}: {}", line_num + 1, e),
                    sink_name: "file".to_string(),
                    source: Some(e.to_string()),
                })?;

            let record = json_to_field_values(&json)?;
            records.push(record);
        }

        Ok(records)
    }
}

/// Convert JSON value to FieldValue map
fn json_to_field_values(
    json: &serde_json::Value,
) -> TestHarnessResult<HashMap<String, FieldValue>> {
    let obj = json
        .as_object()
        .ok_or_else(|| TestHarnessError::CaptureError {
            message: "Expected JSON object".to_string(),
            sink_name: "file".to_string(),
            source: None,
        })?;

    let mut record = HashMap::new();

    for (key, value) in obj {
        let field_value = json_value_to_field_value(value)?;
        record.insert(key.clone(), field_value);
    }

    Ok(record)
}

/// Convert single JSON value to FieldValue
fn json_value_to_field_value(value: &serde_json::Value) -> TestHarnessResult<FieldValue> {
    match value {
        serde_json::Value::Null => Ok(FieldValue::Null),
        serde_json::Value::Bool(b) => Ok(FieldValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(FieldValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(FieldValue::Float(f))
            } else {
                Ok(FieldValue::String(n.to_string()))
            }
        }
        serde_json::Value::String(s) => {
            // Try to parse as timestamp
            if let Ok(ts) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
                Ok(FieldValue::Timestamp(ts))
            } else if let Ok(ts) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                Ok(FieldValue::Timestamp(ts))
            } else {
                Ok(FieldValue::String(s.clone()))
            }
        }
        serde_json::Value::Array(arr) => {
            // Convert array to string representation
            Ok(FieldValue::String(
                serde_json::to_string(arr).unwrap_or_default(),
            ))
        }
        serde_json::Value::Object(obj) => {
            // Convert nested object to string representation
            Ok(FieldValue::String(
                serde_json::to_string(obj).unwrap_or_default(),
            ))
        }
    }
}

/// Utility to wait for file to exist with timeout
pub async fn wait_for_file(path: impl AsRef<Path>, timeout: Duration) -> TestHarnessResult<()> {
    let path = path.as_ref();
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if path.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(TestHarnessError::TimeoutError {
        message: format!("File not created: {}", path.display()),
        operation: "wait_for_file".to_string(),
        timeout_ms: timeout.as_millis() as u64,
    })
}
