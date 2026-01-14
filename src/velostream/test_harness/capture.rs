//! Sink output capture for test harness
//!
//! Captures and deserializes output from query execution for assertion validation.
//!
//! # Supported Capture Sources
//! - **Kafka topics**: Consumes messages from output topics with configurable timeouts
//! - **File sinks**: Reads JSONL files from file-based sinks
//!
//! # Serialization Formats
//! The capture module supports multiple deserialization formats via [`CaptureFormat`]:
//!
//! - **JSON** (default): UTF-8 encoded JSON objects, no schema required
//! - **Avro**: Binary Avro format, requires `capture_schema` with Avro schema JSON
//! - **Protobuf**: Not yet implemented, returns explicit error with workaround suggestion
//!
//! # Test Spec Configuration
//! Configure capture format in your `.test.yaml`:
//!
//! ```yaml
//! queries:
//!   - name: my_query
//!     capture_format: avro  # or json (default)
//!     capture_schema: |
//!       {"type":"record","name":"MyRecord","fields":[...]}
//! ```

use super::error::{TestHarnessError, TestHarnessResult};
use super::executor::CapturedOutput;
use super::infra::create_kafka_config;
use crate::velostream::serialization::avro_codec::deserialize_from_avro;
use crate::velostream::sql::execution::types::{FieldValue, StreamRecord};
use futures::StreamExt;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::message::{Headers, Message};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

/// Serialization format for capture deserialization
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CaptureFormat {
    /// JSON format (default)
    #[default]
    Json,
    /// Avro binary format (requires schema)
    Avro,
    /// Protobuf binary format (requires schema)
    Protobuf,
}

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

    /// Serialization format of captured messages
    pub format: CaptureFormat,

    /// Schema JSON for Avro/Protobuf deserialization (required for non-JSON formats)
    pub schema_json: Option<String>,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            min_records: 1,
            max_records: 100_000,
            idle_timeout: Duration::from_secs(5),
            format: CaptureFormat::Json,
            schema_json: None,
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
        let consumer: StreamConsumer<DefaultConsumerContext> =
            create_kafka_config(&self.bootstrap_servers)
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

        let mut records: Vec<StreamRecord> = Vec::new();
        let mut warnings = Vec::new();
        let mut last_message_time: Option<std::time::Instant> = None;
        let mut received_first_message = false;

        // Create message stream
        let mut stream = consumer.stream();

        loop {
            // Check overall timeout
            if start.elapsed() >= self.config.timeout {
                log::debug!("Overall timeout reached after {:?}", start.elapsed());
                break;
            }

            // Check idle timeout (no messages for a while)
            // Only apply idle timeout AFTER we've received at least one message
            // This prevents exiting early while waiting for partition assignment
            if received_first_message
                && records.len() >= self.config.min_records
                && last_message_time
                    .map(|t| t.elapsed() >= self.config.idle_timeout)
                    .unwrap_or(false)
            {
                log::debug!(
                    "Idle timeout reached after {:?} with {} records",
                    last_message_time.map(|t| t.elapsed()).unwrap_or_default(),
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
                        // Mark that we've received our first message - idle timeout now applies
                        if !received_first_message {
                            received_first_message = true;
                            log::debug!(
                                "Received first message from topic '{}' after {:?}",
                                topic,
                                start.elapsed()
                            );
                        }
                        last_message_time = Some(std::time::Instant::now());

                        // Get message payload and build StreamRecord with full metadata
                        if let Some(payload) = borrowed_message.payload() {
                            match self.deserialize_message(payload, topic) {
                                Ok(fields) => {
                                    // Extract message key as FieldValue
                                    let key = borrowed_message.key().map(|k| {
                                        std::str::from_utf8(k)
                                            .map(|s| FieldValue::String(s.to_string()))
                                            .unwrap_or_else(|_| {
                                                FieldValue::String(format!("{:?}", k))
                                            })
                                    });

                                    // Extract headers
                                    let headers = borrowed_message
                                        .headers()
                                        .map(|h| {
                                            h.iter()
                                                .map(|header| {
                                                    (
                                                        header.key.to_string(),
                                                        header
                                                            .value
                                                            .map(|v| {
                                                                String::from_utf8_lossy(v)
                                                                    .to_string()
                                                            })
                                                            .unwrap_or_default(),
                                                    )
                                                })
                                                .collect::<HashMap<String, String>>()
                                        })
                                        .unwrap_or_default();

                                    // Build StreamRecord with full Kafka metadata
                                    let stream_record = StreamRecord {
                                        fields,
                                        timestamp: borrowed_message
                                            .timestamp()
                                            .to_millis()
                                            .unwrap_or(0),
                                        offset: borrowed_message.offset(),
                                        partition: borrowed_message.partition(),
                                        headers,
                                        event_time: None, // Not extracted during capture
                                        topic: Some(FieldValue::String(topic.to_string())),
                                        key,
                                    };

                                    records.push(stream_record);
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

        // Unsubscribe before dropping to ensure clean shutdown
        consumer.unsubscribe();

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
            topic: Some(topic.to_string()),
            records,
            execution_time_ms,
            warnings,
            memory_peak_bytes: None,
            memory_growth_bytes: None,
        })
    }

    /// Deserialize a message payload to FieldValue map based on configured format
    ///
    /// # Arguments
    /// * `payload` - Raw message bytes to deserialize
    /// * `topic` - Topic name for error context
    fn deserialize_message(
        &self,
        payload: &[u8],
        topic: &str,
    ) -> TestHarnessResult<HashMap<String, FieldValue>> {
        match &self.config.format {
            CaptureFormat::Json => self.deserialize_json(payload, topic),
            CaptureFormat::Avro => self.deserialize_avro(payload, topic),
            CaptureFormat::Protobuf => self.deserialize_protobuf(topic),
        }
    }

    /// Deserialize JSON message
    ///
    /// # Arguments
    /// * `payload` - Raw JSON bytes (UTF-8 encoded)
    /// * `topic` - Topic name for error context
    fn deserialize_json(
        &self,
        payload: &[u8],
        topic: &str,
    ) -> TestHarnessResult<HashMap<String, FieldValue>> {
        let content = std::str::from_utf8(payload).map_err(|e| TestHarnessError::CaptureError {
            message: format!(
                "Invalid UTF-8 in message from topic '{}': {} (payload size: {} bytes)",
                topic,
                e,
                payload.len()
            ),
            sink_name: topic.to_string(),
            source: Some(e.to_string()),
        })?;

        let json: serde_json::Value =
            serde_json::from_str(content).map_err(|e| TestHarnessError::CaptureError {
                message: format!(
                    "Failed to parse JSON from topic '{}': {} (content preview: {}...)",
                    topic,
                    e,
                    &content[..content.len().min(100)]
                ),
                sink_name: topic.to_string(),
                source: Some(e.to_string()),
            })?;

        json_to_field_values(&json, topic)
    }

    /// Deserialize Avro message using configured schema
    ///
    /// # Arguments
    /// * `payload` - Raw Avro binary bytes
    /// * `topic` - Topic name for error context
    ///
    /// # Errors
    /// Returns error if `capture_schema` was not configured in the test spec
    fn deserialize_avro(
        &self,
        payload: &[u8],
        topic: &str,
    ) -> TestHarnessResult<HashMap<String, FieldValue>> {
        let schema_json = self.config.schema_json.as_ref().ok_or_else(|| {
            TestHarnessError::CaptureError {
                message: format!(
                    "Avro deserialization for topic '{}' requires a schema. \
                     Add 'capture_schema' with the Avro schema JSON to your test spec, \
                     or use 'capture_format: json' if the sink outputs JSON.",
                    topic
                ),
                sink_name: topic.to_string(),
                source: None,
            }
        })?;

        deserialize_from_avro(payload, schema_json).map_err(|e| TestHarnessError::CaptureError {
            message: format!(
                "Failed to deserialize Avro from topic '{}': {} (payload size: {} bytes)",
                topic,
                e,
                payload.len()
            ),
            sink_name: topic.to_string(),
            source: Some(e.to_string()),
        })
    }

    /// Deserialize Protobuf message
    ///
    /// # Note
    /// Protobuf capture deserialization is not yet implemented.
    /// Returns an explicit error directing users to use JSON or Avro instead.
    fn deserialize_protobuf(&self, topic: &str) -> TestHarnessResult<HashMap<String, FieldValue>> {
        Err(TestHarnessError::CaptureError {
            message: format!(
                "Protobuf capture deserialization is not yet implemented for topic '{}'. \
                 Workaround: Use 'capture_format: json' if your sink can output JSON, \
                 or 'capture_format: avro' with an equivalent Avro schema.",
                topic
            ),
            sink_name: topic.to_string(),
            source: None,
        })
    }

    /// Capture output from file sink
    ///
    /// Supports JSON Lines (JSONL) format. Each line must be a valid JSON object.
    /// The configured `CaptureFormat` is validated but currently only JSON is supported
    /// for file capture.
    pub async fn capture_file(
        &self,
        path: impl AsRef<Path>,
        query_name: &str,
    ) -> TestHarnessResult<CapturedOutput> {
        let path = path.as_ref();
        let path_str = path.display().to_string();
        let start = std::time::Instant::now();

        log::info!(
            "Capturing from file: {} (format: {:?})",
            path_str,
            self.config.format
        );

        // Validate format - file capture currently only supports JSON
        match &self.config.format {
            CaptureFormat::Json => {}
            CaptureFormat::Avro => {
                return Err(TestHarnessError::CaptureError {
                    message: format!(
                        "Avro format is not supported for file capture from '{}'. \
                         File sinks typically output JSON Lines format. \
                         Use 'capture_format: json' in your test spec.",
                        path_str
                    ),
                    sink_name: path_str,
                    source: None,
                });
            }
            CaptureFormat::Protobuf => {
                return Err(TestHarnessError::CaptureError {
                    message: format!(
                        "Protobuf format is not supported for file capture from '{}'. \
                         File sinks typically output JSON Lines format. \
                         Use 'capture_format: json' in your test spec.",
                        path_str
                    ),
                    sink_name: path_str,
                    source: None,
                });
            }
        }

        // Read file content
        let content = std::fs::read_to_string(path).map_err(|e| TestHarnessError::CaptureError {
            message: format!("Failed to read file '{}': {}", path_str, e),
            sink_name: path_str.clone(),
            source: Some(e.to_string()),
        })?;

        // Parse JSONL format and convert to StreamRecords
        let field_maps = self.parse_jsonl(&content, &path_str)?;
        let records: Vec<StreamRecord> = field_maps.into_iter().map(StreamRecord::new).collect();
        let execution_time_ms = start.elapsed().as_millis() as u64;

        log::info!(
            "Captured {} records from file '{}' in {}ms",
            records.len(),
            path_str,
            execution_time_ms
        );

        Ok(CapturedOutput {
            query_name: query_name.to_string(),
            sink_name: path_str, // Moved, not cloned - last use
            topic: None,         // File capture, not Kafka
            records,
            execution_time_ms,
            warnings: Vec::new(),
            memory_peak_bytes: None,
            memory_growth_bytes: None,
        })
    }

    /// Parse JSONL (JSON Lines) format
    ///
    /// # Arguments
    /// * `content` - File content to parse
    /// * `file_path` - File path for error context
    fn parse_jsonl(
        &self,
        content: &str,
        file_path: &str,
    ) -> TestHarnessResult<Vec<HashMap<String, FieldValue>>> {
        let mut records = Vec::new();

        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let json: serde_json::Value =
                serde_json::from_str(line).map_err(|e| TestHarnessError::CaptureError {
                    message: format!(
                        "Invalid JSON at line {} in '{}': {}",
                        line_num + 1,
                        file_path,
                        e
                    ),
                    sink_name: file_path.to_string(),
                    source: Some(e.to_string()),
                })?;

            let context = format!("{}:line {}", file_path, line_num + 1);
            let record = json_to_field_values(&json, &context)?;
            records.push(record);
        }

        Ok(records)
    }
}

/// Convert JSON value to FieldValue map
///
/// Useful for test assertions that need to convert captured JSON to FieldValue maps.
///
/// # Arguments
/// * `json` - The JSON value to convert (must be an object)
/// * `source_context` - Source identifier for error messages (topic name or file path)
///
/// # Example
/// ```ignore
/// let json = serde_json::json!({"id": 42, "name": "test"});
/// let fields = json_to_field_values(&json, "my_topic")?;
/// assert_eq!(fields.get("id"), Some(&FieldValue::Integer(42)));
/// ```
pub fn json_to_field_values(
    json: &serde_json::Value,
    source_context: &str,
) -> TestHarnessResult<HashMap<String, FieldValue>> {
    let obj = json
        .as_object()
        .ok_or_else(|| TestHarnessError::CaptureError {
            message: format!(
                "Expected JSON object from '{}', got {}",
                source_context,
                json_type_name(json)
            ),
            sink_name: source_context.to_string(),
            source: None,
        })?;

    let mut record = HashMap::new();

    for (key, value) in obj {
        let field_value = json_value_to_field_value(value, key, source_context)?;
        record.insert(key.clone(), field_value);
    }

    Ok(record)
}

/// Get a human-readable name for a JSON value type
///
/// Useful for error messages and debugging.
pub fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Convert single JSON value to FieldValue
///
/// # Arguments
/// * `value` - The JSON value to convert
/// * `field_name` - Field name for error context
/// * `source_context` - Source identifier for error messages
fn json_value_to_field_value(
    value: &serde_json::Value,
    field_name: &str,
    source_context: &str,
) -> TestHarnessResult<FieldValue> {
    match value {
        serde_json::Value::Null => Ok(FieldValue::Null),
        serde_json::Value::Bool(b) => Ok(FieldValue::Boolean(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(FieldValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(FieldValue::Float(f))
            } else {
                // Large numbers that don't fit in i64/f64 - convert to string
                log::debug!(
                    "Number '{}' in field '{}' from '{}' doesn't fit in i64/f64, storing as string",
                    n,
                    field_name,
                    source_context
                );
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
