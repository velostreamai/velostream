//! Dead Letter Queue (DLQ) support for test harness
//!
//! Provides functionality for:
//! - DLQ topic management (creation/cleanup)
//! - Error record capture from DLQ topics
//! - Error classification and statistics
//! - DLQ-specific assertions

use super::error::{TestHarnessError, TestHarnessResult};
use super::utils::{
    HEADER_ERROR_MESSAGE, HEADER_ERROR_MESSAGE_ALT, HEADER_ERROR_TIMESTAMP,
    HEADER_ERROR_TIMESTAMP_ALT, HEADER_ERROR_TYPE, HEADER_ERROR_TYPE_ALT, HEADER_SOURCE_TOPIC,
    HEADER_SOURCE_TOPIC_ALT, MAX_SAMPLE_ERRORS,
};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// DLQ configuration for a test
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DlqConfig {
    /// Enable DLQ capture for this query
    #[serde(default)]
    pub enabled: bool,

    /// Custom DLQ topic name (default: {output_topic}-dlq)
    #[serde(default)]
    pub topic: Option<String>,

    /// Maximum time to wait for DLQ messages
    #[serde(default = "default_dlq_timeout")]
    pub timeout_ms: u64,

    /// Expected error types (for validation)
    #[serde(default)]
    pub expected_error_types: Vec<String>,
}

fn default_dlq_timeout() -> u64 {
    5000 // 5 seconds
}

impl DlqConfig {
    /// Get the DLQ topic name for a given output topic
    pub fn get_dlq_topic(&self, output_topic: &str) -> String {
        self.topic
            .clone()
            .unwrap_or_else(|| format!("{}-dlq", output_topic))
    }
}

/// Classification of error types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ErrorType {
    /// JSON/Avro/Protobuf parsing error
    Deserialization,
    /// Schema validation failure
    SchemaValidation,
    /// Type conversion error
    TypeConversion,
    /// Null value in non-nullable field
    NullValue,
    /// SQL execution error
    SqlExecution,
    /// Timeout during processing
    Timeout,
    /// Network/connection error
    Network,
    /// Unknown/other error
    Unknown,
}

impl ErrorType {
    /// Parse error type from string
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "deserialization" | "parse" | "parsing" => ErrorType::Deserialization,
            "schema" | "schema_validation" => ErrorType::SchemaValidation,
            "type" | "type_conversion" | "cast" => ErrorType::TypeConversion,
            "null" | "null_value" | "nullable" => ErrorType::NullValue,
            "sql" | "sql_execution" | "query" => ErrorType::SqlExecution,
            "timeout" => ErrorType::Timeout,
            "network" | "connection" => ErrorType::Network,
            _ => ErrorType::Unknown,
        }
    }

    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorType::Deserialization => "deserialization",
            ErrorType::SchemaValidation => "schema_validation",
            ErrorType::TypeConversion => "type_conversion",
            ErrorType::NullValue => "null_value",
            ErrorType::SqlExecution => "sql_execution",
            ErrorType::Timeout => "timeout",
            ErrorType::Network => "network",
            ErrorType::Unknown => "unknown",
        }
    }
}

/// A captured error record from the DLQ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqRecord {
    /// Original record key (if available)
    pub key: Option<String>,

    /// Original record payload (may be malformed)
    pub original_payload: Option<String>,

    /// Error message
    pub error_message: String,

    /// Classified error type
    pub error_type: ErrorType,

    /// Source topic
    pub source_topic: Option<String>,

    /// Timestamp when error occurred
    pub error_timestamp: Option<i64>,

    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl DlqRecord {
    /// Create from raw message bytes
    pub fn from_message(
        key: Option<&[u8]>,
        payload: Option<&[u8]>,
        headers: Option<&rdkafka::message::BorrowedHeaders>,
    ) -> Self {
        let mut record = DlqRecord {
            key: key.map(|k| String::from_utf8_lossy(k).to_string()),
            original_payload: payload.map(|p| String::from_utf8_lossy(p).to_string()),
            error_message: String::new(),
            error_type: ErrorType::Unknown,
            source_topic: None,
            error_timestamp: None,
            metadata: HashMap::new(),
        };

        // Extract error info from headers if available
        if let Some(hdrs) = headers {
            use rdkafka::message::Headers;
            for i in 0..hdrs.count() {
                let header = hdrs.get(i);
                let key = header.key;
                if let Some(value) = header.value {
                    let value_str = String::from_utf8_lossy(value).to_string();
                    match key {
                        HEADER_ERROR_MESSAGE | HEADER_ERROR_MESSAGE_ALT => {
                            record.error_message = value_str.clone();
                        }
                        HEADER_ERROR_TYPE | HEADER_ERROR_TYPE_ALT => {
                            record.error_type = ErrorType::parse(&value_str);
                        }
                        HEADER_SOURCE_TOPIC | HEADER_SOURCE_TOPIC_ALT => {
                            record.source_topic = Some(value_str.clone());
                        }
                        HEADER_ERROR_TIMESTAMP | HEADER_ERROR_TIMESTAMP_ALT => {
                            record.error_timestamp = value_str.parse().ok();
                        }
                        _ => {
                            record.metadata.insert(key.to_string(), value_str);
                        }
                    }
                }
            }
        }

        // Try to parse error info from payload if not in headers
        if record.error_message.is_empty() {
            if let Some(ref payload) = record.original_payload {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
                    if let Some(msg) = json.get("error_message").and_then(|v| v.as_str()) {
                        record.error_message = msg.to_string();
                    }
                    if let Some(typ) = json.get("error_type").and_then(|v| v.as_str()) {
                        record.error_type = ErrorType::parse(typ);
                    }
                }
            }
        }

        record
    }
}

/// Statistics about captured DLQ records
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DlqStatistics {
    /// Total number of error records
    pub total_errors: usize,

    /// Errors by type
    pub errors_by_type: HashMap<String, usize>,

    /// Error rate (errors / total records processed)
    pub error_rate: Option<f64>,

    /// Sample error messages (first N of each type)
    pub sample_errors: Vec<DlqRecord>,
}

impl DlqStatistics {
    /// Create new empty statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a DLQ record to statistics
    pub fn add_record(&mut self, record: DlqRecord) {
        self.total_errors += 1;

        let type_key = record.error_type.as_str().to_string();
        *self.errors_by_type.entry(type_key).or_insert(0) += 1;

        // Keep sample of first N errors (configurable via MAX_SAMPLE_ERRORS)
        if self.sample_errors.len() < MAX_SAMPLE_ERRORS {
            self.sample_errors.push(record);
        }
    }

    /// Calculate error rate given total records processed
    pub fn calculate_error_rate(&mut self, total_records: usize) {
        if total_records > 0 {
            self.error_rate = Some(self.total_errors as f64 / total_records as f64);
        }
    }

    /// Get error rate as percentage
    pub fn error_rate_percent(&self) -> Option<f64> {
        self.error_rate.map(|r| r * 100.0)
    }
}

/// DLQ capture utility
pub struct DlqCapture {
    /// Kafka consumer for DLQ topic
    consumer: StreamConsumer,

    /// DLQ topic name
    topic: String,

    /// Captured records
    records: Vec<DlqRecord>,

    /// Statistics
    statistics: DlqStatistics,
}

impl DlqCapture {
    /// Create a new DLQ capture instance
    pub fn new(bootstrap_servers: &str, topic: &str, group_id: &str) -> TestHarnessResult<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("session.timeout.ms", "6000")
            .create()
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to create DLQ consumer: {}", e),
                source: None,
            })?;

        consumer
            .subscribe(&[topic])
            .map_err(|e| TestHarnessError::InfraError {
                message: format!("Failed to subscribe to DLQ topic {}: {}", topic, e),
                source: None,
            })?;

        Ok(Self {
            consumer,
            topic: topic.to_string(),
            records: Vec::new(),
            statistics: DlqStatistics::new(),
        })
    }

    /// Capture DLQ records with timeout
    pub async fn capture(&mut self, timeout: Duration) -> TestHarnessResult<()> {
        use futures::StreamExt;
        use tokio::time::timeout as tokio_timeout;

        let start = std::time::Instant::now();
        let mut stream = self.consumer.stream();

        while start.elapsed() < timeout {
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                break;
            }

            match tokio_timeout(remaining, stream.next()).await {
                Ok(Some(result)) => match result {
                    Ok(msg) => {
                        let record =
                            DlqRecord::from_message(msg.key(), msg.payload(), msg.headers());
                        self.statistics.add_record(record.clone());
                        self.records.push(record);
                    }
                    Err(e) => {
                        log::warn!("Error consuming DLQ message: {}", e);
                    }
                },
                Ok(None) => {
                    // Stream ended
                    break;
                }
                Err(_) => {
                    // Timeout - this is expected
                    break;
                }
            }
        }

        Ok(())
    }

    /// Get captured records
    pub fn records(&self) -> &[DlqRecord] {
        &self.records
    }

    /// Get statistics
    pub fn statistics(&self) -> &DlqStatistics {
        &self.statistics
    }

    /// Get mutable statistics (for calculating error rate)
    pub fn statistics_mut(&mut self) -> &mut DlqStatistics {
        &mut self.statistics
    }

    /// Get topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Check if any errors were captured
    pub fn has_errors(&self) -> bool {
        !self.records.is_empty()
    }

    /// Get count of specific error type
    pub fn count_by_type(&self, error_type: &ErrorType) -> usize {
        self.records
            .iter()
            .filter(|r| &r.error_type == error_type)
            .count()
    }
}

/// Ensure proper cleanup on drop to prevent librdkafka hanging
impl Drop for DlqCapture {
    fn drop(&mut self) {
        log::debug!("DlqCapture: Unsubscribing from topic '{}'", self.topic);
        self.consumer.unsubscribe();
        log::debug!("DlqCapture: Dropped successfully");
    }
}

/// Captured DLQ output for a query
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapturedDlqOutput {
    /// DLQ topic name
    pub topic: String,

    /// All captured error records
    pub records: Vec<DlqRecord>,

    /// Statistics summary
    pub statistics: DlqStatistics,
}

impl CapturedDlqOutput {
    /// Create from DLQ capture
    pub fn from_capture(capture: &DlqCapture) -> Self {
        Self {
            topic: capture.topic().to_string(),
            records: capture.records().to_vec(),
            statistics: capture.statistics().clone(),
        }
    }

    /// Create empty output
    pub fn empty(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            records: Vec::new(),
            statistics: DlqStatistics::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_type_parsing() {
        assert_eq!(
            ErrorType::parse("deserialization"),
            ErrorType::Deserialization
        );
        assert_eq!(ErrorType::parse("parse"), ErrorType::Deserialization);
        assert_eq!(ErrorType::parse("PARSING"), ErrorType::Deserialization);
        assert_eq!(
            ErrorType::parse("schema_validation"),
            ErrorType::SchemaValidation
        );
        assert_eq!(ErrorType::parse("timeout"), ErrorType::Timeout);
        assert_eq!(ErrorType::parse("unknown_type"), ErrorType::Unknown);
    }

    #[test]
    fn test_dlq_statistics() {
        let mut stats = DlqStatistics::new();

        stats.add_record(DlqRecord {
            key: None,
            original_payload: None,
            error_message: "Parse error".to_string(),
            error_type: ErrorType::Deserialization,
            source_topic: None,
            error_timestamp: None,
            metadata: HashMap::new(),
        });

        stats.add_record(DlqRecord {
            key: None,
            original_payload: None,
            error_message: "Timeout".to_string(),
            error_type: ErrorType::Timeout,
            source_topic: None,
            error_timestamp: None,
            metadata: HashMap::new(),
        });

        assert_eq!(stats.total_errors, 2);
        assert_eq!(stats.errors_by_type.get("deserialization"), Some(&1));
        assert_eq!(stats.errors_by_type.get("timeout"), Some(&1));

        stats.calculate_error_rate(100);
        assert_eq!(stats.error_rate, Some(0.02));
        assert_eq!(stats.error_rate_percent(), Some(2.0));
    }

    #[test]
    fn test_dlq_config() {
        let config = DlqConfig {
            enabled: true,
            topic: None,
            timeout_ms: 5000,
            expected_error_types: vec!["deserialization".to_string()],
        };

        assert_eq!(config.get_dlq_topic("output_topic"), "output_topic-dlq");

        let custom_config = DlqConfig {
            enabled: true,
            topic: Some("my-dlq".to_string()),
            timeout_ms: 5000,
            expected_error_types: vec![],
        };

        assert_eq!(custom_config.get_dlq_topic("output_topic"), "my-dlq");
    }
}
