use crate::unit::common::*;
use std::error::Error;
use std::fmt;

/// Enhanced error context for better production debugging
#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub operation: String,
    pub topic: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub correlation_id: String,
    pub metadata: std::collections::HashMap<String, String>,
}

impl ErrorContext {
    pub fn new(operation: &str, topic: &str) -> Self {
        Self {
            operation: operation.to_string(),
            topic: topic.to_string(),
            timestamp: chrono::Utc::now(),
            correlation_id: Uuid::new_v4().to_string(),
            metadata: std::collections::HashMap::new(),
        }
    }

    pub fn with_metadata<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Enhanced Kafka error with operational context
#[derive(Debug)]
pub struct ContextualKafkaError {
    pub inner: KafkaClientError,
    pub context: ErrorContext,
}

impl fmt::Display for ContextualKafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Kafka operation '{}' failed on topic '{}' at {} (correlation_id: {}): {}",
            self.context.operation,
            self.context.topic,
            self.context.timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
            self.context.correlation_id,
            self.inner
        )
    }
}

impl Error for ContextualKafkaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.inner)
    }
}

/// Test comprehensive error chain preservation across multiple layers
#[test]
fn test_comprehensive_error_source_chain_preservation() {
    // Create a deep error chain: JSON -> Serialization -> Kafka -> Contextual
    let invalid_json = "\x00\x01\x02invalid_json";
    let json_err = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
    let serialization_error = SerializationError::JsonSerializationFailed(Box::new(json_err));
    let kafka_error: KafkaClientError = serialization_error.into();

    // Add operational context with detailed metadata
    let context = ErrorContext::new("send_message", "high-priority-orders")
        .with_metadata("key", "order-12345")
        .with_metadata("partition", "3")
        .with_metadata("offset", "987654")
        .with_metadata("broker", "broker-2.kafka.internal:9092")
        .with_metadata("retry_attempt", "2")
        .with_metadata("batch_size", "100")
        .with_metadata("transaction_id", "tx-abcdef123456");

    let contextual_error = ContextualKafkaError {
        inner: kafka_error,
        context,
    };

    // Test complete error chain traversal
    assert!(
        contextual_error.source().is_some(),
        "Top-level error should have source"
    );

    let kafka_source = contextual_error.source().unwrap();
    match kafka_source.downcast_ref::<KafkaClientError>() {
        Some(KafkaClientError::SerializationError(ser_err)) => {
            println!("✅ Layer 1: KafkaClientError found in chain");

            // Traverse deeper into the error chain
            assert!(
                ser_err.source().is_some(),
                "SerializationError should have a source, but it returned None"
            );

            let serialization_source = ser_err.source().unwrap();
            if let Some(_json_err) = serialization_source.downcast_ref::<serde_json::Error>() {
                println!("✅ Layer 2: SerializationError -> JSON error chain preserved");
            } else {
                panic!("Expected serde_json::Error at bottom of chain");
            }
        }
        _ => panic!(
            "Expected SerializationError in chain, got: {:?}",
            kafka_source
        ),
    }

    // Test error display includes comprehensive context
    let error_string = contextual_error.to_string();
    assert!(
        error_string.contains("send_message"),
        "Should contain operation"
    );
    assert!(
        error_string.contains("high-priority-orders"),
        "Should contain topic"
    );
    assert!(
        error_string.contains("correlation_id"),
        "Should contain correlation ID"
    );

    // Verify all metadata is accessible
    assert_eq!(
        contextual_error.context.metadata.get("key"),
        Some(&"order-12345".to_string())
    );
    assert_eq!(
        contextual_error.context.metadata.get("partition"),
        Some(&"3".to_string())
    );
    assert_eq!(
        contextual_error.context.metadata.get("broker"),
        Some(&"broker-2.kafka.internal:9092".to_string())
    );
    assert_eq!(
        contextual_error.context.metadata.get("retry_attempt"),
        Some(&"2".to_string())
    );

    println!("✅ All error chain layers preserved and accessible");
}

/// Test operational error metadata tracking - which operation failed where
#[test]
fn test_operational_error_metadata_tracking() {
    // Simulate a complex pipeline with multiple operations
    let operations = vec![
        ("validate_schema", "user-events", "schema-validator-1"),
        ("transform_message", "user-events", "transformer-pod-3"),
        ("enrich_data", "user-events", "enricher-service-2"),
        ("send_to_topic", "processed-events", "producer-client-7"),
        ("confirm_delivery", "processed-events", "delivery-tracker"),
    ];

    let mut operation_contexts = Vec::new();

    for (i, (operation, topic, component)) in operations.iter().enumerate() {
        let context = ErrorContext::new(operation, topic)
            .with_metadata("component", *component)
            .with_metadata("pipeline_stage", format!("stage_{}", i + 1))
            .with_metadata("instance_id", format!("instance_{:03}", i * 17 + 42))
            .with_metadata("request_id", "req_abc123_xyz789")
            .with_metadata("user_id", "user_987654321")
            .with_metadata("session_id", "sess_def456_uvw012")
            .with_metadata("environment", "production")
            .with_metadata("region", "us-west-2")
            .with_metadata(
                "availability_zone",
                format!("us-west-2{}", char::from(b'a' + i as u8)),
            )
            .with_metadata("cpu_usage", format!("{:.1}%", 45.0 + i as f64 * 8.3))
            .with_metadata("memory_usage", format!("{:.1}MB", 256.0 + i as f64 * 128.0));

        operation_contexts.push(context);
    }

    // Verify each operation context contains detailed metadata
    for (i, context) in operation_contexts.iter().enumerate() {
        assert_eq!(context.operation, operations[i].0);
        assert_eq!(context.topic, operations[i].1);
        assert_eq!(
            context.metadata.get("component"),
            Some(&operations[i].2.to_string())
        );
        assert_eq!(
            context.metadata.get("pipeline_stage"),
            Some(&format!("stage_{}", i + 1))
        );
        assert_eq!(
            context.metadata.get("request_id"),
            Some(&"req_abc123_xyz789".to_string())
        );
        assert_eq!(
            context.metadata.get("environment"),
            Some(&"production".to_string())
        );

        // Verify correlation ID is unique per operation
        assert!(!context.correlation_id.is_empty());
        assert!(Uuid::parse_str(&context.correlation_id).is_ok());

        // Verify timestamp progression (each operation happens sequentially)
        if i > 0 {
            assert!(
                context.timestamp >= operation_contexts[i - 1].timestamp,
                "Operation timestamps should be non-decreasing"
            );
        }
    }

    // Test error creation with operational context
    let failed_operation_idx = 2; // "enrich_data" operation fails
    let error = ContextualKafkaError {
        inner: KafkaClientError::Timeout,
        context: operation_contexts[failed_operation_idx].clone(),
    };

    // Verify we can pinpoint exactly which operation failed and where
    assert_eq!(error.context.operation, "enrich_data");
    assert_eq!(error.context.topic, "user-events");
    assert_eq!(
        error.context.metadata.get("component"),
        Some(&"enricher-service-2".to_string())
    );
    assert_eq!(
        error.context.metadata.get("pipeline_stage"),
        Some(&"stage_3".to_string())
    );

    // Test structured error report generation
    let error_report = format!(
        "OPERATION FAILURE REPORT\n\
         Operation: {}\n\
         Topic: {}\n\
         Component: {}\n\
         Stage: {}\n\
         Instance: {}\n\
         Correlation ID: {}\n\
         Request ID: {}\n\
         Environment: {}\n\
         Region: {}\n\
         AZ: {}\n\
         Resource Usage: CPU {}, Memory {}\n\
         Error: {}",
        error.context.operation,
        error.context.topic,
        error
            .context
            .metadata
            .get("component")
            .unwrap_or(&"unknown".to_string()),
        error
            .context
            .metadata
            .get("pipeline_stage")
            .unwrap_or(&"unknown".to_string()),
        error
            .context
            .metadata
            .get("instance_id")
            .unwrap_or(&"unknown".to_string()),
        error.context.correlation_id,
        error
            .context
            .metadata
            .get("request_id")
            .unwrap_or(&"unknown".to_string()),
        error
            .context
            .metadata
            .get("environment")
            .unwrap_or(&"unknown".to_string()),
        error
            .context
            .metadata
            .get("region")
            .unwrap_or(&"unknown".to_string()),
        error
            .context
            .metadata
            .get("availability_zone")
            .unwrap_or(&"unknown".to_string()),
        error
            .context
            .metadata
            .get("cpu_usage")
            .unwrap_or(&"unknown".to_string()),
        error
            .context
            .metadata
            .get("memory_usage")
            .unwrap_or(&"unknown".to_string()),
        error.inner
    );

    println!("\n{}", error_report);

    // Verify the error report contains all critical debugging information
    assert!(error_report.contains("enrich_data"));
    assert!(error_report.contains("enricher-service-2"));
    assert!(error_report.contains("stage_3"));
    assert!(error_report.contains("production"));
    assert!(error_report.contains("us-west-2"));
    assert!(error_report.contains("req_abc123_xyz789"));

    println!("✅ Operational error metadata tracking validated");
}

/// Test error categorization for alerting
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum ErrorSeverity {
    Critical, // Requires immediate attention
    Warning,  // Should be monitored
    Info,     // Recoverable/expected
}

pub fn categorize_error(error: &KafkaClientError) -> ErrorSeverity {
    match error {
        KafkaClientError::KafkaError(kafka_err) => {
            let error_str = kafka_err.to_string().to_lowercase();
            if error_str.contains("broker") && error_str.contains("not available") {
                ErrorSeverity::Critical
            } else if error_str.contains("timeout") {
                ErrorSeverity::Warning
            } else {
                ErrorSeverity::Critical
            }
        }
        KafkaClientError::SerializationError(_) => ErrorSeverity::Critical,
        KafkaClientError::Timeout => ErrorSeverity::Warning,
        KafkaClientError::NoMessage => ErrorSeverity::Info,
        KafkaClientError::ConfigurationError(_) => ErrorSeverity::Critical,
    }
}

#[test]
fn test_error_severity_categorization() {
    // Test timeout categorization
    let timeout_error = KafkaClientError::Timeout;
    assert_eq!(categorize_error(&timeout_error), ErrorSeverity::Warning);

    // Test no message categorization
    let no_message_error = KafkaClientError::NoMessage;
    assert_eq!(categorize_error(&no_message_error), ErrorSeverity::Info);

    // Test serialization error categorization
    let invalid_json = "\x00\x01\x02invalid_json";
    let json_err = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
    let serialization_error = SerializationError::JsonSerializationFailed(Box::new(json_err));
    let kafka_serialization_error: KafkaClientError = serialization_error.into();
    assert_eq!(
        categorize_error(&kafka_serialization_error),
        ErrorSeverity::Critical
    );
}

/// Test structured logging integration
pub struct ErrorMetrics {
    pub total_errors: std::collections::HashMap<String, u64>,
    pub errors_by_severity: std::collections::HashMap<ErrorSeverity, u64>,
    pub last_error_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl Default for ErrorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ErrorMetrics {
    pub fn new() -> Self {
        Self {
            total_errors: std::collections::HashMap::new(),
            errors_by_severity: std::collections::HashMap::new(),
            last_error_time: None,
        }
    }

    pub fn record_error(&mut self, error: &ContextualKafkaError) {
        // Count by operation
        let operation_count = self
            .total_errors
            .entry(error.context.operation.clone())
            .or_insert(0);
        *operation_count += 1;

        // Count by severity
        let severity = categorize_error(&error.inner);
        let severity_count = self.errors_by_severity.entry(severity).or_insert(0);
        *severity_count += 1;

        // Update last error time
        self.last_error_time = Some(error.context.timestamp);
    }

    pub fn get_error_rate(&self, operation: &str, time_window: chrono::Duration) -> f64 {
        if let Some(last_error) = self.last_error_time {
            let now = chrono::Utc::now();
            if now.signed_duration_since(last_error) <= time_window {
                let count = self.total_errors.get(operation).unwrap_or(&0);
                *count as f64 / time_window.num_seconds() as f64
            } else {
                0.0
            }
        } else {
            0.0
        }
    }
}

#[test]
fn test_error_metrics_collection() {
    let mut metrics = ErrorMetrics::new();

    // Create test errors
    let context1 = ErrorContext::new("send_message", "topic1");
    let error1 = ContextualKafkaError {
        inner: KafkaClientError::Timeout,
        context: context1,
    };

    let context2 = ErrorContext::new("send_message", "topic2");
    let error2 = ContextualKafkaError {
        inner: KafkaClientError::NoMessage,
        context: context2,
    };

    let context3 = ErrorContext::new("poll_message", "topic1");
    let error3 = ContextualKafkaError {
        inner: KafkaClientError::Timeout,
        context: context3,
    };

    // Record errors
    metrics.record_error(&error1);
    metrics.record_error(&error2);
    metrics.record_error(&error3);

    // Verify counts
    assert_eq!(metrics.total_errors.get("send_message"), Some(&2));
    assert_eq!(metrics.total_errors.get("poll_message"), Some(&1));

    // Verify severity counts
    assert_eq!(
        metrics.errors_by_severity.get(&ErrorSeverity::Warning),
        Some(&2)
    ); // Timeouts
    assert_eq!(
        metrics.errors_by_severity.get(&ErrorSeverity::Info),
        Some(&1)
    ); // NoMessage

    // Verify last error time is set
    assert!(metrics.last_error_time.is_some());

    // Test error rate calculation
    let error_rate = metrics.get_error_rate("send_message", chrono::Duration::minutes(1));
    assert!(
        error_rate > 0.0,
        "Should have positive error rate for recent errors"
    );
}

/// Test correlation ID tracking across operations
#[test]
fn test_correlation_id_tracking() {
    let correlation_id = Uuid::new_v4().to_string();

    // Simulate related operations with same correlation ID
    let contexts = vec![
        ErrorContext::new("validate_message", "orders")
            .with_metadata("correlation_id", &correlation_id)
            .with_metadata("stage", "validation"),
        ErrorContext::new("send_message", "orders")
            .with_metadata("correlation_id", &correlation_id)
            .with_metadata("stage", "sending"),
        ErrorContext::new("confirm_delivery", "orders")
            .with_metadata("correlation_id", &correlation_id)
            .with_metadata("stage", "confirmation"),
    ];

    // Verify all contexts share the same correlation metadata
    for context in &contexts {
        assert_eq!(
            context.metadata.get("correlation_id"),
            Some(&correlation_id)
        );
    }

    // Verify we can track a request across multiple operations
    let operations: Vec<&str> = contexts.iter().map(|c| c.operation.as_str()).collect();

    assert_eq!(
        operations,
        vec!["validate_message", "send_message", "confirm_delivery"]
    );

    println!(
        "✅ Correlation ID {} tracked across operations: {:?}",
        correlation_id, operations
    );
}

/// Test error aggregation patterns
#[test]
fn test_error_aggregation() {
    let mut error_buckets: std::collections::HashMap<String, Vec<ContextualKafkaError>> =
        std::collections::HashMap::new();

    // Create errors for different topics
    let topics = vec!["user-events", "order-events", "payment-events"];
    let operations = vec!["send", "poll", "commit"];

    for topic in &topics {
        for operation in &operations {
            let context =
                ErrorContext::new(operation, topic).with_metadata("batch_id", "batch-123");

            let error = ContextualKafkaError {
                inner: KafkaClientError::Timeout,
                context,
            };

            let key = format!("{}:{}", topic, operation);
            error_buckets.entry(key).or_default().push(error);
        }
    }

    // Verify aggregation
    assert_eq!(error_buckets.len(), topics.len() * operations.len());

    for (key, errors) in &error_buckets {
        assert_eq!(errors.len(), 1);
        let error = &errors[0];
        assert!(key.contains(&error.context.topic));
        assert!(key.contains(&error.context.operation));
        assert_eq!(
            error.context.metadata.get("batch_id"),
            Some(&"batch-123".to_string())
        );
    }

    println!(
        "✅ Aggregated {} error types across {} topics and {} operations",
        error_buckets.len(),
        topics.len(),
        operations.len()
    );
}

/// Test retry mechanism integration with error context
#[test]
fn test_retry_mechanism_with_error_context() {
    let mut retry_history: Vec<ContextualKafkaError> = Vec::new();
    let max_retries = 3;

    for attempt in 1..=max_retries {
        let context = ErrorContext::new("send_with_retry", "critical-alerts")
            .with_metadata("retry_attempt", attempt.to_string())
            .with_metadata("max_retries", max_retries.to_string())
            .with_metadata("backoff_ms", (100 * 2_u32.pow(attempt - 1)).to_string())
            .with_metadata("original_error", "broker_timeout")
            .with_metadata("strategy", "exponential_backoff");

        let retry_error = ContextualKafkaError {
            inner: KafkaClientError::Timeout,
            context,
        };

        retry_history.push(retry_error);
    }

    // Verify retry progression tracking
    assert_eq!(retry_history.len(), max_retries as usize);

    for (i, error) in retry_history.iter().enumerate() {
        let attempt = i + 1;
        assert_eq!(
            error.context.metadata.get("retry_attempt"),
            Some(&attempt.to_string())
        );

        let expected_backoff = 100 * 2_u32.pow(i as u32);
        assert_eq!(
            error.context.metadata.get("backoff_ms"),
            Some(&expected_backoff.to_string())
        );

        // Verify timestamps show progression
        if i > 0 {
            assert!(error.context.timestamp >= retry_history[i - 1].context.timestamp);
        }
    }

    println!("✅ Retry mechanism error context tracking validated");
}
