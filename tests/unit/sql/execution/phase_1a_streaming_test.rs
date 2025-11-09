//! Comprehensive tests for FR-058 Phase 1A streaming engine enhancements
//!
//! Tests the core foundation features:
//! - Message injection via get_message_sender()
//! - Correlation IDs in ExecutionMessage
//! - Feature flag system (StreamingConfig)
//! - Event time support in StreamRecord
//! - Backward compatibility guarantees

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{
    types::FieldValue, StreamExecutionEngine, StreamRecord,
};
use velostream::velostream::sql::ast::{SelectField, StreamSource, StreamingQuery};

#[tokio::test]
async fn test_message_injection_capability() {
    // Test that we can inject messages into the engine
    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();
    let engine = StreamExecutionEngine::new(output_sender);

    // Get the message sender - this is the new capability
    let message_sender = engine.get_message_sender();

    // Verify we can send a message (shouldn't panic)
    let correlation_id = "test_correlation_123".to_string();
    let test_message = ExecutionMessage::AdvanceWatermark {
        watermark_timestamp: Utc::now(),
        source_id: "test_source".to_string(),
        correlation_id: correlation_id.clone(),
    };

    // This should succeed without blocking
    assert!(message_sender.send(test_message).is_ok());

    // The engine should be able to handle this message when started
    // (We don't start the engine here to avoid hanging in tests)
    println!("✅ Message injection capability verified");
}

#[tokio::test]
async fn test_correlation_id_support() {
    // Test that ExecutionMessage supports correlation IDs
    let correlation_id = "test_correlation_456".to_string();
    let timestamp = Utc::now();

    // Test various message types with correlation IDs
    let messages = vec![
        ExecutionMessage::StartJob {
            job_id: "job1".to_string(),
            query: create_test_query(),
            correlation_id: correlation_id.clone(),
        },
        ExecutionMessage::AdvanceWatermark {
            watermark_timestamp: timestamp,
            source_id: "source1".to_string(),
            correlation_id: correlation_id.clone(),
        },
        ExecutionMessage::ProcessRecord {
            record: create_test_record(),
            correlation_id: correlation_id.clone(),
        },
        ExecutionMessage::FlushWindows {
            source_id: "source1".to_string(),
            correlation_id: correlation_id.clone(),
        },
    ];

    // Verify all message types can be created with correlation IDs
    for message in messages {
        match &message {
            ExecutionMessage::StartJob {
                correlation_id: id, ..
            } => {
                assert_eq!(id, &correlation_id);
            }
            ExecutionMessage::AdvanceWatermark {
                correlation_id: id, ..
            } => {
                assert_eq!(id, &correlation_id);
            }
            ExecutionMessage::ProcessRecord {
                correlation_id: id, ..
            } => {
                assert_eq!(id, &correlation_id);
            }
            ExecutionMessage::FlushWindows {
                correlation_id: id, ..
            } => {
                assert_eq!(id, &correlation_id);
            }
            _ => panic!("Unexpected message type"),
        }
    }

    println!("✅ Correlation ID support verified");
}

#[tokio::test]
async fn test_streaming_config_feature_flags() {
    // Test the feature flag system for backward compatibility

    // Default configuration should preserve legacy behavior
    let default_config = StreamingConfig::default();
    assert!(!default_config.enable_watermarks);
    assert!(!default_config.enable_enhanced_errors);
    assert!(!default_config.enable_resource_limits);
    assert_eq!(
        default_config.message_passing_mode,
        velostream::velostream::sql::execution::config::MessagePassingMode::Legacy
    );

    // Conservative configuration should be safe to use
    let conservative_config = StreamingConfig::conservative();
    assert!(!conservative_config.enable_watermarks); // Complex feature disabled
    assert!(conservative_config.enable_enhanced_errors); // Safe enhancement
    assert!(conservative_config.enable_resource_limits); // Safety feature
    assert!(conservative_config.max_total_memory.is_some());

    // Enhanced configuration should enable all features
    let enhanced_config = StreamingConfig::enhanced();
    assert!(enhanced_config.enable_watermarks);
    assert!(enhanced_config.enable_enhanced_errors);
    assert!(enhanced_config.enable_resource_limits);
    assert!(enhanced_config.max_total_memory.is_some());
    assert!(enhanced_config.max_concurrent_operations.is_some());

    // Fluent API should work
    let custom_config = StreamingConfig::new()
        .with_watermarks()
        .with_enhanced_errors()
        .with_resource_limits(Some(1024 * 1024 * 1024)); // 1GB

    assert!(custom_config.enable_watermarks);
    assert!(custom_config.enable_enhanced_errors);
    assert!(custom_config.enable_resource_limits);
    assert_eq!(custom_config.max_total_memory, Some(1024 * 1024 * 1024));

    println!("✅ StreamingConfig feature flags verified");
}

#[tokio::test]
async fn test_event_time_support_in_stream_record() {
    // Test that StreamRecord now supports optional event_time

    // Create record without event time (backward compatibility)
    let record_without_event_time = StreamRecord {
        fields: HashMap::new(),
        timestamp: 12345,
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
    };

    assert!(record_without_event_time.event_time.is_none());

    // Create record with event time (new capability)
    let event_time = Utc::now();
    let record_with_event_time = StreamRecord {
        fields: HashMap::new(),
        timestamp: 12345,
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: Some(event_time),
    };

    assert!(record_with_event_time.event_time.is_some());
    assert_eq!(record_with_event_time.event_time.unwrap(), event_time);

    // Test the new methods for event time handling
    let record_with_event_time =
        StreamRecord::with_event_time(HashMap::new(), 12345, 0, 0, HashMap::new(), event_time);

    assert_eq!(record_with_event_time.event_time.unwrap(), event_time);

    // Test fluent API
    let record = StreamRecord::default().with_event_time_fluent(event_time);

    assert_eq!(record.event_time.unwrap(), event_time);

    println!("✅ Event time support in StreamRecord verified");
}

#[tokio::test]
async fn test_engine_configuration_support() {
    // Test that engine can be created with configuration
    let (output_sender, _output_receiver) = mpsc::unbounded_channel();

    // Default engine (backward compatibility)
    let default_engine = StreamExecutionEngine::new(output_sender.clone());
    // Should not panic and should work exactly as before

    // Engine with configuration
    let config = StreamingConfig::conservative();
    let configured_engine = StreamExecutionEngine::new_with_config(output_sender.clone(), config);

    // Both should support message injection
    assert!(default_engine.get_message_sender().is_unbounded());
    assert!(configured_engine.get_message_sender().is_unbounded());

    // Test fluent configuration API
    let fluent_engine = StreamExecutionEngine::new(output_sender)
        .with_watermark_support()
        .with_enhanced_errors()
        .with_resource_limits(Some(512 * 1024 * 1024)); // 512MB

    assert!(fluent_engine.get_message_sender().is_unbounded());

    println!("✅ Engine configuration support verified");
}

#[tokio::test]
async fn test_backward_compatibility_guarantees() {
    // This test ensures that existing code continues to work exactly as before

    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();

    // Create engine the old way - should work identically
    let mut engine = StreamExecutionEngine::new(output_sender);

    // Process a record the old way
    let query = create_test_query();
    let record = create_legacy_test_record(); // Record without event_time

    // This should work exactly as before
    let result = engine.execute_with_record(&query, &record).await;
    assert!(result.is_ok());

    // Check that we get output
    if let Ok(Some(output_record)) = output_receiver.try_recv() {
        // The output record should have event_time field but it should be None
        // for backward compatibility
        assert!(output_record.event_time.is_none());
        assert!(output_record.fields.contains_key("id"));
    }

    println!("✅ Backward compatibility guaranteed");
}

#[tokio::test]
async fn test_phase_1a_integration() {
    // Integration test combining all Phase 1A features
    let (output_sender, mut output_receiver) = mpsc::unbounded_channel();

    // Create engine with enhanced configuration
    let config = StreamingConfig::new()
        .with_enhanced_errors()
        .with_resource_limits(Some(128 * 1024 * 1024));

    let engine = StreamExecutionEngine::new_with_config(output_sender, config);
    let message_sender = engine.get_message_sender();

    // Create a record with event time
    let event_time = Utc::now();
    let record = StreamRecord::with_event_time(
        {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(1));
            fields.insert("name".to_string(), FieldValue::String("test".to_string()));
            fields
        },
        event_time.timestamp_millis(),
        0,
        0,
        HashMap::new(),
        event_time,
    );

    // Send a message with correlation ID
    let correlation_id = "integration_test_789".to_string();
    let test_message = ExecutionMessage::ProcessRecord {
        record: record.clone(),
        correlation_id: correlation_id.clone(),
    };

    assert!(message_sender.send(test_message).is_ok());

    // Verify record structure
    assert_eq!(record.event_time.unwrap(), event_time);
    assert_eq!(record.fields.get("id"), Some(&FieldValue::Integer(1)));
    assert_eq!(
        record.fields.get("name"),
        Some(&FieldValue::String("test".to_string()))
    );

    println!("✅ Phase 1A integration test passed");
}

// Helper functions for creating test data

fn create_test_query() -> StreamingQuery {
    StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Table("test_table".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    }
}

fn create_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("name".to_string(), FieldValue::String("test".to_string()));

    StreamRecord {
        fields,
        timestamp: Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: Some(Utc::now()),
    }
}

fn create_legacy_test_record() -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    fields.insert("name".to_string(), FieldValue::String("test".to_string()));

    StreamRecord {
        fields,
        timestamp: Utc::now().timestamp_millis(),
        offset: 0,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_message_injection_performance() {
        // Performance test to ensure message injection doesn't add significant overhead
        let (output_sender, _output_receiver) = mpsc::unbounded_channel();
        let engine = StreamExecutionEngine::new(output_sender);
        let message_sender = engine.get_message_sender();

        let start = Instant::now();
        let num_messages = 1000;

        for i in 0..num_messages {
            let message = ExecutionMessage::ProcessRecord {
                record: create_test_record(),
                correlation_id: format!("perf_test_{}", i),
            };

            assert!(message_sender.send(message).is_ok());
        }

        let duration = start.elapsed();
        let messages_per_sec = num_messages as f64 / duration.as_secs_f64();

        println!(
            "✅ Message injection performance: {:.0} messages/sec ({:.2}ms total)",
            messages_per_sec,
            duration.as_millis()
        );

        // Should be able to handle at least 10k messages per second
        assert!(
            messages_per_sec > 10_000.0,
            "Performance regression detected"
        );
    }

    #[test]
    fn test_streamrecord_event_time_overhead() {
        // Test that adding event_time field doesn't significantly impact performance
        let start = Instant::now();
        let num_records = 10_000;

        for i in 0..num_records {
            let _record = StreamRecord {
                fields: {
                    let mut fields = HashMap::new();
                    fields.insert("id".to_string(), FieldValue::Integer(i));
                    fields
                },
                timestamp: i as i64,
                offset: i as i64,
                partition: 0,
                headers: HashMap::new(),
                event_time: Some(Utc::now()),
            };
        }

        let duration = start.elapsed();
        let records_per_sec = num_records as f64 / duration.as_secs_f64();

        println!(
            "✅ StreamRecord creation performance: {:.0} records/sec ({:.2}ms total)",
            records_per_sec,
            duration.as_millis()
        );

        // Should be able to create at least 100k records per second
        assert!(
            records_per_sec > 100_000.0,
            "StreamRecord performance regression detected"
        );
    }
}
