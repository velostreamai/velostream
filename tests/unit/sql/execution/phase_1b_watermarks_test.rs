/*!
# Phase 1B: Time Semantics & Watermarks - Comprehensive Tests

This test suite verifies the Phase 1B implementation of watermark-aware processing,
including late data handling and event-time semantics.

## Test Coverage

- WatermarkManager functionality and strategies
- ProcessorContext watermark integration
- WindowProcessor with watermark-aware emission
- Late data handling with configurable strategies
- Event-time vs processing-time semantics
- Backward compatibility with existing code

## Key Features Tested

1. **Watermark Generation**: BoundedOutOfOrderness, Ascending, Punctuated strategies
2. **Late Data Detection**: Lateness calculation and threshold checking
3. **Window Emission**: Watermark-based emission timing for tumbling/sliding windows
4. **Late Data Strategies**: Drop, DeadLetter, IncludeInNextWindow, UpdatePrevious
5. **Multi-Source Watermarks**: Global watermark calculation from multiple sources
*/

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use velostream::velostream::sql::execution::config::{
    LateDataStrategy as ConfigLateDataStrategy, StreamingConfig,
};
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::processors::window::WindowProcessor;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::execution::watermarks::{
    LateDataAction, LateDataStrategy, WatermarkManager, WatermarkStrategy,
};
use velostream::velostream::sql::{SqlError, StreamingQuery, ast::WindowSpec};

/// Helper function to create a test StreamRecord with event time
fn create_test_record_with_event_time(
    id: i64,
    event_time: DateTime<Utc>,
    processing_time: Option<i64>,
) -> StreamRecord {
    let timestamp_ms = processing_time.unwrap_or_else(|| Utc::now().timestamp_millis());
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("value".to_string(), FieldValue::Float(100.0 + id as f64));
    fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp_ms)); // FR-081: window_v2 expects "timestamp"
    fields.insert(
        "event_time".to_string(),
        FieldValue::Integer(event_time.timestamp_millis()),
    ); // Include event_time too

    StreamRecord {
        fields,
        headers: HashMap::new(),
        timestamp: timestamp_ms,
        offset: id,
        partition: 0,
        event_time: Some(event_time),
    }
}

/// Helper function to create a test StreamRecord without event time (legacy)
fn create_test_record_legacy(id: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("value".to_string(), FieldValue::Float(100.0 + id as f64));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        timestamp: Utc::now().timestamp_millis(),
        offset: id,
        partition: 0,
        event_time: None, // Legacy record without event time
    }
}

#[tokio::test]
async fn test_watermark_manager_basic_functionality() {
    let mut manager = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(5),
        watermark_interval: Duration::from_secs(1),
    });

    manager.enable();
    assert!(manager.is_enabled());

    // Test watermark generation
    let event_time = Utc::now() - ChronoDuration::seconds(10);
    let record = create_test_record_with_event_time(1, event_time, None);

    let watermark_event = manager.update_watermark("test_source", &record);
    assert!(watermark_event.is_some());

    let watermark_event = watermark_event.unwrap();
    assert_eq!(watermark_event.source_id, "test_source");
    assert!(watermark_event.watermark < event_time); // Should be earlier due to out-of-orderness

    // Test global watermark
    let global_watermark = manager.get_global_watermark();
    assert!(global_watermark.is_some());
}

#[tokio::test]
async fn test_multi_source_watermark_coordination() {
    let mut manager = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(5),
        watermark_interval: Duration::from_secs(1),
    });

    manager.enable();

    let base_time = Utc::now() - ChronoDuration::seconds(30);

    // Source 1: Fast source (recent events)
    let fast_record =
        create_test_record_with_event_time(1, base_time + ChronoDuration::seconds(10), None);
    manager.update_watermark("fast_source", &fast_record);

    // Source 2: Slow source (older events)
    let slow_record =
        create_test_record_with_event_time(2, base_time + ChronoDuration::seconds(5), None);
    manager.update_watermark("slow_source", &slow_record);

    // Global watermark should be based on the slowest source
    let global_watermark = manager.get_global_watermark().unwrap();
    let slow_source_watermark = manager.get_source_watermark("slow_source").unwrap();

    // Global should be influenced by the slower source
    assert!(global_watermark <= slow_source_watermark + ChronoDuration::seconds(1));

    // Verify all sources are tracked
    let tracked_sources = manager.get_tracked_sources();
    assert_eq!(tracked_sources.len(), 2);
    assert!(tracked_sources.contains(&"fast_source".to_string()));
    assert!(tracked_sources.contains(&"slow_source".to_string()));
}

#[tokio::test]
async fn test_late_data_detection() {
    let mut manager = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(5),
        watermark_interval: Duration::from_secs(1),
    });

    manager.enable();

    // Establish watermark with a recent event
    let recent_time = Utc::now() - ChronoDuration::seconds(10);
    let recent_record = create_test_record_with_event_time(1, recent_time, None);
    manager.update_watermark("source1", &recent_record);

    // Create a late record
    let late_time = Utc::now() - ChronoDuration::seconds(30);
    let late_record = create_test_record_with_event_time(2, late_time, None);

    // Test late data detection
    assert!(manager.is_late(&late_record));

    let lateness = manager.calculate_lateness(&late_record);
    assert!(lateness.is_some());
    assert!(lateness.unwrap() > Duration::from_secs(10));

    // Test allowed lateness
    let manager_with_lateness = WatermarkManager::with_allowed_lateness(
        WatermarkStrategy::Ascending,
        Duration::from_secs(60), // Allow 1 minute lateness
    );

    let mut enabled_manager = manager_with_lateness;
    enabled_manager.enable();
    enabled_manager.update_watermark("source1", &recent_record);

    // This should be within allowed lateness
    assert!(enabled_manager.is_within_allowed_lateness(&late_record));
}

#[tokio::test]
async fn test_late_data_strategy_actions() {
    let mut manager = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(5),
        watermark_interval: Duration::from_secs(1),
    });

    manager.enable();

    // Establish watermark
    let recent_time = Utc::now() - ChronoDuration::seconds(10);
    let recent_record = create_test_record_with_event_time(1, recent_time, None);
    manager.update_watermark("source1", &recent_record);

    // Create late record
    let late_time = Utc::now() - ChronoDuration::seconds(30);
    let late_record = create_test_record_with_event_time(2, late_time, None);

    // Test Drop strategy
    let drop_strategy = LateDataStrategy::Drop;
    let action = manager.determine_late_data_action(&late_record, &drop_strategy);
    assert!(matches!(action, LateDataAction::Drop));

    // Test DeadLetterQueue strategy
    let dlq_strategy = LateDataStrategy::DeadLetterQueue {
        queue_name: "late_data_queue".to_string(),
    };
    let action = manager.determine_late_data_action(&late_record, &dlq_strategy);
    assert!(matches!(action, LateDataAction::DeadLetter));

    // Test IncludeInNextWindow strategy
    let include_strategy = LateDataStrategy::IncludeInNextWindow;
    let action = manager.determine_late_data_action(&late_record, &include_strategy);
    assert!(matches!(action, LateDataAction::Process));

    // Test UpdatePreviousWindow strategy
    let update_strategy = LateDataStrategy::UpdatePreviousWindow {
        grace_period: Duration::from_secs(60), // Allow updates within 1 minute
    };
    let action = manager.determine_late_data_action(&late_record, &update_strategy);
    assert!(matches!(action, LateDataAction::UpdatePrevious { .. }));
}

#[tokio::test]
async fn test_processor_context_watermark_integration() {
    let mut context = ProcessorContext::new("test_query");

    // Initially no watermarks enabled
    assert!(!context.has_watermarks_enabled());
    assert!(context.get_global_watermark().is_none());

    // Enable watermarks
    let manager = Arc::new({
        let mut mgr = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_secs(5),
            watermark_interval: Duration::from_secs(1),
        });
        mgr.enable();
        mgr
    });

    context.enable_watermarks(manager);
    assert!(context.has_watermarks_enabled());

    // Test watermark update through context
    let event_time = Utc::now() - ChronoDuration::seconds(10);
    let record = create_test_record_with_event_time(1, event_time, None);

    let watermark_event = context.update_watermark("test_source", &record);
    assert!(watermark_event.is_some());

    // Test late data detection through context
    let late_time = Utc::now() - ChronoDuration::seconds(30);
    let late_record = create_test_record_with_event_time(2, late_time, None);

    assert!(context.is_late_record(&late_record));

    let lateness = context.calculate_record_lateness(&late_record);
    assert!(lateness.is_some());
}

#[tokio::test]
async fn test_window_processor_watermark_aware_processing() {
    let mut context = ProcessorContext::new("test_windowed_query");

    // FR-081: Enable window_v2 for watermark-aware processing
    context.streaming_config = Some(StreamingConfig::new().with_window_v2());

    // Enable watermarks
    let manager = Arc::new({
        let mut mgr = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_secs(5),
            watermark_interval: Duration::from_secs(1),
        });
        mgr.enable();
        mgr
    });

    context.enable_watermarks(manager);

    // Create a tumbling window query
    let window = WindowSpec::Tumbling {
        size: Duration::from_secs(10),
        time_column: None, // Use event_time field
    };

    let query = StreamingQuery::Select {
        fields: vec![], // Simplified for test
        from_alias: None,
        from: velostream::velostream::sql::ast::StreamSource::Table("test_stream".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        window: Some(window),
        emit_mode: None,
        properties: None,
    };

    // Process records with watermark-aware processing
    let base_time = Utc::now() - ChronoDuration::seconds(30);

    // Record 1: Normal record
    let record1 =
        create_test_record_with_event_time(1, base_time + ChronoDuration::seconds(5), None);
    let result1 = WindowProcessor::process_windowed_query_enhanced(
        "test_query",
        &query,
        &record1,
        &mut context,
        Some("source1"),
    );
    assert!(result1.is_ok(), "Result1 error: {:?}", result1.err());

    // Record 2: Another normal record
    let record2 =
        create_test_record_with_event_time(2, base_time + ChronoDuration::seconds(8), None);
    let result2 = WindowProcessor::process_windowed_query_enhanced(
        "test_query",
        &query,
        &record2,
        &mut context,
        Some("source1"),
    );
    assert!(result2.is_ok());

    // Record 3: Late record (should be handled by late data strategy)
    let late_record =
        create_test_record_with_event_time(3, base_time - ChronoDuration::seconds(10), None);
    let result3 = WindowProcessor::process_windowed_query_enhanced(
        "test_query",
        &query,
        &late_record,
        &mut context,
        Some("source1"),
    );
    assert!(result3.is_ok());
    // Late record should be handled without error (dropped or processed based on strategy)
}

#[tokio::test]
async fn test_streaming_config_integration() {
    // Test different configuration presets
    let default_config = StreamingConfig::default();
    assert!(!default_config.enable_watermarks);

    let conservative_config = StreamingConfig::conservative();
    assert!(!conservative_config.enable_watermarks);

    let enhanced_config = StreamingConfig::enhanced();
    assert!(enhanced_config.enable_watermarks);

    // Test fluent configuration
    let custom_config = StreamingConfig::default()
        .with_watermarks()
        .with_late_data_strategy(ConfigLateDataStrategy::DeadLetterQueue);

    assert!(custom_config.enable_watermarks);
    assert!(matches!(
        custom_config.late_data_strategy,
        ConfigLateDataStrategy::DeadLetterQueue
    ));
}

#[tokio::test]
async fn test_event_time_vs_processing_time_semantics() {
    let mut manager = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
        max_out_of_orderness: Duration::from_secs(5),
        watermark_interval: Duration::from_secs(1),
    });

    manager.enable();

    let now = Utc::now();
    let event_time = now - ChronoDuration::seconds(20); // Event happened 20 seconds ago
    let processing_time = now.timestamp_millis(); // Processed now

    // Create record with both event_time and processing_time
    let record = create_test_record_with_event_time(1, event_time, Some(processing_time));

    // Watermark should be based on event_time, not processing_time
    let watermark_event = manager.update_watermark("source1", &record);
    assert!(watermark_event.is_some());

    let watermark = watermark_event.unwrap().watermark;

    // Watermark should be based on event_time (with out-of-orderness subtracted)
    // It should be earlier than event_time due to bounded out-of-orderness
    assert!(watermark < event_time);
    assert!(watermark > event_time - ChronoDuration::seconds(10)); // But not too early

    // Verify the record's event_time is properly used
    assert_eq!(record.event_time.unwrap(), event_time);
    assert_eq!(record.timestamp, processing_time);
}

#[tokio::test]
async fn test_window_emission_timing_with_watermarks() {
    let mut context = ProcessorContext::new("timing_test");

    // Enable watermarks with short out-of-orderness for faster testing
    let manager = Arc::new({
        let mut mgr = WatermarkManager::new(WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_millis(100),
            watermark_interval: Duration::from_millis(50),
        });
        mgr.enable();
        mgr
    });

    context.enable_watermarks(manager);

    // Create window spec - 5 second tumbling window
    let window = WindowSpec::Tumbling {
        size: Duration::from_secs(5),
        time_column: None,
    };

    let base_time = Utc::now() - ChronoDuration::seconds(10);

    // Test window readiness based on watermarks
    let window_start = base_time.timestamp_millis();

    // Initially, window should not be ready (no watermark)
    assert!(!WindowProcessor::is_window_ready_for_emission(
        &window,
        window_start,
        &context
    ));

    // Add records to advance watermark
    let record1 =
        create_test_record_with_event_time(1, base_time + ChronoDuration::seconds(6), None);
    context.update_watermark("source1", &record1);

    // Now window should be ready (watermark passed window end)
    assert!(WindowProcessor::is_window_ready_for_emission(
        &window,
        window_start,
        &context
    ));
}
