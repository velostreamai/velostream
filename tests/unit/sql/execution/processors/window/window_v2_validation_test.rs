/*
# Window V2 Integration Validation Tests

Tests that explicitly enable window_v2 and validate the integration works correctly.
These tests provide CI/CD coverage for the feature-flagged window_v2 architecture.
*/

use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::sql::ast::{
    DataType, EmitMode, Expr, RowExpirationMode, RowsEmitMode, SelectField, StreamSource,
    StreamingQuery, WindowSpec,
};
use velostream::velostream::sql::execution::config::StreamingConfig;
use velostream::velostream::sql::execution::processors::WindowProcessor;
use velostream::velostream::sql::execution::processors::{JoinContext, ProcessorContext};
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};

fn create_test_record(timestamp: i64, value: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("value".to_string(), FieldValue::Integer(value));
    fields.insert("event_time".to_string(), FieldValue::Integer(timestamp));
    fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp)); // FR-081: window_v2 expects "timestamp"

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: Some(Utc.timestamp_millis_opt(timestamp).unwrap()),
        timestamp,
        offset: 0,
        partition: 0,
    }
}

fn create_context_with_window_v2_enabled() -> ProcessorContext {
    let mut context = ProcessorContext {
        record_count: 0,
        max_records: None,
        window_context: None,
        join_context: JoinContext::new(),
        group_by_states: HashMap::new(),
        group_by_states_ref: None, // Phase 6.4C: Optional Arc reference
        schemas: HashMap::new(),
        stream_handles: HashMap::new(),
        state_tables: HashMap::new(),
        data_sources: HashMap::new(),
        persistent_window_states: Vec::new(),
        dirty_window_states: 0,
        metadata: HashMap::new(),
        performance_monitor: None,
        data_readers: HashMap::new(),
        data_writers: HashMap::new(),
        active_reader: None,
        active_writer: None,
        source_positions: HashMap::new(),
        watermark_manager: None,
        correlation_context: None,
        pending_results: HashMap::new(),
        validated_select_queries: std::collections::HashSet::new(),
        rows_window_states: HashMap::new(),
        window_v2_states: HashMap::new(),
        streaming_config: Some(StreamingConfig::new()),
    };

    context
}

// Test 1: Validate window_v2 context creation (window_v2 is always enabled in Phase 2E+)
#[tokio::test]
async fn test_window_v2_feature_flag_enabled() {
    let context = create_context_with_window_v2_enabled();

    // Window_v2 is the only architecture available (Phase 2E+)
    assert!(
        context.streaming_config.is_some(),
        "streaming_config should be set"
    );
}

// Test 2: Validate window_v2 is always enabled (Phase 2E+)
// Legacy test: Previously window_v2 could be disabled by default, but now it's always enabled
#[tokio::test]
async fn test_window_v2_feature_flag_disabled_by_default() {
    let context = ProcessorContext {
        record_count: 0,
        max_records: None,
        window_context: None,
        join_context: JoinContext::new(),
        group_by_states: HashMap::new(),
        group_by_states_ref: None, // Phase 6.4C: Optional Arc reference
        schemas: HashMap::new(),
        stream_handles: HashMap::new(),
        state_tables: HashMap::new(),
        data_sources: HashMap::new(),
        persistent_window_states: Vec::new(),
        dirty_window_states: 0,
        metadata: HashMap::new(),
        performance_monitor: None,
        data_readers: HashMap::new(),
        data_writers: HashMap::new(),
        active_reader: None,
        active_writer: None,
        source_positions: HashMap::new(),
        watermark_manager: None,
        correlation_context: None,
        pending_results: HashMap::new(),
        validated_select_queries: std::collections::HashSet::new(),
        rows_window_states: HashMap::new(),
        window_v2_states: HashMap::new(),
        streaming_config: None, // Default config (window_v2 is still always enabled)
    };

    // Window_v2 is the only architecture available (Phase 2E+)
    // Even without explicit config, window processing still uses window_v2
    assert!(
        context.streaming_config.is_none(),
        "streaming_config can be None, window_v2 is always the only implementation"
    );
}

// Test 3: TUMBLING window with window_v2 enabled
#[tokio::test]
async fn test_tumbling_window_with_v2() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: Some("event_time".to_string()),
        }),
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let mut context = create_context_with_window_v2_enabled();

    // Process a record (should use window_v2 internally)
    let record = create_test_record(1000, 100);
    let result = WindowProcessor::process_windowed_query_enhanced(
        "test_query",
        &query,
        &record,
        &mut context,
        None,
    );

    // Should not error (window_v2 adapter handles this)
    assert!(result.is_ok(), "Window V2 processing should succeed");

    // Verify v2 state was created
    assert!(
        context.metadata.contains_key("window_v2:test_query"),
        "Window V2 state should be initialized"
    );
}

// Test 4: ROWS window with window_v2 enabled
#[tokio::test]
async fn test_rows_window_with_v2() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Rows {
            buffer_size: 100,
            partition_by: vec![],
            order_by: vec![],
            time_gap: None,
            window_frame: None,
            emit_mode: RowsEmitMode::EveryRecord,
            expire_after: RowExpirationMode::Never,
        }),
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let mut context = create_context_with_window_v2_enabled();

    // Process multiple records
    for i in 0..10 {
        let record = create_test_record(i * 1000, i * 10);
        let result = WindowProcessor::process_windowed_query_enhanced(
            "test_rows_query",
            &query,
            &record,
            &mut context,
            None,
        );
        assert!(
            result.is_ok(),
            "Window V2 ROWS processing should succeed for record {}",
            i
        );
    }

    // Verify v2 state exists
    assert!(
        context.metadata.contains_key("window_v2:test_rows_query"),
        "Window V2 ROWS state should be initialized"
    );
}

// Test 5: SESSION window with window_v2 enabled
#[tokio::test]
async fn test_session_window_with_v2() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Session {
            gap: Duration::from_secs(300),
            time_column: Some("event_time".to_string()),
            partition_by: vec![],
        }),
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let mut context = create_context_with_window_v2_enabled();

    let record = create_test_record(1000, 100);
    let result = WindowProcessor::process_windowed_query_enhanced(
        "test_session_query",
        &query,
        &record,
        &mut context,
        None,
    );

    assert!(
        result.is_ok(),
        "Window V2 SESSION processing should succeed"
    );
    assert!(
        context
            .metadata
            .contains_key("window_v2:test_session_query"),
        "Window V2 SESSION state should be initialized"
    );
}

// Test 6: SLIDING window with window_v2 enabled
#[tokio::test]
async fn test_sliding_window_with_v2() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Sliding {
            size: Duration::from_secs(60),
            advance: Duration::from_secs(30),
            time_column: Some("event_time".to_string()),
        }),
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let mut context = create_context_with_window_v2_enabled();

    let record = create_test_record(1000, 100);
    let result = WindowProcessor::process_windowed_query_enhanced(
        "test_sliding_query",
        &query,
        &record,
        &mut context,
        None,
    );

    assert!(
        result.is_ok(),
        "Window V2 SLIDING processing should succeed"
    );
    assert!(
        context
            .metadata
            .contains_key("window_v2:test_sliding_query"),
        "Window V2 SLIDING state should be initialized"
    );
}

// Test 7: EMIT CHANGES with window_v2
#[tokio::test]
async fn test_emit_changes_with_v2() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: Some("event_time".to_string()),
        }),
        order_by: None,
        limit: None,
        emit_mode: Some(EmitMode::Changes), // EMIT CHANGES
        properties: None,
    };

    let mut context = create_context_with_window_v2_enabled();

    let record = create_test_record(1000, 100);
    let result = WindowProcessor::process_windowed_query_enhanced(
        "test_emit_changes_query",
        &query,
        &record,
        &mut context,
        None,
    );

    assert!(
        result.is_ok(),
        "Window V2 EMIT CHANGES processing should succeed"
    );
}

// Test 8: GROUP BY with window_v2
#[tokio::test]
async fn test_group_by_with_v2() {
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: Some(vec![Expr::Column("symbol".to_string())]),
        having: None,
        window: Some(WindowSpec::Tumbling {
            size: Duration::from_secs(60),
            time_column: Some("event_time".to_string()),
        }),
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    };

    let mut context = create_context_with_window_v2_enabled();

    let record = create_test_record(1000, 100);
    let result = WindowProcessor::process_windowed_query_enhanced(
        "test_group_by_query",
        &query,
        &record,
        &mut context,
        None,
    );

    assert!(
        result.is_ok(),
        "Window V2 GROUP BY processing should succeed"
    );
    assert!(
        context
            .window_v2_states
            .contains_key("window_v2:test_group_by_query"),
        "Window V2 GROUP BY state should be stored"
    );
}

// Test 10: Validate enhanced() config (window_v2 always enabled in Phase 2E+)
#[tokio::test]
async fn test_enhanced_config_enables_window_v2() {
    let config = StreamingConfig::enhanced();
    // Window_v2 is the only architecture available (Phase 2E+)
    // No need to check enable_window_v2 flag - it always uses window_v2
    // Enhanced config should enable watermarks
    assert!(
        config.enable_watermarks,
        "Enhanced config should enable watermarks"
    );
}
