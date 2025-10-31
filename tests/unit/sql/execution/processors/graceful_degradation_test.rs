//! Tests for graceful degradation in Stream-Table JOIN operations
//!
//! Verifies that the graceful degradation framework handles missing table data
//! correctly with all configured strategies.

use std::collections::HashMap;
use std::sync::Arc;

use velostream::velostream::server::graceful_degradation::{
    GracefulDegradationConfig, GracefulDegradationHandler, TableMissingDataStrategy,
};
use velostream::velostream::sql::SqlError;
use velostream::velostream::sql::ast::{BinaryOperator, Expr, JoinClause, JoinType, StreamSource};
use velostream::velostream::sql::execution::processors::ProcessorContext;
use velostream::velostream::sql::execution::processors::stream_table_join::StreamTableJoinProcessor;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::table::UnifiedTable;
use velostream::velostream::table::streaming::{
    RecordBatch, RecordStream, SimpleStreamRecord, StreamResult,
};

/// Mock table that simulates missing data scenarios
struct MockEmptyTable {
    name: String,
}

impl MockEmptyTable {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl UnifiedTable for MockEmptyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_record(&self, _key: &str) -> Result<Option<HashMap<String, FieldValue>>, SqlError> {
        // Always return empty to simulate missing data
        Ok(None)
    }

    fn contains_key(&self, _key: &str) -> bool {
        false // Always empty
    }

    fn iter_records(&self) -> Box<dyn Iterator<Item = (String, HashMap<String, FieldValue>)>> {
        Box::new(std::iter::empty())
    }

    fn record_count(&self) -> usize {
        0
    }

    fn sql_column_values(
        &self,
        _column: &str,
        _where_clause: &str,
    ) -> Result<Vec<FieldValue>, SqlError> {
        Ok(vec![]) // Always empty
    }

    fn sql_scalar(&self, _aggregate: &str, _where_clause: &str) -> Result<FieldValue, SqlError> {
        Ok(FieldValue::Integer(0)) // Default scalar value
    }

    async fn stream_all(&self) -> StreamResult<RecordStream> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(tx); // Close immediately - no records
        Ok(RecordStream { receiver: rx })
    }

    async fn stream_filter(&self, _where_clause: &str) -> StreamResult<RecordStream> {
        self.stream_all().await
    }

    async fn query_batch(
        &self,
        _batch_size: usize,
        _offset: Option<usize>,
    ) -> StreamResult<RecordBatch> {
        Ok(RecordBatch {
            records: vec![],
            has_more: false,
        })
    }

    async fn stream_count(&self, _where_clause: Option<&str>) -> StreamResult<usize> {
        Ok(0)
    }

    async fn stream_aggregate(
        &self,
        _aggregate_expr: &str,
        _where_clause: Option<&str>,
    ) -> StreamResult<FieldValue> {
        Ok(FieldValue::Integer(0))
    }
}

/// Create a test stream record
fn create_test_stream_record(user_id: i64, amount: f64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("user_id".to_string(), FieldValue::Integer(user_id));
    fields.insert("amount".to_string(), FieldValue::Float(amount));
    fields.insert(
        "timestamp".to_string(),
        FieldValue::String("2025-09-27T10:00:00Z".to_string()),
    );

    StreamRecord {
        fields,
        timestamp: 1695808800000, // 2025-09-27T10:00:00Z
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: Some(
            chrono::DateTime::parse_from_rfc3339("2025-09-27T10:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        ),
    }
}

/// Create a test JOIN clause
fn create_test_join_clause() -> JoinClause {
    JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("id".to_string())),
        },
        right_alias: Some("u".to_string()),
        window: None,
    }
}

/// Create processor context with empty table
fn create_test_context() -> ProcessorContext {
    let mut context = ProcessorContext::new("test_query");
    let empty_table = Arc::new(MockEmptyTable::new("user_profiles"));
    context
        .state_tables
        .insert("user_profiles".to_string(), empty_table);
    context
}

#[tokio::test]
async fn test_fail_fast_strategy() {
    // Test that FailFast strategy properly fails when table data is missing
    let processor = StreamTableJoinProcessor::new(); // Default is fail-fast
    let stream_record = create_test_stream_record(123, 1000.0);
    let join_clause = create_test_join_clause();
    let mut context = create_test_context();

    let result = processor.process_stream_table_join(&stream_record, &join_clause, &mut context);

    assert!(result.is_err());
    match result {
        Err(SqlError::ExecutionError { message, .. }) => {
            assert!(message.contains("user_profiles"));
            assert!(message.contains("data not available"));
        }
        _ => panic!("Expected ExecutionError for FailFast strategy"),
    }
}

#[tokio::test]
async fn test_skip_record_strategy() {
    // Test that SkipRecord strategy returns empty results
    let processor = StreamTableJoinProcessor::skip_missing();
    let stream_record = create_test_stream_record(123, 1000.0);
    let join_clause = create_test_join_clause();
    let mut context = create_test_context();

    let result = processor.process_stream_table_join(&stream_record, &join_clause, &mut context);

    assert!(result.is_ok());
    let joined_records = result.unwrap();
    assert!(
        joined_records.is_empty(),
        "SkipRecord should return empty results"
    );
}

#[tokio::test]
async fn test_emit_with_nulls_strategy() {
    // Test that EmitWithNulls strategy creates records with NULL values
    let processor = StreamTableJoinProcessor::with_nulls();
    let stream_record = create_test_stream_record(123, 1000.0);
    let join_clause = create_test_join_clause();
    let mut context = create_test_context();

    let result = processor.process_stream_table_join(&stream_record, &join_clause, &mut context);

    assert!(result.is_ok());
    let joined_records = result.unwrap();
    assert_eq!(
        joined_records.len(),
        1,
        "EmitWithNulls should return one record"
    );

    let record = &joined_records[0];
    // Should contain original stream fields
    assert_eq!(
        record.fields.get("user_id"),
        Some(&FieldValue::Integer(123))
    );
    assert_eq!(
        record.fields.get("amount"),
        Some(&FieldValue::Float(1000.0))
    );

    // Should contain NULL values for missing table fields
    assert_eq!(record.fields.get("u.id"), Some(&FieldValue::Null));
    assert_eq!(record.fields.get("u.name"), Some(&FieldValue::Null));
    assert_eq!(record.fields.get("u.value"), Some(&FieldValue::Null));
}

#[tokio::test]
async fn test_use_defaults_strategy() {
    // Test that UseDefaults strategy uses provided default values
    let mut defaults = HashMap::new();
    defaults.insert("id".to_string(), FieldValue::Integer(999));
    defaults.insert(
        "name".to_string(),
        FieldValue::String("default_user".to_string()),
    );
    defaults.insert("tier".to_string(), FieldValue::String("bronze".to_string()));

    let processor = StreamTableJoinProcessor::with_defaults(defaults.clone());
    let stream_record = create_test_stream_record(123, 1000.0);
    let join_clause = create_test_join_clause();
    let mut context = create_test_context();

    let result = processor.process_stream_table_join(&stream_record, &join_clause, &mut context);

    assert!(result.is_ok());
    let joined_records = result.unwrap();
    assert_eq!(
        joined_records.len(),
        1,
        "UseDefaults should return one record"
    );

    let record = &joined_records[0];
    // Should contain original stream fields
    assert_eq!(
        record.fields.get("user_id"),
        Some(&FieldValue::Integer(123))
    );
    assert_eq!(
        record.fields.get("amount"),
        Some(&FieldValue::Float(1000.0))
    );

    // Should contain default values for missing table fields
    assert_eq!(record.fields.get("u.id"), Some(&FieldValue::Integer(999)));
    assert_eq!(
        record.fields.get("u.name"),
        Some(&FieldValue::String("default_user".to_string()))
    );
    assert_eq!(
        record.fields.get("u.tier"),
        Some(&FieldValue::String("bronze".to_string()))
    );
}

#[tokio::test]
async fn test_wait_and_retry_strategy_fallback() {
    // Test that WaitAndRetry strategy falls back to error in sync mode
    let config = GracefulDegradationConfig {
        primary_strategy: TableMissingDataStrategy::WaitAndRetry {
            max_retries: 3,
            initial_delay: std::time::Duration::from_millis(100),
            max_delay: std::time::Duration::from_secs(1),
            backoff_multiplier: 2.0,
        },
        enable_logging: true,
        enable_metrics: false,
        table_specific_strategies: HashMap::new(),
    };

    let processor = StreamTableJoinProcessor::with_degradation_config(config);
    let stream_record = create_test_stream_record(123, 1000.0);
    let join_clause = create_test_join_clause();
    let mut context = create_test_context();

    let result = processor.process_stream_table_join(&stream_record, &join_clause, &mut context);

    assert!(result.is_err());
    match result {
        Err(SqlError::ExecutionError { message, .. }) => {
            assert!(message.contains("retry not supported in sync mode"));
        }
        _ => panic!("Expected ExecutionError for WaitAndRetry in sync mode"),
    }
}

#[tokio::test]
async fn test_batch_processing_with_graceful_degradation() {
    // Test that batch processing also applies graceful degradation
    let processor = StreamTableJoinProcessor::with_nulls();
    let stream_records = vec![
        create_test_stream_record(123, 1000.0),
        create_test_stream_record(456, 2000.0),
        create_test_stream_record(789, 3000.0),
    ];
    let join_clause = create_test_join_clause();
    let mut context = create_test_context();

    let result =
        processor.process_batch_stream_table_join(stream_records, &join_clause, &mut context);

    assert!(result.is_ok());
    let joined_records = result.unwrap();
    assert_eq!(
        joined_records.len(),
        3,
        "Should process all 3 records with graceful degradation"
    );

    // Each record should have stream fields + NULL values for missing table fields
    for (i, record) in joined_records.iter().enumerate() {
        let expected_user_id = match i {
            0 => 123,
            1 => 456,
            2 => 789,
            _ => panic!("Unexpected record index"),
        };

        assert_eq!(
            record.fields.get("user_id"),
            Some(&FieldValue::Integer(expected_user_id))
        );
        assert_eq!(record.fields.get("u.id"), Some(&FieldValue::Null));
        assert_eq!(record.fields.get("u.name"), Some(&FieldValue::Null));
    }
}

#[tokio::test]
async fn test_graceful_degradation_config_update() {
    // Test that degradation configuration can be updated dynamically
    let mut processor = StreamTableJoinProcessor::new(); // Start with fail-fast
    let stream_record = create_test_stream_record(123, 1000.0);
    let join_clause = create_test_join_clause();
    let mut context = create_test_context();

    // First, verify fail-fast behavior
    let result1 = processor.process_stream_table_join(&stream_record, &join_clause, &mut context);
    assert!(
        result1.is_err(),
        "Should fail with default fail-fast strategy"
    );

    // Update to skip records
    let new_config = GracefulDegradationConfig {
        primary_strategy: TableMissingDataStrategy::SkipRecord,
        enable_logging: true,
        enable_metrics: false,
        table_specific_strategies: HashMap::new(),
    };
    processor.update_degradation_config(new_config);

    // Now verify skip behavior
    let result2 = processor.process_stream_table_join(&stream_record, &join_clause, &mut context);
    assert!(result2.is_ok(), "Should succeed with skip strategy");
    let joined_records = result2.unwrap();
    assert!(
        joined_records.is_empty(),
        "Should skip records with missing data"
    );
}

#[tokio::test]
async fn test_left_join_with_graceful_degradation() {
    // Test that LEFT JOINs work correctly with graceful degradation
    let processor = StreamTableJoinProcessor::with_nulls();
    let stream_record = create_test_stream_record(123, 1000.0);

    let mut join_clause = create_test_join_clause();
    join_clause.join_type = JoinType::Left; // Change to LEFT JOIN

    let mut context = create_test_context();

    let result = processor.process_stream_table_join(&stream_record, &join_clause, &mut context);

    assert!(result.is_ok());
    let joined_records = result.unwrap();
    assert_eq!(
        joined_records.len(),
        1,
        "LEFT JOIN should always emit a record"
    );

    let record = &joined_records[0];
    // Should preserve stream record data
    assert_eq!(
        record.fields.get("user_id"),
        Some(&FieldValue::Integer(123))
    );
    assert_eq!(
        record.fields.get("amount"),
        Some(&FieldValue::Float(1000.0))
    );
}

#[tokio::test]
async fn test_multiple_strategies_performance() {
    // Performance test to ensure graceful degradation doesn't significantly impact throughput
    let processors = vec![
        StreamTableJoinProcessor::new(),          // FailFast
        StreamTableJoinProcessor::skip_missing(), // SkipRecord
        StreamTableJoinProcessor::with_nulls(),   // EmitWithNulls
    ];

    let stream_record = create_test_stream_record(123, 1000.0);
    let join_clause = create_test_join_clause();

    for (i, processor) in processors.iter().enumerate() {
        let mut context = create_test_context();
        let start = std::time::Instant::now();

        // Process multiple times to measure performance
        for _ in 0..100 {
            let _ = processor.process_stream_table_join(&stream_record, &join_clause, &mut context);
        }

        let duration = start.elapsed();
        println!("Strategy {} took {:?} for 100 operations", i, duration);

        // Should complete within reasonable time (adjust threshold as needed)
        assert!(
            duration < std::time::Duration::from_millis(100),
            "Graceful degradation should be fast"
        );
    }
}
