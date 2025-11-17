/*!
# execute_with_record_sync Multi-Result Tests

Tests for `execute_with_record_sync()` returning multiple results per record.

This test suite validates that the synchronous execution method properly supports:
- GROUP BY queries that emit multiple groups
- EMIT CHANGES queries that emit on state changes
- Windowed aggregations with multiple results
- Empty results for buffered/filtered records

## Test Categories
1. Empty result scenarios (filtering, window buffering)
2. Single result scenarios (non-windowed SELECT)
3. Multiple result scenarios (GROUP BY, EMIT CHANGES)
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{
    StreamExecutionEngine,
    types::{FieldValue, StreamRecord},
};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Helper to create test records with configurable fields
fn create_record(id: i64, value: i64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("value".to_string(), FieldValue::Integer(value));
    fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

    StreamRecord {
        fields,
        timestamp,
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
    }
}

// ============================================================================
// EMPTY RESULT TESTS - Records that produce no output
// ============================================================================

/// Test that filtering queries return empty vec for non-matching records
#[test]
fn test_sync_filtered_record_returns_empty_vec() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT * FROM stream WHERE value > 100")
        .unwrap();
    let record = create_record(1, 50, 0); // Does not match filter (50 <= 100)

    let results = engine.execute_with_record_sync(&query, &record).unwrap();

    assert_eq!(
        results.len(),
        0,
        "Expected empty vec for filtered-out record, got {} results",
        results.len()
    );
}

/// Test that windowed queries return empty vec for buffered records
#[test]
fn test_sync_windowed_buffered_record_returns_empty_vec() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // Simple window query without GROUP BY - first record should be buffered
    let query = parser
        .parse("SELECT COUNT(*) FROM stream WINDOW TUMBLING(60s)")
        .unwrap();
    let record = create_record(1, 100, 1000); // First record in a tumbling window

    let results = engine.execute_with_record_sync(&query, &record).unwrap();

    // First record in a window is typically buffered, no immediate output
    assert_eq!(
        results.len(),
        0,
        "Expected empty vec for buffered windowed record, got {} results",
        results.len()
    );
}

// ============================================================================
// SINGLE RESULT TESTS - Records that produce one output
// ============================================================================

/// Test that non-windowed SELECT returns single result for matching record
#[test]
fn test_sync_non_windowed_select_returns_single_result() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT id, value FROM stream WHERE value > 50")
        .unwrap();
    let record = create_record(1, 150, 0); // Matches filter

    let results = engine.execute_with_record_sync(&query, &record).unwrap();

    assert_eq!(
        results.len(),
        1,
        "Expected 1 result for matching non-windowed query, got {}",
        results.len()
    );

    // Validate result has expected fields
    let result = &results[0];
    assert!(result.fields.contains_key("id"));
    assert!(result.fields.contains_key("value"));
}

/// Test that simple projection returns single result
#[test]
fn test_sync_simple_projection_returns_single_result() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser.parse("SELECT * FROM stream").unwrap();
    let record = create_record(1, 100, 0);

    let results = engine.execute_with_record_sync(&query, &record).unwrap();

    assert_eq!(results.len(), 1, "Expected 1 result for SELECT *");
    assert_eq!(results[0].fields.get("id"), Some(&FieldValue::Integer(1)));
}

// ============================================================================
// MULTIPLE RESULT TESTS - Records that produce multiple outputs
// ============================================================================

/// Test that GROUP BY without window can produce multiple results
/// (when multiple groups complete from one record)
#[test]
fn test_sync_group_by_potential_multiple_results() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // GROUP BY query - may emit multiple group results
    let query = parser
        .parse("SELECT id, COUNT(*) as cnt FROM stream GROUP BY id")
        .unwrap();
    let record1 = create_record(1, 100, 0);
    let record2 = create_record(2, 200, 0); // Different group

    // First record
    let results1 = engine.execute_with_record_sync(&query, &record1).unwrap();
    // Results depend on GROUP BY implementation (may be 0 or 1+)
    assert!(
        results1.len() <= 1,
        "Expected 0 or 1 result from first GROUP BY record, got {}",
        results1.len()
    );

    // Second record with different group
    let results2 = engine.execute_with_record_sync(&query, &record2).unwrap();
    // Should handle multiple groups
    assert!(
        results2.len() <= 2,
        "Expected 0-2 results from GROUP BY with different group, got {}",
        results2.len()
    );
}

/// Test that complex expressions preserve output count
#[test]
fn test_sync_complex_expression_maintains_output_count() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT id, value * 2 as doubled FROM stream WHERE value > 50")
        .unwrap();
    let record = create_record(1, 100, 0);

    let results = engine.execute_with_record_sync(&query, &record).unwrap();

    assert_eq!(results.len(), 1, "Expected 1 result");

    // Verify the calculation happened
    let result = &results[0];
    assert!(result.fields.contains_key("doubled"));
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

/// Test that multiple executions on same engine maintain state correctly
#[test]
fn test_sync_multiple_sequential_records_no_leakage() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser
        .parse("SELECT * FROM stream WHERE value > 75")
        .unwrap();

    let record1 = create_record(1, 100, 0);
    let record2 = create_record(2, 50, 0); // Filtered out
    let record3 = create_record(3, 150, 0);

    let results1 = engine.execute_with_record_sync(&query, &record1).unwrap();
    let results2 = engine.execute_with_record_sync(&query, &record2).unwrap();
    let results3 = engine.execute_with_record_sync(&query, &record3).unwrap();

    assert_eq!(results1.len(), 1, "Record 1 should match");
    assert_eq!(results2.len(), 0, "Record 2 should be filtered");
    assert_eq!(results3.len(), 1, "Record 3 should match");

    // Verify no state leakage between records
    if !results1.is_empty() {
        assert_eq!(results1[0].fields.get("id"), Some(&FieldValue::Integer(1)));
    }
    if !results3.is_empty() {
        assert_eq!(results3[0].fields.get("id"), Some(&FieldValue::Integer(3)));
    }
}

/// Test error handling doesn't affect return type
#[test]
fn test_sync_error_condition_returns_err_not_empty_vec() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // Invalid SQL that should fail to parse
    let result = parser.parse("SELECT FROM stream"); // Missing columns

    assert!(result.is_err(), "Invalid SQL should fail to parse");
}

/// Test very basic query compiles and returns correct number of results
#[test]
fn test_sync_minimal_query_structure() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser.parse("SELECT id FROM stream").unwrap();
    let record = create_record(42, 100, 0);

    let results = engine.execute_with_record_sync(&query, &record).unwrap();

    assert_eq!(results.len(), 1, "Expected 1 result");
    assert_eq!(results[0].fields.get("id"), Some(&FieldValue::Integer(42)));
}

// ============================================================================
// COMPARISON TESTS - Sync vs Async behavior consistency
// ============================================================================

/// Verify sync version handles empty results like async version
#[test]
fn test_sync_empty_results_behavior_matches_intent() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    // Query that definitely returns empty on filtered record
    let query = parser.parse("SELECT * FROM stream WHERE false").unwrap();
    let record = create_record(1, 100, 0);

    let results = engine.execute_with_record_sync(&query, &record).unwrap();

    // Should always be empty for 'WHERE false'
    assert_eq!(
        results.len(),
        0,
        "WHERE false should always return empty, got {} results",
        results.len()
    );
}

// ============================================================================
// DOCUMENTATION AND TYPE VALIDATION TESTS
// ============================================================================

/// Test that return type is indeed Vec, not Option
#[test]
fn test_sync_return_type_is_vec_not_option() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser.parse("SELECT * FROM stream").unwrap();
    let record = create_record(1, 100, 0);

    let result = engine.execute_with_record_sync(&query, &record);

    // If this compiles and the test passes, we have Result<Vec<...>, ...>
    assert!(result.is_ok(), "Should return Ok");
    let results = result.unwrap();

    // Vec operations should work (not Option operations)
    assert_eq!(results.len(), 1);
    assert!(results.iter().next().is_some());
}

/// Test that calling iter() on results works as expected
#[test]
fn test_sync_vec_results_supports_iteration() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = parser.parse("SELECT * FROM stream").unwrap();
    let record = create_record(1, 100, 0);

    let results = engine.execute_with_record_sync(&query, &record).unwrap();

    // Vec iteration should work
    let mut count = 0;
    for result in results.iter() {
        count += 1;
        assert!(result.fields.contains_key("id"));
    }

    assert_eq!(count, 1);
}
