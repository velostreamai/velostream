//! Unit tests for SELECT DISTINCT deduplication
//!
//! Tests for DISTINCT execution in SelectProcessor:
//! - Basic deduplication
//! - Memory bounds with FIFO eviction
//! - DistinctState metrics (duplicates_filtered, evictions)
//! - Cross-type hash collision prevention

use std::collections::HashMap;
use velostream::velostream::sql::{
    execution::{
        FieldValue, StreamRecord,
        processors::{DistinctState, ProcessorContext, SelectProcessor},
    },
    parser::StreamingSqlParser,
};

fn create_test_record_with_values(id: i64, name: &str, price: f64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("name".to_string(), FieldValue::String(name.to_string()));
    fields.insert("price".to_string(), FieldValue::Float(price));

    StreamRecord {
        fields,
        timestamp: 0,
        offset: 1,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
        topic: None,
        key: None,
    }
}

// === DistinctState Unit Tests ===

#[test]
fn test_distinct_state_basic_deduplication() {
    let mut state = DistinctState::new(100);

    // First insertion should succeed
    assert!(!state.check_duplicate(12345));
    state.insert(12345);
    assert_eq!(state.len(), 1);

    // Second insertion of same hash should be detected as duplicate
    assert!(state.check_duplicate(12345));
    state.record_duplicate();
    assert_eq!(state.duplicates_filtered, 1);

    // Different hash should succeed
    assert!(!state.check_duplicate(67890));
    state.insert(67890);
    assert_eq!(state.len(), 2);
}

#[test]
fn test_distinct_state_memory_bounds_fifo_eviction() {
    let mut state = DistinctState::new(3); // Small limit for testing

    // Insert 3 records
    state.insert(111);
    state.insert(222);
    state.insert(333);
    assert_eq!(state.len(), 3);
    assert_eq!(state.evictions, 0);

    // Insert 4th record - should evict oldest (111)
    let evictions = state.insert(444);
    assert_eq!(evictions, 1);
    assert_eq!(state.len(), 3); // Still at max
    assert_eq!(state.evictions, 1);

    // 111 should no longer be in state (evicted)
    assert!(!state.check_duplicate(111));
    // But 222, 333, 444 should still be present
    assert!(state.check_duplicate(222));
    assert!(state.check_duplicate(333));
    assert!(state.check_duplicate(444));
}

#[test]
fn test_distinct_state_metrics_tracking() {
    let mut state = DistinctState::new(100);

    // Insert and track duplicates
    state.insert(100);
    state.insert(200);

    // Record 3 duplicates
    assert!(state.check_duplicate(100));
    state.record_duplicate();
    assert!(state.check_duplicate(100));
    state.record_duplicate();
    assert!(state.check_duplicate(200));
    state.record_duplicate();

    assert_eq!(state.duplicates_filtered, 3);
    assert_eq!(state.len(), 2);
}

#[test]
fn test_distinct_state_empty_check() {
    let state = DistinctState::new(100);
    assert!(state.is_empty());
    assert_eq!(state.len(), 0);

    let mut state2 = DistinctState::new(100);
    state2.insert(123);
    assert!(!state2.is_empty());
    assert_eq!(state2.len(), 1);
}

// === SELECT DISTINCT Execution Tests ===

#[test]
fn test_select_distinct_filters_duplicates() {
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT DISTINCT name FROM test_stream";
    let query = parser.parse(query_str).expect("Should parse");

    let mut context = ProcessorContext::new("test");

    // First record with name "Alice"
    let record1 = create_test_record_with_values(1, "Alice", 100.0);
    let result1 = SelectProcessor::process(&query, &record1, &mut context);
    assert!(result1.is_ok());
    assert!(
        result1.unwrap().record.is_some(),
        "First record should pass"
    );

    // Second record with same name "Alice" - should be filtered
    let record2 = create_test_record_with_values(2, "Alice", 200.0);
    let result2 = SelectProcessor::process(&query, &record2, &mut context);
    assert!(result2.is_ok());
    assert!(
        result2.unwrap().record.is_none(),
        "Duplicate should be filtered"
    );

    // Third record with different name "Bob" - should pass
    let record3 = create_test_record_with_values(3, "Bob", 150.0);
    let result3 = SelectProcessor::process(&query, &record3, &mut context);
    assert!(result3.is_ok());
    assert!(
        result3.unwrap().record.is_some(),
        "Different record should pass"
    );
}

#[test]
fn test_select_distinct_multiple_columns() {
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT DISTINCT name, price FROM test_stream";
    let query = parser.parse(query_str).expect("Should parse");

    let mut context = ProcessorContext::new("test");

    // First record
    let record1 = create_test_record_with_values(1, "Alice", 100.0);
    let result1 = SelectProcessor::process(&query, &record1, &mut context);
    assert!(result1.unwrap().record.is_some());

    // Same name, same price - should be filtered
    let record2 = create_test_record_with_values(2, "Alice", 100.0);
    let result2 = SelectProcessor::process(&query, &record2, &mut context);
    assert!(
        result2.unwrap().record.is_none(),
        "Same name+price should be filtered"
    );

    // Same name, different price - should pass
    let record3 = create_test_record_with_values(3, "Alice", 200.0);
    let result3 = SelectProcessor::process(&query, &record3, &mut context);
    assert!(
        result3.unwrap().record.is_some(),
        "Different price should pass"
    );
}

#[test]
fn test_select_distinct_with_where_clause() {
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT DISTINCT name FROM test_stream WHERE price > 50";
    let query = parser.parse(query_str).expect("Should parse");

    let mut context = ProcessorContext::new("test");

    // Record that passes filter
    let record1 = create_test_record_with_values(1, "Alice", 100.0);
    let result1 = SelectProcessor::process(&query, &record1, &mut context);
    assert!(result1.unwrap().record.is_some(), "Should pass filter");

    // Same name, passes filter - should be filtered as duplicate
    let record2 = create_test_record_with_values(2, "Alice", 150.0);
    let result2 = SelectProcessor::process(&query, &record2, &mut context);
    assert!(result2.unwrap().record.is_none(), "Duplicate after WHERE");

    // Record that fails filter - should be filtered by WHERE (not DISTINCT)
    let record3 = create_test_record_with_values(3, "Bob", 30.0);
    let result3 = SelectProcessor::process(&query, &record3, &mut context);
    assert!(
        result3.unwrap().record.is_none(),
        "Should fail WHERE filter"
    );
}

#[test]
fn test_select_without_distinct_allows_duplicates() {
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT name FROM test_stream"; // No DISTINCT
    let query = parser.parse(query_str).expect("Should parse");

    let mut context = ProcessorContext::new("test");

    // First record
    let record1 = create_test_record_with_values(1, "Alice", 100.0);
    let result1 = SelectProcessor::process(&query, &record1, &mut context);
    assert!(result1.unwrap().record.is_some());

    // Same name without DISTINCT - should NOT be filtered
    let record2 = create_test_record_with_values(2, "Alice", 200.0);
    let result2 = SelectProcessor::process(&query, &record2, &mut context);
    assert!(
        result2.unwrap().record.is_some(),
        "Without DISTINCT, duplicates allowed"
    );
}

#[test]
fn test_select_distinct_wildcard() {
    let parser = StreamingSqlParser::new();
    let query_str = "SELECT DISTINCT * FROM test_stream";
    let query = parser.parse(query_str).expect("Should parse");

    let mut context = ProcessorContext::new("test");

    // First record
    let record1 = create_test_record_with_values(1, "Alice", 100.0);
    let result1 = SelectProcessor::process(&query, &record1, &mut context);
    assert!(result1.unwrap().record.is_some());

    // Exact same record - should be filtered
    let record2 = create_test_record_with_values(1, "Alice", 100.0);
    let result2 = SelectProcessor::process(&query, &record2, &mut context);
    assert!(
        result2.unwrap().record.is_none(),
        "Identical record should be filtered"
    );

    // Different id but same name/price - should pass (different hash)
    let record3 = create_test_record_with_values(2, "Alice", 100.0);
    let result3 = SelectProcessor::process(&query, &record3, &mut context);
    assert!(
        result3.unwrap().record.is_some(),
        "Different id means different hash"
    );
}

// === Cross-Type Hash Collision Tests ===

#[test]
fn test_distinct_no_cross_type_collision() {
    // Test that integer 0 and boolean false don't collide
    // This verifies the type discriminant constants work correctly

    let parser = StreamingSqlParser::new();
    let query_str = "SELECT DISTINCT id FROM test_stream";
    let query = parser.parse(query_str).expect("Should parse");

    let mut context = ProcessorContext::new("test");

    // Record with integer 0
    let mut record1 = StreamRecord {
        fields: HashMap::new(),
        timestamp: 0,
        offset: 1,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
        topic: None,
        key: None,
    };
    record1
        .fields
        .insert("id".to_string(), FieldValue::Integer(0));

    let result1 = SelectProcessor::process(&query, &record1, &mut context);
    assert!(result1.unwrap().record.is_some(), "Integer 0 should pass");

    // Record with boolean false (would collide with 0 without type discriminant)
    let mut record2 = StreamRecord {
        fields: HashMap::new(),
        timestamp: 0,
        offset: 1,
        partition: 0,
        headers: HashMap::new(),
        event_time: None,
        topic: None,
        key: None,
    };
    record2
        .fields
        .insert("id".to_string(), FieldValue::Boolean(false));

    let result2 = SelectProcessor::process(&query, &record2, &mut context);
    // Boolean false should NOT be filtered as duplicate of integer 0
    assert!(
        result2.unwrap().record.is_some(),
        "Boolean false should not collide with Integer 0"
    );
}
