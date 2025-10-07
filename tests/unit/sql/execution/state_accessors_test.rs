/*!
# State Accessor Tests

Tests for StreamExecutionEngine state accessor methods used in external batch processing.

These tests verify that GROUP BY and Window states can be safely accessed and modified
by external processors without holding engine locks during batch processing.
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{StreamExecutionEngine, StreamRecord, FieldValue};
use velostream::velostream::sql::execution::internal::{GroupByState, GroupAccumulator, WindowState};
use velostream::velostream::sql::ast::WindowSpec;

#[test]
fn test_get_group_states_empty() {
    // Given: New engine with no GROUP BY state
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = StreamExecutionEngine::new(tx);

    // When: Getting GROUP BY states
    let states = engine.get_group_states();

    // Then: Should return empty HashMap
    assert!(states.is_empty(), "New engine should have no GROUP BY states");
}

#[test]
fn test_set_and_get_group_states() {
    // Given: New engine
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Create test GROUP BY state
    let mut test_states = HashMap::new();
    let mut groups = HashMap::new();

    // Add a sample group accumulator
    let mut accumulator = GroupAccumulator {
        count: 5,
        non_null_counts: HashMap::new(),
        sums: HashMap::new(),
        mins: HashMap::new(),
        maxs: HashMap::new(),
        numeric_values: HashMap::new(),
        first_values: HashMap::new(),
        last_values: HashMap::new(),
        string_values: HashMap::new(),
        distinct_values: HashMap::new(),
        approx_distinct_values: HashMap::new(),
        sample_record: None,
    };
    accumulator.sums.insert("amount".to_string(), 1000.0);

    groups.insert(vec![FieldValue::String("group1".to_string())], accumulator);

    let group_state = GroupByState {
        groups,
        group_expressions: vec![],
        select_fields: vec![],
        having_clause: None,
    };

    test_states.insert("test_query".to_string(), group_state);

    // When: Setting GROUP BY states
    engine.set_group_states(test_states.clone());

    // Then: Should be able to retrieve them
    let retrieved_states = engine.get_group_states();
    assert_eq!(retrieved_states.len(), 1, "Should have 1 GROUP BY state");
    assert!(retrieved_states.contains_key("test_query"), "Should contain test_query state");

    // Verify the state content
    let retrieved_group = retrieved_states.get("test_query").unwrap();
    assert_eq!(retrieved_group.groups.len(), 1, "Should have 1 group");
}

#[test]
fn test_group_states_clone_pattern() {
    // Given: Engine with GROUP BY state
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let mut test_states = HashMap::new();
    test_states.insert(
        "query1".to_string(),
        GroupByState {
            groups: HashMap::new(),
            group_expressions: vec![],
            select_fields: vec![],
            having_clause: None,
        }
    );
    engine.set_group_states(test_states);

    // When: Simulating external batch processing pattern
    // 1. Get state (minimal lock time)
    let mut cloned_states = engine.get_group_states().clone();

    // 2. Modify cloned state (outside lock)
    cloned_states.insert(
        "query2".to_string(),
        GroupByState {
            groups: HashMap::new(),
            group_expressions: vec![],
            select_fields: vec![],
            having_clause: None,
        }
    );

    // 3. Sync back
    engine.set_group_states(cloned_states);

    // Then: Engine should have updated state
    let final_states = engine.get_group_states();
    assert_eq!(final_states.len(), 2, "Should have 2 GROUP BY states after sync");
    assert!(final_states.contains_key("query1"), "Should still have query1");
    assert!(final_states.contains_key("query2"), "Should have new query2");
}

#[test]
fn test_get_window_states_empty() {
    // Given: New engine with no active queries
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = StreamExecutionEngine::new(tx);

    // When: Getting window states
    let states = engine.get_window_states();

    // Then: Should return empty vector
    assert!(states.is_empty(), "New engine should have no window states");
}

#[test]
fn test_set_window_states_no_active_queries() {
    // Given: Engine with no active queries
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Create test window state
    let window_state = WindowState {
        window_spec: WindowSpec::Tumbling {
            size_ms: 5000,
        },
        buffer: vec![],
        last_emit: 0,
    };

    let test_states = vec![
        ("nonexistent_query".to_string(), window_state),
    ];

    // When: Setting window states for non-existent query
    engine.set_window_states(test_states);

    // Then: Should be ignored gracefully (no panic)
    let retrieved_states = engine.get_window_states();
    assert!(retrieved_states.is_empty(), "Should have no window states for non-existent queries");
}

#[test]
fn test_window_states_lifecycle() {
    // Given: Engine (window states require active queries to work properly)
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Note: In real usage, window states are created through query registration
    // This test just verifies the accessor methods work correctly

    // When: Getting states before any are set
    let initial_states = engine.get_window_states();

    // Then: Should be empty
    assert!(initial_states.is_empty(), "Initial window states should be empty");

    // When: Setting empty states
    engine.set_window_states(vec![]);

    // Then: Should still be empty
    let after_empty_set = engine.get_window_states();
    assert!(after_empty_set.is_empty(), "After setting empty vec should still be empty");
}

#[test]
fn test_state_accessors_thread_safety_simulation() {
    // This test simulates the external batch processing pattern
    // to ensure state accessors work correctly

    // Given: Engine with initial state
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let mut initial_states = HashMap::new();
    initial_states.insert(
        "query1".to_string(),
        GroupByState {
            groups: HashMap::new(),
            group_expressions: vec![],
            select_fields: vec![],
            having_clause: None,
        }
    );
    engine.set_group_states(initial_states);

    // When: Simulating batch processing pattern
    // Lock 1: Get state
    let mut batch_states = engine.get_group_states().clone();

    // Process batch (no lock held)
    batch_states.get_mut("query1").unwrap().groups.insert(
        vec![FieldValue::Integer(1)],
        GroupAccumulator {
            count: 10,
            non_null_counts: HashMap::new(),
            sums: HashMap::new(),
            mins: HashMap::new(),
            maxs: HashMap::new(),
            numeric_values: HashMap::new(),
            first_values: HashMap::new(),
            last_values: HashMap::new(),
            string_values: HashMap::new(),
            distinct_values: HashMap::new(),
            approx_distinct_values: HashMap::new(),
            sample_record: None,
        }
    );

    // Lock 2: Sync state back
    engine.set_group_states(batch_states);

    // Then: State should be updated
    let final_state = engine.get_group_states();
    let query1_state = final_state.get("query1").unwrap();
    assert_eq!(query1_state.groups.len(), 1, "Should have 1 group after processing");
}

#[test]
fn test_multiple_state_updates() {
    // Given: Engine
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // When: Performing multiple state updates (simulating multiple batches)
    for i in 0..5 {
        let mut states = HashMap::new();
        states.insert(
            format!("query_{}", i),
            GroupByState {
                groups: HashMap::new(),
                group_expressions: vec![],
                select_fields: vec![],
                having_clause: None,
            }
        );
        engine.set_group_states(states);

        // Then: Each update should work correctly
        let current_states = engine.get_group_states();
        assert_eq!(current_states.len(), 1, "Should have exactly 1 state after each update");
        assert!(current_states.contains_key(&format!("query_{}", i)),
                "Should have current query state");
    }
}

#[test]
fn test_state_independence() {
    // Given: Engine
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // When: Setting GROUP BY states
    let mut group_states = HashMap::new();
    group_states.insert(
        "group_query".to_string(),
        GroupByState {
            groups: HashMap::new(),
            group_expressions: vec![],
            select_fields: vec![],
            having_clause: None,
        }
    );
    engine.set_group_states(group_states);

    // And: Setting window states (they should be independent)
    let window_states = vec![]; // Empty for this test
    engine.set_window_states(window_states);

    // Then: GROUP BY state should not be affected
    let final_group_states = engine.get_group_states();
    assert_eq!(final_group_states.len(), 1, "GROUP BY state should remain after window state update");

    // And: Window states should not be affected by GROUP BY
    let final_window_states = engine.get_window_states();
    assert_eq!(final_window_states.len(), 0, "Window state should remain independent");
}
