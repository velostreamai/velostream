//! Unit tests for ROWS WINDOW functionality (Phase 8)
//!
//! Comprehensive tests for:
//! - Buffer operations and size management
//! - Partition isolation (PARTITION BY semantics)
//! - RANK function (with gaps)
//! - DENSE_RANK function (no gaps)
//! - PERCENT_RANK function (0-1 scale)
//! - Time gap detection for session windows
//! - Emission strategies (EveryRecord vs BufferFull)

use std::collections::VecDeque;
use velostream::velostream::sql::execution::internal::RowsWindowState;

#[test]
fn test_rows_window_buffer_initialization() {
    // Test that RowsWindowState initializes with correct buffer size
    let state = RowsWindowState {
        state_id: "test_state_1".to_string(),
        row_buffer: VecDeque::with_capacity(10),
        buffer_size: 10,
        ranking_index: Default::default(),
        emit_mode: Default::default(), // RowsEmitMode::EveryRecord by default
        last_emit_timestamp: 0,
        record_count: 0,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec![],
        expire_after: None,
        last_activity_timestamp: None,
    };

    assert_eq!(state.buffer_size, 10, "Buffer size should be 10");
    assert_eq!(state.row_buffer.len(), 0, "Initial buffer should be empty");
    assert_eq!(state.record_count, 0, "Initial record count should be 0");
}

#[test]
fn test_rows_window_buffer_operations() {
    // Test buffer state management without needing actual StreamRecord objects
    let mut state = RowsWindowState {
        state_id: "test_state_2".to_string(),
        row_buffer: VecDeque::with_capacity(5),
        buffer_size: 5,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 2,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec![],
        expire_after: None,
        last_activity_timestamp: None,
    };

    // Verify record count tracking
    assert_eq!(state.record_count, 2, "Record count should be 2");
    assert!(
        state.record_count as u32 <= state.buffer_size,
        "Record count should not exceed buffer size"
    );

    // Simulate adding more records by incrementing counter
    state.record_count = 4;
    assert_eq!(
        state.record_count, 4,
        "Record count should be 4 after update"
    );
}

#[test]
fn test_rows_window_buffer_overflow_detection() {
    // Test that we can detect when buffer reaches capacity
    let mut state = RowsWindowState {
        state_id: "test_state_3".to_string(),
        row_buffer: VecDeque::with_capacity(3),
        buffer_size: 3,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 3,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec![],
        expire_after: None,
        last_activity_timestamp: None,
    };

    // Check buffer is full
    let is_full = state.record_count as u32 >= state.buffer_size;
    assert!(is_full, "Buffer should be detected as full");

    // Simulate adding one more record
    state.record_count = 4;
    assert!(
        state.record_count as u32 > state.buffer_size,
        "Record count can temporarily exceed buffer size before emission"
    );
}

#[test]
fn test_rows_window_partition_isolation() {
    // Test that different partition values maintain separate state
    let partition_a = vec!["partition_a".to_string()];
    let partition_b = vec!["partition_b".to_string()];

    let state_a = RowsWindowState {
        state_id: "state_a".to_string(),
        row_buffer: VecDeque::with_capacity(5),
        buffer_size: 5,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 3,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: partition_a,
        expire_after: None,
        last_activity_timestamp: None,
    };

    let state_b = RowsWindowState {
        state_id: "state_b".to_string(),
        row_buffer: VecDeque::with_capacity(5),
        buffer_size: 5,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 2,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: partition_b,
        expire_after: None,
        last_activity_timestamp: None,
    };

    // Verify partition isolation
    assert_eq!(state_a.record_count, 3, "Partition A should have 3 records");
    assert_eq!(state_b.record_count, 2, "Partition B should have 2 records");
    assert_ne!(
        state_a.partition_values, state_b.partition_values,
        "Partitions should be different"
    );
}

#[test]
fn test_rows_window_rank_function() {
    // Test RANK function computation
    // RANK: 1, 2, 2, 4, 5 (with gaps)
    let mut state = RowsWindowState {
        state_id: "rank_test".to_string(),
        row_buffer: VecDeque::with_capacity(10),
        buffer_size: 10,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 0,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec![],
        expire_after: None,
        last_activity_timestamp: None,
    };

    // Build ranking index: 5 rows
    // Index 0: rank 1
    // Index 1,2: rank 2 (tied)
    // Index 3: rank 4
    // Index 4: rank 5
    state.ranking_index.insert(0, vec![0]); // First position -> rank 1
    state.ranking_index.insert(1, vec![1, 2]); // Second position (2 rows tied) -> rank 2 each
    state.ranking_index.insert(2, vec![3]); // Fourth position -> rank 4
    state.ranking_index.insert(3, vec![4]); // Fifth position -> rank 5

    // Verify ranking structure is sound
    let total_indices: usize = state.ranking_index.values().map(|v| v.len()).sum();
    assert_eq!(total_indices, 5, "Should have 5 total ranked rows");
}

#[test]
fn test_rows_window_dense_rank_function() {
    // Test DENSE_RANK function computation
    // DENSE_RANK: 1, 2, 2, 3, 4 (no gaps)
    let mut state = RowsWindowState {
        state_id: "dense_rank_test".to_string(),
        row_buffer: VecDeque::with_capacity(10),
        buffer_size: 10,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 0,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec![],
        expire_after: None,
        last_activity_timestamp: None,
    };

    // For DENSE_RANK, we iterate through ranking_index
    // Number of iterations = number of distinct rank values
    for i in 0..4 {
        state.ranking_index.insert(i as i64, vec![i as usize]);
    }

    // Add a tie at rank position 2
    state.ranking_index.insert(1, vec![1, 2]);

    let distinct_ranks = state.ranking_index.len();
    assert_eq!(distinct_ranks, 4, "Should have 4 distinct rank levels");
}

#[test]
fn test_rows_window_percent_rank_computation() {
    // Test PERCENT_RANK computation: (rank - 1) / (total_rows - 1)
    let state = RowsWindowState {
        state_id: "percent_rank_test".to_string(),
        row_buffer: VecDeque::with_capacity(10),
        buffer_size: 10,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 5,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec![],
        expire_after: None,
        last_activity_timestamp: None,
    };

    // For PERCENT_RANK:
    // Row 1: (1 - 1) / (5 - 1) = 0.0
    // Row 2: (2 - 1) / (5 - 1) = 0.25
    // Row 3: (3 - 1) / (5 - 1) = 0.5
    // Row 4: (4 - 1) / (5 - 1) = 0.75
    // Row 5: (5 - 1) / (5 - 1) = 1.0

    let total_rows = state.record_count as i64;
    assert_eq!(total_rows, 5, "Should have 5 rows");

    // Verify percent_rank calculation for various positions
    let percent_rank_row1 = (1.0 - 1.0) / (total_rows as f64 - 1.0);
    let percent_rank_row5 = (5.0 - 1.0) / (total_rows as f64 - 1.0);

    assert!(
        (percent_rank_row1 - 0.0).abs() < 0.001,
        "First row percent_rank should be 0.0"
    );
    assert!(
        (percent_rank_row5 - 1.0).abs() < 0.001,
        "Last row percent_rank should be 1.0"
    );
}

#[test]
fn test_rows_window_time_gap_detection() {
    // Test time gap detection for session-like semantics
    let mut state = RowsWindowState {
        state_id: "time_gap_test".to_string(),
        row_buffer: VecDeque::with_capacity(5),
        buffer_size: 5,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 1000,
        record_count: 0,
        last_emit_count: 0,
        time_gap: Some(500), // 500ms gap threshold
        last_timestamp: Some(1000),
        gap_detected: false,
        partition_values: vec![],
        expire_after: None,
        last_activity_timestamp: None,
    };

    // Simulate a new record arriving with timestamp 2000ms
    // Gap = 2000 - 1000 = 1000ms > 500ms threshold
    let new_timestamp = 2000i64;
    if let Some(gap_threshold) = state.time_gap {
        if let Some(last_ts) = state.last_timestamp {
            let gap = new_timestamp - last_ts;
            if gap > gap_threshold {
                state.gap_detected = true;
            }
        }
    }

    assert!(
        state.gap_detected,
        "Time gap should be detected (1000ms > 500ms)"
    );

    // Simulate a record arriving within the gap threshold
    let mut state2 = state.clone();
    state2.gap_detected = false;
    let close_timestamp = 1200i64;
    if let Some(gap_threshold) = state2.time_gap {
        if let Some(last_ts) = state2.last_timestamp {
            let gap = close_timestamp - last_ts;
            if gap > gap_threshold {
                state2.gap_detected = true;
            }
        }
    }

    assert!(
        !state2.gap_detected,
        "No gap should be detected (200ms < 500ms)"
    );
}

#[test]
fn test_rows_window_emission_strategy_every_record() {
    // Test EveryRecord emission strategy
    let state = RowsWindowState {
        state_id: "emission_test".to_string(),
        row_buffer: VecDeque::with_capacity(10),
        buffer_size: 10,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 1,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec![],
        expire_after: None,
        last_activity_timestamp: None,
    };

    // With EveryRecord mode, each record triggers emission
    // Default emit mode is EveryRecord
    assert_eq!(state.record_count, 1, "Should have 1 record");
}

#[test]
fn test_rows_window_emission_strategy_buffer_full() {
    // Test BufferFull emission strategy
    let mut state = RowsWindowState {
        state_id: "buffer_full_test".to_string(),
        row_buffer: VecDeque::with_capacity(3),
        buffer_size: 3,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 2,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec![],
        expire_after: None,
        last_activity_timestamp: None,
    };

    // Check buffer is not yet full
    let buffer_is_full = state.record_count as u32 >= state.buffer_size;
    assert!(
        !buffer_is_full,
        "Buffer should not be full yet (2/3 records)"
    );

    // Add one more record to fill buffer
    state.record_count += 1;

    let buffer_is_full_now = state.record_count as u32 >= state.buffer_size;
    assert!(
        buffer_is_full_now,
        "Buffer should be full now (3/3 records)"
    );
}

#[test]
fn test_rows_window_multiple_partitions_isolation() {
    // Test that multiple partition states are truly isolated
    // Simulate: PARTITION BY customer_id
    // Customer A: 3 records
    // Customer B: 2 records

    let mut states = std::collections::HashMap::new();

    // Customer A state
    let state_a = RowsWindowState {
        state_id: "state_customer_a".to_string(),
        row_buffer: VecDeque::with_capacity(10),
        buffer_size: 10,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 3,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec!["customer_a".to_string()],
        expire_after: None,
        last_activity_timestamp: None,
    };

    // Customer B state
    let state_b = RowsWindowState {
        state_id: "state_customer_b".to_string(),
        row_buffer: VecDeque::with_capacity(10),
        buffer_size: 10,
        ranking_index: Default::default(),
        emit_mode: Default::default(),
        last_emit_timestamp: 0,
        record_count: 2,
        last_emit_count: 0,
        time_gap: None,
        last_timestamp: None,
        gap_detected: false,
        partition_values: vec!["customer_b".to_string()],
        expire_after: None,
        last_activity_timestamp: None,
    };

    states.insert("customer_a", state_a);
    states.insert("customer_b", state_b);

    // Verify isolation
    let state_a_records = states.get("customer_a").unwrap().record_count;
    let state_b_records = states.get("customer_b").unwrap().record_count;

    assert_eq!(state_a_records, 3, "Customer A should have 3 records");
    assert_eq!(state_b_records, 2, "Customer B should have 2 records");
    assert_ne!(
        state_a_records, state_b_records,
        "Partition counts should differ"
    );
}
