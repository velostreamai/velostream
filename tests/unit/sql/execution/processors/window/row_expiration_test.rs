use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::sql::ast::RowsEmitMode;
use velostream::velostream::sql::execution::internal::RowsWindowState;
use velostream::velostream::sql::execution::types::StreamRecord;

// Note: These tests verify RowsWindowState expiration functionality.
// The last_activity_timestamp is updated on each check_and_apply_expiration call,
// so gaps must be measured from the last check, not the last record add.

/// Test default 1-minute expiration timeout
#[test]
fn test_row_expiration_default_timeout() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Add first record at timestamp 0
    let record1 = StreamRecord::new(HashMap::new());
    window.add_record(record1, 0);
    let _expired = window.check_and_apply_expiration(0);
    assert_eq!(window.buffer_len(), 1);

    // Add second record and check at timestamp 500ms (no expiration - gap is only 500ms)
    let record2 = StreamRecord::new(HashMap::new());
    window.add_record(record2, 500);
    let expired = window.check_and_apply_expiration(500);
    assert!(!expired, "Should not expire within 1 minute threshold");
    assert_eq!(window.buffer_len(), 2, "Buffer should remain intact");

    // Trigger expiration with 60+ second gap (60000ms + 1ms = 60001ms from 500ms is 60501ms)
    let expired = window.check_and_apply_expiration(60_501);
    assert!(expired, "Should expire after 1 minute gap");
    assert_eq!(
        window.buffer_len(),
        0,
        "Buffer should be completely cleared"
    );
}

/// Test custom expiration duration
#[test]
fn test_row_expiration_custom_duration() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Set custom 30-second timeout
    window.set_expire_after(Some(Duration::from_secs(30)));

    // Add first record and check at timestamp 100ms
    let record1 = StreamRecord::new(HashMap::new());
    window.add_record(record1, 100);
    let _expired = window.check_and_apply_expiration(100);
    assert_eq!(window.buffer_len(), 1);

    // Add second record and check at 500ms (within threshold - gap is 400ms)
    let record2 = StreamRecord::new(HashMap::new());
    window.add_record(record2, 500);
    let expired = window.check_and_apply_expiration(500);
    assert!(!expired, "Should not expire within 30 seconds");
    assert_eq!(window.buffer_len(), 2);

    // Check expiration at 30.5 seconds later (30001ms gap from last check at 500ms)
    let expired = window.check_and_apply_expiration(30_501);
    assert!(expired, "Should expire after 30-second gap");
    assert_eq!(window.buffer_len(), 0, "Buffer should be cleared");
}

/// Test short expiration duration (5 seconds)
#[test]
fn test_row_expiration_short_timeout() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Set 5-second timeout
    window.set_expire_after(Some(Duration::from_secs(5)));

    // Add first record and check at 100ms
    let record1 = StreamRecord::new(HashMap::new());
    window.add_record(record1, 100);
    let _expired = window.check_and_apply_expiration(100);

    // Add second record and check at 200ms (gap is only 100ms - no expiration)
    let record2 = StreamRecord::new(HashMap::new());
    window.add_record(record2, 200);
    window.check_and_apply_expiration(200);
    assert_eq!(window.buffer_len(), 2, "Should not expire within 5 seconds");

    // Trigger expiration at 5201ms (gap is >5000ms from last check at 200ms)
    let expired = window.check_and_apply_expiration(5_201);
    assert!(expired, "Should expire after 5-second gap");
    assert_eq!(
        window.buffer_len(),
        0,
        "Buffer should be completely cleared"
    );
}

/// Test multiple expiration cycles
#[test]
fn test_row_expiration_multiple_cycles() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Set 10-second timeout
    window.set_expire_after(Some(Duration::from_secs(10)));

    // Cycle 1: Add records and check
    let r1 = StreamRecord::new(HashMap::new());
    window.add_record(r1, 100);
    let _expired = window.check_and_apply_expiration(100);

    let r2 = StreamRecord::new(HashMap::new());
    window.add_record(r2, 200);
    window.check_and_apply_expiration(200);
    assert_eq!(window.buffer_len(), 2);

    // Expire cycle 1 (10+ seconds from last check at 200)
    let expired = window.check_and_apply_expiration(10_300);
    assert!(expired, "Should expire first cycle");
    assert_eq!(window.buffer_len(), 0);

    // Cycle 2: Add new record and check
    let r3 = StreamRecord::new(HashMap::new());
    window.add_record(r3, 10_400);
    window.check_and_apply_expiration(10_400);
    assert_eq!(window.buffer_len(), 1);

    let r4 = StreamRecord::new(HashMap::new());
    window.add_record(r4, 10_500);
    window.check_and_apply_expiration(10_500);
    assert_eq!(window.buffer_len(), 2);

    // No expiration yet (only 100ms gap)
    let expired = window.check_and_apply_expiration(10_600);
    assert!(!expired, "Should not expire with small gap");
    assert_eq!(window.buffer_len(), 2);

    // Expire cycle 2 (10+ seconds from 10_600)
    let expired = window.check_and_apply_expiration(20_700);
    assert!(expired, "Should expire second cycle");
}

/// Test that expiration doesn't trigger prematurely
#[test]
fn test_row_expiration_boundary_cases() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Set 1000ms timeout
    window.set_expire_after(Some(Duration::from_millis(1000)));

    let r1 = StreamRecord::new(HashMap::new());
    window.add_record(r1, 100);
    window.check_and_apply_expiration(100);

    // Test at 999ms gap (should not expire - gap is 999ms from last check)
    let expired = window.check_and_apply_expiration(1_099);
    assert!(!expired, "Should not expire at 999ms gap");

    // Test at >1000ms gap (should expire - gap is 1102ms from last check at 1099ms)
    let expired = window.check_and_apply_expiration(2_101);
    assert!(expired, "Should expire at >1000ms gap");
}

/// Test accumulation followed by expiration
#[test]
fn test_row_expiration_with_buffer_accumulation() {
    let mut window = RowsWindowState::new("test".to_string(), 5, RowsEmitMode::EveryRecord, None);

    // Set 20-second timeout
    window.set_expire_after(Some(Duration::from_secs(20)));

    // Add 5 records (fill buffer)
    for i in 0..5 {
        let record = StreamRecord::new(HashMap::new());
        window.add_record(record, i * 1_000);
        window.check_and_apply_expiration(i * 1_000);
    }
    assert_eq!(window.buffer_len(), 5, "Buffer should be full");

    // Add 6th record (triggers buffer overflow, removes oldest)
    let record = StreamRecord::new(HashMap::new());
    window.add_record(record, 5_000);
    window.check_and_apply_expiration(5_000);
    assert_eq!(window.buffer_len(), 5, "Buffer should stay at max size");

    // Now trigger expiration (20+ second gap from last check at 5000ms)
    let expired = window.check_and_apply_expiration(25_001);
    assert!(expired, "Should expire after 20-second gap");
    assert_eq!(
        window.buffer_len(),
        0,
        "Buffer should be completely cleared"
    );
}

/// Test timestamp tracking through expiration events
#[test]
fn test_row_expiration_timestamp_tracking() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Set 5-second timeout
    window.set_expire_after(Some(Duration::from_secs(5)));

    // First activity at 100ms
    window.check_and_apply_expiration(100);

    // Add record and check again at 200ms
    let r1 = StreamRecord::new(HashMap::new());
    window.add_record(r1, 200);
    window.check_and_apply_expiration(200);
    assert_eq!(window.buffer_len(), 1);

    // No expiration at 4000ms (4 seconds from check at 200ms)
    let expired = window.check_and_apply_expiration(4_000);
    assert!(!expired, "Should not expire at 4 seconds");

    // Expire at 9001ms (>5 seconds from last check at 4000ms)
    let expired = window.check_and_apply_expiration(9_001);
    assert!(expired, "Should expire after 5-second gap");

    // Add new record after expiration
    let r2 = StreamRecord::new(HashMap::new());
    window.add_record(r2, 9_200);
    window.check_and_apply_expiration(9_200);
    assert_eq!(
        window.buffer_len(),
        1,
        "Should have 1 new record after expiration"
    );

    // Should not expire immediately (gap is only 100ms)
    let expired = window.check_and_apply_expiration(9_300);
    assert!(!expired, "Should not expire on next immediate check");
}
