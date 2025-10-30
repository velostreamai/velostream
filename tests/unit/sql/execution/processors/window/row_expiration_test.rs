use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::sql::ast::RowsEmitMode;
use velostream::velostream::sql::execution::internal::RowsWindowState;
use velostream::velostream::sql::execution::types::StreamRecord;

/// Test default 1-minute expiration timeout
#[test]
fn test_row_expiration_default_timeout() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Add first record at timestamp 0
    let record1 = StreamRecord::new(HashMap::new());
    window.add_record(record1, 0);
    assert_eq!(window.buffer_len(), 1);

    // Add second record within 1 minute (59 seconds = 59000 ms)
    let record2 = StreamRecord::new(HashMap::new());
    window.add_record(record2, 59_000);
    assert_eq!(
        window.buffer_len(),
        2,
        "Buffer should not expire within 1 minute"
    );

    // Check expiration at 59 seconds - should return false (no expiration)
    let expired = window.check_and_apply_expiration(59_000);
    assert!(!expired, "Should not expire before 1 minute threshold");
    assert_eq!(window.buffer_len(), 2, "Buffer should remain intact");

    // Add third record at 60001 ms (just over 1 minute) - should trigger expiration
    let record3 = StreamRecord::new(HashMap::new());
    window.add_record(record3, 60_001);
    let expired = window.check_and_apply_expiration(60_001);
    assert!(expired, "Should expire after 1 minute gap");
    assert_eq!(
        window.buffer_len(),
        1,
        "Buffer should be cleared (only new record remains)"
    );
}

/// Test custom expiration duration
#[test]
fn test_row_expiration_custom_duration() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Set custom 30-second timeout
    window.set_expire_after(Some(Duration::from_secs(30)));

    // Add first record at timestamp 0
    let record1 = StreamRecord::new(HashMap::new());
    window.add_record(record1, 0);
    window.check_and_apply_expiration(0);
    assert_eq!(window.buffer_len(), 1);

    // Add second record at 25 seconds (within threshold)
    let record2 = StreamRecord::new(HashMap::new());
    window.add_record(record2, 25_000);
    let expired = window.check_and_apply_expiration(25_000);
    assert!(!expired, "Should not expire within 30 seconds");
    assert_eq!(window.buffer_len(), 2);

    // Add third record at 35 seconds (exceeds 30-second threshold)
    let record3 = StreamRecord::new(HashMap::new());
    window.add_record(record3, 35_000);
    let expired = window.check_and_apply_expiration(35_000);
    assert!(expired, "Should expire after 30-second gap");
    assert_eq!(window.buffer_len(), 1, "Old records should be cleared");
}

/// Test short expiration duration (5 seconds)
#[test]
fn test_row_expiration_short_timeout() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Set 5-second timeout
    window.set_expire_after(Some(Duration::from_secs(5)));

    // Add records at different times
    let record1 = StreamRecord::new(HashMap::new());
    window.add_record(record1, 0);
    window.check_and_apply_expiration(0);

    let record2 = StreamRecord::new(HashMap::new());
    window.add_record(record2, 3_000); // 3 seconds later
    window.check_and_apply_expiration(3_000);
    assert_eq!(window.buffer_len(), 2, "Should not expire within 5 seconds");

    // Now trigger expiration at 6 seconds
    let expired = window.check_and_apply_expiration(6_000);
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

    // Cycle 1: Add records 0-15 seconds
    let r1 = StreamRecord::new(HashMap::new());
    window.add_record(r1, 0);
    window.check_and_apply_expiration(0);

    let r2 = StreamRecord::new(HashMap::new());
    window.add_record(r2, 5_000);
    window.check_and_apply_expiration(5_000);
    assert_eq!(window.buffer_len(), 2);

    // Expire cycle 1
    let expired = window.check_and_apply_expiration(15_000);
    assert!(expired, "Should expire first cycle");
    assert_eq!(window.buffer_len(), 0);

    // Cycle 2: Start fresh
    let r3 = StreamRecord::new(HashMap::new());
    window.add_record(r3, 20_000);
    window.check_and_apply_expiration(20_000);
    assert_eq!(window.buffer_len(), 1);

    let r4 = StreamRecord::new(HashMap::new());
    window.add_record(r4, 25_000);
    window.check_and_apply_expiration(25_000);
    assert_eq!(window.buffer_len(), 2);

    // No expiration yet
    let expired = window.check_and_apply_expiration(29_000);
    assert!(!expired, "Should not expire before 10 seconds");
    assert_eq!(window.buffer_len(), 2);

    // Now expire cycle 2
    let expired = window.check_and_apply_expiration(35_000);
    assert!(expired, "Should expire second cycle");
}

/// Test that expiration doesn't trigger prematurely
#[test]
fn test_row_expiration_boundary_cases() {
    let mut window = RowsWindowState::new("test".to_string(), 100, RowsEmitMode::EveryRecord, None);

    // Set 1000ms timeout
    window.set_expire_after(Some(Duration::from_millis(1000)));

    let r1 = StreamRecord::new(HashMap::new());
    window.add_record(r1, 0);
    window.check_and_apply_expiration(0);

    // Test exactly at threshold (should not expire)
    let expired = window.check_and_apply_expiration(999);
    assert!(!expired, "Should not expire at 999ms");

    // Test just over threshold (should expire)
    let expired = window.check_and_apply_expiration(1001);
    assert!(expired, "Should expire at 1001ms");
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
    assert_eq!(window.buffer_len(), 5, "Buffer should stay at max size");

    // Now trigger expiration
    let expired = window.check_and_apply_expiration(25_000);
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

    // No expiration at 4000ms (4 seconds)
    let expired = window.check_and_apply_expiration(4_000);
    assert!(!expired, "Should not expire at 4 seconds");

    // Expire at 5100ms (>5 seconds from last activity at 200ms)
    let expired = window.check_and_apply_expiration(5_100);
    assert!(expired, "Should expire at 5.1 seconds");

    // Add new record after expiration
    let r2 = StreamRecord::new(HashMap::new());
    window.add_record(r2, 5_200);
    window.check_and_apply_expiration(5_200);
    assert_eq!(
        window.buffer_len(),
        1,
        "Should have 1 new record after expiration"
    );

    // Should not expire immediately
    let expired = window.check_and_apply_expiration(5_300);
    assert!(!expired, "Should not expire on next immediate check");
}
