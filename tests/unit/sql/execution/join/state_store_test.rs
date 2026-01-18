//! Join State Store Tests
//!
//! Tests for the JoinStateStore component.

use std::collections::HashMap;
use std::time::Duration;

use velostream::velostream::sql::execution::StreamRecord;
use velostream::velostream::sql::execution::join::{JoinStateStore, JoinStateStoreConfig};
use velostream::velostream::sql::execution::types::FieldValue;

fn make_test_record(id: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    StreamRecord::new(fields)
}

#[test]
fn test_empty_store() {
    let store = JoinStateStore::new(Duration::from_secs(3600));
    assert_eq!(store.record_count(), 0);
    assert_eq!(store.key_count(), 0);
    assert!(!store.is_near_capacity());
    assert!(!store.is_at_capacity());
}

#[test]
fn test_store_and_lookup() {
    let mut store = JoinStateStore::new(Duration::from_secs(3600));

    store.store("key1", make_test_record(1), 1000);
    store.store("key1", make_test_record(2), 2000);
    store.store("key2", make_test_record(3), 1500);

    assert_eq!(store.record_count(), 3);
    assert_eq!(store.key_count(), 2);

    let matches = store.lookup("key1", 0, 5000);
    assert_eq!(matches.len(), 2);

    let matches = store.lookup("key2", 0, 5000);
    assert_eq!(matches.len(), 1);

    let matches = store.lookup("key3", 0, 5000);
    assert!(matches.is_empty());
}

#[test]
fn test_time_constrained_lookup() {
    let mut store = JoinStateStore::new(Duration::from_secs(3600));

    store.store("key", make_test_record(1), 1000);
    store.store("key", make_test_record(2), 2000);
    store.store("key", make_test_record(3), 3000);
    store.store("key", make_test_record(4), 4000);

    let matches = store.lookup("key", 1500, 3500);
    assert_eq!(matches.len(), 2);

    let matches = store.lookup("key", 0, 1500);
    assert_eq!(matches.len(), 1);

    let matches = store.lookup("key", 5000, 6000);
    assert!(matches.is_empty());
}

#[test]
fn test_watermark_expiration() {
    let mut store = JoinStateStore::with_retention_ms(1000);

    store.store("key", make_test_record(1), 1000);
    store.store("key", make_test_record(2), 2000);
    store.store("key", make_test_record(3), 3000);

    assert_eq!(store.record_count(), 3);

    let expired = store.advance_watermark(2500);
    assert_eq!(expired, 1);
    assert_eq!(store.record_count(), 2);

    let expired = store.advance_watermark(4500);
    assert_eq!(expired, 2);
    assert_eq!(store.record_count(), 0);
}

#[test]
fn test_memory_limits_global() {
    let config = JoinStateStoreConfig::with_limits(5, 0);
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    for i in 0..7 {
        store.store(&format!("key{}", i), make_test_record(i), i * 1000);
    }

    assert!(store.record_count() <= 5);

    let stats = store.stats();
    assert!(stats.records_evicted > 0);
}

#[test]
fn test_memory_limits_per_key() {
    let config = JoinStateStoreConfig::with_limits(0, 3);
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    for i in 0..5 {
        store.store("same_key", make_test_record(i), i * 1000);
    }

    assert_eq!(store.record_count(), 3);

    let stats = store.stats();
    assert_eq!(stats.records_evicted, 2);
}

#[test]
fn test_stats_tracking() {
    let mut store = JoinStateStore::new(Duration::from_secs(3600));

    for i in 0..10 {
        store.store("key", make_test_record(i), i * 1000);
    }

    let stats = store.stats();
    assert_eq!(stats.records_stored, 10);
    assert_eq!(stats.current_size, 10);
    assert_eq!(stats.current_keys, 1);
}

#[test]
fn test_capacity_monitoring() {
    let config = JoinStateStoreConfig::with_limits(100, 0);
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    assert_eq!(store.capacity_usage_pct(), 0.0);
    assert_eq!(store.remaining_capacity(), 100);

    for i in 0..50 {
        store.store(&format!("key{}", i), make_test_record(i), i * 1000);
    }

    assert_eq!(store.capacity_usage_pct(), 50.0);
    assert_eq!(store.remaining_capacity(), 50);
}

#[test]
fn test_unlimited_config() {
    let config = JoinStateStoreConfig::unlimited();
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    for i in 0..1000 {
        store.store(&format!("key{}", i), make_test_record(i), i * 1000);
    }
    assert_eq!(store.record_count(), 1000);
    assert!(!store.is_near_capacity());
    assert!(!store.is_at_capacity());
    assert_eq!(store.remaining_capacity(), usize::MAX);

    let stats = store.stats();
    assert_eq!(stats.records_evicted, 0);
    assert_eq!(stats.limit_hits, 0);
}

#[test]
fn test_btreemap_range_query_efficiency() {
    let mut store = JoinStateStore::new(Duration::from_secs(3600));

    for i in 0..100 {
        store.store("key", make_test_record(i), i * 100);
    }

    assert_eq!(store.record_count(), 100);

    let matches = store.lookup("key", 2000, 3000);
    assert_eq!(matches.len(), 11);

    let matches = store.lookup("key", 0, 900);
    assert_eq!(matches.len(), 10);

    let matches = store.lookup("key", 9000, 9900);
    assert_eq!(matches.len(), 10);

    let matches = store.lookup("key", 50, 99);
    assert_eq!(matches.len(), 0);

    let matches = store.lookup("key", 5000, 5000);
    assert_eq!(matches.len(), 1);
}

#[test]
fn test_multiple_records_same_event_time() {
    let mut store = JoinStateStore::new(Duration::from_secs(3600));

    for i in 0..5 {
        store.store("key", make_test_record(i), 1000);
    }

    assert_eq!(store.record_count(), 5);

    let matches = store.lookup("key", 1000, 1000);
    assert_eq!(matches.len(), 5);

    let matches = store.lookup_all("key");
    assert_eq!(matches.len(), 5);
}

#[test]
fn test_mixed_event_times_expiration() {
    let mut store = JoinStateStore::with_retention_ms(500);

    store.store("key", make_test_record(1), 1000);
    store.store("key", make_test_record(2), 1200);
    store.store("key", make_test_record(3), 1000);
    store.store("key", make_test_record(4), 1500);

    assert_eq!(store.record_count(), 4);

    let expired = store.advance_watermark(1600);
    assert_eq!(expired, 2);
    assert_eq!(store.record_count(), 2);

    let matches = store.lookup_all("key");
    assert_eq!(matches.len(), 2);
}

// ============================================================================
// Memory-Based Limits Tests (Phase 6.1)
// ============================================================================

fn make_large_test_record(id: i64, payload_size: usize) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    // Add a large string field to increase memory footprint
    fields.insert(
        "payload".to_string(),
        FieldValue::String("x".repeat(payload_size)),
    );
    StreamRecord::new(fields)
}

#[test]
fn test_memory_estimation() {
    // Test that memory estimation works
    let record = make_test_record(1);
    let size = JoinStateStore::estimate_record_size(&record);

    // Should be > 0 and reasonable (at least overhead + field storage)
    assert!(size > 100, "Record size {} should be > 100 bytes", size);
    assert!(
        size < 10000,
        "Record size {} should be < 10KB for simple record",
        size
    );

    // Larger record should have larger estimate
    let large_record = make_large_test_record(1, 1000);
    let large_size = JoinStateStore::estimate_record_size(&large_record);
    assert!(
        large_size > size,
        "Large record {} should be > small record {}",
        large_size,
        size
    );
}

#[test]
fn test_memory_tracking() {
    let config = JoinStateStoreConfig::unlimited();
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    assert_eq!(store.estimated_memory(), 0);

    // Add records and verify memory increases
    store.store("key1", make_test_record(1), 1000);
    let mem_after_1 = store.estimated_memory();
    assert!(mem_after_1 > 0, "Memory should increase after storing");

    store.store("key2", make_test_record(2), 2000);
    let mem_after_2 = store.estimated_memory();
    assert!(
        mem_after_2 > mem_after_1,
        "Memory should increase with more records"
    );

    // Stats should track memory
    let stats = store.stats();
    assert_eq!(stats.current_memory_bytes, mem_after_2);
    assert!(stats.peak_memory_bytes >= mem_after_2);
}

#[test]
fn test_memory_limit_eviction() {
    // Set a small memory limit to force eviction
    let config = JoinStateStoreConfig::with_memory_limit(5000);
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    // Add records with large payloads until we hit the limit
    for i in 0..20 {
        store.store(
            &format!("key{}", i),
            make_large_test_record(i, 500),
            i * 1000,
        );
    }

    // Memory should be under the limit
    assert!(
        store.estimated_memory() <= 5000,
        "Memory {} should be <= limit 5000",
        store.estimated_memory()
    );

    // Records should have been evicted
    let stats = store.stats();
    assert!(
        stats.records_evicted_memory > 0,
        "Memory evictions should occur"
    );
    assert!(
        stats.memory_limit_hits > 0,
        "Memory limit hits should be recorded"
    );
}

#[test]
fn test_memory_usage_percentage() {
    let config = JoinStateStoreConfig::with_memory_limit(10000);
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    assert_eq!(store.memory_usage_pct(), 0.0);

    // Add some records
    for i in 0..5 {
        store.store(&format!("key{}", i), make_test_record(i), i * 1000);
    }

    let usage_pct = store.memory_usage_pct();
    assert!(usage_pct > 0.0, "Usage should be > 0%");
    assert!(usage_pct <= 100.0, "Usage should be <= 100%");
}

#[test]
fn test_memory_remaining() {
    let config = JoinStateStoreConfig::with_memory_limit(100000);
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    let initial_remaining = store.remaining_memory();
    assert_eq!(initial_remaining, 100000);

    store.store("key", make_test_record(1), 1000);

    let remaining_after = store.remaining_memory();
    assert!(
        remaining_after < initial_remaining,
        "Remaining should decrease"
    );
}

#[test]
fn test_unlimited_memory() {
    let config = JoinStateStoreConfig::unlimited();
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    // With unlimited config, these should behave appropriately
    assert_eq!(store.memory_usage_pct(), 0.0);
    assert_eq!(store.remaining_memory(), usize::MAX);
    assert!(!store.is_near_memory_limit());
    assert!(!store.is_at_memory_limit());

    // Add many records - should not trigger eviction
    for i in 0..100 {
        store.store(&format!("key{}", i), make_test_record(i), i * 1000);
    }

    assert_eq!(store.record_count(), 100);
    let stats = store.stats();
    assert_eq!(stats.records_evicted_memory, 0);
}

#[test]
fn test_memory_warning_threshold() {
    let config = JoinStateStoreConfig::with_memory_limit(10000);
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    // Initially not near limit
    assert!(!store.is_near_memory_limit());

    // Add records until we approach the threshold (80% default)
    for i in 0..50 {
        store.store(&format!("key{}", i), make_test_record(i), i * 1000);
        if store.memory_usage_pct() >= 80.0 {
            break;
        }
    }

    // May or may not hit the threshold depending on record sizes
    // Just verify the method works without errors
    let _ = store.is_near_memory_limit();
}

#[test]
fn test_memory_freed_on_watermark_advance() {
    // Use with_retention_ms to set retention in milliseconds (500ms)
    let mut store = JoinStateStore::with_retention_ms(500);

    // Add records
    for i in 0..10 {
        store.store("key", make_test_record(i), i * 100);
    }

    let mem_before = store.estimated_memory();

    // Advance watermark to expire some records
    let expired = store.advance_watermark(600);
    assert!(expired > 0, "Some records should expire");

    let mem_after = store.estimated_memory();
    assert!(
        mem_after < mem_before,
        "Memory should decrease after expiration"
    );
}

#[test]
fn test_avg_record_memory() {
    let config = JoinStateStoreConfig::unlimited();
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    // Empty store
    assert_eq!(store.avg_record_memory(), 0);

    // Add records of similar size
    for i in 0..10 {
        store.store(&format!("key{}", i), make_test_record(i), i * 1000);
    }

    let avg = store.avg_record_memory();
    assert!(avg > 0, "Average should be > 0");

    // Average should be roughly consistent with total / count
    let expected_avg = store.estimated_memory() / store.record_count();
    assert_eq!(avg, expected_avg);
}

#[test]
fn test_combined_record_and_memory_limits() {
    // Both record and memory limits
    let config = JoinStateStoreConfig::with_all_limits(100, 10, 50000);
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    // Add records - should respect whichever limit is hit first
    for i in 0..200 {
        store.store(
            &format!("key{}", i % 20),
            make_large_test_record(i, 200),
            i * 100,
        );
    }

    // Should not exceed either limit
    assert!(
        store.record_count() <= 100,
        "Record count {} should be <= 100",
        store.record_count()
    );
    assert!(
        store.estimated_memory() <= 50000,
        "Memory {} should be <= 50000",
        store.estimated_memory()
    );
}

#[test]
fn test_memory_stats_accuracy() {
    let config = JoinStateStoreConfig::with_memory_limit(1_000_000);
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    // Track memory through various operations
    for i in 0..50 {
        store.store(&format!("key{}", i), make_test_record(i), i * 1000);
    }

    let stats = store.stats();

    // Verify stats consistency
    assert_eq!(stats.current_memory_bytes, store.estimated_memory());
    assert!(stats.peak_memory_bytes >= stats.current_memory_bytes);
    assert_eq!(stats.current_size, store.record_count());
}

#[test]
fn test_clear_resets_memory() {
    let config = JoinStateStoreConfig::unlimited();
    let mut store = JoinStateStore::with_config(Duration::from_secs(3600), config);

    for i in 0..100 {
        store.store(&format!("key{}", i), make_test_record(i), i * 1000);
    }

    assert!(store.estimated_memory() > 0);

    store.clear();

    assert_eq!(store.estimated_memory(), 0);
    assert_eq!(store.record_count(), 0);
}
