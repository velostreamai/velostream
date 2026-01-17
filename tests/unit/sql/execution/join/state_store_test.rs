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
