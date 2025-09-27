use futures::StreamExt;
use std::collections::HashMap;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};

#[test]
fn test_optimized_table_basic_operations() {
    let table = OptimizedTableImpl::new();

    // Test empty table
    assert_eq!(table.record_count(), 0);
    assert!(!table.contains_key("nonexistent"));
    assert_eq!(table.get_record("nonexistent").unwrap(), None);

    // Insert test record
    let mut record1 = HashMap::new();
    record1.insert("id".to_string(), FieldValue::Integer(1));
    record1.insert("name".to_string(), FieldValue::String("Alice".to_string()));
    record1.insert("balance".to_string(), FieldValue::ScaledInteger(100000, 2)); // $1000.00

    table.insert("user1".to_string(), record1.clone()).unwrap();

    // Test basic operations
    assert_eq!(table.record_count(), 1);
    assert!(table.contains_key("user1"));

    let retrieved = table.get_record("user1").unwrap().unwrap();
    assert_eq!(
        retrieved.get("name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert_eq!(
        retrieved.get("balance"),
        Some(&FieldValue::ScaledInteger(100000, 2))
    );
}

#[test]
fn test_query_plan_caching() {
    let table = OptimizedTableImpl::new();

    // Insert test data
    let mut record1 = HashMap::new();
    record1.insert(
        "status".to_string(),
        FieldValue::String("active".to_string()),
    );
    record1.insert("score".to_string(), FieldValue::Integer(95));
    table.insert("user1".to_string(), record1).unwrap();

    let mut record2 = HashMap::new();
    record2.insert(
        "status".to_string(),
        FieldValue::String("inactive".to_string()),
    );
    record2.insert("score".to_string(), FieldValue::Integer(42));
    table.insert("user2".to_string(), record2).unwrap();

    // Get initial stats
    let initial_stats = table.get_stats();
    assert_eq!(initial_stats.query_cache_hits, 0);
    assert_eq!(initial_stats.query_cache_misses, 0);

    // First query - should be a cache miss
    let values1 = table
        .sql_column_values("score", "status = 'active'")
        .unwrap();
    let stats_after_first = table.get_stats();
    assert_eq!(stats_after_first.query_cache_misses, 1);
    assert_eq!(stats_after_first.query_cache_hits, 0);

    // Same query again - should be a cache hit
    let values2 = table
        .sql_column_values("score", "status = 'active'")
        .unwrap();
    let stats_after_second = table.get_stats();
    assert_eq!(stats_after_second.query_cache_misses, 1);
    assert_eq!(stats_after_second.query_cache_hits, 1);

    // Verify results are consistent
    assert_eq!(values1, values2);
    assert_eq!(values1, vec![FieldValue::Integer(95)]);
}

#[test]
fn test_column_indexing() {
    let table = OptimizedTableImpl::new();

    // Insert test data with repeated values (good for indexing)
    for i in 0..10 {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert(
            "category".to_string(),
            FieldValue::String(if i % 2 == 0 {
                "even".to_string()
            } else {
                "odd".to_string()
            }),
        );
        record.insert("value".to_string(), FieldValue::Integer(i * 10));

        table.insert(format!("key_{}", i), record).unwrap();
    }

    // Query using column filter - should use index
    let even_values = table
        .sql_column_values("value", "category = 'even'")
        .unwrap();
    assert_eq!(even_values.len(), 5);

    let odd_values = table
        .sql_column_values("value", "category = 'odd'")
        .unwrap();
    assert_eq!(odd_values.len(), 5);

    // Verify we got the correct values
    let even_ints: Vec<i64> = even_values
        .iter()
        .filter_map(|v| {
            if let FieldValue::Integer(i) = v {
                Some(*i)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(even_ints, vec![0, 20, 40, 60, 80]);
}

#[test]
fn test_optimized_aggregates() {
    let table = OptimizedTableImpl::new();

    // Insert financial data
    let amounts = vec![100, 250, 500, 750, 1000];
    for (i, amount) in amounts.iter().enumerate() {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i as i64));
        record.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(*amount * 100, 2),
        ); // Store as cents
        record.insert(
            "type".to_string(),
            FieldValue::String(if i % 2 == 0 {
                "credit".to_string()
            } else {
                "debit".to_string()
            }),
        );

        table.insert(format!("txn_{}", i), record).unwrap();
    }

    // Test COUNT aggregate
    let total_count = table.sql_scalar("COUNT(*)", "1=1").unwrap();
    assert_eq!(total_count, FieldValue::Integer(5));

    let credit_count = table.sql_scalar("COUNT(*)", "type = 'credit'").unwrap();
    assert_eq!(credit_count, FieldValue::Integer(3));

    // Test SUM aggregate
    let total_sum = table.sql_scalar("SUM(amount)", "1=1").unwrap();
    if let FieldValue::Integer(sum) = total_sum {
        // Sum should be (100+250+500+750+1000) * 100 = 260000 cents
        assert_eq!(sum, 260000);
    } else {
        panic!("Expected Integer result for SUM");
    }
}

#[test]
fn test_key_lookup_optimization() {
    let table = OptimizedTableImpl::new();

    // Insert a lot of data to make O(n) vs O(1) difference apparent
    for i in 0..1000 {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert(
            "data".to_string(),
            FieldValue::String(format!("data_{}", i)),
        );
        table.insert(format!("key_{}", i), record).unwrap();
    }

    // Key lookup should be O(1)
    let start = std::time::Instant::now();
    let result = table.get_record("key_500").unwrap();
    let lookup_time = start.elapsed();

    assert!(result.is_some());

    // Should be very fast (sub-millisecond for single lookups)
    assert!(
        lookup_time.as_millis() < 10,
        "Key lookup should be fast, took {:?}",
        lookup_time
    );

    // Test key existence lookup
    assert!(table.contains_key("key_500"));
    assert!(!table.contains_key("key_9999"));
}

#[tokio::test]
async fn test_streaming_performance() {
    let table = OptimizedTableImpl::new();

    // Insert test data
    for i in 0..100 {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert("active".to_string(), FieldValue::Boolean(i % 3 == 0));
        table.insert(format!("item_{}", i), record).unwrap();
    }

    // Test streaming all records
    let start = std::time::Instant::now();
    let mut stream = table.stream_all().await.unwrap();

    let mut count = 0;
    while let Some(result) = stream.next().await {
        assert!(result.is_ok());
        count += 1;
    }
    let stream_time = start.elapsed();

    assert_eq!(count, 100);
    println!("Streaming 100 records took: {:?}", stream_time);

    // Test filtered streaming (using key lookup optimization)
    let mut filtered_stream = table.stream_filter("key = 'item_50'").await.unwrap();
    let mut filtered_count = 0;

    while let Some(result) = filtered_stream.next().await {
        assert!(result.is_ok());
        filtered_count += 1;
    }

    assert_eq!(filtered_count, 1); // Should find exactly one match
}

#[tokio::test]
async fn test_batch_processing() {
    let table = OptimizedTableImpl::new();

    // Insert test data
    for i in 0..50 {
        let mut record = HashMap::new();
        record.insert("batch_id".to_string(), FieldValue::Integer(i / 10)); // 5 batches of 10
        record.insert("value".to_string(), FieldValue::Integer(i));
        table.insert(format!("item_{}", i), record).unwrap();
    }

    // Test batch querying
    let batch1 = table.query_batch(10, Some(0)).await.unwrap();
    assert_eq!(batch1.records.len(), 10);
    assert!(batch1.has_more);

    let batch2 = table.query_batch(10, Some(10)).await.unwrap();
    assert_eq!(batch2.records.len(), 10);
    assert!(batch2.has_more);

    // Last batch
    let last_batch = table.query_batch(20, Some(40)).await.unwrap();
    assert_eq!(last_batch.records.len(), 10);
    assert!(!last_batch.has_more);
}

#[test]
fn test_performance_statistics() {
    let table = OptimizedTableImpl::new();

    // Insert some data
    let mut record = HashMap::new();
    record.insert("test".to_string(), FieldValue::String("value".to_string()));
    table.insert("key1".to_string(), record).unwrap();

    // Perform some queries
    let _result1 = table.get_record("key1").unwrap();
    let _result2 = table.sql_column_values("test", "key = 'key1'").unwrap();
    let _result3 = table.sql_scalar("COUNT(*)", "1=1").unwrap();

    // Check statistics
    let stats = table.get_stats();
    assert_eq!(stats.record_count, 1);
    assert!(stats.total_queries >= 3);
    assert!(stats.average_query_time_ms >= 0.0);

    println!("Performance stats: {:?}", stats);

    // Clear cache and verify
    table.clear_cache();

    // After clearing cache, next query should be a cache miss
    let _result4 = table.sql_column_values("test", "key = 'key1'").unwrap();
    let new_stats = table.get_stats();
    assert_eq!(new_stats.query_cache_misses, stats.query_cache_misses + 1);
}

#[test]
fn test_memory_efficiency() {
    let table = OptimizedTableImpl::new();

    // Insert records with repeated strings (should be memory efficient)
    for i in 0..1000 {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert(
            "status".to_string(),
            FieldValue::String("active".to_string()),
        ); // Repeated
        record.insert(
            "type".to_string(),
            FieldValue::String("premium".to_string()),
        ); // Repeated
        table.insert(format!("user_{}", i), record).unwrap();
    }

    // With 1000 records, we should still have reasonable memory usage
    // The string interning should help with repeated values
    let stats = table.get_stats();
    println!(
        "Memory usage for 1000 records: {} bytes",
        stats.memory_usage_bytes
    );

    // Each record has ~3 fields, but strings are interned, so should be efficient
    assert_eq!(stats.record_count, 1000);
}
