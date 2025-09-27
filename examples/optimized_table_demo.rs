/*!
# OptimizedTableImpl Performance Demo

This example demonstrates the high-performance features of OptimizedTableImpl
including O(1) lookups, query caching, column indexing, and streaming capabilities.

## Features Demonstrated

- **O(1) Key Lookups** - Direct HashMap access vs O(n) full scans
- **Query Plan Caching** - Avoid re-parsing identical queries
- **Column Indexing** - Fast column-based filtering
- **Performance Monitoring** - Built-in statistics and timing
- **Memory Efficiency** - String interning and compact storage
- **Streaming Operations** - Async record processing

## Usage

```bash
cargo run --example optimized_table_demo --no-default-features
```
*/

use futures::StreamExt;
use std::collections::HashMap;
use std::time::Instant;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 OptimizedTableImpl Performance Demo");
    println!("========================================\n");

    let table = OptimizedTableImpl::new();

    // Phase 1: Load test data
    println!("📊 Phase 1: Loading Test Data");
    load_test_data(&table).await?;

    // Phase 2: Demonstrate O(1) key lookups
    println!("\n⚡ Phase 2: O(1) Key Lookup Performance");
    demonstrate_key_lookups(&table).await?;

    // Phase 3: Query plan caching
    println!("\n💾 Phase 3: Query Plan Caching");
    demonstrate_query_caching(&table).await?;

    // Phase 4: Column indexing
    println!("\n📑 Phase 4: Column Indexing Performance");
    demonstrate_column_indexing(&table).await?;

    // Phase 5: Streaming operations
    println!("\n🌊 Phase 5: High-Performance Streaming");
    demonstrate_streaming(&table).await?;

    // Phase 6: Aggregation performance
    println!("\n📈 Phase 6: Optimized Aggregations");
    demonstrate_aggregations(&table).await?;

    // Final statistics
    println!("\n📋 Final Performance Statistics");
    print_final_stats(&table);

    println!("\n✅ Demo Complete! OptimizedTableImpl showcases:");
    println!("   • O(1) key lookups for instant data access");
    println!("   • Query plan caching for repeated queries");
    println!("   • Column indexing for fast filtering");
    println!("   • Memory-efficient string interning");
    println!("   • High-performance async streaming");
    println!("   • Built-in performance monitoring");

    Ok(())
}

async fn load_test_data(table: &OptimizedTableImpl) -> Result<(), Box<dyn std::error::Error>> {
    println!("Loading 10,000 financial records...");
    let start = Instant::now();

    // Insert financial transaction data
    for i in 0..10_000 {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert(
            "account_id".to_string(),
            FieldValue::String(format!("ACC{:04}", i % 100)),
        );
        record.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger((i * 123 + 50000) % 1000000, 2),
        ); // Random amounts
        record.insert(
            "status".to_string(),
            FieldValue::String(match i % 4 {
                0 => "pending".to_string(),
                1 => "completed".to_string(),
                2 => "failed".to_string(),
                _ => "processing".to_string(),
            }),
        );
        record.insert(
            "merchant".to_string(),
            FieldValue::String(format!("MERCHANT_{}", i % 50)),
        ); // 50 merchants

        table.insert(format!("txn_{:05}", i), record)?;
    }

    let duration = start.elapsed();
    println!("✅ Loaded 10,000 records in {:?}", duration);
    println!(
        "   Average: {:.2}μs per record",
        duration.as_micros() as f64 / 10_000.0
    );

    Ok(())
}

async fn demonstrate_key_lookups(
    table: &OptimizedTableImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing O(1) key lookup performance...");

    // Test single key lookup
    let start = Instant::now();
    let result = table.get_record("txn_05000")?;
    let single_lookup_time = start.elapsed();

    println!("✅ Single key lookup: {:?}", single_lookup_time);
    println!("   Found record: {}", result.is_some());

    // Test batch key lookups
    let test_keys = [
        "txn_00001",
        "txn_02500",
        "txn_05000",
        "txn_07500",
        "txn_09999",
    ];
    let start = Instant::now();

    let mut found_count = 0;
    for key in &test_keys {
        if table.contains_key(key) {
            found_count += 1;
        }
    }

    let batch_lookup_time = start.elapsed();
    println!("✅ Batch lookup (5 keys): {:?}", batch_lookup_time);
    println!("   Found: {}/5 records", found_count);
    println!(
        "   Average: {:.2}μs per lookup",
        batch_lookup_time.as_micros() as f64 / 5.0
    );

    Ok(())
}

async fn demonstrate_query_caching(
    table: &OptimizedTableImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing query plan caching performance...");

    let query = "status = 'completed'";

    // First query - cache miss
    let start = Instant::now();
    let values1 = table.sql_column_values("amount", query)?;
    let first_query_time = start.elapsed();

    // Second identical query - cache hit
    let start = Instant::now();
    let values2 = table.sql_column_values("amount", query)?;
    let second_query_time = start.elapsed();

    // Third identical query - cache hit
    let start = Instant::now();
    let values3 = table.sql_column_values("amount", query)?;
    let third_query_time = start.elapsed();

    println!("✅ Query: '{}'", query);
    println!("   First query (cache miss):  {:?}", first_query_time);
    println!("   Second query (cache hit):  {:?}", second_query_time);
    println!("   Third query (cache hit):   {:?}", third_query_time);

    let speedup = first_query_time.as_nanos() as f64 / second_query_time.as_nanos() as f64;
    println!("   Cache speedup: {:.1}x faster", speedup);
    println!(
        "   Results consistent: {}",
        values1.len() == values2.len() && values2.len() == values3.len()
    );

    // Check cache statistics
    let stats = table.get_stats();
    println!(
        "   Cache hits: {}, misses: {}",
        stats.query_cache_hits, stats.query_cache_misses
    );

    Ok(())
}

async fn demonstrate_column_indexing(
    table: &OptimizedTableImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing column indexing performance...");

    // Query using column index
    let start = Instant::now();
    let pending_amounts = table.sql_column_values("amount", "status = 'pending'")?;
    let indexed_query_time = start.elapsed();

    println!("✅ Column-indexed query: status = 'pending'");
    println!("   Query time: {:?}", indexed_query_time);
    println!("   Results found: {} records", pending_amounts.len());

    // Test different status values
    let statuses = ["pending", "completed", "failed", "processing"];
    println!("\n📊 Status distribution:");

    for status in &statuses {
        let query = format!("status = '{}'", status);
        let start = Instant::now();
        let count = table.sql_scalar("COUNT(*)", &query)?;
        let query_time = start.elapsed();

        if let FieldValue::Integer(count_val) = count {
            println!(
                "   {:<12}: {:>4} records ({:?})",
                status, count_val, query_time
            );
        }
    }

    Ok(())
}

async fn demonstrate_streaming(
    table: &OptimizedTableImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing high-performance streaming...");

    // Stream all records
    let start = Instant::now();
    let mut stream = table.stream_all().await?;
    let mut count = 0;

    while let Some(result) = stream.next().await {
        if result.is_ok() {
            count += 1;
        }
        // Process first 100 records for demo
        if count >= 100 {
            break;
        }
    }
    let stream_time = start.elapsed();

    println!("✅ Streamed {} records in {:?}", count, stream_time);
    println!(
        "   Throughput: {:.0} records/sec",
        count as f64 / stream_time.as_secs_f64()
    );

    // Filtered streaming with key lookup optimization
    let start = Instant::now();
    let mut filtered_stream = table.stream_filter("key = 'txn_05000'").await?;
    let mut filtered_count = 0;

    while let Some(result) = filtered_stream.next().await {
        if result.is_ok() {
            filtered_count += 1;
        }
    }
    let filtered_time = start.elapsed();

    println!("✅ Filtered stream (key lookup): {:?}", filtered_time);
    println!("   Found: {} record (O(1) performance)", filtered_count);

    // Batch processing
    let start = Instant::now();
    let batch = table.query_batch(100, Some(0)).await?;
    let batch_time = start.elapsed();

    println!("✅ Batch query (100 records): {:?}", batch_time);
    println!(
        "   Retrieved: {} records, has_more: {}",
        batch.records.len(),
        batch.has_more
    );

    Ok(())
}

async fn demonstrate_aggregations(
    table: &OptimizedTableImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing optimized aggregation performance...");

    // COUNT aggregate with different filters
    let aggregates = [
        ("COUNT(*)", "1=1", "Total records"),
        ("COUNT(*)", "status = 'completed'", "Completed transactions"),
        ("COUNT(*)", "status = 'pending'", "Pending transactions"),
        (
            "SUM(amount)",
            "status = 'completed'",
            "Total completed amount",
        ),
    ];

    for (aggregate_expr, where_clause, description) in &aggregates {
        let start = Instant::now();
        let result = table
            .stream_aggregate(aggregate_expr, Some(where_clause))
            .await?;
        let agg_time = start.elapsed();

        match result {
            FieldValue::Integer(value) => {
                println!("✅ {}: {} ({:?})", description, value, agg_time);
            }
            FieldValue::Null => {
                println!("✅ {}: NULL ({:?})", description, agg_time);
            }
            _ => {
                println!("✅ {}: {:?} ({:?})", description, result, agg_time);
            }
        }
    }

    // Count with column index optimization
    let start = Instant::now();
    let merchant_count = table.stream_count(Some("merchant = 'MERCHANT_25'")).await?;
    let count_time = start.elapsed();

    println!(
        "✅ Merchant count (indexed): {} records ({:?})",
        merchant_count, count_time
    );

    Ok(())
}

fn print_final_stats(table: &OptimizedTableImpl) {
    let stats = table.get_stats();

    println!("📊 Performance Statistics:");
    println!("   Records:              {:>8}", stats.record_count);
    println!("   Total queries:        {:>8}", stats.total_queries);
    println!("   Cache hits:           {:>8}", stats.query_cache_hits);
    println!("   Cache misses:         {:>8}", stats.query_cache_misses);

    if stats.total_queries > 0 {
        let cache_hit_rate = (stats.query_cache_hits as f64 / stats.total_queries as f64) * 100.0;
        println!("   Cache hit rate:       {:>7.1}%", cache_hit_rate);
    }

    println!(
        "   Avg query time:       {:>7.2}ms",
        stats.average_query_time_ms
    );
    println!(
        "   Memory usage:         {:>8} bytes",
        stats.memory_usage_bytes
    );

    // Calculate performance metrics
    if stats.average_query_time_ms > 0.0 {
        let queries_per_second = 1000.0 / stats.average_query_time_ms;
        println!(
            "   Query throughput:     {:>7.0} queries/sec",
            queries_per_second
        );
    }
}
