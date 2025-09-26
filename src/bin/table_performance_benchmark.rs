/*!
# Table Performance Benchmark - OptimizedTableImpl

High-performance benchmarking for OptimizedTableImpl demonstrating:
- O(1) key lookup performance
- Query caching effectiveness
- Column indexing speed
- Streaming throughput
- Memory efficiency

Usage:
```bash
cargo run --bin table_performance_benchmark --no-default-features
```
*/

use futures::StreamExt;
use std::collections::HashMap;
use std::time::Instant;

use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};

#[derive(Debug, Clone)]
struct BenchmarkConfig {
    record_count: usize,
    key_lookups: usize,
    stream_batches: usize,
    batch_size: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            record_count: 100_000,
            key_lookups: 10_000,
            stream_batches: 10,
            batch_size: 1000,
        }
    }
}

impl BenchmarkConfig {
    fn production() -> Self {
        Self {
            record_count: 1_000_000,
            key_lookups: 100_000,
            stream_batches: 100,
            batch_size: 10_000,
        }
    }

    fn quick() -> Self {
        Self {
            record_count: 10_000,
            key_lookups: 1_000,
            stream_batches: 5,
            batch_size: 100,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 OptimizedTableImpl Performance Benchmark");
    println!("===========================================");

    let args: Vec<String> = std::env::args().collect();
    let config = match args.get(1).map(|s| s.as_str()) {
        Some("production") => BenchmarkConfig::production(),
        Some("quick") => BenchmarkConfig::quick(),
        _ => BenchmarkConfig::default(),
    };

    println!("📊 Configuration: {:?}", config);
    println!();

    // Phase 1: Data Loading Benchmark
    let table = benchmark_data_loading(&config).await?;

    // Phase 2: Key Lookup Performance
    benchmark_key_lookups(&table, &config).await?;

    // Phase 3: Query Caching Performance
    benchmark_query_caching(&table, &config).await?;

    // Phase 4: Streaming Performance
    benchmark_streaming(&table, &config).await?;

    // Phase 5: Aggregation Performance
    benchmark_aggregations(&table, &config).await?;

    // Phase 6: Memory Efficiency
    benchmark_memory_efficiency(&table, &config).await?;

    // Final Summary
    print_final_summary(&table, &config);

    Ok(())
}

async fn benchmark_data_loading(
    config: &BenchmarkConfig,
) -> Result<OptimizedTableImpl, Box<dyn std::error::Error>> {
    println!("⏱️  Phase 1: Data Loading Performance");
    println!("   Loading {} records...", config.record_count);

    let table = OptimizedTableImpl::new();
    let start = Instant::now();

    // Load financial test data
    for i in 0..config.record_count {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i as i64));
        record.insert(
            "account_id".to_string(),
            FieldValue::String(format!("ACC{:06}", i % 1000)),
        );
        record.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger((i as i64 * 137 + 50000) % 1000000, 2),
        );
        record.insert(
            "status".to_string(),
            FieldValue::String(match i % 4 {
                0 => "active".to_string(),
                1 => "pending".to_string(),
                2 => "completed".to_string(),
                _ => "cancelled".to_string(),
            }),
        );
        record.insert(
            "category".to_string(),
            FieldValue::String(format!("CAT_{}", i % 10)),
        );

        table.insert(format!("txn_{:08}", i), record)?;
    }

    let duration = start.elapsed();
    let records_per_sec = config.record_count as f64 / duration.as_secs_f64();
    let avg_per_record = duration.as_micros() as f64 / config.record_count as f64;

    println!(
        "   ✅ Loaded {} records in {:?}",
        config.record_count, duration
    );
    println!("   📈 Throughput: {:.0} records/sec", records_per_sec);
    println!("   ⚡ Average: {:.2}μs per record", avg_per_record);
    println!();

    Ok(table)
}

async fn benchmark_key_lookups(
    table: &OptimizedTableImpl,
    config: &BenchmarkConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Phase 2: Key Lookup Performance (O(1))");

    // Single key lookup
    let key = format!("txn_{:08}", config.record_count / 2);
    let start = Instant::now();
    let result = table.get_record(&key)?;
    let single_lookup_time = start.elapsed();

    println!(
        "   ✅ Single lookup: {:?} (found: {})",
        single_lookup_time,
        result.is_some()
    );

    // Batch key lookups
    let start = Instant::now();
    let mut found_count = 0;

    for i in 0..config.key_lookups {
        let key = format!("txn_{:08}", i % config.record_count);
        if table.contains_key(&key) {
            found_count += 1;
        }
    }

    let batch_duration = start.elapsed();
    let lookups_per_sec = config.key_lookups as f64 / batch_duration.as_secs_f64();
    let avg_per_lookup = batch_duration.as_nanos() as f64 / config.key_lookups as f64;

    println!(
        "   ✅ Batch lookups: {} lookups in {:?}",
        config.key_lookups, batch_duration
    );
    println!("   📈 Throughput: {:.0} lookups/sec", lookups_per_sec);
    println!("   ⚡ Average: {:.2}ns per lookup", avg_per_lookup);
    println!("   🎯 Found: {}/{} keys", found_count, config.key_lookups);
    println!();

    Ok(())
}

async fn benchmark_query_caching(
    table: &OptimizedTableImpl,
    _config: &BenchmarkConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("💾 Phase 3: Query Caching Performance");

    let queries = [
        "status = 'active'",
        "status = 'pending'",
        "category = 'CAT_5'",
        "amount > 500000",
    ];

    for query in &queries {
        // First query (cache miss)
        let start = Instant::now();
        let values1 = table.sql_column_values("amount", query)?;
        let first_time = start.elapsed();

        // Second query (cache hit)
        let start = Instant::now();
        let values2 = table.sql_column_values("amount", query)?;
        let second_time = start.elapsed();

        let speedup = first_time.as_nanos() as f64 / second_time.as_nanos() as f64;

        println!("   ✅ Query: '{}'", query);
        println!(
            "      Cache miss:  {:?} ({} results)",
            first_time,
            values1.len()
        );
        println!(
            "      Cache hit:   {:?} ({} results)",
            second_time,
            values2.len()
        );
        println!("      Speedup:     {:.1}x faster", speedup);
    }

    let stats = table.get_stats();
    println!(
        "   📊 Cache stats: {} hits, {} misses",
        stats.query_cache_hits, stats.query_cache_misses
    );
    println!();

    Ok(())
}

async fn benchmark_streaming(
    table: &OptimizedTableImpl,
    config: &BenchmarkConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🌊 Phase 4: Streaming Performance");

    // Stream all records
    let start = Instant::now();
    let mut stream = table.stream_all().await?;
    let mut count = 0;
    let max_stream = std::cmp::min(
        config.record_count,
        config.batch_size * config.stream_batches,
    );

    while let Some(result) = stream.next().await {
        if result.is_ok() {
            count += 1;
        }
        if count >= max_stream {
            break;
        }
    }

    let stream_duration = start.elapsed();
    let records_per_sec = count as f64 / stream_duration.as_secs_f64();

    println!("   ✅ Streamed {} records in {:?}", count, stream_duration);
    println!("   📈 Throughput: {:.0} records/sec", records_per_sec);

    // Batch processing
    let start = Instant::now();
    let batch = table.query_batch(config.batch_size, Some(0)).await?;
    let batch_time = start.elapsed();

    println!(
        "   ✅ Batch query ({} records): {:?}",
        config.batch_size, batch_time
    );
    println!(
        "   📦 Retrieved: {} records, has_more: {}",
        batch.records.len(),
        batch.has_more
    );
    println!();

    Ok(())
}

async fn benchmark_aggregations(
    table: &OptimizedTableImpl,
    _config: &BenchmarkConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("📈 Phase 5: Aggregation Performance");

    let aggregates = [
        ("COUNT(*)", "1=1", "Total records"),
        ("COUNT(*)", "status = 'active'", "Active records"),
        ("COUNT(*)", "status = 'completed'", "Completed records"),
        (
            "SUM(amount)",
            "status = 'completed'",
            "Completed amount sum",
        ),
    ];

    for (expr, where_clause, description) in &aggregates {
        let start = Instant::now();
        let result = table.stream_aggregate(expr, Some(where_clause)).await?;
        let agg_time = start.elapsed();

        match result {
            FieldValue::Integer(value) => {
                println!("   ✅ {}: {} ({:?})", description, value, agg_time);
            }
            FieldValue::Null => {
                println!("   ✅ {}: NULL ({:?})", description, agg_time);
            }
            _ => {
                println!("   ✅ {}: {:?} ({:?})", description, result, agg_time);
            }
        }
    }

    println!();
    Ok(())
}

async fn benchmark_memory_efficiency(
    table: &OptimizedTableImpl,
    _config: &BenchmarkConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("💾 Phase 6: Memory Efficiency Analysis");

    let stats = table.get_stats();
    let records = stats.record_count;
    let memory_mb = stats.memory_usage_bytes as f64 / 1024.0 / 1024.0;
    let bytes_per_record = if records > 0 {
        stats.memory_usage_bytes as f64 / records as f64
    } else {
        0.0
    };

    println!("   📊 Records: {}", records);
    println!(
        "   💾 Memory: {:.2} MB ({} bytes)",
        memory_mb, stats.memory_usage_bytes
    );
    println!("   📦 Bytes per record: {:.1}", bytes_per_record);

    // String interning efficiency
    println!("   🔗 String interning: Enabled (shared repeated values)");
    println!();

    Ok(())
}

fn print_final_summary(table: &OptimizedTableImpl, config: &BenchmarkConfig) {
    println!("📋 Final Performance Summary");
    println!("===========================");

    let stats = table.get_stats();

    println!("🚀 Configuration: {:?}", config);
    println!();

    println!("📊 Performance Metrics:");
    println!("   Records:              {:>10}", stats.record_count);
    println!("   Total queries:        {:>10}", stats.total_queries);
    println!("   Cache hits:           {:>10}", stats.query_cache_hits);
    println!("   Cache misses:         {:>10}", stats.query_cache_misses);

    if stats.total_queries > 0 {
        let cache_hit_rate = (stats.query_cache_hits as f64 / stats.total_queries as f64) * 100.0;
        println!("   Cache hit rate:       {:>9.1}%", cache_hit_rate);
    }

    println!(
        "   Avg query time:       {:>9.2}ms",
        stats.average_query_time_ms
    );

    if stats.average_query_time_ms > 0.0 {
        let queries_per_sec = 1000.0 / stats.average_query_time_ms;
        println!(
            "   Query throughput:     {:>10.0} queries/sec",
            queries_per_sec
        );
    }

    println!();
    println!("✅ OptimizedTableImpl Benchmark Complete!");
    println!("   🎯 Demonstrates O(1) key lookups");
    println!("   ⚡ High-performance query caching");
    println!("   🌊 Efficient async streaming");
    println!("   💾 Memory-optimized string interning");
}
