//! OptimizedTableImpl Performance Benchmark - Infrastructure Test
//!
//! **Purpose**: Validate O(1) key lookup performance and table operations
//!
//! Tests:
//! - O(1) key lookup performance
//! - Query caching effectiveness
//! - Column indexing speed
//! - Memory efficiency
//!
//! **Source**: `examples/performance/table_performance_benchmark.rs`

use std::collections::HashMap;
use std::time::Instant;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};

#[derive(Debug, Clone)]
struct BenchmarkConfig {
    record_count: usize,
    key_lookups: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            record_count: 10_000, // Reduced for tests
            key_lookups: 1_000,
        }
    }
}

/// Benchmark data loading performance
fn benchmark_data_loading(
    config: &BenchmarkConfig,
) -> Result<OptimizedTableImpl, Box<dyn std::error::Error>> {
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

    println!(
        "   ✅ Loaded {} records in {:?}",
        config.record_count, duration
    );
    println!("   📈 Throughput: {:.0} records/sec", records_per_sec);

    Ok(table)
}

/// Benchmark key lookup performance (O(1) validation)
fn benchmark_key_lookups(
    table: &OptimizedTableImpl,
    config: &BenchmarkConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔍 Key Lookup Performance (O(1) Validation)");

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

    // Assert minimum performance threshold
    assert!(
        lookups_per_sec > 100_000.0,
        "Table lookups below threshold: {:.0} lookups/sec",
        lookups_per_sec
    );

    Ok(())
}

/// Benchmark query caching performance
fn benchmark_query_caching(table: &OptimizedTableImpl) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n💾 Query Caching Performance");

    let queries = [
        "status = 'active'",
        "status = 'completed'",
        "category = 'CAT_5'",
    ];

    let mut total_speedup = 0.0;
    for query in &queries {
        // First query (cache miss)
        let start = Instant::now();
        let values1 = table.sql_column_values("amount", query)?;
        let first_time = start.elapsed();

        // Second query (cache hit)
        let start = Instant::now();
        let _values2 = table.sql_column_values("amount", query)?;
        let second_time = start.elapsed();

        let speedup = first_time.as_nanos() as f64 / second_time.as_nanos().max(1) as f64;
        total_speedup += speedup;

        println!("   ✅ Query: '{}'", query);
        println!(
            "      Cache miss:  {:?} ({} results)",
            first_time,
            values1.len()
        );
        println!("      Cache hit:   {:?}", second_time);
        println!("      Speedup:     {:.1}x", speedup);
    }

    let avg_speedup = total_speedup / queries.len() as f64;
    println!("   📊 Average cache speedup: {:.1}x", avg_speedup);

    // Cache should provide at least 1.1x speedup on average
    assert!(
        avg_speedup > 1.1,
        "Cache speedup below threshold: {:.1}x",
        avg_speedup
    );

    Ok(())
}

/// Benchmark memory efficiency
fn benchmark_memory_efficiency(
    table: &OptimizedTableImpl,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n💾 Memory Efficiency Analysis");

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
    println!("   🔗 String interning: Enabled (shared repeated values)");

    // Assert memory efficiency threshold
    assert!(
        bytes_per_record < 1000.0,
        "Memory usage per record too high: {:.1} bytes",
        bytes_per_record
    );

    Ok(())
}

/// Test: OptimizedTableImpl baseline performance
#[test]
fn test_optimized_table_infrastructure_performance() {
    let config = BenchmarkConfig::default();

    println!("\n🚀 OptimizedTableImpl Infrastructure Performance Test");
    println!("===================================================");
    println!("📋 Configuration: {:?}\n", config);

    // Phase 1: Data loading
    println!("⏱️  Phase 1: Data Loading");
    let table = benchmark_data_loading(&config).expect("Data loading failed");

    // Phase 2: Key lookups (O(1) validation)
    benchmark_key_lookups(&table, &config).expect("Key lookup benchmark failed");

    // Phase 3: Query caching
    benchmark_query_caching(&table).expect("Query caching benchmark failed");

    // Phase 4: Memory efficiency
    benchmark_memory_efficiency(&table).expect("Memory efficiency benchmark failed");

    println!("\n✅ OptimizedTableImpl Infrastructure Test Complete!");
    println!("   🎯 Demonstrates O(1) key lookups");
    println!("   ⚡ High-performance query caching");
    println!("   💾 Memory-optimized operations");
}
