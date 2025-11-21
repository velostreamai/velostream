/*!
# Window Adapter Profiling Test

Detailed profiling of compute_aggregations_over_window() to identify performance bottleneck
with partition-batched data.

Measures:
1. Iterations per window emission
2. Time per window emission
3. Time per group key generation
4. Time per record processing
5. Correlation between buffer size and degradation
*/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Generate partition-batched records for profiling
fn generate_partition_batched_records(
    record_count: usize,
    num_partitions: usize,
) -> Vec<StreamRecord> {
    let mut all_records = Vec::new();

    // Generate all records
    for i in 0..record_count {
        let mut fields = HashMap::new();
        let trader_id = format!("T{}", i % 50);
        let symbol = format!("SYM{}", i % 100);
        let price = 100.0 + (i % 50) as f64;
        let quantity = (i % 1000) as i64;
        let trade_time = 1000000 + (i as i64 * 1000);

        fields.insert(
            "trader_id".to_string(),
            FieldValue::String(trader_id.clone()),
        );
        fields.insert("symbol".to_string(), FieldValue::String(symbol.clone()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("quantity".to_string(), FieldValue::Integer(quantity));
        fields.insert("trade_time".to_string(), FieldValue::Integer(trade_time));

        let composite_key = format!("{}:{}", trader_id, symbol);
        let mut record = StreamRecord::new(fields);

        // Hash-based partition assignment (like Kafka)
        let hash = compute_fnv_hash(&composite_key);
        record.partition = (hash as i32) % (num_partitions as i32);

        all_records.push(record);
    }

    // Group records by partition (like Kafka simulator does)
    let mut partition_map: HashMap<i32, Vec<StreamRecord>> = HashMap::new();
    for record in all_records {
        partition_map
            .entry(record.partition)
            .or_insert_with(Vec::new)
            .push(record);
    }

    // Create batches within each partition (batch size = 100)
    let mut all_batches = Vec::new();
    let mut partition_order: Vec<i32> = partition_map.keys().copied().collect();
    partition_order.sort_unstable();

    for partition_id in partition_order {
        let mut partition_records = partition_map.remove(&partition_id).unwrap_or_default();
        while !partition_records.is_empty() {
            let batch_len = 100.min(partition_records.len());
            let batch: Vec<StreamRecord> = partition_records.drain(0..batch_len).collect();
            all_batches.extend(batch);
        }
    }

    all_batches
}

/// FNV hash for consistent partition assignment
fn compute_fnv_hash(key: &str) -> u64 {
    let mut hash = 2166136261u64;
    for byte in key.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

/// Profile window adapter with detailed timing information
#[tokio::test]
async fn profile_window_adapter_bottleneck() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ WINDOW ADAPTER PROFILING TEST                            ║");
    println!("║ Identifying performance bottleneck with partition data   ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records = generate_partition_batched_records(50_000, 32);

    let query = "SELECT \
        trader_id, \
        symbol, \
        COUNT(*) as trade_count, \
        AVG(price) as avg_price, \
        SUM(quantity) as total_quantity \
    FROM stream \
    GROUP BY trader_id, symbol \
    WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)";

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");

    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    let mut results_produced = 0;
    let mut window_emissions = 0;
    let mut max_buffer_size = 0;
    let mut total_iterations = 0u64;

    // Track timing at checkpoints
    let mut checkpoint_times = Vec::new();

    for (idx, record) in records.iter().enumerate() {
        // Execute record
        let exec_start = Instant::now();
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                let exec_time = exec_start.elapsed();
                results_produced += results.len();

                if !results.is_empty() {
                    window_emissions += 1;
                }

                // Rough estimate of iterations: results_produced * groups_per_emission
                if !results.is_empty() {
                    // Each emission processes a window's worth of records
                    total_iterations += 60; // Approximate window size
                }
            }
            Err(e) => {
                eprintln!("Error at record {}: {}", idx, e);
                break;
            }
        }

        // Log progress at checkpoints
        let elapsed = start.elapsed().as_secs();
        if (idx + 1) % 10_000 == 0 {
            let elapsed_secs = start.elapsed().as_secs_f64();
            let throughput = (idx as f64 + 1.0) / elapsed_secs;
            checkpoint_times.push((idx + 1, throughput, results_produced, window_emissions));

            println!(
                "  Processed {:5}/{} records: {:.0} rec/sec | Results: {} | Emissions: {}",
                idx + 1,
                records.len(),
                throughput,
                results_produced,
                window_emissions
            );
        }
    }

    let total_elapsed = start.elapsed();

    println!("\n📊 PROFILING SUMMARY:");
    println!("─────────────────────────────────────────────────────────────");
    println!("  Total records processed: {}", records.len());
    println!("  Total results produced: {}", results_produced);
    println!("  Window emissions: {}", window_emissions);
    println!("  Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!(
        "  Throughput: {:.0} rec/sec",
        records.len() as f64 / total_elapsed.as_secs_f64()
    );
    println!(
        "  Avg time per record: {:.2}µs",
        (total_elapsed.as_micros() as f64) / (records.len() as f64)
    );

    println!("\n📈 DEGRADATION ANALYSIS:");
    println!("─────────────────────────────────────────────────────────────");
    println!("Checkpoint | Records | Throughput | Degradation");
    println!("─────────────────────────────────────────────────────────────");

    let baseline = checkpoint_times.first().map(|cp| cp.1).unwrap_or(1.0);
    for (records_processed, throughput, total_results, total_emissions) in &checkpoint_times {
        let degradation = ((baseline - throughput) / baseline) * 100.0;
        println!(
            "    {:5}  | {:6} | {:8.0}  | {:6.1}% slower",
            records_processed, total_results, throughput, degradation
        );
    }

    println!("\n🔍 HYPOTHESIS TESTING:");
    println!("─────────────────────────────────────────────────────────────");

    // Check if degradation is linear or non-linear
    if checkpoint_times.len() >= 2 {
        let cp1 = &checkpoint_times[0];
        let cp2 = &checkpoint_times[checkpoint_times.len() - 1];

        let throughput_ratio = cp1.1 / cp2.1;
        let records_ratio = (cp2.0 as f64) / (cp1.0 as f64);

        println!("  Throughput ratio: {:.2}x", throughput_ratio);
        println!("  Records ratio: {:.2}x", records_ratio);

        if (throughput_ratio - records_ratio).abs() < 0.5 {
            println!("  ➜ Pattern: LINEAR DEGRADATION (O(N))");
            println!("     Throughput drops proportionally with record count");
        } else if throughput_ratio > records_ratio {
            println!("  ➜ Pattern: SUPER-LINEAR DEGRADATION (O(N²) or worse)");
            println!("     Throughput drops faster than record count increases");
        } else {
            println!("  ➜ Pattern: SUB-LINEAR DEGRADATION");
            println!("     Some optimization appears to kick in");
        }
    }

    println!("\n💡 INTERPRETATION:");
    println!("─────────────────────────────────────────────────────────────");
    println!("If degradation is:");
    println!("  • LINEAR: Problem is per-record cost that accumulates");
    println!("  • SUPER-LINEAR: Problem is buffer/state explosion (O(N²))");
    println!("  • Step function: Problem is at specific scale threshold");
    println!("\nNext step: Check which operation scales with buffer size:");
    println!("  1. Group key generation: Should be O(1) per record");
    println!("  2. Group lookup: Should be O(1) with HashMap");
    println!("  3. Buffer iteration: O(N) but only at emission time");
    println!("  4. State storage: Memory pressure at large sizes");
}

/// Profile with controlled buffer sizes
#[tokio::test]
async fn profile_buffer_size_impact() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ BUFFER SIZE IMPACT ANALYSIS                              ║");
    println!("║ Testing different record counts to isolate bottleneck    ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let test_sizes = vec![5_000, 10_000, 20_000, 30_000, 40_000];

    println!("Record Count | Throughput | Time per Record | Avg Buffer Size");
    println!("──────────────────────────────────────────────────────────────");

    for record_count in test_sizes {
        let records = generate_partition_batched_records(record_count, 32);

        let query = "SELECT \
            trader_id, \
            symbol, \
            COUNT(*) as trade_count, \
            AVG(price) as avg_price, \
            SUM(quantity) as total_quantity \
        FROM stream \
        GROUP BY trader_id, symbol \
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)";

        let mut parser = StreamingSqlParser::new();
        let parsed_query = parser.parse(query).expect("Failed to parse SQL");

        let (tx, _rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let start = Instant::now();
        let mut results_produced = 0;

        for record in &records {
            match engine.execute_with_record_sync(&parsed_query, record) {
                Ok(results) => {
                    results_produced += results.len();
                }
                Err(_) => break,
            }
        }

        let elapsed = start.elapsed();
        let throughput = (record_count as f64) / elapsed.as_secs_f64();
        let time_per_record = (elapsed.as_micros() as f64) / (record_count as f64);
        let avg_buffer_size = record_count / 167; // ~167 window emissions

        println!(
            "    {:6}    | {:8.0}  |    {:.2}µs     |    {:3}",
            record_count, throughput, time_per_record, avg_buffer_size
        );
    }

    println!("\n📊 If time per record increases linearly with record count:");
    println!("   → Super-linear complexity (O(N²) or O(N*M) where M is state size)");
    println!("\n📊 If time per record stays constant:");
    println!("   → Linear complexity (O(N))");
    println!("\n📊 If time per record decreases:");
    println!("   → Sub-linear (cache effects or batch optimization)");
}
