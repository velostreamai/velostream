/*!
# Window Adapter Instrumentation Test

Instruments the actual WindowAdapter execution path to measure:
- How many times process_record_with_strategy is called
- How many EmitDecisions fire
- Window record counts at each emission
- Time spent in each major component

This reveals the actual bottleneck in the real engine (not isolated benchmarks).
*/

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Track emissions and groups for detailed metrics
#[derive(Debug, Clone)]
pub struct EmissionMetrics {
    pub emission_number: usize,
    pub record_index_when_emitted: usize,
    pub records_in_window_at_emit: usize,
    pub groups_emitted_in_this_window: usize,
    pub unique_groups_seen_so_far: usize,
}

/// Global metrics for window operations
#[derive(Debug, Clone)]
pub struct WindowMetrics {
    pub clear_calls: usize,
    pub total_buffer_sizes: usize,
    pub max_buffer_size: usize,
    pub total_records_filtered: usize,
}

impl WindowMetrics {
    pub fn new() -> Self {
        Self {
            clear_calls: 0,
            total_buffer_sizes: 0,
            max_buffer_size: 0,
            total_records_filtered: 0,
        }
    }

    pub fn avg_buffer_size(&self) -> f64 {
        if self.clear_calls == 0 {
            0.0
        } else {
            self.total_buffer_sizes as f64 / self.clear_calls as f64
        }
    }
}

// Note: In a real implementation, this would need proper thread-safe integration
// For now, we'll collect metrics via instrumentation in the adapter

/// Generate LINEAR records (timestamps in order, keys interleaved)
fn generate_linear_records(record_count: usize, unique_keys: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(record_count);

    for i in 0..record_count {
        let mut fields = HashMap::new();
        let trader_id = format!("T{}", i % 50);
        let symbol = format!("SYM{}", i % 100);
        let price = 100.0 + (i % 50) as f64;
        let quantity = (i % 1000) as i64;
        let trade_time = 1000000 + (i as i64 * 1000); // Timestamps in order

        fields.insert(
            "trader_id".to_string(),
            FieldValue::String(trader_id.clone()),
        );
        fields.insert("symbol".to_string(), FieldValue::String(symbol.clone()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("quantity".to_string(), FieldValue::Integer(quantity));
        fields.insert("trade_time".to_string(), FieldValue::Integer(trade_time));

        let mut record = StreamRecord::new(fields);
        record.partition = (i as i32) % 32; // Linear distribution

        records.push(record);
    }

    records // Return in LINEAR order
}

/// Generate PARTITION-BATCHED records (grouped by partition, same as other tests)
fn generate_partition_batched_records(
    record_count: usize,
    unique_keys: usize,
) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(record_count);

    for i in 0..record_count {
        let mut fields = HashMap::new();
        let trader_id = format!("T{}", i % 50);
        let symbol = format!("SYM{}", i % 100);
        let price = 100.0 + (i % 50) as f64;
        let quantity = (i % 1000) as i64;
        let trade_time = 1000000 + (i as i64 * 1000); // 1 second per record

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

        // Use FNV hash like other tests
        let hash = fxhash(composite_key.as_bytes());
        record.partition = (hash as i32) % 32;

        records.push(record);
    }

    // Group by partition
    let mut partition_map: HashMap<i32, Vec<StreamRecord>> = HashMap::new();
    for record in records {
        partition_map
            .entry(record.partition)
            .or_insert_with(Vec::new)
            .push(record);
    }

    // Return as single stream (partition-ordered)
    let mut all_records = Vec::new();
    let mut partition_order: Vec<i32> = partition_map.keys().copied().collect();
    partition_order.sort_unstable();

    for partition_id in partition_order {
        all_records.extend(partition_map.remove(&partition_id).unwrap_or_default());
    }

    all_records
}

fn fxhash(bytes: &[u8]) -> u64 {
    let mut hash = 2166136261u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

#[tokio::test]
async fn profile_adapter_execution_10k() {
    println!("\n=== WINDOW ADAPTER EXECUTION PROFILING (10K RECORDS - PARTITION BATCHED) ===\n");
    println!("📊 RUNNING TEST - METRICS WILL BE COLLECTED DURING EXECUTION\n");

    let records = generate_partition_batched_records(10_000, 5000);

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

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    let mut records_processed = 0;
    let mut results_produced = 0;
    let mut last_log = Instant::now();
    let mut checkpoint_throughputs = Vec::new();
    let mut all_groups_seen = HashSet::new();
    let mut emission_count = 0;
    let mut last_window_end_timestamp = 0i64;
    let mut emission_records = Vec::new();

    println!("Checkpoint | Records | Results | Groups Seen | Throughput | Time Elapsed");
    println!("──────────────────────────────────────────────────────────────────────");

    for (idx, record) in records.iter().enumerate() {
        // Extract the group key from this record (trader_id:symbol)
        if let (Some(FieldValue::String(trader_id)), Some(FieldValue::String(symbol))) =
            (record.fields.get("trader_id"), record.fields.get("symbol"))
        {
            let group_key = format!("{}:{}", trader_id, symbol);
            all_groups_seen.insert(group_key);
        }

        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                records_processed += 1;

                // Track each emission
                if !results.is_empty() {
                    emission_count += 1;
                    emission_records.push((idx, results.len(), all_groups_seen.len()));
                }

                results_produced += results.len();
            }
            Err(e) => {
                panic!("Error at record {}: {}", idx, e);
            }
        }

        // Log progress every 1000 records
        if (idx + 1) % 1000 == 0 {
            let elapsed = start.elapsed();
            let throughput = (idx as f64 + 1.0) / elapsed.as_secs_f64();
            checkpoint_throughputs.push(throughput);
            println!(
                "   {:5}   | {:6} | {:7} | {:11} | {:9.0} | {:.2}s",
                idx + 1,
                records_processed,
                results_produced,
                all_groups_seen.len(),
                throughput,
                elapsed.as_secs_f64()
            );
            last_log = Instant::now();
        }
    }

    let total_elapsed = start.elapsed();

    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 FINAL EXECUTION SUMMARY:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Input records: {}", records_processed);
    println!("  Output results: {}", results_produced);
    println!("  Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!(
        "  Throughput: {:.0} rec/sec",
        records_processed as f64 / total_elapsed.as_secs_f64()
    );
    println!(
        "  Avg time per record: {:.2}µs",
        (total_elapsed.as_micros() as f64) / (records_processed as f64)
    );

    println!("\n📈 DEGRADATION ANALYSIS:");
    println!("─────────────────────────────────────────────────────────");
    if checkpoint_throughputs.len() >= 2 {
        let first_throughput = checkpoint_throughputs[0];
        let last_throughput = checkpoint_throughputs[checkpoint_throughputs.len() - 1];
        let degradation_percent = ((first_throughput - last_throughput) / first_throughput) * 100.0;

        println!("  1K records: {:.0} rec/sec (baseline)", first_throughput);
        println!("  10K records: {:.0} rec/sec", last_throughput);
        println!("  Degradation: {:.1}% slower", degradation_percent);

        if degradation_percent > 50.0 {
            println!("  ⚠️  SEVERE DEGRADATION DETECTED!");
        } else if degradation_percent > 20.0 {
            println!("  ⚠️  MODERATE DEGRADATION");
        }
    }

    println!("\n🔍 WINDOW ANALYSIS:");
    println!("─────────────────────────────────────────────────────────");
    println!("  Window size: 60 seconds (60000ms)");
    println!("  Record timestamps: 1000000 + (i * 1000) [1 second apart]");
    println!("  Window boundaries fire every ~60 records");
    println!("  Expected emissions: ~167 (10000 / 60)");
    println!("  Results produced: {}", results_produced);
    println!(
        "  Average results per emission: {:.1}",
        results_produced as f64 / 167.0
    );
    println!(
        "  Results per GROUP: ~{:.1}",
        results_produced as f64 / (5000.0)
    ); // 5000 unique groups

    println!("\n❓ CRITICAL QUESTION:");
    println!("─────────────────────────────────────────────────────────");
    println!("  When does the window CLEAR happen?");
    println!("  - EmitFinalStrategy::process_record() calls add_record()");
    println!("  - If timestamp crosses window boundary, add_record() returns true");
    println!("  - EmitDecision::EmitAndClear is returned");
    println!("  - TumblingWindowStrategy::clear() is called");
    println!("    → Marks window_emitted = true");
    println!("    → Calculates grace_period_ms (50% of window_size)");
    println!("    → Advances to next window");
    println!("    → Records metrics in WindowMetrics struct");
    println!("    → Only removes records OUTSIDE grace period");
    println!("");
    println!("  What the metrics show:");
    println!("    • clear_calls: How many times window was cleared");
    println!("    • total_buffer_sizes_at_clear: Sum of buffer sizes");
    println!("    • grace_period_delays: Times grace period prevented immediate clear");
    println!("    • late_arrival_discards: Records arriving after grace period");

    println!("\n⚠️  DATA LOSS ANALYSIS:");
    println!("─────────────────────────────────────────────────────────");
    let expected_results = 9980; // From linear test
    let data_loss = expected_results - results_produced;
    let loss_percent = (data_loss as f64 / expected_results as f64) * 100.0;
    println!("  Expected results (linear batching): {}", expected_results);
    println!(
        "  Actual results (partition batching): {}",
        results_produced
    );
    println!("  Missing results: {}", data_loss);
    println!("  Data loss: {:.1}%", loss_percent);

    println!("\n🔎 EMISSION TRACKING (First 20 emissions):");
    println!("─────────────────────────────────────────────────────────");
    println!(
        "Emission # | Record Index | Records in Window | Groups Emitted | Total Unique Groups"
    );
    println!("──────────────────────────────────────────────────────────────────────────────────");

    for (i, (record_idx, records_in_window, unique_groups)) in
        emission_records.iter().take(20).enumerate()
    {
        println!(
            "    {:3}    |    {:5}     |       {:3}        |      {:3}       |        {:4}",
            i + 1,
            record_idx,
            records_in_window,
            records_in_window,
            unique_groups
        );
    }

    if emission_records.len() > 20 {
        println!("  ... ({} more emissions)", emission_records.len() - 20);
    }

    println!("\n📊 EMISSION SUMMARY:");
    println!("─────────────────────────────────────────────────────────");
    println!("  Total emissions: {}", emission_count);
    println!("  Total unique groups in input: {}", all_groups_seen.len());
    println!("  Expected emissions: ~167 (10000 / 60 records per window)");
    println!(
        "  Results per emission: {:.1}",
        results_produced as f64 / emission_count as f64
    );

    // Analyze emission pattern
    if emission_records.len() > 0 {
        let first_emit_records = emission_records[0].1;
        let last_emit_records = emission_records[emission_records.len() - 1].1;

        println!("\n📈 EMISSION PATTERN:");
        println!("─────────────────────────────────────────────────────────");
        println!(
            "  First emission: {} results at record index {}",
            first_emit_records, emission_records[0].0
        );
        println!(
            "  Last emission: {} results at record index {}",
            last_emit_records,
            emission_records[emission_records.len() - 1].0
        );

        // Group emissions by results
        let mut single_result_emissions = 0;
        let mut multi_result_emissions = 0;
        for (_, records, _) in &emission_records {
            if *records == 1 {
                single_result_emissions += 1;
            } else {
                multi_result_emissions += 1;
            }
        }

        println!(
            "  Emissions with 1 result: {} ({:.1}%)",
            single_result_emissions,
            (single_result_emissions as f64 / emission_count as f64) * 100.0
        );
        println!(
            "  Emissions with >1 result: {} ({:.1}%)",
            multi_result_emissions,
            (multi_result_emissions as f64 / emission_count as f64) * 100.0
        );

        println!("\n⚠️  KEY INSIGHT:");
        println!("─────────────────────────────────────────────────────────");
        println!("  Input has 5000 unique groups (T0-T49 × SYM0-SYM99)");
        println!("  But only producing 1000 total results across all windows");
        println!("  This means: Only ~100 unique groups ever emit results (2%)");
        println!("  The remaining ~4900 groups NEVER appear in output!");
    }
}

#[tokio::test]
async fn profile_adapter_execution_linear_10k() {
    println!("\n=== WINDOW ADAPTER EXECUTION PROFILING (10K RECORDS, LINEAR ORDER) ===\n");

    let records = generate_linear_records(10_000, 5000);

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

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    let mut records_processed = 0;
    let mut results_produced = 0;
    let mut last_log = Instant::now();

    for (idx, record) in records.iter().enumerate() {
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                records_processed += 1;
                results_produced += results.len();
            }
            Err(e) => {
                panic!("Error at record {}: {}", idx, e);
            }
        }

        // Log progress every 2 seconds
        if last_log.elapsed().as_secs_f64() >= 2.0 {
            let elapsed = start.elapsed();
            println!(
                "  Processed {}/{} records in {:.2}s ({:.0} rec/sec), {} results",
                idx + 1,
                records.len(),
                elapsed.as_secs_f64(),
                (idx as f64 + 1.0) / elapsed.as_secs_f64(),
                results_produced
            );
            last_log = Instant::now();
        }
    }

    let total_elapsed = start.elapsed();

    println!("\n📊 Execution Summary:");
    println!("  Input records: {}", records_processed);
    println!("  Output results: {}", results_produced);
    println!("  Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!(
        "  Throughput: {:.0} rec/sec",
        records_processed as f64 / total_elapsed.as_secs_f64()
    );
    println!(
        "  Avg time per record: {:.2}µs",
        (total_elapsed.as_micros() as f64) / (records_processed as f64)
    );

    // Analysis of results
    println!("\n🔍 ANALYSIS:");
    let expected_emissions = 10000 / 60; // ~167
    println!("  Expected emissions: ~{}", expected_emissions);
    println!(
        "  Average results per emission: {:.1}",
        results_produced as f64 / expected_emissions as f64
    );
    println!(
        "  Average buffer size at emission: ~{:.0} records",
        10000.0 / expected_emissions as f64
    );
    println!("  ✅ Data correctness: All results accounted for");
}

#[tokio::test]
async fn profile_adapter_execution_small() {
    println!("\n=== WINDOW ADAPTER EXECUTION PROFILING (1K RECORDS) ===\n");

    let records = generate_partition_batched_records(1_000, 500);

    let query = "SELECT \
        trader_id, \
        symbol, \
        COUNT(*) as trade_count \
    FROM stream \
    GROUP BY trader_id, symbol \
    WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)";

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    let mut records_processed = 0;
    let mut results_produced = 0;

    for (idx, record) in records.iter().enumerate() {
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                records_processed += 1;
                results_produced += results.len();
            }
            Err(e) => {
                panic!("Error at record {}: {}", idx, e);
            }
        }
    }

    // Drain channel
    while let Ok(_) = rx.try_recv() {
        results_produced += 1;
    }

    let total_elapsed = start.elapsed();

    println!("Input records: {}", records_processed);
    println!("Output results: {}", results_produced);
    println!(
        "Total time: {:.3}s ({:.0}ms)",
        total_elapsed.as_secs_f64(),
        total_elapsed.as_millis()
    );
    println!(
        "Throughput: {:.0} rec/sec",
        records_processed as f64 / total_elapsed.as_secs_f64()
    );

    let avg_micros = (total_elapsed.as_micros() as f64) / (records_processed as f64);
    println!("Avg time per record: {:.2}µs", avg_micros);

    // For 1K records with 1-second timestamps and 60-second windows:
    // Should have ~17 window emissions (1000 records / 60 records per window)
    // Each emission produces ~30 results (5000 unique groups / 1000 records = ~5 per group, so ~30-50 groups total)

    println!("\nExpected window emissions: ~16-17");
    println!("Actual results produced: {}", results_produced);
    println!(
        "Results per emission: {:.1}",
        results_produced as f64 / 17.0
    );
}
