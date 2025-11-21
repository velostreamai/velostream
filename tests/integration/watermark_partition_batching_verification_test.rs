/*!
# Watermark Implementation Verification Test

Tests whether the watermark implementation fixes the partition-batched data issues:
- 90% data loss (1,000 results instead of 9,980)
- 87% performance degradation (8,527 → 1,082 rec/sec)

This test verifies that with watermark tracking and late firing mechanism:
1. All 9,980 results are produced (0% data loss)
2. Performance degrades much less (expect ~50,000+ rec/sec)
3. Late-arriving groups from different partitions are properly aggregated
*/

use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Generate partition-batched records (same as comprehensive_baseline_comparison)
fn generate_partition_batched_records(record_count: usize) -> Vec<StreamRecord> {
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

        // Use FNV hash like KafkaSimulatorDataSource
        let hash = compute_fnv_hash(&composite_key);
        record.partition = (hash as i32) % 32;

        all_records.push(record);
    }

    // Group records by partition (like KafkaSimulatorDataSource does)
    let mut partition_map: HashMap<i32, Vec<StreamRecord>> = HashMap::new();
    for record in all_records {
        partition_map
            .entry(record.partition)
            .or_insert_with(Vec::new)
            .push(record);
    }

    // Flatten back to sequential order (partition-batched delivery)
    // This simulates how Kafka delivers records: all of partition 0, then all of partition 1, etc.
    let mut partition_order: Vec<i32> = partition_map.keys().copied().collect();
    partition_order.sort_unstable();

    let mut result = Vec::new();
    for partition_id in partition_order {
        if let Some(mut partition_records) = partition_map.remove(&partition_id) {
            result.append(&mut partition_records);
        }
    }

    result
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

#[tokio::test]
async fn test_watermark_fixes_partition_batching_data_loss() {
    println!("\n=== Watermark Implementation Verification Test ===");
    println!("Testing partition-batched data with 10K records (50 traders, 100 symbols)");
    println!("");

    let all_records = generate_partition_batched_records(10000);

    // Scenario 4 query: GROUP BY + TUMBLING WINDOW
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

    // Track which groups we see
    let mut groups_seen = HashSet::new();
    let mut results_produced = 0;

    let start = Instant::now();
    for (idx, record) in all_records.iter().enumerate() {
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                results_produced += results.len();

                // Track which groups emitted results
                for result in &results {
                    if let (Some(FieldValue::String(trader_id)), Some(FieldValue::String(symbol))) =
                        (result.fields.get("trader_id"), result.fields.get("symbol"))
                    {
                        let group_key = format!("{}:{}", trader_id, symbol);
                        groups_seen.insert(group_key);
                    }
                }
            }
            Err(e) => {
                panic!("Error processing record {}: {}", idx, e);
            }
        }

        // Log progress every 2000 records
        if (idx + 1) % 2000 == 0 {
            let elapsed = start.elapsed();
            println!(
                "  Processed {}/{} records in {:.2}s ({:.0} rec/sec)",
                idx + 1,
                all_records.len(),
                elapsed.as_secs_f64(),
                (idx as f64 + 1.0) / elapsed.as_secs_f64()
            );
        }
    }

    // Drain channel for final results
    while let Ok(_) = rx.try_recv() {
        results_produced += 1;
    }

    let elapsed = start.elapsed();
    let throughput = all_records.len() as f64 / elapsed.as_secs_f64();

    // Print results
    println!("\n=== Results ===");
    println!("Total records processed: {}", all_records.len());
    println!("Total time: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {:.0} rec/sec", throughput);
    println!("");
    println!("Results produced: {}", results_produced);
    println!("Unique groups with results: {}", groups_seen.len());
    println!("Expected unique groups: 5,000 (50 traders * 100 symbols)");
    println!("");

    // Calculate data loss
    let expected_results = 10000; // Should approximate to ~10K after windowing
    let data_loss_percent = if results_produced > 0 {
        ((expected_results - results_produced) as f64 / expected_results as f64) * 100.0
    } else {
        100.0
    };

    println!("=== Data Loss Analysis ===");
    println!("Expected results (baseline): ~{}", expected_results);
    println!("Actual results (with watermark): {}", results_produced);
    println!("Data loss: {:.1}%", data_loss_percent);
    println!("");

    // BEFORE FIX (baseline):
    // - Results: 1,000 (90% loss)
    // - Throughput: 1,082 rec/sec
    // - Groups with results: 100
    //
    // AFTER FIX (with watermark):
    // - Results: 9,980+ (0% loss) - should be near full count
    // - Throughput: 50,000+ rec/sec (50x improvement)
    // - Groups with results: 5,000 (all groups)

    // Assertions
    println!("=== Verification ===");

    // Data loss should be minimal (< 10%)
    assert!(
        data_loss_percent < 10.0,
        "Data loss too high: {:.1}% (expected < 10%). \
         BEFORE fix: 90% loss. \
         WITH watermark implementation, should capture late-arriving groups.",
        data_loss_percent
    );
    println!("✅ Data loss acceptable: {:.1}%", data_loss_percent);

    // Throughput should be reasonable (> 10,000 rec/sec)
    assert!(
        throughput > 10000.0,
        "Throughput too low: {:.0} rec/sec (expected > 10,000). \
         BEFORE fix: 1,082 rec/sec. \
         WITH watermark, overhead should be minimal.",
        throughput
    );
    println!("✅ Throughput acceptable: {:.0} rec/sec", throughput);

    // Should capture most groups (> 4,000 out of 5,000)
    assert!(
        groups_seen.len() > 4000,
        "Group capture too low: {} groups (expected > 4,000). \
         BEFORE fix: only 100 groups. \
         WITH watermark + allowed lateness, should capture late-arriving groups.",
        groups_seen.len()
    );
    println!("✅ Group capture acceptable: {}/{} groups", groups_seen.len(), 5000);

    println!("\n🎉 Watermark implementation successfully fixes partition batching issues!");
}

#[tokio::test]
async fn test_watermark_pure_select_performance() {
    println!("\n=== Watermark Pure SELECT Performance Test ===");

    let all_records = generate_partition_batched_records(10000);

    let query = "SELECT trader_id, symbol, price, quantity FROM stream";
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    let mut records_processed = 0;

    for record in &all_records {
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                records_processed += results.len();
            }
            Err(e) => {
                panic!("Error processing record: {}", e);
            }
        }
    }

    // Drain channel
    while let Ok(_) = rx.try_recv() {
        records_processed += 1;
    }

    let elapsed = start.elapsed();
    let throughput = all_records.len() as f64 / elapsed.as_secs_f64();

    println!("Pure SELECT: {} records in {:.2}s", records_processed, elapsed.as_secs_f64());
    println!("Throughput: {:.0} rec/sec", throughput);

    // Should be very fast (baseline for comparison)
    assert!(
        throughput > 100000.0,
        "Pure SELECT throughput too low: {:.0} rec/sec",
        throughput
    );
    println!("✅ Pure SELECT performance acceptable");
}
