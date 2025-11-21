/*!
# Data Generation Performance Check

Verify that the non-linear degradation we see is NOT from test data generation.
*/

use std::collections::HashMap;
use std::time::Instant;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

fn compute_fnv_hash(key: &str) -> u64 {
    let mut hash = 2166136261u64;
    for byte in key.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

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

        let hash = compute_fnv_hash(&composite_key);
        record.partition = (hash as i32) % (num_partitions as i32);

        all_records.push(record);
    }

    // Group records by partition
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

#[test]
fn verify_data_generation_is_linear() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ DATA GENERATION PERFORMANCE CHECK                        ║");
    println!("║ Verify non-linearity is NOT from test data generation   ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let test_sizes = vec![5_000, 10_000, 20_000, 30_000, 40_000, 50_000];

    println!("Record Count | Gen Time (ms) | Throughput");
    println!("──────────────────────────────────────────");

    let mut times = Vec::new();

    for record_count in test_sizes {
        let start = Instant::now();
        let _records = generate_partition_batched_records(record_count, 32);
        let elapsed = start.elapsed();

        let throughput = (record_count as f64) / elapsed.as_secs_f64();
        times.push((record_count, elapsed.as_millis(), throughput));

        println!(
            "    {:6}    |    {:.2}ms     |  {:.0} rec/sec",
            record_count,
            elapsed.as_millis() as f64,
            throughput
        );
    }

    println!("\n📊 ANALYSIS:");
    println!("──────────────────────────────────────────");

    // Check if linear
    if times.len() >= 2 {
        let (size1, time1, throughput1) = times[0];
        let (size_last, time_last, throughput_last) = times[times.len() - 1];

        let size_ratio = (size_last as f64) / (size1 as f64);
        let time_ratio = (time_last as f64) / (time1 as f64);
        let throughput_ratio = throughput1 / throughput_last;

        println!("  Size ratio: {:.2}x", size_ratio);
        println!("  Time ratio: {:.2}x", time_ratio);
        println!("  Throughput ratio: {:.2}x", throughput_ratio);

        if (time_ratio - size_ratio).abs() < 0.5 {
            println!("\n  ✅ Result: LINEAR data generation");
            println!("     Time increases proportionally with record count");
            println!("     → Non-linearity is NOT in data generation");
            println!("     → Non-linearity MUST be in algorithm processing");
        } else if time_ratio > size_ratio {
            println!("\n  ❌ Result: NON-LINEAR data generation");
            println!("     Time increases faster than record count");
            println!("     → Cannot isolate cause until data generation is fixed");
        } else {
            println!("\n  ℹ️  Result: SUB-LINEAR data generation");
            println!("     Time increases slower than record count");
            println!("     → Good baseline for isolation");
        }
    }
}
