/*!
# Window State Analysis Test

Isolate the performance degradation in TUMBLING WINDOW + GROUP BY processing
with partition-distributed data.

Key hypothesis: The window aggregation state grows unbounded or accumulates
incorrectly with partition-distributed records.
*/

use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Test if window state grows unbounded with partition-batched data
#[tokio::test]
async fn test_window_state_growth_with_partition_batching() {
    // Generate smaller dataset but with partition batching pattern
    let batch_records = generate_small_partition_batched_records(1000);
    let all_records: Vec<StreamRecord> = batch_records.into_iter().flatten().collect();

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
    let mut batch_times = Vec::new();

    for (idx, record) in all_records.iter().enumerate() {
        let batch_start = Instant::now();

        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                records_processed += results.len();
            }
            Err(e) => {
                panic!("Error processing record {}: {}", idx, e);
            }
        }

        let batch_elapsed = batch_start.elapsed();
        batch_times.push(batch_elapsed.as_micros());

        // Print every 100 records
        if (idx + 1) % 100 == 0 {
            let elapsed = start.elapsed();
            let recent_avg_micros = batch_times[batch_times.len().saturating_sub(10)..]
                .iter()
                .sum::<u128>()
                / 10.max(batch_times.len()) as u128;

            println!(
                "  Record {}: avg_batch_time={:.1}µs, throughput={:.0} rec/sec",
                idx + 1,
                recent_avg_micros as f64,
                (idx as f64 + 1.0) / elapsed.as_secs_f64()
            );
        }
    }

    let elapsed = start.elapsed();
    let avg_batch_time_micros = batch_times.iter().sum::<u128>() as f64 / batch_times.len() as f64;

    println!(
        "✓ 1K partition-batched records: {:.2}s avg_batch_time={:.1}µs",
        elapsed.as_secs_f64(),
        avg_batch_time_micros
    );

    // With 1K records, average batch time should be < 1ms
    assert!(
        avg_batch_time_micros < 1000.0,
        "Average batch processing time too high: {:.1}µs (indicates state growth)",
        avg_batch_time_micros
    );
}

/// Test linear batching (should be fast)
#[tokio::test]
async fn test_window_state_linear_batching() {
    let all_records = generate_linear_batched_records(1000);

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
    let mut batch_times = Vec::new();

    for record in &all_records {
        let batch_start = Instant::now();

        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(_results) => {}
            Err(e) => panic!("Error: {}", e),
        }

        batch_times.push(batch_start.elapsed().as_micros());
    }

    let elapsed = start.elapsed();
    let avg_batch_time = batch_times.iter().sum::<u128>() as f64 / batch_times.len() as f64;

    println!(
        "✓ 1K linear-batched records: {:.2}s avg_batch_time={:.1}µs",
        elapsed.as_secs_f64(),
        avg_batch_time
    );

    assert!(
        avg_batch_time < 1000.0,
        "Linear batching too slow: {:.1}µs",
        avg_batch_time
    );
}

/// Generate partition-batched records
fn generate_small_partition_batched_records(record_count: usize) -> Vec<Vec<StreamRecord>> {
    let mut all_records = Vec::new();

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
        record.partition = (hash as i32) % 32;

        all_records.push(record);
    }

    // Group by partition
    let mut partition_map: HashMap<i32, Vec<StreamRecord>> = HashMap::new();
    for record in all_records {
        partition_map
            .entry(record.partition)
            .or_insert_with(Vec::new)
            .push(record);
    }

    // Create batches
    let mut all_batches = Vec::new();
    let mut partition_order: Vec<i32> = partition_map.keys().copied().collect();
    partition_order.sort_unstable();

    for partition_id in partition_order {
        let mut partition_records = partition_map.remove(&partition_id).unwrap_or_default();
        while !partition_records.is_empty() {
            let batch_len = 100.min(partition_records.len());
            let batch: Vec<StreamRecord> = partition_records.drain(0..batch_len).collect();
            all_batches.push(batch);
        }
    }

    all_batches
}

/// Generate linear-batched records
fn generate_linear_batched_records(record_count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::new();

    for i in 0..record_count {
        let mut fields = HashMap::new();
        let trader_id = format!("T{}", i % 50);
        let symbol = format!("SYM{}", i % 100);
        let price = 100.0 + (i % 50) as f64;
        let quantity = (i % 1000) as i64;
        let trade_time = 1000000 + (i as i64 * 1000);

        fields.insert("trader_id".to_string(), FieldValue::String(trader_id));
        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("quantity".to_string(), FieldValue::Integer(quantity));
        fields.insert("trade_time".to_string(), FieldValue::Integer(trade_time));

        records.push(StreamRecord::new(fields));
    }

    records
}

fn compute_fnv_hash(key: &str) -> u64 {
    let mut hash = 2166136261u64;
    for byte in key.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}
