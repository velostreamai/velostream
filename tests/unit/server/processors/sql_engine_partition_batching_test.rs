/*!
# SQLExecutionEngine + Partition Batching Test

Tests whether SQLExecutionEngine can handle partition-batched data without hanging.
This is the critical test to isolate whether the hang is in:
1. SimpleJobProcessor (processor layer)
2. StreamExecutionEngine (execution layer)
3. Window/aggregation logic (implementation layer)

If SQLExecutionEngine PASSES with partition-batched data, then hang is in SimpleJobProcessor.
If SQLExecutionEngine HANGS with partition-batched data, then hang is in execution engine itself.
*/

use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Generate partition-batched records (same as KafkaSimulatorDataSource)
fn generate_partition_batched_records(record_count: usize) -> Vec<Vec<StreamRecord>> {
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

    // Create batches within each partition (batch size = 100)
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

/// FNV hash for consistent partition assignment
fn compute_fnv_hash(key: &str) -> u64 {
    let mut hash = 2166136261u64;
    for byte in key.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

/// Test SQLExecutionEngine with partition-batched data (Scenario 1: Pure SELECT)
#[tokio::test]
async fn test_sql_engine_partition_batching_pure_select() {
    let batch_records = generate_partition_batched_records(10000);
    let all_records: Vec<StreamRecord> = batch_records.into_iter().flatten().collect();

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
    println!(
        "✓ SQLEngine Pure SELECT: {} records in {:.2}s ({:.0} rec/sec)",
        records_processed,
        elapsed.as_secs_f64(),
        all_records.len() as f64 / elapsed.as_secs_f64()
    );

    // Should process all records
    assert_eq!(records_processed, all_records.len());
    // Should complete in reasonable time (< 10 seconds for 10K records)
    assert!(
        elapsed.as_secs() < 10,
        "Pure SELECT took too long: {:.2}s",
        elapsed.as_secs_f64()
    );
}

/// Test SQLExecutionEngine with partition-batched data (Scenario 4: TUMBLING WINDOW + GROUP BY)
#[tokio::test]
async fn test_sql_engine_partition_batching_tumbling_group_by() {
    let batch_records = generate_partition_batched_records(10000);
    let all_records: Vec<StreamRecord> = batch_records.into_iter().flatten().collect();

    // Scenario 4 query (using WINDOW TUMBLING syntax)
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
    let mut batches_processed = 0;
    let mut last_log_time = start;

    for record in &all_records {
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                records_processed += results.len();
            }
            Err(e) => {
                panic!("Error processing record: {}", e);
            }
        }

        // Log progress every second
        if start.elapsed().as_secs() > last_log_time.elapsed().as_secs() + 1 {
            let elapsed = start.elapsed();
            println!(
                "  Progress: {}/{} records, {:.0} rec/sec",
                all_records.len(),
                all_records.len(),
                all_records.len() as f64 / elapsed.as_secs_f64()
            );
            last_log_time = start;
        }
        batches_processed += 1;
    }

    // Drain channel for final results
    while let Ok(_) = rx.try_recv() {
        records_processed += 1;
    }

    let elapsed = start.elapsed();
    println!(
        "✓ SQLEngine TUMBLING WINDOW + GROUP BY: {} output records in {:.2}s ({:.0} rec/sec)",
        records_processed,
        elapsed.as_secs_f64(),
        all_records.len() as f64 / elapsed.as_secs_f64()
    );

    // Should process all records
    assert!(
        records_processed > 0,
        "No records produced by TUMBLING WINDOW + GROUP BY query"
    );
    // Should complete in reasonable time (< 15 seconds for 10K records)
    assert!(
        elapsed.as_secs() < 15,
        "TUMBLING WINDOW + GROUP BY took too long: {:.2}s",
        elapsed.as_secs_f64()
    );
}

/// Test SQLExecutionEngine with larger partition-batched dataset
#[tokio::test]
async fn test_sql_engine_partition_batching_100k() {
    let batch_records = generate_partition_batched_records(100000);
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

    for (idx, record) in all_records.iter().enumerate() {
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                records_processed += results.len();
            }
            Err(e) => {
                panic!("Error processing record {}: {}", idx, e);
            }
        }

        // Log progress every 10K records
        if (idx + 1) % 10000 == 0 {
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
        records_processed += 1;
    }

    let elapsed = start.elapsed();
    println!(
        "✓ SQLEngine 100K TUMBLING WINDOW + GROUP BY: {} output records in {:.2}s ({:.0} rec/sec)",
        records_processed,
        elapsed.as_secs_f64(),
        all_records.len() as f64 / elapsed.as_secs_f64()
    );

    // Should produce results
    assert!(
        records_processed > 0,
        "No records produced by TUMBLING WINDOW + GROUP BY query with 100K input"
    );
    // Should complete in reasonable time (< 60 seconds for 100K records)
    assert!(
        elapsed.as_secs() < 60,
        "TUMBLING WINDOW + GROUP BY 100K took too long: {:.2}s",
        elapsed.as_secs_f64()
    );
}
