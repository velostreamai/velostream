/*!
# Debug test to understand late arrival handling
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

fn compute_fnv_hash(key: &str) -> u64 {
    let mut hash = 2166136261u64;
    for byte in key.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

#[tokio::test]
async fn debug_watermark_late_arrivals() {
    println!("\n=== Debug: Late Arrival Handling ===\n");

    // Create a SMALL test with just 120 records (2 partitions, 60 records each)
    // This will help us trace the exact flow
    let mut all_records = Vec::new();

    // Generate 120 records: 50 traders, 100 symbols → will map to fewer partitions
    for i in 0..120 {
        let mut fields = HashMap::new();
        let trader_id = format!("T{}", i % 5);  // 5 traders for simplicity
        let symbol = format!("SYM{}", i % 10); // 10 symbols for simplicity
        let price = 100.0 + (i % 5) as f64;
        let quantity = (i % 10) as i64;
        let trade_time = 1000000 + (i as i64 * 1000); // Times: 1000000, 1001000, 1002000, ...

        fields.insert("trader_id".to_string(), FieldValue::String(trader_id.clone()));
        fields.insert("symbol".to_string(), FieldValue::String(symbol.clone()));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("quantity".to_string(), FieldValue::Integer(quantity));
        fields.insert("trade_time".to_string(), FieldValue::Integer(trade_time));

        let composite_key = format!("{}:{}", trader_id, symbol);
        let mut record = StreamRecord::new(fields);
        let hash = compute_fnv_hash(&composite_key);
        record.partition = (hash as i32) % 4; // 4 partitions
        let partition = record.partition;

        all_records.push((i, record, trade_time, partition));
    }

    // Group by partition to simulate partition-batched delivery
    let mut partition_map: HashMap<i32, Vec<(StreamRecord, i64)>> = HashMap::new();
    for (_, record, ts, partition) in &all_records {
        partition_map
            .entry(*partition)
            .or_insert_with(Vec::new)
            .push((record.clone(), *ts));
    }

    // Flatten in partition order
    let mut partition_order: Vec<i32> = partition_map.keys().copied().collect();
    partition_order.sort_unstable();

    let mut ordered_records = Vec::new();
    for partition_id in partition_order {
        if let Some(records) = partition_map.remove(&partition_id) {
            for (record, ts) in records {
                ordered_records.push((record, ts, partition_id));
            }
        }
    }

    println!("Input records (partition-batched):");
    for (idx, (record, ts, partition)) in ordered_records.iter().enumerate() {
        let trader = record
            .fields
            .get("trader_id")
            .and_then(|v| match v {
                FieldValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_default();
        println!("  {}: partition={}, time={}, trader={}", idx, partition, ts, trader);
    }
    println!();

    // Run query
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

    let mut results_produced = 0;

    println!("Processing records:");
    for (idx, (record, _ts, partition)) in ordered_records.iter().enumerate() {
        let trader = record
            .fields
            .get("trader_id")
            .and_then(|v| match v {
                FieldValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_default();

        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                if !results.is_empty() {
                    println!(
                        "  Record {}: partition={}, trader={} → {} results",
                        idx,
                        partition,
                        trader,
                        results.len()
                    );
                    results_produced += results.len();
                }
            }
            Err(e) => {
                println!("  Record {}: ERROR - {}", idx, e);
            }
        }
    }

    // Drain channel
    while let Ok(_) = rx.try_recv() {
        results_produced += 1;
    }

    println!("\nResults produced: {}", results_produced);
    println!("Expected: All 5*10 = 50 unique groups should appear in results");
}
