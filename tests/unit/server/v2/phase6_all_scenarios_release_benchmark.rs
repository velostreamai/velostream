//! Phase 6: All Scenarios Release-Mode Benchmarks
//!
//! Comprehensive benchmarking of all 5 scenarios from FR-082-BASELINE-MEASUREMENTS.md
//! Testing all three architectures:
//! - V1 Direct SQL (Raw StreamExecutionEngine)
//! - V1 JobServer (Full SimpleJobProcessor pipeline with per-record locking)
//! - V2 Batch-based (Batch locking optimization)
//!
//! Scenarios:
//! 1. Scenario 0: Pure SELECT (passthrough, no aggregation)
//! 2. Scenario 1: ROWS WINDOW (memory-bounded buffering, no GROUP BY)
//! 3. Scenario 2: Pure GROUP BY (no windowing)
//! 4. Scenario 3a: TUMBLING + GROUP BY (standard emission)
//! 5. Scenario 3b: TUMBLING + GROUP BY + EMIT CHANGES (continuous emission)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use velostream::velostream::sql::{
    StreamExecutionEngine,
    execution::types::{FieldValue, StreamRecord},
    parser::StreamingSqlParser,
};

// ============================================================================
// SCENARIO 0: PURE SELECT (Passthrough)
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_scenario_0_pure_select() {
    println!("\n\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  SCENARIO 0: PURE SELECT (Passthrough)                          ║");
    println!("║  Filter + Projection - No aggregation, no windowing             ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    let num_records = 5_000;
    let batch_size = 100;
    let parser = StreamingSqlParser::new();
    let query = parser.parse(
        "SELECT order_id, customer_id, order_date, total_amount FROM orders WHERE total_amount > 100"
    ).unwrap();

    println!("Query: SELECT order_id, customer_id, order_date, total_amount");
    println!("       FROM orders WHERE total_amount > 100");
    println!("Records: {}\n", num_records);

    // Create realistic test data
    let mut records = Vec::new();
    for i in 0..num_records {
        let mut fields = HashMap::new();
        fields.insert("order_id".to_string(), FieldValue::Integer(i as i64));
        fields.insert(
            "customer_id".to_string(),
            FieldValue::Integer((i % 1000) as i64),
        );
        fields.insert(
            "order_date".to_string(),
            FieldValue::String("2024-01-15".to_string()),
        );
        fields.insert(
            "total_amount".to_string(),
            FieldValue::Float(150.0 + (i % 100) as f64),
        );
        records.push(StreamRecord::new(fields));
    }

    // ────────────────────────────────────────────────────────────────────
    // Test 1: V1 Direct SQL
    // ────────────────────────────────────────────────────────────────────
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
    let mut engine1 = StreamExecutionEngine::new(tx1);

    let start1 = Instant::now();
    for record in &records {
        let _ = engine1
            .execute_with_record(&query, record.clone())
            .await
            .ok();
    }
    let t1 = start1.elapsed();
    let tp1 = (num_records as f64 / t1.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 2: V1 JobServer (per-record locking)
    // ────────────────────────────────────────────────────────────────────
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
    let engine2 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx2)));

    let start2 = Instant::now();
    for record in &records {
        let mut lock = engine2.write().await;
        let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        drop(lock);
    }
    let t2 = start2.elapsed();
    let tp2 = (num_records as f64 / t2.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 3: V2 Batch-based
    // ────────────────────────────────────────────────────────────────────
    let (tx3, _rx3) = tokio::sync::mpsc::unbounded_channel();
    let engine3 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx3)));

    let start3 = Instant::now();
    for batch in records.chunks(batch_size) {
        let mut lock = engine3.write().await;
        for record in batch {
            let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        }
        drop(lock);
    }
    let t3 = start3.elapsed();
    let tp3 = (num_records as f64 / t3.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Results
    // ────────────────────────────────────────────────────────────────────
    println!("┌─────────────────────────┬──────────────┬──────────┐");
    println!("│ Architecture            │ Throughput   │ Time     │");
    println!("├─────────────────────────┼──────────────┼──────────┤");
    println!(
        "│ V1 Direct SQL           │ {:>6}k rec/s │ {:?}   │",
        tp1 / 1000,
        t1
    );
    println!(
        "│ V1 JobServer (per-rec)  │ {:>6}k rec/s │ {:?}   │",
        tp2 / 1000,
        t2
    );
    println!(
        "│ V2 Batch (100 records)  │ {:>6}k rec/s │ {:?}   │",
        tp3 / 1000,
        t3
    );
    println!("└─────────────────────────┴──────────────┴──────────┘\n");

    println!("Analysis:");
    println!(
        "  V1 Overhead: {:.1}%",
        ((t2.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs Baseline: {:.1}%",
        ((t3.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs V1: {:.1}% faster",
        ((tp3 as f64 / tp2 as f64) - 1.0) * 100.0
    );
    println!();
}

// ============================================================================
// SCENARIO 1: ROWS WINDOW (No GROUP BY)
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_scenario_1_rows_window() {
    println!("\n\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  SCENARIO 1: ROWS WINDOW (No GROUP BY)                          ║");
    println!("║  Memory-bounded sliding buffers, AVG/MIN/MAX functions          ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    let num_records = 5_000;
    let batch_size = 100;
    let parser = StreamingSqlParser::new();
    let query = parser.parse(
        "SELECT symbol, price, \
         AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol ORDER BY timestamp) as moving_avg, \
         MIN(price) OVER (ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol ORDER BY timestamp) as min_price, \
         MAX(price) OVER (ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol ORDER BY timestamp) as max_price \
         FROM market_data"
    ).unwrap();

    println!("Query: SELECT symbol, price, AVG/MIN/MAX OVER ROWS WINDOW");
    println!("       Buffer: 100 rows, Partitioned by symbol");
    println!("Records: {}\n", num_records);

    // Create test data
    let mut records = Vec::new();
    for i in 0..num_records {
        let mut fields = HashMap::new();
        fields.insert(
            "symbol".to_string(),
            FieldValue::String(format!("SYM{}", i % 10)),
        );
        fields.insert(
            "price".to_string(),
            FieldValue::Float(100.0 + (i as f64 * 0.5) % 50.0),
        );
        fields.insert(
            "timestamp".to_string(),
            FieldValue::String(format!("2024-01-15T{:02}:{:02}:00Z", (i / 60) % 24, i % 60)),
        );
        records.push(StreamRecord::new(fields));
    }

    // ────────────────────────────────────────────────────────────────────
    // Test 1: V1 Direct SQL
    // ────────────────────────────────────────────────────────────────────
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
    let mut engine1 = StreamExecutionEngine::new(tx1);

    let start1 = Instant::now();
    for record in &records {
        let _ = engine1
            .execute_with_record(&query, record.clone())
            .await
            .ok();
    }
    let t1 = start1.elapsed();
    let tp1 = (num_records as f64 / t1.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 2: V1 JobServer (per-record locking)
    // ────────────────────────────────────────────────────────────────────
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
    let engine2 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx2)));

    let start2 = Instant::now();
    for record in &records {
        let mut lock = engine2.write().await;
        let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        drop(lock);
    }
    let t2 = start2.elapsed();
    let tp2 = (num_records as f64 / t2.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 3: V2 Batch-based
    // ────────────────────────────────────────────────────────────────────
    let (tx3, _rx3) = tokio::sync::mpsc::unbounded_channel();
    let engine3 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx3)));

    let start3 = Instant::now();
    for batch in records.chunks(batch_size) {
        let mut lock = engine3.write().await;
        for record in batch {
            let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        }
        drop(lock);
    }
    let t3 = start3.elapsed();
    let tp3 = (num_records as f64 / t3.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Results
    // ────────────────────────────────────────────────────────────────────
    println!("┌─────────────────────────┬──────────────┬──────────┐");
    println!("│ Architecture            │ Throughput   │ Time     │");
    println!("├─────────────────────────┼──────────────┼──────────┤");
    println!(
        "│ V1 Direct SQL           │ {:>6}k rec/s │ {:?}   │",
        tp1 / 1000,
        t1
    );
    println!(
        "│ V1 JobServer (per-rec)  │ {:>6}k rec/s │ {:?}   │",
        tp2 / 1000,
        t2
    );
    println!(
        "│ V2 Batch (100 records)  │ {:>6}k rec/s │ {:?}   │",
        tp3 / 1000,
        t3
    );
    println!("└─────────────────────────┴──────────────┴──────────┘\n");

    println!("Analysis:");
    println!(
        "  V1 Overhead: {:.1}%",
        ((t2.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs Baseline: {:.1}%",
        ((t3.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs V1: {:.1}% faster",
        ((tp3 as f64 / tp2 as f64) - 1.0) * 100.0
    );
    println!();
}

// ============================================================================
// SCENARIO 2: Pure GROUP BY (No WINDOW)
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_scenario_2_pure_group_by() {
    println!("\n\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  SCENARIO 2: Pure GROUP BY (No WINDOW)                          ║");
    println!("║  Hash aggregation with 5 aggregate functions                    ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    let num_records = 5_000;
    let batch_size = 100;
    let parser = StreamingSqlParser::new();
    let query = parser
        .parse(
            "SELECT trader_id, symbol, \
         COUNT(*) as trade_count, \
         AVG(price) as avg_price, \
         SUM(quantity) as total_quantity, \
         SUM(price * quantity) as total_value \
         FROM market_data \
         GROUP BY trader_id, symbol",
        )
        .unwrap();

    println!(
        "Query: SELECT trader_id, symbol, COUNT(*), AVG(price), SUM(quantity), SUM(price*quantity)"
    );
    println!("       FROM market_data GROUP BY trader_id, symbol");
    println!("Records: {}\n", num_records);

    // Create test data
    let mut records = Vec::new();
    for i in 0..num_records {
        let mut fields = HashMap::new();
        fields.insert(
            "trader_id".to_string(),
            FieldValue::Integer((i % 100) as i64),
        );
        fields.insert(
            "symbol".to_string(),
            FieldValue::String(format!("SYM{}", i % 50)),
        );
        fields.insert(
            "price".to_string(),
            FieldValue::Float(100.0 + (i as f64 * 0.1) % 50.0),
        );
        fields.insert(
            "quantity".to_string(),
            FieldValue::Integer((i % 1000) as i64),
        );
        records.push(StreamRecord::new(fields));
    }

    // ────────────────────────────────────────────────────────────────────
    // Test 1: V1 Direct SQL
    // ────────────────────────────────────────────────────────────────────
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
    let mut engine1 = StreamExecutionEngine::new(tx1);

    let start1 = Instant::now();
    for record in &records {
        let _ = engine1
            .execute_with_record(&query, record.clone())
            .await
            .ok();
    }
    let t1 = start1.elapsed();
    let tp1 = (num_records as f64 / t1.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 2: V1 JobServer (per-record locking)
    // ────────────────────────────────────────────────────────────────────
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
    let engine2 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx2)));

    let start2 = Instant::now();
    for record in &records {
        let mut lock = engine2.write().await;
        let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        drop(lock);
    }
    let t2 = start2.elapsed();
    let tp2 = (num_records as f64 / t2.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 3: V2 Batch-based
    // ────────────────────────────────────────────────────────────────────
    let (tx3, _rx3) = tokio::sync::mpsc::unbounded_channel();
    let engine3 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx3)));

    let start3 = Instant::now();
    for batch in records.chunks(batch_size) {
        let mut lock = engine3.write().await;
        for record in batch {
            let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        }
        drop(lock);
    }
    let t3 = start3.elapsed();
    let tp3 = (num_records as f64 / t3.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Results
    // ────────────────────────────────────────────────────────────────────
    println!("┌─────────────────────────┬──────────────┬──────────┐");
    println!("│ Architecture            │ Throughput   │ Time     │");
    println!("├─────────────────────────┼──────────────┼──────────┤");
    println!(
        "│ V1 Direct SQL           │ {:>6}k rec/s │ {:?}   │",
        tp1 / 1000,
        t1
    );
    println!(
        "│ V1 JobServer (per-rec)  │ {:>6}k rec/s │ {:?}   │",
        tp2 / 1000,
        t2
    );
    println!(
        "│ V2 Batch (100 records)  │ {:>6}k rec/s │ {:?}   │",
        tp3 / 1000,
        t3
    );
    println!("└─────────────────────────┴──────────────┴──────────┘\n");

    println!("Analysis:");
    println!(
        "  V1 Overhead: {:.1}%",
        ((t2.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs Baseline: {:.1}%",
        ((t3.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs V1: {:.1}% faster",
        ((tp3 as f64 / tp2 as f64) - 1.0) * 100.0
    );
    println!();
}

// ============================================================================
// SCENARIO 3a: TUMBLING + GROUP BY (Standard Emission)
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_scenario_3a_tumbling_standard() {
    println!("\n\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  SCENARIO 3a: TUMBLING + GROUP BY (Standard Emission)           ║");
    println!("║  Window-based aggregation with batch emission on window close   ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    let num_records = 5_000;
    let batch_size = 100;
    let parser = StreamingSqlParser::new();
    let query = parser
        .parse(
            "SELECT trader_id, symbol, \
         COUNT(*) as trade_count, \
         AVG(price) as avg_price, \
         SUM(quantity) as total_quantity, \
         SUM(price * quantity) as total_value \
         FROM market_data \
         GROUP BY trader_id, symbol \
         WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)",
        )
        .unwrap();

    println!(
        "Query: SELECT trader_id, symbol, COUNT(*), AVG(price), SUM(quantity), SUM(price*quantity)"
    );
    println!("       FROM market_data");
    println!("       GROUP BY trader_id, symbol");
    println!("       WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)");
    println!("Records: {}\n", num_records);

    // Create test data with timestamps
    let mut records = Vec::new();
    for i in 0..num_records {
        let mut fields = HashMap::new();
        fields.insert(
            "trader_id".to_string(),
            FieldValue::Integer((i % 100) as i64),
        );
        fields.insert(
            "symbol".to_string(),
            FieldValue::String(format!("SYM{}", i % 50)),
        );
        fields.insert(
            "price".to_string(),
            FieldValue::Float(100.0 + (i as f64 * 0.1) % 50.0),
        );
        fields.insert(
            "quantity".to_string(),
            FieldValue::Integer((i % 1000) as i64),
        );
        fields.insert(
            "trade_time".to_string(),
            FieldValue::String(format!("2024-01-15T12:{:02}:{:02}Z", (i / 60) % 60, i % 60)),
        );
        records.push(StreamRecord::new(fields));
    }

    // ────────────────────────────────────────────────────────────────────
    // Test 1: V1 Direct SQL
    // ────────────────────────────────────────────────────────────────────
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
    let mut engine1 = StreamExecutionEngine::new(tx1);

    let start1 = Instant::now();
    for record in &records {
        let _ = engine1
            .execute_with_record(&query, record.clone())
            .await
            .ok();
    }
    let t1 = start1.elapsed();
    let tp1 = (num_records as f64 / t1.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 2: V1 JobServer (per-record locking)
    // ────────────────────────────────────────────────────────────────────
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
    let engine2 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx2)));

    let start2 = Instant::now();
    for record in &records {
        let mut lock = engine2.write().await;
        let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        drop(lock);
    }
    let t2 = start2.elapsed();
    let tp2 = (num_records as f64 / t2.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 3: V2 Batch-based
    // ────────────────────────────────────────────────────────────────────
    let (tx3, _rx3) = tokio::sync::mpsc::unbounded_channel();
    let engine3 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx3)));

    let start3 = Instant::now();
    for batch in records.chunks(batch_size) {
        let mut lock = engine3.write().await;
        for record in batch {
            let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        }
        drop(lock);
    }
    let t3 = start3.elapsed();
    let tp3 = (num_records as f64 / t3.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Results
    // ────────────────────────────────────────────────────────────────────
    println!("┌─────────────────────────┬──────────────┬──────────┐");
    println!("│ Architecture            │ Throughput   │ Time     │");
    println!("├─────────────────────────┼──────────────┼──────────┤");
    println!(
        "│ V1 Direct SQL           │ {:>6}k rec/s │ {:?}   │",
        tp1 / 1000,
        t1
    );
    println!(
        "│ V1 JobServer (per-rec)  │ {:>6}k rec/s │ {:?}   │",
        tp2 / 1000,
        t2
    );
    println!(
        "│ V2 Batch (100 records)  │ {:>6}k rec/s │ {:?}   │",
        tp3 / 1000,
        t3
    );
    println!("└─────────────────────────┴──────────────┴──────────┘\n");

    println!("Analysis:");
    println!(
        "  V1 Overhead: {:.1}%",
        ((t2.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs Baseline: {:.1}%",
        ((t3.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs V1: {:.1}% faster",
        ((tp3 as f64 / tp2 as f64) - 1.0) * 100.0
    );
    println!();
}

// ============================================================================
// SCENARIO 3b: TUMBLING + GROUP BY + EMIT CHANGES (Continuous Emission)
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_scenario_3b_tumbling_emit_changes() {
    println!("\n\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  SCENARIO 3b: TUMBLING + GROUP BY + EMIT CHANGES                ║");
    println!("║  Window-based aggregation with continuous emission              ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    let num_records = 5_000;
    let batch_size = 100;
    let parser = StreamingSqlParser::new();
    let query = parser
        .parse(
            "SELECT trader_id, symbol, \
         COUNT(*) as trade_count, \
         AVG(price) as avg_price, \
         SUM(quantity) as total_quantity, \
         SUM(price * quantity) as total_value \
         FROM market_data \
         GROUP BY trader_id, symbol \
         WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE) EMIT CHANGES",
        )
        .unwrap();

    println!(
        "Query: SELECT trader_id, symbol, COUNT(*), AVG(price), SUM(quantity), SUM(price*quantity)"
    );
    println!("       FROM market_data");
    println!("       GROUP BY trader_id, symbol");
    println!("       WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE) EMIT CHANGES");
    println!("Records: {}", num_records);
    println!("Note: EMIT CHANGES causes high result amplification\n");

    // Create test data with timestamps
    let mut records = Vec::new();
    for i in 0..num_records {
        let mut fields = HashMap::new();
        fields.insert(
            "trader_id".to_string(),
            FieldValue::Integer((i % 100) as i64),
        );
        fields.insert(
            "symbol".to_string(),
            FieldValue::String(format!("SYM{}", i % 50)),
        );
        fields.insert(
            "price".to_string(),
            FieldValue::Float(100.0 + (i as f64 * 0.1) % 50.0),
        );
        fields.insert(
            "quantity".to_string(),
            FieldValue::Integer((i % 1000) as i64),
        );
        fields.insert(
            "trade_time".to_string(),
            FieldValue::String(format!("2024-01-15T12:{:02}:{:02}Z", (i / 60) % 60, i % 60)),
        );
        records.push(StreamRecord::new(fields));
    }

    // ────────────────────────────────────────────────────────────────────
    // Test 1: V1 Direct SQL
    // ────────────────────────────────────────────────────────────────────
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
    let mut engine1 = StreamExecutionEngine::new(tx1);

    let start1 = Instant::now();
    for record in &records {
        let _ = engine1
            .execute_with_record(&query, record.clone())
            .await
            .ok();
    }
    let t1 = start1.elapsed();
    let tp1 = (num_records as f64 / t1.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 2: V1 JobServer (per-record locking)
    // ────────────────────────────────────────────────────────────────────
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
    let engine2 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx2)));

    let start2 = Instant::now();
    for record in &records {
        let mut lock = engine2.write().await;
        let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        drop(lock);
    }
    let t2 = start2.elapsed();
    let tp2 = (num_records as f64 / t2.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Test 3: V2 Batch-based
    // ────────────────────────────────────────────────────────────────────
    let (tx3, _rx3) = tokio::sync::mpsc::unbounded_channel();
    let engine3 = Arc::new(RwLock::new(StreamExecutionEngine::new(tx3)));

    let start3 = Instant::now();
    for batch in records.chunks(batch_size) {
        let mut lock = engine3.write().await;
        for record in batch {
            let _ = lock.execute_with_record(&query, record.clone()).await.ok();
        }
        drop(lock);
    }
    let t3 = start3.elapsed();
    let tp3 = (num_records as f64 / t3.as_secs_f64()) as u64;

    // ────────────────────────────────────────────────────────────────────
    // Results
    // ────────────────────────────────────────────────────────────────────
    println!("┌─────────────────────────┬──────────────┬──────────┐");
    println!("│ Architecture            │ Throughput   │ Time     │");
    println!("├─────────────────────────┼──────────────┼──────────┤");
    println!(
        "│ V1 Direct SQL           │ {:>6}k rec/s │ {:?}   │",
        tp1 / 1000,
        t1
    );
    println!(
        "│ V1 JobServer (per-rec)  │ {:>6}k rec/s │ {:?}   │",
        tp2 / 1000,
        t2
    );
    println!(
        "│ V2 Batch (100 records)  │ {:>6}k rec/s │ {:?}   │",
        tp3 / 1000,
        t3
    );
    println!("└─────────────────────────┴──────────────┴──────────┘\n");

    println!("Analysis:");
    println!(
        "  V1 Overhead: {:.1}%",
        ((t2.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs Baseline: {:.1}%",
        ((t3.as_secs_f64() / t1.as_secs_f64()) - 1.0) * 100.0
    );
    println!(
        "  V2 vs V1: {:.1}% faster",
        ((tp3 as f64 / tp2 as f64) - 1.0) * 100.0
    );
    println!();
}

// ============================================================================
// MASTER SUMMARY TABLE
// ============================================================================

#[tokio::test]
#[ignore]
async fn benchmark_master_summary() {
    println!("\n\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  MASTER SUMMARY: All Scenarios & Architectures                  ║");
    println!("║  Release Mode - Comparable Measurements                         ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    println!("This benchmark suite tests 5 scenarios from FR-082-BASELINE-MEASUREMENTS.md:");
    println!("  Scenario 0: Pure SELECT (passthrough)");
    println!("  Scenario 1: ROWS WINDOW (memory-bounded)");
    println!("  Scenario 2: Pure GROUP BY (hash aggregation)");
    println!("  Scenario 3a: TUMBLING + GROUP BY (standard emission)");
    println!("  Scenario 3b: TUMBLING + GROUP BY + EMIT CHANGES (continuous emission)");
    println!();
    println!("Each scenario tests three architectures:");
    println!("  V1 Direct SQL:       Raw StreamExecutionEngine, no framework overhead");
    println!("  V1 JobServer:        Full V1 pipeline with per-record RwLock locking");
    println!("  V2 Batch-based:      V2 optimization with batch-level locking (100 records)");
    println!();
    println!("Run individual tests with:");
    println!(
        "  cargo test --release --no-default-features benchmark_scenario_0_pure_select -- --nocapture --ignored"
    );
    println!(
        "  cargo test --release --no-default-features benchmark_scenario_1_rows_window -- --nocapture --ignored"
    );
    println!(
        "  cargo test --release --no-default-features benchmark_scenario_2_pure_group_by -- --nocapture --ignored"
    );
    println!(
        "  cargo test --release --no-default-features benchmark_scenario_3a_tumbling_standard -- --nocapture --ignored"
    );
    println!(
        "  cargo test --release --no-default-features benchmark_scenario_3b_tumbling_emit_changes -- --nocapture --ignored"
    );
    println!();
}
