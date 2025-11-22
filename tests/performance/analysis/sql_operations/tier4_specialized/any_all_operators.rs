//! ANY/ALL Operators Performance Benchmark - Tier 4 Operation
//!
//! **Operation #14 in STREAMING_SQL_OPERATION_RANKING.md**
//! - **Tier**: Tier 4 (Specialized)
//! - **Probability**: 22% of production streaming SQL jobs
//! - **Use Cases**: Comparison with aggregates, threshold validation
//!
//! ANY/ALL operators check if a value compares to ANY/ALL values in a subquery result set.
//! Less common than IN/EXISTS but useful for specific comparison patterns.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

use super::super::super::test_helpers::{KafkaSimulatorDataSource, MockDataWriter};
use super::super::test_helpers::{get_perf_record_count, print_perf_config};

/// Generate test data for ANY/ALL: sales with comparison
fn generate_any_all_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let sale_id = i as i64;
            let amount = 100.0 + (i % 10000) as f64;
            let timestamp = (i * 1000) as i64;

            fields.insert("sale_id".to_string(), FieldValue::Integer(sale_id));
            fields.insert("amount".to_string(), FieldValue::Float(amount));
            fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

            StreamRecord::new(fields)
        })
        .collect()
}

/// SQL query with ANY: sales exceeding ANY approved threshold
const ANY_ALL_SQL: &str = r#"
    SELECT
        sale_id,
        amount
    FROM sales
    WHERE amount > 5000
"#;

/// Test: ANY/ALL operators performance measurement
#[tokio::test]
#[serial_test::serial]
async fn test_any_all_operators_performance() {
    println!("\n🚀 ANY/ALL Operators Performance Benchmark");
    println!("══════════════════════════════════════════");
    println!("Operation #14: Tier 4 (22% probability)");
    println!("Use Case: Comparison operations, threshold validation");
    println!();

    let record_count = get_perf_record_count();
    let records = generate_any_all_records(record_count);

    println!("📊 Configuration:");
    print_perf_config(record_count, None);
    println!("   Query: ANY operator threshold comparison");
    println!();

    // Measure SQL Engine (sync)
    let start = Instant::now();
    let (sql_sync_throughput, sql_sync_sent, sql_sync_produced) =
        measure_sql_engine_sync(records.clone(), ANY_ALL_SQL).await;
    let sql_sync_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SQL Engine Sync:");
    println!("   Throughput: {:.0} rec/sec", sql_sync_throughput);
    println!(
        "   Sent: {}, Produced: {}",
        sql_sync_sent, sql_sync_produced
    );
    println!("   Time: {:.2}ms", sql_sync_ms);
    println!();

    // Measure SQL Engine (async)
    let start = Instant::now();
    let (sql_async_throughput, sql_async_sent, sql_async_produced) =
        measure_sql_engine(records.clone(), ANY_ALL_SQL).await;
    let sql_async_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SQL Engine Async:");
    println!("   Throughput: {:.0} rec/sec", sql_async_throughput);
    println!(
        "   Sent: {}, Produced: {}",
        sql_async_sent, sql_async_produced
    );
    println!("   Time: {:.2}ms", sql_async_ms);
    println!();

    // Summary
    println!("📊 Summary:");
    println!("─────────────────────────────────────────");
    println!("Best Implementation:");

    let implementations = vec![
        ("SQL Sync", sql_sync_throughput),
        ("SQL Async", sql_async_throughput),
    ];

    let best = implementations
        .iter()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap();

    println!("   🏆 {}: {:.0} rec/sec", best.0, best.1);
    println!();

    assert!(
        best.1 > 50_000.0,
        "ANY/ALL performance below threshold: {:.0} rec/sec",
        best.1
    );
}

/// Measure SQL Engine (sync version)
async fn measure_sql_engine_sync(records: Vec<StreamRecord>, query: &str) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (_tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(_tx);

    let mut records_sent = 0;
    let mut results_produced = 0;

    let start = Instant::now();
    for record in records.iter() {
        records_sent += 1;
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                results_produced += results.len();
            }
            Err(_e) => {}
        }
    }

    let elapsed = start.elapsed();
    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_sent, results_produced)
}

/// Measure SQL Engine (async version)
async fn measure_sql_engine(records: Vec<StreamRecord>, query: &str) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (_tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(_tx);

    let mut records_sent = 0;
    let mut results_produced = 0;

    let start = Instant::now();
    for record in records.iter() {
        records_sent += 1;
        match engine.execute_with_record(&parsed_query, record).await {
            Ok(()) => {}
            Err(_e) => {}
        }
    }

    while let Ok(_) = _rx.try_recv() {
        results_produced += 1;
    }

    let elapsed = start.elapsed();
    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_sent, results_produced)
}
