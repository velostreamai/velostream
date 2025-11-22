//! ROWS WINDOW Performance Benchmark - Tier 1 Operation
//!
//! **Operation #2 in STREAMING_SQL_OPERATION_RANKING.md**
//! - **Tier**: Tier 1 (Essential)
//! - **Probability**: 78% of production streaming SQL jobs
//! - **Use Cases**: Moving averages, running aggregations, sliding window calculations
//!
//! ROWS WINDOW is a fundamental windowing operation for computing running aggregates
//! over ordered streams of data. Critical for financial time series analysis.

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

/// Generate test data: market data with symbols and prices
/// 10 unique symbols distributed across partitions
fn generate_rows_window_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let symbol = format!("SYM{}", i % 10);
            fields.insert("symbol".to_string(), FieldValue::String(symbol.clone()));
            fields.insert(
                "price".to_string(),
                FieldValue::Float(100.0 + (i % 50) as f64),
            );
            fields.insert(
                "timestamp".to_string(),
                FieldValue::Integer((i * 1000) as i64),
            );
            // Partition by symbol: 10 unique symbols → consistent distribution for sticky partition strategy
            StreamRecord::new(fields).with_partition_from_key(&symbol, 32)
        })
        .collect()
}

const ROWS_WINDOW_SQL: &str = r#"
    SELECT symbol, price,
        AVG(price) OVER (
            ROWS WINDOW
                BUFFER 100 ROWS
                PARTITION BY symbol
                ORDER BY timestamp
        ) as moving_avg
    FROM market_data
"#;

#[tokio::test]
#[serial_test::serial]
async fn test_rows_window_performance() {
    println!("\n🚀 ROWS WINDOW Performance Benchmark");
    println!("════════════════════════════════════");
    println!("Operation #2: Tier 1 (78% probability)");
    println!("Use Case: Moving averages, running aggregations");
    println!();

    let record_count = get_perf_record_count();
    let records = generate_rows_window_records(record_count);

    println!("📊 Configuration:");
    print_perf_config(record_count, None);
    println!("   Query: ROWS WINDOW with 100-row buffer");
    println!("   Symbols: 10 unique");
    println!();

    let start = Instant::now();
    let (sql_sync_throughput, sql_sync_sent, sql_sync_produced) =
        measure_sql_engine_sync(records.clone(), ROWS_WINDOW_SQL).await;
    let sql_sync_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SQL Engine Sync:");
    println!("   Throughput: {:.0} rec/sec", sql_sync_throughput);
    println!(
        "   Sent: {}, Produced: {}",
        sql_sync_sent, sql_sync_produced
    );
    println!("   Time: {:.2}ms", sql_sync_ms);
    println!();

    let start = Instant::now();
    let (sql_async_throughput, sql_async_sent, sql_async_produced) =
        measure_sql_engine(records.clone(), ROWS_WINDOW_SQL).await;
    let sql_async_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SQL Engine Async:");
    println!("   Throughput: {:.0} rec/sec", sql_async_throughput);
    println!(
        "   Sent: {}, Produced: {}",
        sql_async_sent, sql_async_produced
    );
    println!("   Time: {:.2}ms", sql_async_ms);
    println!();

    let start = Instant::now();
    let (simple_jp_throughput, simple_jp_produced) =
        measure_v1(records.clone(), ROWS_WINDOW_SQL).await;
    let simple_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SimpleJp:");
    println!("   Throughput: {:.0} rec/sec", simple_jp_throughput);
    println!("   Results: {}", simple_jp_produced);
    println!("   Time: {:.2}ms", simple_jp_ms);
    println!();

    let start = Instant::now();
    let (transactional_jp_throughput, transactional_jp_produced) =
        measure_transactional_jp(records.clone(), ROWS_WINDOW_SQL).await;
    let transactional_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ TransactionalJp:");
    println!("   Throughput: {:.0} rec/sec", transactional_jp_throughput);
    println!("   Results: {}", transactional_jp_produced);
    println!("   Time: {:.2}ms", transactional_jp_ms);
    println!();

    println!("📊 Summary:");
    println!("─────────────────────────────────────────");
    println!("Best Implementation:");

    let implementations = vec![
        ("SQL Sync", sql_sync_throughput),
        ("SQL Async", sql_async_throughput),
        ("SimpleJp", simple_jp_throughput),
        ("TransactionalJp", transactional_jp_throughput),
    ];

    let best = implementations
        .iter()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap();

    println!("   🏆 {}: {:.0} rec/sec", best.0, best.1);
    println!();

    assert!(
        best.1 > 50_000.0,
        "ROWS WINDOW performance below threshold: {:.0} rec/sec",
        best.1
    );
}

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

async fn measure_v1(records: Vec<StreamRecord>, query: &str) -> (f64, usize) {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 100,
        batch_timeout: Duration::from_millis(100),
        max_retries: 2,
        retry_backoff: Duration::from_millis(50),
        progress_interval: 100,
        log_progress: false,
        empty_batch_count: 0,
        wait_on_empty_batch_ms: 10,
        enable_dlq: true,
        dlq_max_size: Some(100),
    };

    let processor = JobProcessorFactory::create_simple_with_config(config);
    let data_source = KafkaSimulatorDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let timeout_duration = Duration::from_secs(60);
    let _result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "rows_window_v1_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();
    let records_written = data_writer.get_count();

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}

async fn measure_transactional_jp(records: Vec<StreamRecord>, query: &str) -> (f64, usize) {
    let processor = JobProcessorFactory::create(JobProcessorConfig::Transactional);
    let data_source = KafkaSimulatorDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let timeout_duration = Duration::from_secs(60);
    let _result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "rows_window_transactional_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();
    let records_written = data_writer.get_count();

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}
