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
use velostream::velostream::table::{OptimizedTableImpl, UnifiedTable};

use super::super::super::test_helpers::{KafkaSimulatorDataSource, MockDataWriter};
use super::super::test_helpers::{
    create_adaptive_processor, get_perf_record_count, print_perf_config,
};

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

/// Create the market_data table for the ROWS WINDOW test
fn create_market_data_table() -> Arc<dyn UnifiedTable> {
    let mut table = OptimizedTableImpl::new();
    // Pre-populate with sample data
    for i in 0..10 {
        let mut fields = HashMap::new();
        let symbol = format!("SYM{}", i % 10);
        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert(
            "price".to_string(),
            FieldValue::Float(100.0 + (i % 50) as f64),
        );
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Integer((i * 1000) as i64),
        );
        let key = i.to_string();
        let _ = table.insert(key, fields);
    }
    Arc::new(table)
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

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn test_rows_window_performance() {
    let record_count = get_perf_record_count();
    let records = generate_rows_window_records(record_count);

    let (sql_sync_throughput, _, _) =
        measure_sql_engine_sync(records.clone(), ROWS_WINDOW_SQL).await;
    let (sql_async_throughput, _, _) = measure_sql_engine(records.clone(), ROWS_WINDOW_SQL).await;
    let (simple_jp_throughput, _) = measure_v1(records.clone(), ROWS_WINDOW_SQL).await;
    let (transactional_jp_throughput, _) =
        measure_transactional_jp(records.clone(), ROWS_WINDOW_SQL).await;
    let (adaptive_1c_throughput, _) =
        measure_adaptive_jp(records.clone(), ROWS_WINDOW_SQL, 1).await;
    let (adaptive_4c_throughput, _) =
        measure_adaptive_jp(records.clone(), ROWS_WINDOW_SQL, 4).await;

    println!(
        "🚀 BENCHMARK_RESULT | rows_window | tier1 | SQL Sync: {:.0} | SQL Async: {:.0} | SimpleJp: {:.0} | TransactionalJp: {:.0} | AdaptiveJp (1c): {:.0} | AdaptiveJp (4c): {:.0}",
        sql_sync_throughput,
        sql_async_throughput,
        simple_jp_throughput,
        transactional_jp_throughput,
        adaptive_1c_throughput,
        adaptive_4c_throughput
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

    // Create table registry
    let mut table_registry = HashMap::new();
    table_registry.insert("market_data".to_string(), create_market_data_table());

    let processor = JobProcessorFactory::create_with_config_and_tables(
        JobProcessorConfig::Simple,
        Some(config),
        Some(table_registry),
    );
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
    // Create table registry
    let mut table_registry = HashMap::new();
    table_registry.insert("market_data".to_string(), create_market_data_table());

    let processor = JobProcessorFactory::create_with_config_and_tables(
        JobProcessorConfig::Transactional,
        None,
        Some(table_registry),
    );
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

async fn measure_adaptive_jp(
    records: Vec<StreamRecord>,
    query: &str,
    num_cores: usize,
) -> (f64, usize) {
    // Create table registry
    let mut table_registry = HashMap::new();
    table_registry.insert("market_data".to_string(), create_market_data_table());

    let processor = JobProcessorFactory::create_with_config_and_tables(
        JobProcessorConfig::Adaptive {
            num_partitions: Some(num_cores),
            enable_core_affinity: false,
        },
        None,
        Some(table_registry),
    );
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
            format!("rows_window_adaptive_{}c_test", num_cores),
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
