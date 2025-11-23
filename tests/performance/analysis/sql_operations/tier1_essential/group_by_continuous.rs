//! GROUP BY (Continuous) Performance Benchmark - Tier 1 Operation
//!
//! **Operation #4 in STREAMING_SQL_OPERATION_RANKING.md**
//! - **Tier**: Tier 1 (Essential)
//! - **Probability**: 93% of production streaming SQL jobs
//! - **Use Cases**: Continuous aggregation, statistical summaries, group analytics
//!
//! Continuous GROUP BY without windowing is one of the most fundamental streaming SQL operations.
//! It aggregates streams by group keys without temporal boundaries.

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
    create_adaptive_processor, get_perf_cardinality, get_perf_record_count, print_perf_config,
};

/// Generate test data: market data with multiple symbols and prices
/// Cardinality is configurable via VELOSTREAM_PERF_CARDINALITY
fn generate_group_by_continuous_records(count: usize, cardinality: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let symbol = format!("SYM{}", i % cardinality);
            fields.insert("symbol".to_string(), FieldValue::String(symbol.clone()));
            fields.insert(
                "price".to_string(),
                FieldValue::Float(100.0 + (i % 100) as f64),
            );
            fields.insert(
                "quantity".to_string(),
                FieldValue::Integer((i % 1000) as i64),
            );
            // Partition by symbol: 200 unique symbols → excellent distribution across 32 partitions
            StreamRecord::new(fields).with_partition_from_key(&symbol, 32)
        })
        .collect()
}

/// Create the market_data table for the GROUP BY test
fn create_market_data_table() -> Arc<dyn UnifiedTable> {
    let mut table = OptimizedTableImpl::new();
    // Pre-populate with a small sample for reference lookups
    for i in 0..10 {
        let mut fields = HashMap::new();
        let symbol = format!("SYM{}", i);
        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert(
            "price".to_string(),
            FieldValue::Float(100.0 + (i % 100) as f64),
        );
        fields.insert(
            "quantity".to_string(),
            FieldValue::Integer((i % 1000) as i64),
        );
        let key = i.to_string();
        let _ = table.insert(key, fields);
    }
    Arc::new(table)
}

const GROUP_BY_CONTINUOUS_SQL: &str = r#"
    SELECT symbol,
        COUNT(*) as trade_count,
        AVG(price) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        SUM(quantity) as total_quantity
    FROM market_data
    GROUP BY symbol
"#;

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn test_group_by_continuous_performance() {
    let record_count = get_perf_record_count();
    let cardinality = get_perf_cardinality(record_count);
    let records = generate_group_by_continuous_records(record_count, cardinality);

    let (sql_sync_throughput, _, _) =
        measure_sql_engine_sync(records.clone(), GROUP_BY_CONTINUOUS_SQL).await;
    let (sql_async_throughput, _, _) =
        measure_sql_engine(records.clone(), GROUP_BY_CONTINUOUS_SQL).await;
    let (simple_jp_throughput, _) = measure_v1(records.clone(), GROUP_BY_CONTINUOUS_SQL).await;
    let (transactional_jp_throughput, _) =
        measure_transactional_jp(records.clone(), GROUP_BY_CONTINUOUS_SQL).await;
    let (adaptive_1c_throughput, _) =
        measure_adaptive_jp(records.clone(), GROUP_BY_CONTINUOUS_SQL, 1).await;
    let (adaptive_4c_throughput, _) =
        measure_adaptive_jp(records.clone(), GROUP_BY_CONTINUOUS_SQL, 4).await;

    println!(
        "🚀 BENCHMARK_RESULT | group_by_continuous | tier1 | SQL Sync: {:.0} | SQL Async: {:.0} | SimpleJp: {:.0} | TransactionalJp: {:.0} | AdaptiveJp (1c): {:.0} | AdaptiveJp (4c): {:.0}",
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
            "group_by_continuous_v1_test".to_string(),
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
            "group_by_continuous_transactional_test".to_string(),
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
            format!("group_by_continuous_adaptive_{}c_test", num_cores),
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
