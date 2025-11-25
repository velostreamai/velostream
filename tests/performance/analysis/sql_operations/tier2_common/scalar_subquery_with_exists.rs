//! Scalar Subquery with EXISTS Performance Benchmark - Tier 2 Operation
//!
//! **Operation #2 in STREAMING_SQL_OPERATION_RANKING.md**
//! - **Tier**: Tier 2 (Common)
//! - **Probability**: 68% of production streaming SQL jobs
//! - **Use Cases**: Filtering based on reference data existence, fraud list checks, compliance verification
//!
//! Scalar subqueries with EXISTS check if related data exists in a reference table.
//! They're common in financial analytics for conditional filtering (e.g., exclude blocked customers).
//!
//! **Test Pattern**: Measures throughput across implementations comparing:
//! - SQL Engine (sync/async baseline)
//! - SimpleJp (single-threaded best-effort)
//! - TransactionalJp (single-threaded at-least-once)
//! - AdaptiveJp (partitioned, 1-core and 4-core)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::server::v2::PartitionerSelector;
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::table::{OptimizedTableImpl, UnifiedTable};

use super::super::super::test_helpers::{KafkaSimulatorDataSource, MockDataWriter};
use super::super::test_helpers::{
    create_adaptive_processor, get_perf_record_count, print_perf_config, validate_sql_query,
};

/// Generate test data for scalar subquery with EXISTS: trades and fraud checks
fn generate_scalar_subquery_with_exists_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let trader_id = (i % 100) as i64; // 100 unique traders
            let symbol = format!("SYM{}", i % 50); // 50 symbols

            fields.insert("trade_id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("trader_id".to_string(), FieldValue::Integer(trader_id));
            fields.insert("symbol".to_string(), FieldValue::String(symbol.clone()));
            fields.insert(
                "quantity".to_string(),
                FieldValue::Integer((i % 1000) as i64),
            );
            fields.insert(
                "price".to_string(),
                FieldValue::Float(100.0 + (i % 100) as f64),
            );
            fields.insert(
                "timestamp".to_string(),
                FieldValue::Integer((i * 1000) as i64),
            );

            // Partition by trader_id for even distribution
            StreamRecord::new(fields).with_partition_from_key(&trader_id.to_string(), 32)
        })
        .collect()
}

/// Benchmark configuration
#[derive(Clone, Debug)]
struct BenchmarkResult {
    name: String,
    throughput: f64,
    records_sent: usize,
    results_produced: usize,
    duration_ms: f64,
}

/// Create the blacklist table for EXISTS checks
fn create_blacklist_table() -> Arc<dyn UnifiedTable> {
    let mut table = OptimizedTableImpl::new();
    // Pre-populate with blacklisted traders (about 10% of traders)
    for i in (0..100).step_by(10) {
        let mut fields = HashMap::new();
        fields.insert("trader_id".to_string(), FieldValue::Integer(i as i64));
        fields.insert(
            "reason".to_string(),
            FieldValue::String("compliance_flag".to_string()),
        );
        let key = i.to_string();
        let _ = table.insert(key, fields);
    }
    Arc::new(table)
}

/// SQL query using scalar subquery with EXISTS for compliance filtering
/// Selects only trades from traders NOT on the blacklist
const SCALAR_SUBQUERY_WITH_EXISTS_SQL: &str = r#"
    SELECT
        trade_id,
        trader_id,
        symbol,
        quantity,
        price
    FROM trades
    WHERE NOT EXISTS (
        SELECT 1 FROM blacklist
        WHERE blacklist.trader_id = trades.trader_id
    )
    AND quantity > 0
"#;

/// Test: Scalar subquery with EXISTS performance measurement
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn test_scalar_subquery_with_exists_performance() {
    validate_sql_query(SCALAR_SUBQUERY_WITH_EXISTS_SQL);
    let record_count = get_perf_record_count();
    let records = generate_scalar_subquery_with_exists_records(record_count);

    let (sql_sync_throughput, _, _) =
        measure_sql_engine_sync(records.clone(), SCALAR_SUBQUERY_WITH_EXISTS_SQL).await;
    let (sql_async_throughput, _, _) =
        measure_sql_engine(records.clone(), SCALAR_SUBQUERY_WITH_EXISTS_SQL).await;
    let (simple_jp_throughput, _) =
        measure_v1(records.clone(), SCALAR_SUBQUERY_WITH_EXISTS_SQL).await;
    let (transactional_jp_throughput, _) =
        measure_transactional_jp(records.clone(), SCALAR_SUBQUERY_WITH_EXISTS_SQL).await;
    let (adaptive_1c_throughput, _) =
        measure_adaptive_jp(records.clone(), SCALAR_SUBQUERY_WITH_EXISTS_SQL, 1).await;
    let (adaptive_4c_throughput, _) =
        measure_adaptive_jp(records.clone(), SCALAR_SUBQUERY_WITH_EXISTS_SQL, 4).await;

    println!(
        "🚀 BENCHMARK_RESULT | scalar_subquery_with_exists | tier2 | SQL Sync: {:.0} | SQL Async: {:.0} | SimpleJp: {:.0} | TransactionalJp: {:.0} | AdaptiveJp (1c): {:.0} | AdaptiveJp (4c): {:.0}",
        sql_sync_throughput,
        sql_async_throughput,
        simple_jp_throughput,
        transactional_jp_throughput,
        adaptive_1c_throughput,
        adaptive_4c_throughput
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
            Err(e) => {
                eprintln!("Error: {}", e);
            }
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
            Ok(()) => {
                // Message sent to channel
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }

    // Drain remaining results
    while let Ok(_) = _rx.try_recv() {
        results_produced += 1;
    }

    let elapsed = start.elapsed();
    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_sent, results_produced)
}

/// Measure SimpleJp (V1)
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
    let trades_table: Arc<dyn UnifiedTable> = Arc::new(OptimizedTableImpl::new());
    table_registry.insert("trades".to_string(), trades_table);
    table_registry.insert("blacklist".to_string(), create_blacklist_table());

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
            "scalar_exists_v1_test".to_string(),
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

/// Measure TransactionalJp
async fn measure_transactional_jp(records: Vec<StreamRecord>, query: &str) -> (f64, usize) {
    // Create table registry
    let mut table_registry = HashMap::new();
    let trades_table: Arc<dyn UnifiedTable> = Arc::new(OptimizedTableImpl::new());
    table_registry.insert("trades".to_string(), trades_table);
    table_registry.insert("blacklist".to_string(), create_blacklist_table());

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
            "scalar_exists_transactional_test".to_string(),
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
    let trades_table: Arc<dyn UnifiedTable> = Arc::new(OptimizedTableImpl::new());
    table_registry.insert("trades".to_string(), trades_table);
    table_registry.insert("blacklist".to_string(), create_blacklist_table());

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
            format!("scalar_subquery_exists_adaptive_{}c_test", num_cores),
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
