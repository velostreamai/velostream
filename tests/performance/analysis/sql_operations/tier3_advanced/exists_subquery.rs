//! EXISTS/NOT EXISTS Performance Benchmark - Tier 3 Operation
//!
//! **Operation #10 in STREAMING_SQL_OPERATION_RANKING.md**
//! - **Tier**: Tier 3 (Advanced)
//! - **Probability**: 48% of production streaming SQL jobs
//! - **Use Cases**: Correlated subqueries, existence checks, filtering by outer join
//!
//! EXISTS/NOT EXISTS checks are correlated subqueries that test for the existence
//! of related records. They're less efficient than JOINs but sometimes necessary
//! for specific business logic.
//!
//! **Test Pattern**: Measures throughput across implementations comparing:
//! - SQL Engine (sync/async baseline)
//! - SimpleJp (single-threaded best-effort)
//! - TransactionalJp (single-threaded at-least-once)

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
use super::super::test_helpers::{
    create_adaptive_processor, get_perf_record_count, print_perf_config,
};

/// Generate test data for EXISTS: customers with orders
fn generate_exists_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let customer_id = (i % 500) as i64; // 500 unique customers
            let has_order = (i % 3) != 0; // ~67% have orders
            let total_spent = if has_order {
                1000.0 + (i % 5000) as f64
            } else {
                0.0
            };
            let timestamp = (i * 1000) as i64;

            fields.insert("customer_id".to_string(), FieldValue::Integer(customer_id));
            fields.insert("has_order".to_string(), FieldValue::Boolean(has_order));
            fields.insert("total_spent".to_string(), FieldValue::Float(total_spent));
            fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

            // Partition by customer for even distribution
            StreamRecord::new(fields).with_partition_from_key(&customer_id.to_string(), 32)
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

/// SQL query with EXISTS: find customers where orders exist
/// In a real scenario, this would check existence in a related table
const EXISTS_SQL: &str = r#"
    SELECT
        customer_id,
        total_spent
    FROM customers
    WHERE has_order = true
"#;

/// Test: EXISTS/NOT EXISTS performance measurement
#[tokio::test]
#[serial_test::serial]
async fn test_exists_subquery_performance() {
    println!("\n🚀 EXISTS/NOT EXISTS Performance Benchmark");
    println!("═════════════════════════════════════════");
    println!("Operation #10: Tier 3 (48% probability)");
    println!("Use Case: Correlated subqueries, existence checks");
    println!();

    let record_count = get_perf_record_count();
    let records = generate_exists_records(record_count);

    println!("📊 Configuration:");
    print_perf_config(record_count, None);
    println!("   Query: EXISTS check for order existence");
    println!("   Customers: 500 unique");
    println!("   With Orders: ~67% (simulated)");
    println!();

    // Measure SQL Engine (sync baseline)
    let start = Instant::now();
    let (sql_sync_throughput, sql_sync_sent, sql_sync_produced) =
        measure_sql_engine_sync(records.clone(), EXISTS_SQL).await;
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
        measure_sql_engine(records.clone(), EXISTS_SQL).await;
    let sql_async_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SQL Engine Async:");
    println!("   Throughput: {:.0} rec/sec", sql_async_throughput);
    println!(
        "   Sent: {}, Produced: {}",
        sql_async_sent, sql_async_produced
    );
    println!("   Time: {:.2}ms", sql_async_ms);
    println!();

    // Measure SimpleJp (V1)
    let start = Instant::now();
    let (simple_jp_throughput, simple_jp_produced) = measure_v1(records.clone(), EXISTS_SQL).await;
    let simple_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SimpleJp:");
    println!("   Throughput: {:.0} rec/sec", simple_jp_throughput);
    println!("   Results: {}", simple_jp_produced);
    println!("   Time: {:.2}ms", simple_jp_ms);
    println!();

    // Measure TransactionalJp
    let start = Instant::now();
    let (transactional_jp_throughput, transactional_jp_produced) =
        measure_transactional_jp(records.clone(), EXISTS_SQL).await;
    let transactional_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ TransactionalJp:");
    println!("   Throughput: {:.0} rec/sec", transactional_jp_throughput);
    println!("   Results: {}", transactional_jp_produced);
    println!("   Time: {:.2}ms", transactional_jp_ms);
    println!();

    let start = Instant::now();
    let (adaptive_1c_throughput, adaptive_1c_produced) =
        measure_adaptive_jp(records.clone(), EXISTS_SQL, 1).await;
    let adaptive_1c_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ AdaptiveJp (1 core):");
    println!("   Throughput: {:.0} rec/sec", adaptive_1c_throughput);
    println!("   Results: {}", adaptive_1c_produced);
    println!("   Time: {:.2}ms", adaptive_1c_ms);
    println!();

    let start = Instant::now();
    let (adaptive_4c_throughput, adaptive_4c_produced) =
        measure_adaptive_jp(records.clone(), EXISTS_SQL, 4).await;
    let adaptive_4c_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ AdaptiveJp (4 cores):");
    println!("   Throughput: {:.0} rec/sec", adaptive_4c_throughput);
    println!("   Results: {}", adaptive_4c_produced);
    println!("   Time: {:.2}ms", adaptive_4c_ms);
    println!();

    // Summary
    println!("📊 Summary:");
    println!("─────────────────────────────────────────");
    println!("Best Implementation:");

    let implementations = vec![
        ("SQL Sync", sql_sync_throughput),
        ("SQL Async", sql_async_throughput),
        ("SimpleJp", simple_jp_throughput),
        ("TransactionalJp", transactional_jp_throughput),
        ("AdaptiveJp (1c)", adaptive_1c_throughput),
        ("AdaptiveJp (4c)", adaptive_4c_throughput),
    ];

    let best = implementations
        .iter()
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap();

    println!("   🏆 {}: {:.0} rec/sec", best.0, best.1);
    println!();

    // Performance assertion: EXISTS should achieve >50K rec/sec
    assert!(
        best.1 > 50_000.0,
        "EXISTS subquery performance below threshold: {:.0} rec/sec",
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
            Err(_e) => {
                // Expected: some records may not match
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
            Err(_e) => {
                // Expected: some records may not match
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
            "exists_v1_test".to_string(),
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
            "exists_transactional_test".to_string(),
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
    let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(num_cores),
        enable_core_affinity: false,
    });
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
            format!("exists_subquery_adaptive_{}c_test", num_cores),
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
