//! Time-Based JOIN (WITHIN) Performance Benchmark - Tier 2 Operation
//!
//! **Operation #8 in STREAMING_SQL_OPERATION_RANKING.md**
//! - **Tier**: Tier 2 (Common)
//! - **Probability**: 68% of production streaming SQL jobs
//! - **Use Cases**: Temporal correlation, event matching, sequence detection
//!
//! Time-based JOINs match events from two streams within a time window.
//! They're critical for detecting related activities (e.g., order + payment within 5 minutes).
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

/// Generate test data for time-based JOIN: orders and payments streams
fn generate_timebased_join_records(count: usize) -> (Vec<StreamRecord>, Vec<StreamRecord>) {
    let orders: Vec<StreamRecord> = (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let order_id = (i % 1000) as i64; // 1000 unique orders
            let customer_id = (i % 500) as i64; // 500 customers
            let amount = 100.0 + (i % 1000) as f64;
            let timestamp = (i * 100) as i64; // 100ms apart

            fields.insert("order_id".to_string(), FieldValue::Integer(order_id));
            fields.insert("customer_id".to_string(), FieldValue::Integer(customer_id));
            fields.insert("order_amount".to_string(), FieldValue::Float(amount));
            fields.insert(
                "order_timestamp".to_string(),
                FieldValue::Integer(timestamp),
            );

            StreamRecord::new(fields).with_partition_from_key(&customer_id.to_string(), 32)
        })
        .collect();

    let payments: Vec<StreamRecord> = (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let order_id = (i % 1000) as i64; // Match with orders
            let payment_amount = 100.0 + (i % 1000) as f64;
            // Payments arrive slightly after orders (offset by 50ms)
            let timestamp = (i * 100 + 50) as i64;

            fields.insert("order_id".to_string(), FieldValue::Integer(order_id));
            fields.insert(
                "payment_amount".to_string(),
                FieldValue::Float(payment_amount),
            );
            fields.insert(
                "payment_timestamp".to_string(),
                FieldValue::Integer(timestamp),
            );

            StreamRecord::new(fields).with_partition_from_key(&order_id.to_string(), 32)
        })
        .collect();

    (orders, payments)
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

/// SQL query for time-based JOIN: orders within 5 minutes of payments
/// Simulates matching order and payment events
const TIMEBASED_JOIN_SQL: &str = r#"
    SELECT
        o.order_id,
        o.customer_id,
        o.order_amount,
        p.payment_amount,
        (p.payment_amount - o.order_amount) as difference
    FROM orders o
    JOIN payments p ON o.order_id = p.order_id
    WHERE ABS(p.payment_timestamp - o.order_timestamp) < 300000
"#;

/// Test: Time-based JOIN performance measurement
#[tokio::test]
#[serial_test::serial]
async fn test_timebased_join_performance() {
    println!("\n🚀 Time-Based JOIN (WITHIN) Performance Benchmark");
    println!("═════════════════════════════════════════════════");
    println!("Operation #8: Tier 2 (68% probability)");
    println!("Use Case: Temporal correlation, event matching");
    println!();

    let record_count = get_perf_record_count();
    let (orders, payments) = generate_timebased_join_records(record_count);

    println!("📊 Configuration:");
    print_perf_config(record_count, None);
    println!(
        "   Two-stream JOIN: {} + {} = {} total records",
        record_count,
        record_count,
        record_count * 2
    );
    println!("   Query: Time-based JOIN with 5-minute window");
    println!("   Match Cardinality: 1000 unique order IDs");
    println!();

    // Measure SQL Engine (sync baseline)
    let start = Instant::now();
    let (sql_sync_throughput, sql_sync_sent, sql_sync_produced) =
        measure_sql_engine_sync(orders.clone(), payments.clone(), TIMEBASED_JOIN_SQL).await;
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
        measure_sql_engine(orders.clone(), payments.clone(), TIMEBASED_JOIN_SQL).await;
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
    let (simple_jp_throughput, simple_jp_produced) =
        measure_v1(orders.clone(), payments.clone(), TIMEBASED_JOIN_SQL).await;
    let simple_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SimpleJp:");
    println!("   Throughput: {:.0} rec/sec", simple_jp_throughput);
    println!("   Results: {}", simple_jp_produced);
    println!("   Time: {:.2}ms", simple_jp_ms);
    println!();

    // Measure TransactionalJp
    let start = Instant::now();
    let (transactional_jp_throughput, transactional_jp_produced) =
        measure_transactional_jp(orders.clone(), payments.clone(), TIMEBASED_JOIN_SQL).await;
    let transactional_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ TransactionalJp:");
    println!("   Throughput: {:.0} rec/sec", transactional_jp_throughput);
    println!("   Results: {}", transactional_jp_produced);
    println!("   Time: {:.2}ms", transactional_jp_ms);
    println!();

    let start = Instant::now();
    let (adaptive_1c_throughput, adaptive_1c_produced) =
        measure_adaptive_jp(orders.clone(), TIMEBASED_JOIN_SQL, 1).await;
    let adaptive_1c_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ AdaptiveJp (1 core):");
    println!("   Throughput: {:.0} rec/sec", adaptive_1c_throughput);
    println!("   Results: {}", adaptive_1c_produced);
    println!("   Time: {:.2}ms", adaptive_1c_ms);
    println!();

    let start = Instant::now();
    let (adaptive_4c_throughput, adaptive_4c_produced) =
        measure_adaptive_jp(orders.clone(), TIMEBASED_JOIN_SQL, 4).await;
    let adaptive_4c_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ AdaptiveJp (4 cores):");
    println!("   Throughput: {:.0} rec/sec", adaptive_4c_throughput);
    println!("   Results: {}", adaptive_4c_produced);
    println!("   Time: {:.2}ms", adaptive_4c_ms);
    println!();

    // Summary
    println!("📊 Summary:");
    println!("─────────────────────────────────────────────────");
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

    // Performance assertion: Time-based JOINs should achieve >8K rec/sec
    assert!(
        best.1 > 8_000.0,
        "Time-based JOIN performance below threshold: {:.0} rec/sec",
        best.1
    );
}

/// Measure SQL Engine (sync version) with two streams
async fn measure_sql_engine_sync(
    orders: Vec<StreamRecord>,
    payments: Vec<StreamRecord>,
    query: &str,
) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (_tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(_tx);

    let mut records_sent = 0;
    let mut results_produced = 0;

    let start = Instant::now();
    // Process orders first
    for record in orders.iter() {
        records_sent += 1;
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                results_produced += results.len();
            }
            Err(_e) => {
                // Expected: no matching payments yet
            }
        }
    }
    // Then process payments
    for record in payments.iter() {
        records_sent += 1;
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                results_produced += results.len();
            }
            Err(_e) => {
                // Expected: might not match
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = (records_sent as f64) / elapsed.as_secs_f64();
    (throughput, records_sent, results_produced)
}

/// Measure SQL Engine (async version) with two streams
async fn measure_sql_engine(
    orders: Vec<StreamRecord>,
    payments: Vec<StreamRecord>,
    query: &str,
) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (_tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(_tx);

    let mut records_sent = 0;
    let mut results_produced = 0;

    let start = Instant::now();
    // Process orders first
    for record in orders.iter() {
        records_sent += 1;
        match engine.execute_with_record(&parsed_query, record).await {
            Ok(()) => {
                // Message sent to channel
            }
            Err(_e) => {
                // Expected: no matching payments yet
            }
        }
    }
    // Then process payments
    for record in payments.iter() {
        records_sent += 1;
        match engine.execute_with_record(&parsed_query, record).await {
            Ok(()) => {
                // Message sent to channel
            }
            Err(_e) => {
                // Expected: might not match
            }
        }
    }

    // Drain remaining results
    while let Ok(_) = _rx.try_recv() {
        results_produced += 1;
    }

    let elapsed = start.elapsed();
    let throughput = (records_sent as f64) / elapsed.as_secs_f64();
    (throughput, records_sent, results_produced)
}

/// Measure SimpleJp (V1) with two streams
async fn measure_v1(
    orders: Vec<StreamRecord>,
    payments: Vec<StreamRecord>,
    query: &str,
) -> (f64, usize) {
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

    // Combine orders and payments, keeping track of order
    let mut combined = Vec::new();
    for order in orders.iter() {
        combined.push(order.clone());
    }
    for payment in payments.iter() {
        combined.push(payment.clone());
    }

    let combined_len = combined.len();
    let data_source = KafkaSimulatorDataSource::new(combined, 100);
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
            "timebased_join_v1_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();
    let records_written = data_writer.get_count();

    let throughput = (combined_len as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}

/// Measure TransactionalJp with two streams
async fn measure_transactional_jp(
    orders: Vec<StreamRecord>,
    payments: Vec<StreamRecord>,
    query: &str,
) -> (f64, usize) {
    let processor = JobProcessorFactory::create(JobProcessorConfig::Transactional);

    // Combine orders and payments
    let mut combined = Vec::new();
    for order in orders.iter() {
        combined.push(order.clone());
    }
    for payment in payments.iter() {
        combined.push(payment.clone());
    }

    let combined_len = combined.len();
    let data_source = KafkaSimulatorDataSource::new(combined, 100);
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
            "timebased_join_transactional_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();
    let records_written = data_writer.get_count();

    let throughput = (combined_len as f64) / elapsed.as_secs_f64();
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
            format!("timebased_join_adaptive_{}c_test", num_cores),
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
