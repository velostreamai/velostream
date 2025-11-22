//! Stream-Stream JOIN Performance Benchmark - Tier 3 Operation
//!
//! **Operation #11 in STREAMING_SQL_OPERATION_RANKING.md**
//! - **Tier**: Tier 3 (Advanced)
//! - **Probability**: 42% of production streaming SQL jobs
//! - **Use Cases**: Correlated events, pattern detection, sequence correlation
//!
//! Stream-Stream JOINs are the most memory-intensive operation, requiring buffering
//! both streams to find matching events. Critical window size for memory management.
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
use velostream::velostream::table::{OptimizedTableImpl, UnifiedTable};

use super::super::super::test_helpers::{KafkaSimulatorDataSource, MockDataWriter};
use super::super::test_helpers::{
    create_adaptive_processor, get_perf_record_count, print_perf_config,
};

/// Generate test data for Stream-Stream JOIN: click and purchase events
fn generate_stream_stream_join_records(count: usize) -> (Vec<StreamRecord>, Vec<StreamRecord>) {
    let clicks: Vec<StreamRecord> = (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let user_id = (i % 200) as i64; // 200 unique users
            let product_id = (i % 100) as i64; // 100 products
            let timestamp = (i * 50) as i64; // 50ms apart

            fields.insert("user_id".to_string(), FieldValue::Integer(user_id));
            fields.insert("product_id".to_string(), FieldValue::Integer(product_id));
            fields.insert(
                "click_timestamp".to_string(),
                FieldValue::Integer(timestamp),
            );

            StreamRecord::new(fields).with_partition_from_key(&user_id.to_string(), 32)
        })
        .collect();

    let purchases: Vec<StreamRecord> = (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let user_id = (i % 200) as i64; // Match with clicks
            let product_id = (i % 100) as i64;
            // Purchases arrive slightly after clicks (offset by 1000ms)
            let timestamp = (i * 50 + 1000) as i64;
            let amount = 50.0 + (i % 500) as f64;

            fields.insert("user_id".to_string(), FieldValue::Integer(user_id));
            fields.insert("product_id".to_string(), FieldValue::Integer(product_id));
            fields.insert(
                "purchase_timestamp".to_string(),
                FieldValue::Integer(timestamp),
            );
            fields.insert("amount".to_string(), FieldValue::Float(amount));

            StreamRecord::new(fields).with_partition_from_key(&user_id.to_string(), 32)
        })
        .collect();

    (clicks, purchases)
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

/// Create the clicks table for the stream-stream join test
fn create_clicks_table() -> Arc<dyn UnifiedTable> {
    let mut table = OptimizedTableImpl::new();
    for i in 0..10 {
        let mut fields = HashMap::new();
        let user_id = (i % 20) as i64;
        let product_id = (i % 10) as i64;
        fields.insert("user_id".to_string(), FieldValue::Integer(user_id));
        fields.insert("product_id".to_string(), FieldValue::Integer(product_id));
        fields.insert(
            "click_timestamp".to_string(),
            FieldValue::Integer((i * 50) as i64),
        );
        let key = i.to_string();
        let _ = table.insert(key, fields);
    }
    Arc::new(table)
}

/// Create the purchases table for the stream-stream join test
fn create_purchases_table() -> Arc<dyn UnifiedTable> {
    let mut table = OptimizedTableImpl::new();
    for i in 0..10 {
        let mut fields = HashMap::new();
        let user_id = (i % 20) as i64;
        let product_id = (i % 10) as i64;
        fields.insert("user_id".to_string(), FieldValue::Integer(user_id));
        fields.insert("product_id".to_string(), FieldValue::Integer(product_id));
        fields.insert(
            "purchase_timestamp".to_string(),
            FieldValue::Integer((i * 50 + 1000) as i64),
        );
        fields.insert(
            "amount".to_string(),
            FieldValue::Float(50.0 + (i % 50) as f64),
        );
        let key = i.to_string();
        let _ = table.insert(key, fields);
    }
    Arc::new(table)
}

/// SQL query for Stream-Stream JOIN: clicks matched with purchases within 30 seconds
const STREAM_STREAM_JOIN_SQL: &str = r#"
    SELECT
        c.user_id,
        c.product_id,
        p.amount,
        (p.purchase_timestamp - c.click_timestamp) as time_to_purchase
    FROM clicks c
    JOIN purchases p ON c.user_id = p.user_id
      AND c.product_id = p.product_id
    WHERE p.purchase_timestamp - c.click_timestamp < 30000
"#;

/// Test: Stream-Stream JOIN performance measurement
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn test_stream_stream_join_performance() {
    println!("\n🚀 Stream-Stream JOIN Performance Benchmark");
    println!("════════════════════════════════════════════");
    println!("Operation #11: Tier 3 (42% probability)");
    println!("Use Case: Correlated events, pattern detection");
    println!("⚠️  WARNING: Memory-intensive operation!");
    println!();

    let record_count = get_perf_record_count();
    let (clicks, purchases) = generate_stream_stream_join_records(record_count);

    println!("📊 Configuration:");
    print_perf_config(record_count, None);
    println!(
        "   Two-stream JOIN: {} + {} = {} total records",
        record_count,
        record_count,
        record_count * 2
    );
    println!("   Query: Stream-Stream JOIN on user + product within 30s");
    println!("   Match Cardinality: 200 users × 100 products");
    println!("   Window: 30 seconds (30,000ms)");
    println!();

    // Measure SQL Engine (sync baseline)
    let start = Instant::now();
    let (sql_sync_throughput, sql_sync_sent, sql_sync_produced) =
        measure_sql_engine_sync(clicks.clone(), purchases.clone(), STREAM_STREAM_JOIN_SQL).await;
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
        measure_sql_engine(clicks.clone(), purchases.clone(), STREAM_STREAM_JOIN_SQL).await;
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
        measure_v1(clicks.clone(), purchases.clone(), STREAM_STREAM_JOIN_SQL).await;
    let simple_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SimpleJp:");
    println!("   Throughput: {:.0} rec/sec", simple_jp_throughput);
    println!("   Results: {}", simple_jp_produced);
    println!("   Time: {:.2}ms", simple_jp_ms);
    println!();

    // Measure TransactionalJp
    let start = Instant::now();
    let (transactional_jp_throughput, transactional_jp_produced) =
        measure_transactional_jp(clicks.clone(), purchases.clone(), STREAM_STREAM_JOIN_SQL).await;
    let transactional_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ TransactionalJp:");
    println!("   Throughput: {:.0} rec/sec", transactional_jp_throughput);
    println!("   Results: {}", transactional_jp_produced);
    println!("   Time: {:.2}ms", transactional_jp_ms);
    println!();

    let start = Instant::now();
    let (adaptive_1c_throughput, adaptive_1c_produced) =
        measure_adaptive_jp(clicks.clone(), purchases.clone(), STREAM_STREAM_JOIN_SQL, 1).await;
    let adaptive_1c_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ AdaptiveJp (1 core):");
    println!("   Throughput: {:.0} rec/sec", adaptive_1c_throughput);
    println!("   Results: {}", adaptive_1c_produced);
    println!("   Time: {:.2}ms", adaptive_1c_ms);
    println!();

    let start = Instant::now();
    let (adaptive_4c_throughput, adaptive_4c_produced) =
        measure_adaptive_jp(clicks.clone(), purchases.clone(), STREAM_STREAM_JOIN_SQL, 4).await;
    let adaptive_4c_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ AdaptiveJp (4 cores):");
    println!("   Throughput: {:.0} rec/sec", adaptive_4c_throughput);
    println!("   Results: {}", adaptive_4c_produced);
    println!("   Time: {:.2}ms", adaptive_4c_ms);
    println!();

    // Summary
    println!("📊 Summary:");
    println!("─────────────────────────────────────────────");
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

    // Performance assertion: Stream-Stream JOIN should achieve >3K rec/sec
    assert!(
        best.1 > 3_000.0,
        "Stream-Stream JOIN performance below threshold: {:.0} rec/sec",
        best.1
    );
}

/// Measure SQL Engine (sync version) with two streams
async fn measure_sql_engine_sync(
    clicks: Vec<StreamRecord>,
    purchases: Vec<StreamRecord>,
    query: &str,
) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (_tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(_tx);

    let mut records_sent = 0;
    let mut results_produced = 0;

    let start = Instant::now();
    // Process clicks first
    for record in clicks.iter() {
        records_sent += 1;
        match engine.execute_with_record_sync(&parsed_query, record) {
            Ok(results) => {
                results_produced += results.len();
            }
            Err(_e) => {
                // Expected: no matching purchases yet
            }
        }
    }
    // Then process purchases
    for record in purchases.iter() {
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
    clicks: Vec<StreamRecord>,
    purchases: Vec<StreamRecord>,
    query: &str,
) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (_tx, mut _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(_tx);

    let mut records_sent = 0;
    let mut results_produced = 0;

    let start = Instant::now();
    // Process clicks first
    for record in clicks.iter() {
        records_sent += 1;
        match engine.execute_with_record(&parsed_query, record).await {
            Ok(()) => {
                // Message sent to channel
            }
            Err(_e) => {
                // Expected: no matching purchases yet
            }
        }
    }
    // Then process purchases
    for record in purchases.iter() {
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
    clicks: Vec<StreamRecord>,
    purchases: Vec<StreamRecord>,
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

    // Create table registry
    let mut table_registry = HashMap::new();
    table_registry.insert("clicks".to_string(), create_clicks_table());
    table_registry.insert("purchases".to_string(), create_purchases_table());

    let processor = JobProcessorFactory::create_with_config_and_tables(
        JobProcessorConfig::Simple,
        Some(config),
        Some(table_registry),
    );

    // Combine clicks and purchases
    let mut combined = Vec::new();
    for click in clicks.iter() {
        combined.push(click.clone());
    }
    for purchase in purchases.iter() {
        combined.push(purchase.clone());
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
            "stream_stream_join_v1_test".to_string(),
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
    clicks: Vec<StreamRecord>,
    purchases: Vec<StreamRecord>,
    query: &str,
) -> (f64, usize) {
    // Create table registry
    let mut table_registry = HashMap::new();
    table_registry.insert("clicks".to_string(), create_clicks_table());
    table_registry.insert("purchases".to_string(), create_purchases_table());

    let processor = JobProcessorFactory::create_with_config_and_tables(
        JobProcessorConfig::Transactional,
        None,
        Some(table_registry),
    );

    // Combine clicks and purchases
    let mut combined = Vec::new();
    for click in clicks.iter() {
        combined.push(click.clone());
    }
    for purchase in purchases.iter() {
        combined.push(purchase.clone());
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
            "stream_stream_join_transactional_test".to_string(),
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
    clicks: Vec<StreamRecord>,
    purchases: Vec<StreamRecord>,
    query: &str,
    num_cores: usize,
) -> (f64, usize) {
    // Create table registry
    let mut table_registry = HashMap::new();
    table_registry.insert("clicks".to_string(), create_clicks_table());
    table_registry.insert("purchases".to_string(), create_purchases_table());

    let processor = JobProcessorFactory::create_with_config_and_tables(
        JobProcessorConfig::Adaptive {
            num_partitions: Some(num_cores),
            enable_core_affinity: false,
        },
        None,
        Some(table_registry),
    );
    // Combine both streams for data source
    let mut combined = clicks.clone();
    combined.extend(purchases.clone());
    let data_source = KafkaSimulatorDataSource::new(combined.clone(), 100);
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
            format!("stream_stream_join_adaptive_{}c_test", num_cores),
            shutdown_rx,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();
    let records_written = data_writer.get_count();

    let throughput = (combined.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}
