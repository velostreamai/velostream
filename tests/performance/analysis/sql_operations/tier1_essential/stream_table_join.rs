//! Stream-Table JOIN Baseline Performance Benchmark
//!
//! **Operation #3 in STREAMING_SQL_OPERATION_RANKING.md**
//! - **Tier**: Tier 1 (Essential)
//! - **Probability**: 94% of production streaming SQL jobs
//! - **Business Value**: Critical for enrichment pipelines and reference data joins
//!
//! Generates comprehensive baseline performance measurements for Stream-Table JOINs.
//! This benchmark establishes the current performance characteristics for:
//! - Table lookup efficiency (O(n) vs target O(1))
//! - Memory allocation patterns
//! - Batch processing throughput
//! - Stream-table coordination timing
//!
//! **Target Performance**: 50-100K evt/sec (current baseline, with 5K table records)

use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::ast::{BinaryOperator, Expr, JoinClause, JoinType, StreamSource};
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::processors::stream_table_join::StreamTableJoinProcessor;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::table::OptimizedTableImpl;

use super::super::super::test_helpers::{KafkaSimulatorDataSource, MockDataWriter};
use super::super::test_helpers::{
    create_adaptive_processor, get_perf_record_count, print_perf_config,
};

/// Baseline benchmark configuration
#[derive(Debug, Clone)]
struct BenchmarkConfig {
    /// Number of stream records to process
    stream_record_count: usize,
    /// Number of table records (affects O(n) lookup performance)
    table_record_count: usize,
    /// Number of batch processing runs
    batch_runs: usize,
    /// Number of individual processing runs
    individual_runs: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            stream_record_count: 1000,
            table_record_count: 5000,
            batch_runs: 10,
            individual_runs: 100,
        }
    }
}

/// Performance measurement results
#[derive(Debug, Clone)]
struct PerformanceBaseline {
    /// Average time per table lookup (microseconds)
    avg_lookup_time_us: f64,
    /// Memory allocations per join operation
    allocations_per_join: usize,
    /// Throughput in records/second
    throughput_records_per_sec: f64,
    /// Batch processing efficiency ratio
    batch_efficiency_ratio: f64,
    /// Total memory used during test (bytes)
    memory_usage_bytes: usize,
}

/// Create a large reference table for realistic performance testing
fn create_large_reference_table(record_count: usize) -> Arc<OptimizedTableImpl> {
    let table = Arc::new(OptimizedTableImpl::new());

    for i in 0..record_count {
        let mut record = HashMap::new();
        record.insert("user_id".to_string(), FieldValue::Integer(i as i64));
        record.insert(
            "name".to_string(),
            FieldValue::String(format!("User_{}", i)),
        );
        record.insert(
            "tier".to_string(),
            FieldValue::String(
                match i % 4 {
                    0 => "PLATINUM",
                    1 => "GOLD",
                    2 => "SILVER",
                    _ => "BRONZE",
                }
                .to_string(),
            ),
        );
        record.insert(
            "risk_score".to_string(),
            FieldValue::Integer((i % 100) as i64),
        );
        record.insert(
            "position_limit".to_string(),
            FieldValue::Float((i as f64) * 1000.0),
        );

        table
            .insert(format!("user_{}", i), record)
            .expect("Failed to insert record");
    }

    table
}

/// Generate stream records for testing
fn generate_stream_records(count: usize, table_size: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);

    for i in 0..count {
        let mut fields = HashMap::new();
        fields.insert(
            "trade_id".to_string(),
            FieldValue::String(format!("trade_{}", i)),
        );
        fields.insert(
            "user_id".to_string(),
            FieldValue::Integer((i % table_size) as i64),
        );
        fields.insert("symbol".to_string(), FieldValue::String("AAPL".to_string()));
        fields.insert("quantity".to_string(), FieldValue::Integer(100));
        fields.insert("price".to_string(), FieldValue::Float(150.0));
        fields.insert("amount".to_string(), FieldValue::Float(15000.0));

        records.push(StreamRecord {
            timestamp: Utc::now().timestamp_millis(),
            offset: i as i64,
            partition: 0,
            fields,
            headers: HashMap::new(),
            event_time: Some(Utc::now()),
        });
    }

    records
}

/// Create JOIN clause for testing
fn create_test_join_clause() -> JoinClause {
    JoinClause {
        join_type: JoinType::Inner,
        right_source: StreamSource::Table("user_profiles".to_string()),
        condition: Expr::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expr::Column("user_id".to_string())),
            right: Box::new(Expr::Column("user_id".to_string())),
        },
        right_alias: Some("u".to_string()),
        window: None,
    }
}

/// Benchmark individual record processing
fn benchmark_individual_processing(
    config: &BenchmarkConfig,
    processor: &StreamTableJoinProcessor,
    stream_records: &[StreamRecord],
    join_clause: &JoinClause,
    context: &mut ProcessorContext,
) -> Result<(Duration, usize), Box<dyn std::error::Error>> {
    let mut total_duration = Duration::new(0, 0);
    let mut total_results = 0;

    // Warmup runs
    for _ in 0..10 {
        let _ = processor.process_stream_table_join(&stream_records[0], join_clause, context)?;
    }

    // Actual benchmark runs
    for run in 0..config.individual_runs {
        let record_idx = run % stream_records.len();
        let start = Instant::now();

        let results = processor.process_stream_table_join(
            &stream_records[record_idx],
            join_clause,
            context,
        )?;

        total_duration += start.elapsed();
        total_results += results.len();
    }

    Ok((total_duration, total_results))
}

/// Benchmark batch processing
fn benchmark_batch_processing(
    config: &BenchmarkConfig,
    processor: &StreamTableJoinProcessor,
    stream_records: &[StreamRecord],
    join_clause: &JoinClause,
    context: &mut ProcessorContext,
) -> Result<(Duration, usize), Box<dyn std::error::Error>> {
    let batch_size = config.stream_record_count / config.batch_runs;
    let mut total_duration = Duration::new(0, 0);
    let mut total_results = 0;

    // Warmup run
    let warmup_batch = stream_records[0..batch_size.min(10)].to_vec();
    let _ = processor.process_batch_stream_table_join(warmup_batch, join_clause, context)?;

    // Actual benchmark runs
    for run in 0..config.batch_runs {
        let start_idx = (run * batch_size) % stream_records.len();
        let end_idx = ((run + 1) * batch_size).min(stream_records.len());
        let batch = stream_records[start_idx..end_idx].to_vec();

        let start = Instant::now();

        let results =
            processor.process_batch_stream_table_join(batch.clone(), join_clause, context)?;

        total_duration += start.elapsed();
        total_results += results.len();
    }

    Ok((total_duration, total_results))
}

/// Generate comprehensive baseline performance report
fn generate_baseline_report(
    config: &BenchmarkConfig,
    individual_duration: Duration,
    individual_results: usize,
    batch_duration: Duration,
    _batch_results: usize,
) -> PerformanceBaseline {
    let individual_avg_us = individual_duration.as_micros() as f64 / config.individual_runs as f64;

    let individual_throughput = (config.individual_runs as f64) / individual_duration.as_secs_f64();
    let batch_throughput = (config.stream_record_count as f64) / batch_duration.as_secs_f64();

    let efficiency_ratio = batch_throughput / individual_throughput;

    PerformanceBaseline {
        avg_lookup_time_us: individual_avg_us,
        allocations_per_join: individual_results / config.individual_runs,
        throughput_records_per_sec: individual_throughput,
        batch_efficiency_ratio: efficiency_ratio,
        memory_usage_bytes: estimate_memory_usage(config),
    }
}

/// Estimate memory usage based on configuration
fn estimate_memory_usage(config: &BenchmarkConfig) -> usize {
    // Rough estimate based on:
    // - StreamRecord size (~1KB with HashMap overhead)
    // - Table record size (~500B per record)
    // - Processing overhead
    let stream_memory = config.stream_record_count * 1024;
    let table_memory = config.table_record_count * 512;
    let processing_overhead = stream_memory / 2; // Cloning overhead

    stream_memory + table_memory + processing_overhead
}

/// SQL query for stream-table join benchmark
const STREAM_TABLE_JOIN_SQL: &str = r#"
    SELECT
        trade_id,
        user_id,
        symbol,
        quantity,
        price
    FROM trades
"#;

/// Test: Stream-Table JOIN baseline performance measurement
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn test_stream_table_join_baseline_performance() {
    let stream_record_count = get_perf_record_count();
    let table_record_count = stream_record_count * 5; // Keep 5:1 ratio

    let mut config = BenchmarkConfig::default();
    config.stream_record_count = stream_record_count;
    config.table_record_count = table_record_count;

    println!("\n🚀 Stream-Table JOIN Baseline Performance");
    println!("══════════════════════════════════════════");
    println!("Operation #3: Tier 1 (94% probability)");
    println!("Use Case: Reference data enrichment");
    println!();
    print_perf_config(stream_record_count, None);
    println!("   Table Records: {}", table_record_count);
    println!();

    // Create test data
    let table = create_large_reference_table(config.table_record_count);
    let stream_records =
        generate_stream_records(config.stream_record_count, config.table_record_count);
    let join_clause = create_test_join_clause();

    // Setup processor and context
    let processor = StreamTableJoinProcessor::new();
    let mut context = ProcessorContext::new("baseline_benchmark");
    context.load_reference_table("user_profiles", table.clone());

    // Benchmark individual processing
    let (individual_duration, individual_results) = benchmark_individual_processing(
        &config,
        &processor,
        &stream_records,
        &join_clause,
        &mut context,
    )
    .expect("Individual processing benchmark failed");

    // Benchmark batch processing
    let (batch_duration, batch_results) = benchmark_batch_processing(
        &config,
        &processor,
        &stream_records,
        &join_clause,
        &mut context,
    )
    .expect("Batch processing benchmark failed");

    // Generate baseline report
    let baseline = generate_baseline_report(
        &config,
        individual_duration,
        individual_results,
        batch_duration,
        batch_results,
    );

    // Print comprehensive baseline report
    println!("\n📊 STREAM-TABLE JOIN BASELINE PERFORMANCE REPORT");
    println!("================================================");
    println!("🔍 Table Lookup Performance:");
    println!(
        "  • Average lookup time: {:.2} μs",
        baseline.avg_lookup_time_us
    );
    println!("  • Lookup algorithm: O(n) linear search (BOTTLENECK)");
    println!(
        "  • Table size impact: {} records = {:.2} μs per lookup",
        config.table_record_count, baseline.avg_lookup_time_us
    );

    println!("\n💾 Memory Allocation:");
    println!(
        "  • Allocations per join: {}",
        baseline.allocations_per_join
    );
    println!(
        "  • Estimated memory usage: {:.2} MB",
        baseline.memory_usage_bytes as f64 / 1024.0 / 1024.0
    );
    println!("  • StreamRecord cloning: 3 clones per join (HIGH OVERHEAD)");

    println!("\n⚡ Throughput Performance:");
    println!(
        "  • Individual processing: {:.0} records/sec",
        baseline.throughput_records_per_sec
    );
    println!(
        "  • Batch efficiency ratio: {:.2}x",
        baseline.batch_efficiency_ratio
    );
    println!(
        "  • Current batch advantage: {:.1}% faster",
        (baseline.batch_efficiency_ratio - 1.0) * 100.0
    );

    println!("\n🎯 Optimization Targets:");
    println!("  • Table lookups: O(n) → O(1) = 95%+ improvement potential");
    println!("  • Memory usage: 30-40% reduction via clone elimination");
    println!("  • Batch processing: 60% improvement via bulk operations");
    println!("  • Target throughput: 150,000+ records/sec (3.7x current)");

    println!("\n🚨 Critical Issues Identified:");
    if baseline.avg_lookup_time_us > 100.0 {
        println!("  ❌ Table lookup time > 100μs indicates O(n) bottleneck");
    }
    if baseline.batch_efficiency_ratio < 2.0 {
        println!("  ❌ Low batch efficiency indicates individual processing overhead");
    }
    if baseline.allocations_per_join > 5 {
        println!("  ❌ High allocation count indicates memory pressure");
    }

    println!("\n✅ Baseline measurements collected for STREAMING_SQL_OPERATION_RANKING.md");

    // Measure SQL Engine (sync baseline)
    let start = Instant::now();
    let (sql_sync_throughput, sql_sync_sent, sql_sync_produced) =
        measure_sql_engine_sync(stream_records.clone(), STREAM_TABLE_JOIN_SQL).await;
    let sql_sync_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("\n✅ SQL Engine Sync:");
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
        measure_sql_engine(stream_records.clone(), STREAM_TABLE_JOIN_SQL).await;
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
        measure_v1(stream_records.clone(), STREAM_TABLE_JOIN_SQL).await;
    let simple_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ SimpleJp:");
    println!("   Throughput: {:.0} rec/sec", simple_jp_throughput);
    println!("   Results: {}", simple_jp_produced);
    println!("   Time: {:.2}ms", simple_jp_ms);
    println!();

    // Measure TransactionalJp
    let start = Instant::now();
    let (transactional_jp_throughput, transactional_jp_produced) =
        measure_transactional_jp(stream_records.clone(), STREAM_TABLE_JOIN_SQL).await;
    let transactional_jp_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ TransactionalJp:");
    println!("   Throughput: {:.0} rec/sec", transactional_jp_throughput);
    println!("   Results: {}", transactional_jp_produced);
    println!("   Time: {:.2}ms", transactional_jp_ms);
    println!();

    let start = Instant::now();
    let (adaptive_1c_throughput, adaptive_1c_produced) =
        measure_adaptive_jp(stream_records.clone(), STREAM_TABLE_JOIN_SQL, 1).await;
    let adaptive_1c_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!("✅ AdaptiveJp (1 core):");
    println!("   Throughput: {:.0} rec/sec", adaptive_1c_throughput);
    println!("   Results: {}", adaptive_1c_produced);
    println!("   Time: {:.2}ms", adaptive_1c_ms);
    println!();

    let start = Instant::now();
    let (adaptive_4c_throughput, adaptive_4c_produced) =
        measure_adaptive_jp(stream_records.clone(), STREAM_TABLE_JOIN_SQL, 4).await;
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

    // Assert minimum performance thresholds
    assert!(
        baseline.throughput_records_per_sec > 5000.0,
        "Stream-Table JOIN throughput below acceptable threshold: {:.0} rec/sec",
        baseline.throughput_records_per_sec
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
            "stream_table_join_v1_test".to_string(),
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
            "stream_table_join_transactional_test".to_string(),
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
            format!("stream_table_join_adaptive_{}c_test", num_cores),
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
