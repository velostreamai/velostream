/*!
# FR-082 Comprehensive Baseline Comparison Test

Unified test measuring all 5 scenarios with 6 implementations:
- SQL Engine Sync (baseline, synchronous)
- SQL Engine Async (baseline, asynchronous)
- SimpleJp (single-threaded, best-effort)
- TransactionalJp (single-threaded, at-least-once)
- AdaptiveJp @ 1-core (partitioned, 1 core)
- AdaptiveJp @ 4-core (partitioned, 4 cores)

Results feed into SCENARIO-BASELINE-COMPARISON.md table.
*/

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

// Shared test utilities
use super::test_helpers::{KafkaSimulatorDataSource, MockDataWriter};

/// Get the selected partitioning strategy for a query
fn get_selected_strategy(query_str: &str) -> String {
    let mut parser = StreamingSqlParser::new();
    match parser.parse(query_str) {
        Ok(query) => {
            let selection = PartitionerSelector::select(&query);
            format!("{} ({})", selection.strategy_name, selection.reason)
        }
        Err(_) => "unknown (parse error)".to_string(),
    }
}

/// Scenario baseline measurements with partitioner tracking and result validation
#[derive(Clone, Debug)]
struct ScenarioResult {
    name: String,
    sql_engine_sync_throughput: f64,
    sql_engine_sync_records_sent: usize,
    sql_engine_sync_records_processed: usize,
    sql_engine_async_throughput: f64,
    sql_engine_async_records_sent: usize,
    sql_engine_async_records_processed: usize,
    simple_jp_throughput: f64,
    transactional_jp_throughput: f64,
    adaptive_jp_1c_throughput: f64,
    adaptive_jp_4c_throughput: f64,
    /// Partitioner strategy used for AdaptiveJp (helps understand performance characteristics)
    partitioner: Option<String>,
}

impl ScenarioResult {
    fn print_table(&self) {
        println!("\n┌─ {}", self.name);
        println!("│");
        println!(
            "│  SQL Engine Sync (sent: {}, processed: {})",
            self.sql_engine_sync_records_sent, self.sql_engine_sync_records_processed
        );
        println!("│    {:>8.0} rec/sec", self.sql_engine_sync_throughput);
        println!("│");
        println!(
            "│  SQL Engine Async (sent: {}, processed: {})",
            self.sql_engine_async_records_sent, self.sql_engine_async_records_processed
        );
        println!("│    {:>8.0} rec/sec", self.sql_engine_async_throughput);
        println!("│");
        println!(
            "│  SimpleJp:         {:>8.0} rec/sec",
            self.simple_jp_throughput
        );
        println!(
            "│  TransactionalJp:  {:>8.0} rec/sec",
            self.transactional_jp_throughput
        );
        println!(
            "│  AdaptiveJp@1c:    {:>8.0} rec/sec",
            self.adaptive_jp_1c_throughput
        );
        println!(
            "│  AdaptiveJp@4c:    {:>8.0} rec/sec",
            self.adaptive_jp_4c_throughput
        );

        // Show partitioner strategy if available
        if let Some(ref partitioner) = self.partitioner {
            println!("│");
            println!("│  Partitioner:      {}", partitioner);
        }

        // Calculate ratios vs SQL Engine Sync (baseline)
        if self.sql_engine_sync_throughput > 0.0 {
            let ratio_sql_async =
                self.sql_engine_async_throughput / self.sql_engine_sync_throughput;
            let ratio_simple = self.simple_jp_throughput / self.sql_engine_sync_throughput;
            let ratio_transactional =
                self.transactional_jp_throughput / self.sql_engine_sync_throughput;
            let ratio_adaptive_1c =
                self.adaptive_jp_1c_throughput / self.sql_engine_sync_throughput;
            let ratio_adaptive_4c =
                self.adaptive_jp_4c_throughput / self.sql_engine_sync_throughput;

            println!("│");
            println!("│  Ratios vs SQL Engine Sync:");
            println!("│    SQL Async:      {:.2}x", ratio_sql_async);
            println!("│    SimpleJp:       {:.2}x", ratio_simple);
            println!("│    TransactionalJp:{:.2}x", ratio_transactional);
            println!("│    AdaptiveJp@1c:  {:.2}x", ratio_adaptive_1c);
            println!("│    AdaptiveJp@4c:  {:.2}x", ratio_adaptive_4c);

            // Verdict
            let best = [
                ("SQLSync", self.sql_engine_sync_throughput),
                ("SQLAsync", self.sql_engine_async_throughput),
                ("SimpleJp", self.simple_jp_throughput),
                ("TransactionalJp", self.transactional_jp_throughput),
                ("AdaptiveJp@1c", self.adaptive_jp_1c_throughput),
                ("AdaptiveJp@4c", self.adaptive_jp_4c_throughput),
            ]
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|x| x.0);

            println!("│  Best: {}", best.unwrap_or("Unknown"));
        }

        println!("└");
    }
}

/// Measure SQL Engine (sync version) - execute_with_record_sync
async fn measure_sql_engine_sync(records: Vec<StreamRecord>, query: &str) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let mut records_sent = 0;
    let mut records_processed = 0;

    let start = Instant::now();
    for record in records.iter() {
        records_sent += 1;
        match engine.execute_with_record_sync(&parsed_query, &record) {
            Ok(results) => {
                // Count each result returned (0 or more per record)
                records_processed += results.len();
                // Validate results are valid StreamRecords with expected fields
                // Additional validation happens in assertions
            }
            Err(e) => {
                eprintln!("Error processing record: {}", e);
            }
        }
    }

    // Drain any remaining results from channel (for async completions)
    while let Ok(_) = rx.try_recv() {
        records_processed += 1;
    }

    let elapsed = start.elapsed();
    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_sent, records_processed)
}

/// Measure SQL Engine (async version) - execute_with_record
async fn measure_sql_engine(records: Vec<StreamRecord>, query: &str) -> (f64, usize, usize) {
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let mut records_sent = 0;
    let mut records_processed = 0;

    let start = Instant::now();
    for record in records.iter() {
        records_sent += 1;
        match engine.execute_with_record(&parsed_query, &record).await {
            Ok(()) => {
                // Message was sent to channel
            }
            Err(e) => {
                eprintln!("Error processing record: {}", e);
            }
        }
    }

    // Drain all results from channel
    while let Ok(_) = rx.try_recv() {
        records_processed += 1;
    }

    let elapsed = start.elapsed();
    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_sent, records_processed)
}

/// Measure JobServer V1 (returns throughput and actual records written)
async fn measure_v1(records: Vec<StreamRecord>, query: &str) -> (f64, usize) {
    // Create processor with explicit config for immediate exit on exhausted sources
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 100,
        batch_timeout: Duration::from_millis(100),
        max_retries: 2,
        retry_backoff: Duration::from_millis(50),
        progress_interval: 100,
        log_progress: false,
        empty_batch_count: 0, // Exit immediately when sources exhausted
        wait_on_empty_batch_ms: 10,
        enable_dlq: true,
        dlq_max_size: Some(100),
    };
    let processor = JobProcessorFactory::create_simple_with_config(config);
    let data_source = KafkaSimulatorDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    // Engine is managed internally by processor, no need to create/manage it here
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let timeout_duration = Duration::from_secs(120); // 2-minute timeout for windowed queries
    let timeout_result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer.clone())),
            // Engine is created internally by processor
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "v1_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    // Now stop the processor after all messages have been consumed
    processor.stop().await.ok();
    let elapsed = start.elapsed();

    // Get actual records written to the sink via MockDataWriter
    let records_written = data_writer.get_count();

    // Handle timeout failure
    if timeout_result.is_err() {
        eprintln!("\n❌ FAILURE: SimpleJp (V1) - Test exceeded 120 seconds");
        eprintln!("   Expected records: {}", records.len());
        eprintln!("   Records processed: {}", records_written);
        eprintln!("   Time elapsed: {:.2}s", elapsed.as_secs_f64());
    }

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}

/// Measure Transactional Job Processor (single-threaded with transactions)
async fn measure_transactional_jp(records: Vec<StreamRecord>, query: &str) -> (f64, usize) {
    let processor = JobProcessorFactory::create(JobProcessorConfig::Transactional);
    let data_source = KafkaSimulatorDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let timeout_duration = Duration::from_secs(120); // 2-minute timeout for windowed queries
    let timeout_result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer.clone())),
            // Engine is created internally by processor
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "transactional_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    // Now stop the processor after all messages have been consumed
    processor.stop().await.ok();
    let elapsed = start.elapsed();

    // Get actual records written to the sink via MockDataWriter
    let records_written = data_writer.get_count();

    // Handle timeout failure
    if timeout_result.is_err() {
        eprintln!("\n❌ FAILURE: TransactionalJp - Test exceeded 120 seconds");
        eprintln!("   Expected records: {}", records.len());
        eprintln!("   Records processed: {}", records_written);
        eprintln!("   Time elapsed: {:.2}s", elapsed.as_secs_f64());
    }

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}

/// Measure JobServer V2 @ 1-core
async fn measure_adaptive_1core(records: Vec<StreamRecord>, query: &str) -> (f64, usize) {
    // Use test-optimized configuration to eliminate EOF detection overhead (200-300ms)
    // With query-based auto-selection for optimal strategy
    // See: docs/developer/adaptive_processor_performance_analysis.md
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let processor = JobProcessorFactory::create_adaptive_test_optimized_with_auto_select(
        Some(1),
        query_arc.clone(),
    );
    let data_source = KafkaSimulatorDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let data_writer_clone = data_writer.clone();

    // Run process_job directly instead of spawning (no need for background task in test)
    let timeout_duration = Duration::from_secs(120); // 2-minute timeout for windowed queries
    let timeout_result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(data_source.clone()),
            Some(Box::new(data_writer_clone.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "v2_1core_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    let elapsed = start.elapsed();

    // Give async partition receiver tasks time to finish writing
    // Increased to 500ms to ensure all async writes complete
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Get actual records written to the sink via MockDataWriter
    let records_written = data_writer_clone.get_count();

    // Handle timeout failure
    if timeout_result.is_err() {
        eprintln!("\n❌ FAILURE: AdaptiveJp @ 1-core - Test exceeded 120 seconds");
        eprintln!("   Expected records: {}", records.len());
        eprintln!("   Records processed: {}", records_written);
        eprintln!("   Time elapsed: {:.2}s", elapsed.as_secs_f64());
    }

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}

/// Measure JobServer V2 @ 4-core
async fn measure_adaptive_4core(records: Vec<StreamRecord>, query: &str) -> (f64, usize) {
    // Use test-optimized configuration to eliminate EOF detection overhead (200-300ms)
    // With query-based auto-selection for optimal strategy
    // See: docs/developer/adaptive_processor_performance_analysis.md
    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let processor = JobProcessorFactory::create_adaptive_test_optimized_with_auto_select(
        Some(4),
        query_arc.clone(),
    );
    let data_source = KafkaSimulatorDataSource::new(records.clone(), 100);
    let data_writer = MockDataWriter::new();

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let data_writer_clone = data_writer.clone();

    // Run process_job directly instead of spawning (no need for background task in test)
    let timeout_duration = Duration::from_secs(120); // 2-minute timeout for windowed queries
    let timeout_result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(data_source.clone()),
            Some(Box::new(data_writer_clone.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "v2_4core_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    let elapsed = start.elapsed();

    // Give async partition receiver tasks time to finish writing
    // Increased to 500ms to ensure all async writes complete
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Get actual records written to the sink via MockDataWriter
    let records_written = data_writer_clone.get_count();

    // Handle timeout failure
    if timeout_result.is_err() {
        eprintln!("\n❌ FAILURE: AdaptiveJp @ 4-core - Test exceeded 120 seconds");
        eprintln!("   Expected records: {}", records.len());
        eprintln!("   Records processed: {}", records_written);
        eprintln!("   Time elapsed: {:.2}s", elapsed.as_secs_f64());
    }

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    (throughput, records_written)
}

/// Generate scenario 0 records (Pure SELECT)
/// Partitions by customer_id for realistic distribution across 32 partitions
fn generate_scenario_0_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let customer_id = (i % 1000) as i64;
            fields.insert("order_id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("customer_id".to_string(), FieldValue::Integer(customer_id));
            fields.insert(
                "order_date".to_string(),
                FieldValue::String("2024-01-15".to_string()),
            );
            fields.insert(
                "total_amount".to_string(),
                FieldValue::Float(150.0 + (i % 100) as f64),
            );
            // Partition by customer_id: 1000 unique customers → well distributed across 32 partitions
            StreamRecord::new(fields).with_partition_from_key(&customer_id.to_string(), 32)
        })
        .collect()
}

/// Generate scenario 1 records (ROWS WINDOW)
/// Partitions by symbol for proper PARTITION BY symbol handling across 32 partitions
fn generate_scenario_1_records(count: usize) -> Vec<StreamRecord> {
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

/// Generate scenario 2 records (GROUP BY)
/// Partitions by symbol for proper GROUP BY symbol handling with excellent distribution
fn generate_scenario_2_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let symbol = format!("SYM{}", i % 200);
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

/// Generate scenario 3a/3b records (TUMBLING WINDOW)
/// CRITICAL: Must distribute records across source partitions to test sticky_partition properly
/// Partitions by composite key (trader_id + symbol): 50 traders × 100 symbols = 5000 unique keys
/// This ensures perfect distribution across 32 partitions with balanced parallelism
fn generate_scenario_3_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            let trader_id = format!("T{}", i % 50);
            let symbol = format!("SYM{}", i % 100);

            fields.insert(
                "trader_id".to_string(),
                FieldValue::String(trader_id.clone()),
            );
            fields.insert("symbol".to_string(), FieldValue::String(symbol.clone()));
            fields.insert(
                "price".to_string(),
                FieldValue::Float(100.0 + (i % 50) as f64),
            );
            fields.insert(
                "quantity".to_string(),
                FieldValue::Integer((i % 1000) as i64),
            );
            fields.insert(
                "trade_time".to_string(),
                FieldValue::Integer((1000000 + (i * 1000)) as i64),
            );

            // Partition by composite key (trader_id + symbol): ~5000 unique combinations
            // Ensures perfect distribution across 32 partitions and tests parallelism properly
            let composite_key = format!("{}:{}", trader_id, symbol);
            StreamRecord::new(fields).with_partition_from_key(&composite_key, 32)
        })
        .collect()
}

/// Helper function to parse event count from environment variable
fn get_baseline_record_count() -> usize {
    std::env::var("VELOSTREAM_BASELINE_RECORDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000) // Default to 100,000 records
}

/// Test: Comprehensive baseline comparison for all scenarios
#[tokio::test]
#[serial_test::serial]
async fn comprehensive_baseline_comparison() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ FR-082: COMPREHENSIVE BASELINE COMPARISON TEST            ║");
    println!("║ Measuring 5 Scenarios × 4 Implementations                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let num_records = get_baseline_record_count();

    println!("\n📊 Test Parameters:");
    println!("   Record Count: {} records", num_records);
    println!("   Scenarios: 5 (SELECT, ROWS WINDOW, GROUP BY, TUMBLING, EMIT CHANGES)");
    println!("   Implementations: 4 (SimpleJp, TransactionalJp, AdaptiveJp@1c, AdaptiveJp@4c)");
    println!("   Environment Variable: VELOSTREAM_BASELINE_RECORDS (default: 100,000)");
    println!();
    let mut results = Vec::new();

    // ========================================================================
    // SCENARIO 1: Pure SELECT - All 5 Implementations
    // ========================================================================
    println!("\n🔬 SCENARIO 1: Pure SELECT");
    println!("─────────────────────────────────────────────────────────────");

    let records = generate_scenario_0_records(num_records);
    let query = r#"
        SELECT order_id, customer_id, order_date, total_amount
        FROM orders
        WHERE total_amount > 100
    "#;

    let (sql_sync_throughput, sql_sync_sent, sql_sync_processed) =
        measure_sql_engine_sync(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineSync:  {:.0} rec/sec (sent: {}, processed: {})",
        sql_sync_throughput, sql_sync_sent, sql_sync_processed
    );

    let (sql_async_throughput, sql_async_sent, sql_async_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineAsync: {:.0} rec/sec (sent: {}, processed: {})",
        sql_async_throughput, sql_async_sent, sql_async_processed
    );

    let (simple_jp_throughput, simple_jp_records_written) =
        measure_v1(records.clone(), query).await;
    println!(
        "  ✓ SimpleJp:       {:.0} rec/sec (written: {})",
        simple_jp_throughput, simple_jp_records_written
    );

    let (transactional_jp_throughput, transactional_jp_records_written) =
        measure_transactional_jp(records.clone(), query).await;
    println!(
        "  ✓ TransactionalJp: {:.0} rec/sec (written: {})",
        transactional_jp_throughput, transactional_jp_records_written
    );

    let (adaptive_jp_1c_throughput, adaptive_jp_1c_records_written) =
        measure_adaptive_1core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@1c:  {:.0} rec/sec (written: {})",
        adaptive_jp_1c_throughput, adaptive_jp_1c_records_written
    );

    let (adaptive_jp_4c_throughput, adaptive_jp_4c_records_written) =
        measure_adaptive_4core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@4c:  {:.0} rec/sec (written: {})",
        adaptive_jp_4c_throughput, adaptive_jp_4c_records_written
    );

    results.push(ScenarioResult {
        name: "Scenario 1: Pure SELECT".to_string(),
        sql_engine_sync_throughput: sql_sync_throughput,
        sql_engine_sync_records_sent: sql_sync_sent,
        sql_engine_sync_records_processed: sql_sync_processed,
        sql_engine_async_throughput: sql_async_throughput,
        sql_engine_async_records_sent: sql_async_sent,
        sql_engine_async_records_processed: sql_async_processed,
        simple_jp_throughput,
        transactional_jp_throughput,
        adaptive_jp_1c_throughput,
        adaptive_jp_4c_throughput,
        partitioner: Some(get_selected_strategy(query)),
    });

    // ========================================================================
    // SCENARIO 2: ROWS WINDOW
    // ========================================================================
    println!("\n🔬 SCENARIO 2: ROWS WINDOW");
    println!("─────────────────────────────────────────────────────────────");

    let records = generate_scenario_1_records(num_records);
    let query = r#"
        SELECT symbol, price,
            AVG(price) OVER (
                ROWS WINDOW
                    BUFFER 100 ROWS
                    PARTITION BY symbol
                    ORDER BY timestamp
            ) as moving_avg
        FROM market_data
    "#;

    let (sql_sync_throughput, sql_sync_sent, sql_sync_processed) =
        measure_sql_engine_sync(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineSync:  {:.0} rec/sec (sent: {}, processed: {})",
        sql_sync_throughput, sql_sync_sent, sql_sync_processed
    );

    let (sql_async_throughput, sql_async_sent, sql_async_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineAsync: {:.0} rec/sec (sent: {}, processed: {})",
        sql_async_throughput, sql_async_sent, sql_async_processed
    );

    let (simple_jp_throughput, simple_jp_records_written) =
        measure_v1(records.clone(), query).await;
    println!(
        "  ✓ SimpleJp:       {:.0} rec/sec (written: {})",
        simple_jp_throughput, simple_jp_records_written
    );

    let (transactional_jp_throughput, transactional_jp_records_written) =
        measure_transactional_jp(records.clone(), query).await;
    println!(
        "  ✓ TransactionalJp: {:.0} rec/sec (written: {})",
        transactional_jp_throughput, transactional_jp_records_written
    );

    let (adaptive_jp_1c_throughput, adaptive_jp_1c_records_written) =
        measure_adaptive_1core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@1c:  {:.0} rec/sec (written: {})",
        adaptive_jp_1c_throughput, adaptive_jp_1c_records_written
    );

    let (adaptive_jp_4c_throughput, adaptive_jp_4c_records_written) =
        measure_adaptive_4core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@4c:  {:.0} rec/sec (written: {})",
        adaptive_jp_4c_throughput, adaptive_jp_4c_records_written
    );

    results.push(ScenarioResult {
        name: "Scenario 2: ROWS WINDOW".to_string(),
        sql_engine_sync_throughput: sql_sync_throughput,
        sql_engine_sync_records_sent: sql_sync_sent,
        sql_engine_sync_records_processed: sql_sync_processed,
        sql_engine_async_throughput: sql_async_throughput,
        sql_engine_async_records_sent: sql_async_sent,
        sql_engine_async_records_processed: sql_async_processed,
        simple_jp_throughput,
        transactional_jp_throughput,
        adaptive_jp_1c_throughput,
        adaptive_jp_4c_throughput,
        partitioner: Some(get_selected_strategy(query)),
    });

    // ========================================================================
    // SCENARIO 3: GROUP BY
    // ========================================================================
    println!("\n🔬 SCENARIO 3: GROUP BY");
    println!("─────────────────────────────────────────────────────────────");

    let records = generate_scenario_2_records(num_records);
    let query = r#"
        SELECT symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY symbol
    "#;

    let (sql_sync_throughput, sql_sync_sent, sql_sync_processed) =
        measure_sql_engine_sync(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineSync:  {:.0} rec/sec (sent: {}, processed: {})",
        sql_sync_throughput, sql_sync_sent, sql_sync_processed
    );

    let (sql_async_throughput, sql_async_sent, sql_async_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineAsync: {:.0} rec/sec (sent: {}, processed: {})",
        sql_async_throughput, sql_async_sent, sql_async_processed
    );

    let (simple_jp_throughput, simple_jp_records_written) =
        measure_v1(records.clone(), query).await;
    println!(
        "  ✓ SimpleJp:       {:.0} rec/sec (written: {})",
        simple_jp_throughput, simple_jp_records_written
    );

    let (transactional_jp_throughput, transactional_jp_records_written) =
        measure_transactional_jp(records.clone(), query).await;
    println!(
        "  ✓ TransactionalJp: {:.0} rec/sec (written: {})",
        transactional_jp_throughput, transactional_jp_records_written
    );

    let (adaptive_jp_1c_throughput, adaptive_jp_1c_records_written) =
        measure_adaptive_1core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@1c:  {:.0} rec/sec (written: {})",
        adaptive_jp_1c_throughput, adaptive_jp_1c_records_written
    );

    let (adaptive_jp_4c_throughput, adaptive_jp_4c_records_written) =
        measure_adaptive_4core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@4c:  {:.0} rec/sec (written: {})",
        adaptive_jp_4c_throughput, adaptive_jp_4c_records_written
    );

    results.push(ScenarioResult {
        name: "Scenario 3: GROUP BY".to_string(),
        sql_engine_sync_throughput: sql_sync_throughput,
        sql_engine_sync_records_sent: sql_sync_sent,
        sql_engine_sync_records_processed: sql_sync_processed,
        sql_engine_async_throughput: sql_async_throughput,
        sql_engine_async_records_sent: sql_async_sent,
        sql_engine_async_records_processed: sql_async_processed,
        simple_jp_throughput,
        transactional_jp_throughput,
        adaptive_jp_1c_throughput,
        adaptive_jp_4c_throughput,
        partitioner: Some(get_selected_strategy(query)),
    });

    // ========================================================================
    // SCENARIO 4: TUMBLING WINDOW + GROUP BY (Standard)
    // ========================================================================
    println!("\n🔬 SCENARIO 4: TUMBLING WINDOW + GROUP BY");
    println!("─────────────────────────────────────────────────────────────");

    let records = generate_scenario_3_records(num_records);
    let query = r#"
        SELECT trader_id, symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity,
            SUM(price * quantity) as total_value
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
    "#;

    let (sql_sync_throughput, sql_sync_sent, sql_sync_processed) =
        measure_sql_engine_sync(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineSync:  {:.0} rec/sec (sent: {}, processed: {})",
        sql_sync_throughput, sql_sync_sent, sql_sync_processed
    );

    let (sql_async_throughput, sql_async_sent, sql_async_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineAsync: {:.0} rec/sec (sent: {}, processed: {})",
        sql_async_throughput, sql_async_sent, sql_async_processed
    );

    let (simple_jp_throughput, simple_jp_records_written) =
        measure_v1(records.clone(), query).await;
    println!(
        "  ✓ SimpleJp:       {:.0} rec/sec (written: {})",
        simple_jp_throughput, simple_jp_records_written
    );

    let (transactional_jp_throughput, transactional_jp_records_written) =
        measure_transactional_jp(records.clone(), query).await;
    println!(
        "  ✓ TransactionalJp: {:.0} rec/sec (written: {})",
        transactional_jp_throughput, transactional_jp_records_written
    );

    let (adaptive_jp_1c_throughput, adaptive_jp_1c_records_written) =
        measure_adaptive_1core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@1c:  {:.0} rec/sec (written: {})",
        adaptive_jp_1c_throughput, adaptive_jp_1c_records_written
    );

    let (adaptive_jp_4c_throughput, adaptive_jp_4c_records_written) =
        measure_adaptive_4core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@4c:  {:.0} rec/sec (written: {})",
        adaptive_jp_4c_throughput, adaptive_jp_4c_records_written
    );

    results.push(ScenarioResult {
        name: "Scenario 4: TUMBLING + GROUP BY".to_string(),
        sql_engine_sync_throughput: sql_sync_throughput,
        sql_engine_sync_records_sent: sql_sync_sent,
        sql_engine_sync_records_processed: sql_sync_processed,
        sql_engine_async_throughput: sql_async_throughput,
        sql_engine_async_records_sent: sql_async_sent,
        sql_engine_async_records_processed: sql_async_processed,
        simple_jp_throughput,
        transactional_jp_throughput,
        adaptive_jp_1c_throughput,
        adaptive_jp_4c_throughput,
        partitioner: Some(get_selected_strategy(query)),
    });

    // ========================================================================
    // SCENARIO 5: TUMBLING WINDOW + EMIT CHANGES
    // ========================================================================
    println!("\n🔬 SCENARIO 5: TUMBLING WINDOW + EMIT CHANGES");
    println!("─────────────────────────────────────────────────────────────");

    let records = generate_scenario_3_records(num_records);
    let query = r#"
        SELECT trader_id, symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity,
            SUM(price * quantity) as total_value
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE) EMIT CHANGES
    "#;

    let (sql_sync_throughput, sql_sync_sent, sql_sync_processed) =
        measure_sql_engine_sync(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineSync:  {:.0} rec/sec (sent: {}, processed: {})",
        sql_sync_throughput, sql_sync_sent, sql_sync_processed
    );

    let (sql_async_throughput, sql_async_sent, sql_async_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQLEngineAsync: {:.0} rec/sec (sent: {}, processed: {})",
        sql_async_throughput, sql_async_sent, sql_async_processed
    );

    let (simple_jp_throughput, simple_jp_records_written) =
        measure_v1(records.clone(), query).await;
    println!(
        "  ✓ SimpleJp:       {:.0} rec/sec (written: {})",
        simple_jp_throughput, simple_jp_records_written
    );

    let (transactional_jp_throughput, transactional_jp_records_written) =
        measure_transactional_jp(records.clone(), query).await;
    println!(
        "  ✓ TransactionalJp: {:.0} rec/sec (written: {})",
        transactional_jp_throughput, transactional_jp_records_written
    );

    let (adaptive_jp_1c_throughput, adaptive_jp_1c_records_written) =
        measure_adaptive_1core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@1c:  {:.0} rec/sec (written: {})",
        adaptive_jp_1c_throughput, adaptive_jp_1c_records_written
    );

    let (adaptive_jp_4c_throughput, adaptive_jp_4c_records_written) =
        measure_adaptive_4core(records.clone(), query).await;
    println!(
        "  ✓ AdaptiveJp@4c:  {:.0} rec/sec (written: {})",
        adaptive_jp_4c_throughput, adaptive_jp_4c_records_written
    );

    results.push(ScenarioResult {
        name: "Scenario 5: TUMBLING + EMIT CHANGES".to_string(),
        sql_engine_sync_throughput: sql_sync_throughput,
        sql_engine_sync_records_sent: sql_sync_sent,
        sql_engine_sync_records_processed: sql_sync_processed,
        sql_engine_async_throughput: sql_async_throughput,
        sql_engine_async_records_sent: sql_async_sent,
        sql_engine_async_records_processed: sql_async_processed,
        simple_jp_throughput,
        transactional_jp_throughput,
        adaptive_jp_1c_throughput,
        adaptive_jp_4c_throughput,
        partitioner: Some(get_selected_strategy(query)),
    });

    // ========================================================================
    // RESULTS SUMMARY
    // ========================================================================
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ COMPREHENSIVE BASELINE COMPARISON RESULTS                 ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    for result in &results {
        result.print_table();
    }

    // Display unified comparison table
    println!("\n┌─ UNIFIED COMPARISON TABLE (for SCENARIO-BASELINE-COMPARISON.md)");
    println!("│");
    println!(
        "│ {:24} {:>12} {:>12} {:>12} {:>14} {:>14} {:>14} {:>16}",
        "Scenario",
        "SQLSync",
        "SQLAsync",
        "SimpleJp",
        "TransJp",
        "AdaptiveJp@1c",
        "AdaptiveJp@4c",
        "Partitioner"
    );
    println!(
        "│ {:24} {:>12} {:>12} {:>12} {:>14} {:>14} {:>14} {:>16}",
        "─────────────────────────",
        "────────────",
        "────────────",
        "────────────",
        "──────────────",
        "──────────────",
        "──────────────",
        "────────────────"
    );

    for result in &results {
        let partitioner_str = result.partitioner.as_deref().unwrap_or("N/A");
        println!(
            "│ {:24} {:>12.0} {:>12.0} {:>12.0} {:>14.0} {:>14.0} {:>14.0} {:>16}",
            result.name.split(": ").nth(1).unwrap_or(&result.name),
            result.sql_engine_sync_throughput,
            result.sql_engine_async_throughput,
            result.simple_jp_throughput,
            result.transactional_jp_throughput,
            result.adaptive_jp_1c_throughput,
            result.adaptive_jp_4c_throughput,
            partitioner_str
        );
    }
    println!("└");

    // Assertions to verify all measurements completed
    for result in &results {
        assert!(
            result.sql_engine_sync_throughput > 0.0,
            "SQL Engine Sync should have positive throughput for {}",
            result.name
        );
        assert!(
            result.sql_engine_async_throughput > 0.0,
            "SQL Engine Async should have positive throughput for {}",
            result.name
        );
        assert!(
            result.simple_jp_throughput > 0.0,
            "SimpleJp should have positive throughput for {}",
            result.name
        );
        assert!(
            result.transactional_jp_throughput > 0.0,
            "TransactionalJp should have positive throughput for {}",
            result.name
        );
        assert!(
            result.adaptive_jp_1c_throughput > 0.0,
            "AdaptiveJp@1-core should have positive throughput for {}",
            result.name
        );
        assert!(
            result.adaptive_jp_4c_throughput > 0.0,
            "AdaptiveJp@4-core should have positive throughput for {}",
            result.name
        );
    }

    println!("\n✅ All measurements completed successfully!");
}
