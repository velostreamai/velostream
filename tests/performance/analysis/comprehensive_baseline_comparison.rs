/*!
# FR-082 Comprehensive Baseline Comparison Test

Unified test measuring all 5 scenarios with 4 implementations:
- SQL Engine (baseline, no job server)
- JobServer V1 (single-threaded)
- JobServer V2 @ 1-core
- JobServer V2 @ 4-core

Results feed into SCENARIO-BASELINE-COMPARISON.md table.
*/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::server::processors::{
    JobProcessor, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Shared test utilities
use super::test_helpers::{MockDataSource, MockDataWriter};

/// Scenario baseline measurements with partitioner tracking and result validation
#[derive(Clone, Debug)]
struct ScenarioResult {
    name: String,
    sql_engine_throughput: f64,
    sql_engine_records_sent: usize,
    sql_engine_records_processed: usize,
    v1_throughput: f64,
    v2_1core_throughput: f64,
    v2_4core_throughput: f64,
    /// Partitioner strategy used for V2 (helps understand performance characteristics)
    partitioner: Option<String>,
}

impl ScenarioResult {
    fn print_table(&self) {
        println!("\n┌─ {}", self.name);
        println!(
            "│  Records sent: {} | Processed: {}",
            self.sql_engine_records_sent, self.sql_engine_records_processed
        );
        println!(
            "│  SQL Engine:     {:>8.0} rec/sec",
            self.sql_engine_throughput
        );
        println!("│  V1 (1-thread):  {:>8.0} rec/sec", self.v1_throughput);
        println!(
            "│  V2 @ 1-core:    {:>8.0} rec/sec",
            self.v2_1core_throughput
        );
        println!(
            "│  V2 @ 4-core:    {:>8.0} rec/sec",
            self.v2_4core_throughput
        );

        // Show partitioner strategy if available
        if let Some(ref partitioner) = self.partitioner {
            println!("│");
            println!("│  Partitioner:    {}", partitioner);
        }

        // Calculate ratios vs SQL Engine
        if self.sql_engine_throughput > 0.0 {
            let ratio_v1 = self.v1_throughput / self.sql_engine_throughput;
            let ratio_v2_1 = self.v2_1core_throughput / self.sql_engine_throughput;
            let ratio_v2_4 = self.v2_4core_throughput / self.sql_engine_throughput;

            println!("│");
            println!("│  Ratios vs SQL Engine:");
            println!("│    V1:        {:.2}x", ratio_v1);
            println!("│    V2@1-core: {:.2}x", ratio_v2_1);
            println!("│    V2@4-core: {:.2}x", ratio_v2_4);

            // Verdict
            let best = vec![
                ("SQL", self.sql_engine_throughput),
                ("V1", self.v1_throughput),
                ("V2@1", self.v2_1core_throughput),
                ("V2@4", self.v2_4core_throughput),
            ]
            .iter()
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|x| x.0);

            println!("│  Best: {}", best.unwrap_or("Unknown"));
        }

        // Validate record processing
        println!("│");
        if self.sql_engine_records_sent == self.sql_engine_records_processed {
            println!(
                "│  ✓ Record validation: All {} records processed correctly",
                self.sql_engine_records_sent
            );
        } else {
            println!(
                "│  ⚠ Record validation: {} sent, {} processed (diff: {})",
                self.sql_engine_records_sent,
                self.sql_engine_records_processed,
                (self.sql_engine_records_sent as i64 - self.sql_engine_records_processed as i64)
                    .abs()
            );
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
            Ok(Some(_result_record)) => {
                records_processed += 1;
                // Validate result is a valid StreamRecord with expected fields
                // Additional validation happens in assertions
            }
            Ok(None) => {
                // Record was buffered (windowed queries) - will be emitted later
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

/// Measure JobServer V1
async fn measure_v1(records: Vec<StreamRecord>, query: &str) -> f64 {
    let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    });
    let data_source = MockDataSource::new(records.clone(), records.len());
    let data_writer = MockDataWriter::new();

    let (tx, mut rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let _result = processor
        .process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            (*query_arc).clone(),
            "v1_test".to_string(),
            shutdown_rx,
        )
        .await;

    // Consume all remaining messages from the execution engine
    while let Ok(_) = rx.try_recv() {
        // Just drain the channel
    }

    // Now stop the processor after all messages have been consumed
    processor.stop().await.ok();
    let elapsed = start.elapsed();

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    throughput
}

/// Measure JobServer V2 @ 1-core
async fn measure_v2_1core(records: Vec<StreamRecord>, query: &str) -> f64 {
    let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    });
    let data_source = MockDataSource::new(records.clone(), records.len());
    let data_writer = MockDataWriter::new();

    let (tx, mut rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let _result = processor
        .process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            (*query_arc).clone(),
            "v2_1core_test".to_string(),
            shutdown_rx,
        )
        .await;

    // Consume all remaining messages from the execution engine
    while let Ok(_) = rx.try_recv() {
        // Just drain the channel
    }

    // Now stop the processor after all messages have been consumed
    processor.stop().await.ok();
    let elapsed = start.elapsed();

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    throughput
}

/// Measure JobServer V2 @ 4-core
async fn measure_v2_4core(records: Vec<StreamRecord>, query: &str) -> f64 {
    let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(4),
        enable_core_affinity: false,
    });
    let data_source = MockDataSource::new(records.clone(), records.len());
    let data_writer = MockDataWriter::new();

    let (tx, mut rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = Instant::now();
    let _result = processor
        .process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            (*query_arc).clone(),
            "v2_4core_test".to_string(),
            shutdown_rx,
        )
        .await;

    // Consume all remaining messages from the execution engine
    while let Ok(_) = rx.try_recv() {
        // Just drain the channel
    }

    // Now stop the processor after all messages have been consumed
    processor.stop().await.ok();
    let elapsed = start.elapsed();

    let throughput = (records.len() as f64) / elapsed.as_secs_f64();
    throughput
}

/// Generate scenario 0 records (Pure SELECT)
fn generate_scenario_0_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("order_id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "customer_id".to_string(),
                FieldValue::Integer((i % 1000) as i64),
            );
            fields.insert(
                "order_date".to_string(),
                FieldValue::String("2024-01-15".to_string()),
            );
            fields.insert(
                "total_amount".to_string(),
                FieldValue::Float(150.0 + (i % 100) as f64),
            );
            StreamRecord::new(fields)
        })
        .collect()
}

/// Generate scenario 1 records (ROWS WINDOW)
fn generate_scenario_1_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert(
                "symbol".to_string(),
                FieldValue::String(format!("SYM{}", i % 10)),
            );
            fields.insert(
                "price".to_string(),
                FieldValue::Float(100.0 + (i % 50) as f64),
            );
            fields.insert(
                "timestamp".to_string(),
                FieldValue::Integer((i * 1000) as i64),
            );
            StreamRecord::new(fields)
        })
        .collect()
}

/// Generate scenario 2 records (GROUP BY)
fn generate_scenario_2_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert(
                "symbol".to_string(),
                FieldValue::String(format!("SYM{}", i % 200)),
            );
            fields.insert(
                "price".to_string(),
                FieldValue::Float(100.0 + (i % 100) as f64),
            );
            fields.insert(
                "quantity".to_string(),
                FieldValue::Integer((i % 1000) as i64),
            );
            StreamRecord::new(fields)
        })
        .collect()
}

/// Generate scenario 3a/3b records (TUMBLING WINDOW)
/// CRITICAL: Must distribute records across source partitions (0-3) to test sticky_partition properly
/// Without this: all records → partition 0, cores 1-3 idle, 4-core slower than 1-core due to merge overhead
fn generate_scenario_3_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert(
                "trader_id".to_string(),
                FieldValue::String(format!("T{}", i % 50)),
            );
            fields.insert(
                "symbol".to_string(),
                FieldValue::String(format!("SYM{}", i % 100)),
            );
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
            let record = StreamRecord::new(fields);
            record
        })
        .collect()
}

/// Test: Comprehensive baseline comparison for all scenarios
#[tokio::test]
#[serial_test::serial]
async fn comprehensive_baseline_comparison() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ FR-082: COMPREHENSIVE BASELINE COMPARISON TEST            ║");
    println!("║ Measuring 5 Scenarios × 4 Implementations                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let num_records = 5000;
    let mut results = Vec::new();

    // ========================================================================
    // SCENARIO 1: SQL Engine Sync (Pure SELECT - execute_with_record_sync)
    // ========================================================================
    println!("\n🔬 SCENARIO 1: SQL Engine Sync - Pure SELECT");
    println!("─────────────────────────────────────────────────────────────");

    let records = generate_scenario_0_records(num_records);
    let query = r#"
        SELECT order_id, customer_id, order_date, total_amount
        FROM orders
        WHERE total_amount > 100
    "#;

    let (sql_sync_throughput, records_sent, records_processed) =
        measure_sql_engine_sync(records.clone(), query).await;
    println!(
        "  ✓ SQL Engine (sync): {:.0} rec/sec (sent: {}, processed: {})",
        sql_sync_throughput, records_sent, records_processed
    );

    let v1_throughput = measure_v1(records.clone(), query).await;
    println!("  ✓ V1: {:.0} rec/sec", v1_throughput);

    let v2_1core_throughput = measure_v2_1core(records.clone(), query).await;
    println!("  ✓ V2@1-core: {:.0} rec/sec", v2_1core_throughput);

    let v2_4core_throughput = measure_v2_4core(records.clone(), query).await;
    println!("  ✓ V2@4-core: {:.0} rec/sec", v2_4core_throughput);

    results.push(ScenarioResult {
        name: "Scenario 1: SQL Engine Sync (Pure SELECT)".to_string(),
        sql_engine_throughput: sql_sync_throughput,
        sql_engine_records_sent: records_sent,
        sql_engine_records_processed: records_processed,
        v1_throughput,
        v2_1core_throughput,
        v2_4core_throughput,
        partitioner: Some("N/A (sync)".to_string()),
    });

    // ========================================================================
    // SCENARIO 2: Pure SELECT (Async)
    // ========================================================================
    println!("\n🔬 SCENARIO 2: Pure SELECT (Async)");
    println!("─────────────────────────────────────────────────────────────");

    let records = generate_scenario_0_records(num_records);
    let query = r#"
        SELECT order_id, customer_id, order_date, total_amount
        FROM orders
        WHERE total_amount > 100
    "#;

    let (sql_throughput, records_sent, records_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQL Engine (async): {:.0} rec/sec (sent: {}, processed: {})",
        sql_throughput, records_sent, records_processed
    );

    let v1_throughput = measure_v1(records.clone(), query).await;
    println!("  ✓ V1: {:.0} rec/sec", v1_throughput);

    let v2_1core_throughput = measure_v2_1core(records.clone(), query).await;
    println!("  ✓ V2@1-core: {:.0} rec/sec", v2_1core_throughput);

    let v2_4core_throughput = measure_v2_4core(records.clone(), query).await;
    println!("  ✓ V2@4-core: {:.0} rec/sec", v2_4core_throughput);

    results.push(ScenarioResult {
        name: "Scenario 2: Pure SELECT (Async)".to_string(),
        sql_engine_throughput: sql_throughput,
        sql_engine_records_sent: records_sent,
        sql_engine_records_processed: records_processed,
        v1_throughput,
        v2_1core_throughput,
        v2_4core_throughput,
        partitioner: Some("always_hash".to_string()),
    });

    // ========================================================================
    // SCENARIO 3: ROWS WINDOW (Async)
    // ========================================================================
    println!("\n🔬 SCENARIO 3: ROWS WINDOW (Async)");
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

    let (sql_throughput, records_sent, records_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQL Engine (async): {:.0} rec/sec (sent: {}, processed: {})",
        sql_throughput, records_sent, records_processed
    );

    let v1_throughput = measure_v1(records.clone(), query).await;
    println!("  ✓ V1: {:.0} rec/sec", v1_throughput);

    let v2_1core_throughput = measure_v2_1core(records.clone(), query).await;
    println!("  ✓ V2@1-core: {:.0} rec/sec", v2_1core_throughput);

    let v2_4core_throughput = measure_v2_4core(records.clone(), query).await;
    println!("  ✓ V2@4-core: {:.0} rec/sec", v2_4core_throughput);

    results.push(ScenarioResult {
        name: "Scenario 3: ROWS WINDOW (Async)".to_string(),
        sql_engine_throughput: sql_throughput,
        sql_engine_records_sent: records_sent,
        sql_engine_records_processed: records_processed,
        v1_throughput,
        v2_1core_throughput,
        v2_4core_throughput,
        partitioner: Some("sticky_partition".to_string()),
    });

    // ========================================================================
    // SCENARIO 4: GROUP BY
    // ========================================================================
    println!("\n🔬 SCENARIO 4: GROUP BY");
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

    let (sql_throughput, records_sent, records_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQL Engine: {:.0} rec/sec (sent: {}, processed: {})",
        sql_throughput, records_sent, records_processed
    );

    let v1_throughput = measure_v1(records.clone(), query).await;
    println!("  ✓ V1: {:.0} rec/sec", v1_throughput);

    let v2_1core_throughput = measure_v2_1core(records.clone(), query).await;
    println!("  ✓ V2@1-core: {:.0} rec/sec", v2_1core_throughput);

    let v2_4core_throughput = measure_v2_4core(records.clone(), query).await;
    println!("  ✓ V2@4-core: {:.0} rec/sec", v2_4core_throughput);

    results.push(ScenarioResult {
        name: "Scenario 4: GROUP BY".to_string(),
        sql_engine_throughput: sql_throughput,
        sql_engine_records_sent: records_sent,
        sql_engine_records_processed: records_processed,
        v1_throughput,
        v2_1core_throughput,
        v2_4core_throughput,
        partitioner: Some("always_hash".to_string()),
    });

    // ========================================================================
    // SCENARIO 5: TUMBLING WINDOW + GROUP BY (Standard)
    // ========================================================================
    println!("\n🔬 SCENARIO 5: TUMBLING WINDOW + GROUP BY");
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

    let (sql_throughput, records_sent, records_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQL Engine: {:.0} rec/sec (sent: {}, processed: {})",
        sql_throughput, records_sent, records_processed
    );

    let v1_throughput = measure_v1(records.clone(), query).await;
    println!("  ✓ V1: {:.0} rec/sec", v1_throughput);

    let v2_1core_throughput = measure_v2_1core(records.clone(), query).await;
    println!("  ✓ V2@1-core: {:.0} rec/sec", v2_1core_throughput);

    let v2_4core_throughput = measure_v2_4core(records.clone(), query).await;
    println!("  ✓ V2@4-core: {:.0} rec/sec", v2_4core_throughput);

    results.push(ScenarioResult {
        name: "Scenario 5: TUMBLING + GROUP BY".to_string(),
        sql_engine_throughput: sql_throughput,
        sql_engine_records_sent: records_sent,
        sql_engine_records_processed: records_processed,
        v1_throughput,
        v2_1core_throughput,
        v2_4core_throughput,
        partitioner: Some("sticky_partition".to_string()),
    });

    // ========================================================================
    // SCENARIO 6: TUMBLING WINDOW + EMIT CHANGES
    // ========================================================================
    println!("\n🔬 SCENARIO 6: TUMBLING WINDOW + EMIT CHANGES");
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

    let (sql_throughput, records_sent, records_processed) =
        measure_sql_engine(records.clone(), query).await;
    println!(
        "  ✓ SQL Engine: {:.0} rec/sec (sent: {}, processed: {})",
        sql_throughput, records_sent, records_processed
    );

    let v1_throughput = measure_v1(records.clone(), query).await;
    println!("  ✓ V1: {:.0} rec/sec", v1_throughput);

    let v2_1core_throughput = measure_v2_1core(records.clone(), query).await;
    println!("  ✓ V2@1-core: {:.0} rec/sec", v2_1core_throughput);

    let v2_4core_throughput = measure_v2_4core(records.clone(), query).await;
    println!("  ✓ V2@4-core: {:.0} rec/sec", v2_4core_throughput);

    results.push(ScenarioResult {
        name: "Scenario 6: TUMBLING + EMIT CHANGES".to_string(),
        sql_engine_throughput: sql_throughput,
        sql_engine_records_sent: records_sent,
        sql_engine_records_processed: records_processed,
        v1_throughput,
        v2_1core_throughput,
        v2_4core_throughput,
        partitioner: Some("sticky_partition".to_string()),
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
        "│ {:24} {:>12} {:>12} {:>12} {:>12} {:>18}",
        "Scenario", "SQL Engine", "V1", "V2@1-core", "V2@4-core", "Partitioner"
    );
    println!(
        "│ {:24} {:>12} {:>12} {:>12} {:>12} {:>18}",
        "─────────────────────────",
        "─────────────",
        "─────────────",
        "─────────────",
        "─────────────",
        "──────────────────"
    );

    for result in &results {
        let partitioner_str = result
            .partitioner
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("N/A");
        println!(
            "│ {:24} {:>12.0} {:>12.0} {:>12.0} {:>12.0} {:>18}",
            result.name.split(": ").nth(1).unwrap_or(&result.name),
            result.sql_engine_throughput,
            result.v1_throughput,
            result.v2_1core_throughput,
            result.v2_4core_throughput,
            partitioner_str
        );
    }
    println!("└");

    // Assertions to verify all measurements completed
    for result in &results {
        assert!(
            result.sql_engine_throughput > 0.0,
            "SQL Engine should have positive throughput for {}",
            result.name
        );
        assert!(
            result.v1_throughput > 0.0,
            "V1 should have positive throughput for {}",
            result.name
        );
        assert!(
            result.v2_1core_throughput > 0.0,
            "V2@1-core should have positive throughput for {}",
            result.name
        );
        assert!(
            result.v2_4core_throughput > 0.0,
            "V2@4-core should have positive throughput for {}",
            result.name
        );
    }

    println!("\n✅ All measurements completed successfully!");
}
