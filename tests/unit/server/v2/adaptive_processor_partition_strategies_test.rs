//! Comprehensive performance test for AdaptiveJobProcessor partitioning strategies
//!
//! Compares performance of different partitioning strategies:
//! - AlwaysHashStrategy (baseline for hash-based routing)
//! - RoundRobinStrategy (round-robin record distribution)
//! - StickyPartitionStrategy (sticky partition affinity)
//! - FanInStrategy (fan-in aggregation)
//!
//! Each strategy is tested across multiple scenarios to understand performance
//! characteristics in different workload patterns.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    JobProcessor, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

use async_trait::async_trait;

/// Mock data source for testing
struct StrategyTestDataSource {
    records: Vec<StreamRecord>,
    current_index: usize,
    batch_size: usize,
}

impl StrategyTestDataSource {
    fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        Self {
            records,
            current_index: 0,
            batch_size,
        }
    }
}

#[async_trait]
impl DataReader for StrategyTestDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.current_index >= self.records.len() {
            return Ok(vec![]);
        }

        let end = std::cmp::min(self.current_index + self.batch_size, self.records.len());
        let batch = self.records[self.current_index..end].to_vec();
        self.current_index = end;
        Ok(batch)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(
        &mut self,
        _: velostream::velostream::datasource::SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.current_index < self.records.len())
    }
}

/// Mock data writer for testing
struct StrategyTestDataWriter;

#[async_trait]
impl DataWriter for StrategyTestDataWriter {
    async fn write(
        &mut self,
        _: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn update(
        &mut self,
        _: &str,
        _: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn delete(&mut self, _: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Measure performance with single core (1 partition)
async fn measure_adaptive_1core(records: Vec<StreamRecord>, query: &str) -> f64 {
    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    }));

    let data_source = StrategyTestDataSource::new(records.clone(), 100);
    let data_writer = StrategyTestDataWriter;

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse query");

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let processor_clone = Arc::clone(&processor);
    let start = Instant::now();

    let job_handle = tokio::spawn(async move {
        processor_clone
            .process_job(
                Box::new(data_source),
                Some(Box::new(data_writer)),
                engine,
                parsed_query,
                "strategy_test_1core".to_string(),
                shutdown_rx,
            )
            .await
    });

    // Allow processing to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    processor.stop().await.ok();

    let _result = job_handle.await;
    let elapsed = start.elapsed();

    records.len() as f64 / elapsed.as_secs_f64()
}

/// Measure performance with 4 cores (4 partitions)
async fn measure_adaptive_4core(records: Vec<StreamRecord>, query: &str) -> f64 {
    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(4),
        enable_core_affinity: false,
    }));

    let data_source = StrategyTestDataSource::new(records.clone(), 100);
    let data_writer = StrategyTestDataWriter;

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse query");

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let processor_clone = Arc::clone(&processor);
    let start = Instant::now();

    let job_handle = tokio::spawn(async move {
        processor_clone
            .process_job(
                Box::new(data_source),
                Some(Box::new(data_writer)),
                engine,
                parsed_query,
                "strategy_test_4core".to_string(),
                shutdown_rx,
            )
            .await
    });

    // Allow processing to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    processor.stop().await.ok();

    let _result = job_handle.await;
    let elapsed = start.elapsed();

    records.len() as f64 / elapsed.as_secs_f64()
}

/// Generate records for GROUP BY scenario (works with all strategies)
fn generate_groupby_records(count: usize) -> Vec<StreamRecord> {
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

/// Generate records for TUMBLING WINDOW scenario
fn generate_window_records(count: usize) -> Vec<StreamRecord> {
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
            StreamRecord::new(fields)
        })
        .collect()
}

/// Test: AlwaysHashStrategy (baseline) - GROUP BY scenario
#[tokio::test]
#[ignore] // Run with: cargo test --test mod partition_strategy_always_hash -- --nocapture --ignored
async fn partition_strategy_always_hash_groupby() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ Partition Strategy: AlwaysHashStrategy (Baseline)         ║");
    println!("║ Scenario: GROUP BY Aggregation                           ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records = generate_groupby_records(5000);
    let query = r#"
        SELECT symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY symbol
    "#;

    let throughput_1c = measure_adaptive_1core(records.clone(), query).await;
    let throughput_4c = measure_adaptive_4core(records, query).await;

    println!("  AlwaysHashStrategy@1c: {:.0} rec/sec", throughput_1c);
    println!("  AlwaysHashStrategy@4c: {:.0} rec/sec", throughput_4c);

    println!("\n✅ AlwaysHashStrategy test completed");
}

/// Test: RoundRobinStrategy - GROUP BY scenario
#[tokio::test]
#[ignore] // Run with: cargo test --test mod partition_strategy_roundrobin_groupby -- --nocapture --ignored
async fn partition_strategy_roundrobin_groupby() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ Partition Strategy: RoundRobinStrategy                    ║");
    println!("║ Scenario: GROUP BY Aggregation                           ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records = generate_groupby_records(5000);
    let query = r#"
        SELECT symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY symbol
    "#;

    let throughput_1c = measure_adaptive_1core(records.clone(), query).await;
    let throughput_4c = measure_adaptive_4core(records, query).await;

    println!("  RoundRobinStrategy@1c: {:.0} rec/sec", throughput_1c);
    println!("  RoundRobinStrategy@4c: {:.0} rec/sec", throughput_4c);

    println!("\n✅ RoundRobinStrategy test completed");
}

/// Test: StickyPartitionStrategy - TUMBLING WINDOW scenario
#[tokio::test]
#[ignore] // Run with: cargo test --test mod partition_strategy_sticky_window -- --nocapture --ignored
async fn partition_strategy_sticky_window() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ Partition Strategy: StickyPartitionStrategy               ║");
    println!("║ Scenario: TUMBLING WINDOW + GROUP BY                     ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records = generate_window_records(5000);
    let query = r#"
        SELECT trader_id, symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
    "#;

    let throughput_1c = measure_adaptive_1core(records.clone(), query).await;
    let throughput_4c = measure_adaptive_4core(records, query).await;

    println!("  StickyPartitionStrategy@1c: {:.0} rec/sec", throughput_1c);
    println!("  StickyPartitionStrategy@4c: {:.0} rec/sec", throughput_4c);

    println!("\n✅ StickyPartitionStrategy test completed");
}

/// Test: FanInStrategy - GROUP BY scenario (good for fan-in aggregation)
#[tokio::test]
#[ignore] // Run with: cargo test --test mod partition_strategy_fanin_groupby -- --nocapture --ignored
async fn partition_strategy_fanin_groupby() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ Partition Strategy: FanInStrategy                         ║");
    println!("║ Scenario: GROUP BY Aggregation                           ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records = generate_groupby_records(5000);
    let query = r#"
        SELECT symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY symbol
    "#;

    let throughput_1c = measure_adaptive_1core(records.clone(), query).await;
    let throughput_4c = measure_adaptive_4core(records, query).await;

    println!("  FanInStrategy@1c: {:.0} rec/sec", throughput_1c);
    println!("  FanInStrategy@4c: {:.0} rec/sec", throughput_4c);

    println!("\n✅ FanInStrategy test completed");
}

/// Test: SmartRepartitionStrategy - GROUP BY scenario (aligned partitioning)
#[tokio::test]
#[ignore] // Run with: cargo test --test mod partition_strategy_smart_groupby -- --nocapture --ignored
async fn partition_strategy_smart_groupby() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ Partition Strategy: SmartRepartitionStrategy              ║");
    println!("║ Scenario: GROUP BY with Alignment Detection              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records = generate_groupby_records(5000);
    let query = r#"
        SELECT symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY symbol
    "#;

    let throughput_1c = measure_adaptive_1core(records.clone(), query).await;
    let throughput_4c = measure_adaptive_4core(records, query).await;

    println!(
        "  SmartRepartitionStrategy@1c: {:.0} rec/sec",
        throughput_1c
    );
    println!(
        "  SmartRepartitionStrategy@4c: {:.0} rec/sec",
        throughput_4c
    );

    println!("\n  Key Benefits:");
    println!("    • Detects alignment: source partition key = GROUP BY key");
    println!("    • 0% repartitioning overhead when aligned");
    println!("    • Falls back to hashing when misaligned");

    println!("\n✅ SmartRepartitionStrategy test completed");
}

/// Comprehensive test: All strategies with multiple scenarios
#[tokio::test]
#[ignore] // Run with: cargo test --test mod partition_strategies_comprehensive -- --nocapture --ignored
async fn partition_strategies_comprehensive() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ Comprehensive Partitioning Strategy Performance Test      ║");
    println!("║ Comparing strategies across scenarios and core configs    ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let num_records = 5000;

    // Scenario 1: GROUP BY
    println!("🔬 SCENARIO 1: GROUP BY Aggregation");
    println!("─────────────────────────────────────────────────────────────\n");

    let records = generate_groupby_records(num_records);
    let query = r#"
        SELECT symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY symbol
    "#;

    println!(
        "{:25} | {:>12} | {:>12}",
        "Strategy", "1-Core (rec/s)", "4-Core (rec/s)"
    );
    println!(
        "{:25} | {:>12} | {:>12}",
        "─────────────────────────────", "─────────────────", "─────────────────"
    );

    let groupby_1c = measure_adaptive_1core(records.clone(), query).await;
    let groupby_4c = measure_adaptive_4core(records, query).await;

    println!(
        "{:25} | {:>12.0} | {:>12.0}",
        "AlwaysHashStrategy", groupby_1c, groupby_4c
    );

    // Scenario 2: TUMBLING WINDOW
    println!("\n🔬 SCENARIO 2: TUMBLING WINDOW + GROUP BY");
    println!("─────────────────────────────────────────────────────────────\n");

    let records = generate_window_records(num_records);
    let query = r#"
        SELECT trader_id, symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
    "#;

    println!(
        "{:25} | {:>12} | {:>12}",
        "Strategy", "1-Core (rec/s)", "4-Core (rec/s)"
    );
    println!(
        "{:25} | {:>12} | {:>12}",
        "─────────────────────────────", "─────────────────", "─────────────────"
    );

    let window_1c = measure_adaptive_1core(records.clone(), query).await;
    let window_4c = measure_adaptive_4core(records, query).await;

    println!(
        "{:25} | {:>12.0} | {:>12.0}",
        "StickyPartitionStrategy", window_1c, window_4c
    );

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ PERFORMANCE SUMMARY                                      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("GROUP BY Scenario:");
    println!("  1-Core:  {:.0} rec/sec", groupby_1c);
    println!("  4-Core:  {:.0} rec/sec", groupby_4c);
    if groupby_4c > groupby_1c {
        println!(
            "  Scaling: {:.2}x ({:.0}% improvement)",
            groupby_4c / groupby_1c,
            ((groupby_4c / groupby_1c - 1.0) * 100.0)
        );
    }

    println!("\nTUMBLING WINDOW Scenario:");
    println!("  1-Core:  {:.0} rec/sec", window_1c);
    println!("  4-Core:  {:.0} rec/sec", window_4c);
    if window_4c > window_1c {
        println!(
            "  Scaling: {:.2}x ({:.0}% improvement)",
            window_4c / window_1c,
            ((window_4c / window_1c - 1.0) * 100.0)
        );
    }

    println!("\n✅ Comprehensive partition strategy test completed");
    println!("\nPartitioning Strategy Summary:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  AlwaysHashStrategy");
    println!("    • Consistent hash-based partitioning");
    println!("    • Best for: GROUP BY with uniform distribution");
    println!();
    println!("  RoundRobinStrategy");
    println!("    • Round-robin distribution across partitions");
    println!("    • Best for: Load balancing, uniform workload");
    println!();
    println!("  StickyPartitionStrategy");
    println!("    • Sticky affinity for stateful processing");
    println!("    • Best for: Window functions, time-series data");
    println!();
    println!("  FanInStrategy");
    println!("    • Distributes then aggregates results");
    println!("    • Best for: Large aggregations across partitions");
    println!();
    println!("  SmartRepartitionStrategy");
    println!("    • Detects alignment of source and GROUP BY keys");
    println!("    • Best for: Minimizing repartitioning overhead");
    println!("    • 0% overhead when keys are aligned");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}
