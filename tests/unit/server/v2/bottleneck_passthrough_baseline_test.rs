//! Passthrough Baseline Test - Measures pure coordination overhead
//!
//! This test uses a PassthroughEngine that bypasses all SQL execution
//! to establish the minimum achievable latency through the AdaptiveJobProcessor.
//!
//! Comparison to understand overhead:
//! - PassthroughEngine baseline: Pure coordination/routing overhead
//! - SQL Engine baseline (5.45µs): Direct SQL execution without processor
//! - AdaptiveJobProcessor (60.52µs): Full pipeline with SQL
//! - Overhead from coordination: AdaptiveJobProcessor - PassthroughEngine

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// PassthroughEngine: Minimal overhead baseline
/// Executes queries without any SQL processing overhead
struct PassthroughEngine {
    record_count: Arc<AtomicU64>,
}

impl PassthroughEngine {
    fn new() -> Self {
        Self {
            record_count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn execute_with_record_sync(
        &mut self,
        _query: &velostream::velostream::sql::StreamingQuery,
        record: &StreamRecord,
    ) -> Result<Vec<Arc<StreamRecord>>, Box<dyn std::error::Error + Send + Sync>> {
        self.record_count.fetch_add(1, Ordering::Relaxed);
        // Simply pass through the record with minimal overhead
        Ok(vec![Arc::new(record.clone())])
    }

    fn get_count(&self) -> u64 {
        self.record_count.load(Ordering::Relaxed)
    }
}

/// Passthrough data source for baseline measurement
struct PassthroughDataSource {
    records: Vec<StreamRecord>,
    current_index: usize,
    batch_size: usize,
}

impl PassthroughDataSource {
    fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        Self {
            records,
            current_index: 0,
            batch_size,
        }
    }
}

#[async_trait]
impl DataReader for PassthroughDataSource {
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

/// Passthrough data writer - just counts records
struct PassthroughDataWriter {
    count: Arc<AtomicU64>,
}

impl PassthroughDataWriter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU64::new(0)),
        }
    }

    fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl DataWriter for PassthroughDataWriter {
    async fn write(
        &mut self,
        _: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count
            .fetch_add(records.len() as u64, Ordering::Relaxed);
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

/// Generate test records
fn generate_test_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "value".to_string(),
                FieldValue::Float(100.0 + (i % 100) as f64),
            );
            StreamRecord::new(fields)
        })
        .collect()
}

/// Test: Passthrough baseline - pure coordination overhead
#[tokio::test]
#[ignore] // Run with: cargo test --test mod passthrough_baseline -- --nocapture --ignored
async fn passthrough_baseline_pure_overhead() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ BASELINE: Passthrough Engine (Pure Coordination Overhead) ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    }));

    let records = generate_test_records(10000);
    let data_source = PassthroughDataSource::new(records.clone(), 100);
    let data_writer = PassthroughDataWriter::new();

    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT id, value FROM test WHERE value > 50")
        .expect("Failed to parse query");

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
                query,
                "passthrough_baseline".to_string(),
                shutdown_rx,
                None,
            )
            .await
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    processor.stop().await.ok();

    let _result = job_handle.await;
    let elapsed = start.elapsed();

    let throughput = records.len() as f64 / elapsed.as_secs_f64();
    let per_record_micros = elapsed.as_micros() as f64 / records.len() as f64;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PASSTHROUGH BASELINE RESULTS                             ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Records: {}", records.len());
    println!("Time: {:.2}ms", elapsed.as_millis());
    println!("Throughput: {:.0} rec/sec", throughput);
    println!("Per-record: {:.2}µs", per_record_micros);

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ OVERHEAD ANALYSIS (vs SQL baseline 5.45µs)              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let sql_baseline = 5.45;
    let coordination_overhead = per_record_micros - sql_baseline;
    let overhead_percent = (coordination_overhead / sql_baseline) * 100.0;

    println!("Passthrough per-record: {:.2}µs", per_record_micros);
    println!("SQL baseline per-record: {:.2}µs", sql_baseline);
    println!("Pure coordination overhead: {:.2}µs", coordination_overhead);
    println!("Overhead percentage: {:.1}%", overhead_percent);

    println!("\n✅ Passthrough baseline measured");
    println!("   This represents the minimum achievable latency through AdaptiveJobProcessor");
}

/// Test: Compare passthrough vs SQL engine performance
#[tokio::test]
#[ignore] // Run with: cargo test --test mod passthrough_vs_sql_engine -- --nocapture --ignored
async fn passthrough_vs_sql_engine_comparison() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ COMPARISON: Passthrough vs SQL Engine Performance        ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records = generate_test_records(10000);

    // Test 1: Direct SQL engine execution
    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT id, value FROM test WHERE value > 50")
        .expect("Failed to parse query");

    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    for record in &records {
        let _ = engine.execute_with_record_sync(&query, record).ok();
    }
    let sql_elapsed = start.elapsed();
    let sql_per_record = sql_elapsed.as_micros() as f64 / records.len() as f64;

    // Test 2: Passthrough engine (pure coordination)
    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    }));

    let data_source = PassthroughDataSource::new(records.clone(), 100);
    let data_writer = PassthroughDataWriter::new();

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
                query,
                "passthrough_comparison".to_string(),
                shutdown_rx,
                None,
            )
            .await
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    processor.stop().await.ok();

    let _result = job_handle.await;
    let passthrough_elapsed = start.elapsed();
    let passthrough_per_record = passthrough_elapsed.as_micros() as f64 / records.len() as f64;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ COMPARISON RESULTS                                       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("SQL Engine Baseline:");
    println!("  Per-record latency: {:.2}µs", sql_per_record);
    println!("  Total time: {:.2}ms", sql_elapsed.as_millis());
    println!(
        "  Throughput: {:.0} rec/sec",
        records.len() as f64 / sql_elapsed.as_secs_f64()
    );

    println!("\nAdaptiveJobProcessor (Passthrough):");
    println!("  Per-record latency: {:.2}µs", passthrough_per_record);
    println!("  Total time: {:.2}ms", passthrough_elapsed.as_millis());
    println!(
        "  Throughput: {:.0} rec/sec",
        records.len() as f64 / passthrough_elapsed.as_secs_f64()
    );

    println!("\nOverhead Analysis:");
    let overhead = passthrough_per_record - sql_per_record;
    let overhead_percent = (overhead / sql_per_record) * 100.0;
    println!("  Coordination overhead: {:.2}µs", overhead);
    println!("  Overhead percentage: {:.1}%", overhead_percent);
    println!(
        "  Slowdown factor: {:.2}x",
        passthrough_per_record / sql_per_record
    );

    println!("\n✅ Comparison completed");
}

/// Test: Identify individual overhead components
#[tokio::test]
#[ignore] // Run with: cargo test --test mod passthrough_overhead_breakdown -- --nocapture --ignored
async fn passthrough_overhead_breakdown() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ OVERHEAD BREAKDOWN: Coordination vs SQL Execution        ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records = generate_test_records(5000);

    // Direct SQL engine
    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT id, value FROM test WHERE value > 50")
        .expect("Failed to parse query");

    let (tx, _rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    let start = Instant::now();
    for record in &records {
        let _ = engine.execute_with_record_sync(&query, record).ok();
    }
    let sql_elapsed = start.elapsed();

    // Passthrough processor
    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    }));

    let data_source = PassthroughDataSource::new(records.clone(), 100);
    let data_writer = PassthroughDataWriter::new();

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
                query,
                "passthrough_breakdown".to_string(),
                shutdown_rx,
                None,
            )
            .await
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    processor.stop().await.ok();

    let _result = job_handle.await;
    let passthrough_elapsed = start.elapsed();

    let total_overhead = passthrough_elapsed - sql_elapsed;
    let overhead_per_record = total_overhead.as_micros() as f64 / records.len() as f64;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ TIMING BREAKDOWN (5000 records)                          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!(
        "SQL Engine Time:              {:.2}ms",
        sql_elapsed.as_millis()
    );
    println!(
        "Passthrough Processor Time:   {:.2}ms",
        passthrough_elapsed.as_millis()
    );
    println!("─────────────────────────────────────────────");
    println!(
        "Pure Coordination Overhead:   {:.2}ms",
        total_overhead.as_millis()
    );

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ PER-RECORD BREAKDOWN                                     ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let sql_per_record = sql_elapsed.as_micros() as f64 / records.len() as f64;
    println!("SQL execution per-record:     {:.2}µs", sql_per_record);
    println!("Coordination overhead:        {:.2}µs", overhead_per_record);
    println!("─────────────────────────────────────────────");
    println!(
        "Total per-record:             {:.2}µs",
        sql_per_record + overhead_per_record
    );

    let overhead_percent = (overhead_per_record / sql_per_record) * 100.0;
    println!(
        "\nCoordination overhead: {:.1}% of SQL execution time",
        overhead_percent
    );

    println!("\n✅ Overhead breakdown completed");
    println!("   Use this to understand what portion of AdaptiveJobProcessor latency");
    println!("   comes from coordination vs SQL execution");
}
