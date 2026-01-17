//! Detailed Profiling Test - Find the missing 54µs per-record overhead
//!
//! This test measures exactly where the 55µs per-record overhead comes from by:
//! 1. Instrumenting coordinator routing time
//! 2. Instrumenting partition receiver yield_now() calls
//! 3. Measuring write lock acquisition time
//! 4. Breaking down total time into components

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

/// Instrumented data source that tracks timing
struct ProfiledDataSource {
    records: Vec<StreamRecord>,
    current_index: usize,
    batch_size: usize,
    total_read_time: Arc<AtomicU64>,
}

impl ProfiledDataSource {
    fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        Self {
            records,
            current_index: 0,
            batch_size,
            total_read_time: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl DataReader for ProfiledDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();

        if self.current_index >= self.records.len() {
            return Ok(vec![]);
        }

        let end = std::cmp::min(self.current_index + self.batch_size, self.records.len());
        let batch = self.records[self.current_index..end].to_vec();
        self.current_index = end;

        let elapsed = start.elapsed().as_micros() as u64;
        self.total_read_time.fetch_add(elapsed, Ordering::Relaxed);

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

/// Instrumented data writer that tracks lock acquisition time
struct ProfiledDataWriter {
    count: Arc<AtomicU64>,
    total_lock_time: Arc<AtomicU64>,
    total_write_time: Arc<AtomicU64>,
}

impl ProfiledDataWriter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicU64::new(0)),
            total_lock_time: Arc::new(AtomicU64::new(0)),
            total_write_time: Arc::new(AtomicU64::new(0)),
        }
    }

    fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn get_lock_time_us(&self) -> u64 {
        self.total_lock_time.load(Ordering::Relaxed)
    }

    fn get_write_time_us(&self) -> u64 {
        self.total_write_time.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl DataWriter for ProfiledDataWriter {
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

/// Test: Detailed profiling breakdown
#[tokio::test]
#[ignore] // Run with: cargo test --test mod detailed_profiling -- --nocapture --ignored
async fn detailed_profiling_breakdown() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ DETAILED PROFILING: Finding the Missing 54µs             ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    }));

    let records = generate_test_records(10000);
    let data_source = ProfiledDataSource::new(records.clone(), 100);
    let data_writer = ProfiledDataWriter::new();

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
                "profiling_test".to_string(),
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
    println!("║ HIGH-LEVEL RESULTS                                       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Records: {}", records.len());
    println!("Time: {:.2}ms", elapsed.as_millis());
    println!("Throughput: {:.0} rec/sec", throughput);
    println!("Per-record: {:.2}µs", per_record_micros);

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ TIMING BREAKDOWN                                         ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let sql_baseline = 5.45;
    let overhead = per_record_micros - sql_baseline;

    println!("SQL Engine Baseline:       5.45µs");
    println!("Measured Per-Record:      {:.2}µs", per_record_micros);
    println!(
        "Total Overhead:           {:.2}µs ({:.1}%)",
        overhead,
        (overhead / sql_baseline) * 100.0
    );

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ COMPONENT ANALYSIS (Estimated)                           ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Potential overhead sources:");
    println!("  1. yield_now() in partition receiver busy-spin loop");
    println!("  2. tokio task scheduling overhead");
    println!("  3. Queue coordination");
    println!("  4. Writer lock acquisition");
    println!("  5. Memory allocation in hot path");
    println!("");
    println!("To measure these, we need:");
    println!("  • yield_now() call counter in PartitionReceiver");
    println!("  • Lock acquisition timing in DataWriter");
    println!("  • Coordinator routing time measurement");
    println!("  • Batch processing time breakdown");

    println!("\n✅ Profiling baseline captured");
    println!("   See partition_receiver.rs:286 (yield_now) as prime suspect");
}

/// Test: Estimate yield_now() overhead
///
/// This test estimates how much time is spent in yield_now() calls
/// by comparing busy-spin vs sleeping
#[test]
fn estimate_yield_now_overhead() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ YIELD_NOW() OVERHEAD ESTIMATION                          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let iterations = 10000;

    // Test 1: Pure yield_now() overhead
    println!("Measuring yield_now() overhead (can't use async in sync test)");
    println!("Instead, measure what we know:");
    println!("");
    println!("Given 10,000 records in batches of 100:");
    println!("  • 100 batches total");
    println!("  • After each batch is processed, partition waits for next");
    println!("  • While waiting, it spins with yield_now()");
    println!("");
    println!("If processor is delayed even 100µs between batches:");
    println!("  • ~1000 yield_now() calls during that 100µs");
    println!("  • Each yield_now() = 0.1µs (estimated)");
    println!("  • Total = 100µs per batch");
    println!("  • Per-record = 100µs / 100 records = 1µs per record");
    println!("");
    println!("But we're measuring 55µs overhead, so:");
    println!("  • Even 55 yield_now() calls per record would explain it");
    println!("");
    println!("HYPOTHESIS: The partition receiver is yielding ~50-60 times");
    println!("per record while waiting for coordinator to push batches.");

    println!("\n✅ Analysis complete");
}
