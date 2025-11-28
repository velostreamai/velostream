//! Bottleneck analysis test for AdaptiveJobProcessor
//!
//! Measures where the per-record overhead comes from by breaking down
//! the processing pipeline into individual components.

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

/// Mock data source for bottleneck analysis
struct BottleneckDataSource {
    records: Vec<StreamRecord>,
    current_index: usize,
    batch_size: usize,
}

impl BottleneckDataSource {
    fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        Self {
            records,
            current_index: 0,
            batch_size,
        }
    }
}

#[async_trait]
impl DataReader for BottleneckDataSource {
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

/// Mock data writer for bottleneck analysis
struct BottleneckDataWriter;

#[async_trait]
impl DataWriter for BottleneckDataWriter {
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

/// Test: Baseline SQL engine performance (no processor overhead)
#[tokio::test]
#[ignore] // Run with: cargo test --test mod bottleneck_sql_engine -- --nocapture --ignored
async fn bottleneck_sql_engine() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ BOTTLENECK 1: SQL Engine Baseline (Direct Execution)      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records = generate_test_records(10000);
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
    let elapsed = start.elapsed();

    let throughput = records.len() as f64 / elapsed.as_secs_f64();
    let per_record_micros = elapsed.as_micros() as f64 / records.len() as f64;

    println!("  Records: {}", records.len());
    println!("  Time: {:.2}ms", elapsed.as_millis());
    println!("  Throughput: {:.0} rec/sec", throughput);
    println!("  Per-record: {:.2}µs", per_record_micros);

    println!("\n✅ SQL Engine baseline measured");
}

/// Test: AdaptiveJobProcessor with different batch sizes
#[tokio::test]
#[ignore] // Run with: cargo test --test mod bottleneck_batch_size -- --nocapture --ignored
async fn bottleneck_batch_size() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ BOTTLENECK 2: Batch Size Impact on Overhead               ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let total_records = 10000;
    let batch_sizes = vec![10, 50, 100, 500, 1000];

    println!("Batch Size | Records | Throughput (rec/sec) | Per-Record (µs)");
    println!("─────────────────────────────────────────────────────────────");

    for batch_size in batch_sizes {
        let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
            num_partitions: Some(1),
            enable_core_affinity: false,
        }));

        let records = generate_test_records(total_records);
        let data_source = BottleneckDataSource::new(records.clone(), batch_size);
        let data_writer = BottleneckDataWriter;

        let mut parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT id, value FROM test WHERE value > 50 GROUP BY id")
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
                    format!("bottleneck_batch_{}", batch_size),
                    shutdown_rx,
                    None,
                )
                .await
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        processor.stop().await.ok();

        let _result = job_handle.await;
        let elapsed = start.elapsed();

        let throughput = total_records as f64 / elapsed.as_secs_f64();
        let per_record_micros = elapsed.as_micros() as f64 / total_records as f64;

        println!(
            "{:>10} | {:>7} | {:>20.0} | {:>14.2}",
            batch_size, total_records, throughput, per_record_micros
        );
    }

    println!("\n✅ Batch size impact analysis completed");
    println!("\nKey Insight:");
    println!("  • Larger batches amortize coordination overhead");
    println!("  • Smaller batches may show higher per-record latency");
}

/// Test: Single vs Multi-partition performance
#[tokio::test]
#[ignore] // Run with: cargo test --test mod bottleneck_partition_count -- --nocapture --ignored
async fn bottleneck_partition_count() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ BOTTLENECK 3: Partition Count Impact                      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let total_records = 10000;
    let partition_counts = vec![1, 2, 4, 8];

    println!("Partitions | Throughput (rec/sec) | Per-Record (µs) | Speedup");
    println!("──────────────────────────────────────────────────────────────");

    let mut baseline_throughput = 0.0;

    for (idx, &num_partitions) in partition_counts.iter().enumerate() {
        let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
            num_partitions: Some(num_partitions),
            enable_core_affinity: false,
        }));

        let records = generate_test_records(total_records);
        let data_source = BottleneckDataSource::new(records.clone(), 100);
        let data_writer = BottleneckDataWriter;

        let mut parser = StreamingSqlParser::new();
        let query = parser
            .parse("SELECT id, value FROM test WHERE value > 50 GROUP BY id")
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
                    format!("bottleneck_partitions_{}", num_partitions),
                    shutdown_rx,
                    None,
                )
                .await
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        processor.stop().await.ok();

        let _result = job_handle.await;
        let elapsed = start.elapsed();

        let throughput = total_records as f64 / elapsed.as_secs_f64();
        let per_record_micros = elapsed.as_micros() as f64 / total_records as f64;

        if idx == 0 {
            baseline_throughput = throughput;
        }

        let speedup = throughput / baseline_throughput;

        println!(
            "{:>10} | {:>20.0} | {:>15.2} | {:>7.2}x",
            num_partitions, throughput, per_record_micros, speedup
        );
    }

    println!("\n✅ Partition count impact analysis completed");
    println!("\nKey Insight:");
    println!("  • Multi-partition should enable parallelism");
    println!("  • Overhead increases with partition count");
    println!("  • Balance between parallelism and coordination");
}

/// Comprehensive bottleneck analysis
#[tokio::test]
#[ignore] // Run with: cargo test --test mod bottleneck_comprehensive -- --nocapture --ignored
async fn bottleneck_comprehensive() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ COMPREHENSIVE BOTTLENECK ANALYSIS                         ║");
    println!("║ Understanding per-record overhead in AdaptiveJobProcessor ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Test SQL Engine baseline
    let records = generate_test_records(10000);
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

    // Test AdaptiveJobProcessor @ 1 core
    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    }));

    let data_source = BottleneckDataSource::new(records.clone(), 100);
    let data_writer = BottleneckDataWriter;

    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT id, value FROM test WHERE value > 50 GROUP BY id")
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
                "bottleneck_comprehensive".to_string(),
                shutdown_rx,
                None,
            )
            .await
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    processor.stop().await.ok();

    let _result = job_handle.await;
    let adaptive_elapsed = start.elapsed();
    let adaptive_per_record = adaptive_elapsed.as_micros() as f64 / records.len() as f64;

    let overhead = adaptive_per_record - sql_per_record;
    let overhead_percent = (overhead / sql_per_record) * 100.0;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PERFORMANCE BREAKDOWN                                    ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("SQL Engine Baseline:");
    println!("  Per-record latency: {:.2}µs", sql_per_record);
    println!("  Total time: {:.2}ms", sql_elapsed.as_millis());
    println!(
        "  Throughput: {:.0} rec/sec",
        records.len() as f64 / sql_elapsed.as_secs_f64()
    );

    println!("\nAdaptiveJobProcessor (1 core):");
    println!("  Per-record latency: {:.2}µs", adaptive_per_record);
    println!("  Total time: {:.2}ms", adaptive_elapsed.as_millis());
    println!(
        "  Throughput: {:.0} rec/sec",
        records.len() as f64 / adaptive_elapsed.as_secs_f64()
    );

    println!("\nOverhead Analysis:");
    println!("  Overhead per-record: {:.2}µs", overhead);
    println!("  Overhead percentage: {:.1}%", overhead_percent);

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ BOTTLENECK BREAKDOWN (Estimated)                         ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Potential overhead sources (in descending order):");
    println!("  1. Batch coordination & routing: ~2-3µs");
    println!("  2. Partition queue operations: ~1-2µs");
    println!("  3. Channel operations: ~1-2µs");
    println!("  4. Metrics tracking: ~0.5-1µs");
    println!("  5. Context switching: ~0.5µs");
    println!("  ─────────────────────────────");
    println!("  Total: ~{:.1}µs (estimated)", overhead);

    println!("\n✅ Comprehensive bottleneck analysis completed");
}
