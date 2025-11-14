//! Profiling test to measure and analyze bottlenecks in AdaptiveJobProcessor
//!
//! This test enables debug logging to measure:
//! - Batch routing overhead in coordinator
//! - Queue push/pop latency
//! - Partition receiver processing overhead

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

struct ProfilingDataSource {
    records: Vec<StreamRecord>,
    current_index: usize,
    batch_size: usize,
}

impl ProfilingDataSource {
    fn new(record_count: usize, batch_size: usize) -> Self {
        let records = (0..record_count)
            .map(|i| {
                let mut fields = HashMap::new();
                fields.insert("id".to_string(), FieldValue::Integer(i as i64));
                fields.insert("value".to_string(), FieldValue::Integer((i * 10) as i64));
                StreamRecord::new(fields)
            })
            .collect();

        Self {
            records,
            current_index: 0,
            batch_size,
        }
    }
}

#[async_trait]
impl DataReader for ProfilingDataSource {
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

struct ProfilingDataWriter;

#[async_trait]
impl DataWriter for ProfilingDataWriter {
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

#[tokio::test]
#[ignore] // Run with: RUST_LOG=debug cargo test --test mod profiling_bottleneck -- --nocapture --ignored
async fn adaptive_processor_profiling_bottleneck() {
    // Initialize logging to see debug output
    let _ = env_logger::Builder::from_default_env()
        .is_test(true)
        .try_init();

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║   AdaptiveJobProcessor Bottleneck Profiling Test           ║");
    println!("║   Run with: RUST_LOG=debug cargo test ... --nocapture      ║");
    println!("║   Look for PartitionReceiver debug logs below              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    }));

    // Use moderate-sized dataset to see detailed logs
    let record_count = 5000;
    let batch_size = 500; // Larger batches to see overhead impact

    println!(
        "Profiling with {} records in batches of {}\n",
        record_count, batch_size
    );
    println!("Watch debug output below for PartitionReceiver timing:\n");

    let data_source = ProfilingDataSource::new(record_count, batch_size);
    let data_writer = ProfilingDataWriter;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT id, COUNT(*) as cnt FROM test WHERE value > 0 GROUP BY id")
        .expect("Failed to parse query");

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
            )
            .await
    });

    // Let all records process
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    processor.stop().await.expect("Failed to stop processor");

    let result = job_handle.await;
    let elapsed = start.elapsed();

    match result {
        Ok(Ok(stats)) => {
            let throughput = (record_count as f64 / elapsed.as_secs_f64()) as u64;
            println!("\n═══════════════════════════════════════════════════════════");
            println!("Results:");
            println!("  Records: {}", record_count);
            println!("  Time: {}ms", elapsed.as_millis());
            println!("  Throughput: {} rec/sec", throughput);
            println!(
                "  Per-record latency: {:.2}µs",
                (elapsed.as_micros() as f64) / (record_count as f64)
            );
            println!("═══════════════════════════════════════════════════════════\n");

            println!("Bottleneck Analysis:");
            println!("  SQL Engine baseline: ~3-4µs per record");
            println!(
                "  Actual per-record: {:.2}µs",
                (elapsed.as_micros() as f64) / (record_count as f64)
            );
            println!(
                "  Unaccounted overhead: ~{:.2}µs",
                (elapsed.as_micros() as f64) / (record_count as f64) - 3.5
            );
            println!("\nCheck debug logs above for PartitionReceiver breakdown");
        }
        Ok(Err(e)) => eprintln!("Error: {:?}", e),
        Err(e) => eprintln!("Task error: {:?}", e),
    }
}
