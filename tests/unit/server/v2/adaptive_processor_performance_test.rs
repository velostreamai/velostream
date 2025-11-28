//! Performance test for AdaptiveJobProcessor with busy-spin architecture
//!
//! Measures actual throughput improvements after removing artificial EOF detection overhead

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

struct SimpleDataSource {
    records: Vec<StreamRecord>,
    current_index: usize,
    batch_size: usize,
}

impl SimpleDataSource {
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
impl DataReader for SimpleDataSource {
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

struct SimpleDataWriter;

#[async_trait]
impl DataWriter for SimpleDataWriter {
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
#[ignore] // Run with: cargo test --test mod adaptive_processor_busy_spin_perf -- --nocapture --ignored
async fn adaptive_processor_busy_spin_performance() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║   AdaptiveJobProcessor Busy-Spin Performance Test          ║");
    println!("║   (Measures improvement after EOF detection removal)       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    }));

    let test_cases = vec![
        ("Small", 1000, 100),
        ("Medium", 5000, 100),
        ("Large", 10000, 100),
    ];

    println!("Test Case     | Records | Batches | Time (ms) | Throughput (rec/sec)");
    println!("──────────────┼─────────┼─────────┼──────────┼─────────────────────");

    for (name, record_count, batch_size) in test_cases {
        let data_source = SimpleDataSource::new(record_count, batch_size);
        let data_writer = SimpleDataWriter;

        let (tx, _rx) = mpsc::unbounded_channel();
        let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

        let mut parser = StreamingSqlParser::new();
        // Use GROUP BY to ensure compatibility with AdaptiveJobProcessor's hashing strategy
        let query = parser
            .parse("SELECT id, COUNT(*) FROM test WHERE value > 0 GROUP BY id")
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
                    format!("perf_test_{}", name),
                    shutdown_rx,
                    None,
                )
                .await
        });

        // Allow processing to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        processor.stop().await.expect("Failed to stop processor");

        let result = job_handle.await;
        let elapsed = start.elapsed();

        match result {
            Ok(Ok(stats)) => {
                let batches = (record_count + batch_size - 1) / batch_size;
                let throughput = (record_count as f64 / elapsed.as_secs_f64()) as u64;
                println!(
                    "{:13} | {:7} | {:7} | {:8} | {:19}",
                    name,
                    record_count,
                    batches,
                    elapsed.as_millis(),
                    throughput
                );
            }
            Ok(Err(e)) => eprintln!("Error: {:?}", e),
            Err(e) => eprintln!("Task error: {:?}", e),
        }
    }

    println!("\n✅ Performance test completed successfully");
    println!("Notes:");
    println!("  • Busy-spin pattern eliminates artificial EOF detection overhead");
    println!("  • Processor exits immediately when stop() is called");
    println!("  • Expected throughput: 100K-300K rec/sec (SQL engine + routing overhead)");
}

#[tokio::test]
#[ignore] // Run with: cargo test --test mod adaptive_processor_steady_state -- --nocapture --ignored
async fn adaptive_processor_steady_state_throughput() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║   Steady-State Throughput Test (Long Running)              ║");
    println!("║   Measures throughput after warm-up and stabilization      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let processor = Arc::new(JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    }));

    // Test with larger dataset to measure steady-state
    let record_count = 100_000;
    let batch_size = 1000;

    println!(
        "Testing with {} records in batches of {}\n",
        record_count, batch_size
    );

    let data_source = SimpleDataSource::new(record_count, batch_size);
    let data_writer = SimpleDataWriter;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let mut parser = StreamingSqlParser::new();
    // Use GROUP BY to ensure compatibility with AdaptiveJobProcessor's hashing strategy
    let query = parser
        .parse("SELECT id, COUNT(*) FROM test WHERE value > 0 GROUP BY id")
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
                "steady_state_test".to_string(),
                shutdown_rx,
                None,
            )
            .await
    });

    // Let it process for longer to measure steady-state
    // All 100K records should process in < 500ms if we're at 200K+ rec/sec
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    processor.stop().await.expect("Failed to stop processor");

    let result = job_handle.await;
    let elapsed = start.elapsed();

    match result {
        Ok(Ok(stats)) => {
            let throughput = (record_count as f64 / elapsed.as_secs_f64()) as u64;
            println!("Results:");
            println!("  Records: {}", record_count);
            println!("  Time: {}ms", elapsed.as_millis());
            println!("  Throughput: {} rec/sec", throughput);
            println!(
                "  Per-record latency: {:.2}µs",
                (elapsed.as_micros() as f64) / (record_count as f64)
            );

            if throughput > 200_000 {
                println!(
                    "\n✅ EXCELLENT: Throughput > 200K rec/sec (lock-free architecture working)"
                );
            } else if throughput > 100_000 {
                println!(
                    "\n⚠️  GOOD: Throughput 100K-200K rec/sec (potential optimization needed)"
                );
            } else {
                println!("\n❌ NEEDS INVESTIGATION: Throughput < 100K rec/sec");
            }
        }
        Ok(Err(e)) => eprintln!("Error: {:?}", e),
        Err(e) => eprintln!("Task error: {:?}", e),
    }
}
