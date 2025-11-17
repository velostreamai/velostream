//! Test processor lifecycle - focus on stop() and process_job completion
//!
//! This test verifies that:
//! 1. process_job() completes when data source is exhausted
//! 2. stop() actually stops the processor
//! 3. Each processor type (Simple, Transactional, Adaptive) handles lifecycle correctly

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessor, JobProcessorConfig, JobProcessorFactory,
};
use velostream::velostream::sql::execution::{
    StreamExecutionEngine,
    types::{FieldValue, StreamRecord},
};
use velostream::velostream::sql::parser::StreamingSqlParser;

use async_trait::async_trait;

/// Simple mock data source that properly signals EOF
struct SimpleMockDataSource {
    records: Vec<StreamRecord>,
    current_index: usize,
    batch_size: usize,
}

impl SimpleMockDataSource {
    fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        Self {
            records,
            current_index: 0,
            batch_size,
        }
    }
}

#[async_trait]
impl DataReader for SimpleMockDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.current_index >= self.records.len() {
            println!(
                "[MockDataSource] EOF reached at index {}",
                self.current_index
            );
            return Ok(vec![]);
        }

        let end = std::cmp::min(self.current_index + self.batch_size, self.records.len());
        let batch: Vec<StreamRecord> = self.records[self.current_index..end].to_vec();
        self.current_index = end;

        println!(
            "[MockDataSource] Read batch of {} records (total so far: {})",
            batch.len(),
            self.current_index
        );
        Ok(batch)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("[MockDataSource] Commit called");
        Ok(())
    }

    async fn seek(
        &mut self,
        _offset: velostream::velostream::datasource::SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let has_more = self.current_index < self.records.len();
        println!(
            "[MockDataSource] has_more() = {} (index: {}, len: {})",
            has_more,
            self.current_index,
            self.records.len()
        );
        Ok(has_more)
    }
}

/// Simple mock data writer
struct SimpleMockDataWriter {
    records_written: usize,
}

impl SimpleMockDataWriter {
    fn new() -> Self {
        Self { records_written: 0 }
    }
}

#[async_trait]
impl DataWriter for SimpleMockDataWriter {
    async fn write(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records_written += 1;
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records_written += records.len();
        println!(
            "[MockDataWriter] write_batch: {} records (total: {})",
            records.len(),
            self.records_written
        );
        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("[MockDataWriter] Flush called");
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("[MockDataWriter] Commit called");
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

fn create_test_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("value".to_string(), FieldValue::Integer((i * 10) as i64));
            StreamRecord::new(fields)
        })
        .collect()
}

fn create_test_query() -> String {
    "SELECT id, value FROM test_source WHERE value > 0".to_string()
}

#[tokio::test]
async fn test_simple_processor_completes() {
    println!("\n=== TEST: Simple Processor Lifecycle ===");

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
        wait_on_empty_batch_ms: 100,
        enable_dlq: true,
        dlq_max_size: Some(100),
    };
    let processor = JobProcessorFactory::create_simple_with_config(config);
    let records = create_test_records(100);
    let data_source = SimpleMockDataSource::new(records.clone(), 10);
    let data_writer = SimpleMockDataWriter::new();

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse(&create_test_query())
        .expect("Failed to parse query");
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    println!("Starting process_job...");
    let start = std::time::Instant::now();

    // Test with 30-second timeout
    let result = timeout(
        Duration::from_secs(30),
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            query,
            "simple_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    let elapsed = start.elapsed();
    println!("process_job completed in {:?}", elapsed);

    match result {
        Ok(Ok(stats)) => {
            println!("✅ SUCCESS: process_job completed");
            println!("  Records processed: {}", stats.records_processed);
            println!("  Batches processed: {}", stats.batches_processed);
            assert!(stats.records_processed > 0, "Should have processed records");
        }
        Ok(Err(e)) => {
            panic!("❌ FAILED: process_job returned error: {:?}", e);
        }
        Err(_) => {
            panic!("❌ TIMEOUT: process_job did not complete within 30 seconds");
        }
    }

    println!("Calling processor.stop()...");
    processor.stop().await.expect("Failed to stop processor");
    println!("✅ stop() completed");
}

#[tokio::test]
async fn test_transactional_processor_completes() {
    println!("\n=== TEST: Transactional Processor Lifecycle ===");

    let processor = JobProcessorFactory::create(JobProcessorConfig::Transactional);
    let records = create_test_records(100);
    let data_source = SimpleMockDataSource::new(records.clone(), 10);
    let data_writer = SimpleMockDataWriter::new();

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse(&create_test_query())
        .expect("Failed to parse query");
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    println!("Starting process_job...");
    let start = std::time::Instant::now();

    let result = timeout(
        Duration::from_secs(30),
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            query,
            "transactional_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    let elapsed = start.elapsed();
    println!("process_job completed in {:?}", elapsed);

    match result {
        Ok(Ok(stats)) => {
            println!("✅ SUCCESS: process_job completed");
            println!("  Records processed: {}", stats.records_processed);
            println!("  Batches processed: {}", stats.batches_processed);
            assert!(stats.records_processed > 0, "Should have processed records");
        }
        Ok(Err(e)) => {
            panic!("❌ FAILED: process_job returned error: {:?}", e);
        }
        Err(_) => {
            panic!("❌ TIMEOUT: process_job did not complete within 30 seconds");
        }
    }

    println!("Calling processor.stop()...");
    processor.stop().await.expect("Failed to stop processor");
    println!("✅ stop() completed");
}

#[tokio::test]
async fn test_adaptive_processor_completes() {
    println!("\n=== TEST: Adaptive Processor Lifecycle ===");

    let processor = JobProcessorFactory::create(JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    });
    let records = create_test_records(100);
    let data_source = SimpleMockDataSource::new(records.clone(), 10);
    let data_writer = SimpleMockDataWriter::new();

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let mut parser = StreamingSqlParser::new();
    let query = parser
        .parse(&create_test_query())
        .expect("Failed to parse query");
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    println!("Starting process_job...");
    let start = std::time::Instant::now();

    let result = timeout(
        Duration::from_secs(30),
        processor.process_job(
            Box::new(data_source),
            Some(Box::new(data_writer)),
            engine,
            query,
            "adaptive_test".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    let elapsed = start.elapsed();
    println!("process_job completed in {:?}", elapsed);

    match result {
        Ok(Ok(stats)) => {
            println!("✅ SUCCESS: process_job completed");
            println!("  Records processed: {}", stats.records_processed);
            println!("  Batches processed: {}", stats.batches_processed);
            // The critical fix is that process_job completes promptly (< 1 second)
            // Previously it would hang for 30+ seconds
            assert!(
                elapsed.as_secs() < 2,
                "process_job should complete quickly (< 2 seconds), took {:?}",
                elapsed
            );
        }
        Ok(Err(e)) => {
            panic!("❌ FAILED: process_job returned error: {:?}", e);
        }
        Err(_) => {
            panic!("❌ TIMEOUT: process_job did not complete within 30 seconds");
        }
    }

    println!("Calling processor.stop()...");
    processor.stop().await.expect("Failed to stop processor");
    println!("✅ stop() completed");
}
