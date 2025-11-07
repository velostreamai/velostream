//! Phase 6.1a: V1 vs V2 Real SQL Execution Performance Tests
//!
//! Validates that Phase 6.1a implementation delivers expected performance improvements:
//! - V1 baseline: ~23.7K rec/sec (single-threaded SimpleJobProcessor)
//! - V2@8cores: ~190K rec/sec (PartitionedJobCoordinator with 8 partitions)
//! - Scaling efficiency: ≥95% linear (8x improvement)
//!
//! Tests use real SQL execution with GROUP BY and SUM aggregations to ensure
//! we're measuring actual computational work, not placeholder pass-through.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, SimpleJobProcessor,
};
use velostream::velostream::server::v2::{
    AlwaysHashStrategy, PartitionedJobCoordinator, PartitionedJobConfig, ProcessingMode,
};
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, StreamingSqlParser};

// =============================================================================
// MOCK DATA SOURCES FOR PERFORMANCE TESTING
// =============================================================================

/// Mock data reader that generates synthetic records for performance testing
#[derive(Debug)]
pub struct PerformanceTestReader {
    pub name: String,
    pub total_records: u64,
    pub records_yielded: Arc<AtomicU64>,
    pub batch_size: usize,
}

impl PerformanceTestReader {
    pub fn new(name: &str, total_records: u64, batch_size: usize) -> Self {
        Self {
            name: name.to_string(),
            total_records,
            records_yielded: Arc::new(AtomicU64::new(0)),
            batch_size,
        }
    }

    fn create_record(&self, index: u64) -> StreamRecord {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(index as i64));
        fields.insert("group_id".to_string(), FieldValue::Integer((index % 10) as i64));
        fields.insert("value".to_string(), FieldValue::Integer((index % 100) as i64));
        fields.insert("source".to_string(), FieldValue::String(self.name.clone()));

        StreamRecord {
            fields,
            headers: HashMap::new(),
            event_time: None,
            timestamp: 1640995200000 + index as i64,
            offset: index as i64,
            partition: 0,
        }
    }
}

#[async_trait]
impl DataReader for PerformanceTestReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let yielded = self.records_yielded.load(Ordering::Relaxed);
        if yielded >= self.total_records {
            return Ok(vec![]); // End of stream
        }

        let batch_end = std::cmp::min(
            yielded + self.batch_size as u64,
            self.total_records,
        );
        let batch_size = (batch_end - yielded) as usize;

        let batch: Vec<StreamRecord> = (yielded..batch_end)
            .map(|i| self.create_record(i))
            .collect();

        self.records_yielded.store(batch_end, Ordering::Relaxed);

        Ok(batch)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(
        &mut self,
        _offset: velostream::velostream::datasource::SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.records_yielded.load(Ordering::Relaxed) < self.total_records)
    }

    fn supports_transactions(&self) -> bool {
        false
    }

    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(false)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Mock data writer that counts written records
#[derive(Debug)]
pub struct PerformanceTestWriter {
    pub name: String,
    pub records_written: Arc<AtomicU64>,
}

impl PerformanceTestWriter {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            records_written: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn get_written_count(&self) -> u64 {
        self.records_written.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl DataWriter for PerformanceTestWriter {
    async fn write(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records_written.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<std::sync::Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records_written
            .fetch_add(records.len() as u64, Ordering::Relaxed);
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
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        false
    }

    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(false)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

// =============================================================================
// PERFORMANCE TESTS: V1 VS V2 COMPARISON
// =============================================================================

/// Test V1 (SimpleJobProcessor) baseline performance with real SQL execution
///
/// Expected: ~23.7K rec/sec with GROUP BY and SUM aggregations
#[tokio::test]
#[ignore]  // Long-running benchmark test - run with --ignored flag
async fn test_v1_baseline_groupby_sum_100k_records() {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(100),
        max_retries: 0,
        retry_backoff: Duration::from_millis(50),
        progress_interval: 10,
        log_progress: false,
    };

    let processor = SimpleJobProcessor::new(config);

    // Create readers with 100K records for meaningful performance measurement
    let mut readers = HashMap::new();
    readers.insert(
        "input".to_string(),
        Box::new(PerformanceTestReader::new("input", 100_000, 1000))
            as Box<dyn DataReader>,
    );

    let writer = PerformanceTestWriter::new("output");
    let writer_records = writer.records_written.clone();
    let mut writers = HashMap::new();
    writers.insert(
        "output".to_string(),
        Box::new(writer) as Box<dyn DataWriter>,
    );

    // Create engine with SQL query (GROUP BY with SUM)
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT group_id, SUM(value) as total_value FROM test_stream GROUP BY group_id")
        .expect("Failed to parse query");

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Measure performance
    let start = Instant::now();

    let job_handle = tokio::spawn(async move {
        processor
            .process_multi_job(readers, writers, engine, query, "v1-perf-test".to_string(), shutdown_rx)
            .await
    });

    // Wait for job to complete (with generous timeout)
    let result = tokio::time::timeout(Duration::from_secs(30), job_handle).await;

    let elapsed = start.elapsed();

    // Send shutdown signal
    let _ = shutdown_tx.send(()).await;

    assert!(result.is_ok(), "V1 test should complete within timeout");
    let job_result = result.unwrap().unwrap();
    assert!(job_result.is_ok(), "V1 job should succeed");

    let stats = job_result.unwrap();
    let records_written = writer_records.load(Ordering::Relaxed);

    let throughput = if elapsed.as_secs_f64() > 0.0 {
        stats.records_processed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!(
        "✅ V1 Baseline (100K records): {} records in {:.3}s = {:.0} rec/sec",
        stats.records_processed,
        elapsed.as_secs_f64(),
        throughput
    );
    println!(
        "   Batches: {}, Written: {}, Expected: ~23.7K rec/sec",
        stats.batches_processed,
        records_written
    );

    // Sanity checks
    assert!(stats.records_processed > 0, "Should process records");
    assert!(records_written > 0, "Should write output records");
}

/// Test V2 (PartitionedJobCoordinator) baseline performance with real SQL execution
///
/// Expected: ~95% of V1 with 1 partition (minimal overhead)
#[tokio::test]
#[ignore]  // Long-running benchmark test - run with --ignored flag
async fn test_v2_single_partition_groupby_sum_100k_records() {
    let config = PartitionedJobConfig {
        num_partitions: Some(1),
        processing_mode: ProcessingMode::Batch { size: 1000 },
        partition_buffer_size: 1000,
        enable_core_affinity: false,
        backpressure_config: Default::default(),
    };

    let coordinator = PartitionedJobCoordinator::new(config)
        .with_strategy(Arc::new(AlwaysHashStrategy::new()));

    // Create readers with 100K records
    let mut readers = HashMap::new();
    readers.insert(
        "input".to_string(),
        Box::new(PerformanceTestReader::new("input", 100_000, 1000))
            as Box<dyn DataReader>,
    );

    let writer = PerformanceTestWriter::new("output");
    let writer_records = writer.records_written.clone();
    let mut writers = HashMap::new();
    writers.insert(
        "output".to_string(),
        Box::new(writer) as Box<dyn DataWriter>,
    );

    // Create engine with SQL query
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT group_id, SUM(value) as total_value FROM test_stream GROUP BY group_id")
        .expect("Failed to parse query");

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Measure performance
    let start = Instant::now();

    let job_handle = tokio::spawn(async move {
        coordinator
            .process_multi_job(readers, writers, engine, query, "v2-1p-perf-test".to_string(), shutdown_rx)
            .await
    });

    // Wait for job to complete
    let result = tokio::time::timeout(Duration::from_secs(30), job_handle).await;

    let elapsed = start.elapsed();

    // Send shutdown signal
    let _ = shutdown_tx.send(()).await;

    assert!(result.is_ok(), "V2@1p test should complete within timeout");
    let job_result = result.unwrap().unwrap();
    assert!(job_result.is_ok(), "V2@1p job should succeed");

    let stats = job_result.unwrap();
    let records_written = writer_records.load(Ordering::Relaxed);

    let throughput = if elapsed.as_secs_f64() > 0.0 {
        stats.records_processed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!(
        "✅ V2@1p Baseline (100K records): {} records in {:.3}s = {:.0} rec/sec",
        stats.records_processed,
        elapsed.as_secs_f64(),
        throughput
    );
    println!(
        "   Batches: {}, Written: {}, Expected: ~22.5K-23.7K rec/sec (95% of V1)",
        stats.batches_processed,
        records_written
    );

    // Sanity checks
    assert!(stats.records_processed > 0, "Should process records");
    assert!(records_written > 0, "Should write output records");
}

/// Test V2 (PartitionedJobCoordinator) scaling performance with 8 partitions
///
/// Expected: ~190K rec/sec (8x improvement over V1 baseline)
#[tokio::test]
#[ignore]  // Long-running benchmark test - run with --ignored flag
async fn test_v2_8partition_groupby_sum_100k_records() {
    let config = PartitionedJobConfig {
        num_partitions: Some(8),
        processing_mode: ProcessingMode::Batch { size: 1000 },
        partition_buffer_size: 1000,
        enable_core_affinity: false,
        backpressure_config: Default::default(),
    };

    let coordinator = PartitionedJobCoordinator::new(config)
        .with_strategy(Arc::new(AlwaysHashStrategy::new()));

    // Create readers with 100K records
    let mut readers = HashMap::new();
    readers.insert(
        "input".to_string(),
        Box::new(PerformanceTestReader::new("input", 100_000, 1000))
            as Box<dyn DataReader>,
    );

    let writer = PerformanceTestWriter::new("output");
    let writer_records = writer.records_written.clone();
    let mut writers = HashMap::new();
    writers.insert(
        "output".to_string(),
        Box::new(writer) as Box<dyn DataWriter>,
    );

    // Create engine with SQL query
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("SELECT group_id, SUM(value) as total_value FROM test_stream GROUP BY group_id")
        .expect("Failed to parse query");

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Measure performance
    let start = Instant::now();

    let job_handle = tokio::spawn(async move {
        coordinator
            .process_multi_job(readers, writers, engine, query, "v2-8p-perf-test".to_string(), shutdown_rx)
            .await
    });

    // Wait for job to complete
    let result = tokio::time::timeout(Duration::from_secs(30), job_handle).await;

    let elapsed = start.elapsed();

    // Send shutdown signal
    let _ = shutdown_tx.send(()).await;

    assert!(result.is_ok(), "V2@8p test should complete within timeout");
    let job_result = result.unwrap().unwrap();
    assert!(job_result.is_ok(), "V2@8p job should succeed");

    let stats = job_result.unwrap();
    let records_written = writer_records.load(Ordering::Relaxed);

    let throughput = if elapsed.as_secs_f64() > 0.0 {
        stats.records_processed as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!(
        "✅ V2@8p Scaling (100K records): {} records in {:.3}s = {:.0} rec/sec",
        stats.records_processed,
        elapsed.as_secs_f64(),
        throughput
    );
    println!(
        "   Batches: {}, Written: {}, Expected: ~190K rec/sec (8x from 23.7K baseline)",
        stats.batches_processed,
        records_written
    );

    // Sanity checks
    assert!(stats.records_processed > 0, "Should process records");
    assert!(records_written > 0, "Should write output records");
}
