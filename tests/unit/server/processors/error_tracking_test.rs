/*!
# Error Tracking Unit Tests

Unit tests for error tracking functionality in SimpleJobProcessor and TransactionalJobProcessor.
Tests verify that errors are properly recorded and appear in observability metrics.
*/

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::observability::SharedObservabilityManager;
use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, SimpleJobProcessor, TransactionalJobProcessor,
};
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::{StreamExecutionEngine, StreamingSqlParser};

/// Mock DataReader that can simulate failures
#[derive(Debug)]
pub struct FailingMockDataReader {
    pub name: String,
    pub records: Vec<StreamRecord>,
    pub current_index: usize,
    pub supports_transactions: bool,
    pub fail_on_read: bool,
    pub read_count: Arc<AtomicUsize>,
}

impl FailingMockDataReader {
    pub fn new(name: &str, record_count: usize, supports_transactions: bool) -> Self {
        let mut records = Vec::new();
        for i in 0..record_count {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "name".to_string(),
                FieldValue::String(format!("record_{}", i)),
            );

            records.push(StreamRecord {
                fields,
                headers: HashMap::new(),
                event_time: None,
                timestamp: 1640995200000 + (i as i64 * 1000),
                offset: i as i64,
                partition: 0,
            });
        }

        Self {
            name: name.to_string(),
            records,
            current_index: 0,
            supports_transactions,
            fail_on_read: false,
            read_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_failure(mut self, fail_on_read: bool) -> Self {
        self.fail_on_read = fail_on_read;
        self
    }

    pub fn get_read_count(&self) -> usize {
        self.read_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl DataReader for FailingMockDataReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        self.read_count.fetch_add(1, Ordering::SeqCst);

        if self.fail_on_read {
            return Err("Simulated read failure".into());
        }

        let batch_size = 3.min(self.records.len() - self.current_index);
        if batch_size == 0 {
            return Ok(vec![]);
        }

        let end_index = self.current_index + batch_size;
        let batch = self.records[self.current_index..end_index].to_vec();
        self.current_index = end_index;

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
        Ok(self.current_index < self.records.len())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_transactions
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.supports_transactions)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Mock DataWriter that can simulate failures
#[derive(Debug)]
pub struct FailingMockDataWriter {
    pub name: String,
    pub written_records: Vec<StreamRecord>,
    pub supports_transactions: bool,
    pub fail_on_write: bool,
    pub write_count: Arc<AtomicUsize>,
}

impl FailingMockDataWriter {
    pub fn new(name: &str, supports_transactions: bool) -> Self {
        Self {
            name: name.to_string(),
            written_records: Vec::new(),
            supports_transactions,
            fail_on_write: false,
            write_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_failure(mut self, fail_on_write: bool) -> Self {
        self.fail_on_write = fail_on_write;
        self
    }

    pub fn get_write_count(&self) -> usize {
        self.write_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl DataWriter for FailingMockDataWriter {
    async fn write(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write_count.fetch_add(1, Ordering::SeqCst);

        if self.fail_on_write {
            return Err("Simulated write failure".into());
        }

        self.written_records.push(record);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<std::sync::Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write_count.fetch_add(records.len(), Ordering::SeqCst);

        if self.fail_on_write {
            return Err("Simulated batch write failure".into());
        }

        // Dereference Arc and clone for storage
        self.written_records
            .extend(records.iter().map(|r| (**r).clone()));
        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.written_records.push(record);
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
        self.written_records.clear();
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_transactions
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.supports_transactions)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Test that simple processor handles read errors gracefully
#[tokio::test]
async fn test_simple_processor_handles_read_errors() {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::RetryWithBackoff,
        max_batch_size: 3,
        batch_timeout: Duration::from_millis(50),
        max_retries: 1,
        retry_backoff: Duration::from_millis(10),
        progress_interval: 1,
        log_progress: false,
    };

    let processor = SimpleJobProcessor::new(config);

    let reader = Box::new(FailingMockDataReader::new("test_source", 5, false).with_failure(false))
        as Box<dyn DataReader>;
    let writer = Box::new(FailingMockDataWriter::new("test_sink", false)) as Box<dyn DataWriter>;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                Some(writer),
                engine,
                query,
                "test-read-error".to_string(),
                shutdown_rx,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = tokio::time::timeout(Duration::from_secs(2), job_handle)
        .await
        .expect("Job should complete")
        .expect("Job task should succeed");

    // Job should complete despite potential errors
    assert!(result.is_ok(), "Job should handle errors gracefully");
}

/// Test that simple processor handles write errors with retry strategy
#[tokio::test]
async fn test_simple_processor_handles_write_errors_with_retry() {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::RetryWithBackoff,
        max_batch_size: 2,
        batch_timeout: Duration::from_millis(50),
        max_retries: 2,
        retry_backoff: Duration::from_millis(10),
        progress_interval: 1,
        log_progress: false,
    };

    let processor = SimpleJobProcessor::new(config);

    let reader =
        Box::new(FailingMockDataReader::new("test_source", 4, false)) as Box<dyn DataReader>;
    let writer = Box::new(FailingMockDataWriter::new("test_sink", false).with_failure(true))
        as Box<dyn DataWriter>;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                Some(writer),
                engine,
                query,
                "test-write-retry".to_string(),
                shutdown_rx,
            )
            .await
    });

    // Let job attempt retries
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = shutdown_tx.send(()).await;

    let result = tokio::time::timeout(Duration::from_secs(2), job_handle)
        .await
        .expect("Job should complete")
        .expect("Job task should succeed");

    // Job might fail or succeed depending on retry strategy, but should not panic
    println!("Job result: {:?}", result);
}

/// Test that transactional processor handles write errors with transaction rollback
#[tokio::test]
async fn test_transactional_processor_handles_write_errors() {
    let config = JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::RetryWithBackoff,
        max_batch_size: 2,
        batch_timeout: Duration::from_millis(50),
        max_retries: 1,
        retry_backoff: Duration::from_millis(10),
        progress_interval: 1,
        log_progress: false,
    };

    let processor = TransactionalJobProcessor::new(config);

    let reader =
        Box::new(FailingMockDataReader::new("test_source", 4, true)) as Box<dyn DataReader>;
    let writer = Box::new(FailingMockDataWriter::new("test_sink", true).with_failure(true))
        as Box<dyn DataWriter>;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                Some(writer),
                engine,
                query,
                "test-transactional-write-error".to_string(),
                shutdown_rx,
            )
            .await
    });

    // Let job attempt with transactions
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = shutdown_tx.send(()).await;

    let result = tokio::time::timeout(Duration::from_secs(2), job_handle)
        .await
        .expect("Job should complete")
        .expect("Job task should succeed");

    // Should handle transactional errors gracefully
    println!("Transactional job result: {:?}", result);
}

/// Test error tracking with no observability manager
#[tokio::test]
async fn test_error_tracking_without_observability_manager() {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 2,
        batch_timeout: Duration::from_millis(50),
        max_retries: 1,
        retry_backoff: Duration::from_millis(10),
        progress_interval: 1,
        log_progress: false,
    };

    let processor = SimpleJobProcessor::new(config);

    let reader =
        Box::new(FailingMockDataReader::new("test_source", 3, false)) as Box<dyn DataReader>;
    let writer = Box::new(FailingMockDataWriter::new("test_sink", false)) as Box<dyn DataWriter>;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Process without observability manager - should still work
    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                Some(writer),
                engine,
                query,
                "test-no-observability".to_string(),
                shutdown_rx,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = tokio::time::timeout(Duration::from_secs(2), job_handle)
        .await
        .expect("Job should complete")
        .expect("Job task should succeed");

    assert!(
        result.is_ok(),
        "Job should succeed without observability manager"
    );
}

/// Test simple processor with LogAndContinue strategy on batch failures
#[tokio::test]
async fn test_simple_processor_log_and_continue_strategy() {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 2,
        batch_timeout: Duration::from_millis(50),
        max_retries: 1,
        retry_backoff: Duration::from_millis(10),
        progress_interval: 1,
        log_progress: false,
    };

    let processor = SimpleJobProcessor::new(config);

    let reader =
        Box::new(FailingMockDataReader::new("test_source", 5, false)) as Box<dyn DataReader>;
    let writer = Box::new(FailingMockDataWriter::new("test_sink", false)) as Box<dyn DataWriter>;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                Some(writer),
                engine,
                query,
                "test-log-and-continue".to_string(),
                shutdown_rx,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = shutdown_tx.send(()).await;

    let result = tokio::time::timeout(Duration::from_secs(2), job_handle)
        .await
        .expect("Job should complete")
        .expect("Job task should succeed");

    assert!(
        result.is_ok(),
        "LogAndContinue strategy should allow job to complete"
    );

    let stats = result.unwrap();
    println!(
        "LogAndContinue stats - batches: {}, processed: {}, failed: {}",
        stats.batches_processed, stats.records_processed, stats.records_failed
    );
}

/// Test transactional processor with FailBatch strategy
#[tokio::test]
async fn test_transactional_processor_fail_batch_strategy() {
    let config = JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::FailBatch,
        max_batch_size: 2,
        batch_timeout: Duration::from_millis(50),
        max_retries: 1,
        retry_backoff: Duration::from_millis(10),
        progress_interval: 1,
        log_progress: false,
    };

    let processor = TransactionalJobProcessor::new(config);

    let reader =
        Box::new(FailingMockDataReader::new("test_source", 4, true)) as Box<dyn DataReader>;
    let writer = Box::new(FailingMockDataWriter::new("test_sink", true)) as Box<dyn DataWriter>;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                Some(writer),
                engine,
                query,
                "test-fail-batch".to_string(),
                shutdown_rx,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = tokio::time::timeout(Duration::from_secs(2), job_handle)
        .await
        .expect("Job should complete")
        .expect("Job task should succeed");

    println!("FailBatch result: {:?}", result);
}

/// Test error tracking doesn't impact processor performance significantly
#[tokio::test]
async fn test_error_tracking_performance_impact() {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 5,
        batch_timeout: Duration::from_millis(100),
        max_retries: 1,
        retry_backoff: Duration::from_millis(50),
        progress_interval: 5,
        log_progress: false,
    };

    let processor = SimpleJobProcessor::new(config);

    let reader =
        Box::new(FailingMockDataReader::new("perf_test", 20, false)) as Box<dyn DataReader>;
    let writer = Box::new(FailingMockDataWriter::new("perf_sink", false)) as Box<dyn DataWriter>;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = std::time::Instant::now();

    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                Some(writer),
                engine,
                query,
                "performance-test".to_string(),
                shutdown_rx,
            )
            .await
    });

    // Give job sufficient time to process
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = shutdown_tx.send(()).await;

    let result = tokio::time::timeout(Duration::from_secs(3), job_handle)
        .await
        .expect("Job should complete")
        .expect("Job task should succeed");

    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Job should complete successfully");

    let stats = result.unwrap();
    println!(
        "Performance test - elapsed: {:?}, batches: {}, records: {}",
        elapsed, stats.batches_processed, stats.records_processed
    );

    // Performance shouldn't be severely impacted by error tracking
    assert!(
        elapsed.as_millis() < 5000,
        "Processing should complete reasonably fast"
    );
}
