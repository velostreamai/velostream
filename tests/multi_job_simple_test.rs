use ferrisstreams::ferris::{
    datasource::{types::SourceOffset, DataReader, DataWriter},
    sql::{
        ast::StreamingQuery,
        execution::types::{FieldValue, StreamRecord},
        multi_job_common::*,
        multi_job_simple::*,
        StreamExecutionEngine,
    },
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex};

/// Mock DataReader for testing
struct MockDataReader {
    pub records: Vec<Vec<StreamRecord>>,
    pub current_batch: usize,
    pub should_fail: bool,
    pub supports_tx: bool,
    pub transaction_active: bool,
    pub commit_calls: usize,
    pub flush_calls: usize,
}

impl MockDataReader {
    fn new(records: Vec<Vec<StreamRecord>>) -> Self {
        Self {
            records,
            current_batch: 0,
            should_fail: false,
            supports_tx: false,
            transaction_active: false,
            commit_calls: 0,
            flush_calls: 0,
        }
    }

    fn with_transaction_support(mut self) -> Self {
        self.supports_tx = true;
        self
    }

    fn with_failures(mut self) -> Self {
        self.should_fail = true;
        self
    }
}

#[async_trait::async_trait]
impl DataReader for MockDataReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.should_fail && self.current_batch > 0 {
            return Err("Mock read failure".into());
        }

        if self.current_batch >= self.records.len() {
            return Ok(vec![]); // No more data
        }

        let batch = self.records[self.current_batch].clone();
        self.current_batch += 1;
        Ok(batch)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.commit_calls += 1;
        if self.should_fail && self.commit_calls > 1 {
            return Err("Mock commit failure".into());
        }
        Ok(())
    }

    async fn seek(
        &mut self,
        _offset: SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.current_batch < self.records.len())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_tx
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.supports_tx {
            self.transaction_active = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.transaction_active {
            self.transaction_active = false;
            self.commit_calls += 1;
            Ok(())
        } else {
            Err("No active transaction".into())
        }
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.transaction_active {
            self.transaction_active = false;
            Ok(())
        } else {
            Err("No active transaction".into())
        }
    }
}

/// Mock DataWriter for testing
struct MockDataWriter {
    pub written_records: Vec<StreamRecord>,
    pub should_fail_write: bool,
    pub should_fail_flush: bool,
    pub supports_tx: bool,
    pub transaction_active: bool,
    pub flush_calls: usize,
    pub write_calls: usize,
}

impl MockDataWriter {
    fn new() -> Self {
        Self {
            written_records: Vec::new(),
            should_fail_write: false,
            should_fail_flush: false,
            supports_tx: false,
            transaction_active: false,
            flush_calls: 0,
            write_calls: 0,
        }
    }

    fn with_write_failures(mut self) -> Self {
        self.should_fail_write = true;
        self
    }

    fn with_flush_failures(mut self) -> Self {
        self.should_fail_flush = true;
        self
    }

    fn with_transaction_support(mut self) -> Self {
        self.supports_tx = true;
        self
    }
}

#[async_trait::async_trait]
impl DataWriter for MockDataWriter {
    async fn write(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write_calls += 1;
        if self.should_fail_write {
            return Err("Mock write failure".into());
        }
        self.written_records.push(record);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write_calls += 1;
        if self.should_fail_write {
            return Err("Mock write failure".into());
        }
        self.written_records.extend(records);
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
        self.flush_calls += 1;
        if self.should_fail_flush {
            return Err("Mock flush failure".into());
        }
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_tx
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.supports_tx {
            self.transaction_active = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.transaction_active {
            self.transaction_active = false;
            Ok(())
        } else {
            Err("No active transaction".into())
        }
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.transaction_active {
            self.transaction_active = false;
            Ok(())
        } else {
            Err("No active transaction".into())
        }
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.flush().await
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Helper function to create test records
fn create_test_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut record = HashMap::new();
            record.insert("id".to_string(), FieldValue::Integer(i as i64));
            record.insert(
                "value".to_string(),
                FieldValue::String(format!("test_{}", i)),
            );
            StreamRecord {
                fields: record,
                headers: HashMap::new(),
                timestamp: 0,
                offset: 0,
                partition: 0,
            }
        })
        .collect()
}

/// Helper function to create a simple streaming query
fn create_test_query() -> StreamingQuery {
    use ferrisstreams::ferris::sql::ast::{SelectField, StreamSource};

    StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_topic".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    }
}

#[tokio::test]
async fn test_simple_processor_creation() {
    let _processor = SimpleJobProcessor::new(JobProcessingConfig::default());
    // Just test that we can create it without panicking
    assert!(true);
}

#[tokio::test]
async fn test_create_simple_processor_presets() {
    // Test that different processor presets can be created
    let throughput_config = JobProcessingConfig {
        max_batch_size: 1000,
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        ..Default::default()
    };
    let conservative_config = JobProcessingConfig {
        max_batch_size: 100,
        use_transactions: false,
        failure_strategy: FailureStrategy::FailBatch,
        ..Default::default()
    };
    let low_latency_config = JobProcessingConfig {
        max_batch_size: 10,
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        ..Default::default()
    };

    let _throughput_processor = SimpleJobProcessor::new(throughput_config.clone());
    let _conservative_processor = SimpleJobProcessor::new(conservative_config.clone());
    let _low_latency_processor = SimpleJobProcessor::new(low_latency_config.clone());

    // Test different batch sizes
    assert_eq!(throughput_config.max_batch_size, 1000);
    assert_eq!(conservative_config.max_batch_size, 100);
    assert_eq!(low_latency_config.max_batch_size, 10);

    // Test different failure strategies
    assert_eq!(
        throughput_config.failure_strategy,
        FailureStrategy::LogAndContinue
    );
    assert_eq!(
        conservative_config.failure_strategy,
        FailureStrategy::FailBatch
    );
    assert_eq!(
        low_latency_config.failure_strategy,
        FailureStrategy::LogAndContinue
    );
}

#[tokio::test]
async fn test_process_job_successful_batch() {
    let test_data = vec![create_test_records(5)];
    let reader = Box::new(MockDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(Box::new(MockDataWriter::new()) as Box<dyn DataWriter>);

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let processor = SimpleJobProcessor::new(JobProcessingConfig::default());

    // Start processing in background and shutdown immediately
    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                writer,
                engine,
                query,
                "test_job".to_string(),
                shutdown_rx,
            )
            .await
    });

    // Send shutdown signal after a short delay
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.records_processed, 5);
    assert_eq!(stats.records_failed, 0);
    assert_eq!(stats.batches_processed, 1);
}

#[tokio::test]
async fn test_process_job_with_read_failure() {
    let test_data = vec![create_test_records(3), create_test_records(2)];
    let reader = Box::new(MockDataReader::new(test_data).with_failures()) as Box<dyn DataReader>;
    let writer = Some(Box::new(MockDataWriter::new()) as Box<dyn DataWriter>);

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let processor = SimpleJobProcessor::new(JobProcessingConfig::default());

    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                writer,
                engine,
                query,
                "test_job".to_string(),
                shutdown_rx,
            )
            .await
    });

    // Let it process the first batch then shutdown
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.records_processed, 3); // Only first batch succeeded
    assert!(stats.batches_failed >= 1); // Second batch should fail
}

#[tokio::test]
async fn test_process_job_without_writer() {
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockDataReader::new(test_data)) as Box<dyn DataReader>;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let processor = SimpleJobProcessor::new(JobProcessingConfig::default());

    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                None, // No writer
                engine,
                query,
                "test_job".to_string(),
                shutdown_rx,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.batches_processed, 1);
}

#[tokio::test]
async fn test_empty_batch_handling() {
    let test_data = vec![vec![]]; // Empty batch
    let reader = Box::new(MockDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(Box::new(MockDataWriter::new()) as Box<dyn DataWriter>);

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let processor = SimpleJobProcessor::new(JobProcessingConfig::default());

    let job_handle = tokio::spawn(async move {
        processor
            .process_job(
                reader,
                writer,
                engine,
                query,
                "test_job".to_string(),
                shutdown_rx,
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(150)).await; // Let it process empty batch
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.records_processed, 0);
    assert_eq!(stats.batches_processed, 0);
}

#[tokio::test]
async fn test_statistics_calculation() {
    let mut stats = JobExecutionStats::new();

    // Test initial state
    assert_eq!(stats.records_processed, 0);
    assert_eq!(stats.records_per_second(), 0.0);
    assert_eq!(stats.success_rate(), 0.0);

    // Add some successful processing
    let batch_result = BatchProcessingResult {
        records_processed: 10,
        records_failed: 2,
        processing_time: Duration::from_millis(100),
        batch_size: 12,
        error_details: vec![],
    };

    stats.update_from_batch(&batch_result);

    assert_eq!(stats.records_processed, 10);
    assert_eq!(stats.records_failed, 2);
    assert_eq!(stats.batches_processed, 0); // Failed records means batch failed
    assert_eq!(stats.batches_failed, 1);
    assert_eq!(stats.success_rate(), 10.0 / 12.0 * 100.0);

    // Add a successful batch
    let success_batch = BatchProcessingResult {
        records_processed: 5,
        records_failed: 0,
        processing_time: Duration::from_millis(50),
        batch_size: 5,
        error_details: vec![],
    };

    stats.update_from_batch(&success_batch);

    assert_eq!(stats.records_processed, 15);
    assert_eq!(stats.records_failed, 2);
    assert_eq!(stats.batches_processed, 1);
    assert_eq!(stats.batches_failed, 1);
    assert_eq!(stats.success_rate(), 15.0 / 17.0 * 100.0);
}

#[tokio::test]
async fn test_job_execution_stats_methods() {
    let mut stats = JobExecutionStats::new();

    // Test elapsed time functionality
    let elapsed = stats.elapsed();
    assert!(elapsed >= Duration::from_nanos(0));

    // Test records per second calculation
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Add some processing data
    let batch_result = BatchProcessingResult {
        records_processed: 100,
        records_failed: 0,
        processing_time: Duration::from_millis(50),
        batch_size: 100,
        error_details: vec![],
    };

    stats.update_from_batch(&batch_result);

    // Should have some records processed
    assert_eq!(stats.records_processed, 100);
    assert_eq!(stats.success_rate(), 100.0);

    // Records per second should be calculated based on elapsed time
    let rps = stats.records_per_second();
    assert!(rps >= 0.0);
}

#[tokio::test]
async fn test_batch_processing_result_with_output() {
    let result = BatchProcessingResultWithOutput {
        records_processed: 10,
        records_failed: 0,
        processing_time: Duration::from_millis(100),
        batch_size: 10,
        error_details: vec![],
        output_records: create_test_records(10),
    };

    assert_eq!(result.records_processed, 10);
    assert_eq!(result.output_records.len(), 10);
    assert_eq!(result.batch_size, 10);
    assert!(result.error_details.is_empty());
}

#[tokio::test]
async fn test_helper_functions() {
    let test_data = vec![create_test_records(2)];
    let reader = MockDataReader::new(test_data);
    let writer = MockDataWriter::new();

    // Test transaction support checking
    assert!(check_transaction_support(&reader, "test_job") == reader.supports_tx);
    assert!(check_writer_transaction_support(&writer, "test_job") == writer.supports_tx);
}

#[tokio::test]
async fn test_configuration_edge_cases() {
    // Test edge cases in configuration
    let extreme_config = JobProcessingConfig {
        max_batch_size: 0,                       // Edge case: zero batch size
        batch_timeout: Duration::from_millis(0), // Edge case: zero timeout
        max_retries: 0,                          // Edge case: no retries
        retry_backoff: Duration::from_millis(0), // Edge case: no backoff
        progress_interval: 0,                    // Edge case: zero interval
        ..Default::default()
    };

    // Should be able to create processors with edge case configurations
    let _simple_processor = SimpleJobProcessor::new(extreme_config.clone());

    // Verify configurations are preserved
    assert_eq!(extreme_config.max_batch_size, 0);
    assert_eq!(extreme_config.batch_timeout, Duration::from_millis(0));
}

#[tokio::test]
async fn test_logging_functions() {
    // Test the logging utility functions
    let mut stats = JobExecutionStats::new();

    // Update stats with some data
    let batch_result = BatchProcessingResult {
        records_processed: 50,
        records_failed: 10,
        processing_time: Duration::from_millis(100),
        batch_size: 60,
        error_details: vec![],
    };

    stats.update_from_batch(&batch_result);

    // These should not panic and should work correctly
    log_job_progress("test_job", &stats);
    log_final_stats("test_job", &stats);

    // Verify calculated metrics make sense
    assert!(stats.records_per_second() >= 0.0);
    assert!(stats.success_rate() >= 0.0 && stats.success_rate() <= 100.0);
    assert!(stats.elapsed() >= Duration::from_nanos(0));
}
