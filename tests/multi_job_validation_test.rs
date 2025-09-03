use ferrisstreams::ferris::{
    datasource::{DataReader, DataWriter},
    sql::{
        ast::{EmitMode, SelectField, StreamSource},
        execution::types::{FieldValue, StreamRecord},
        multi_job_common::*,
        multi_job_simple::*,
        multi_job_transactional::*,
        StreamExecutionEngine, StreamingQuery,
    },
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex};

/// Mock failing DataReader for testing error conditions
struct FailingMockDataReader {
    pub fail_on_read: bool,
    pub fail_on_commit: bool,
    pub fail_on_begin_tx: bool,
    pub fail_on_commit_tx: bool,
    pub supports_tx: bool,
    pub read_count: AtomicU32,
    pub max_reads: u32,
}

impl FailingMockDataReader {
    fn new() -> Self {
        Self {
            fail_on_read: false,
            fail_on_commit: false,
            fail_on_begin_tx: false,
            fail_on_commit_tx: false,
            supports_tx: true,
            read_count: AtomicU32::new(0),
            max_reads: 1, // Default to only one read to prevent infinite loops
        }
    }

    fn with_read_failure(mut self) -> Self {
        self.fail_on_read = true;
        self
    }

    fn with_commit_failure(mut self) -> Self {
        self.fail_on_commit = true;
        self
    }

    fn with_begin_tx_failure(mut self) -> Self {
        self.fail_on_begin_tx = true;
        self
    }

    fn with_commit_tx_failure(mut self) -> Self {
        self.fail_on_commit_tx = true;
        self
    }

    fn without_transactions(mut self) -> Self {
        self.supports_tx = false;
        self
    }
}

#[async_trait::async_trait]
impl DataReader for FailingMockDataReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_on_read {
            return Err("Intentional read failure".into());
        }

        // Increment read count
        self.read_count.fetch_add(1, Ordering::Relaxed);

        let mut record = HashMap::new();
        record.insert(
            "test_field".to_string(),
            FieldValue::String("test_value".to_string()),
        );
        Ok(vec![StreamRecord {
            fields: record,
            headers: HashMap::new(),
            timestamp: 0,
            offset: 0,
            partition: 0,
        }])
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_on_commit {
            return Err("Intentional commit failure".into());
        }
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_tx
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_on_begin_tx {
            return Err("Intentional begin transaction failure".into());
        }
        Ok(self.supports_tx)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_on_commit_tx {
            return Err("Intentional commit transaction failure".into());
        }
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn seek(
        &mut self,
        _offset: ferrisstreams::ferris::datasource::types::SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.read_count.load(Ordering::Relaxed) < self.max_reads)
    }
}

/// Mock failing DataWriter for testing error conditions
struct FailingMockDataWriter {
    pub fail_on_write: bool,
    pub fail_on_flush: bool,
    pub fail_on_begin_tx: bool,
    pub fail_on_commit_tx: bool,
    pub supports_tx: bool,
}

impl FailingMockDataWriter {
    fn new() -> Self {
        Self {
            fail_on_write: false,
            fail_on_flush: false,
            fail_on_begin_tx: false,
            fail_on_commit_tx: false,
            supports_tx: true,
        }
    }

    fn with_write_failure(mut self) -> Self {
        self.fail_on_write = true;
        self
    }

    fn with_flush_failure(mut self) -> Self {
        self.fail_on_flush = true;
        self
    }

    fn with_begin_tx_failure(mut self) -> Self {
        self.fail_on_begin_tx = true;
        self
    }

    fn with_commit_tx_failure(mut self) -> Self {
        self.fail_on_commit_tx = true;
        self
    }

    fn without_transactions(mut self) -> Self {
        self.supports_tx = false;
        self
    }
}

#[async_trait::async_trait]
impl DataWriter for FailingMockDataWriter {
    async fn write_batch(
        &mut self,
        _records: Vec<StreamRecord>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_on_write {
            return Err("Intentional write failure".into());
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_on_flush {
            return Err("Intentional flush failure".into());
        }
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_tx
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_on_begin_tx {
            return Err("Intentional begin transaction failure".into());
        }
        Ok(self.supports_tx)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_on_commit_tx {
            return Err("Intentional commit transaction failure".into());
        }
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn write(
        &mut self,
        _record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.fail_on_write {
            return Err("Intentional write failure".into());
        }
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

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.flush().await
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Helper function to create a simple streaming query
fn create_test_query() -> StreamingQuery {
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
        emit_mode: Some(EmitMode::Changes),
    }
}

#[tokio::test]
async fn test_job_processing_config_validation() {
    // Test default configuration
    let default_config = JobProcessingConfig::default();
    assert_eq!(default_config.max_batch_size, 100);
    assert_eq!(default_config.batch_timeout, Duration::from_millis(1000));
    assert!(!default_config.use_transactions);
    assert_eq!(
        default_config.failure_strategy,
        FailureStrategy::LogAndContinue
    );
    assert_eq!(default_config.max_retries, 3);
    assert_eq!(default_config.retry_backoff, Duration::from_millis(1000));
    assert!(default_config.log_progress);
    assert_eq!(default_config.progress_interval, 10);

    // Test custom configuration
    let custom_config = JobProcessingConfig {
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(2000),
        use_transactions: true,
        failure_strategy: FailureStrategy::FailBatch,
        max_retries: 5,
        retry_backoff: Duration::from_millis(500),
        log_progress: false,
        progress_interval: 20,
    };

    assert_eq!(custom_config.max_batch_size, 500);
    assert_eq!(custom_config.batch_timeout, Duration::from_millis(2000));
    assert!(custom_config.use_transactions);
    assert_eq!(custom_config.failure_strategy, FailureStrategy::FailBatch);
    assert_eq!(custom_config.max_retries, 5);
    assert_eq!(custom_config.retry_backoff, Duration::from_millis(500));
    assert!(!custom_config.log_progress);
    assert_eq!(custom_config.progress_interval, 20);
}

#[tokio::test]
async fn test_failure_strategy_validation() {
    // Test all failure strategy variants
    let strategies = vec![
        FailureStrategy::LogAndContinue,
        FailureStrategy::SendToDLQ,
        FailureStrategy::FailBatch,
        FailureStrategy::RetryWithBackoff,
    ];

    for strategy in strategies {
        let config = JobProcessingConfig {
            failure_strategy: strategy,
            ..Default::default()
        };

        // Should be able to create processors with any valid strategy
        let simple_processor = SimpleJobProcessor::new(config.clone());
        let transactional_processor = TransactionalJobProcessor::new(config);

        // Note: config fields are private, so we just check processor creation succeeds
        // The failure strategy should be set correctly internally
        drop(simple_processor);
        drop(transactional_processor);
    }
}

#[tokio::test]
async fn test_batch_processing_result_validation() {
    let mut result = BatchProcessingResult {
        records_processed: 10,
        records_failed: 5,
        processing_time: Duration::from_millis(100),
        batch_size: 15,
        error_details: vec![],
    };

    // Validate that the result makes sense
    assert_eq!(
        result.records_processed + result.records_failed,
        result.batch_size
    );
    assert!(result.processing_time > Duration::from_millis(0));

    // Test with error details
    result.error_details = vec![
        ProcessingError {
            record_index: 3,
            error_message: "Test error 1".to_string(),
            recoverable: true,
        },
        ProcessingError {
            record_index: 7,
            error_message: "Test error 2".to_string(),
            recoverable: false,
        },
    ];

    assert_eq!(result.error_details.len(), 2);
    assert_eq!(result.error_details[0].record_index, 3);
    assert!(result.error_details[0].recoverable);
    assert!(!result.error_details[1].recoverable);
}

#[tokio::test]
async fn test_job_execution_stats_validation() {
    let mut stats = JobExecutionStats::new();

    // Verify initial state
    assert_eq!(stats.records_processed, 0);
    assert_eq!(stats.records_failed, 0);
    assert_eq!(stats.batches_processed, 0);
    assert_eq!(stats.batches_failed, 0);
    assert!(stats.start_time.is_some());
    assert_eq!(stats.records_per_second(), 0.0);
    assert_eq!(stats.success_rate(), 0.0);

    // Test with successful batch
    let successful_result = BatchProcessingResult {
        records_processed: 100,
        records_failed: 0,
        processing_time: Duration::from_millis(200),
        batch_size: 100,
        error_details: vec![],
    };

    stats.update_from_batch(&successful_result);

    assert_eq!(stats.records_processed, 100);
    assert_eq!(stats.records_failed, 0);
    assert_eq!(stats.batches_processed, 1);
    assert_eq!(stats.batches_failed, 0);
    assert_eq!(stats.success_rate(), 100.0);

    // Test with failed batch
    let failed_result = BatchProcessingResult {
        records_processed: 50,
        records_failed: 25,
        processing_time: Duration::from_millis(150),
        batch_size: 75,
        error_details: vec![],
    };

    stats.update_from_batch(&failed_result);

    assert_eq!(stats.records_processed, 150);
    assert_eq!(stats.records_failed, 25);
    assert_eq!(stats.batches_processed, 1); // Still 1 successful batch
    assert_eq!(stats.batches_failed, 1); // Now 1 failed batch
    assert_eq!(stats.success_rate(), 150.0 / 175.0 * 100.0);
}

#[tokio::test]
async fn test_datasource_creation_error_handling() {
    // Test invalid datasource configuration
    let invalid_config = DataSourceConfig {
        requirement: ferrisstreams::ferris::sql::query_analyzer::DataSourceRequirement {
            name: "test_source".to_string(),
            source_type: ferrisstreams::ferris::sql::query_analyzer::DataSourceType::Generic(
                "unknown_type".to_string(),
            ),
            properties: HashMap::new(),
        },
        default_topic: "test_topic".to_string(),
        job_name: "test_job".to_string(),
    };

    let result = create_datasource_reader(&invalid_config).await;
    assert!(result.is_err());
    // Error is expected due to unsupported datasource type
}

#[tokio::test]
async fn test_process_datasource_records_with_transaction_fallback() {
    // Test that process_datasource_records correctly falls back to simple processing
    // when transactions are requested but not supported
    let reader =
        Box::new(FailingMockDataReader::new().without_transactions()) as Box<dyn DataReader>;
    let writer = Some(Box::new(FailingMockDataWriter::new()) as Box<dyn DataWriter>);
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true, // Request transactions
        ..Default::default()
    };

    let job_handle = tokio::spawn(async move {
        process_datasource_records(
            reader,
            writer,
            engine,
            query,
            "test_job".to_string(),
            shutdown_rx,
            config,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    // Should have successfully processed using simple mode
    let stats = result.unwrap();
    assert_eq!(stats.records_processed, 1);
}

#[tokio::test]
async fn test_retry_with_backoff_function() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();
    let start_time = Instant::now();

    let operation = move || {
        let count = attempt_count_clone.clone();
        Box::pin(async move {
            let current_attempt = count.fetch_add(1, Ordering::SeqCst) + 1;
            if current_attempt < 3 {
                Err("Simulated failure".into())
            } else {
                Ok("Success".to_string())
            }
        })
            as std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<String, Box<dyn std::error::Error + Send + Sync>>,
                        > + Send,
                >,
            >
    };

    let result = retry_with_backoff(
        operation,
        5,                         // max retries
        Duration::from_millis(10), // short backoff for testing
        "test_job",
        "test_operation",
    )
    .await;

    let elapsed = start_time.elapsed();

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Success");
    assert_eq!(attempt_count.load(Ordering::SeqCst), 3);

    // Should have spent time on backoff (at least 10ms + 20ms for exponential backoff)
    assert!(elapsed >= Duration::from_millis(25));
}

#[tokio::test]
async fn test_retry_with_backoff_exhausted() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();

    let operation = move || {
        let count = attempt_count_clone.clone();
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err("Persistent failure".into())
        })
            as std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<(), Box<dyn std::error::Error + Send + Sync>>,
                        > + Send,
                >,
            >
    };

    let result = retry_with_backoff(
        operation,
        3,                        // max retries
        Duration::from_millis(1), // very short backoff
        "test_job",
        "test_operation",
    )
    .await;

    assert!(result.is_err());
    assert_eq!(attempt_count.load(Ordering::SeqCst), 4); // Initial attempt + 3 retries
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Persistent failure"));
}

#[tokio::test]
async fn test_error_handling_in_simple_processor() {
    // Test various error conditions in simple processor
    let reader = Box::new(FailingMockDataReader::new().with_read_failure()) as Box<dyn DataReader>;
    let writer = Some(Box::new(FailingMockDataWriter::new()) as Box<dyn DataWriter>);
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let processor = create_simple_processor();

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

    // Let it fail a few times
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    // Should have failed batches due to read failures
    assert!(stats.batches_failed > 0);
}

#[tokio::test]
async fn test_error_handling_in_transactional_processor() {
    // Test various error conditions in transactional processor
    let reader =
        Box::new(FailingMockDataReader::new().with_begin_tx_failure()) as Box<dyn DataReader>;
    let writer = Some(Box::new(FailingMockDataWriter::new()) as Box<dyn DataWriter>);
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let processor = create_transactional_processor();

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

    // Let it fail a few times
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    // Should have failed batches due to transaction begin failures
    assert!(stats.batches_failed > 0);
}

#[tokio::test]
async fn test_transaction_support_checking() {
    let reader_with_tx = FailingMockDataReader::new();
    let reader_without_tx = FailingMockDataReader::new().without_transactions();
    let writer_with_tx = FailingMockDataWriter::new();
    let writer_without_tx = FailingMockDataWriter::new().without_transactions();

    // Test transaction support checking
    assert!(check_transaction_support(&reader_with_tx, "test_job"));
    assert!(!check_transaction_support(&reader_without_tx, "test_job"));
    assert!(check_writer_transaction_support(
        &writer_with_tx,
        "test_job"
    ));
    assert!(!check_writer_transaction_support(
        &writer_without_tx,
        "test_job"
    ));
}

#[tokio::test]
async fn test_batch_processing_with_output() {
    // Test the process_batch_with_output function
    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();

    let batch = vec![StreamRecord {
        fields: {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(1));
            fields.insert("name".to_string(), FieldValue::String("test".to_string()));
            fields
        },
        headers: HashMap::new(),
        timestamp: 0,
        offset: 0,
        partition: 0,
    }];

    let result = process_batch_with_output(batch, &engine, &query, "test_job").await;

    assert_eq!(result.records_processed, 1);
    assert_eq!(result.records_failed, 0);
    assert_eq!(result.batch_size, 1);
    assert!(result.processing_time > Duration::from_nanos(0));
    assert_eq!(result.output_records.len(), 1);
    assert!(result.error_details.is_empty());
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
    let simple_processor = SimpleJobProcessor::new(extreme_config.clone());
    let transactional_processor = TransactionalJobProcessor::new(extreme_config);

    // Verify configurations are preserved (Note: config fields are private, so we just check processor creation succeeds)
    // These processors should be created successfully with extreme edge case configurations
    drop(simple_processor);
    drop(transactional_processor);
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
