use ferrisstreams::ferris::{
    datasource::{types::SourceOffset, DataReader, DataWriter},
    sql::{
        ast::StreamingQuery,
        execution::types::{FieldValue, StreamRecord},
        multi_job_common::*,
        multi_job_transactional::*,
        StreamExecutionEngine,
    },
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex};

/// Mock DataReader with transaction support tracking
struct MockTransactionalDataReader {
    pub records: Vec<Vec<StreamRecord>>,
    pub current_batch: usize,
    pub should_fail_read: bool,
    pub should_fail_commit: bool,
    pub should_fail_begin: bool,
    pub supports_tx: bool,
    pub transaction_active: bool,
    pub begin_tx_calls: usize,
    pub commit_tx_calls: usize,
    pub abort_tx_calls: usize,
    pub commit_calls: usize,
    pub abort_after_sink_success: bool,
}

impl MockTransactionalDataReader {
    fn new(records: Vec<Vec<StreamRecord>>) -> Self {
        Self {
            records,
            current_batch: 0,
            should_fail_read: false,
            should_fail_commit: false,
            should_fail_begin: false,
            supports_tx: true,
            transaction_active: false,
            begin_tx_calls: 0,
            commit_tx_calls: 0,
            abort_tx_calls: 0,
            commit_calls: 0,
            abort_after_sink_success: false,
        }
    }

    fn with_read_failures(mut self) -> Self {
        self.should_fail_read = true;
        self
    }

    fn with_commit_failures(mut self) -> Self {
        self.should_fail_commit = true;
        self
    }

    fn with_begin_failures(mut self) -> Self {
        self.should_fail_begin = true;
        self
    }

    fn without_transaction_support(mut self) -> Self {
        self.supports_tx = false;
        self
    }

    fn with_abort_after_sink_success(mut self) -> Self {
        self.abort_after_sink_success = true;
        self
    }
}

#[async_trait::async_trait]
impl DataReader for MockTransactionalDataReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.should_fail_read && self.current_batch > 0 {
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
        if self.should_fail_commit {
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
        self.begin_tx_calls += 1;
        if self.should_fail_begin {
            return Err("Mock begin transaction failure".into());
        }
        if self.supports_tx {
            self.transaction_active = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.commit_tx_calls += 1;
        if !self.transaction_active {
            return Err("No active transaction".into());
        }
        if self.should_fail_commit || self.abort_after_sink_success {
            self.transaction_active = false;
            return Err("Mock commit transaction failure".into());
        }
        self.transaction_active = false;
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.abort_tx_calls += 1;
        if !self.transaction_active {
            return Err("No active transaction".into());
        }
        self.transaction_active = false;
        Ok(())
    }
}

/// Mock DataWriter with transaction support tracking
struct MockTransactionalDataWriter {
    pub written_records: Vec<StreamRecord>,
    pub should_fail_write: bool,
    pub should_fail_flush: bool,
    pub should_fail_commit: bool,
    pub should_fail_begin: bool,
    pub supports_tx: bool,
    pub transaction_active: bool,
    pub begin_tx_calls: usize,
    pub commit_tx_calls: usize,
    pub abort_tx_calls: usize,
    pub flush_calls: usize,
    pub write_calls: usize,
}

impl MockTransactionalDataWriter {
    fn new() -> Self {
        Self {
            written_records: Vec::new(),
            should_fail_write: false,
            should_fail_flush: false,
            should_fail_commit: false,
            should_fail_begin: false,
            supports_tx: true,
            transaction_active: false,
            begin_tx_calls: 0,
            commit_tx_calls: 0,
            abort_tx_calls: 0,
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

    fn with_commit_failures(mut self) -> Self {
        self.should_fail_commit = true;
        self
    }

    fn with_begin_failures(mut self) -> Self {
        self.should_fail_begin = true;
        self
    }

    fn without_transaction_support(mut self) -> Self {
        self.supports_tx = false;
        self
    }
}

#[async_trait::async_trait]
impl DataWriter for MockTransactionalDataWriter {
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
        self.begin_tx_calls += 1;
        if self.should_fail_begin {
            return Err("Mock begin transaction failure".into());
        }
        if self.supports_tx {
            self.transaction_active = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.commit_tx_calls += 1;
        if !self.transaction_active {
            return Err("No active transaction".into());
        }
        if self.should_fail_commit {
            self.transaction_active = false;
            return Err("Mock commit transaction failure".into());
        }
        self.transaction_active = false;
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.abort_tx_calls += 1;
        if !self.transaction_active {
            return Err("No active transaction".into());
        }
        self.transaction_active = false;
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // For non-transactional commit, just flush
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
async fn test_transactional_processor_creation() {
    let _processor = TransactionalJobProcessor::new(JobProcessingConfig::default());
    // Just test that we can create it without panicking
    assert!(true);
}

#[tokio::test]
async fn test_create_transactional_processor_presets() {
    let strict_config = JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::FailBatch,
        ..Default::default()
    };
    let best_effort_config = JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_retries: 1,
        ..Default::default()
    };

    let _strict_processor = TransactionalJobProcessor::new(strict_config.clone());
    let _best_effort_processor = TransactionalJobProcessor::new(best_effort_config.clone());

    // Verify different configurations
    assert!(strict_config.use_transactions);
    assert!(best_effort_config.use_transactions);

    // Test different failure strategies
    assert_eq!(strict_config.failure_strategy, FailureStrategy::FailBatch);
    assert_eq!(
        best_effort_config.failure_strategy,
        FailureStrategy::LogAndContinue
    );

    // Test retry configurations
    assert_eq!(best_effort_config.max_retries, 1);
}

#[tokio::test]
async fn test_successful_transactional_processing() {
    let test_data = vec![create_test_records(5)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(Box::new(MockTransactionalDataWriter::new()) as Box<dyn DataWriter>);

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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
async fn test_writer_commit_failure_aborts_reader() {
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(
        Box::new(MockTransactionalDataWriter::new().with_commit_failures()) as Box<dyn DataWriter>,
    );

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    // Writer commit failure should prevent any successful processing
    assert!(stats.batches_failed > 0);
}

#[tokio::test]
async fn test_reader_commit_failure_after_writer_success() {
    let test_data = vec![create_test_records(3)];
    let reader =
        Box::new(MockTransactionalDataReader::new(test_data).with_abort_after_sink_success())
            as Box<dyn DataReader>;
    let writer = Some(Box::new(MockTransactionalDataWriter::new()) as Box<dyn DataWriter>);

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    // Reader commit failure after writer success should fail the batch
    assert!(stats.batches_failed > 0);
}

#[tokio::test]
async fn test_write_batch_failure() {
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(
        Box::new(MockTransactionalDataWriter::new().with_write_failures()) as Box<dyn DataWriter>,
    );

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    // Write failures should cause batch failures
    assert!(stats.batches_failed > 0);
}

#[tokio::test]
async fn test_mixed_transaction_support() {
    // Reader supports transactions, writer doesn't
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(
        Box::new(MockTransactionalDataWriter::new().without_transaction_support())
            as Box<dyn DataWriter>,
    );

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.records_processed, 3);
    // Mixed transaction support should still work
    assert_eq!(stats.batches_processed, 1);
}

#[tokio::test]
async fn test_no_transaction_support() {
    // Neither reader nor writer support transactions
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data).without_transaction_support())
        as Box<dyn DataReader>;
    let writer = Some(
        Box::new(MockTransactionalDataWriter::new().without_transaction_support())
            as Box<dyn DataWriter>,
    );

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.records_processed, 3);
    // Should fall back to non-transactional processing
    assert_eq!(stats.batches_processed, 1);
}

#[tokio::test]
async fn test_begin_transaction_failures() {
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data).with_begin_failures())
        as Box<dyn DataReader>;
    let writer = Some(Box::new(MockTransactionalDataWriter::new()) as Box<dyn DataWriter>);

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    // Begin transaction failures should cause batch failures
    assert!(stats.batches_failed > 0);
}

#[tokio::test]
async fn test_empty_batch_handling() {
    let test_data = vec![vec![]]; // Empty batch
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(Box::new(MockTransactionalDataWriter::new()) as Box<dyn DataWriter>);

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    assert_eq!(stats.records_processed, 0);
    assert_eq!(stats.batches_processed, 0);
}

#[tokio::test]
async fn test_failure_strategy_log_and_continue() {
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(
        Box::new(MockTransactionalDataWriter::new().with_write_failures()) as Box<dyn DataWriter>,
    );

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::LogAndContinue,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    // With LogAndContinue, even write failures should result in attempted commits
    // But they will fail during the commit phase
    assert!(stats.batches_failed > 0);
}

#[tokio::test]
async fn test_processing_without_writer() {
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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
async fn test_non_transactional_writer_flush_failure() {
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(Box::new(
        MockTransactionalDataWriter::new()
            .without_transaction_support()
            .with_flush_failures(),
    ) as Box<dyn DataWriter>);

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    // Non-transactional writer flush failures should prevent commits
    assert!(stats.batches_failed > 0);
}

#[tokio::test]
async fn test_abort_transaction_cleanup() {
    let test_data = vec![create_test_records(3)];
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(
        Box::new(MockTransactionalDataWriter::new().with_commit_failures()) as Box<dyn DataWriter>,
    );

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);

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

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let stats = result.unwrap();
    // Transaction abort should result in failed batches
    assert!(stats.batches_failed > 0);
}

#[tokio::test]
async fn test_performance_with_transactions() {
    // Test that transactional processing doesn't have major performance issues
    let large_batch = create_test_records(100);
    let test_data = vec![large_batch];
    let reader = Box::new(MockTransactionalDataReader::new(test_data)) as Box<dyn DataReader>;
    let writer = Some(Box::new(MockTransactionalDataWriter::new()) as Box<dyn DataWriter>);

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let config = JobProcessingConfig {
        use_transactions: true,
        ..Default::default()
    };
    let processor = TransactionalJobProcessor::new(config);
    let start_time = Instant::now();

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

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = shutdown_tx.send(()).await;

    let result = job_handle.await.unwrap();
    assert!(result.is_ok());

    let elapsed = start_time.elapsed();
    let stats = result.unwrap();

    assert_eq!(stats.records_processed, 100);
    assert_eq!(stats.batches_processed, 1);

    // Should process 100 records reasonably quickly
    assert!(elapsed < Duration::from_secs(1));
}

#[tokio::test]
async fn test_transaction_state_management() {
    let test_data = vec![create_test_records(2)];
    let reader = MockTransactionalDataReader::new(test_data);
    let writer = MockTransactionalDataWriter::new();

    // Test transaction state tracking
    assert!(!reader.transaction_active);
    assert!(!writer.transaction_active);
    assert_eq!(reader.begin_tx_calls, 0);
    assert_eq!(writer.begin_tx_calls, 0);
}

#[tokio::test]
async fn test_transaction_call_tracking() {
    let test_data = vec![create_test_records(2)];
    let mut reader = MockTransactionalDataReader::new(test_data);
    let mut writer = MockTransactionalDataWriter::new();

    // Test begin transaction
    let reader_result = reader.begin_transaction().await;
    let writer_result = writer.begin_transaction().await;

    assert!(reader_result.is_ok());
    assert!(writer_result.is_ok());
    assert_eq!(reader.begin_tx_calls, 1);
    assert_eq!(writer.begin_tx_calls, 1);
    assert!(reader.transaction_active);
    assert!(writer.transaction_active);

    // Test commit transaction
    let reader_commit = reader.commit_transaction().await;
    let writer_commit = writer.commit_transaction().await;

    assert!(reader_commit.is_ok());
    assert!(writer_commit.is_ok());
    assert_eq!(reader.commit_tx_calls, 1);
    assert_eq!(writer.commit_tx_calls, 1);
    assert!(!reader.transaction_active);
    assert!(!writer.transaction_active);
}

#[tokio::test]
async fn test_transaction_error_scenarios() {
    let test_data = vec![create_test_records(1)];
    let mut reader = MockTransactionalDataReader::new(test_data).with_begin_failures();
    let mut writer = MockTransactionalDataWriter::new().with_begin_failures();

    // Test failed begin transaction
    let reader_result = reader.begin_transaction().await;
    let writer_result = writer.begin_transaction().await;

    assert!(reader_result.is_err());
    assert!(writer_result.is_err());
    assert!(!reader.transaction_active);
    assert!(!writer.transaction_active);
}
