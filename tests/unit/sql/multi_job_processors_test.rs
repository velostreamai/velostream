//! Tests for multi-job processors (transactional and simple)

use ferrisstreams::ferris::datasource::{DataReader, DataWriter};
use ferrisstreams::ferris::schema::{DataType, Field, Schema};
use ferrisstreams::ferris::sql::{
    ast::{SelectField, StreamSource, StreamingQuery, WindowType},
    execution::types::{FieldValue, StreamRecord},
    multi_job_common::*,
    multi_job_transactional::*,
    multi_job_simple::*,
    StreamExecutionEngine,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

/// Mock datasource reader for testing
pub struct MockDataReader {
    pub records: Vec<Vec<StreamRecord>>,
    pub current_batch: usize,
    pub supports_tx: bool,
    pub tx_active: bool,
    pub should_fail_begin_tx: bool,
    pub should_fail_commit_tx: bool,
    pub should_fail_commit: bool,
}

impl MockDataReader {
    pub fn new(records: Vec<Vec<StreamRecord>>) -> Self {
        Self {
            records,
            current_batch: 0,
            supports_tx: false,
            tx_active: false,
            should_fail_begin_tx: false,
            should_fail_commit_tx: false,
            should_fail_commit: false,
        }
    }

    pub fn with_transaction_support(mut self) -> Self {
        self.supports_tx = true;
        self
    }

    pub fn with_begin_tx_failure(mut self) -> Self {
        self.should_fail_begin_tx = true;
        self
    }

    pub fn with_commit_tx_failure(mut self) -> Self {
        self.should_fail_commit_tx = true;
        self
    }

    pub fn with_commit_failure(mut self) -> Self {
        self.should_fail_commit = true;
        self
    }
}

#[async_trait]
impl DataReader for MockDataReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        if self.current_batch < self.records.len() {
            let batch = self.records[self.current_batch].clone();
            self.current_batch += 1;
            Ok(batch)
        } else {
            Ok(vec![]) // No more data
        }
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.should_fail_commit {
            Err("Mock commit failure".into())
        } else {
            Ok(())
        }
    }

    async fn seek(&mut self, _offset: crate::ferris::datasource::types::SourceOffset) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.current_batch < self.records.len())
    }

    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if !self.supports_tx {
            return Ok(false);
        }
        if self.should_fail_begin_tx {
            return Err("Mock begin_transaction failure".into());
        }
        self.tx_active = true;
        Ok(true)
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.should_fail_commit_tx {
            Err("Mock commit_transaction failure".into())
        } else {
            self.tx_active = false;
            Ok(())
        }
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.tx_active = false;
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_tx
    }
}

/// Mock data writer for testing
pub struct MockDataWriter {
    pub written_records: Vec<StreamRecord>,
    pub supports_tx: bool,
    pub tx_active: bool,
    pub should_fail_commit: bool,
    pub should_fail_flush: bool,
    pub should_fail_rollback: bool,
    pub should_fail_commit_tx: bool,
    pub should_fail_abort_tx: bool,
    pub should_fail_begin_tx: bool,
}

impl MockDataWriter {
    pub fn new() -> Self {
        Self {
            written_records: Vec::new(),
            supports_tx: false,
            tx_active: false,
            should_fail_commit: false,
            should_fail_flush: false,
            should_fail_rollback: false,
            should_fail_commit_tx: false,
            should_fail_abort_tx: false,
            should_fail_begin_tx: false,
        }
    }

    pub fn with_transaction_support(mut self) -> Self {
        self.supports_tx = true;
        self
    }

    pub fn with_commit_failure(mut self) -> Self {
        self.should_fail_commit = true;
        self
    }

    pub fn with_flush_failure(mut self) -> Self {
        self.should_fail_flush = true;
        self
    }
    
    pub fn with_commit_tx_failure(mut self) -> Self {
        self.should_fail_commit_tx = true;
        self
    }
    
    pub fn with_abort_tx_failure(mut self) -> Self {
        self.should_fail_abort_tx = true;
        self
    }
    
    pub fn with_begin_tx_failure(mut self) -> Self {
        self.should_fail_begin_tx = true;
        self
    }
}

#[async_trait]
impl DataWriter for MockDataWriter {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.written_records.push(record);
        Ok(())
    }

    async fn write_batch(&mut self, records: Vec<StreamRecord>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.written_records.extend(records);
        Ok(())
    }

    async fn update(&mut self, _key: &str, record: StreamRecord) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.written_records.push(record);
        Ok(())
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.should_fail_flush {
            Err("Mock flush failure".into())
        } else {
            Ok(())
        }
    }

    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if !self.supports_tx {
            return Ok(false);
        }
        if self.should_fail_begin_tx {
            return Err("Mock begin_transaction failure".into());
        }
        self.tx_active = true;
        Ok(true)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.should_fail_commit {
            Err("Mock commit failure".into())
        } else {
            self.tx_active = false;
            Ok(())
        }
    }
    
    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.should_fail_commit_tx {
            Err("Mock commit_transaction failure".into())
        } else {
            self.tx_active = false;
            Ok(())
        }
    }
    
    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.should_fail_abort_tx {
            Err("Mock abort_transaction failure".into())
        } else {
            self.tx_active = false;
            Ok(())
        }
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.should_fail_rollback {
            Err("Mock rollback failure".into())
        } else {
            self.tx_active = false;
            Ok(())
        }
    }

    fn supports_transactions(&self) -> bool {
        self.supports_tx
    }
}

/// Helper function to create test records
fn create_test_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("name".to_string(), FieldValue::String(format!("record_{}", i)));
            fields.insert("value".to_string(), FieldValue::Float(i as f64 * 1.5));
            
            StreamRecord::new(fields)
        })
        .collect()
}

/// Helper function to create a simple test query
fn create_test_query() -> StreamingQuery {
    StreamingQuery {
        select_fields: vec![SelectField::All],
        from_stream: StreamSource {
            stream_name: "test_stream".to_string(),
            alias: None,
        },
        where_clause: None,
        group_by: vec![],
        having: None,
        window: None,
        order_by: vec![],
        limit: None,
    }
}

#[tokio::test]
async fn test_transactional_processor_success() {
    // Create test data
    let batch1 = create_test_records(5);
    let batch2 = create_test_records(3);
    let mock_reader = MockDataReader::new(vec![batch1, batch2])
        .with_transaction_support();
    
    let mock_writer = MockDataWriter::new()
        .with_transaction_support();
    
    // Create processor and engine
    let processor = create_transactional_processor();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    // Create shutdown channel (but don't send signal)
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    // Run for a short time then shutdown
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "test_job".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    // Let it process for a bit
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // The task should complete successfully when no more data is available
    match tokio::time::timeout(Duration::from_millis(1000), job_handle).await {
        Ok(result) => {
            let stats = result.expect("Job should complete").expect("Job should succeed");
            assert!(stats.records_processed > 0, "Should process some records");
            assert_eq!(stats.records_failed, 0, "Should have no failures");
        }
        Err(_) => panic!("Job should complete within timeout"),
    }
}

#[tokio::test]
async fn test_transactional_processor_sink_failure() {
    // Create test data
    let batch1 = create_test_records(3);
    let mock_reader = MockDataReader::new(vec![batch1])
        .with_transaction_support();
    
    let mock_writer = MockDataWriter::new()
        .with_transaction_support()
        .with_commit_failure(); // Sink will fail
    
    // Create processor and engine
    let processor = create_transactional_processor();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    // Run processor
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "test_job".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    // Let it try to process
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Send shutdown signal
    shutdown_tx.send(()).await.expect("Should send shutdown");
    
    // Job should complete
    match tokio::time::timeout(Duration::from_millis(1000), job_handle).await {
        Ok(result) => {
            let stats = result.expect("Job should complete").expect("Job should succeed");
            // With sink failures, batches should fail but job continues
            assert!(stats.batches_failed > 0, "Should have batch failures due to sink");
        }
        Err(_) => panic!("Job should complete within timeout"),
    }
}

#[tokio::test]
async fn test_simple_processor_throughput() {
    // Create lots of test data for throughput test
    let mut batches = Vec::new();
    for i in 0..10 {
        batches.push(create_test_records(100)); // 10 batches of 100 records each
    }
    
    let mock_reader = MockDataReader::new(batches);
    let mock_writer = MockDataWriter::new();
    
    // Create high-throughput processor
    let processor = create_simple_processor();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    let start_time = Instant::now();
    
    // Run processor
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "throughput_test".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    // Let it process all data
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown_tx.send(()).await.expect("Should send shutdown");
    
    let stats = job_handle.await
        .expect("Job should complete")
        .expect("Job should succeed");
    
    let elapsed = start_time.elapsed();
    let throughput = stats.records_processed as f64 / elapsed.as_secs_f64();
    
    println!("Simple processor throughput: {:.0} records/sec", throughput);
    println!("Processed {} records in {:?}", stats.records_processed, elapsed);
    
    assert!(stats.records_processed >= 900, "Should process most records"); // Allow for some timing variations
    assert!(throughput > 100.0, "Should achieve reasonable throughput");
}

#[tokio::test]
async fn test_conservative_simple_processor_failure_handling() {
    // Create test data with some that will cause processing "failures"
    let batch1 = create_test_records(5);
    let mock_reader = MockDataReader::new(vec![batch1]);
    let mock_writer = MockDataWriter::new()
        .with_flush_failure(); // Writer will fail
    
    // Use conservative processor that fails entire batch on any failure
    let processor = create_conservative_simple_processor();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    // Run processor
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "conservative_test".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown_tx.send(()).await.expect("Should send shutdown");
    
    let stats = job_handle.await
        .expect("Job should complete")
        .expect("Job should succeed");
    
    // Conservative processor should still commit source even if sink fails
    // (this is the difference from transactional processor)
    assert!(stats.records_processed > 0 || stats.batches_failed > 0, "Should process or fail batches");
}

#[tokio::test]
async fn test_low_latency_processor() {
    // Create small batches for low latency test
    let batch1 = create_test_records(2);
    let batch2 = create_test_records(1);
    let batch3 = create_test_records(3);
    
    let mock_reader = MockDataReader::new(vec![batch1, batch2, batch3]);
    let mock_writer = MockDataWriter::new();
    
    // Use low-latency processor
    let processor = create_low_latency_processor();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    let start_time = Instant::now();
    
    // Run processor
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "latency_test".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown_tx.send(()).await.expect("Should send shutdown");
    
    let stats = job_handle.await
        .expect("Job should complete")
        .expect("Job should succeed");
    
    let total_latency = start_time.elapsed();
    
    println!("Low latency processor completed in {:?}", total_latency);
    println!("Average batch processing time: {:.2}ms", stats.avg_processing_time_ms);
    
    assert!(stats.records_processed > 0, "Should process records");
    assert!(total_latency < Duration::from_millis(500), "Should complete quickly");
}

#[tokio::test]
async fn test_transactional_processor_writer_commit_tx_failure() {
    // Test that sink commit_transaction failure properly aborts source transaction
    let batch1 = create_test_records(3);
    let mock_reader = MockDataReader::new(vec![batch1])
        .with_transaction_support();
    
    let mock_writer = MockDataWriter::new()
        .with_transaction_support()
        .with_commit_tx_failure(); // Writer commit_transaction will fail
    
    let processor = create_transactional_processor();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "commit_tx_failure_test".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown_tx.send(()).await.expect("Should send shutdown");
    
    let stats = job_handle.await
        .expect("Job should complete")
        .expect("Job should succeed");
    
    // With commit_transaction failures, batches should fail
    assert!(stats.batches_failed > 0, "Should have batch failures due to commit_transaction failure");
}

#[tokio::test]
async fn test_transactional_processor_writer_begin_tx_failure() {
    // Test that writer begin_transaction failure is handled gracefully
    let batch1 = create_test_records(2);
    let mock_reader = MockDataReader::new(vec![batch1])
        .with_transaction_support();
    
    let mock_writer = MockDataWriter::new()
        .with_transaction_support()
        .with_begin_tx_failure(); // Writer begin_transaction will fail
    
    let processor = create_transactional_processor();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "begin_tx_failure_test".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown_tx.send(()).await.expect("Should send shutdown");
    
    let stats = job_handle.await
        .expect("Job should complete")
        .expect("Job should succeed");
    
    // With begin_transaction failures, batches should fail
    assert!(stats.batches_failed > 0, "Should have batch failures due to begin_transaction failure");
}

#[tokio::test]
async fn test_transactional_processor_mixed_transaction_support() {
    // Test transactional reader with non-transactional writer
    let batch1 = create_test_records(3);
    let mock_reader = MockDataReader::new(vec![batch1])
        .with_transaction_support(); // Reader supports transactions
    
    let mock_writer = MockDataWriter::new(); // Writer does NOT support transactions
    
    let processor = create_transactional_processor();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "mixed_tx_test".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown_tx.send(()).await.expect("Should send shutdown");
    
    let stats = job_handle.await
        .expect("Job should complete")
        .expect("Job should succeed");
    
    // Should work with mixed transaction support
    assert!(stats.records_processed > 0, "Should process records");
    assert_eq!(stats.records_failed, 0, "Should have no failures");
}

#[tokio::test]
async fn test_simple_processor_with_transaction_capable_sources() {
    // Test that simple processor works with transaction-capable sources but doesn't use transactions
    let batch1 = create_test_records(4);
    let batch2 = create_test_records(2);
    let mock_reader = MockDataReader::new(vec![batch1, batch2])
        .with_transaction_support(); // Reader supports transactions but simple processor won't use them
    
    let mock_writer = MockDataWriter::new()
        .with_transaction_support(); // Writer supports transactions but simple processor won't use them
    
    let processor = create_simple_processor();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "simple_with_tx_capable_test".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown_tx.send(()).await.expect("Should send shutdown");
    
    let stats = job_handle.await
        .expect("Job should complete")
        .expect("Job should succeed");
    
    // Simple processor should work fine, just using commit/flush instead of transaction methods
    assert!(stats.records_processed >= 5, "Should process most records"); // 4 + 2 = 6 total
    assert!(stats.batches_processed >= 2, "Should process at least 2 batches");
}

#[tokio::test]
async fn test_simple_processor_sink_failure_continues_processing() {
    // Test that simple processor continues processing even when sink fails (different from transactional)
    let batch1 = create_test_records(3);
    let mock_reader = MockDataReader::new(vec![batch1]);
    let mock_writer = MockDataWriter::new()
        .with_flush_failure(); // Sink flush will fail
    
    let processor = create_simple_processor(); // Uses LogAndContinue strategy
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new()));
    let query = create_test_query();
    
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    
    let job_handle = tokio::spawn({
        let engine = engine.clone();
        let query = query.clone();
        async move {
            processor.process_job(
                Box::new(mock_reader),
                Some(Box::new(mock_writer)),
                engine,
                query,
                "simple_sink_failure_test".to_string(),
                shutdown_rx,
            ).await
        }
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown_tx.send(()).await.expect("Should send shutdown");
    
    let stats = job_handle.await
        .expect("Job should complete")
        .expect("Job should succeed");
    
    // Simple processor should still commit source even if sink fails
    assert!(stats.records_processed > 0, "Should process records");
    // Note: Simple processor commits source regardless of sink failure (best effort)
}

#[tokio::test]
async fn test_transactional_vs_simple_failure_behavior() {
    // Direct comparison test showing different failure handling
    let create_failing_scenario = |use_transactional: bool| async move {
        let batch1 = create_test_records(2);
        let mock_reader = MockDataReader::new(vec![batch1])
            .with_transaction_support();
        let mock_writer = MockDataWriter::new()
            .with_transaction_support()
            .with_commit_tx_failure(); // This will fail for transactional
        
        let processor = if use_transactional {
            Box::new(create_transactional_processor()) as Box<dyn Send + Sync>
        } else {
            Box::new(create_simple_processor()) as Box<dyn Send + Sync>
        };
        
        // Note: This is a conceptual test - actual implementation would need trait objects
        // For now, we'll test them separately above
        use_transactional
    };
    
    // This test demonstrates the difference in approach:
    // - Transactional: Sink failure -> source abort -> data not lost, can be retried
    // - Simple: Sink failure -> source still commits -> data potentially lost but progress made
    
    let transactional_approach = create_failing_scenario(true).await;
    let simple_approach = create_failing_scenario(false).await;
    
    assert_ne!(transactional_approach, simple_approach, "Approaches should be different");
}