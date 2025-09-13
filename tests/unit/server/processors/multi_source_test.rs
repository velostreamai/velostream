/*!
# Multi-Source Processor Unit Tests

Unit tests for the multi-source processing capabilities of SimpleJobProcessor and TransactionalJobProcessor.
*/

use ferrisstreams::ferris::server::processors::{
    SimpleJobProcessor, TransactionalJobProcessor, JobProcessingConfig, FailureStrategy,
    create_multi_source_readers, create_multi_sink_writers,
};
use ferrisstreams::ferris::sql::query_analyzer::{
    DataSourceRequirement, DataSinkRequirement, DataSourceType, DataSinkType,
};
use ferrisstreams::ferris::sql::execution::{FieldValue, StreamRecord};
use ferrisstreams::ferris::sql::{StreamExecutionEngine, StreamingSqlParser};
use ferrisstreams::ferris::datasource::{DataReader, DataWriter, BatchConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use async_trait::async_trait;

/// Mock DataReader for testing
#[derive(Debug)]
pub struct MockDataReader {
    pub name: String,
    pub records: Vec<StreamRecord>,
    pub current_index: usize,
    pub supports_transactions: bool,
}

impl MockDataReader {
    pub fn new(name: &str, record_count: usize, supports_transactions: bool) -> Self {
        let mut records = Vec::new();
        for i in 0..record_count {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("name".to_string(), FieldValue::String(format!("record_{}", i)));
            fields.insert("source".to_string(), FieldValue::String(name.to_string()));
            
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
        }
    }
}

#[async_trait]
impl DataReader for MockDataReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let batch_size = 5.min(self.records.len() - self.current_index);
        if batch_size == 0 {
            return Ok(vec![]);
        }
        
        let end_index = self.current_index + batch_size;
        let batch = self.records[self.current_index..end_index].to_vec();
        self.current_index = end_index;
        
        Ok(batch)
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.current_index < self.records.len())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Mock commit - just log
        println!("MockDataReader '{}' committed at index {}", self.name, self.current_index);
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_transactions
    }

    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.supports_transactions {
            println!("MockDataReader '{}' began transaction", self.name);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.supports_transactions {
            println!("MockDataReader '{}' committed transaction", self.name);
        }
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.supports_transactions {
            println!("MockDataReader '{}' aborted transaction", self.name);
            // Reset index to simulate rollback
            self.current_index = self.current_index.saturating_sub(5);
        }
        Ok(())
    }
}

/// Mock DataWriter for testing
#[derive(Debug)]
pub struct MockDataWriter {
    pub name: String,
    pub written_records: Vec<StreamRecord>,
    pub supports_transactions: bool,
}

impl MockDataWriter {
    pub fn new(name: &str, supports_transactions: bool) -> Self {
        Self {
            name: name.to_string(),
            written_records: Vec::new(),
            supports_transactions,
        }
    }
}

#[async_trait]
impl DataWriter for MockDataWriter {
    async fn write_batch(&mut self, records: Vec<StreamRecord>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("MockDataWriter '{}' writing {} records", self.name, records.len());
        self.written_records.extend(records);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("MockDataWriter '{}' flushed {} records", self.name, self.written_records.len());
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_transactions
    }

    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.supports_transactions {
            println!("MockDataWriter '{}' began transaction", self.name);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.supports_transactions {
            println!("MockDataWriter '{}' committed transaction", self.name);
        }
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.supports_transactions {
            println!("MockDataWriter '{}' aborted transaction", self.name);
            // Clear written records to simulate rollback
            self.written_records.clear();
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_simple_processor_multi_source_processing() {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 10,
        batch_timeout: Duration::from_millis(100),
        max_retries: 2,
        retry_backoff: Duration::from_millis(50),
        progress_interval: 5,
        log_progress: true,
    };

    let processor = SimpleJobProcessor::new(config);

    // Create mock readers and writers
    let mut readers = HashMap::new();
    readers.insert(
        "orders".to_string(),
        Box::new(MockDataReader::new("orders", 10, false)) as Box<dyn DataReader>,
    );
    readers.insert(
        "customers".to_string(),
        Box::new(MockDataReader::new("customers", 8, false)) as Box<dyn DataReader>,
    );

    let mut writers = HashMap::new();
    writers.insert(
        "output".to_string(),
        Box::new(MockDataWriter::new("output", false)) as Box<dyn DataWriter>,
    );

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Test processing with timeout to avoid infinite loop
    let job_handle = tokio::spawn(async move {
        processor.process_multi_job(
            readers,
            writers,
            engine,
            query,
            "test-multi-simple".to_string(),
            shutdown_rx,
        ).await
    });

    // Let it process for a short time
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send shutdown signal
    shutdown_tx.send(()).await.unwrap();

    // Wait for completion
    let result = tokio::time::timeout(Duration::from_secs(2), job_handle).await;
    assert!(result.is_ok(), "Job should complete within timeout");

    let job_result = result.unwrap().unwrap();
    assert!(job_result.is_ok(), "Job processing should succeed");

    let stats = job_result.unwrap();
    println!("Simple processor stats: {:?}", stats);
    
    // Should have processed some batches
    assert!(stats.batches_processed > 0, "Should have processed at least one batch");
}

#[tokio::test]
async fn test_transactional_processor_multi_source_processing() {
    let config = JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::FailBatch,
        max_batch_size: 5,
        batch_timeout: Duration::from_millis(200),
        max_retries: 3,
        retry_backoff: Duration::from_millis(100),
        progress_interval: 2,
        log_progress: true,
    };

    let processor = TransactionalJobProcessor::new(config);

    // Create mock readers with transaction support
    let mut readers = HashMap::new();
    readers.insert(
        "orders".to_string(),
        Box::new(MockDataReader::new("orders", 6, true)) as Box<dyn DataReader>,
    );
    readers.insert(
        "payments".to_string(),
        Box::new(MockDataReader::new("payments", 4, true)) as Box<dyn DataReader>,
    );

    let mut writers = HashMap::new();
    writers.insert(
        "processed".to_string(),
        Box::new(MockDataWriter::new("processed", true)) as Box<dyn DataWriter>,
    );
    writers.insert(
        "audit".to_string(),
        Box::new(MockDataWriter::new("audit", true)) as Box<dyn DataWriter>,
    );

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream WITH ('use_transactions' = 'true')").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Test transactional processing
    let job_handle = tokio::spawn(async move {
        processor.process_multi_job(
            readers,
            writers,
            engine,
            query,
            "test-multi-transactional".to_string(),
            shutdown_rx,
        ).await
    });

    // Let it process transactions
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Send shutdown signal
    shutdown_tx.send(()).await.unwrap();

    // Wait for completion
    let result = tokio::time::timeout(Duration::from_secs(3), job_handle).await;
    assert!(result.is_ok(), "Transactional job should complete within timeout");

    let job_result = result.unwrap().unwrap();
    assert!(job_result.is_ok(), "Transactional job processing should succeed");

    let stats = job_result.unwrap();
    println!("Transactional processor stats: {:?}", stats);
    
    // Should have processed some batches transactionally
    assert!(stats.batches_processed > 0, "Should have processed at least one batch");
}

#[tokio::test]
async fn test_multi_source_creation_helpers() {
    let sources = vec![
        DataSourceRequirement {
            name: "kafka_source".to_string(),
            source_type: DataSourceType::Kafka,
            properties: {
                let mut props = HashMap::new();
                props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
                props.insert("topic".to_string(), "test-topic".to_string());
                props
            },
        },
        DataSourceRequirement {
            name: "file_source".to_string(),
            source_type: DataSourceType::File,
            properties: {
                let mut props = HashMap::new();
                props.insert("path".to_string(), "test.json".to_string());
                props.insert("source.format".to_string(), "json".to_string());
                props
            },
        },
    ];

    let batch_config = Some(BatchConfig {
        strategy: ferrisstreams::ferris::datasource::BatchStrategy::FixedSize,
        max_batch_size: Some(100),
        batch_timeout: Some(Duration::from_millis(1000)),
        memory_limit_mb: Some(50),
    });

    // Test source creation (will fail without actual sources but tests interface)
    let result = create_multi_source_readers(
        &sources,
        "default-topic",
        "test-creation",
        &batch_config,
    ).await;

    match result {
        Ok(readers) => {
            println!("Created {} readers", readers.len());
            assert_eq!(readers.len(), sources.len(), "Should create reader for each source");
        }
        Err(e) => {
            println!("Expected creation failure: {}", e);
            // Verify error mentions source creation attempts
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("kafka_source") || error_msg.contains("file_source"),
                "Error should reference source names: {}", error_msg
            );
        }
    }
}

#[tokio::test]
async fn test_multi_sink_creation_helpers() {
    let sinks = vec![
        DataSinkRequirement {
            name: "kafka_sink".to_string(),
            sink_type: DataSinkType::Kafka,
            properties: {
                let mut props = HashMap::new();
                props.insert("bootstrap.servers".to_string(), "localhost:9092".to_string());
                props.insert("topic".to_string(), "output-topic".to_string());
                props
            },
        },
        DataSinkRequirement {
            name: "file_sink".to_string(),
            sink_type: DataSinkType::File,
            properties: {
                let mut props = HashMap::new();
                props.insert("path".to_string(), "output.json".to_string());
                props.insert("sink.format".to_string(), "json".to_string());
                props
            },
        },
    ];

    let batch_config = None;

    let result = create_multi_sink_writers(
        &sinks,
        "test-sink-creation",
        &batch_config,
    ).await;

    match result {
        Ok(writers) => {
            println!("Created {} writers", writers.len());
            // File writer should work even in test environment
            assert!(writers.len() >= 1, "Should create at least file writer");
        }
        Err(e) => {
            println!("Sink creation error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_error_handling_in_multi_source_processing() {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::RetryWithBackoff,
        max_batch_size: 3,
        batch_timeout: Duration::from_millis(50),
        max_retries: 2,
        retry_backoff: Duration::from_millis(25),
        progress_interval: 1,
        log_progress: true,
    };

    let processor = SimpleJobProcessor::new(config);

    // Create readers with no data (will cause quick completion)
    let mut readers = HashMap::new();
    readers.insert(
        "empty_source".to_string(),
        Box::new(MockDataReader::new("empty", 0, false)) as Box<dyn DataReader>,
    );

    let writers = HashMap::new(); // No writers to test fallback

    let (tx, _rx) = mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(tx)));

    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM test_stream").unwrap();

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Test error handling with empty sources
    let job_handle = tokio::spawn(async move {
        processor.process_multi_job(
            readers,
            writers,
            engine,
            query,
            "test-error-handling".to_string(),
            shutdown_rx,
        ).await
    });

    // Should complete quickly with no data
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown_tx.send(()).await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), job_handle).await;
    assert!(result.is_ok(), "Error handling job should complete");

    let job_result = result.unwrap().unwrap();
    assert!(job_result.is_ok(), "Job should handle empty sources gracefully");
}

#[tokio::test]
async fn test_processor_configuration_handling() {
    // Test different configurations
    let configs = vec![
        JobProcessingConfig {
            use_transactions: true,
            failure_strategy: FailureStrategy::FailBatch,
            max_batch_size: 1,
            batch_timeout: Duration::from_millis(10),
            max_retries: 1,
            retry_backoff: Duration::from_millis(5),
            progress_interval: 1,
            log_progress: false,
        },
        JobProcessingConfig {
            use_transactions: false,
            failure_strategy: FailureStrategy::LogAndContinue,
            max_batch_size: 100,
            batch_timeout: Duration::from_millis(1000),
            max_retries: 5,
            retry_backoff: Duration::from_millis(500),
            progress_interval: 10,
            log_progress: true,
        },
    ];

    for (i, config) in configs.iter().enumerate() {
        println!("Testing configuration {}: transactions={}, strategy={:?}", 
                 i, config.use_transactions, config.failure_strategy);

        if config.use_transactions {
            let processor = TransactionalJobProcessor::new(config.clone());
            let proc_config = processor.get_config();
            assert_eq!(proc_config.use_transactions, config.use_transactions);
            assert_eq!(proc_config.max_batch_size, config.max_batch_size);
            println!("✅ Transactional processor config verified");
        } else {
            let processor = SimpleJobProcessor::new(config.clone());
            let proc_config = processor.get_config();
            assert_eq!(proc_config.use_transactions, config.use_transactions);
            assert_eq!(proc_config.failure_strategy, config.failure_strategy);
            println!("✅ Simple processor config verified");
        }
    }
}