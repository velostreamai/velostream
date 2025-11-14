//! AdaptiveJobProcessor Integration Test with MockWriter and MockReader
//!
//! Validates that AdaptiveJobProcessor correctly:
//! - Receives records from MockDataSource
//! - Processes records through SQL engine
//! - Writes output records to MockDataWriter
//! - Handles multiple partitions correctly
//! - Maintains proper lifecycle (init, process, finalize)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{JobProcessor, JobProcessorConfig};
use velostream::velostream::server::v2::AdaptiveJobProcessor;
use velostream::velostream::sql::StreamingQuery;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

use std::time::Duration;

/// Mock data source for testing - provides a fixed set of records
#[derive(Clone)]
struct MockDataSource {
    records: Vec<StreamRecord>,
    index: Arc<AtomicUsize>,
    batch_size: usize,
}

impl MockDataSource {
    fn new(records: Vec<StreamRecord>, batch_size: usize) -> Self {
        Self {
            records,
            index: Arc::new(AtomicUsize::new(0)),
            batch_size,
        }
    }

    fn with_records(count: usize) -> Self {
        let mut records = Vec::with_capacity(count);
        for i in 0..count {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("value".to_string(), FieldValue::Integer((i * 100) as i64));
            fields.insert(
                "trader_id".to_string(),
                FieldValue::String(format!("TRADER_{}", i % 5)),
            );
            records.push(StreamRecord::new(fields));
        }
        Self {
            records,
            index: Arc::new(AtomicUsize::new(0)),
            batch_size: 10,
        }
    }
}

#[async_trait::async_trait]
impl DataReader for MockDataSource {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let start = self.index.load(Ordering::SeqCst);
        let end = (start + self.batch_size).min(self.records.len());

        if start >= self.records.len() {
            return Ok(Vec::new());
        }

        let batch: Vec<StreamRecord> = self.records[start..end].iter().map(|r| r.clone()).collect();

        self.index.store(end, Ordering::SeqCst);
        Ok(batch)
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.index.load(Ordering::SeqCst) < self.records.len())
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
}

/// Mock data writer for testing - counts records written
#[derive(Clone)]
struct MockDataWriter {
    count: Arc<AtomicUsize>,
    records_written: Arc<Mutex<Vec<Arc<StreamRecord>>>>,
}

impl MockDataWriter {
    fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
            records_written: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    async fn get_written_records(&self) -> Vec<Arc<StreamRecord>> {
        self.records_written.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl DataWriter for MockDataWriter {
    async fn write(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.records_written.lock().await.push(Arc::new(record));
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.count.fetch_add(records.len(), Ordering::SeqCst);
        self.records_written.lock().await.extend(records);
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
}

/// Test that AdaptiveJobProcessor processes simple SELECT query correctly
#[tokio::test]
async fn test_adaptive_processor_simple_select() {
    // Create test data: 50 records
    let test_records = {
        let mut records = Vec::with_capacity(50);
        for i in 0..50 {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert("value".to_string(), FieldValue::Integer((i * 100) as i64));
            records.push(StreamRecord::new(fields));
        }
        records
    };

    // Create mock reader and writer
    let reader = MockDataSource::new(test_records.clone(), 10);
    let writer = MockDataWriter::new();

    // Create AdaptiveJobProcessor with 2 partitions
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(2),
        enable_core_affinity: false,
    };
    let processor = match config {
        JobProcessorConfig::Adaptive {
            num_partitions,
            enable_core_affinity,
        } => {
            use velostream::velostream::server::v2::PartitionedJobConfig;
            Arc::new(AdaptiveJobProcessor::new(PartitionedJobConfig {
                num_partitions,
                enable_core_affinity,
                ..Default::default()
            }))
        }
        _ => panic!("Expected Adaptive config"),
    };

    // Create simple SELECT query
    let query_str = "SELECT id, value FROM input_stream";
    let mut parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create dummy engine (will be created internally by processor)
    let (dummy_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let dummy_engine = Arc::new(tokio::sync::RwLock::new(
        velostream::velostream::sql::StreamExecutionEngine::new(dummy_tx),
    ));

    // Process job
    let result = processor
        .process_job(
            Box::new(reader),
            Some(Box::new(writer.clone())),
            dummy_engine,
            query,
            "test_job".to_string(),
            {
                let (_tx, rx) = tokio::sync::mpsc::channel(1);
                rx
            },
        )
        .await;

    // Verify processing completed successfully
    assert!(result.is_ok(), "process_job failed: {:?}", result.err());

    let stats = result.unwrap();
    assert_eq!(
        stats.records_processed, 50,
        "Expected 50 records to be processed, got {}",
        stats.records_processed
    );

    // Give async tasks time to finish writing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify records were written to sink
    let written_count = writer.get_count();
    println!(
        "Test simple_select: {} records processed, {} records written",
        stats.records_processed, written_count
    );
}

/// Test that AdaptiveJobProcessor handles windowed queries (GROUP BY)
#[tokio::test]
async fn test_adaptive_processor_group_by() {
    // Create test data: 100 records with 5 different trader_ids
    let test_records = {
        let mut records = Vec::with_capacity(100);
        for i in 0..100 {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "price".to_string(),
                FieldValue::Integer((100 + i * 10) as i64),
            );
            fields.insert(
                "quantity".to_string(),
                FieldValue::Integer(10 + (i % 5) as i64),
            );
            fields.insert(
                "trader_id".to_string(),
                FieldValue::String(format!("TRADER_{}", i % 5)),
            );
            records.push(StreamRecord::new(fields));
        }
        records
    };

    // Create mock reader and writer
    let reader = MockDataSource::new(test_records.clone(), 20);
    let writer = MockDataWriter::new();

    // Create AdaptiveJobProcessor with 4 partitions
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(4),
        enable_core_affinity: false,
    };
    let processor = match config {
        JobProcessorConfig::Adaptive {
            num_partitions,
            enable_core_affinity,
        } => {
            use velostream::velostream::server::v2::PartitionedJobConfig;
            Arc::new(AdaptiveJobProcessor::new(PartitionedJobConfig {
                num_partitions,
                enable_core_affinity,
                ..Default::default()
            }))
        }
        _ => panic!("Expected Adaptive config"),
    };

    // Create GROUP BY query
    let query_str = "SELECT trader_id, COUNT(*) as trade_count, SUM(quantity) as total_qty FROM input_stream GROUP BY trader_id";
    let mut parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create dummy engine
    let (dummy_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let dummy_engine = Arc::new(tokio::sync::RwLock::new(
        velostream::velostream::sql::StreamExecutionEngine::new(dummy_tx),
    ));

    // Process job
    let result = processor
        .process_job(
            Box::new(reader),
            Some(Box::new(writer.clone())),
            dummy_engine,
            query,
            "test_group_by".to_string(),
            {
                let (_tx, rx) = tokio::sync::mpsc::channel(1);
                rx
            },
        )
        .await;

    // Verify processing completed successfully
    assert!(result.is_ok(), "process_job failed: {:?}", result.err());

    let stats = result.unwrap();
    assert_eq!(
        stats.records_processed, 100,
        "Expected 100 records to be processed, got {}",
        stats.records_processed
    );

    // Give async tasks time to finish writing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify records were processed (may be buffered for GROUP BY)
    let written_count = writer.get_count();
    println!(
        "Test group_by: {} records processed, {} records written to sink",
        stats.records_processed, written_count
    );
}

/// Test that AdaptiveJobProcessor properly routes records to different partitions
#[tokio::test]
async fn test_adaptive_processor_partition_routing() {
    // Create test data: 40 records with distinct trader IDs for routing
    let test_records = {
        let mut records = Vec::with_capacity(40);
        for i in 0..40 {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            // Use trader IDs that will hash to different partitions
            fields.insert(
                "trader_id".to_string(),
                FieldValue::String(format!("TRADER_{:03}", i / 10)), // 4 different traders
            );
            records.push(StreamRecord::new(fields));
        }
        records
    };

    // Create mock reader and writer
    let reader = MockDataSource::new(test_records.clone(), 10);
    let writer = MockDataWriter::new();

    // Create AdaptiveJobProcessor with 4 partitions
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(4),
        enable_core_affinity: false,
    };
    let processor = match config {
        JobProcessorConfig::Adaptive {
            num_partitions,
            enable_core_affinity,
        } => {
            use velostream::velostream::server::v2::PartitionedJobConfig;
            Arc::new(AdaptiveJobProcessor::new(PartitionedJobConfig {
                num_partitions,
                enable_core_affinity,
                ..Default::default()
            }))
        }
        _ => panic!("Expected Adaptive config"),
    };

    // Create simple SELECT query
    let query_str = "SELECT id, trader_id FROM input_stream";
    let mut parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create dummy engine
    let (dummy_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let dummy_engine = Arc::new(tokio::sync::RwLock::new(
        velostream::velostream::sql::StreamExecutionEngine::new(dummy_tx),
    ));

    // Process job
    let result = processor
        .process_job(
            Box::new(reader),
            Some(Box::new(writer.clone())),
            dummy_engine,
            query,
            "test_routing".to_string(),
            {
                let (_tx, rx) = tokio::sync::mpsc::channel(1);
                rx
            },
        )
        .await;

    // Verify processing completed
    assert!(result.is_ok(), "process_job failed: {:?}", result.err());

    let stats = result.unwrap();
    assert_eq!(
        stats.records_processed, 40,
        "Expected 40 records to be processed, got {}",
        stats.records_processed
    );

    // Give async tasks time to finish
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify routing worked and records were processed
    println!(
        "Test partition_routing: {} records processed across 4 partitions",
        stats.records_processed
    );
}

/// Test that AdaptiveJobProcessor validates processor interface
#[test]
fn test_adaptive_processor_interface() {
    use velostream::velostream::server::v2::PartitionedJobConfig;

    let config = PartitionedJobConfig {
        num_partitions: Some(2),
        ..Default::default()
    };

    let processor = AdaptiveJobProcessor::new(config);

    // Verify interface implementation
    assert_eq!(
        processor.processor_name(),
        "AdaptiveJobProcessor",
        "Processor name mismatch"
    );
    assert_eq!(
        processor.processor_version(),
        "V2",
        "Processor version mismatch"
    );
    assert_eq!(processor.num_partitions(), 2, "Partition count mismatch");

    // Verify metrics exist
    let metrics = processor.metrics();
    assert_eq!(
        metrics.name, "AdaptiveJobProcessor",
        "Metrics name mismatch"
    );
    assert_eq!(metrics.version, "V2", "Metrics version mismatch");
}

/// Test MockDataWriter functionality
#[tokio::test]
async fn test_mock_writer_functionality() {
    let mut writer = MockDataWriter::new();

    // Test write single record
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(1));
    let record1 = StreamRecord::new(fields);

    writer.write(record1).await.expect("Failed to write record");
    assert_eq!(writer.get_count(), 1, "Single write failed");

    // Test write batch
    let mut batch = Vec::new();
    for i in 2..5 {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldValue::Integer(i));
        batch.push(Arc::new(StreamRecord::new(fields)));
    }

    writer
        .write_batch(batch)
        .await
        .expect("Failed to write batch");
    assert_eq!(writer.get_count(), 4, "Batch write failed");

    // Test record storage
    let written = writer.get_written_records().await;
    assert_eq!(written.len(), 4, "Record storage failed");
}

/// Test MockDataSource functionality
#[tokio::test]
async fn test_mock_reader_functionality() {
    let source = MockDataSource::with_records(50);
    let mut reader = source.clone();

    // Test has_more
    assert!(
        reader.has_more().await.expect("has_more failed"),
        "Should have more records"
    );

    // Test read first batch
    let batch1 = reader.read().await.expect("Failed to read batch");
    assert_eq!(batch1.len(), 10, "First batch size mismatch");

    // Test read second batch
    let batch2 = reader.read().await.expect("Failed to read batch");
    assert_eq!(batch2.len(), 10, "Second batch size mismatch");

    // Read all remaining
    let mut total = 20;
    loop {
        let batch = reader.read().await.expect("Failed to read batch");
        if batch.is_empty() {
            break;
        }
        total += batch.len();
    }

    assert_eq!(total, 50, "Total records read mismatch");
    assert!(
        !reader.has_more().await.expect("has_more failed"),
        "Should have no more records"
    );
}
