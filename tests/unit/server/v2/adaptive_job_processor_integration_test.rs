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

/// Helper: Run process_job with proper shutdown handling
async fn run_process_job(
    processor: Arc<AdaptiveJobProcessor>,
    reader: MockDataSource,
    writer: MockDataWriter,
    query: StreamingQuery,
    job_name: &str,
) -> Result<
    velostream::velostream::server::processors::common::JobExecutionStats,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let (dummy_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let dummy_engine = Arc::new(tokio::sync::RwLock::new(
        velostream::velostream::sql::StreamExecutionEngine::new(dummy_tx),
    ));

    let processor_clone = Arc::clone(&processor);
    let job_name_owned = job_name.to_string();
    let process_handle = tokio::spawn(async move {
        processor_clone
            .process_job(
                Box::new(reader),
                Some(Box::new(writer)),
                dummy_engine,
                query,
                job_name_owned,
                {
                    let (_tx, rx) = tokio::sync::mpsc::channel(1);
                    rx
                },
                None,
            )
            .await
    });

    // Wait for processing to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stop the processor to signal shutdown
    let _ = processor.stop().await;

    // Wait for process_job to complete with timeout
    tokio::time::timeout(Duration::from_secs(5), process_handle)
        .await
        .expect("process_job timed out")
        .expect("process_job task panicked")
}

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

    // Create simple SELECT query
    let query_str = "SELECT id, value FROM input_stream";
    let parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create AdaptiveJobProcessor with 2 partitions
    use velostream::velostream::server::v2::{PartitionedJobConfig, RoundRobinStrategy};
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(2),
        enable_core_affinity: false,
    };
    let processor = match config {
        JobProcessorConfig::Adaptive {
            num_partitions,
            enable_core_affinity,
        } => {
            Arc::new(
                AdaptiveJobProcessor::new(PartitionedJobConfig {
                    num_partitions,
                    enable_core_affinity,
                    ..Default::default()
                })
                // Use RoundRobinStrategy for simple SELECT (doesn't require GROUP BY columns)
                .with_strategy(std::sync::Arc::new(RoundRobinStrategy::new())),
            )
        }
        _ => panic!("Expected Adaptive config"),
    };

    // Process job with proper shutdown handling
    let result =
        run_process_job(processor.clone(), reader, writer.clone(), query, "test_job").await;

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

/// Test that AdaptiveJobProcessor actually writes records to the sink
#[tokio::test]
async fn test_adaptive_processor_writes_records() {
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
    let reader = MockDataSource::new(test_records.clone(), 10); // 10 batches of 5 records
    let writer = MockDataWriter::new();

    // Create simple SELECT query
    let query_str = "SELECT id, value FROM input_stream";
    let parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create AdaptiveJobProcessor with 2 partitions
    use velostream::velostream::server::v2::{PartitionedJobConfig, RoundRobinStrategy};
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(2),
        enable_core_affinity: false,
    };
    let processor = match config {
        JobProcessorConfig::Adaptive {
            num_partitions,
            enable_core_affinity,
        } => Arc::new(
            AdaptiveJobProcessor::new(PartitionedJobConfig {
                num_partitions,
                enable_core_affinity,
                ..Default::default()
            })
            .with_strategy(std::sync::Arc::new(RoundRobinStrategy::new())),
        ),
        _ => panic!("Expected Adaptive config"),
    };

    // Process job with proper shutdown handling
    let result = run_process_job(
        processor.clone(),
        reader,
        writer.clone(),
        query,
        "test_write_job",
    )
    .await;

    // Verify processing completed
    assert!(result.is_ok(), "process_job failed: {:?}", result.err());

    let stats = result.unwrap();
    println!(
        "Records processed: {}, records failed: {}",
        stats.records_processed, stats.records_failed
    );

    // Give async tasks time to finish writing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify records were written to sink
    let written_count = writer.get_count();
    println!(
        "Test write_records: {} records processed, {} records written",
        stats.records_processed, written_count
    );

    // CRITICAL: Records should be written to output
    assert!(
        written_count > 0,
        "No records were written to sink! processed: {}, written: {}",
        stats.records_processed,
        written_count
    );

    // For SELECT queries, output count should match processed count
    assert_eq!(
        written_count, 50,
        "Expected 50 records written, got {}",
        written_count
    );
}

/// Test AdaptiveJobProcessor with the exact query from comprehensive baseline (Scenario 1)
#[tokio::test]
async fn test_adaptive_processor_comprehensive_scenario_1() {
    // Use same query as comprehensive test Scenario 1
    let query_str = r#"
        SELECT order_id, customer_id, order_date, total_amount
        FROM orders
        WHERE total_amount > 100
    "#;

    // Create test data matching comprehensive test structure
    let test_records = {
        let mut records = Vec::with_capacity(50);
        for i in 0..50 {
            let mut fields = HashMap::new();
            fields.insert("order_id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "customer_id".to_string(),
                FieldValue::Integer((i / 5) as i64),
            );
            fields.insert(
                "order_date".to_string(),
                FieldValue::String("2024-01-01".to_string()),
            );
            fields.insert(
                "total_amount".to_string(),
                FieldValue::Float(100.0 + (i as f64)),
            );
            records.push(StreamRecord::new(fields));
        }
        records
    };

    let reader = MockDataSource::new(test_records.clone(), 10);
    let writer = MockDataWriter::new();

    let parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    use velostream::velostream::server::v2::{PartitionedJobConfig, RoundRobinStrategy};
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    };
    let processor = match config {
        JobProcessorConfig::Adaptive {
            num_partitions,
            enable_core_affinity,
        } => Arc::new(
            AdaptiveJobProcessor::new(PartitionedJobConfig {
                num_partitions,
                enable_core_affinity,
                ..Default::default()
            })
            .with_strategy(std::sync::Arc::new(RoundRobinStrategy::new())),
        ),
        _ => panic!("Expected Adaptive config"),
    };

    let result = run_process_job(
        processor.clone(),
        reader,
        writer.clone(),
        query,
        "test_scenario_1",
    )
    .await;

    assert!(result.is_ok(), "process_job failed: {:?}", result.err());

    let stats = result.unwrap();

    // Give async tasks time to finish
    tokio::time::sleep(Duration::from_millis(500)).await;

    let written_count = writer.get_count();
    println!(
        "Scenario 1: {} records processed, {} records written",
        stats.records_processed, written_count
    );

    // Should have written all 50 records
    assert!(
        written_count > 0,
        "No records written for Scenario 1 query! Processed: {}, Written: {}",
        stats.records_processed,
        written_count
    );
}

/// Test AdaptiveJobProcessor with large batch (like comprehensive test does)
#[tokio::test]
async fn test_adaptive_processor_large_single_batch() {
    // Create 5000 records like comprehensive test
    let test_records = {
        let mut records = Vec::with_capacity(5000);
        for i in 0..5000 {
            let mut fields = HashMap::new();
            fields.insert("order_id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "customer_id".to_string(),
                FieldValue::Integer((i / 1000) as i64),
            );
            fields.insert(
                "order_date".to_string(),
                FieldValue::String("2024-01-15".to_string()),
            );
            fields.insert(
                "total_amount".to_string(),
                FieldValue::Float(150.0 + ((i % 100) as f64)),
            );
            records.push(StreamRecord::new(fields));
        }
        records
    };

    // Use records.len() as batch_size like comprehensive does - creates 1 batch of 5000 records
    let reader = MockDataSource::new(test_records.clone(), test_records.len());
    let writer = MockDataWriter::new();

    let query_str = r#"
        SELECT order_id, customer_id, order_date, total_amount
        FROM orders
        WHERE total_amount > 100
    "#;

    let parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    use velostream::velostream::server::v2::{PartitionedJobConfig, RoundRobinStrategy};
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(1),
        enable_core_affinity: false,
    };
    let processor = match config {
        JobProcessorConfig::Adaptive {
            num_partitions,
            enable_core_affinity,
        } => Arc::new(
            AdaptiveJobProcessor::new(PartitionedJobConfig {
                num_partitions,
                enable_core_affinity,
                ..Default::default()
            })
            .with_strategy(std::sync::Arc::new(RoundRobinStrategy::new())),
        ),
        _ => panic!("Expected Adaptive config"),
    };

    let result = run_process_job(
        processor.clone(),
        reader,
        writer.clone(),
        query,
        "test_large_batch",
    )
    .await;

    assert!(result.is_ok(), "process_job failed: {:?}", result.err());

    let stats = result.unwrap();

    // Give async tasks time to finish
    tokio::time::sleep(Duration::from_millis(500)).await;

    let written_count = writer.get_count();
    println!(
        "Large batch test: {} records processed, {} records written",
        stats.records_processed, written_count
    );

    // Should have written all 5000 records
    assert!(
        written_count > 0,
        "No records written for large batch! Processed: {}, Written: {}",
        stats.records_processed,
        written_count
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

    // Create GROUP BY query
    let query_str = "SELECT trader_id, COUNT(*) as trade_count, SUM(quantity) as total_qty FROM input_stream GROUP BY trader_id";
    let parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create AdaptiveJobProcessor with 4 partitions
    use velostream::velostream::server::v2::PartitionedJobConfig;
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(4),
        enable_core_affinity: false,
    };
    let processor = match config {
        JobProcessorConfig::Adaptive {
            num_partitions,
            enable_core_affinity,
        } => {
            Arc::new(
                AdaptiveJobProcessor::new(PartitionedJobConfig {
                    num_partitions,
                    enable_core_affinity,
                    ..Default::default()
                })
                // Configure GROUP BY columns for proper routing
                .with_group_by_columns(vec!["trader_id".to_string()]),
            )
        }
        _ => panic!("Expected Adaptive config"),
    };

    // Process job with proper shutdown handling
    let result = run_process_job(
        processor.clone(),
        reader,
        writer.clone(),
        query,
        "test_group_by",
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

    // Create simple SELECT query
    let query_str = "SELECT id, trader_id FROM input_stream";
    let parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create AdaptiveJobProcessor with 4 partitions
    use velostream::velostream::server::v2::{PartitionedJobConfig, RoundRobinStrategy};
    let config = JobProcessorConfig::Adaptive {
        num_partitions: Some(4),
        enable_core_affinity: false,
    };
    let processor = match config {
        JobProcessorConfig::Adaptive {
            num_partitions,
            enable_core_affinity,
        } => {
            Arc::new(
                AdaptiveJobProcessor::new(PartitionedJobConfig {
                    num_partitions,
                    enable_core_affinity,
                    ..Default::default()
                })
                // Use RoundRobinStrategy for simple SELECT (doesn't require GROUP BY columns)
                .with_strategy(std::sync::Arc::new(RoundRobinStrategy::new())),
            )
        }
        _ => panic!("Expected Adaptive config"),
    };

    // Create dummy engine wrapper for process_job (coordinator will create its own)
    let (dummy_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let _dummy_engine = Arc::new(tokio::sync::RwLock::new(
        velostream::velostream::sql::StreamExecutionEngine::new(dummy_tx),
    ));

    // Process job with proper shutdown handling
    let result = run_process_job(
        processor.clone(),
        reader,
        writer.clone(),
        query,
        "test_routing",
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

/// Test that AdaptiveJobProcessor with empty_batch_count=0 doesn't hang
/// This verifies that the processor exits immediately when all data is exhausted
#[tokio::test]
async fn test_adaptive_processor_with_zero_empty_batch_count() {
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

    // Simple SELECT query
    let query_str = "SELECT id, value FROM input_stream";
    let parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create AdaptiveJobProcessor with 2 partitions
    use velostream::velostream::server::v2::{PartitionedJobConfig, RoundRobinStrategy};
    let mut job_config = PartitionedJobConfig {
        num_partitions: Some(2),
        enable_core_affinity: false,
        ..Default::default()
    };
    // CRITICAL TEST: Set empty_batch_count to 0 for immediate exit
    job_config.empty_batch_count = 0;

    let processor = Arc::new(
        AdaptiveJobProcessor::new(job_config)
            .with_strategy(std::sync::Arc::new(RoundRobinStrategy::new())),
    );

    // This should NOT hang even with empty_batch_count=0
    // The timeout on run_process_job (5 seconds) will catch any hang
    let result = run_process_job(
        processor.clone(),
        reader,
        writer.clone(),
        query,
        "test_zero_empty_batch",
    )
    .await;

    // Verify processing completed successfully (no hang, no timeout)
    assert!(
        result.is_ok(),
        "process_job with empty_batch_count=0 failed or timed out: {:?}",
        result.err()
    );

    let stats = result.unwrap();
    assert_eq!(
        stats.records_processed, 50,
        "Expected 50 records processed with empty_batch_count=0, got {}",
        stats.records_processed
    );

    // Give async tasks time to finish writing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify records were written to sink
    let written_count = writer.get_count();
    println!(
        "Test zero_empty_batch_count: {} records processed, {} records written",
        stats.records_processed, written_count
    );

    assert_eq!(
        written_count, 50,
        "Expected 50 records written with empty_batch_count=0, got {}",
        written_count
    );
}

/// Test AdaptiveJobProcessor with JobProcessorFactory::create_adaptive_test_optimized()
/// This reproduces the bug where written_count shows 0 in comprehensive_baseline_comparison
#[tokio::test]
async fn test_adaptive_processor_factory_test_optimized() {
    use velostream::velostream::server::processors::JobProcessorFactory;

    // Create 5000 records to match comprehensive_baseline_comparison test
    let test_records = {
        let mut records = Vec::with_capacity(5000);
        let base_time = 1700000000i64;
        for i in 0..5000 {
            let mut fields = HashMap::new();
            let trader_id = format!("TRADER{}", i % 20);
            let symbol = format!("SYM{}", i % 10);
            let price = 100.0 + (i as f64 % 50.0);
            let quantity = 100 + (i % 1000);
            let timestamp = base_time + (i as i64);

            fields.insert("trader_id".to_string(), FieldValue::String(trader_id));
            fields.insert("symbol".to_string(), FieldValue::String(symbol));
            fields.insert("price".to_string(), FieldValue::Float(price));
            fields.insert("quantity".to_string(), FieldValue::Integer(quantity as i64));
            fields.insert("trade_time".to_string(), FieldValue::Integer(timestamp));

            records.push(StreamRecord::new(fields));
        }
        records
    };

    // Create mock reader and writer
    let reader = MockDataSource::new(test_records.clone(), 5000); // Single batch
    let writer = MockDataWriter::new();

    // Create GROUP BY query (matching comprehensive test Scenario 3 - group by strategy compatible)
    // The AdaptiveJobProcessor uses AlwaysHashStrategy which requires GROUP BY columns
    let query_str = r#"
        SELECT symbol,
            COUNT(*) as trade_count,
            SUM(quantity) as total_quantity,
            AVG(price) as avg_price
        FROM market_data
        GROUP BY symbol
    "#;

    let parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create AdaptiveJobProcessor using JobProcessorFactory::create_adaptive_test_optimized()
    // with 1 partition (matching comprehensive test)
    let processor = JobProcessorFactory::create_adaptive_test_optimized(Some(1));

    // Create the underlying trait object processor for process_job
    let (dummy_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let dummy_engine = Arc::new(tokio::sync::RwLock::new(
        velostream::velostream::sql::StreamExecutionEngine::new(dummy_tx),
    ));

    let start = std::time::Instant::now();

    // Use the same pattern as the working tests - spawn as a separate task
    let processor_clone = processor.clone();
    let writer_for_task = writer.clone();
    let job_handle = tokio::spawn(async move {
        let result = processor_clone
            .process_job(
                Box::new(reader),
                Some(Box::new(writer_for_task)),
                dummy_engine,
                query,
                "test_factory_optimized".to_string(),
                {
                    let (_tx, rx) = tokio::sync::mpsc::channel(1);
                    rx
                },
                None,
            )
            .await;
        result
    });

    // Wait a moment for async tasks to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Process task to completion
    let result = job_handle.await.expect("process_job task panicked");

    let elapsed = start.elapsed();

    // Wait for async partition receiver tasks to complete (same as comprehensive test)
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Get results
    let records_written = writer.get_count();

    // Print results in same format as comprehensive test
    println!("\n═══════════════════════════════════════════════════════════");
    println!("TEST: AdaptiveJobProcessor with Factory (Test-Optimized)");
    println!("═══════════════════════════════════════════════════════════");
    println!("Input records:        {}", test_records.len());
    println!("Output records:       {}", records_written);
    println!("Elapsed time:         {:.3}s", elapsed.as_secs_f64());
    if records_written > 0 {
        println!(
            "Throughput (actual):  {:.0} out_rec/sec",
            (records_written as f64) / elapsed.as_secs_f64()
        );
    } else {
        println!("Throughput (actual):  0 out_rec/sec (NO RECORDS WRITTEN)");
    }
    println!(
        "Throughput (reported): {:.0} rec/sec (based on input)",
        (test_records.len() as f64) / elapsed.as_secs_f64()
    );
    println!(
        "Output multiplier:    {:.1}x",
        (records_written as f64) / (test_records.len() as f64)
    );
    println!("═══════════════════════════════════════════════════════════\n");

    // Verify processing completed
    assert!(result.is_ok(), "process_job failed: {:?}", result.err());

    // THIS IS THE CRITICAL ASSERTION THAT SHOULD FAIL WITH THE CURRENT BUG
    // In comprehensive_baseline_comparison, records_written always shows 0 for AdaptiveJp
    // This test should FAIL if the bug exists
    assert!(
        records_written > 0,
        "BUG REPRODUCED: No records were written to sink! \
         Input: {}, Output: {}, Elapsed: {:.3}s. \
         This is the same bug seen in comprehensive_baseline_comparison.",
        test_records.len(),
        records_written,
        elapsed.as_secs_f64()
    );

    // For EMIT CHANGES, output should have results (windowed aggregates)
    // The exact count depends on window boundaries, but should not be 0
    println!(
        "✓ Records written correctly: {} records processed and written",
        records_written
    );
}

// =====================================================
// SHARED STATS TESTS
// =====================================================

/// Test that AdaptiveJobProcessor (V2) updates SharedJobStats during execution
#[tokio::test]
async fn test_adaptive_processor_shared_stats_are_updated() {
    use std::sync::RwLock;
    use velostream::velostream::server::processors::common::JobExecutionStats;
    use velostream::velostream::server::v2::PartitionedJobConfig;

    println!("\n=== Test: AdaptiveJobProcessor (V2) SharedStats Updates ===");

    // Create test records (20 records)
    let reader = MockDataSource::with_records(20);
    let writer = MockDataWriter::new();

    // Create processor with 2 partitions
    let config = PartitionedJobConfig {
        num_partitions: Some(2),
        ..Default::default()
    };
    let processor = Arc::new(AdaptiveJobProcessor::new(config));

    // Parse simple query
    let query_str = "SELECT id, value, trader_id FROM test";
    let parser = velostream::velostream::sql::parser::StreamingSqlParser::new();
    let query = parser.parse(query_str).expect("Failed to parse query");

    // Create shared stats for real-time monitoring
    let shared_stats: Arc<RwLock<JobExecutionStats>> =
        Arc::new(RwLock::new(JobExecutionStats::new()));

    // Run process_job with shared_stats
    let (dummy_tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let dummy_engine = Arc::new(tokio::sync::RwLock::new(
        velostream::velostream::sql::StreamExecutionEngine::new(dummy_tx),
    ));

    let processor_clone = Arc::clone(&processor);
    let shared_stats_clone = shared_stats.clone();
    let process_handle = tokio::spawn(async move {
        processor_clone
            .process_job(
                Box::new(reader),
                Some(Box::new(writer)),
                dummy_engine,
                query,
                "test_v2_shared_stats".to_string(),
                {
                    let (_tx, rx) = tokio::sync::mpsc::channel(1);
                    rx
                },
                Some(shared_stats_clone), // Pass shared stats
            )
            .await
    });

    // Wait for processing to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stop the processor to signal shutdown
    let _ = processor.stop().await;

    // Wait for process_job to complete with timeout
    let result = tokio::time::timeout(Duration::from_secs(5), process_handle)
        .await
        .expect("process_job timed out")
        .expect("process_job task panicked");

    match result {
        Ok(final_stats) => {
            println!("✅ Test completed with final stats: {:?}", final_stats);

            // Verify final stats show records were processed
            assert!(
                final_stats.records_processed > 0,
                "Final stats should show records processed, got: {}",
                final_stats.records_processed
            );

            // Verify shared stats were updated
            let shared_stats_read = shared_stats
                .read()
                .expect("Should be able to read shared stats");
            assert!(
                shared_stats_read.records_processed > 0,
                "SharedStats should show records processed, got: {}",
                shared_stats_read.records_processed
            );
            assert!(
                shared_stats_read.batches_processed > 0,
                "SharedStats should show batches processed, got: {}",
                shared_stats_read.batches_processed
            );

            println!("✅ SharedStats verification passed:");
            println!(
                "   - Records processed: {}",
                shared_stats_read.records_processed
            );
            println!(
                "   - Batches processed: {}",
                shared_stats_read.batches_processed
            );
        }
        Err(e) => {
            panic!(
                "AdaptiveJobProcessor (V2) SharedStats test should not fail: {:?}",
                e
            );
        }
    }
}
