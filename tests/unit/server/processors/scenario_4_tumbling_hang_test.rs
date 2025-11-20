/*!
# Scenario 4 TUMBLING WINDOW + GROUP BY Hang Test

Reproduces the hang issue observed in comprehensive_baseline_comparison when SimpleJobProcessor
processes 100K records with a TUMBLING WINDOW + GROUP BY query.
*/

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use velostream::velostream::datasource::{BatchConfig, DataReader, DataWriter};
use velostream::velostream::server::processors::{
    FailureStrategy, JobProcessingConfig, JobProcessorFactory,
};
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Mock DataReader that produces records similar to Scenario 4
#[derive(Clone)]
struct Scenario4MockReader {
    records: Vec<StreamRecord>,
    current_index: Arc<Mutex<usize>>,
    read_count: Arc<AtomicUsize>,
}

impl Scenario4MockReader {
    fn new(record_count: usize) -> Self {
        let mut records = Vec::new();

        for i in 0..record_count {
            let mut fields = HashMap::new();
            // Use same pattern as comprehensive_baseline_comparison
            let trader_id = format!("T{}", i % 50); // 50 unique traders
            let symbol = format!("SYM{}", i % 100); // 100 symbols
            let price = 100.0 + (i % 50) as f64;
            let quantity = (i % 1000) as i64;
            // Increment by 1000ms (1 second) per record → 60 records = 1 minute
            let trade_time = 1000000 + (i as i64 * 1000);

            fields.insert("trader_id".to_string(), FieldValue::String(trader_id.clone()));
            fields.insert("symbol".to_string(), FieldValue::String(symbol.clone()));
            fields.insert("price".to_string(), FieldValue::Float(price));
            fields.insert("quantity".to_string(), FieldValue::Integer(quantity));
            fields.insert("trade_time".to_string(), FieldValue::Integer(trade_time));

            // Partition by composite key (trader_id + symbol)
            let composite_key = format!("{}:{}", trader_id, symbol);
            let mut record = StreamRecord::new(fields);
            record = record.with_partition_from_key(&composite_key, 32);
            record.timestamp = trade_time;
            record.offset = i as i64;

            records.push(record);
        }

        Self {
            records,
            current_index: Arc::new(Mutex::new(0)),
            read_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl DataReader for Scenario4MockReader {
    async fn read(&mut self) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        let mut idx = self.current_index.lock().await;
        let batch_size = 100.min(self.records.len() - *idx);

        if batch_size == 0 {
            return Ok(vec![]);
        }

        let end_index = *idx + batch_size;
        let batch = self.records[*idx..end_index].to_vec();
        *idx = end_index;

        self.read_count.fetch_add(batch_size, Ordering::SeqCst);
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
        let idx = self.current_index.lock().await;
        Ok(*idx < self.records.len())
    }
}

/// Mock DataWriter that collects output records
#[derive(Clone)]
struct Scenario4MockWriter {
    records: Arc<Mutex<Vec<StreamRecord>>>,
}

impl Scenario4MockWriter {
    fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn get_count(&self) -> usize {
        self.records.lock().await.len()
    }
}

#[async_trait]
impl DataWriter for Scenario4MockWriter {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records.lock().await.push(record);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<std::sync::Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records
            .lock()
            .await
            .extend(records.iter().map(|r| (**r).clone()));
        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.records.lock().await.push(record);
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

/// Test with smaller record count (1000) to verify basic functionality
#[tokio::test]
async fn test_scenario_4_simple_jp_small_dataset() {
    let record_count = 1000;
    let mut reader = Scenario4MockReader::new(record_count);
    let mut writer = Scenario4MockWriter::new();

    let query = r#"
        SELECT trader_id, symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity,
            SUM(price * quantity) as total_value
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
    "#;

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

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
        wait_on_empty_batch_ms: 10,
        enable_dlq: true,
        dlq_max_size: Some(100),
    };

    let processor = JobProcessorFactory::create_simple_with_config(config);
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = std::time::Instant::now();
    let timeout_duration = Duration::from_secs(30);

    let timeout_result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(reader),
            Some(Box::new(writer.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "test_scenario_4_small".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();

    let records_written = writer.get_count().await;

    match timeout_result {
        Ok(_) => {
            println!(
                "✓ Test passed in {:.2}s - wrote {} records",
                elapsed.as_secs_f64(),
                records_written
            );
            assert!(records_written > 0, "Expected at least some output records");
        }
        Err(_) => {
            panic!(
                "❌ Test TIMEOUT after {:.2}s - only wrote {} out of {} records",
                elapsed.as_secs_f64(),
                records_written,
                record_count
            );
        }
    }
}

/// Test with larger dataset (10K) to see if hang appears
#[tokio::test]
async fn test_scenario_4_simple_jp_medium_dataset() {
    let record_count = 10000;
    let mut reader = Scenario4MockReader::new(record_count);
    let mut writer = Scenario4MockWriter::new();

    let query = r#"
        SELECT trader_id, symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity,
            SUM(price * quantity) as total_value
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
    "#;

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

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
        wait_on_empty_batch_ms: 10,
        enable_dlq: true,
        dlq_max_size: Some(100),
    };

    let processor = JobProcessorFactory::create_simple_with_config(config);
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = std::time::Instant::now();
    let timeout_duration = Duration::from_secs(60);

    let timeout_result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(reader),
            Some(Box::new(writer.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "test_scenario_4_medium".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();

    let records_written = writer.get_count().await;

    match timeout_result {
        Ok(_) => {
            println!(
                "✓ Test passed in {:.2}s - wrote {} records",
                elapsed.as_secs_f64(),
                records_written
            );
            assert!(records_written > 0, "Expected at least some output records");
        }
        Err(_) => {
            eprintln!(
                "❌ Test TIMEOUT after {:.2}s - only wrote {} out of {} records",
                elapsed.as_secs_f64(),
                records_written,
                record_count
            );
            panic!("Test timed out - SimpleJobProcessor hangs on Scenario 4 with larger dataset");
        }
    }
}

/// Test with large dataset (50K) to reproduce hang at scale
#[tokio::test]
async fn test_scenario_4_simple_jp_large_dataset() {
    let record_count = 50000;
    let mut reader = Scenario4MockReader::new(record_count);
    let mut writer = Scenario4MockWriter::new();

    let query = r#"
        SELECT trader_id, symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity,
            SUM(price * quantity) as total_value
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE)
    "#;

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");
    let query_arc = Arc::new(parsed_query);

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
        wait_on_empty_batch_ms: 10,
        enable_dlq: true,
        dlq_max_size: Some(100),
    };

    let processor = JobProcessorFactory::create_simple_with_config(config);
    let (_shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let start = std::time::Instant::now();
    let timeout_duration = Duration::from_secs(120);

    let timeout_result = tokio::time::timeout(
        timeout_duration,
        processor.process_job(
            Box::new(reader),
            Some(Box::new(writer.clone())),
            Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(
                mpsc::unbounded_channel().0,
            ))),
            (*query_arc).clone(),
            "test_scenario_4_large".to_string(),
            shutdown_rx,
        ),
    )
    .await;

    processor.stop().await.ok();
    let elapsed = start.elapsed();

    let records_written = writer.get_count().await;

    match timeout_result {
        Ok(_) => {
            println!(
                "✓ Test passed in {:.2}s - wrote {} records",
                elapsed.as_secs_f64(),
                records_written
            );
            assert!(records_written > 0, "Expected at least some output records");
        }
        Err(_) => {
            eprintln!(
                "❌ Test TIMEOUT after {:.2}s - only wrote {} out of {} records",
                elapsed.as_secs_f64(),
                records_written,
                record_count
            );
            panic!("Test timed out - SimpleJobProcessor hangs on Scenario 4 with 50K dataset");
        }
    }
}
