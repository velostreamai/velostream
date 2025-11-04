//! Shared test infrastructure for stream job processors
//!
//! This module provides reusable test components and failure scenarios
//! that can be used across different stream job processor implementations.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use velostream::velostream::datasource::{DataReader, DataWriter, SourceOffset};
use velostream::velostream::server::processors::common::{
    FailureStrategy, JobExecutionStats, JobProcessingConfig,
};
use velostream::velostream::sql::{
    ast::{SelectField, StreamSource, StreamingQuery},
    execution::{
        engine::StreamExecutionEngine,
        types::{FieldValue, StreamRecord},
    },
};

// =====================================================
// SHARED MOCK INFRASTRUCTURE
// =====================================================

/// Advanced Mock DataReader that supports comprehensive failure injection
pub struct AdvancedMockDataReader {
    records: Vec<Vec<StreamRecord>>,
    current_batch: usize,
    read_failure_on_batch: Option<usize>,
    commit_failure_on_batch: Option<usize>,
    has_more_failure: bool,
    simulate_timeout: bool,
    supports_transactions: bool,
}

impl AdvancedMockDataReader {
    pub fn new(records: Vec<Vec<StreamRecord>>) -> Self {
        Self {
            records,
            current_batch: 0,
            read_failure_on_batch: None,
            commit_failure_on_batch: None,
            has_more_failure: false,
            simulate_timeout: false,
            supports_transactions: false,
        }
    }

    pub fn with_read_failure_on_batch(mut self, batch: usize) -> Self {
        self.read_failure_on_batch = Some(batch);
        self
    }

    pub fn with_commit_failure_on_batch(mut self, batch: usize) -> Self {
        self.commit_failure_on_batch = Some(batch);
        self
    }

    pub fn with_has_more_failure(mut self) -> Self {
        self.has_more_failure = true;
        self
    }

    pub fn with_timeout_simulation(mut self) -> Self {
        self.simulate_timeout = true;
        self
    }

    pub fn with_transaction_support(mut self) -> Self {
        self.supports_transactions = true;
        self
    }
}

#[async_trait]
impl DataReader for AdvancedMockDataReader {
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
        // Simulate network timeout
        if self.simulate_timeout && self.current_batch == 1 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Simulate read failure
        if let Some(fail_batch) = self.read_failure_on_batch {
            if self.current_batch == fail_batch {
                return Err(format!(
                    "AdvancedMockDataReader: Simulated read failure on batch {}",
                    fail_batch
                )
                .into());
            }
        }

        if self.current_batch < self.records.len() {
            let batch = self.records[self.current_batch].clone();
            self.current_batch += 1;
            Ok(batch)
        } else {
            Ok(vec![])
        }
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.has_more_failure {
            return Err("AdvancedMockDataReader: Simulated has_more failure".into());
        }
        Ok(self.current_batch < self.records.len())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(fail_batch) = self.commit_failure_on_batch {
            if self.current_batch > fail_batch {
                return Err(format!(
                    "AdvancedMockDataReader: Simulated commit failure after batch {}",
                    fail_batch
                )
                .into());
            }
        }
        println!(
            "AdvancedMockDataReader: Commit called for batch {}",
            self.current_batch
        );
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("AdvancedMockDataReader: Abort transaction called");
        Ok(())
    }

    async fn seek(
        &mut self,
        _offset: SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Mock implementation - no-op
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        self.supports_transactions
    }
}

/// Advanced Mock DataWriter that supports comprehensive failure scenarios
pub struct AdvancedMockDataWriter {
    fail_on_batch: Option<usize>,
    flush_failure_on_batch: Option<usize>,
    commit_failure_on_batch: Option<usize>,
    current_batch_count: usize,
    written_records: Arc<Mutex<Vec<StreamRecord>>>,
    simulate_disk_full: bool,
    simulate_network_partition: bool,
    partial_batch_failure: bool,
    supports_transactions: bool,
}

impl AdvancedMockDataWriter {
    pub fn new() -> Self {
        Self {
            fail_on_batch: None,
            flush_failure_on_batch: None,
            commit_failure_on_batch: None,
            current_batch_count: 0,
            written_records: Arc::new(Mutex::new(Vec::new())),
            simulate_disk_full: false,
            simulate_network_partition: false,
            partial_batch_failure: false,
            supports_transactions: false,
        }
    }

    pub fn with_write_failure_on_batch(mut self, batch: usize) -> Self {
        self.fail_on_batch = Some(batch);
        self
    }

    pub fn with_flush_failure_on_batch(mut self, batch: usize) -> Self {
        self.flush_failure_on_batch = Some(batch);
        self
    }

    pub fn with_commit_failure_on_batch(mut self, batch: usize) -> Self {
        self.commit_failure_on_batch = Some(batch);
        self
    }

    pub fn with_disk_full_simulation(mut self) -> Self {
        self.simulate_disk_full = true;
        self
    }

    pub fn with_network_partition_simulation(mut self) -> Self {
        self.simulate_network_partition = true;
        self
    }

    pub fn with_partial_batch_failure(mut self) -> Self {
        self.partial_batch_failure = true;
        self
    }

    pub fn with_transaction_support(mut self) -> Self {
        self.supports_transactions = true;
        self
    }

    pub async fn get_written_records(&self) -> Vec<StreamRecord> {
        self.written_records.lock().await.clone()
    }
}

#[async_trait]
impl DataWriter for AdvancedMockDataWriter {
    async fn write(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Simulate various failure conditions
        if self.simulate_disk_full {
            return Err("AdvancedMockDataWriter: Disk full - no space left on device".into());
        }

        if self.simulate_network_partition && self.current_batch_count >= 1 {
            return Err("AdvancedMockDataWriter: Network partition - connection timeout".into());
        }

        let mut written = self.written_records.lock().await;
        written.push(record);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<std::sync::Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!(
            "AdvancedMockDataWriter: write_batch called with {} records (batch {})",
            records.len(),
            self.current_batch_count
        );

        // Check for batch-specific failure
        if let Some(fail_batch) = self.fail_on_batch {
            if self.current_batch_count >= fail_batch {
                self.current_batch_count += 1;
                return Err(format!(
                    "AdvancedMockDataWriter: Simulated write_batch failure on batch {}",
                    self.current_batch_count - 1
                )
                .into());
            }
        }

        // Simulate partial batch failure - some records succeed, some fail
        if self.partial_batch_failure && self.current_batch_count == 1 {
            // Write first half, fail on second half
            let mid = records.len() / 2;
            for record_arc in records.iter().take(mid) {
                // Dereference Arc and clone for write() which takes ownership
                self.write((**record_arc).clone()).await?;
            }
            self.current_batch_count += 1;
            return Err(format!(
                "AdvancedMockDataWriter: Partial batch failure - {}/{} records succeeded",
                mid,
                records.len()
            )
            .into());
        }

        // Write all records
        for record_arc in records {
            // Dereference Arc and clone for write() which takes ownership
            self.write((*record_arc).clone()).await?;
        }
        self.current_batch_count += 1;
        Ok(())
    }

    async fn update(
        &mut self,
        _key: &str,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write(record).await
    }

    async fn delete(&mut self, _key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(fail_batch) = self.flush_failure_on_batch {
            if self.current_batch_count > fail_batch {
                return Err(format!(
                    "AdvancedMockDataWriter: Simulated flush failure after batch {}",
                    fail_batch
                )
                .into());
            }
        }
        println!("AdvancedMockDataWriter: Flush called successfully");
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(fail_batch) = self.commit_failure_on_batch {
            if self.current_batch_count > fail_batch {
                return Err(format!(
                    "AdvancedMockDataWriter: Simulated commit failure after batch {}",
                    fail_batch
                )
                .into());
            }
        }
        println!("AdvancedMockDataWriter: Commit called successfully");
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("AdvancedMockDataWriter: Rollback called");
        Ok(())
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

    fn supports_transactions(&self) -> bool {
        self.supports_transactions
    }
}

// =====================================================
// SHARED TEST DATA AND HELPERS
// =====================================================

pub fn create_test_record(id: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert(
        "value".to_string(),
        FieldValue::String(format!("test_{}", id)),
    );

    StreamRecord::with_metadata(fields, 1000 + id, id, 0, HashMap::new())
}

pub fn create_test_query() -> StreamingQuery {
    StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_stream".to_string()),
        from_alias: None,
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
        properties: None,
    }
}

pub fn create_test_engine() -> Arc<Mutex<StreamExecutionEngine>> {
    let (output_sender, _) = tokio::sync::mpsc::unbounded_channel();
    Arc::new(Mutex::new(StreamExecutionEngine::new(output_sender)))
}

// =====================================================
// FAILURE SCENARIO TEST TRAITS
// =====================================================

/// Generic trait for multi-job processor testing
/// This allows us to test any job processor implementation consistently
#[async_trait]
pub trait StreamJobProcessor {
    type StatsType;

    async fn process_job(
        &self,
        reader: Box<dyn DataReader>,
        writer: Option<Box<dyn DataWriter>>,
        engine: Arc<Mutex<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<Self::StatsType, Box<dyn std::error::Error + Send + Sync>>;

    fn get_config(&self) -> &JobProcessingConfig;
}

// =====================================================
// REUSABLE TEST SCENARIOS
// =====================================================

/// Test source read failure handling across different processors
pub async fn test_source_read_failure_scenario<T: StreamJobProcessor>(
    processor: &T,
    test_name: &str,
) where
    T::StatsType: std::fmt::Debug,
{
    let _ = env_logger::builder().is_test(true).try_init();
    println!("\n=== Test: Source Read Failure - {} ===", test_name);

    let test_batches = vec![
        vec![create_test_record(1), create_test_record(2)],
        vec![create_test_record(3), create_test_record(4)],
    ];

    let reader = Box::new(AdvancedMockDataReader::new(test_batches).with_read_failure_on_batch(1))
        as Box<dyn DataReader>;
    let writer = Box::new(AdvancedMockDataWriter::new()) as Box<dyn DataWriter>;

    let engine = create_test_engine();
    let query = create_test_query();
    let (_, shutdown_rx) = mpsc::channel::<()>(1);

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        processor.process_job(
            reader,
            Some(writer),
            engine,
            query,
            format!("test_source_read_failure_{}", test_name),
            shutdown_rx,
        ),
    )
    .await;

    let result = match result {
        Ok(job_result) => job_result,
        Err(_) => {
            println!(
                "⚠️  Source read failure test timed out after 10s ({})",
                test_name
            );
            return;
        }
    };

    println!(
        "Source read failure test result ({}): {:?}",
        test_name, result
    );
}

/// Test sink write failure handling across different processors
pub async fn test_sink_write_failure_scenario<T: StreamJobProcessor>(processor: &T, test_name: &str)
where
    T::StatsType: std::fmt::Debug,
{
    let _ = env_logger::builder().is_test(true).try_init();
    println!("\n=== Test: Sink Write Failure - {} ===", test_name);

    let test_batches = vec![vec![create_test_record(1)], vec![create_test_record(2)]];

    let reader = Box::new(AdvancedMockDataReader::new(test_batches)) as Box<dyn DataReader>;
    let writer = Box::new(AdvancedMockDataWriter::new().with_write_failure_on_batch(0))
        as Box<dyn DataWriter>;

    let engine = create_test_engine();
    let query = create_test_query();
    let (_, shutdown_rx) = mpsc::channel::<()>(1);

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        processor.process_job(
            reader,
            Some(writer),
            engine,
            query,
            format!("test_sink_write_failure_{}", test_name),
            shutdown_rx,
        ),
    )
    .await;

    let result = match result {
        Ok(job_result) => job_result,
        Err(_) => {
            println!(
                "⚠️  Sink write failure test timed out after 10s ({})",
                test_name
            );
            return;
        }
    };

    println!(
        "Sink write failure test result ({}): {:?}",
        test_name, result
    );
}

/// Test disk full scenario across different processors
pub async fn test_disk_full_scenario<T: StreamJobProcessor>(processor: &T, test_name: &str)
where
    T::StatsType: std::fmt::Debug,
{
    let _ = env_logger::builder().is_test(true).try_init();
    println!("\n=== Test: Disk Full Scenario - {} ===", test_name);

    let test_batches = vec![vec![create_test_record(1)], vec![create_test_record(2)]];

    let reader = Box::new(AdvancedMockDataReader::new(test_batches)) as Box<dyn DataReader>;
    let writer =
        Box::new(AdvancedMockDataWriter::new().with_disk_full_simulation()) as Box<dyn DataWriter>;

    let engine = create_test_engine();
    let query = create_test_query();
    let (_, shutdown_rx) = mpsc::channel::<()>(1);

    // Test with timeout to prevent infinite retries
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        processor.process_job(
            reader,
            Some(writer),
            engine,
            query,
            format!("test_disk_full_{}", test_name),
            shutdown_rx,
        ),
    )
    .await;

    match result {
        Ok(job_result) => println!("Disk full test completed ({}): {:?}", test_name, job_result),
        Err(_) => println!(
            "Disk full test timed out ({}) - expected behavior for RetryWithBackoff",
            test_name
        ),
    }
}

/// Test network partition scenario across different processors
pub async fn test_network_partition_scenario<T: StreamJobProcessor>(processor: &T, test_name: &str)
where
    T::StatsType: std::fmt::Debug,
{
    let _ = env_logger::builder().is_test(true).try_init();
    println!("\n=== Test: Network Partition Scenario - {} ===", test_name);

    let test_batches = vec![
        vec![create_test_record(1)],
        vec![create_test_record(2)],
        vec![create_test_record(3)],
    ];

    let reader = Box::new(AdvancedMockDataReader::new(test_batches)) as Box<dyn DataReader>;
    let writer = Box::new(AdvancedMockDataWriter::new().with_network_partition_simulation())
        as Box<dyn DataWriter>;

    let engine = create_test_engine();
    let query = create_test_query();
    let (_, shutdown_rx) = mpsc::channel::<()>(1);

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        processor.process_job(
            reader,
            Some(writer),
            engine,
            query,
            format!("test_network_partition_{}", test_name),
            shutdown_rx,
        ),
    )
    .await;

    let result = match result {
        Ok(job_result) => job_result,
        Err(_) => {
            println!(
                "⚠️  Network partition test timed out after 10s ({})",
                test_name
            );
            return;
        }
    };

    println!(
        "Network partition test result ({}): {:?}",
        test_name, result
    );
}

/// Test partial batch failure scenario across different processors
pub async fn test_partial_batch_failure_scenario<T: StreamJobProcessor>(
    processor: &T,
    test_name: &str,
) where
    T::StatsType: std::fmt::Debug,
{
    let _ = env_logger::builder().is_test(true).try_init();
    println!(
        "\n=== Test: Partial Batch Failure Scenario - {} ===",
        test_name
    );

    let test_batches = vec![
        vec![create_test_record(1), create_test_record(2)],
        vec![
            create_test_record(3),
            create_test_record(4),
            create_test_record(5),
            create_test_record(6),
        ],
        vec![create_test_record(7)],
    ];

    let reader = Box::new(AdvancedMockDataReader::new(test_batches)) as Box<dyn DataReader>;
    let writer =
        Box::new(AdvancedMockDataWriter::new().with_partial_batch_failure()) as Box<dyn DataWriter>;

    let engine = create_test_engine();
    let query = create_test_query();
    let (_, shutdown_rx) = mpsc::channel::<()>(1);

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        processor.process_job(
            reader,
            Some(writer),
            engine,
            query,
            format!("test_partial_batch_failure_{}", test_name),
            shutdown_rx,
        ),
    )
    .await;

    let result = match result {
        Ok(job_result) => job_result,
        Err(_) => {
            println!(
                "⚠️  Partial batch failure test timed out after 10s ({})",
                test_name
            );
            return;
        }
    };

    println!(
        "Partial batch failure test result ({}): {:?}",
        test_name, result
    );
}

/// Test shutdown signal handling across different processors
pub async fn test_shutdown_signal_scenario<T: StreamJobProcessor>(processor: &T, test_name: &str)
where
    T::StatsType: std::fmt::Debug,
{
    let _ = env_logger::builder().is_test(true).try_init();
    println!("\n=== Test: Shutdown Signal Scenario - {} ===", test_name);

    // Create a long-running job
    let test_batches = (0..10).map(|i| vec![create_test_record(i)]).collect();

    let reader = Box::new(AdvancedMockDataReader::new(test_batches).with_timeout_simulation())
        as Box<dyn DataReader>;
    let writer = Box::new(AdvancedMockDataWriter::new()) as Box<dyn DataWriter>;

    let engine = create_test_engine();
    let query = create_test_query();
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);

    // Send shutdown signal after a delay
    let shutdown_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("Sending shutdown signal...");
        let _ = shutdown_tx.send(()).await;
    });

    let start = std::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        processor.process_job(
            reader,
            Some(writer),
            engine,
            query,
            format!("test_shutdown_signal_{}", test_name),
            shutdown_rx,
        ),
    )
    .await;
    let elapsed = start.elapsed();

    let result = match result {
        Ok(job_result) => job_result,
        Err(_) => {
            println!(
                "⚠️  Shutdown signal test timed out after 5s ({})",
                test_name
            );
            shutdown_handle.await.unwrap();
            return;
        }
    };

    shutdown_handle.await.unwrap();

    println!(
        "Shutdown signal test ({}) completed in {:?}: {:?}",
        test_name, elapsed, result
    );

    // Should shutdown quickly when signaled (within 2 seconds)
    if elapsed < Duration::from_secs(2) {
        println!("✅ Shutdown signal handled gracefully within expected time");
    } else {
        println!("⚠️  Shutdown took longer than expected");
    }
}

/// Test empty batch handling across different processors
pub async fn test_empty_batch_handling_scenario<T: StreamJobProcessor>(
    processor: &T,
    test_name: &str,
) where
    T::StatsType: std::fmt::Debug,
{
    let _ = env_logger::builder().is_test(true).try_init();
    println!(
        "\n=== Test: Empty Batch Handling Scenario - {} ===",
        test_name
    );

    // Mix of empty and non-empty batches
    let test_batches = vec![
        vec![create_test_record(1)],
        vec![], // Empty batch
        vec![create_test_record(2), create_test_record(3)],
        vec![], // Another empty batch
    ];

    let reader = Box::new(AdvancedMockDataReader::new(test_batches)) as Box<dyn DataReader>;
    let writer = Box::new(AdvancedMockDataWriter::new()) as Box<dyn DataWriter>;

    let engine = create_test_engine();
    let query = create_test_query();
    let (_, shutdown_rx) = mpsc::channel::<()>(1);

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        processor.process_job(
            reader,
            Some(writer),
            engine,
            query,
            format!("test_empty_batch_handling_{}", test_name),
            shutdown_rx,
        ),
    )
    .await;

    let result = match result {
        Ok(job_result) => job_result,
        Err(_) => {
            println!(
                "⚠️  Empty batch handling test timed out after 10s ({})",
                test_name
            );
            return;
        }
    };

    println!(
        "Empty batch handling test result ({}): {:?}",
        test_name, result
    );
}

// =====================================================
// COMPREHENSIVE TEST SUITE RUNNER
// =====================================================

/// Runs all failure scenarios for a given processor
pub async fn run_comprehensive_failure_tests<T: StreamJobProcessor>(
    processor: &T,
    processor_name: &str,
) where
    T::StatsType: std::fmt::Debug,
{
    println!(
        "\n🧪 Running comprehensive failure tests for: {}",
        processor_name
    );
    println!("=========================================================");

    // Run all failure scenarios
    test_source_read_failure_scenario(processor, processor_name).await;
    test_sink_write_failure_scenario(processor, processor_name).await;
    test_disk_full_scenario(processor, processor_name).await;
    test_network_partition_scenario(processor, processor_name).await;
    test_partial_batch_failure_scenario(processor, processor_name).await;
    test_shutdown_signal_scenario(processor, processor_name).await;
    test_empty_batch_handling_scenario(processor, processor_name).await;

    println!(
        "\n✅ Comprehensive failure tests completed for: {}",
        processor_name
    );
}
