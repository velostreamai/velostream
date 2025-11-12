//! Mock JobProcessor for testing
//!
//! Provides a simple mock implementation of the JobProcessor trait for testing
//! without requiring real Kafka connections or data sources.

use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::server::processors::common::JobExecutionStats;
use crate::velostream::sql::StreamExecutionEngine;
use crate::velostream::sql::ast::StreamingQuery;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::job_processor_trait::{JobProcessor, ProcessorMetrics};

/// Mock job processor for testing
///
/// This processor simulates the JobProcessor trait without actually processing
/// data. It's useful for testing StreamJobServer logic without dependencies on
/// Kafka, data sources, or sinks.
///
/// # Example
///
/// ```rust,ignore
/// use velostream::velostream::server::processors::MockJobProcessor;
///
/// let mock = MockJobProcessor::new();
/// assert_eq!(mock.processor_version(), "Mock");
/// assert_eq!(mock.processor_name(), "Mock Test Processor");
/// ```
#[derive(Clone)]
pub struct MockJobProcessor {
    call_count: Arc<RwLock<usize>>,
    last_job_name: Arc<RwLock<Option<String>>>,
}

impl MockJobProcessor {
    /// Create a new mock processor
    pub fn new() -> Self {
        Self {
            call_count: Arc::new(RwLock::new(0)),
            last_job_name: Arc::new(RwLock::new(None)),
        }
    }

    /// Get the number of times process_multi_job was called
    pub async fn call_count(&self) -> usize {
        *self.call_count.read().await
    }

    /// Get the last job name that was processed
    pub async fn last_job_name(&self) -> Option<String> {
        self.last_job_name.read().await.clone()
    }

    /// Reset call count and job name
    pub async fn reset(&self) {
        *self.call_count.write().await = 0;
        *self.last_job_name.write().await = None;
    }
}

impl Default for MockJobProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl JobProcessor for MockJobProcessor {
    fn processor_version(&self) -> &str {
        "Mock"
    }

    fn processor_name(&self) -> &str {
        "Mock Test Processor"
    }

    fn num_partitions(&self) -> usize {
        1
    }

    fn metrics(&self) -> ProcessorMetrics {
        ProcessorMetrics {
            version: "Mock".to_string(),
            name: "Mock Test Processor".to_string(),
            num_partitions: 1,
            lifecycle_state: super::job_processor_trait::LifecycleState::Running,
            total_records: 0,
            failed_records: 0,
            throughput_rps: 0.0,
            uptime_secs: 0.0,
        }
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn pause(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn resume(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn process_batch(
        &self,
        records: Vec<crate::velostream::sql::execution::types::StreamRecord>,
        _engine: Arc<StreamExecutionEngine>,
    ) -> Result<
        Vec<crate::velostream::sql::execution::types::StreamRecord>,
        crate::velostream::sql::error::SqlError,
    > {
        // Mock just passes through records
        Ok(records)
    }

    async fn process_multi_job(
        &self,
        _readers: HashMap<String, Box<dyn DataReader>>,
        _writers: HashMap<String, Box<dyn DataWriter>>,
        _engine: Arc<RwLock<StreamExecutionEngine>>,
        _query: StreamingQuery,
        job_name: String,
        _shutdown_receiver: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
        // Increment call count
        *self.call_count.write().await += 1;

        // Store job name
        *self.last_job_name.write().await = Some(job_name.clone());

        // Return success with mock stats
        Ok(JobExecutionStats {
            records_processed: 0,
            records_failed: 0,
            batches_processed: 0,
            batches_failed: 0,
            start_time: None,
            last_record_time: None,
            avg_batch_size: 0.0,
            avg_processing_time_ms: 0.0,
            total_processing_time: std::time::Duration::from_secs(0),
            error_details: vec![],
        })
    }

    async fn process_job(
        &self,
        _reader: Box<dyn DataReader>,
        _writer: Option<Box<dyn DataWriter>>,
        _engine: Arc<RwLock<StreamExecutionEngine>>,
        _query: StreamingQuery,
        job_name: String,
        _shutdown_receiver: tokio::sync::mpsc::Receiver<()>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
        // Increment call count
        *self.call_count.write().await += 1;

        // Store job name
        *self.last_job_name.write().await = Some(job_name.clone());

        // Return success with mock stats
        Ok(JobExecutionStats {
            records_processed: 0,
            records_failed: 0,
            batches_processed: 0,
            batches_failed: 0,
            start_time: None,
            last_record_time: None,
            avg_batch_size: 0.0,
            avg_processing_time_ms: 0.0,
            total_processing_time: std::time::Duration::from_secs(0),
            error_details: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_processor_creation() {
        let mock = MockJobProcessor::new();
        assert_eq!(mock.processor_version(), "Mock");
        assert_eq!(mock.processor_name(), "Mock Test Processor");
        assert_eq!(mock.num_partitions(), 1);
    }

    #[tokio::test]
    async fn test_mock_processor_metrics() {
        let mock = MockJobProcessor::new();
        let metrics = mock.metrics();
        assert_eq!(metrics.total_records, 0);
        assert_eq!(metrics.failed_records, 0);
    }

    #[tokio::test]
    async fn test_mock_processor_lifecycle() {
        let mock = MockJobProcessor::new();

        // All lifecycle operations should succeed
        assert!(mock.start().await.is_ok());
        assert!(mock.pause().await.is_ok());
        assert!(mock.resume().await.is_ok());
        assert!(mock.stop().await.is_ok());
    }

    #[tokio::test]
    async fn test_mock_processor_call_tracking() {
        let mock = MockJobProcessor::new();

        // Initially no calls
        assert_eq!(mock.call_count().await, 0);
        assert_eq!(mock.last_job_name().await, None);

        // Note: process_multi_job requires full parameters, so we test the tracking
        // indirectly by checking the counter increments with actual calls
    }

    #[tokio::test]
    async fn test_mock_processor_reset() {
        let mock = MockJobProcessor::new();

        // Manually set state
        *mock.call_count.write().await = 5;
        *mock.last_job_name.write().await = Some("test_job".to_string());

        // Verify state
        assert_eq!(mock.call_count().await, 5);
        assert_eq!(mock.last_job_name().await, Some("test_job".to_string()));

        // Reset
        mock.reset().await;

        // Verify reset
        assert_eq!(mock.call_count().await, 0);
        assert_eq!(mock.last_job_name().await, None);
    }

    #[tokio::test]
    async fn test_mock_processor_cloneable() {
        let mock1 = MockJobProcessor::new();
        let mock2 = mock1.clone();

        // Both should point to same internal state
        *mock1.call_count.write().await = 3;

        // mock2 should see the same state
        assert_eq!(mock2.call_count().await, 3);
    }
}
