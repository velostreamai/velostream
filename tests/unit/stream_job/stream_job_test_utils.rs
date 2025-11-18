//! Shared test utilities for multi-job processor testing
//!
//! This module contains common mock implementations and helper functions
//! used across multiple processor test files to avoid duplication.

use async_trait::async_trait;
use std::collections::HashMap;
use velostream::velostream::datasource::{DataReader, DataWriter};
use velostream::velostream::server::processors::{
    common::*, simple::SimpleJobProcessor, transactional::TransactionalJobProcessor,
};
use velostream::velostream::sql::{
    ast::{SelectField, StreamSource, StreamingQuery},
    execution::types::{FieldValue, StreamRecord},
};

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
    async fn read(
        &mut self,
    ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
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

    async fn seek(
        &mut self,
        _offset: velostream::velostream::datasource::types::SourceOffset,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.current_batch < self.records.len())
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
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
    async fn write(
        &mut self,
        record: StreamRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.written_records.push(record);
        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<std::sync::Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Dereference Arc and clone for storage
        self.written_records
            .extend(records.iter().map(|r| (**r).clone()));
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
        if self.should_fail_flush {
            Err("Mock flush failure".into())
        } else {
            Ok(())
        }
    }

    async fn begin_transaction(
        &mut self,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
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
pub fn create_test_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "name".to_string(),
                FieldValue::String(format!("record_{}", i)),
            );
            fields.insert("value".to_string(), FieldValue::Float(i as f64 * 1.5));

            StreamRecord::new(fields)
        })
        .collect()
}

/// Helper function to create a simple test query
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
            job_mode: None,
            batch_size: None,
            num_partitions: None,
            partitioning_strategy: None,
    }
}

/// Helper function to create transactional processor with default config
pub fn create_transactional_processor() -> TransactionalJobProcessor {
    let config = JobProcessingConfig {
        use_transactions: true,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_retries: 3,
        retry_backoff: std::time::Duration::from_millis(100),
        ..Default::default()
    };
    TransactionalJobProcessor::new(config)
}

/// Helper function to create simple processor with default config
pub fn create_simple_processor() -> SimpleJobProcessor {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_retries: 3,
        retry_backoff: std::time::Duration::from_millis(100),
        empty_batch_count: 1, // Test config: exit immediately on empty batch
        wait_on_empty_batch_ms: 1000,
        enable_dlq: true,
        dlq_max_size: Some(100),
        ..Default::default()
    };
    SimpleJobProcessor::new(config)
}

/// Helper function to create conservative simple processor
pub fn create_conservative_simple_processor() -> SimpleJobProcessor {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::FailBatch,
        max_batch_size: 100,
        batch_timeout: std::time::Duration::from_millis(50),
        max_retries: 1,
        retry_backoff: std::time::Duration::from_millis(50),
        empty_batch_count: 1, // Test config: exit immediately on empty batch
        wait_on_empty_batch_ms: 1000,
        enable_dlq: true,
        dlq_max_size: Some(100),
        ..Default::default()
    };
    SimpleJobProcessor::new(config)
}

/// Helper function to create low latency processor
pub fn create_low_latency_processor() -> SimpleJobProcessor {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_batch_size: 10,
        batch_timeout: std::time::Duration::from_millis(10),
        max_retries: 2,
        retry_backoff: std::time::Duration::from_millis(25),
        empty_batch_count: 1, // Test config: exit immediately on empty batch
        wait_on_empty_batch_ms: 1000,
        enable_dlq: true,
        dlq_max_size: Some(100),
        ..Default::default()
    };
    SimpleJobProcessor::new(config)
}

/// Helper function to create throughput-focused processor
/// Uses higher empty_batch_count to allow for proper completion testing
pub fn create_throughput_processor() -> SimpleJobProcessor {
    let config = JobProcessingConfig {
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_retries: 3,
        retry_backoff: std::time::Duration::from_millis(100),
        empty_batch_count: 0, // Test config: exit immediately when sources exhausted
        wait_on_empty_batch_ms: 1000,
        enable_dlq: true,
        dlq_max_size: Some(100),
        ..Default::default()
    };
    SimpleJobProcessor::new(config)
}
