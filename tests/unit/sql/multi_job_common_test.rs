//! Tests for multi-job common functionality

use ferrisstreams::ferris::sql::{
    execution::types::{FieldValue, StreamRecord},
    multi_job_common::*,
    StreamExecutionEngine,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Helper function to create test records
fn create_test_record(id: i64, name: &str, value: f64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("name".to_string(), FieldValue::String(name.to_string()));
    fields.insert("value".to_string(), FieldValue::Float(value));

    StreamRecord::new(fields)
}

/// Helper function to create a simple test query
fn create_simple_query() -> ferrisstreams::ferris::sql::ast::StreamingQuery {
    use ferrisstreams::ferris::sql::ast::{SelectField, StreamSource, StreamingQuery};

    StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_stream".to_string()),
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
async fn test_job_execution_stats_creation() {
    let stats = JobExecutionStats::new();

    assert_eq!(stats.records_processed, 0);
    assert_eq!(stats.records_failed, 0);
    assert_eq!(stats.batches_processed, 0);
    assert_eq!(stats.batches_failed, 0);
    assert!(stats.start_time.is_some());
    assert_eq!(stats.avg_batch_size, 0.0);
    assert_eq!(stats.avg_processing_time_ms, 0.0);
}

#[tokio::test]
async fn test_job_execution_stats_update() {
    let mut stats = JobExecutionStats::new();

    // Create first batch result
    let batch_result1 = BatchProcessingResult {
        records_processed: 10,
        records_failed: 2,
        processing_time: Duration::from_millis(100),
        batch_size: 12,
        error_details: vec![
            ProcessingError {
                record_index: 5,
                error_message: "Test error 1".to_string(),
                recoverable: true,
            },
            ProcessingError {
                record_index: 8,
                error_message: "Test error 2".to_string(),
                recoverable: false,
            },
        ],
    };

    stats.update_from_batch(&batch_result1);

    assert_eq!(stats.records_processed, 10);
    assert_eq!(stats.records_failed, 2);
    assert_eq!(stats.batches_processed, 0); // Failed batch
    assert_eq!(stats.batches_failed, 1);
    assert_eq!(stats.avg_batch_size, 12.0);
    assert_eq!(stats.avg_processing_time_ms, 100.0);

    // Create second batch result (successful)
    let batch_result2 = BatchProcessingResult {
        records_processed: 8,
        records_failed: 0,
        processing_time: Duration::from_millis(50),
        batch_size: 8,
        error_details: vec![],
    };

    stats.update_from_batch(&batch_result2);

    assert_eq!(stats.records_processed, 18);
    assert_eq!(stats.records_failed, 2);
    assert_eq!(stats.batches_processed, 1); // This batch succeeded
    assert_eq!(stats.batches_failed, 1);
    assert_eq!(stats.avg_batch_size, 10.0); // (12 + 8) / 2
    assert_eq!(stats.avg_processing_time_ms, 75.0); // (100 + 50) / 2
}

#[tokio::test]
async fn test_job_execution_stats_metrics() {
    let mut stats = JobExecutionStats::new();

    // Wait a bit to get some elapsed time
    tokio::time::sleep(Duration::from_millis(100)).await;

    stats.records_processed = 1000;
    stats.records_failed = 50;

    let rps = stats.records_per_second();
    let success_rate = stats.success_rate();
    let elapsed = stats.elapsed();

    assert!(rps > 0.0, "Should have positive records per second");
    assert!(success_rate > 90.0, "Should have high success rate"); // 1000/(1000+50) ≈ 95.2%
    assert!(success_rate < 100.0, "Should not be 100% due to failures");
    assert!(
        elapsed >= Duration::from_millis(100),
        "Should have elapsed time"
    );

    println!("Records per second: {:.2}", rps);
    println!("Success rate: {:.1}%", success_rate);
    println!("Elapsed time: {:?}", elapsed);
}

#[tokio::test]
async fn test_batch_processing_result_creation() {
    let errors = vec![
        ProcessingError {
            record_index: 0,
            error_message: "Validation failed".to_string(),
            recoverable: true,
        },
        ProcessingError {
            record_index: 5,
            error_message: "Parse error".to_string(),
            recoverable: false,
        },
    ];

    let result = BatchProcessingResult {
        records_processed: 8,
        records_failed: 2,
        processing_time: Duration::from_millis(75),
        batch_size: 10,
        error_details: errors.clone(),
    };

    assert_eq!(result.records_processed, 8);
    assert_eq!(result.records_failed, 2);
    assert_eq!(result.processing_time, Duration::from_millis(75));
    assert_eq!(result.batch_size, 10);
    assert_eq!(result.error_details.len(), 2);
    assert_eq!(result.error_details[0].record_index, 0);
    assert_eq!(result.error_details[1].recoverable, false);
}

#[tokio::test]
async fn test_process_batch_common_success() {
    // Create test data
    let records = vec![
        create_test_record(1, "alice", 100.0),
        create_test_record(2, "bob", 200.0),
        create_test_record(3, "charlie", 300.0),
    ];

    let (output_sender, _output_receiver) = tokio::sync::mpsc::unbounded_channel();
    let engine = Arc::new(Mutex::new(StreamExecutionEngine::new(output_sender)));
    let query = create_simple_query();
    let job_name = "test_batch_processing";

    let result = process_batch_common(records, &engine, &query, job_name).await;

    assert_eq!(result.batch_size, 3);
    assert_eq!(result.records_processed, 3);
    assert_eq!(result.records_failed, 0);
    assert!(result.processing_time > Duration::from_nanos(0));
    assert!(result.error_details.is_empty());
}

#[test]
fn test_job_processing_config_default() {
    let config = JobProcessingConfig::default();

    assert_eq!(config.max_batch_size, 100);
    assert_eq!(config.batch_timeout, Duration::from_millis(1000));
    assert!(!config.use_transactions);
    assert_eq!(config.failure_strategy, FailureStrategy::LogAndContinue);
    assert_eq!(config.max_retries, 3);
    assert_eq!(config.retry_backoff, Duration::from_millis(1000));
    assert!(config.log_progress);
    assert_eq!(config.progress_interval, 10);
}

#[test]
fn test_failure_strategy_variants() {
    let strategies = [
        FailureStrategy::LogAndContinue,
        FailureStrategy::SendToDLQ,
        FailureStrategy::FailBatch,
        FailureStrategy::RetryWithBackoff,
    ];

    for strategy in &strategies {
        let config = JobProcessingConfig {
            failure_strategy: *strategy,
            ..Default::default()
        };

        assert_eq!(config.failure_strategy, *strategy);
    }
}

#[test]
fn test_processing_error_creation() {
    let error = ProcessingError {
        record_index: 42,
        error_message: "Field validation failed: missing required field 'amount'".to_string(),
        recoverable: true,
    };

    assert_eq!(error.record_index, 42);
    assert!(error.error_message.contains("validation failed"));
    assert!(error.recoverable);
}

#[tokio::test]
async fn test_log_functions() {
    // Test logging functions don't panic
    let mut stats = JobExecutionStats::new();
    stats.records_processed = 1000;
    stats.records_failed = 50;
    stats.batches_processed = 10;
    stats.batches_failed = 2;
    stats.avg_batch_size = 100.0;
    stats.avg_processing_time_ms = 25.5;

    // These should not panic
    log_job_progress("test_job", &stats);
    log_final_stats("test_job", &stats);
}

#[tokio::test]
async fn test_retry_with_backoff_success() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let attempt_count = Arc::new(AtomicUsize::new(0));

    let result = retry_with_backoff(
        {
            let attempt_count = attempt_count.clone();
            move || {
                let count = attempt_count.fetch_add(1, Ordering::SeqCst) + 1;
                Box::pin(async move {
                    if count == 1 {
                        Err("First attempt fails".into())
                    } else {
                        Ok("Success on second attempt")
                    }
                })
            }
        },
        3,                        // max_retries
        Duration::from_millis(1), // Very short backoff for testing
        "test_job",
        "test_operation",
    )
    .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Success on second attempt");
    assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_retry_with_backoff_max_retries_exceeded() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let attempt_count = Arc::new(AtomicUsize::new(0));

    let result = retry_with_backoff(
        {
            let attempt_count = attempt_count.clone();
            move || {
                let count = attempt_count.fetch_add(1, Ordering::SeqCst) + 1;
                Box::pin(async move {
                    Err(format!("Attempt {} failed", count).into()) as DataSourceResult<&str>
                })
            }
        },
        2,                        // max_retries
        Duration::from_millis(1), // Very short backoff for testing
        "test_job",
        "failing_operation",
    )
    .await;

    assert!(result.is_err());
    assert_eq!(attempt_count.load(Ordering::SeqCst), 3); // Original attempt + 2 retries
    assert!(result.unwrap_err().to_string().contains("Attempt 3 failed"));
}

#[test]
fn test_check_transaction_support_logging() {
    // Mock reader that supports transactions
    struct MockTransactionalReader;
    #[async_trait::async_trait]
    impl ferrisstreams::ferris::datasource::DataReader for MockTransactionalReader {
        fn supports_transactions(&self) -> bool {
            true
        }

        async fn read(
            &mut self,
        ) -> Result<
            Vec<ferrisstreams::ferris::sql::execution::types::StreamRecord>,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            Ok(vec![])
        }

        async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn seek(
            &mut self,
            _offset: ferrisstreams::ferris::datasource::types::SourceOffset,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
            Ok(false)
        }
    }

    // Mock reader that doesn't support transactions
    struct MockNonTransactionalReader;
    #[async_trait::async_trait]
    impl ferrisstreams::ferris::datasource::DataReader for MockNonTransactionalReader {
        fn supports_transactions(&self) -> bool {
            false
        }

        async fn read(
            &mut self,
        ) -> Result<
            Vec<ferrisstreams::ferris::sql::execution::types::StreamRecord>,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            Ok(vec![])
        }

        async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn seek(
            &mut self,
            _offset: ferrisstreams::ferris::datasource::types::SourceOffset,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
            Ok(false)
        }
    }

    // These should not panic and should log appropriately
    let tx_reader = MockTransactionalReader;
    let non_tx_reader = MockNonTransactionalReader;

    assert!(check_transaction_support(&tx_reader, "tx_job"));
    assert!(!check_transaction_support(&non_tx_reader, "non_tx_job"));
}
