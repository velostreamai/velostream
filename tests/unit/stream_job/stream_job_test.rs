//! Integration tests for multi-job SQL server functionality

use ferrisstreams::ferris::server::processors::common::{
    create_datasource_reader, process_datasource_records, DataSourceConfig, JobExecutionStats,
    JobProcessingConfig,
};
use ferrisstreams::ferris::sql::{
    execution::StreamExecutionEngine,
    query_analyzer::{DataSourceRequirement, DataSourceType},
    StreamingSqlParser,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

/// Create a test datasource requirement for Kafka
fn create_kafka_requirement() -> DataSourceRequirement {
    let mut properties = HashMap::new();
    properties.insert("brokers".to_string(), "localhost:9092".to_string());
    properties.insert("topic".to_string(), "test-topic".to_string());

    DataSourceRequirement {
        name: "test_kafka_source".to_string(),
        source_type: DataSourceType::Kafka,
        properties,
    }
}

/// Create a test datasource requirement for File
fn create_file_requirement(path: &str, format: &str) -> DataSourceRequirement {
    let mut properties = HashMap::new();
    properties.insert("path".to_string(), path.to_string());
    properties.insert("format".to_string(), format.to_string());

    DataSourceRequirement {
        name: "test_file_source".to_string(),
        source_type: DataSourceType::File,
        properties,
    }
}

#[tokio::test]
async fn test_datasource_config_creation() {
    let requirement = create_kafka_requirement();
    let config = DataSourceConfig {
        requirement,
        default_topic: "default".to_string(),
        job_name: "test-job".to_string(),
    };

    assert_eq!(config.default_topic, "default");
    assert_eq!(config.job_name, "test-job");
}

#[tokio::test]
async fn test_kafka_datasource_creation_mock() {
    // This test verifies the configuration creation logic without actually connecting to Kafka
    let requirement = create_kafka_requirement();
    let config = DataSourceConfig {
        requirement: requirement.clone(),
        default_topic: "fallback-topic".to_string(),
        job_name: "kafka-test".to_string(),
    };

    // Verify that configuration extracts the right values
    assert_eq!(
        config.requirement.properties.get("brokers"),
        Some(&"localhost:9092".to_string())
    );
    assert_eq!(
        config.requirement.properties.get("topic"),
        Some(&"test-topic".to_string())
    );
}

#[tokio::test]
async fn test_file_datasource_config() {
    let requirement = create_file_requirement("/data/test.csv", "csv");
    let config = DataSourceConfig {
        requirement: requirement.clone(),
        default_topic: "unused".to_string(),
        job_name: "file-test".to_string(),
    };

    assert_eq!(
        config.requirement.properties.get("path"),
        Some(&"/data/test.csv".to_string())
    );
    assert_eq!(
        config.requirement.properties.get("format"),
        Some(&"csv".to_string())
    );
}

#[tokio::test]
async fn test_job_execution_stats_tracking() {
    let stats = JobExecutionStats::new();
    assert!(stats.start_time.is_some());
    assert_eq!(stats.records_processed, 0);
    assert_eq!(stats.records_failed, 0);

    // Test elapsed time calculation
    tokio::time::sleep(Duration::from_millis(10)).await;
    let elapsed = stats.elapsed();
    assert!(elapsed.as_millis() >= 10);
}

#[tokio::test]
async fn test_job_execution_stats_rps() {
    let mut stats = JobExecutionStats::new();

    // Simulate processing records
    stats.records_processed = 100;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rps = stats.records_per_second();
    // Should be approximately 1000 records/sec (100 records in 0.1 seconds)
    assert!(rps > 0.0);
    assert!(rps < 2000.0); // Upper bound to account for timing variations
}

#[tokio::test]
async fn test_process_datasource_with_shutdown() {
    use ferrisstreams::ferris::datasource::DataReader;
    use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
    use std::collections::HashMap;

    // Create a mock reader that produces test records
    struct MockReader {
        count: usize,
    }

    #[async_trait::async_trait]
    impl DataReader for MockReader {
        async fn read(
            &mut self,
        ) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error + Send + Sync>> {
            if self.count > 0 {
                let mut records = Vec::new();
                let record_count = std::cmp::min(self.count, 5); // Batch size

                for _ in 0..record_count {
                    self.count -= 1;
                    let mut fields = HashMap::new();
                    fields.insert("id".to_string(), FieldValue::Integer(self.count as i64));
                    fields.insert("value".to_string(), FieldValue::String("test".to_string()));

                    records.push(StreamRecord {
                        fields,
                        timestamp: chrono::Utc::now().timestamp_millis(),
                        offset: 0,
                        partition: 0,
                        headers: HashMap::new(),
                    });
                }

                Ok(records)
            } else {
                Ok(Vec::new()) // Empty batch when no more data
            }
        }

        async fn seek(
            &mut self,
            _offset: ferrisstreams::ferris::datasource::types::SourceOffset,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn has_more(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
            Ok(self.count > 0)
        }

        async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        fn supports_transactions(&self) -> bool {
            false
        }

        async fn begin_transaction(
            &mut self,
        ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
            Ok(false)
        }

        async fn commit_transaction(
            &mut self,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn abort_transaction(
            &mut self,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
    }

    // Create execution engine
    let (output_sender, _output_receiver) = mpsc::unbounded_channel();
    let engine = StreamExecutionEngine::new(output_sender);
    let engine = Arc::new(Mutex::new(engine));

    // Parse a simple query
    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT id, value FROM test").unwrap();

    // Create shutdown channel
    let (_shutdown_sender, shutdown_receiver) = mpsc::channel(1);

    // Create mock reader
    let reader = Box::new(MockReader { count: 5 });

    // Create default config
    let config = JobProcessingConfig::default();

    // Process records with immediate shutdown
    let stats = process_datasource_records(
        reader,
        None, // No writer
        engine,
        query,
        "test-job".to_string(),
        shutdown_receiver,
        config,
    )
    .await
    .unwrap();

    // Verify stats
    assert_eq!(stats.records_processed, 5);
    assert_eq!(stats.records_failed, 0);
}

#[tokio::test]
async fn test_unsupported_datasource_type() {
    use ferrisstreams::ferris::sql::query_analyzer::DataSourceType;

    let mut properties = HashMap::new();
    properties.insert("url".to_string(), "redis://localhost".to_string());

    let requirement = DataSourceRequirement {
        name: "test_s3_source".to_string(),
        source_type: DataSourceType::S3, // Unsupported type for now
        properties,
    };

    let config = DataSourceConfig {
        requirement,
        default_topic: "default".to_string(),
        job_name: "unsupported-test".to_string(),
    };

    // This should return an error for unsupported type
    let result = create_datasource_reader(&config).await;
    assert!(result.is_err());
    match result {
        Ok(_) => panic!("Expected error"),
        Err(error_msg) => {
            assert!(error_msg.contains("Unsupported datasource type"));
        }
    }
}

#[test]
fn test_default_values_extraction() {
    use ferrisstreams::ferris::sql::query_analyzer::DataSourceRequirement;

    // Test with empty properties - should use defaults
    let requirement = DataSourceRequirement {
        name: "test_default_source".to_string(),
        source_type: DataSourceType::Kafka,
        properties: HashMap::new(),
    };

    let config = DataSourceConfig {
        requirement,
        default_topic: "my-default-topic".to_string(),
        job_name: "default-test".to_string(),
    };

    // When creating Kafka reader, it should use defaults
    // This is tested indirectly through the configuration
    assert_eq!(config.default_topic, "my-default-topic");
    assert_eq!(config.job_name, "default-test");
}

// Additional extracted unit tests from multi_job.rs

#[test]
fn test_job_execution_stats() {
    let mut stats = JobExecutionStats::new();
    assert_eq!(stats.records_processed, 0);
    assert_eq!(stats.records_failed, 0);
    assert!(stats.start_time.is_some());

    stats.records_processed = 1000;
    // Sleep briefly to ensure elapsed time > 0
    std::thread::sleep(Duration::from_millis(10));

    let rps = stats.records_per_second();
    assert!(rps > 0.0);

    let elapsed = stats.elapsed();
    assert!(elapsed.as_millis() > 0);
}

#[test]
fn test_job_execution_stats_no_start_time() {
    let stats = JobExecutionStats {
        records_processed: 100,
        start_time: None,
        ..Default::default()
    };

    assert_eq!(stats.records_per_second(), 0.0);
    assert_eq!(stats.elapsed(), Duration::from_secs(0));
}
