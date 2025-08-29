//! Multi-job SQL server support
//!
//! This module provides functionality for running multiple SQL jobs concurrently,
//! with support for different datasources and execution monitoring.

use crate::ferris::datasource::{
    config::{FileFormat, SourceConfig},
    file::FileDataSource,
    kafka::KafkaDataSource,
    DataReader, DataSource,
};
// InternalValue no longer needed - using FieldValue directly
// use crate::ferris::serialization::InternalValue;
use crate::ferris::sql::{
    query_analyzer::{DataSourceRequirement, DataSourceType},
    StreamExecutionEngine, StreamingQuery,
};
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

/// Configuration for creating a datasource
#[derive(Debug, Clone)]
pub struct DataSourceConfig {
    pub requirement: DataSourceRequirement,
    pub default_topic: String,
    pub job_name: String,
}

/// Result of datasource creation
pub type DataSourceResult = Result<Box<dyn DataReader>, String>;

/// Statistics for job execution
#[derive(Debug, Clone, Default)]
pub struct JobExecutionStats {
    pub records_processed: u64,
    pub start_time: Option<Instant>,
    pub errors: u64,
    pub last_record_time: Option<Instant>,
}

impl JobExecutionStats {
    pub fn new() -> Self {
        Self {
            start_time: Some(Instant::now()),
            ..Default::default()
        }
    }

    pub fn records_per_second(&self) -> f64 {
        if let Some(start) = self.start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return self.records_processed as f64 / elapsed;
            }
        }
        0.0
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time
            .map(|s| s.elapsed())
            .unwrap_or(Duration::from_secs(0))
    }
}

/// Extract Kafka configuration from datasource requirement
fn extract_kafka_config(
    props: &HashMap<String, String>,
    default_topic: &str,
    job_name: &str,
) -> SourceConfig {
    let brokers = props
        .get("brokers")
        .cloned()
        .unwrap_or_else(|| "localhost:9092".to_string());
    let topic = props
        .get("topic")
        .cloned()
        .unwrap_or_else(|| default_topic.to_string());
    let group_id = props
        .get("group_id")
        .cloned()
        .unwrap_or_else(|| format!("ferris-sql-{}", job_name));

    SourceConfig::Kafka {
        brokers,
        topic,
        group_id: Some(group_id),
        properties: props.clone(),
    }
}

/// Extract file configuration from datasource requirement
fn extract_file_config(props: &HashMap<String, String>) -> SourceConfig {
    let path = props
        .get("path")
        .cloned()
        .unwrap_or_else(|| "./demo_data/sample.csv".to_string());
    let format_str = props
        .get("format")
        .cloned()
        .unwrap_or_else(|| "csv".to_string());

    let format = parse_file_format(&format_str);

    SourceConfig::File {
        path,
        format,
        properties: props.clone(),
    }
}

/// Parse file format string into FileFormat enum
fn parse_file_format(format_str: &str) -> FileFormat {
    match format_str.to_lowercase().as_str() {
        "json" => FileFormat::Json,
        "parquet" => FileFormat::Parquet,
        _ => FileFormat::Csv {
            header: true,
            delimiter: ',',
            quote: '"',
        },
    }
}

/// Create a datasource reader based on configuration
pub async fn create_datasource_reader(config: &DataSourceConfig) -> DataSourceResult {
    let requirement = &config.requirement;

    match requirement.source_type {
        DataSourceType::Kafka => {
            create_kafka_reader(
                &requirement.properties,
                &config.default_topic,
                &config.job_name,
            )
            .await
        }
        DataSourceType::File => create_file_reader(&requirement.properties).await,
        _ => Err(format!(
            "Unsupported datasource type '{:?}'",
            requirement.source_type
        )),
    }
}

/// Create a Kafka datasource reader
async fn create_kafka_reader(
    props: &HashMap<String, String>,
    default_topic: &str,
    job_name: &str,
) -> DataSourceResult {
    let config = extract_kafka_config(props, default_topic, job_name);

    if let SourceConfig::Kafka {
        ref brokers,
        ref topic,
        ..
    } = config
    {
        let mut datasource = KafkaDataSource::new(brokers.clone(), topic.clone());
        datasource
            .initialize(config)
            .await
            .map_err(|e| format!("Failed to initialize Kafka datasource: {}", e))?;

        datasource
            .create_reader()
            .await
            .map_err(|e| format!("Failed to create Kafka reader: {}", e))
    } else {
        Err("Invalid Kafka configuration".to_string())
    }
}

/// Create a file datasource reader
async fn create_file_reader(props: &HashMap<String, String>) -> DataSourceResult {
    let config = extract_file_config(props);

    let mut datasource = FileDataSource::new();
    datasource
        .initialize(config)
        .await
        .map_err(|e| format!("Failed to initialize File datasource: {}", e))?;

    datasource
        .create_reader()
        .await
        .map_err(|e| format!("Failed to create File reader: {}", e))
}

/// Process records from a datasource
pub async fn process_datasource_records(
    mut reader: Box<dyn DataReader>,
    execution_engine: Arc<Mutex<StreamExecutionEngine>>,
    parsed_query: StreamingQuery,
    job_name: String,
    mut shutdown_receiver: mpsc::UnboundedReceiver<()>,
) -> JobExecutionStats {
    let mut stats = JobExecutionStats::new();

    loop {
        // Check for shutdown signal
        if shutdown_receiver.try_recv().is_ok() {
            info!("Job '{}' received shutdown signal", job_name);
            break;
        }

        // Process next record
        match process_single_record(
            &mut reader,
            &execution_engine,
            &parsed_query,
            &job_name,
            &mut stats,
        )
        .await
        {
            ProcessResult::Continue => continue,
            ProcessResult::NoData => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            ProcessResult::Error => {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }
    }

    log_final_stats(&job_name, &stats);
    stats
}

/// Result of processing a single record
#[derive(Debug)]
enum ProcessResult {
    Continue,
    NoData,
    Error,
}

/// Process a single record from the datasource
async fn process_single_record(
    reader: &mut Box<dyn DataReader>,
    execution_engine: &Arc<Mutex<StreamExecutionEngine>>,
    parsed_query: &StreamingQuery,
    job_name: &str,
    stats: &mut JobExecutionStats,
) -> ProcessResult {
    match reader.read().await {
        Ok(Some(record)) => {
            stats.records_processed += 1;
            stats.last_record_time = Some(Instant::now());

            // Execute the query directly with StreamRecord (most efficient)
            let mut engine = execution_engine.lock().await;
            if let Err(e) = engine
                .execute_with_record(parsed_query, record)
                .await
            {
                error!("Job '{}' failed to process record: {:?}", job_name, e);
                stats.errors += 1;
            }
            drop(engine);

            // Log progress periodically
            if stats.records_processed % 1000 == 0 {
                log_progress(job_name, stats);
            }

            ProcessResult::Continue
        }
        Ok(None) => ProcessResult::NoData,
        Err(e) => {
            warn!("Job '{}' error reading from datasource: {:?}", job_name, e);
            stats.errors += 1;
            ProcessResult::Error
        }
    }
}

/// Log progress information
fn log_progress(job_name: &str, stats: &JobExecutionStats) {
    let rps = stats.records_per_second();
    info!(
        "Job '{}': processed {} records ({:.2} records/sec)",
        job_name, stats.records_processed, rps
    );
}

/// Log final execution statistics
fn log_final_stats(job_name: &str, stats: &JobExecutionStats) {
    let elapsed = stats.elapsed().as_secs_f64();
    let rps = stats.records_per_second();
    info!(
        "Job '{}' completed: processed {} records in {:.2}s ({:.2} records/sec), {} errors",
        job_name, stats.records_processed, elapsed, rps, stats.errors
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_file_format() {
        assert!(matches!(parse_file_format("json"), FileFormat::Json));
        assert!(matches!(parse_file_format("JSON"), FileFormat::Json));
        assert!(matches!(parse_file_format("parquet"), FileFormat::Parquet));
        assert!(matches!(parse_file_format("csv"), FileFormat::Csv { .. }));
        assert!(matches!(
            parse_file_format("unknown"),
            FileFormat::Csv { .. }
        ));
    }

    #[test]
    fn test_extract_kafka_config() {
        let mut props = HashMap::new();
        props.insert("brokers".to_string(), "kafka:9092".to_string());
        props.insert("topic".to_string(), "events".to_string());

        let config = extract_kafka_config(&props, "default", "job1");

        if let SourceConfig::Kafka {
            brokers,
            topic,
            group_id,
            ..
        } = config
        {
            assert_eq!(brokers, "kafka:9092");
            assert_eq!(topic, "events");
            assert_eq!(group_id, Some("ferris-sql-job1".to_string()));
        } else {
            panic!("Expected Kafka config");
        }
    }

    #[test]
    fn test_extract_kafka_config_defaults() {
        let props = HashMap::new();
        let config = extract_kafka_config(&props, "default-topic", "test-job");

        if let SourceConfig::Kafka {
            brokers,
            topic,
            group_id,
            ..
        } = config
        {
            assert_eq!(brokers, "localhost:9092");
            assert_eq!(topic, "default-topic");
            assert_eq!(group_id, Some("ferris-sql-test-job".to_string()));
        } else {
            panic!("Expected Kafka config");
        }
    }

    #[test]
    fn test_extract_file_config() {
        let mut props = HashMap::new();
        props.insert("path".to_string(), "/data/file.json".to_string());
        props.insert("format".to_string(), "json".to_string());

        let config = extract_file_config(&props);

        if let SourceConfig::File { path, format, .. } = config {
            assert_eq!(path, "/data/file.json");
            assert!(matches!(format, FileFormat::Json));
        } else {
            panic!("Expected File config");
        }
    }

    #[test]
    fn test_job_execution_stats() {
        let mut stats = JobExecutionStats::new();
        assert_eq!(stats.records_processed, 0);
        assert_eq!(stats.errors, 0);
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
}
