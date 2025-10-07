use log::info;
use std::time::Duration;
use velostream::velostream::datasource::kafka::reader::KafkaDataReader;
use velostream::velostream::datasource::{BatchConfig, BatchStrategy};
use velostream::velostream::kafka::serialization_format::SerializationFormat;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize env_logger to see the log output
    env_logger::init();

    info!("=== Testing Kafka Consumer Configuration Logging ===\n");

    // Test 1: FixedSize batch strategy
    info!("1. Testing FixedSize batch strategy (100 records)...");
    let fixed_size_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(100),
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(5000),
    };

    let _fixed_reader = KafkaDataReader::new_with_schema(
        "localhost:9092",
        "test-topic".to_string(),
        "test-group-fixed",
        SerializationFormat::Json,
        Some(fixed_size_config),
        None,
        None, // event_time_config
    )
    .await;

    info!("\n2. Testing LowLatency batch strategy (eager processing)...");
    let low_latency_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::LowLatency {
            max_batch_size: 5,
            max_wait_time: Duration::from_millis(1),
            eager_processing: true,
        },
        max_batch_size: 10,
        batch_timeout: Duration::from_millis(100),
    };

    let _low_latency_reader = KafkaDataReader::new_with_schema(
        "localhost:9092",
        "test-topic".to_string(),
        "test-group-low-latency",
        SerializationFormat::Json,
        Some(low_latency_config),
        None,
        None, // event_time_config
    )
    .await;

    info!("\n3. Testing AdaptiveSize batch strategy...");
    let adaptive_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::AdaptiveSize {
            min_size: 10,
            max_size: 200,
            target_latency: Duration::from_millis(50),
        },
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(2000),
    };

    let _adaptive_reader = KafkaDataReader::new_with_schema(
        "localhost:9092",
        "test-topic".to_string(),
        "test-group-adaptive",
        SerializationFormat::Json,
        Some(adaptive_config),
        None,
        None, // event_time_config
    )
    .await;

    info!("\n4. Testing MemoryBased batch strategy...");
    let memory_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(1024 * 1024), // 1MB
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(10000),
    };

    let _memory_reader = KafkaDataReader::new_with_schema(
        "localhost:9092",
        "test-topic".to_string(),
        "test-group-memory",
        SerializationFormat::Json,
        Some(memory_config),
        None,
        None, // event_time_config
    )
    .await;

    info!("\n=== Configuration Logging Test Complete ===");
    Ok(())
}
