use log::info;
use std::collections::HashMap;
use std::time::Duration;
use velostream::velostream::datasource::{
    kafka::writer::KafkaDataWriter, BatchConfig, BatchStrategy,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize env_logger to see the log output
    env_logger::init();

    info!("=== Testing KafkaDataWriter Batch Configuration ===\n");

    // Test 1: Direct batch configuration through KafkaDataWriter
    test_kafka_writer_with_batch_config().await?;

    // Test 2: Compression independence through KafkaDataWriter
    test_kafka_writer_compression_independence().await?;

    info!("\n=== KafkaDataWriter Batch Configuration Tests Complete ===");
    Ok(())
}

async fn test_kafka_writer_with_batch_config(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("🚀 Testing KafkaDataWriter with direct batch configuration");

    // Test 1: FixedSize strategy with explicit compression
    info!("\n1. Testing FixedSize strategy with explicit lz4 compression...");
    let mut props = HashMap::new();
    props.insert("compression.type".to_string(), "lz4".to_string()); // Explicit compression
                                                                     // Note: format is handled by KafkaDataWriter internally, no need to set it as a Kafka producer property

    let batch_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(200), // Would suggest snappy
        max_batch_size: 2000,
        batch_timeout: Duration::from_millis(3000),
    };

    let _writer = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test-writer-fixed".to_string(),
        &props,
        batch_config,
    )
    .await?;
    // Log should show compression.type: lz4 (explicit setting preserved)

    // Test 2: LowLatency strategy (should suggest "none" compression)
    info!("\n2. Testing LowLatency strategy with no explicit compression...");
    let props_low_latency = HashMap::new(); // No compression set

    let low_latency_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::LowLatency {
            max_batch_size: 1,
            max_wait_time: Duration::from_millis(1),
            eager_processing: true,
        },
        max_batch_size: 5,
        batch_timeout: Duration::from_millis(50),
    };

    let _writer_ll = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test-writer-low-latency".to_string(),
        &props_low_latency,
        low_latency_config,
    )
    .await?;
    // Log should show compression.type: none (suggested by LowLatency strategy)

    // Test 3: MemoryBased strategy (should suggest gzip compression)
    info!("\n3. Testing MemoryBased strategy with no explicit compression...");
    let props_memory = HashMap::new(); // No compression set

    let memory_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(2 * 1024 * 1024), // 2MB
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(15000),
    };

    let _writer_mem = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test-writer-memory".to_string(),
        &props_memory,
        memory_config,
    )
    .await?;
    // Log should show compression.type: gzip (suggested by MemoryBased strategy)

    Ok(())
}

async fn test_kafka_writer_compression_independence(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("\n🎯 Testing KafkaDataWriter compression independence");

    // Test: Explicit gzip compression should not be overridden by TimeWindow strategy
    info!(
        "\n1. Testing explicit gzip compression with TimeWindow strategy (should remain gzip)..."
    );
    let mut props = HashMap::new();
    props.insert("compression.type".to_string(), "gzip".to_string()); // Explicit compression
                                                                      // Note: format is handled by KafkaDataWriter internally

    let time_window_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::TimeWindow(Duration::from_millis(2000)), // Would suggest lz4
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(10000),
    };

    let _writer = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test-writer-gzip".to_string(),
        &props,
        time_window_config,
    )
    .await?;
    // Log should show compression.type: gzip (explicit setting preserved)

    // Test: Compare with same strategy but no explicit compression
    info!("\n2. Testing same TimeWindow strategy with no explicit compression (should get lz4)...");
    let props_auto = HashMap::new(); // No explicit compression

    let time_window_config_auto = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::TimeWindow(Duration::from_millis(2000)), // Should suggest lz4
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(10000),
    };

    let _writer_auto = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test-writer-auto-lz4".to_string(),
        &props_auto,
        time_window_config_auto,
    )
    .await?;
    // Log should show compression.type: lz4 (suggested by TimeWindow strategy)

    Ok(())
}
