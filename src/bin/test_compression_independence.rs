use ferrisstreams::ferris::datasource::{
    config::{types::CompressionType, FileFormat, SinkConfig},
    file::data_sink::FileDataSink,
    kafka::data_sink::KafkaDataSink,
    traits::DataSink,
    BatchConfig, BatchStrategy,
};
use log::info;
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize env_logger to see the log output
    env_logger::init();

    info!("=== Testing Compression Independence ===\n");

    // Test Kafka Sink with explicit compression settings
    test_kafka_explicit_compression().await?;

    // Test File Sink with explicit compression settings
    test_file_explicit_compression().await?;

    info!("\n=== Compression Independence Tests Complete ===");
    Ok(())
}

async fn test_kafka_explicit_compression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("🔧 Testing Kafka Sink with explicit compression settings");

    // Test 1: Explicit gzip compression should not be overridden by FixedSize strategy (which suggests snappy)
    info!("\n1. Testing explicit gzip compression with FixedSize strategy (should remain gzip)...");
    let mut explicit_gzip_props = HashMap::new();
    explicit_gzip_props.insert("compression.type".to_string(), "gzip".to_string());

    let mut kafka_sink = KafkaDataSink::from_properties(&explicit_gzip_props, "test-job");

    let sink_config = SinkConfig::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "test-explicit-gzip".to_string(),
        properties: explicit_gzip_props,
    };

    kafka_sink.initialize(sink_config).await?;

    let fixed_size_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(100), // This would suggest snappy
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(5000),
    };

    let _writer = kafka_sink
        .create_writer_with_batch_config(fixed_size_config)
        .await?;
    // The log output should show compression.type: gzip (explicit setting preserved)

    // Test 2: Explicit none compression should not be overridden by MemoryBased strategy (which suggests gzip)
    info!("\n2. Testing explicit 'none' compression with MemoryBased strategy (should remain none)...");
    let mut explicit_none_props = HashMap::new();
    explicit_none_props.insert("compression.type".to_string(), "none".to_string());

    let mut kafka_sink_none = KafkaDataSink::from_properties(&explicit_none_props, "test-job");

    let sink_config_none = SinkConfig::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "test-explicit-none".to_string(),
        properties: explicit_none_props,
    };

    kafka_sink_none.initialize(sink_config_none).await?;

    let memory_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(2 * 1024 * 1024), // This would suggest gzip
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(10000),
    };

    let _writer = kafka_sink_none
        .create_writer_with_batch_config(memory_config)
        .await?;
    // The log output should show compression.type: none (explicit setting preserved)

    Ok(())
}

async fn test_file_explicit_compression() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("\n🗂️  Testing File Sink with explicit compression settings");

    // Test 1: Explicit compression should not be overridden by MemoryBased strategy
    info!(
        "\n1. Testing explicit compression with MemoryBased strategy (should remain explicit)..."
    );

    let mut file_sink = FileDataSink::new();

    let sink_config = SinkConfig::File {
        path: "./test_explicit_compression.json".to_string(),
        format: FileFormat::Json,
        properties: HashMap::new(),
        compression: Some(CompressionType::Zstd), // Explicit compression setting
    };

    file_sink.initialize(sink_config).await?;

    let memory_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(2 * 1024 * 1024), // This would suggest gzip if not set
        max_batch_size: 10000,
        batch_timeout: Duration::from_millis(30000),
    };

    let _writer = file_sink
        .create_writer_with_batch_config(memory_config)
        .await?;
    // The log output should show compression: Zstd (explicit setting preserved)

    // Test 2: No explicit compression with MemoryBased strategy should get suggested compression
    info!("\n2. Testing no explicit compression with MemoryBased strategy (should get suggested gzip)...");

    let mut file_sink_auto = FileDataSink::new();

    let sink_config_auto = SinkConfig::File {
        path: "./test_auto_compression.json".to_string(),
        format: FileFormat::Json,
        properties: HashMap::new(),
        compression: None, // No explicit compression setting
    };

    file_sink_auto.initialize(sink_config_auto).await?;

    let memory_config_auto = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(2 * 1024 * 1024), // This would suggest gzip if not set
        max_batch_size: 10000,
        batch_timeout: Duration::from_millis(30000),
    };

    let _writer = file_sink_auto
        .create_writer_with_batch_config(memory_config_auto)
        .await?;
    // The log output should show compression: Gzip (suggested by batch strategy)

    Ok(())
}
