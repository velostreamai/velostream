use ferrisstreams::ferris::datasource::{
    config::{FileFormat, SinkConfig},
    file::data_sink::FileSink,
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

    info!("=== Testing FileSink Batch Config Does NOT Override User Properties ===\n");

    // Test that explicit user properties are never overridden by batch config
    test_file_user_properties_not_overridden().await?;

    info!("\n=== FileSink Batch Config Override Prevention Tests Complete ===");
    Ok(())
}

async fn test_file_user_properties_not_overridden(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("🔒 Testing that explicit file user properties are never overridden");

    // Test 1: User sets custom buffer_size_bytes, batch config should not override it
    info!("\n1. Testing explicit buffer_size_bytes with FixedSize strategy...");
    let mut props = HashMap::new();
    props.insert("buffer_size_bytes".to_string(), "1048576".to_string()); // User wants 1MB buffer

    let mut file_sink = FileSink::new();

    let sink_config = SinkConfig::File {
        path: "./test_no_override_1.json".to_string(),
        format: FileFormat::Json,
        properties: props,
        compression: None,
    };

    file_sink.initialize(sink_config).await?;

    let batch_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(100), // Would suggest different buffer size
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(5000),
    };

    let _writer = file_sink
        .create_writer_with_batch_config(batch_config)
        .await?;
    // Log should show: buffer_size_bytes: 1048576 (user's explicit setting, not batch config suggestion)

    // Test 2: User sets custom buffer_size_bytes, MemoryBased strategy should not override it
    info!("\n2. Testing explicit buffer_size_bytes with MemoryBased strategy...");
    let mut props_mem = HashMap::new();
    props_mem.insert("buffer_size_bytes".to_string(), "2097152".to_string()); // User wants 2MB buffer
    props_mem.insert("compression".to_string(), "gzip".to_string()); // User wants compression

    let mut file_sink_mem = FileSink::new();

    let sink_config_mem = SinkConfig::File {
        path: "./test_no_override_2.json".to_string(),
        format: FileFormat::Json,
        properties: props_mem,
        compression: None, // Will be overridden by properties parsing
    };

    file_sink_mem.initialize(sink_config_mem).await?;

    let memory_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(8 * 1024 * 1024), // 8MB - would suggest different buffer size
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(30000),
    };

    let _writer_mem = file_sink_mem
        .create_writer_with_batch_config(memory_config)
        .await?;
    // Log should show:
    // - buffer_size_bytes: 2097152 (user's explicit setting, not 8MB from MemoryBased strategy)
    // - compression: Gzip (user's explicit setting preserved)

    // Test 3: User sets custom buffer_size_bytes, LowLatency strategy should not override it
    info!("\n3. Testing explicit buffer_size_bytes with LowLatency strategy...");
    let mut props_ll = HashMap::new();
    props_ll.insert("buffer_size_bytes".to_string(), "131072".to_string()); // User wants 128KB buffer

    let mut file_sink_ll = FileSink::new();

    let sink_config_ll = SinkConfig::File {
        path: "./test_no_override_3.csv".to_string(),
        format: FileFormat::Csv {
            header: true,
            delimiter: ',',
            quote: '"',
        },
        properties: props_ll,
        compression: None,
    };

    file_sink_ll.initialize(sink_config_ll).await?;

    let low_latency_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::LowLatency {
            max_batch_size: 1,
            max_wait_time: Duration::from_millis(1),
            eager_processing: true, // Would suggest buffer_size_bytes = 0
        },
        max_batch_size: 5,
        batch_timeout: Duration::from_millis(50),
    };

    let _writer_ll = file_sink_ll
        .create_writer_with_batch_config(low_latency_config)
        .await?;
    // Log should show: buffer_size_bytes: 131072 (user's explicit setting, not "0" from LowLatency strategy)

    // Test 4: Default buffer size should get optimized by batch config
    info!("\n4. Testing default buffer_size_bytes gets optimized by TimeWindow strategy...");
    let props_default = HashMap::new(); // No explicit buffer size - should use default (65536)

    let mut file_sink_default = FileSink::new();

    let sink_config_default = SinkConfig::File {
        path: "./test_default_optimized.json".to_string(),
        format: FileFormat::Json,
        properties: props_default,
        compression: None,
    };

    file_sink_default.initialize(sink_config_default).await?;

    let time_window_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::TimeWindow(Duration::from_millis(2000)), // Should suggest 1MB buffer
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(10000),
    };

    let _writer_default = file_sink_default
        .create_writer_with_batch_config(time_window_config)
        .await?;
    // Log should show: buffer_size_bytes: 1048576 (optimized by batch strategy from default 65536)

    // Test 5: Disabled batching should not override user settings
    info!("\n5. Testing disabled batching does not override user buffer settings...");
    let mut props_disabled = HashMap::new();
    props_disabled.insert("buffer_size_bytes".to_string(), "524288".to_string()); // User wants 512KB buffer

    let mut file_sink_disabled = FileSink::new();

    let sink_config_disabled = SinkConfig::File {
        path: "./test_no_override_disabled.json".to_string(),
        format: FileFormat::Json,
        properties: props_disabled,
        compression: None,
    };

    file_sink_disabled.initialize(sink_config_disabled).await?;

    let disabled_config = BatchConfig {
        enable_batching: false, // Would suggest buffer_size_bytes = 0
        strategy: BatchStrategy::FixedSize(100),
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(1000),
    };

    let _writer_disabled = file_sink_disabled
        .create_writer_with_batch_config(disabled_config)
        .await?;
    // Log should show: buffer_size_bytes: 524288 (user's explicit setting, not "0" from disabled batching)

    Ok(())
}
