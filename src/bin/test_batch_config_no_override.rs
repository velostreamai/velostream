use ferrisstreams::ferris::datasource::{
    BatchConfig, BatchStrategy,
    kafka::writer::KafkaDataWriter,
};
use std::collections::HashMap;
use std::time::Duration;
use log::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize env_logger to see the log output
    env_logger::init();

    info!("=== Testing Batch Config Does NOT Override User Properties ===\n");

    // Test that explicit user properties are never overridden by batch config
    test_user_properties_not_overridden().await?;

    info!("\n=== Batch Config Override Prevention Tests Complete ===");
    Ok(())
}

async fn test_user_properties_not_overridden() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("🔒 Testing that explicit user properties are never overridden");

    // Test 1: User sets batch.size=2048, FixedSize strategy should not override it
    info!("\n1. Testing explicit batch.size with FixedSize strategy...");
    let mut props = HashMap::new();
    props.insert("batch.size".to_string(), "2048".to_string()); // User's explicit setting
    props.insert("linger.ms".to_string(), "200".to_string()); // User's explicit setting
    props.insert("compression.type".to_string(), "snappy".to_string()); // User's explicit setting
    
    let batch_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::FixedSize(500), // Would suggest different batch.size
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(5000),
    };

    let _writer = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test-no-override-1".to_string(),
        &props,
        batch_config,
    ).await?;
    // Log should show:
    // - batch.size: 2048 (user's explicit setting, not batch config suggestion)
    // - linger.ms: 200 (user's explicit setting, not batch config suggestion)
    // - compression.type: snappy (user's explicit setting, not batch config suggestion)

    // Test 2: User sets acknowledgment settings, LowLatency strategy should not override them
    info!("\n2. Testing explicit acks/retries with LowLatency strategy...");
    let mut props_ll = HashMap::new();
    props_ll.insert("acks".to_string(), "all".to_string()); // User wants durability
    props_ll.insert("retries".to_string(), "10".to_string()); // User wants reliability
    props_ll.insert("max.in.flight.requests.per.connection".to_string(), "1".to_string()); // User wants ordering
    props_ll.insert("compression.type".to_string(), "gzip".to_string()); // User wants compression
    
    let low_latency_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::LowLatency { // Would suggest different acks/retries
            max_batch_size: 1,
            max_wait_time: Duration::from_millis(1),
            eager_processing: true,
        },
        max_batch_size: 5,
        batch_timeout: Duration::from_millis(50),
    };

    let _writer_ll = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test-no-override-2".to_string(),
        &props_ll,
        low_latency_config,
    ).await?;
    // Log should show:
    // - acks: all (user's explicit setting, not "1" from LowLatency strategy)
    // - retries: 10 (user's explicit setting, not "0" from LowLatency strategy)
    // - max.in.flight.requests.per.connection: 1 (user's explicit setting, not "5")
    // - compression.type: gzip (user's explicit setting, not "none")

    // Test 3: User sets memory buffer, MemoryBased strategy should not override it
    info!("\n3. Testing explicit buffer settings with MemoryBased strategy...");
    let mut props_mem = HashMap::new();
    props_mem.insert("queue.buffering.max.kbytes".to_string(), "1024".to_string()); // User's explicit buffer
    props_mem.insert("request.timeout.ms".to_string(), "5000".to_string()); // User's explicit timeout
    
    let memory_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(4 * 1024 * 1024), // 4MB - would suggest different buffer
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(30000), // Would suggest different timeout
    };

    let _writer_mem = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test-no-override-3".to_string(),
        &props_mem,
        memory_config,
    ).await?;
    // Log should show:
    // - queue.buffering.max.kbytes: 1024KB (user's explicit setting)
    // - request.timeout.ms: 5000 (user's explicit setting, not 30000 from batch timeout)

    // Test 4: Disabled batching should not override user settings
    info!("\n4. Testing disabled batching does not override user settings...");
    let mut props_disabled = HashMap::new();
    props_disabled.insert("batch.size".to_string(), "32768".to_string()); // User wants some batching
    props_disabled.insert("linger.ms".to_string(), "50".to_string()); // User wants some linger
    
    let disabled_config = BatchConfig {
        enable_batching: false, // Would suggest batch.size=0, linger.ms=0
        strategy: BatchStrategy::FixedSize(100),
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(1000),
    };

    let _writer_disabled = KafkaDataWriter::from_properties_with_batch_config(
        "localhost:9092",
        "test-no-override-4".to_string(),
        &props_disabled,
        disabled_config,
    ).await?;
    // Log should show:
    // - batch.size: 32768 (user's explicit setting, not "0" from disabled batching)
    // - linger.ms: 50 (user's explicit setting, not "0" from disabled batching)

    Ok(())
}