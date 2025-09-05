# Compression Independence in Batch Configuration

## Overview

FerrisStreams supports independent compression configuration that works alongside batch strategies. Users can specify compression settings explicitly, and these settings will never be overridden by batch configuration optimizations. When no explicit compression is configured, batch strategies provide intelligent compression suggestions based on performance characteristics.

## Key Principle: Suggestion vs Override

The system follows a **suggestion pattern** rather than an **override pattern**:

- ✅ **Explicit compression settings are always preserved**
- ✅ **Batch strategies suggest compression only when none is specified**
- ✅ **User choice takes precedence over automatic optimization**

## Supported Sinks

### 1. Kafka Data Sink

Kafka producers support various compression algorithms that can be configured independently from batch strategies.

#### Configuration Methods

**Method 1: Direct Properties**
```rust
let mut props = HashMap::new();
props.insert("compression.type".to_string(), "gzip".to_string());

let kafka_sink = KafkaDataSink::from_properties(&props, "job-name");
```

**Method 2: SinkConfig**
```rust
let mut properties = HashMap::new();
properties.insert("compression.type".to_string(), "lz4".to_string());

let config = SinkConfig::Kafka {
    brokers: "localhost:9092".to_string(),
    topic: "my-topic".to_string(),
    properties,
};
```

#### Supported Compression Types
- `none` - No compression (fastest)
- `gzip` - Best compression ratio (slower)
- `snappy` - Balanced compression and speed
- `lz4` - Fast compression (good for high throughput)
- `zstd` - Modern compression (best balance)

### 2. File Data Sink

File sinks support compression for output files, configurable independently from batch strategies.

#### Configuration Method
```rust
use ferrisstreams::ferris::datasource::config::CompressionType;

let config = SinkConfig::File {
    path: "./output.json".to_string(),
    format: FileFormat::Json,
    compression: Some(CompressionType::Zstd), // Explicit compression
    properties: HashMap::new(),
};
```

#### Supported Compression Types
```rust
pub enum CompressionType {
    Gzip,    // Good compression ratio
    Zstd,    // Modern, balanced compression
    Lz4,     // Fast compression
    Brotli,  // Web-optimized compression
}
```

## Batch Strategy Behavior

### When Explicit Compression is Set

Batch strategies **respect** explicit compression settings and never override them:

```rust
// Example: Explicit gzip with FixedSize strategy
let mut props = HashMap::new();
props.insert("compression.type".to_string(), "gzip".to_string());

let batch_config = BatchConfig {
    strategy: BatchStrategy::FixedSize(100), // Would suggest "snappy"
    // ... other config
};

// Result: compression.type remains "gzip" (explicit setting preserved)
```

### When No Explicit Compression is Set

Batch strategies **suggest** optimal compression based on performance characteristics:

| Batch Strategy | Kafka Suggestion | File Suggestion | Rationale |
|----------------|------------------|-----------------|-----------|
| `FixedSize` | `snappy` | None | Balanced speed/compression |
| `TimeWindow` | `lz4` | None | Fast compression for time-based |
| `AdaptiveSize` | `snappy` | None | Balanced for adaptive workloads |
| `MemoryBased` | `gzip` | `gzip` (large batches) | Better compression for large data |
| `LowLatency` | `none` | None | Minimize processing overhead |

## Configuration Examples

### Example 1: Independent Kafka Compression

```rust
use ferrisstreams::ferris::datasource::{
    kafka::data_sink::KafkaDataSink,
    BatchConfig, BatchStrategy,
    config::SinkConfig,
};
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Explicit compression configuration
    let mut props = HashMap::new();
    props.insert("compression.type".to_string(), "zstd".to_string()); // User choice
    
    let mut kafka_sink = KafkaDataSink::from_properties(&props, "my-job");
    
    let sink_config = SinkConfig::Kafka {
        brokers: "localhost:9092".to_string(),
        topic: "output-topic".to_string(),
        properties: props,
    };
    
    kafka_sink.initialize(sink_config).await?;
    
    // Batch configuration with MemoryBased strategy
    let batch_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(2 * 1024 * 1024), // Would suggest gzip
        max_batch_size: 1000,
        batch_timeout: Duration::from_millis(10000),
    };
    
    // Create writer - compression.type will be "zstd" (user's explicit choice)
    let writer = kafka_sink.create_writer_with_batch_config(batch_config).await?;
    
    Ok(())
}
```

### Example 2: Independent File Compression

```rust
use ferrisstreams::ferris::datasource::{
    file::sink::FileSink,
    BatchConfig, BatchStrategy,
    config::{SinkConfig, FileFormat, CompressionType},
};
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut file_sink = FileSink::new();
    
    // Explicit compression configuration
    let sink_config = SinkConfig::File {
        path: "./data/output.json".to_string(),
        format: FileFormat::Json,
        compression: Some(CompressionType::Brotli), // User choice
        properties: HashMap::new(),
    };
    
    file_sink.initialize(sink_config).await?;
    
    // Batch configuration with MemoryBased strategy  
    let batch_config = BatchConfig {
        enable_batching: true,
        strategy: BatchStrategy::MemoryBased(5 * 1024 * 1024), // Would suggest gzip
        max_batch_size: 10000,
        batch_timeout: Duration::from_millis(30000),
    };
    
    // Create writer - compression will be Brotli (user's explicit choice)
    let writer = file_sink.create_writer_with_batch_config(batch_config).await?;
    
    Ok(())
}
```

### Example 3: Automatic Compression Suggestion

```rust
// No explicit compression - let batch strategy suggest optimal compression
let sink_config = SinkConfig::Kafka {
    brokers: "localhost:9092".to_string(),
    topic: "output-topic".to_string(),
    properties: HashMap::new(), // No compression.type specified
};

let batch_config = BatchConfig {
    enable_batching: true,
    strategy: BatchStrategy::LowLatency { 
        max_batch_size: 1,
        max_wait_time: Duration::from_millis(1),
        eager_processing: true,
    },
    max_batch_size: 5,
    batch_timeout: Duration::from_millis(50),
};

// Result: compression.type will be "none" (suggested by LowLatency strategy)
```

## Implementation Details

### Kafka Data Sink Implementation

The `apply_batch_config_to_producer()` method checks for existing compression settings:

```rust
// Suggest compression only if not explicitly set
if !producer_config.contains_key("compression.type") {
    producer_config.insert("compression.type".to_string(), "snappy".to_string());
}
```

### File Data Sink Implementation

The `optimize_config_for_batch_strategy()` method respects existing compression:

```rust
// Suggest compression for large batches only if not explicitly set
if optimized_config.compression.is_none() && buffer_size > 1024 * 1024 {
    optimized_config.compression = Some(CompressionType::Gzip);
}
```

## Configuration Logging

Both sinks provide detailed logging to show applied compression settings:

### Kafka Sink Logging
```
=== Kafka Producer Configuration ===
Batch Strategy: MemoryBased(2097152)
Applied Producer Settings:
  - batch.size: 1048576
  - linger.ms: 100  
  - compression.type: gzip    <- Shows final compression setting
  - acks: all
  - topic: my-topic
=====================================
```

### File Sink Logging
```
=== File Writer Configuration ===  
Batch Strategy: MemoryBased(2097152)
Applied File Writer Settings:
  - Path: ./output.json
  - Format: Json
  - Compression: Zstd           <- Shows final compression setting
  - Buffer Size: 2097152 bytes
=====================================
```

## Testing Compression Independence

Use the provided test program to verify compression independence:

```bash
# Run compression independence test
RUST_LOG=info cargo run --bin test_compression_independence --no-default-features
```

Expected behavior:
- ✅ Explicit settings are preserved regardless of batch strategy
- ✅ Automatic suggestions apply only when no explicit setting exists
- ✅ Logging clearly shows which compression is actually used

## Best Practices

### 1. Choose Compression Based on Use Case

**High Throughput Applications:**
```rust
// Prioritize speed over compression ratio
props.insert("compression.type".to_string(), "lz4".to_string());
```

**Storage-Optimized Applications:**
```rust  
// Prioritize compression ratio
props.insert("compression.type".to_string(), "gzip".to_string());
```

**Balanced Applications:**
```rust
// Modern balanced compression
props.insert("compression.type".to_string(), "zstd".to_string());
```

### 2. Let Batch Strategies Suggest When Unsure

If you're not sure which compression to use, omit explicit compression and let batch strategies suggest optimal settings based on your performance requirements.

### 3. Monitor Compression Performance

Use the logging output to verify compression settings and monitor performance impacts:

```bash
# Enable detailed logging
RUST_LOG=info cargo run --bin your_application
```

### 4. Test with Production-Like Data

Compression effectiveness varies significantly with data types. Test with representative data:

```rust
// Test different compression with your actual data patterns
let test_strategies = vec![
    ("none", "none"),
    ("lz4", "lz4"), 
    ("snappy", "snappy"),
    ("gzip", "gzip"),
    ("zstd", "zstd"),
];
```

## Migration Guide

### From Override Pattern to Suggestion Pattern

If you were previously relying on batch strategies to set compression, and now want explicit control:

**Before (automatic override):**
```rust
let batch_config = BatchConfig {
    strategy: BatchStrategy::MemoryBased(1024 * 1024), // Would force gzip
    // ...
};
```

**After (explicit control):**
```rust
// Set explicit compression
props.insert("compression.type".to_string(), "gzip".to_string());

let batch_config = BatchConfig {
    strategy: BatchStrategy::MemoryBased(1024 * 1024), // Respects explicit gzip
    // ...
};
```

### Preserving Automatic Behavior

If you want to keep the automatic compression selection behavior, simply don't specify explicit compression - batch strategies will continue to provide intelligent defaults.

## Troubleshooting

### Compression Not Working as Expected

1. **Check Configuration Order:** Ensure compression is set before calling `create_writer_with_batch_config()`

2. **Verify Property Names:** 
   - Kafka: Use `"compression.type"` key
   - File: Use `compression` field in `SinkConfig::File`

3. **Check Logs:** Enable INFO level logging to see applied compression settings

4. **Test Independence:** Use the test program to verify your configuration works correctly

### Performance Issues

1. **CPU-Intensive Compression:** Consider switching from `gzip` to `lz4` or `snappy`
2. **Network Bandwidth:** Higher compression ratios (`gzip`, `zstd`) reduce network usage
3. **Disk I/O:** For file sinks, compression reduces disk usage but increases CPU load

## Related Documentation

- [Batch Configuration Guide](./BATCH_CONFIGURATION.md)
- [Kafka Integration Guide](./KAFKA_INTEGRATION.md) 
- [File Sink Configuration](./FILE_SINK_CONFIGURATION.md)
- [Performance Tuning Guide](./PERFORMANCE_TUNING.md)