comm# Kafka Consumer Performance Tiers

## Overview

Velostream provides a tiered consumer architecture that allows you to select the optimal Kafka consumer implementation based on your throughput and latency requirements. Choose from **Standard**, **Buffered**, or **Dedicated** tiers to match your workload characteristics.

## Performance Tier Architecture

```text
┌─────────────────────────────────────┐
│   KafkaStreamConsumer<K, V>        │  ← Unified trait interface
└─────────────────────────────────────┘
           ▲          ▲          ▲
           │          │          │
  ┌────────┴──┐  ┌────┴─────┐  ┌┴──────────┐
  │ Standard  │  │ Buffered │  │ Dedicated │
  │ Adapter   │  │ Adapter  │  │ Adapter   │
  └───────────┘  └──────────┘  └───────────┘
        │              │              │
        └──────────────┴──────────────┘
                       │
             ┌─────────▼─────────┐
             │ Consumer<K, V>    │  ← BaseConsumer (rdkafka)
             │ (fast consumer)   │
             └───────────────────┘
```

## Available Tiers

| Tier | Throughput | Latency (p99) | CPU Usage | Memory | Use Case |
|------|-----------|---------------|-----------|--------|----------|
| **Standard (default)** | 10K-15K msg/s | ~1ms | 2-5% | Low | Real-time events, low-latency apps |
| **Buffered** | 50K-75K msg/s | ~1ms | 3-8% | Medium | Analytics, batch processing |
| **Dedicated** | 100K-150K msg/s | <1ms | 10-15% | Medium | Maximum throughput, high-volume streaming |

## Quick Start

### Standard Tier (10K-15K msg/s)

Best for: Real-time event processing, low-latency applications

```rust
use velostream::velostream::kafka::{
    consumer_config::{ConsumerConfig, ConsumerTier},
    consumer_factory::ConsumerFactory,
    serialization::JsonSerializer,
};

// Create consumer with Standard tier
let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Standard);

let consumer = ConsumerFactory::create::<String, String, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;

// Use unified stream interface
consumer.subscribe(&["my-topic"])?;
let mut stream = consumer.stream();

while let Some(result) = stream.next().await {
    match result {
        Ok(message) => {
            println!("Received: {:?}", message.value());
        }
        Err(e) => eprintln!("Error: {}", e),
    }
}
```

### Buffered Tier (50K-75K msg/s)

Best for: Analytics pipelines, batch processing, high-volume ingestion

```rust
// Create consumer with Buffered tier and 32-message batches
let config = ConsumerConfig::new("localhost:9092", "analytics-group")
    .performance_tier(ConsumerTier::Buffered { batch_size: 32 });

let consumer = ConsumerFactory::create::<String, MyEvent, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;

consumer.subscribe(&["analytics-events"])?;
let mut stream = consumer.stream();

// Process in batches for better throughput
while let Some(result) = stream.next().await {
    // Messages are efficiently buffered internally
    process_event(result?).await;
}
```

**Batch Size Recommendations**:
- **16**: Lower latency, moderate throughput
- **32**: Balanced (recommended default)
- **64**: Higher throughput, slight latency increase
- **128**: Maximum throughput, higher latency

### Dedicated Tier (100K-150K msg/s)

Best for: Maximum throughput workloads, CPU-bound processing

```rust
// Create consumer with Dedicated tier (dedicated polling thread)
let config = ConsumerConfig::new("localhost:9092", "high-volume-group")
    .performance_tier(ConsumerTier::Dedicated);

let consumer = ConsumerFactory::create::<String, String, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;

consumer.subscribe(&["high-volume-stream"])?;
let mut stream = consumer.stream();

// Dedicated thread polls Kafka continuously
// Your application thread just receives messages
while let Some(result) = stream.next().await {
    // Process with maximum throughput
    handle_message(result?).await;
}
```

**How it works**:
- Spawns a dedicated background thread for Kafka polling
- Eliminates polling overhead from your application thread
- Best for CPU-bound processing where you want to offload I/O

### Default Behavior (No Tier Specified)

When no tier is specified, Standard tier is automatically selected, providing excellent performance out of the box.

```rust
// No performance_tier() specified → Standard tier automatically selected
let config = ConsumerConfig::new("localhost:9092", "my-group");

let consumer = ConsumerFactory::create::<String, String, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;

// Uses Standard tier (10K-15K msg/s) by default
consumer.subscribe(&["my-topic"])?;
let mut stream = consumer.stream();
```

## Choosing the Right Tier

### Decision Tree

```text
Start
  │
  ├─ Need < 20K msg/s with low latency? → **Standard** (default)
  │
  ├─ Need 20K-80K msg/s for analytics? → **Buffered**
  │
  └─ Need > 80K msg/s maximum throughput? → **Dedicated**
```

### Use Case Examples

#### Standard Tier Use Cases
- Real-time dashboards
- User activity tracking
- IoT sensor data (moderate volume)
- Transaction processing (< 10K TPS)
- Event-driven microservices

#### Buffered Tier Use Cases
- Analytics pipelines
- Data lake ingestion
- Aggregation workloads
- Machine learning feature collection
- High-volume logging
- Time-series data collection

#### Dedicated Tier Use Cases
- Maximum throughput requirements
- High-volume event streaming (100K+ msg/s)
- CPU-bound message processing
- Real-time data replication
- Large-scale ETL pipelines

## Configuration Examples

### With Custom Kafka Properties

```rust
let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Buffered { batch_size: 64 })
    .auto_commit(true, Duration::from_secs(5))
    .session_timeout(Duration::from_secs(30))
    .custom_property("fetch.min.bytes", "1024")
    .custom_property("fetch.max.wait.ms", "500");
```

### Multiple Consumers with Different Tiers

```rust
// Real-time consumer (Standard)
let realtime_config = ConsumerConfig::new("localhost:9092", "realtime-group")
    .performance_tier(ConsumerTier::Standard);
let realtime_consumer = ConsumerFactory::create(...)?;

// Analytics consumer (Buffered)
let analytics_config = ConsumerConfig::new("localhost:9092", "analytics-group")
    .performance_tier(ConsumerTier::Buffered { batch_size: 128 });
let analytics_consumer = ConsumerFactory::create(...)?;

// High-volume consumer (Dedicated)
let highvol_config = ConsumerConfig::new("localhost:9092", "highvol-group")
    .performance_tier(ConsumerTier::Dedicated);
let highvol_consumer = ConsumerFactory::create(...)?;
```

### With Different Serializers

```rust
// JSON serialization (Standard tier)
let json_consumer = ConsumerFactory::create::<String, MyEvent, _, _>(
    config.clone().performance_tier(ConsumerTier::Standard),
    JsonSerializer,
    JsonSerializer,
)?;

// Avro serialization (Buffered tier)
let avro_consumer = ConsumerFactory::create::<String, MyAvroType, _, _>(
    config.clone().performance_tier(ConsumerTier::Buffered { batch_size: 32 }),
    AvroSerializer::new(schema),
    AvroSerializer::new(schema),
)?;

// Protobuf serialization (Dedicated tier)
let proto_consumer = ConsumerFactory::create::<String, MyProtoType, _, _>(
    config.performance_tier(ConsumerTier::Dedicated),
    ProtobufSerializer::new(),
    ProtobufSerializer::new(),
)?;
```

## Performance Tuning

### Standard Tier Optimization
```rust
let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Standard)
    .session_timeout(Duration::from_secs(10))      // Lower timeout
    .heartbeat_interval(Duration::from_millis(3000)) // Faster heartbeats
    .custom_property("fetch.min.bytes", "1");       // Fetch immediately
```

### Buffered Tier Optimization
```rust
let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Buffered { batch_size: 64 })
    .session_timeout(Duration::from_secs(30))
    .custom_property("fetch.min.bytes", "10240")    // Wait for 10KB batches
    .custom_property("fetch.max.wait.ms", "500");   // Max wait 500ms
```

### Dedicated Tier Optimization
```rust
let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Dedicated)
    .session_timeout(Duration::from_secs(45))       // Higher timeout (background thread)
    .custom_property("fetch.min.bytes", "51200")    // Large batches (50KB)
    .custom_property("fetch.max.wait.ms", "100");   // Low latency
```

## Unified Interface

All tiers implement the same `KafkaStreamConsumer` trait, so you can switch tiers without changing your application code:

```rust
pub trait KafkaStreamConsumer<K, V> {
    fn stream(&self) -> Pin<Box<dyn Stream<Item = Result<Message<K, V>, ConsumerError>> + Send + '_>>;
    fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError>;
    fn commit(&self) -> Result<(), KafkaError>;
    fn current_offsets(&self) -> Result<Option<TopicPartitionList>, KafkaError>;
    fn assignment(&self) -> Result<Option<TopicPartitionList>, KafkaError>;
}
```

**Benefits**:
- Switch tiers via configuration only
- No code changes required
- Test different tiers easily
- Polymorphic consumer usage

## Monitoring and Metrics

### Check Consumer Performance

```rust
use tokio::time::Instant;

let start = Instant::now();
let mut count = 0;

while let Some(result) = stream.next().await {
    match result {
        Ok(_) => count += 1,
        Err(e) => eprintln!("Error: {}", e),
    }

    // Print throughput every 10K messages
    if count % 10_000 == 0 {
        let elapsed = start.elapsed();
        let msg_per_sec = count as f64 / elapsed.as_secs_f64();
        println!("Throughput: {:.0} msg/s", msg_per_sec);
    }
}
```

### Kafka Monitoring Commands

```bash
# Check consumer group lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group my-group

# Monitor topic throughput
kafka-run-class.sh kafka.tools.ConsumerPerformance \
  --broker-list localhost:9092 \
  --topic my-topic \
  --messages 100000
```

## Migration Guide

### No Migration Needed!

**Great news**: The tier system is now the default! When you don't specify a tier, Standard tier is automatically selected, giving you 2-3x better performance than before.

```rust
// This code automatically uses Standard tier now
let config = ConsumerConfig::new("localhost:9092", "my-group");
let consumer = ConsumerFactory::create(...)?;
// Automatically uses Standard tier (10K-15K msg/s)
```

**Optional**: Explicitly specify tier for clarity or to use Buffered/Dedicated:
```rust
let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Buffered { batch_size: 32 });
let consumer = ConsumerFactory::create(...)?;
```

### From Direct Consumer Usage

**Before**:
```rust
use velostream::velostream::kafka::kafka_consumer::KafkaConsumer;

let consumer = KafkaConsumer::<String, String, _, _>::with_config(
    config,
    JsonSerializer,
    JsonSerializer,
)?;
```

**After** (Factory pattern with tiers):
```rust
use velostream::velostream::kafka::consumer_factory::ConsumerFactory;

let config = config.performance_tier(ConsumerTier::Standard);
let consumer = ConsumerFactory::create::<String, String, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;
```

## Testing

### Integration Tests

The tier system includes comprehensive integration tests using testcontainers (requires Docker):

```bash
# Run all tier integration tests
cargo test --test mod kafka::kafka_consumer_integration_test -- --ignored

# Run specific tier test
cargo test test_standard_tier_adapter -- --ignored
cargo test test_buffered_tier_adapter -- --ignored
cargo test test_dedicated_tier_adapter -- --ignored
```

### Unit Tests

```bash
# Test tier adapters
cargo test consumer_adapters

# Test factory pattern
cargo test consumer_factory
```

## Intelligent Tier Selection in KafkaDataReader

**FR-081 Phase 2C** introduces automatic tier selection in the datasource layer. `KafkaDataReader` intelligently chooses the optimal consumer tier based on your batch configuration, eliminating the need for manual tier selection in most cases.

### The Relationship: batch.size Drives max.poll.records

**Key Insight**: Your `batch.size` directly sets Kafka's `max.poll.records` property, ensuring efficient polling:

```
BatchConfig.strategy = FixedSize(500)
    ↓
consumer_config.max_poll_records = 500  (Kafka polls 500 records)
    ↓
ConsumerTier = Buffered { batch_size: 500 }  (processes 500 records)
    ↓
Result: Poll size matches processing batch size (efficient!)
```

This design ensures:
- ✅ **No wasted polls**: Poll exactly what you'll process
- ✅ **No memory waste**: Don't buffer more than you need
- ✅ **Optimal throughput**: Single poll satisfies batch requirement

### How Auto-Selection Works

When creating a `KafkaDataReader`, the system analyzes your `BatchConfig` strategy and automatically:
1. Sets `max.poll.records` based on your batch size
2. Selects the optimal consumer tier for that batch size
3. Configures fetch settings for efficiency

**YAML Configuration** (most common):
```yaml
# config/orders_source.yaml

# Example 1: Large batch → Auto-selects Buffered tier
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"
    # No max.poll.records specified, uses batch.size below

# BatchConfig determines both max.poll.records and tier
batch_config:
  strategy: FixedSize
  batch_size: 500  # → max.poll.records=500, Buffered tier (50-75K msg/s)

# Example 2: Small batch → Auto-selects Standard tier
batch_config:
  strategy: FixedSize
  batch_size: 50  # → max.poll.records=50, Standard tier (10-15K msg/s)

# Example 3: Memory-based → Auto-selects Buffered tier
batch_config:
  strategy: MemoryBased
  max_memory_bytes: 10485760  # 10MB → Buffered tier with batch_size=500
```

**Programmatic Configuration**:
```rust
use velostream::velostream::datasource::{BatchConfig, BatchStrategy};

// Example 1: Large batch → Buffered tier
let config = BatchConfig {
    strategy: BatchStrategy::FixedSize(500),
    ..Default::default()
};
// Result: max_poll_records=500, Buffered tier (50-75K msg/s)

// Example 2: Small batch → Standard tier
let config = BatchConfig {
    strategy: BatchStrategy::FixedSize(50),
    ..Default::default()
};
// Result: max_poll_records=50, Standard tier (10-15K msg/s)
```

### Auto-Selection Rules

**Key**: `batch.size` → `max.poll.records` → `tier`

| Batch Strategy | max.poll.records | Selected Tier | Throughput |
|----------------|------------------|---------------|------------|
| `FixedSize(500)` | **500** | `Buffered { batch_size: 500 }` | 50-75K msg/s |
| `FixedSize(2000)` | **2000** | `Buffered { batch_size: 1000 }*` | 50-75K msg/s |
| `FixedSize(100)` | **100** | `Standard` | 10-15K msg/s |
| `FixedSize(50)` | **50** | `Standard` | 10-15K msg/s |
| `MemoryBased(10MB)` | **~10,000** | `Buffered { batch_size: 500 }` | 50-75K msg/s |
| `AdaptiveSize { min: 50, max: 500 }` | **50** (starts at min) | `Standard` | 10-15K msg/s |
| `LowLatency { max: 10 }` | **10** | `Standard` | 10-15K msg/s |

*Tier batch_size is capped at 1000 to prevent memory issues, but `max.poll.records` is not capped

### Explicit Tier Override

You can override auto-selection by explicitly setting the `performance_tier`:

**YAML Configuration**:
```yaml
# config/high_throughput_source.yaml
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"
    # Explicit tier override (advanced usage)
    performance.tier: dedicated  # Override auto-selection

batch_config:
  strategy: FixedSize
  batch_size: 100  # Would normally select Standard tier
  # But explicit override forces Dedicated tier (100K-150K msg/s)
```

**Programmatic Override**:
```rust
use velostream::velostream::kafka::consumer_config::{ConsumerConfig, ConsumerTier};
use velostream::velostream::datasource::{BatchConfig, BatchStrategy};

let mut consumer_config = ConsumerConfig::new("localhost:9092", "my-group");

// Override: Force Dedicated tier regardless of batch size
consumer_config.performance_tier = Some(ConsumerTier::Dedicated);

let batch_config = BatchConfig {
    strategy: BatchStrategy::FixedSize(100),  // Would select Standard
    ..Default::default()
};

// Result: Uses Dedicated tier (100K-150K msg/s) despite small batch size
```

**When to Override**:
- ✅ Maximum throughput needed regardless of batch size
- ✅ Testing different tiers for performance comparison
- ⚠️ Usually not needed - auto-selection is optimal for most cases

### Performance Impact

Auto-tier selection provides significant performance improvements with zero configuration:

| Scenario | Auto-Selected Tier | Throughput Gain |
|----------|-------------------|-----------------|
| Small batches (≤100) | Standard | **2-3x** vs legacy StreamConsumer |
| Large batches (>100) | Buffered | **5-7x** vs legacy StreamConsumer |
| Memory-based batching | Buffered | **5-7x** vs legacy StreamConsumer |

### Tier-Appropriate Streaming Methods

Each `KafkaDataReader` method uses the optimal streaming approach based on the selected tier:

| Method | Standard Tier | Buffered Tier | Dedicated Tier |
|--------|--------------|---------------|----------------|
| `read_single()` | Direct polling | Batched polling | Dedicated thread |
| `read_fixed_size()` | Buffered with target size | Native batch size | Buffered with target size |
| `read_time_window()` | Direct polling | Direct polling | Direct polling |
| `read_memory_based()` | Buffered (100 msgs) | Buffered (500 msgs) | Buffered (500 msgs) |
| `read_low_latency()` | Direct polling | Direct polling | Direct polling |

### Example: Complete Datasource Setup

**YAML Configuration** (recommended):
```yaml
# config/analytics_source.yaml
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"
    group.id: "analytics_group"

  schema:
    value.format: json

# batch.size drives max.poll.records and tier selection
batch_config:
  strategy: FixedSize
  batch_size: 200  # → max.poll.records=200, Buffered tier
  batch_timeout_ms: 500
  max_batch_size: 1000
```

**SQL Usage** (references configured source):
```sql
-- SQL uses pre-configured table
-- Tier selection already determined by YAML config
SELECT order_id, amount, status
FROM orders  -- References YAML-configured source
WHERE amount > 1000;
```

**Programmatic Setup**:
```rust
use velostream::velostream::datasource::{
    kafka::KafkaDataSource,
    BatchConfig, BatchStrategy, create_source,
};
use std::time::Duration;

// 1. Configure batch strategy
let batch_config = BatchConfig {
    strategy: BatchStrategy::FixedSize(200),
    batch_timeout: Duration::from_millis(500),
    max_batch_size: 1000,
    enable_batching: true,
};
// Result: max_poll_records=200, Buffered tier auto-selected

// 2. Create source from connection string
let uri = "kafka://localhost:9092/orders?group.id=analytics";
let source = create_source(uri)?;

// 3. Create reader with batch config
let mut reader = source.create_reader_with_batch_config(batch_config).await?;
// → Uses Buffered tier (50-75K msg/s)
// → Kafka polls 200 records per poll
// → Processes 200 records per batch

// 4. Read batches efficiently
let records = reader.read_batch().await?;
```

### Logging

Auto-tier selection is logged for visibility:

```log
INFO: FR-081: Auto-selecting Buffered tier (50-75K msg/s) for large batch size: 200
INFO: FR-081: KafkaDataReader using Buffered tier for optimal CPU/memory efficiency
```

### Migration from Legacy Consumer

**Before (Phase 2A - StreamConsumer)**:
```yaml
# Old configuration - no tier concept
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"
    max.poll.records: 500  # Ignored by old implementation

# → Always used StreamConsumer (5-10K msg/s)
# → max.poll.records didn't affect processing
```

**After (Phase 2C - Intelligent Tier Selection)**:
```yaml
# New configuration - batch.size drives everything
datasource:
  type: kafka
  consumer_config:
    bootstrap.servers: "localhost:9092"
    # max.poll.records automatically set from batch.size

batch_config:
  strategy: FixedSize
  batch_size: 500  # → max.poll.records=500, Buffered tier

# → Uses FastConsumer with Buffered tier (50-75K msg/s)
# → 2-7x performance improvement with zero code changes
# → batch.size and poll size perfectly aligned
```

**Key Improvements**:
- ✅ `batch.size` → `max.poll.records` synchronization (no wasted polls)
- ✅ Automatic tier selection based on workload
- ✅ 2-7x throughput improvement
- ✅ Lower CPU usage per message
- ✅ Better memory efficiency

### Benefits

1. **Zero Configuration**: Optimal tier selected automatically
2. **Performance by Default**: 2-7x throughput improvement without code changes
3. **CPU/Memory Efficient**: Each tier optimized for its use case
4. **Backward Compatible**: Existing code gets automatic performance boost
5. **Override Available**: Explicit tier selection when needed

## Troubleshooting

### Consumer Group Rebalancing Issues

**Problem**: Frequent rebalances with Dedicated tier

**Solution**: Increase session timeout
```rust
let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Dedicated)
    .session_timeout(Duration::from_secs(60));  // Increase from default 30s
```

### High CPU Usage with Dedicated Tier

**Expected**: Dedicated tier uses 10-15% CPU for continuous polling

**If excessive**: Check if you have CPU-bound processing in consumer loop
```rust
// Move heavy processing to separate tasks
while let Some(result) = stream.next().await {
    let message = result?;
    tokio::spawn(async move {
        heavy_processing(message).await;
    });
}
```

### Buffered Tier Not Improving Throughput

**Check batch size configuration**:
```rust
// Too small - increase batch size
.performance_tier(ConsumerTier::Buffered { batch_size: 16 })  // Try 64 or 128

// Also tune Kafka fetch settings
.custom_property("fetch.min.bytes", "10240")
.custom_property("fetch.max.wait.ms", "500")
```

## Best Practices

1. **Start with Standard tier** for new applications
2. **Monitor throughput** before switching tiers
3. **Use Buffered tier** when you need 2-5x more throughput than Standard
4. **Reserve Dedicated tier** for maximum throughput requirements (>80K msg/s)
5. **Test tier changes** in staging before production
6. **Match tier to workload**:
   - Real-time → Standard
   - Analytics → Buffered
   - High-volume → Dedicated
7. **Consider multiple consumers** with different tiers for different workloads
8. **Use manual commit** with Buffered/Dedicated tiers for better control

## Further Reading

- [Kafka Performance Configuration Guide](kafka-performance-configs.md)
- [Streaming Kafka API Reference](streaming-kafka-api.md)
- [Kafka Schema Configuration](kafka-schema-configuration.md)
- [Phase 2B Implementation Schedule](../feature/FR-081-sql-engine-perf/FR-081-08-IMPLEMENTATION-SCHEDULE.md)

## Support

For issues or questions about consumer performance tiers:
- File an issue: [GitHub Issues](https://github.com/anthropics/velostream/issues)
- Check documentation: [Velostream Docs](../README.md)
