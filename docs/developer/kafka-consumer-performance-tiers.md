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
