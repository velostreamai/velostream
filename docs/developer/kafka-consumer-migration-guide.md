# Kafka Consumer Migration Guide

## Overview

This guide helps you migrate existing Kafka consumer applications to use the new performance tier system introduced in Phase 2B. The tier system provides optimized consumer implementations for different throughput requirements while maintaining full backward compatibility.

## Quick Reference

| Current Implementation | Recommended Tier | Migration Effort |
|------------------------|------------------|------------------|
| No tier specified (Legacy) | Keep as-is or upgrade to Standard | Minimal (1-2 lines) |
| Low-latency real-time apps | Standard tier | Easy (5 minutes) |
| Analytics/batch processing | Buffered tier | Easy (5 minutes) |
| High-volume streaming | Dedicated tier | Moderate (10-15 minutes) |

## Backward Compatibility

**Important**: The tier system is **100% backward compatible**. Existing applications will continue to work without any changes:

```rust
// Existing code (no changes required)
let config = ConsumerConfig::new("localhost:9092", "my-group");
let consumer = ConsumerFactory::create(...)?;
// Automatically uses Legacy tier (StreamConsumer)
```

## Migration Patterns

### Pattern 1: Migrating from Direct KafkaConsumer

**Before** (Direct KafkaConsumer usage):
```rust
use velostream::velostream::kafka::{
    consumer_config::ConsumerConfig,
    kafka_consumer::KafkaConsumer,
    serialization::JsonSerializer,
};

let config = ConsumerConfig::new("localhost:9092", "my-group");
let consumer = KafkaConsumer::<String, String, _, _>::with_config(
    config,
    JsonSerializer,
    JsonSerializer,
)?;

consumer.subscribe(&["my-topic"])?;
let mut stream = consumer.stream();

while let Some(result) = stream.next().await {
    // Process message
}
```

**After** (With Standard tier - 10K-15K msg/s):
```rust
use velostream::velostream::kafka::{
    consumer_config::{ConsumerConfig, ConsumerTier},
    consumer_factory::ConsumerFactory,
    serialization::JsonSerializer,
};

let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Standard);  // ← Add this line

let consumer = ConsumerFactory::create::<String, String, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;

consumer.subscribe(&["my-topic"])?;
let mut stream = consumer.stream();

// Rest of code unchanged
while let Some(result) = stream.next().await {
    // Process message
}
```

**Changes**:
1. Import `ConsumerTier` enum
2. Import `ConsumerFactory` instead of `KafkaConsumer`
3. Add `.performance_tier(ConsumerTier::Standard)` to config
4. Use `ConsumerFactory::create()` instead of `KafkaConsumer::with_config()`

**Effort**: 2 imports + 2 lines changed = ~5 minutes

### Pattern 2: Migrating Legacy StreamConsumer to Buffered Tier

**Before** (Legacy):
```rust
let config = ConsumerConfig::new("localhost:9092", "analytics-group");
let consumer = ConsumerFactory::create::<String, MyEvent, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;
```

**After** (Buffered tier - 50K-75K msg/s):
```rust
let config = ConsumerConfig::new("localhost:9092", "analytics-group")
    .performance_tier(ConsumerTier::Buffered { batch_size: 32 });  // ← Add this

let consumer = ConsumerFactory::create::<String, MyEvent, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;
```

**Batch Size Selection**:
- **16**: Lower latency, moderate throughput
- **32**: Balanced (recommended default)
- **64**: Higher throughput, slight latency increase
- **128**: Maximum throughput, higher latency

**Effort**: 1 line changed = ~2 minutes

### Pattern 3: Upgrading to Dedicated Tier for Maximum Throughput

**Before** (Any tier):
```rust
let config = ConsumerConfig::new("localhost:9092", "high-volume-group");
let consumer = ConsumerFactory::create::<String, String, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;
```

**After** (Dedicated tier - 100K-150K msg/s):
```rust
let config = ConsumerConfig::new("localhost:9092", "high-volume-group")
    .performance_tier(ConsumerTier::Dedicated);  // ← Add this

let consumer = ConsumerFactory::create::<String, String, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;

// Same stream usage - no changes needed
```

**Considerations**:
- Spawns dedicated background thread for Kafka polling
- Higher CPU usage (10-15%) due to continuous polling
- Best for CPU-bound processing where you want to offload I/O
- Monitor resource usage after deployment

**Effort**: 1 line changed + monitoring = ~10 minutes

## Migration by Use Case

### Use Case 1: Real-Time Event Processing

**Characteristics**:
- Low latency requirements (< 1ms p99)
- Moderate throughput (< 20K msg/s)
- Event-driven microservices

**Recommended Tier**: Standard

**Migration**:
```rust
let config = ConsumerConfig::new("localhost:9092", "events-group")
    .performance_tier(ConsumerTier::Standard)
    .session_timeout(Duration::from_secs(10))
    .heartbeat_interval(Duration::from_millis(3000))
    .custom_property("fetch.min.bytes", "1");  // Fetch immediately
```

### Use Case 2: Analytics Pipelines

**Characteristics**:
- Batch processing
- High throughput (20K-80K msg/s)
- Can tolerate slight latency

**Recommended Tier**: Buffered

**Migration**:
```rust
let config = ConsumerConfig::new("localhost:9092", "analytics-group")
    .performance_tier(ConsumerTier::Buffered { batch_size: 64 })
    .session_timeout(Duration::from_secs(30))
    .custom_property("fetch.min.bytes", "10240")    // 10KB batches
    .custom_property("fetch.max.wait.ms", "500");   // Max wait 500ms
```

### Use Case 3: High-Volume Data Replication

**Characteristics**:
- Maximum throughput (> 80K msg/s)
- Large-scale ETL
- CPU-bound processing

**Recommended Tier**: Dedicated

**Migration**:
```rust
let config = ConsumerConfig::new("localhost:9092", "replication-group")
    .performance_tier(ConsumerTier::Dedicated)
    .session_timeout(Duration::from_secs(45))       // Higher timeout
    .custom_property("fetch.min.bytes", "51200")    // 50KB batches
    .custom_property("fetch.max.wait.ms", "100");   // Low latency
```

## Step-by-Step Migration Process

### Step 1: Identify Current Usage

Audit your codebase to find all Kafka consumer instances:

```bash
# Find all KafkaConsumer usage
grep -r "KafkaConsumer::" src/

# Find all ConsumerFactory usage
grep -r "ConsumerFactory::create" src/

# Check for performance_tier configuration
grep -r "performance_tier" src/
```

### Step 2: Determine Tier Requirements

For each consumer, assess:

1. **Throughput requirements**: How many messages/sec?
2. **Latency tolerance**: Real-time vs batch?
3. **Resource constraints**: CPU/memory available?

Use this decision tree:

```
Is throughput < 20K msg/s?
├─ YES → Standard tier
└─ NO  → Is throughput < 80K msg/s?
         ├─ YES → Buffered tier (batch_size: 32-64)
         └─ NO  → Dedicated tier
```

### Step 3: Update Imports

Add necessary imports to your files:

```rust
use velostream::velostream::kafka::{
    consumer_config::{ConsumerConfig, ConsumerTier},  // Add ConsumerTier
    consumer_factory::ConsumerFactory,
    serialization::JsonSerializer,
};
```

### Step 4: Update Configuration

Modify consumer configuration:

```rust
// Add .performance_tier(...) call
let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Standard)  // or Buffered/Dedicated
    .session_timeout(Duration::from_secs(30))
    // ... other config
```

### Step 5: Update Consumer Creation

Change from direct instantiation to factory:

```rust
// Replace:
// let consumer = KafkaConsumer::with_config(...)?;

// With:
let consumer = ConsumerFactory::create::<String, MyType, _, _>(
    config,
    JsonSerializer,
    JsonSerializer,
)?;
```

### Step 6: Test Locally

Verify the migration works:

```bash
# Run your application locally
cargo run

# Monitor performance
# Check logs for throughput/latency metrics
```

### Step 7: Benchmark Performance

Compare before/after performance:

```rust
use tokio::time::Instant;

let start = Instant::now();
let mut count = 0;

while let Some(result) = stream.next().await {
    match result {
        Ok(_) => count += 1,
        Err(e) => eprintln!("Error: {}", e),
    }

    if count % 10_000 == 0 {
        let elapsed = start.elapsed();
        let msg_per_sec = count as f64 / elapsed.as_secs_f64();
        println!("Throughput: {:.0} msg/s", msg_per_sec);
    }
}
```

### Step 8: Deploy to Staging

Deploy to staging environment and monitor:

- Consumer lag (via `kafka-consumer-groups.sh`)
- CPU usage (should match tier expectations)
- Memory usage
- Message processing latency

### Step 9: Gradual Production Rollout

For production deployments:

1. **Blue-Green Deployment**: Deploy new tier alongside old consumer
2. **Canary Release**: Route 10% traffic to new tier, monitor, increase gradually
3. **Monitor Metrics**: Track lag, throughput, errors
4. **Rollback Plan**: Keep old deployment ready if issues arise

## Common Migration Scenarios

### Scenario 1: Multiple Consumers with Different Needs

**Application**: Trading platform with multiple consumer groups

**Before**:
```rust
// All using same consumer implementation
let market_data_consumer = KafkaConsumer::new(...)?;
let trade_consumer = KafkaConsumer::new(...)?;
let analytics_consumer = KafkaConsumer::new(...)?;
```

**After**:
```rust
// Market data: Real-time, low latency → Standard
let market_config = ConsumerConfig::new("localhost:9092", "market-data")
    .performance_tier(ConsumerTier::Standard);
let market_data_consumer = ConsumerFactory::create(...)?;

// Trades: Real-time processing → Standard
let trade_config = ConsumerConfig::new("localhost:9092", "trades")
    .performance_tier(ConsumerTier::Standard);
let trade_consumer = ConsumerFactory::create(...)?;

// Analytics: High volume, batch → Buffered
let analytics_config = ConsumerConfig::new("localhost:9092", "analytics")
    .performance_tier(ConsumerTier::Buffered { batch_size: 128 });
let analytics_consumer = ConsumerFactory::create(...)?;
```

### Scenario 2: Configuration-Driven Tier Selection

**Application**: Multi-tenant system with configurable performance tiers

```rust
#[derive(Clone, Debug)]
struct ConsumerSettings {
    bootstrap_servers: String,
    group_id: String,
    tier: String,  // "standard" | "buffered" | "dedicated"
    batch_size: Option<usize>,
}

impl ConsumerSettings {
    fn to_consumer_tier(&self) -> ConsumerTier {
        match self.tier.as_str() {
            "standard" => ConsumerTier::Standard,
            "buffered" => ConsumerTier::Buffered {
                batch_size: self.batch_size.unwrap_or(32),
            },
            "dedicated" => ConsumerTier::Dedicated,
            _ => ConsumerTier::Standard,  // Default
        }
    }
}

// Usage
let settings = ConsumerSettings::from_config_file("config.yaml")?;
let config = ConsumerConfig::new(&settings.bootstrap_servers, &settings.group_id)
    .performance_tier(settings.to_consumer_tier());
let consumer = ConsumerFactory::create(...)?;
```

### Scenario 3: Dynamic Tier Switching

**Application**: Adaptive system that switches tiers based on load

**Note**: Currently requires creating a new consumer instance. Future enhancement may support hot tier switching.

```rust
async fn create_adaptive_consumer(
    current_load: f64,  // messages/sec
) -> Result<Box<dyn KafkaStreamConsumer<String, String>>, KafkaError> {
    let tier = if current_load < 20_000.0 {
        ConsumerTier::Standard
    } else if current_load < 80_000.0 {
        ConsumerTier::Buffered { batch_size: 64 }
    } else {
        ConsumerTier::Dedicated
    };

    let config = ConsumerConfig::new("localhost:9092", "adaptive-group")
        .performance_tier(tier);

    ConsumerFactory::create(config, JsonSerializer, JsonSerializer)
}

// Usage
let consumer = create_adaptive_consumer(estimated_load).await?;
```

## Testing Your Migration

### Unit Tests

Create tests for each tier:

```rust
#[tokio::test]
async fn test_standard_tier_migration() {
    let config = ConsumerConfig::new("localhost:9092", "test-group")
        .performance_tier(ConsumerTier::Standard);

    let consumer = ConsumerFactory::create::<String, String, _, _>(
        config,
        JsonSerializer,
        JsonSerializer,
    ).expect("Failed to create Standard tier consumer");

    // Verify consumer can subscribe
    consumer.subscribe(&["test-topic"]).expect("Failed to subscribe");
}

#[tokio::test]
async fn test_buffered_tier_migration() {
    let config = ConsumerConfig::new("localhost:9092", "test-group")
        .performance_tier(ConsumerTier::Buffered { batch_size: 32 });

    let consumer = ConsumerFactory::create::<String, String, _, _>(
        config,
        JsonSerializer,
        JsonSerializer,
    ).expect("Failed to create Buffered tier consumer");

    consumer.subscribe(&["test-topic"]).expect("Failed to subscribe");
}
```

### Integration Tests

Run integration tests with real Kafka (requires Docker):

```bash
# Run all Kafka consumer integration tests
cargo test integration::kafka::kafka_consumer_integration_test -- --ignored

# Run specific tier tests
cargo test test_standard_tier_adapter -- --ignored
cargo test test_buffered_tier_adapter -- --ignored
cargo test test_dedicated_tier_adapter -- --ignored
```

## Troubleshooting

### Issue 1: Consumer Group Rebalancing

**Symptom**: Frequent rebalances after migration to Dedicated tier

**Solution**: Increase session timeout

```rust
let config = ConsumerConfig::new("localhost:9092", "my-group")
    .performance_tier(ConsumerTier::Dedicated)
    .session_timeout(Duration::from_secs(60));  // Increase from default
```

### Issue 2: High CPU Usage

**Symptom**: CPU usage higher than expected with Dedicated tier

**Expected**: Dedicated tier uses 10-15% CPU for continuous polling

**If Excessive**: Move heavy processing to separate tasks

```rust
while let Some(result) = stream.next().await {
    let message = result?;
    tokio::spawn(async move {
        heavy_processing(message).await;
    });
}
```

### Issue 3: Buffered Tier Not Improving Throughput

**Symptom**: Similar throughput after migrating to Buffered tier

**Solutions**:

1. **Increase batch size**:
```rust
.performance_tier(ConsumerTier::Buffered { batch_size: 128 })  // Try larger batches
```

2. **Tune Kafka fetch settings**:
```rust
.custom_property("fetch.min.bytes", "10240")  // Wait for 10KB
.custom_property("fetch.max.wait.ms", "500")   // Max wait 500ms
```

3. **Check Kafka topic partitions**: Ensure enough partitions for parallelism

### Issue 4: Serialization Errors

**Symptom**: "Failed to deserialize from JSON bytes" error

**Cause**: Message format doesn't match serializer expectations

**Solution**: Ensure producers use matching serializers

```rust
// Producer
let producer = KafkaProducer::<String, MyType, _, _>::new(
    "localhost:9092",
    "my-topic",
    JsonSerializer,  // ← Must match consumer
    JsonSerializer,
)?;

// Consumer
let consumer = ConsumerFactory::create::<String, MyType, _, _>(
    config,
    JsonSerializer,  // ← Must match producer
    JsonSerializer,
)?;
```

## Rollback Procedure

If you encounter issues after migration:

### Quick Rollback

Remove the `.performance_tier()` call to revert to Legacy tier:

```rust
// Rollback: Remove this line
// .performance_tier(ConsumerTier::Standard)

let config = ConsumerConfig::new("localhost:9092", "my-group");
// Automatically uses Legacy tier
```

### Gradual Rollback

For production environments:

1. **Deploy Legacy Version**: Redeploy previous version without tier config
2. **Monitor Consumer Lag**: Ensure lag decreases
3. **Verify Message Processing**: Check application logs
4. **Document Issue**: Record what went wrong for future fix

## Best Practices

1. **Start with Standard tier** for new migrations unless you have specific performance needs
2. **Benchmark in staging** before production deployment
3. **Monitor consumer lag** after migration using `kafka-consumer-groups.sh`
4. **Use manual commit** with Buffered/Dedicated tiers for better control
5. **Test tier changes** with real production-like load
6. **Document tier choice** in code comments explaining why that tier was selected
7. **Set up alerting** for consumer lag, CPU usage, and error rates

## Performance Expectations After Migration

| Tier | Throughput Increase | Latency Change | CPU Impact | Memory Impact |
|------|---------------------|----------------|------------|---------------|
| Legacy → Standard | +2-3x | Same (~1ms) | +0-2% | Minimal |
| Standard → Buffered | +4-5x | +<1ms | +1-3% | +10-20MB |
| Buffered → Dedicated | +2x | -10-20% | +5-10% | +5-10MB |

## Next Steps

After successful migration:

1. **Review [Kafka Consumer Performance Tiers](kafka-consumer-performance-tiers.md)** for optimization tips
2. **Check [Kafka Performance Configuration](kafka-performance-configs.md)** for advanced tuning
3. **Explore Phase 2C** for producer performance tiers (upcoming)

## Support

For migration assistance:
- **Documentation**: [Velostream Docs](../README.md)
- **Issues**: [GitHub Issues](https://github.com/anthropics/velostream/issues)
- **Examples**: See `examples/typed_kafka_example.rs` for working code
