# Kafka Consumer Migration Plan

## Executive Summary

Migrate from `StreamConsumer`-based `KafkaConsumer` to `BaseConsumer`-based `kafka_fast_consumer`
with unified API and configuration-driven consumer selection.

## Current vs New Architecture

### Current: kafka_consumer.rs (StreamConsumer)
```rust
KafkaConsumer<K, V, KS, VS>
├── Based on: rdkafka::StreamConsumer
├── API Methods:
│   ├── poll(timeout) -> Result<Message<K, V>>
│   ├── stream() -> impl Stream<Item = Result<Message<K, V>>>
│   ├── raw_stream() -> MessageStream
│   ├── commit() -> Result<()>
│   ├── current_offsets() -> Result<TopicPartitionList>
│   └── subscribe(topics) -> Result<()>
├── Configuration: Integrated with ConsumerConfig
├── Builder Pattern: ConsumerBuilder
└── Performance: Moderate (async overhead)
```

**Strengths:**
- ✅ Comprehensive API with commit/offset management
- ✅ Full ConsumerConfig integration
- ✅ Builder pattern for flexible construction
- ✅ Well-documented with examples
- ✅ Transaction coordination support

**Weaknesses:**
- ❌ StreamConsumer adds async overhead
- ❌ Single performance tier (no optimization options)
- ❌ Less control over polling behavior

### New: kafka_fast_consumer.rs (BaseConsumer)
```rust
Consumer<K, V>
├── Based on: rdkafka::BaseConsumer (faster, lower-level)
├── Three Performance Tiers:
│   ├── stream() → KafkaStream (1ms latency, 10K msg/s)
│   ├── buffered_stream(batch_size) → BufferedKafkaStream (50K+ msg/s)
│   └── dedicated_stream() → DedicatedKafkaStream (100K+ msg/s)
├── Direct Methods:
│   ├── poll_blocking(timeout) -> Result<Message<K, V>>
│   └── try_poll() -> Result<Option<Message<K, V>>>
├── Configuration: Simple constructor (no config integration)
└── Performance: High (minimal overhead)
```

**Strengths:**
- ✅ 5-10x higher throughput potential
- ✅ Multiple performance tiers for different use cases
- ✅ No CPU spinning (fixed critical bug)
- ✅ Dedicated message processing function (DRY)
- ✅ Comprehensive documentation

**Weaknesses:**
- ❌ No ConsumerConfig integration
- ❌ No commit/offset management
- ❌ No builder pattern
- ❌ No transaction coordination support

## Migration Strategy

### Phase 1: Unified Consumer Trait (Week 1)

Create a trait that both implementations can satisfy:

```rust
/// Unified Kafka consumer interface supporting both StreamConsumer and BaseConsumer
pub trait KafkaStreamConsumer<K, V>: Send + Sync {
    /// Get a stream of deserialized messages
    fn stream(&self) -> impl Stream<Item = Result<Message<K, V>, ConsumerError>> + '_;

    /// Subscribe to topics
    fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError>;

    /// Commit current consumer state (if supported)
    fn commit(&self) -> Result<(), KafkaError> {
        Ok(()) // Default: no-op for consumers without commit support
    }

    /// Get current offsets (if supported)
    fn current_offsets(&self) -> Result<Option<rdkafka::TopicPartitionList>, KafkaError> {
        Ok(None) // Default: not supported
    }
}
```

**Files to create:**
- `src/velostream/kafka/unified_consumer.rs` - Trait definition
- Implement for both `KafkaConsumer` and `Consumer`

### Phase 2: Configuration Extension (Week 1-2)

Add configuration support to `kafka_fast_consumer`:

```rust
/// Consumer performance tier selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerTier {
    /// Basic streaming (1ms latency, moderate throughput)
    Standard,
    /// Buffered batching (high throughput, configurable batch size)
    Buffered { batch_size: usize },
    /// Dedicated thread (maximum throughput, one thread per consumer)
    Dedicated,
}

/// Extended consumer configuration
pub struct FastConsumerConfig {
    pub tier: ConsumerTier,
    pub base_config: ConsumerConfig,
}
```

**Add to ConsumerConfig:**
```rust
// In consumer_config.rs
pub struct ConsumerConfig {
    // ... existing fields ...

    /// Performance tier (None = use legacy StreamConsumer)
    pub performance_tier: Option<ConsumerTier>,
}
```

**Files to modify:**
- `src/velostream/kafka/consumer_config.rs` - Add performance_tier field
- `src/velostream/kafka/kafka_fast_consumer.rs` - Add `with_config()` constructor

### Phase 3: Feature Parity (Week 2)

Add missing features to `kafka_fast_consumer`:

```rust
impl<K, V> Consumer<K, V> {
    /// Create from ConsumerConfig (feature parity)
    pub fn with_config(
        config: ConsumerConfig,
        key_serializer: Box<dyn Serde<K> + Send + Sync>,
        value_serializer: Box<dyn Serde<V> + Send + Sync>,
    ) -> Result<Self, KafkaError> {
        // Build BaseConsumer from config
        // ...
    }

    /// Subscribe to topics (feature parity)
    pub fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError> {
        self.consumer.subscribe(topics)
    }

    /// Commit consumer state (feature parity)
    pub fn commit(&self) -> Result<(), KafkaError> {
        use rdkafka::consumer::{Consumer as RdKafkaConsumer, CommitMode};
        self.consumer.commit_consumer_state(CommitMode::Sync)
    }

    /// Get current offsets (feature parity)
    pub fn current_offsets(&self) -> Result<rdkafka::TopicPartitionList, KafkaError> {
        use rdkafka::consumer::Consumer as RdKafkaConsumer;
        self.consumer.assignment()
    }
}
```

**Files to modify:**
- `src/velostream/kafka/kafka_fast_consumer.rs` - Add methods

### Phase 4: Unified Factory (Week 2-3)

Create a factory that returns trait objects based on configuration:

```rust
/// Factory for creating consumers based on configuration
pub enum ConsumerFactory {}

impl ConsumerFactory {
    /// Create a consumer from configuration
    pub fn create<K, V, KS, VS>(
        config: ConsumerConfig,
        key_serializer: KS,
        value_serializer: VS,
    ) -> Result<Box<dyn KafkaStreamConsumer<K, V>>, KafkaError>
    where
        K: Send + Sync + 'static,
        V: Send + Sync + 'static,
        KS: Serde<K> + Send + Sync + 'static,
        VS: Serde<V> + Send + Sync + 'static,
    {
        match config.performance_tier {
            None => {
                // Legacy StreamConsumer
                let consumer = KafkaConsumer::with_config(
                    config,
                    key_serializer,
                    value_serializer,
                )?;
                Ok(Box::new(consumer))
            }
            Some(ConsumerTier::Standard) => {
                // FastConsumer with standard tier
                let consumer = Consumer::with_config(
                    config,
                    Box::new(key_serializer),
                    Box::new(value_serializer),
                )?;
                Ok(Box::new(StandardConsumerAdapter::new(consumer)))
            }
            Some(ConsumerTier::Buffered { batch_size }) => {
                let consumer = Consumer::with_config(
                    config,
                    Box::new(key_serializer),
                    Box::new(value_serializer),
                )?;
                Ok(Box::new(BufferedConsumerAdapter::new(consumer, batch_size)))
            }
            Some(ConsumerTier::Dedicated) => {
                let consumer = Arc::new(Consumer::with_config(
                    config,
                    Box::new(key_serializer),
                    Box::new(value_serializer),
                )?);
                Ok(Box::new(DedicatedConsumerAdapter::new(consumer)))
            }
        }
    }
}
```

**Files to create:**
- `src/velostream/kafka/consumer_factory.rs` - Factory implementation
- `src/velostream/kafka/consumer_adapters.rs` - Adapters for each tier

### Phase 5: Testing Strategy (Week 3)

#### 5.1 Unit Tests
```rust
// tests/unit/kafka/fast_consumer_test.rs

#[cfg(test)]
mod fast_consumer_tests {
    #[test]
    fn test_consumer_creation() {
        // Test all three tiers can be created
    }

    #[test]
    fn test_message_processing() {
        // Test message deserialization
    }

    #[test]
    fn test_error_handling() {
        // Test error propagation
    }
}
```

#### 5.2 Integration Tests

Create Kafka integration test infrastructure:

```rust
// tests/integration/kafka/mod.rs

use testcontainers::{clients::Cli, images::kafka::Kafka};

pub struct KafkaTestEnv {
    docker: Cli,
    kafka: Kafka,
}

impl KafkaTestEnv {
    pub fn new() -> Self {
        let docker = Cli::default();
        let kafka = Kafka::default();
        Self { docker, kafka }
    }

    pub async fn produce_test_messages(&self, topic: &str, count: usize) {
        // Produce test messages
    }

    pub fn bootstrap_servers(&self) -> String {
        // Return bootstrap servers
    }
}

#[tokio::test]
async fn test_standard_consumer_integration() {
    let env = KafkaTestEnv::new();

    // Create consumer
    let consumer = Consumer::new(
        env.kafka.get_host_port(),
        Box::new(JsonSerializer),
        Box::new(JsonSerializer),
    );

    consumer.subscribe(&["test-topic"]).unwrap();

    // Produce messages
    env.produce_test_messages("test-topic", 100).await;

    // Consume and verify
    let mut stream = consumer.stream();
    let mut count = 0;

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => count += 1,
            Err(e) => panic!("Error: {:?}", e),
        }

        if count >= 100 {
            break;
        }
    }

    assert_eq!(count, 100);
}

#[tokio::test]
async fn test_buffered_consumer_throughput() {
    // Test buffered consumer with batch_size=32
    // Verify higher throughput than standard
}

#[tokio::test]
async fn test_dedicated_consumer_maximum_throughput() {
    // Test dedicated thread consumer
    // Verify highest throughput of all tiers
}

#[tokio::test]
async fn test_consumer_tier_comparison() {
    // Benchmark all three tiers side-by-side
    // Document performance characteristics
}
```

**Dependencies to add:**
```toml
[dev-dependencies]
testcontainers = "0.15"
```

#### 5.3 Performance Tests
```rust
// tests/performance/consumer_benchmark.rs

#[tokio::test]
async fn benchmark_consumer_tiers() {
    let test_message_count = 10_000;

    // Benchmark standard tier
    let start = Instant::now();
    // ... consume messages ...
    let standard_duration = start.elapsed();

    // Benchmark buffered tier
    let start = Instant::now();
    // ... consume messages ...
    let buffered_duration = start.elapsed();

    // Benchmark dedicated tier
    let start = Instant::now();
    // ... consume messages ...
    let dedicated_duration = start.elapsed();

    println!("Performance Results:");
    println!("  Standard:  {:?} ({:.0} msg/s)",
        standard_duration,
        test_message_count as f64 / standard_duration.as_secs_f64()
    );
    println!("  Buffered:  {:?} ({:.0} msg/s)",
        buffered_duration,
        test_message_count as f64 / buffered_duration.as_secs_f64()
    );
    println!("  Dedicated: {:?} ({:.0} msg/s)",
        dedicated_duration,
        test_message_count as f64 / dedicated_duration.as_secs_f64()
    );
}
```

### Phase 6: Backwards Compatibility (Week 3-4)

Keep existing API but delegate to new implementation:

```rust
// In kafka_consumer.rs

impl<K, V, KS, VS, C> KafkaConsumer<K, V, KS, VS, C> {
    /// Migrate to fast consumer (opt-in)
    pub fn into_fast_consumer(self) -> Consumer<K, V> {
        // Convert StreamConsumer to BaseConsumer
        // This may require re-subscribing
    }
}
```

## Configuration Examples

### Legacy Configuration (No Change)
```yaml
# Uses StreamConsumer (existing behavior)
consumer:
  brokers: "localhost:9092"
  group_id: "my-group"
  # performance_tier: not specified = use StreamConsumer
```

### Standard Tier (Low Latency)
```yaml
consumer:
  brokers: "localhost:9092"
  group_id: "my-group"
  performance_tier: "standard"  # KafkaStream
```

### Buffered Tier (High Throughput)
```yaml
consumer:
  brokers: "localhost:9092"
  group_id: "my-group"
  performance_tier:
    buffered:
      batch_size: 32  # BufferedKafkaStream
```

### Dedicated Tier (Maximum Performance)
```yaml
consumer:
  brokers: "localhost:9092"
  group_id: "my-group"
  performance_tier: "dedicated"  # DedicatedKafkaStream
```

## Migration Path for Existing Code

### Step 1: No Changes (Default)
Existing code continues to work unchanged - uses StreamConsumer.

### Step 2: Opt-In via Config
Update configuration to use faster consumer:
```rust
let mut config = ConsumerConfig::new(brokers, group_id);
config.performance_tier = Some(ConsumerTier::Standard);
```

### Step 3: Direct Usage (Advanced)
Use `Consumer` directly for maximum control:
```rust
use velostream::kafka::kafka_fast_consumer::Consumer;

let consumer = Consumer::new(
    base_consumer,
    Box::new(value_serializer),
    Box::new(key_serializer),
);

// Choose tier based on use case
match use_case {
    UseCase::RealTime => consumer.stream(),
    UseCase::Analytics => consumer.buffered_stream(32),
    UseCase::HighVolume => Arc::new(consumer).dedicated_stream(),
}
```

## Decision Matrix: Which Consumer Tier?

| Use Case | Volume | Latency Need | CPU Budget | Recommended Tier |
|----------|--------|--------------|------------|------------------|
| Real-time events | Low-Medium | <5ms | Low | **Standard** |
| Analytics pipeline | High | <100ms | Low | **Buffered (32-64)** |
| Data ingestion | Very High | <100ms | Medium | **Buffered (128+)** |
| Critical pipeline | Very High | <1ms | Medium-High | **Dedicated** |
| Legacy systems | Any | Any | Any | **StreamConsumer** |

## Stream Interface Compatibility

**Q: Do we need to support the "stream" interface like in the old version?**

**A: YES** - The stream interface is critical for:
1. **Reactive programming patterns** - Most existing code uses `consumer.stream()`
2. **Functional composition** - `.filter()`, `.map()`, `.for_each()` patterns
3. **Integration with futures ecosystem**
4. **Backwards compatibility**

Both implementations already provide this:
- **Old**: `consumer.stream()` returns `impl Stream`
- **New**: `consumer.stream()` returns `KafkaStream` which implements `Stream`

The trait-based approach ensures both satisfy the same interface.

## Testing Priority

### Phase 1: Core Functionality ✅
1. Basic message consumption
2. Serialization/deserialization
3. Error handling
4. Subscribe/commit operations

### Phase 2: Integration ✅
5. Kafka integration tests (testcontainers)
6. Multi-tier comparison tests
7. Configuration-driven consumer selection

### Phase 3: Performance ✅
8. Throughput benchmarks
9. Latency measurements
10. CPU usage profiling

### Phase 4: Regression ✅
11. Existing code compatibility
12. Migration smoke tests

## Implementation Checklist

### Week 1
- [ ] Create `KafkaStreamConsumer` trait
- [ ] Implement trait for `KafkaConsumer`
- [ ] Implement trait for `Consumer`
- [ ] Add `ConsumerTier` enum to `ConsumerConfig`
- [ ] Add `with_config()` to `kafka_fast_consumer`

### Week 2
- [ ] Add `subscribe()`, `commit()`, `current_offsets()` to `Consumer`
- [ ] Create `ConsumerFactory`
- [ ] Create tier adapters
- [ ] Write unit tests for each tier

### Week 3
- [ ] Set up testcontainers for integration tests
- [ ] Write basic integration test
- [ ] Write tier comparison test
- [ ] Write performance benchmarks
- [ ] Document migration guide

### Week 4
- [ ] Add backwards compatibility layer
- [ ] Update existing examples
- [ ] Update documentation
- [ ] Create migration FAQ
- [ ] Prepare for production rollout

## Risk Mitigation

### Risk 1: Performance Regression
**Mitigation**: Keep StreamConsumer as default, opt-in migration via config

### Risk 2: API Breaking Changes
**Mitigation**: Trait-based abstraction maintains API compatibility

### Risk 3: Missing Features
**Mitigation**: Feature parity checklist, comprehensive testing

### Risk 4: Integration Issues
**Mitigation**: Start with integration tests early, validate against real Kafka

## Success Criteria

✅ **Functional**
- All existing tests pass
- New integration tests pass
- No breaking changes to public API

✅ **Performance**
- Standard tier: 10K+ msg/s
- Buffered tier: 50K+ msg/s
- Dedicated tier: 100K+ msg/s

✅ **Documentation**
- Migration guide complete
- Performance tuning guide
- Configuration examples

✅ **Adoption**
- Existing code works unchanged
- Easy opt-in migration path
- Clear decision matrix for tier selection

## Recommended First Steps

1. **Start with Integration Test** ✅
   - Set up testcontainers
   - Write basic consume/produce test
   - Validate BaseConsumer behavior

2. **Create Unified Trait** ✅
   - Define `KafkaStreamConsumer`
   - Implement for both consumers
   - Verify API compatibility

3. **Add Configuration Support** ✅
   - Extend `ConsumerConfig`
   - Add tier selection
   - Implement factory pattern

4. **Incremental Migration** ✅
   - Test one tier at a time
   - Compare against existing consumer
   - Document performance gains
