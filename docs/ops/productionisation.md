# Productionisation Guide

This guide covers best practices for deploying and running the Kafka client library in production environments.

## Error Handling & Monitoring

### Error Context Tracking
The library provides comprehensive error context tracking for production debugging:

**See:** [`tests/unit/error_context_test.rs`](../tests/unit/error_context_test.rs) - Complete error context implementation

```rust
use velo::kafka::*;

// Error context includes operation details, timestamps, and correlation IDs
let context = ErrorContext::new("send_message", "user-events")
    .with_metadata("key", "user-12345")
    .with_metadata("partition", "3")
    .with_metadata("retry_attempt", "2");
```

### Error Categorization & Metrics
Errors are automatically categorized by severity for alerting:

- **Critical**: Serialization errors, broker unavailable
- **Warning**: Timeouts, connection issues  
- **Info**: No messages available, expected conditions

**See:** [`tests/unit/error_context_test.rs:252-268`](../tests/unit/error_context_test.rs) - Error severity categorization tests
**See:** [`tests/unit/error_context_test.rs:315-357`](../tests/unit/error_context_test.rs) - Error metrics collection implementation

## Retry & Recovery Patterns

### Client-Side Retry Logic
Implement retry logic with exponential backoff for transient failures:

**See:** [`tests/integration/failure_recovery_test.rs:9-85`](../../tests/integration/failure_recovery_test.rs) - Retry logic test
**See:** [`tests/integration/failure_recovery_test.rs:154-201`](../../tests/integration/failure_recovery_test.rs) - Exponential backoff implementation

```rust
let mut delay = Duration::from_millis(100);
for attempt in 1..=max_retries {
    match producer.send(key, message, headers, None).await {
        Ok(_) => break,
        Err(e) => {
            if attempt < max_retries {
                sleep(delay).await;
                delay *= 2; // Exponential backoff
            }
        }
    }
}
```

### Partial Broker Failures
Handle scenarios where some brokers are down while others remain operational:

**See:** [`tests/integration/failure_recovery_test.rs:205-302`](../../tests/integration/failure_recovery_test.rs) - Partial broker failure scenarios


## Performance Optimization

### High-Performance Examples
Review performance-optimized implementations:

**See:** [`examples/performance/`](../examples/performance/) - Performance test suite including:
- [`json_performance_test.rs`](../../examples/performance/json_performance_test.rs) - JSON serialization performance
- [`latency_performance_test.rs`](../../examples/performance/latency_performance_test.rs) - Latency optimization
- [`simple_zero_copy_test.rs`](../../examples/performance/simple_zero_copy_test.rs) - Zero-copy implementations
- [`resource_monitoring_test.rs`](../../examples/performance/resource_monitoring_test.rs) - Resource usage tracking

### Client Instance Management
Reuse producer/consumer instances rather than creating new ones per operation:

```rust
// Good: Create once, reuse many times
let producer = KafkaProducer::new(broker, topic, serializer, serializer)?;
for message in messages {
    producer.send(key, message, headers, None).await?;
}

// Bad: Creating new instances per operation
for message in messages {
    let producer = KafkaProducer::new(broker, topic, serializer, serializer)?;
    producer.send(key, message, headers, None).await?;
}
```

**See:** [`tests/integration/failure_recovery_test.rs:205-302`](../../tests/integration/failure_recovery_test.rs) - Multiple client instance patterns

### Configuration Examples
**See:** [`examples/builder_configuration.rs`](../../examples/builder_configuration.rs) - Advanced configuration patterns
**See:** [`examples/fluent_api_example.rs`](../../examples/fluent_api_example.rs) - Fluent API usage for production

## Message Handling

### Headers & Metadata
Implement proper message headers for tracing and routing:

**See:** [`examples/headers_example.rs`](../../examples/headers_example.rs) - Complete headers implementation
**See:** [`examples/consumer_with_headers.rs`](../../examples/consumer_with_headers.rs) - Consumer-side header processing
**See:** [`examples/message_metadata_example.rs`](../../examples/message_metadata_example.rs) - Message metadata patterns

### Type Safety
Use type-safe Kafka patterns for production reliability:

**See:** [`examples/typed_kafka_example.rs`](../../examples/typed_kafka_example.rs) - Type-safe Kafka implementation

### Transactions & Exactly-Once Semantics
Enable true exactly-once processing with consumer-producer coordination:

**See:** [`tests/integration/transaction_test.rs`](../tests/integration/transaction_test.rs) - Complete transaction test suite

#### Producer-Only Transactions
For atomic batch processing without consumer coordination:

```rust
// Configure transactional producer
let config = ProducerConfig::new("localhost:9092", "topic")
    .transactional("my-transaction-id")
    .idempotence(true)
    .acks(AckMode::All);

let producer = KafkaProducer::with_config(config, serializer, serializer)?;

// Send multiple messages atomically
producer.begin_transaction().await?;
for message in messages {
    producer.send(key, &message, headers, None).await?;
}
producer.commit_transaction().await?;
```

#### True Exactly-Once Semantics (Consumer + Producer)
For exactly-once processing pipelines that consume, transform, and produce:

```rust
use futures::StreamExt;

// Configure consumer for exactly-once
let consumer_config = ConsumerConfig::new("brokers", "group-id")
    .isolation_level(IsolationLevel::ReadCommitted) // Only read committed transactions
    .auto_commit(false, Duration::from_secs(5));     // Manual offset management

let consumer = KafkaConsumer::with_config(consumer_config, key_ser, val_ser)?;

// Configure transactional producer
let producer_config = ProducerConfig::new("brokers", "output-topic")
    .transactional("processor-tx-id")
    .idempotence(true)
    .acks(AckMode::All);

let producer = KafkaProducer::with_config(producer_config, key_ser, val_ser)?;

consumer.subscribe(&["input-topic"])?;

// Process messages with exactly-once guarantee using streams
let mut stream = consumer.stream();

while let Some(message_result) = stream.next().await {
    match message_result {
        Ok(message) => {
            // Begin transaction for each message
            producer.begin_transaction().await?;
            
            // Transform message
            let processed = transform(&message);
            
            // Send processed message
            producer.send(key, &processed, headers, None).await?;
            
            // Coordinate consumer offsets with transaction (KEY STEP)
            let offsets = consumer.current_offsets()?;
            producer.send_offsets_to_transaction(&offsets, consumer.group_id()).await?;
            
            // Commit both message and offset atomically
            producer.commit_transaction().await?;
        }
        Err(e) => {
            println!("Stream processing error: {:?}", e);
            // Abort transaction on error
            producer.abort_transaction().await?;
            break;
        }
    }
}
```

**Key Exactly-Once Features:**
- **Consumer Isolation**: `read_committed` ensures only committed messages are processed
- **Atomic Processing**: Message production and offset commits in single transaction
- **Failure Recovery**: Transaction abort/retry with no duplicate processing
- **Cross-Topic Coordination**: Input and output topics coordinated atomically
- **Idempotent Processing**: Safe retries without data duplication

**⚠️ Current Limitation:**
The `send_offsets_to_transaction()` method is currently a placeholder implementation. Full exactly-once semantics (EoS) with consumer-producer coordination requires proper integration with the underlying rust-rdkafka library's transaction APIs, which is not yet fully implemented. The current implementation provides:
- Producer-side transactions (atomic batching)
- Consumer isolation levels (`read_committed`)
- Transaction lifecycle management (`begin`, `commit`, `abort`)

For true exactly-once processing, the offset coordination step needs to be completed when rust-rdkafka's transaction support is fully available.

#### Alternative: Polling-Based Processing
For scenarios where you need more control over message consumption timing:

```rust
// Process messages using polling instead of streams
loop {
    match consumer.poll(Duration::from_secs(1)).await {
        Ok(message) => {
            // Begin transaction for each message
            producer.begin_transaction().await?;
            
            // Transform message
            let processed = transform(&message);
            
            // Send processed message
            producer.send(key, &processed, headers, None).await?;
            
            // Coordinate offsets with transaction
            let offsets = consumer.current_offsets()?;
            producer.send_offsets_to_transaction(&offsets, consumer.group_id()).await?;
            
            // Commit atomically
            producer.commit_transaction().await?;
        }
        Err(KafkaClientError::NoMessage) => break,
        Err(e) => {
            println!("Polling error: {:?}", e);
            producer.abort_transaction().await?;
            return Err(e);
        }
    }
}
```

**Stream vs Polling Comparison:**
- **Stream Processing**: Reactive, composable, natural backpressure
- **Polling**: Explicit timeout control, traditional loop-based processing
- **Both Support**: Same exactly-once guarantees and transaction coordination

**See Test Examples:**
- [`test_exactly_once_consumer_producer_coordination()`](../tests/integration/transaction_test.rs) - End-to-end exactly-once processing (placeholder)
- [`test_exactly_once_with_failure_recovery()`](../tests/integration/transaction_test.rs) - Failure recovery without duplicates (placeholder)
- [`test_exactly_once_with_consumer_stream()`](../tests/integration/transaction_test.rs) - Stream-based exactly-once processing (placeholder)
- [`test_exactly_once_stream_with_error_handling()`](../tests/integration/transaction_test.rs) - Stream error handling and recovery (placeholder)

**Note:** The test examples demonstrate the API design and structure for exactly-once processing, but the actual offset coordination is not yet functional due to the placeholder implementation.

## State Management with KTables

For applications requiring materialized views of Kafka topics, use KTables for efficient state management:

**See:** [`tests/integration/ktable_test.rs`](../../tests/integration/ktable_test.rs) - Complete KTable test suite

### Basic KTable Usage

```rust
use velostream::velo::kafka::*;
use velostream::velo::kafka::consumer_config::{OffsetReset, IsolationLevel};

// Create a KTable from a compacted topic
let config = ConsumerConfig::new("localhost:9092", "user-table-group")
    .auto_offset_reset(OffsetReset::Earliest)
    .isolation_level(IsolationLevel::ReadCommitted);

let user_table = KTable::new(
    config,
    "users".to_string(),
    JsonSerializer,
    JsonSerializer,
).await?;

// Start consuming and building state in background
let table_clone = user_table.clone();
tokio::spawn(async move {
    table_clone.start().await
});

// Query current state
let user = user_table.get(&"user-123".to_string());
let all_users = user_table.snapshot();
```

### KTable Features

- **Materialized Views**: Automatic state rebuilding from compacted topics
- **Real-time Queries**: Fast key-based lookups with O(1) complexity
- **State Transformations**: Built-in `map_values()` and `filter()` operations
- **Lifecycle Management**: Start/stop consumption with `start()` and `stop()`
- **Statistics**: Monitor table size and last update times
- **Thread Safety**: Share tables across threads with `Clone`

### Stream-Table Joins

Combine KTables with stream processing for enrichment:

```rust
// User profile table
let user_table = KTable::new(config, "users".to_string(), serializer, serializer).await?;

// Process order stream with user enrichment
let mut order_stream = order_consumer.stream();
while let Some(order_result) = order_stream.next().await {
    if let Ok(order) = order_result {
        // Enrich order with user profile
        if let Some(user) = user_table.get(order.value().user_id) {
            let enriched_order = EnrichedOrder {
                order: order.value().clone(),
                user_profile: user,
            };
            // Process enriched order
        }
    }
}
```

### KTable Best Practices

1. **Use Compacted Topics**: Configure source topics with `cleanup.policy=compact`
2. **Read Committed**: Use `IsolationLevel::ReadCommitted` for transactional consistency
3. **Background Processing**: Run table updates in dedicated background tasks
4. **Memory Management**: Monitor table size for large datasets
5. **Error Handling**: Implement retry logic for table startup failures

## Testing Strategies

### Integration Testing
Comprehensive integration tests that handle Kafka availability:

**See:** [`tests/integration/kafka_integration_test.rs`](../tests/integration/kafka_integration_test.rs) - Basic integration patterns
**See:** [`tests/integration/kafka_advanced_test.rs`](../../tests/integration/kafka_advanced_test.rs) - Advanced integration scenarios

### Unit Testing Patterns
**See:** [`tests/unit/`](../tests/unit/) - Complete unit test suite including:
- [`builder_pattern_test.rs`](../tests/unit/builder_pattern_test.rs) - Configuration validation
- [`config_validation_test.rs`](../tests/unit/config_validation_test.rs) - Config validation patterns
- [`error_handling_test.rs`](../tests/unit/error_handling_test.rs) - Error handling validation
- [`serialization_unit_test.rs`](../tests/unit/serialization_unit_test.rs) - Serialization testing

## Deployment Considerations

### Configuration Management
Use builder patterns for environment-specific configuration:

```rust
// See examples/builder_configuration.rs for complete patterns
let config = ProducerConfig::new("broker:9092", "topic")
    .batch_size(16384)
    .linger_ms(100)
    .compression_type("gzip")
    .request_timeout(Duration::from_secs(30));
```

### Health Checks

#### Using VeloStream CLI (Recommended)
For production environments, use the built-in CLI tool for comprehensive health monitoring:

```bash
# Quick health check of all components
./velo-cli --remote --sql-host prod-server.com health

# Detailed monitoring with real-time updates
./velo-cli --remote --sql-host prod-server.com --sql-port 8080 status --refresh 10

# Check specific components
./velo-cli --remote --sql-host prod-server.com jobs --sql --topics
```

#### Programmatic Health Checks
For custom health check implementations:

```rust
async fn kafka_health_check() -> Result<(), KafkaClientError> {
    let producer = KafkaProducer::new(broker_addr, topic, serializer, serializer)?;
    let test_message = TestMessage::new(0, "health_check");
    producer.send(None, &test_message, Headers::new(), None).await?;
    Ok(())
}
```

### Resource Management
**See:** [`examples/performance/resource_monitoring_test.rs`](../../examples/performance/resource_monitoring_test.rs) - Resource usage monitoring

## Observability

### Correlation ID Tracking
Track requests across distributed systems:

**See:** [`tests/unit/error_context_test.rs:359-394`](../tests/unit/error_context_test.rs) - Correlation ID tracking implementation

### Error Aggregation
Collect and analyze error patterns:

**See:** [`tests/unit/error_context_test.rs:396-434`](../tests/unit/error_context_test.rs) - Error aggregation patterns

### Retry Mechanism Monitoring
Track retry attempts and patterns:

**See:** [`tests/unit/error_context_test.rs:437-481`](../tests/unit/error_context_test.rs) - Retry mechanism with error context

## Quick Start Scripts

### Testing Your Setup
```bash
# Test basic Kafka connectivity
./test-kafka.sh

# Test typed Kafka patterns  
./test-typed-kafka.sh

# Run performance comparison
./run_performance_comparison.sh
```

## Advanced Patterns

### Consumer Resilience
Handle consumer failures gracefully:

**See:** [`tests/integration/failure_recovery_test.rs:88-151`](../../tests/integration/failure_recovery_test.rs) - Consumer graceful degradation

### Error Chain Analysis
Deep error analysis for production debugging:

**See:** [`tests/unit/error_context_test.rs:60-117`](../tests/unit/error_context_test.rs) - Comprehensive error source chain preservation

### Operational Metadata
Track detailed operational context:

**See:** [`tests/unit/error_context_test.rs:119-224`](../tests/unit/error_context_test.rs) - Operational error metadata tracking

## Documentation References

- [Performance Configurations](../developer/KAFKA_PERFORMANCE_CONFIGS.md) - Detailed performance tuning
- [Headers Guide](../developer/HEADERS_GUIDE.md) - Message header patterns
- [Builder Pattern Guide](../developer/BUILDER_PATTERN_GUIDE.md) - Configuration patterns
- [Type-Safe Kafka](../feature/TYPE_SAFE_KAFKA.md) - Type safety patterns

## Support

For production support and advanced configurations, refer to the test coverage improvement plan:
**See:** [Test Coverage Improvement Plan](../feature/TEST_COVERAGE_IMPROVEMENT_PLAN.md)