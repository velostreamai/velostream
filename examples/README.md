# ferrisstreams Examples

This directory contains comprehensive examples demonstrating the full capabilities of ferrisstreams, a type-safe Rust Kafka client library.

## 🚀 Quick Start

Before running any examples, ensure Kafka is running:

```bash
# Start Kafka (Docker Compose recommended)
docker-compose up -d

# Or use the test script
./test-kafka.sh
```

## 📚 Examples Overview

### Basic Usage

#### 1. [typed_kafka_example.rs](typed_kafka_example.rs)
**Basic Producer/Consumer Example**
- Core functionality introduction
- Type-safe producers and consumers with JSON serialization
- Polling-based message consumption
- Basic error handling and offset management

```bash
cargo run --example typed_kafka_example
```

**What you'll learn:**
- Creating producers and consumers with direct constructors
- Automatic JSON serialization/deserialization
- Processing structured messages (OrderEvent)
- Handling different order statuses

---

#### 2. [headers_example.rs](headers_example.rs)
**Headers Usage Example**
- Working with Kafka message headers
- Header creation with builder pattern
- Accessing and iterating over headers

```bash
cargo run --example headers_example
```

**What you'll learn:**
- Creating headers with metadata (source, version, trace-id)
- Sending messages with headers
- Consuming and accessing header information
- Common header patterns for production use

---

#### 3. [consumer_with_headers.rs](consumer_with_headers.rs)
**Comprehensive Consumer with Headers**
- Advanced header processing patterns
- Multiple message types with varied headers
- Production-ready header usage

```bash
cargo run --example consumer_with_headers
```

**What you'll learn:**
- Complex header creation and processing
- Message routing based on headers
- Different extraction patterns (references vs owned)
- UUID-based topic generation for isolation

---

### Advanced Usage

#### 4. [builder_configuration.rs](builder_configuration.rs)
**Advanced Builder Configuration**
- Builder patterns for flexible configuration
- Performance presets for different use cases
- Custom configuration options

```bash
cargo run --example builder_configuration
```

**What you'll learn:**
- When to use builders vs direct constructors
- Performance presets: `high_throughput()`, `low_latency()`, `max_durability()`, `development()`
- Custom configuration with compression, batching, retries
- Consumer configuration patterns

**Configuration types demonstrated:**
- **Basic**: Default settings for most use cases
- **High-Throughput**: Optimized for maximum message volume
- **Low-Latency**: Optimized for minimal delivery time
- **Max-Durability**: Optimized for data safety
- **Custom**: Manual configuration of all parameters

---

#### 5. [fluent_api_example.rs](fluent_api_example.rs) ⭐
**Fluent API & Stream Processing** (Recommended for Production)
- Stream-based message consumption
- Functional programming patterns with method chaining
- Complex filtering and transformation operations
- Asynchronous stream processing pipelines

```bash
cargo run --example fluent_api_example
```

**What you'll learn:**
- **Stream processing vs polling** - Why streams are recommended
- **Filtering** by headers (priority, source, region) and content
- **Transforming** messages to business objects
- **Collecting, folding, and reducing** streams
- **Error handling** in stream contexts
- **Async processing** with priority-based timing
- **Method chaining** for readable, maintainable code

**7 Stream Processing Patterns:**
1. Basic collection with `take()` and `collect()`
2. Header-based filtering
3. Message transformation with tax calculation
4. Complex multi-condition filtering
5. Async processing with different priorities
6. Error handling in streams
7. Folding/reducing for aggregation

**Why use streams over polling?**
- ✅ More efficient (no timeout overhead)
- ✅ Better resource usage (native rdkafka pattern)
- ✅ Async-friendly (works naturally with tokio)
- ✅ Automatic backpressure handling
- ✅ Production-ready (used by most real applications)
- ✅ Composable (easy to chain operations)

---

### State Management & Stream Processing

#### 6. [simple_ktable_example.rs](simple_ktable_example.rs)
**Basic KTable Usage**
- Materialized views from Kafka topics
- Real-time state queries and transformations
- Thread-safe state management
- Background consumption patterns

```bash
cargo run --example simple_ktable_example
```

**What you'll learn:**
- Creating KTables from compacted topics
- Background consumption with lifecycle management
- State queries: `get()`, `contains_key()`, `keys()`, `snapshot()`
- Basic transformations: `map_values()`, `filter()`
- Table statistics and monitoring

---

#### 7. [ktable_example.rs](ktable_example.rs) ⭐
**Advanced Stream-Table Joins** (Production Pattern)
- Real-world order enrichment scenario
- Stream-table joins for data enrichment
- Sample data generation and processing
- Complete end-to-end pipeline

```bash
# Run with sample data generation
cargo run --example ktable_example -- --populate

# Or monitor existing data
cargo run --example ktable_example
```

**What you'll learn:**
- **Stream-table joins** for order enrichment with user profiles
- **Real-time enrichment** of streaming data
- **Sample data generation** for testing and demos
- **Background processing** with multiple concurrent tasks
- **Production patterns** for state management
- **Error handling** for missing lookup data

**Complete Pipeline:**
1. User profiles stored in KTable (`user-profiles` topic)
2. Orders stream processed in real-time (`orders` topic)
3. Orders enriched with user data (discount eligibility, subscription tier)
4. Enriched orders published to output topic (`enriched-orders`)

**Use Cases Demonstrated:**
- E-commerce order processing with customer data
- Real-time personalization and pricing
- Reference data lookups in stream processing
- Multi-topic coordination patterns

---

### Performance Testing

#### 8. [json_performance_test.rs](json_performance_test.rs)
**JSON Performance Testing**
- High-throughput JSON message processing with performance monitoring
- Configurable message sizes and batch configurations
- Real-time throughput measurement and comprehensive metrics
- Multiple concurrent producers for stress testing

```bash
cargo run --example json_performance_test
```

**What you'll learn:**
- Performance optimization for JSON serialization
- Concurrent producer patterns for maximum throughput
- Real-time metrics collection and reporting
- Batch configuration and compression effects
- Memory-efficient message processing

**Performance scenarios tested:**
- Small Messages (~100B) - High volume scenarios
- Medium Messages (~1KB) - Typical application data
- Large Messages (~10KB) - Document or payload processing

---

#### 9. [raw_bytes_performance_test.rs](raw_bytes_performance_test.rs)
**Raw Bytes Performance Testing** 
- Maximum throughput using raw bytes without serialization overhead
- Direct Kafka message access for ultimate performance
- Multiple payload sizes for comprehensive testing
- Raw network throughput measurement

```bash
cargo run --example raw_bytes_performance_test
```

**What you'll learn:**
- Bypassing serialization for maximum performance
- Raw bytes handling with StringSerializer and BytesSerializer
- Network-level throughput optimization
- Performance comparison baseline (no JSON overhead)

**Performance scenarios tested:**
- Tiny Payloads (64B) - Network latency testing
- Small Payloads (512B) - Typical small messages
- Medium Payloads (4KB) - Standard message sizes
- Large Payloads (32KB) - Bulk data transfer

**Performance comparison:**
- Raw bytes typically achieve 2-5x higher throughput than JSON
- Lower CPU usage due to no serialization/deserialization
- Better memory efficiency for large message volumes

---

## 🎯 Recommended Learning Path

1. **Start here**: `typed_kafka_example.rs` - Learn the basics
2. **Add metadata**: `headers_example.rs` - Understanding headers
3. **Go deeper**: `consumer_with_headers.rs` - Advanced header usage
4. **Configure**: `builder_configuration.rs` - Performance optimization
5. **Stream processing**: `fluent_api_example.rs` - Stream processing patterns
6. **State management**: `simple_ktable_example.rs` - KTable basics
7. **Production patterns**: `ktable_example.rs` - Stream-table joins (recommended for stateful apps)

## 🔧 Prerequisites

All examples require:
- **Rust 1.70+** with async support
- **Kafka running** on `localhost:9092`
- **Docker** (recommended for Kafka setup)

## 🌟 Key Concepts Covered

### Type Safety
- Strongly typed producers and consumers
- Automatic serialization/deserialization
- Compile-time guarantees for message structure

### Headers Support
- Rich metadata alongside message payloads
- Header-based routing and filtering
- Distributed tracing and versioning patterns

### Performance Optimization
- Different performance presets for various use cases
- Custom configuration for specific requirements
- Throughput vs latency vs durability trade-offs

### Stream Processing
- Functional programming patterns
- Method chaining for readable code
- Efficient async message processing
- Complex filtering and transformation pipelines

### State Management
- Materialized views with KTables
- Real-time state queries and updates
- Stream-table joins for data enrichment
- Thread-safe concurrent state access

### Production Patterns
- Error handling strategies
- Offset management
- Consumer group coordination
- Monitoring and observability

## 🧪 Testing Examples

Each example is designed to be self-contained and can be run independently. They use unique topic names to avoid conflicts.

```bash
# Run all examples in sequence
for example in typed_kafka_example headers_example consumer_with_headers builder_configuration fluent_api_example simple_ktable_example ktable_example; do
    echo "Running $example..."
    cargo run --example $example
    echo "---"
done

# Run KTable example with sample data
cargo run --example ktable_example -- --populate
```

## 🚀 Next Steps

After working through these examples:

1. **Explore the test suite** in `tests/unit/` and `tests/integration/` for more advanced patterns
2. **Read the documentation** in `docs/` for detailed guides
3. **Check out the shared test infrastructure** in `tests/unit/`:
   - `test_messages.rs` - Common message types
   - `test_utils.rs` - Shared utilities
   - `common.rs` - Consolidated imports

## 🤝 Contributing

Found an issue or want to improve an example? Contributions are welcome!

1. Ensure your changes work with the existing Kafka setup
2. Add comprehensive documentation comments
3. Test your changes with `cargo run --example <name>`
4. Update this README if adding new examples

## 📖 Additional Resources

- **[Main README](../README.md)** - Project overview and installation
- **[Headers Guide](../docs/developer/HEADERS_GUIDE.md)** - Comprehensive headers documentation
- **[Builder Pattern Guide](../docs/developer/BUILDER_PATTERN_GUIDE.md)** - When and how to use builders
- **[Test Suite](../tests/)** - Advanced patterns and edge cases

---

**Happy streaming with ferrisstreams! 🦀📡**