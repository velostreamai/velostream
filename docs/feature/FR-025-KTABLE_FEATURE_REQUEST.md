# KTable Implementation - Feature Request

## 🚀 Feature Overview

**KTable** - Materialized view implementation for Kafka topics providing real-time state management and stream-table join capabilities.

### 📋 Summary

This feature adds support for **KTables** (materialized views) to the Kafka client library, enabling applications to maintain and query real-time state derived from Kafka topics. KTables are essential for stream processing applications that need to perform lookups, joins, and transformations on streaming data.

## 🎯 Motivation

### Problem Statement
Currently, the library only supports basic producer/consumer patterns. Many stream processing applications require:

1. **State Management**: Maintaining materialized views of data from compacted topics
2. **Stream-Table Joins**: Enriching streaming data with reference information
3. **Real-time Queries**: Fast O(1) lookups for current state
4. **Event Sourcing**: Rebuilding state from event logs stored in Kafka

### Use Cases
- **User Profile Management**: Maintain current user profiles from user events
- **Configuration Services**: Real-time configuration updates from config topics
- **Reference Data**: Product catalogs, pricing information, lookup tables
- **Stream Enrichment**: Join order streams with user profile tables
- **Event Sourcing**: Reconstruct application state from event logs

## 🔧 Proposed Solution

### Core Components

#### 1. KTable Structure
```rust
pub struct KTable<K, V, KS, VS> {
    consumer: Arc<KafkaConsumer<K, V, KS, VS>>,
    state: Arc<RwLock<HashMap<K, V>>>,
    topic: String,
    group_id: String,
    running: Arc<AtomicBool>,
    last_updated: Arc<RwLock<Option<SystemTime>>>,
}
```

#### 2. Key Features
- **Thread-Safe State**: `Arc<RwLock<HashMap<K, V>>>` for concurrent access
- **Reactive Consumption**: Stream-based processing with `consumer.stream()`
- **Lifecycle Management**: Start/stop consumption with proper cleanup
- **Query Operations**: Get, contains, keys, snapshot operations
- **Transformations**: Map and filter operations on state
- **Statistics**: Monitoring table size and update timestamps

#### 3. API Design
```rust
// Create KTable from compacted topic
let config = ConsumerConfig::new("brokers", "group")
    .auto_offset_reset(OffsetReset::Earliest)
    .isolation_level(IsolationLevel::ReadCommitted);

let user_table = KTable::new(
    config,
    "users".to_string(),
    JsonSerializer,
    JsonSerializer,
).await?;

// Start background consumption
let table_clone = user_table.clone();
tokio::spawn(async move {
    table_clone.start().await
});

// Query current state
let user = user_table.get(&"user-123".to_string());
let all_users = user_table.snapshot();

// Transform data
let emails = user_table.map_values(|user| user.email.clone());
let adults = user_table.filter(|_key, user| user.age >= 18);
```

## 📊 Implementation Details

### Files Added/Modified

#### New Files
- `src/ferris/kafka/ktable.rs` - Core KTable implementation
- `tests/integration/ktable_test.rs` - Comprehensive test suite
- `docs/KTABLE_FEATURE_REQUEST.md` - This feature request

#### Modified Files
- `src/ferris/kafka/mod.rs` - Added KTable exports
- `tests/integration/mod.rs` - Added ktable_test module
- `docs/PRODUCTIONISATION.md` - Added KTable documentation section

### Key Methods

#### Lifecycle Management
```rust
pub async fn new(config: ConsumerConfig, topic: String, key_serializer: KS, value_serializer: VS) -> Result<Self, ConsumerError>
pub fn from_consumer(consumer: KafkaConsumer<K, V, KS, VS>, topic: String) -> Self
pub async fn start(&self) -> Result<(), ConsumerError>
pub fn stop(&self)
pub fn is_running(&self) -> bool
```

#### State Queries
```rust
pub fn get(&self, key: &K) -> Option<V>
pub fn contains_key(&self, key: &K) -> bool
pub fn keys(&self) -> Vec<K>
pub fn len(&self) -> usize
pub fn is_empty(&self) -> bool
pub fn snapshot(&self) -> HashMap<K, V>
```

#### Transformations
```rust
pub fn map_values<V2, F>(&self, mapper: F) -> HashMap<K, V2>
pub fn filter<F>(&self, predicate: F) -> HashMap<K, V>
```

#### Utilities
```rust
pub fn stats(&self) -> KTableStats
pub async fn wait_for_keys(&self, min_keys: usize, timeout: Duration) -> bool
pub fn topic(&self) -> &str
pub fn group_id(&self) -> &str
```

## 🧪 Testing Strategy

### Test Coverage
- ✅ **Basic Creation**: KTable instantiation with various configurations
- ✅ **Consumer Integration**: Creating KTables from existing consumers
- ✅ **Lifecycle Management**: Start/stop operations and state tracking
- ✅ **State Operations**: Get, contains, keys, and snapshot operations
- ✅ **Transformations**: Map and filter operations
- ✅ **Statistics**: Metadata and stats collection
- ✅ **Clone Behavior**: Shared state across cloned instances
- ✅ **Error Handling**: Invalid broker and configuration scenarios
- ✅ **Multiple Types**: Support for different key/value types
- ✅ **Background Processing**: Simulated producer-consumer scenarios

### Test Structure
```rust
// Example test demonstrating KTable lifecycle
#[tokio::test]
async fn test_ktable_lifecycle_management() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "test-group")
        .auto_offset_reset(OffsetReset::Latest);

    let ktable = KTable::<String, UserProfile, _, _>::new(
        config, "user-profiles".to_string(), 
        JsonSerializer, JsonSerializer
    ).await.unwrap();

    // Start background processing
    let ktable_clone = ktable.clone();
    let handle = tokio::spawn(async move {
        ktable_clone.start().await
    });

    assert!(ktable.is_running());
    
    // Stop and cleanup
    ktable.stop();
    assert!(!ktable.is_running());
    
    let _ = handle.await;
}
```

## 📚 Documentation

### Production Guide Integration
Added comprehensive KTable section to `docs/PRODUCTIONISATION.md`:

#### Topics Covered
1. **Basic Usage Patterns**
2. **Stream-Table Joins** for data enrichment
3. **Configuration Best Practices**
4. **Memory Management** considerations
5. **Error Handling** strategies
6. **Performance Optimization** tips

#### Example: Stream-Table Join
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

## 🔒 Security & Performance Considerations

### Security
- **Thread Safety**: All operations are thread-safe using `Arc` and `RwLock`
- **Memory Safety**: Rust's ownership system prevents data races
- **Error Handling**: Comprehensive error handling for network and serialization issues

### Performance
- **O(1) Lookups**: HashMap-based state for fast key-based queries
- **Minimal Allocations**: Efficient memory usage with shared references
- **Configurable Consumption**: Customizable consumer settings for throughput vs latency
- **Lock Contention**: Read-write locks allow concurrent reads with exclusive writes

### Memory Management
- **Bounded Growth**: Consider implementing size limits for large datasets
- **Cleanup Strategies**: Topic compaction and TTL for automatic cleanup
- **Monitoring**: Built-in statistics for tracking memory usage

## 🚦 Backward Compatibility

### Zero Breaking Changes
- ✅ No changes to existing APIs
- ✅ Additive feature only
- ✅ Optional dependency - KTable usage is opt-in
- ✅ Existing producer/consumer functionality unchanged

### Migration Path
- **Immediate Use**: Can be used alongside existing code
- **Gradual Adoption**: Teams can migrate specific use cases incrementally
- **No Code Changes**: Existing applications continue working unchanged

## 🎯 Success Criteria

### Functional Requirements
- ✅ **State Rebuilding**: Automatic reconstruction from topic beginning
- ✅ **Real-time Updates**: Live updates as new messages arrive
- ✅ **Query Performance**: Fast O(1) key-based lookups
- ✅ **Thread Safety**: Safe concurrent access from multiple threads
- ✅ **Error Recovery**: Graceful handling of network and serialization errors

### Non-Functional Requirements
- ✅ **Performance**: Minimal overhead over direct HashMap access
- ✅ **Memory Efficiency**: Reasonable memory usage for typical datasets
- ✅ **Testability**: Comprehensive test coverage with integration tests
- ✅ **Documentation**: Clear usage examples and best practices
- ✅ **Maintainability**: Clean, well-structured code following Rust idioms

## 🔮 Future Enhancements

### Potential Extensions
1. **Windowed Operations**: Time-based and count-based windows
2. **Join Operations**: Direct KTable-KTable and KStream-KTable joins
3. **Persistent Storage**: Disk-backed state for large datasets
4. **Change Streams**: Observable change events for state modifications
5. **Aggregations**: Built-in aggregation operations (count, sum, etc.)
6. **Serialization Formats**: Support for Avro, Protobuf schema evolution

### Architecture Considerations
- **Pluggable Storage**: Interface for alternative storage backends
- **State Stores**: RocksDB integration for persistent state
- **Checkpointing**: Periodic state snapshots for fast recovery
- **Partitioning**: Distributed state across multiple instances

## 📋 Checklist

### Implementation Status
- ✅ Core KTable implementation
- ✅ Thread-safe state management
- ✅ Stream-based consumption
- ✅ Query operations (get, contains, keys, snapshot)
- ✅ Transformation operations (map_values, filter)
- ✅ Lifecycle management (start, stop, is_running)
- ✅ Statistics and metadata
- ✅ Error handling and recovery
- ✅ Clone support for shared state
- ✅ Consumer configuration integration
- ✅ Module exports and public API

### Testing Status
- ✅ Basic creation and configuration tests
- ✅ Consumer integration tests
- ✅ Lifecycle management tests
- ✅ State operation tests
- ✅ Transformation tests
- ✅ Statistics and metadata tests
- ✅ Clone behavior tests
- ✅ Error handling tests
- ✅ Multiple type support tests
- ✅ Background processing simulation tests

### Documentation Status
- ✅ Code documentation and examples
- ✅ Production guide integration
- ✅ Best practices documentation
- ✅ API reference documentation
- ✅ Feature request documentation (this document)

## 💡 Conclusion

The KTable implementation provides a robust foundation for stream processing applications requiring materialized views and real-time state management. This feature significantly enhances the library's capabilities while maintaining full backward compatibility and following established Rust and Kafka best practices.

**Key Benefits:**
- 🚀 **Enhanced Functionality**: Enables complex stream processing patterns
- 🔒 **Production Ready**: Thread-safe, error-resilient, and well-tested
- 📈 **Performance Optimized**: Fast queries with minimal overhead
- 🛠️ **Developer Friendly**: Clean API with comprehensive documentation
- 🔄 **Future Proof**: Extensible design for advanced features

This implementation opens the door for sophisticated stream processing applications while maintaining the simplicity and reliability that users expect from the Kafka client library.