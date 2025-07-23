# Test Coverage Improvement Plan

## Current Status: ~40% Coverage ✅ Fixed Failing Tests

### ✅ **COMPLETED**
- All 29 unit tests now pass
- Basic configuration and preset testing
- Message and headers functionality testing
- Error type validation

### ❌ **CRITICAL GAPS (High Priority)**

#### 1. **Error Handling Tests** - MISSING
```rust
// Need to add these tests
#[tokio::test]
async fn test_producer_send_serialization_error() {
    // Test what happens when serialization fails
}

#[tokio::test] 
async fn test_consumer_poll_deserialization_error() {
    // Test malformed message handling
}

#[tokio::test]
async fn test_producer_broker_disconnection() {
    // Test network failure scenarios
}
```

#### 2. **Builder Pattern Validation** - UNUSED CODE
```rust
// ProducerBuilder and ConsumerBuilder are marked as dead code
// Need tests like:
#[test]
fn test_producer_builder_configuration_chain() {
    let producer = ProducerBuilder::new(...)
        .client_id("test")
        .high_throughput() 
        .build()?;
    // Validate configuration was applied
}
```

#### 3. **Configuration Validation** - MAJOR GAP
```rust
#[test] 
fn test_invalid_broker_configuration() {
    // Test malformed broker strings: "invalid:broker:format"
}

#[test]
fn test_timeout_boundary_values() {
    // Test Duration::ZERO, Duration::MAX, negative values
}

#[test]
fn test_custom_property_validation() {
    // Test invalid Kafka properties
}
```

#### 4. **ClientConfigBuilder Integration** - MINIMAL COVERAGE
```rust
#[test]
fn test_client_config_builder_creates_valid_rdkafka_config() {
    let config = ClientConfigBuilder::new()
        .bootstrap_servers("localhost:9092")
        .build();
    // Validate it works with actual rdkafka ClientConfig
}
```

### ⚠️ **MEDIUM PRIORITY GAPS**

#### 5. **Headers Edge Cases**
```rust
#[test]
fn test_headers_with_binary_data() {
    // Test non-UTF8 header values
}

#[test]
fn test_headers_size_limits() {
    // Test very large headers
}
```

#### 6. **Message Metadata Testing**
```rust
#[test]
fn test_message_timestamp_preservation() {
    // Verify timestamps are correctly handled
}

#[test]
fn test_message_partition_information() {
    // Test partition and offset information
}
```

#### 7. **Performance Preset Validation**
```rust
#[test]
fn test_preset_configuration_conflicts() {
    // Test when presets set conflicting values
}

#[test]
fn test_preset_effectiveness() {
    // Benchmark different presets (integration test)
}
```

### 🔧 **LOWER PRIORITY**

#### 8. **Feature-Gated Serializers**
```rust
#[cfg(feature = "protobuf")]
#[test]
fn test_proto_serializer_integration() {
    // Test protobuf serialization end-to-end
}

#[cfg(feature = "avro")]  
#[test]
fn test_avro_serializer_integration() {
    // Test avro serialization end-to-end
}
```

#### 9. **Load Testing**
```rust
#[tokio::test]
async fn test_high_volume_producer() {
    // Send 10k messages, verify memory usage
}

#[tokio::test] 
async fn test_consumer_long_running_stability() {
    // Consumer runs for extended period
}
```

## **Recommended Implementation Order**

### Phase 1: Fix Critical Issues (1-2 days)
1. ✅ Fix failing tests (DONE)
2. Add error handling tests for producer/consumer operations
3. Test and utilize builder patterns (remove dead code warnings)
4. Add configuration validation tests

### Phase 2: Improve Coverage (3-4 days)  
5. ClientConfigBuilder integration testing
6. Headers edge case testing
7. Message metadata testing
8. Performance preset validation

### Phase 3: Advanced Testing (1-2 weeks)
9. Feature-gated serializer testing
10. Load and stress testing
11. Mock Kafka integration for network failure testing
12. Property-based testing for configurations

## **Testing Architecture Improvements**

### Add Mock Layer
```rust
// Create a mock Kafka layer for testing without running Kafka
pub struct MockKafkaClient {
    should_fail: bool,
    simulated_latency: Duration,
}
```

### Separate Test Categories
```
tests/
├── unit/           # Fast tests, no external dependencies  
├── integration/    # Require running Kafka
├── performance/    # Benchmarks and load tests
└── property/       # Property-based testing
```

### Add Test Utilities
```rust
pub fn create_test_producer() -> KafkaProducer<String, TestMessage, _, _> {
    // Standard test producer setup
}

pub fn assert_config_equals(actual: &ProducerConfig, expected: &ProducerConfig) {
    // Compare configurations ignoring generated fields
}
```

## **Coverage Metrics Target**

- **Current**: ~40% functional coverage
- **Phase 1 Target**: ~65% functional coverage  
- **Phase 2 Target**: ~80% functional coverage
- **Phase 3 Target**: ~90% functional coverage + performance validation

## **Quality Gates**

### Before Production
- [ ] All error scenarios tested
- [ ] Configuration validation complete
- [ ] Builder patterns fully tested
- [ ] Performance presets validated
- [ ] Integration tests cover happy path + error cases
- [ ] Load testing demonstrates stability under expected usage

### Continuous Monitoring
- [ ] Test coverage tracking in CI
- [ ] Performance regression testing
- [ ] Memory leak detection
- [ ] Dependency vulnerability scanning

---

**Status**: Unit tests now pass ✅, but significant gaps remain for production readiness.
**Next Action**: Implement Phase 1 error handling tests.