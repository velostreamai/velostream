# Performance Testing Configuration Guide

## Overview
This guide provides configuration recommendations for optimizing throughput in your Kafka streaming application, covering both JSON serialization and raw bytes processing.

## Performance Test Results

### JSON Performance Test (with serialization)
Run the JSON performance test to get baseline metrics:
```bash
cargo run --example json_performance_test
```

### Raw Bytes Performance Test (no serialization)
Run the raw bytes test for maximum throughput:
```bash  
cargo run --example raw_bytes_performance_test
```

### Comparison Test
Run both tests for direct comparison:
```bash
./run_performance_comparison.sh
```

## Optimization Configurations

### 1. High-Throughput Producer Configuration

```rust
    .acks(AckMode::Leader)              // Balance durability vs throughput
    .batching(65536, Duration::from_millis(5))  // 64KB batches, 5ms linger
    .retries(3, Duration::from_millis(100))
    .custom_property("buffer.memory", "67108864")  // 64MB buffer
    .custom_property("max.in.flight.requests.per.connection", "5")
    .custom_property("compression.type", "lz4")
    .high_throughput();
```

### 2. High-Throughput Consumer Configuration

```rust
    .session_timeout(Duration::from_secs(30))
    .heartbeat_interval(Duration::from_secs(3))
    .max_poll_records(1000)             // Process many messages per poll
    .session_timeout(Duration::from_secs(30))
    .heartbeat_interval(Duration::from_secs(3))
    .fetch_min_bytes(1024)              // Wait for reasonable batch
    .fetch_max_wait(Duration::from_millis(500))
    .high_throughput();
    .high_throughput();
Raw performance without serialization overhead:

| Payload Size | Expected Throughput | Performance Gain |
|--------------|-------------------|------------------|
| 64B          | 25,000-50,000 msg/s | 3-4x faster |
| 512B         | 15,000-30,000 msg/s | 3-5x faster |
| 4KB          | 8,000-15,000 msg/s  | 4-6x faster |
| 32KB         | 2,000-5,000 msg/s   | 3-4x faster |

## Tuning Tips

### For Maximum Throughput
1. Use multiple concurrent producers
2. Increase batch size if network allows
3. Use LZ4 compression for JSON
4. Tune `buffer.memory` based on message rate
5. Consider `acks=1` vs `acks=all` trade-off

### For Large Messages (>10KB)
1. Consider message splitting or pagination
2. Increase `max.request.size` and `buffer.memory`
3. Use Snappy compression for better ratio
4. Reduce concurrent connections

### For High Message Rates
1. Use multiple partitions for parallelism
2. Increase consumer `max.poll.records`
3. Implement consumer groups for scaling
4. Monitor consumer lag

## Monitoring Commands

Check performance during testing:
```bash
# Monitor topic
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic perf-test-*

# Monitor consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group perf-test-group
```

## Additional Performance Tests

### Latency Testing
For end-to-end latency measurement:
```bash
cargo run --example latency_performance_test
```

### Resource Monitoring
To monitor system resources during testing:
```bash
cargo run --example resource_monitoring_test
```

## Troubleshooting

### Low Throughput
- Check batch.size and linger.ms settings
- Verify compression is working
- Monitor consumer lag
- Check network bandwidth utilization
- Run resource monitoring to identify bottlenecks

### High Latency
- Reduce linger.ms
- Decrease batch.size
- Check GC pauses
- Verify broker configuration
- Use latency test to measure P95/P99

### Memory Issues
- Reduce buffer.memory
- Implement backpressure handling
- Monitor heap usage with resource monitoring
- Check for message accumulation

### Performance Regression Detection
1. Establish baseline with current tests
2. Run tests regularly in CI/CD
3. Compare latency percentiles over time
4. Monitor resource usage trends
5. Alert on performance degradation