# JSON Performance Testing Configuration Guide

## Overview
This guide provides configuration recommendations for optimizing JSON throughput in your Kafka streaming application.

## Performance Test Results

Run the performance test to get baseline metrics:
```bash
cargo run --example json_performance_test
```

## Optimization Configurations

### 1. High-Throughput Producer Configuration

```rust
let config = ProducerConfig::new("localhost:9092", "high-throughput-topic")
    .client_id("ht-producer")
    .compression(CompressionType::Lz4)  // Fast compression for JSON
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
let config = ConsumerConfig::new("localhost:9092", "ht-group")
    .client_id("ht-consumer")
    .auto_offset_reset(OffsetReset::Latest)
    .session_timeout(Duration::from_secs(30))
    .heartbeat_interval(Duration::from_secs(3))
    .max_poll_records(1000)             // Process many messages per poll
    .fetch_min_bytes(1024)              // Wait for reasonable batch
    .fetch_max_wait(Duration::from_millis(500))
    .high_throughput();
```

## Key Performance Parameters

### Producer Optimization
- **Batch Size**: 64KB (65536 bytes) for optimal network utilization
- **Linger Time**: 5ms to allow batching without excessive latency
- **Compression**: LZ4 for fast compression/decompression of JSON
- **Buffer Memory**: 64MB to handle bursts
- **In-Flight Requests**: 5 for pipeline efficiency

### Consumer Optimization  
- **Max Poll Records**: 1000 messages per poll
- **Fetch Min Bytes**: 1KB minimum to encourage batching
- **Fetch Max Wait**: 500ms maximum wait for batches

## Expected Performance

Based on message size:

| Message Size | Expected Throughput | Optimal Use Case |
|--------------|-------------------|------------------|
| ~100B        | 8,000-15,000 msg/s | IoT sensors, logs |
| ~1KB         | 3,000-8,000 msg/s  | API events, metrics |
| ~10KB        | 500-2,000 msg/s    | Rich payloads, documents |

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

## Troubleshooting

### Low Throughput
- Check batch.size and linger.ms settings
- Verify compression is working
- Monitor consumer lag
- Check network bandwidth utilization

### High Latency
- Reduce linger.ms
- Decrease batch.size
- Check GC pauses
- Verify broker configuration

### Memory Issues
- Reduce buffer.memory
- Implement backpressure handling
- Monitor heap usage
- Check for message accumulation