# Kafka Transaction Configuration Guide

## Overview

Velostream provides comprehensive support for Kafka transactions to enable exactly-once processing semantics. This guide covers the transaction-enabled consumer and producer configurations for financial-grade data processing.

## Transaction Architecture

```
Transactional Consumer → Velostream Engine → Transactional Producer
       ↓                        ↓                        ↓
  Read Committed         Transaction Boundary      Exactly-Once Delivery
    Messages               Management                  Guarantees
```

## Configuration Files

### Transaction Consumer (`configs/transaction-consumer.yaml`)

Configures Kafka consumer for exactly-once reading with transactional semantics:

```yaml
datasource:
  type: kafka
  consumer_config:
    isolation_level: "read_committed"  # Only committed transactions
    enable_auto_commit: false          # Manual commit control
    group_id: "velo-tx-consumer-group"
```

**Key Features:**
- **Read Committed Isolation**: Only processes committed transactional messages
- **Manual Offset Management**: Precise control over transaction boundaries
- **Enhanced Error Handling**: Transaction-aware recovery strategies
- **Financial Precision Support**: ScaledInteger field definitions

### Transaction Producer (`configs/transaction-producer.yaml`)

Configures Kafka producer for exactly-once delivery with transactional guarantees:

```yaml
datasink:
  type: kafka
  producer_config:
    enable_idempotence: true
    transactional_id: "velo-tx-producer-1"
    acks: "all"
    max_in_flight_requests_per_connection: 1
```

**Key Features:**
- **Idempotent Producer**: Prevents duplicate messages
- **Transactional ID**: Unique identifier for producer instance
- **All Acknowledgments**: Ensures all replicas receive messages
- **Ordering Guarantees**: Single in-flight request preserves order

## Performance Profiles

### Financial Critical
- **Use Case**: High-value financial transactions, regulatory compliance
- **Characteristics**: Maximum reliability, zero data loss tolerance
- **Configuration**:
  ```yaml
  financial_critical:
    acks: "all"
    transaction_timeout_ms: 60000
    delivery_timeout_ms: 180000
    compression_type: "none"
    linger_ms: 0
  ```

### Batch Processing  
- **Use Case**: Large batch processing jobs, ETL operations
- **Characteristics**: High throughput with transaction boundaries
- **Configuration**:
  ```yaml
  batch_processing:
    transaction_timeout_ms: 300000
    max_poll_records: 1000
    batch_size: 65536
    compression_type: "lz4"
  ```

### Real-Time Stream
- **Use Case**: Low-latency stream processing, trading systems
- **Characteristics**: Sub-second latency with transaction guarantees
- **Configuration**:
  ```yaml
  real_time_stream:
    transaction_timeout_ms: 30000
    linger_ms: 1
    fetch_max_wait_ms: 100
    max_poll_records: 50
  ```

### High Throughput
- **Use Case**: Maximum throughput with transactions
- **Characteristics**: Optimized batching and compression
- **Configuration**:
  ```yaml
  high_throughput:
    batch_size: 131072
    buffer_memory: 134217728
    compression_type: "zstd"
    linger_ms: 50
  ```

## Velostream Integration

### Multi-Job Processor Configuration

Transaction configurations integrate with Velostream' transactional job processor:

```yaml
velo_integration:
  processor_type: "transactional"
  job_config:
    use_transactions: true
    commit_strategy: "transactional"
    rollback_strategy: "abort_transaction"
    transaction_batch_size: 1000
```

### Transaction Processing Flow

1. **Begin Transaction**: Producer starts transaction
2. **Process Batch**: Consumer reads committed messages
3. **Execute SQL**: Velostream processes data
4. **Produce Results**: Write to output topics
5. **Commit Transaction**: Atomically commit all operations
6. **Handle Failures**: Abort and retry on errors

## Schema Support

### Transaction Envelope

All transactional messages include metadata envelope:

```json
{
  "transaction_id": "tx-12345678-1234-1234-1234-123456789012",
  "sequence_number": 1001,
  "timestamp": 1672531200000,
  "producer_id": "velo-tx-producer-1",
  "payload": "{actual_message_data}",
  "checksum": "sha256_hash"
}
```

### Financial Data Schema

Financial transactions use precision-preserving schema:

```json
{
  "amount": {"type": "scaled_integer", "scale": 4},
  "currency": "USD",
  "account_id": "ACC-123456",
  "transaction_type": "TRANSFER"
}
```

## Error Handling and Recovery

### Transaction Failure Strategies

1. **Abort and Retry**: Rollback transaction and retry batch
2. **Partial Commit**: Commit successful records, retry failures
3. **Dead Letter Queue**: Route failed transactions to DLQ
4. **Circuit Breaker**: Temporary failure handling

### Producer Recovery

```yaml
error_handling:
  transaction_failure_strategy: "abort_and_retry"
  max_transaction_retries: 3
  producer_recovery_strategy: "recreate_producer"
  dlq_topic: "velo-tx-producer-dlq"
```

### Consumer Recovery

```yaml
error_handling:
  transaction_failure_strategy: "abort_and_retry"
  max_retries: 3
  retry_backoff_ms: 1000
  dlq_topic: "velo-tx-dlq"
```

## Security Configuration

### SASL Authentication

```yaml
security:
  protocol: "SASL_SSL"
  sasl_mechanism: "SCRAM-SHA-512"
  sasl_username: "tx_user"
  sasl_password: "secure_password"
```

### SSL/TLS Configuration

```yaml
ssl_ca_location: "/etc/ssl/certs/ca-cert.pem"
ssl_certificate_location: "/etc/ssl/certs/client-cert.pem"
ssl_key_location: "/etc/ssl/private/client-key.pem"
```

## Topic Configuration Requirements

### Minimum Requirements

Transactional topics must have specific configurations:

```yaml
recommended_topic_config:
  min.insync.replicas: 2                    # Minimum 2 replicas
  unclean.leader.election.enable: false     # Prevent data loss
  default.replication.factor: 3             # High availability
  log.flush.interval.messages: 1           # Immediate flush
```

### Transaction Coordinator Topics

Kafka automatically creates transaction coordinator topics:
- `__transaction_state` - Transaction metadata
- `__consumer_offsets` - Consumer position tracking

## Performance Tuning

### Memory Management

```yaml
# Consumer Memory
receive_buffer_bytes: 262144              # 256KB receive buffer
fetch_max_bytes: 52428800                # 50MB max fetch

# Producer Memory  
buffer_memory: 33554432                  # 32MB send buffer
batch_size: 16384                        # 16KB batch size
```

### Timeout Configuration

```yaml
# Transaction Timeouts
transaction_timeout_ms: 60000            # 1 minute transaction
delivery_timeout_ms: 120000              # 2 minute delivery
request_timeout_ms: 30000                # 30 second request
```

### Compression Settings

| Profile | Compression | Use Case |
|---------|------------|----------|
| Financial Critical | `none` | Maximum reliability |
| Real-Time | `lz4` | Low latency |
| Batch | `lz4` | Balanced performance |
| High Throughput | `zstd` | Maximum compression |

## Monitoring and Metrics

### Transaction Metrics

Monitor these key transaction metrics:

```yaml
transaction_metrics:
  - transaction-count              # Total transactions
  - transaction-abort-rate         # Failed transaction rate
  - transaction-commit-latency     # Commit time
  - pending-transaction-count      # Uncommitted transactions
```

### Producer Metrics

```yaml
producer_metrics:
  - record-send-rate              # Records per second
  - batch-size-avg               # Average batch size
  - request-latency-avg          # Request latency
  - buffer-available-bytes       # Available buffer space
```

### Consumer Metrics

```yaml
consumer_metrics:
  - records-consumed-rate         # Consumption rate
  - fetch-latency-avg            # Fetch latency
  - lag-sum                      # Consumer lag
  - commit-latency-avg           # Commit latency
```

## Best Practices

### 1. Producer Configuration

- **Always enable idempotence** for exactly-once semantics
- **Use unique transactional IDs** per producer instance
- **Set `max_in_flight_requests=1`** to preserve ordering
- **Configure appropriate timeouts** for your use case

### 2. Consumer Configuration

- **Use `read_committed` isolation** for transactional data
- **Disable auto-commit** for manual transaction control
- **Configure session timeouts** based on processing time
- **Set appropriate batch sizes** for your throughput needs

### 3. Topic Configuration

- **Set `min.insync.replicas=2`** minimum
- **Disable unclean leader election** to prevent data loss
- **Use replication factor of 3+** for production
- **Configure appropriate retention** policies

### 4. Error Handling

- **Implement transaction retry logic** with backoff
- **Use dead letter queues** for permanent failures
- **Monitor transaction abort rates** for system health
- **Plan for producer recreation** on failures

### 5. Performance Optimization

- **Tune batch sizes** for your latency requirements
- **Use appropriate compression** for your data
- **Monitor buffer utilization** to prevent blocking
- **Configure connection pooling** for high throughput

## Deployment Examples

### Development Environment

```bash
# Start with local Kafka
docker-compose -f docker-compose.yml up -d

# Use development profile
cargo run --bin velo-sql-multi -- \
  --consumer-config configs/transaction-consumer.yaml \
  --producer-config configs/transaction-producer.yaml \
  --profile development
```

### Production Environment

```bash
# Production deployment with monitoring
cargo run --bin velo-sql-multi -- \
  --consumer-config configs/transaction-consumer.yaml \
  --producer-config configs/transaction-producer.yaml \
  --profile financial_critical \
  --enable-metrics \
  --metrics-port 8080
```

## Troubleshooting

### Common Issues

1. **Transaction Timeouts**
   - Increase `transaction_timeout_ms`
   - Reduce batch sizes
   - Check network latency

2. **High Abort Rates**
   - Check for duplicate transactional IDs
   - Verify topic configurations
   - Monitor system resources

3. **Producer Fencing**
   - Ensure unique transactional IDs
   - Check for multiple producer instances
   - Verify authentication credentials

4. **Consumer Lag**
   - Increase consumer instances
   - Optimize processing logic
   - Check for long-running transactions

### Diagnostic Commands

```bash
# Check transaction state
kafka-transactions.sh --bootstrap-server localhost:9092 \
  --describe --transactional-id velo-tx-producer-1

# Monitor consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group velo-tx-consumer-group

# Check topic configuration
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic your-transaction-topic
```

## Integration with Performance Optimizations

The transaction configurations work seamlessly with the performance optimizations outlined in `TODO-optimisation-plan.MD`:

- **Memory Pools**: Transaction-aware object pooling
- **Batch Processing**: Transactional batch boundaries
- **Async Processing**: Non-blocking transaction commits
- **Monitoring**: Transaction-specific performance metrics

---

*For additional information, see:*
- `TODO-optimisation-plan.MD` - Performance optimization strategies
- `CLAUDE.md` - Development guidelines and patterns
- `docs/KAFKA_SCHEMA_CONFIGURATION.md` - Schema management