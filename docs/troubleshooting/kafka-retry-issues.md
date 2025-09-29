# Kafka Retry Troubleshooting Guide

## Common Issues and Solutions

### Issue: Retry timeout too short
**Symptoms**: Table creation fails even though topic exists
**Solution**: Increase `topic.wait.timeout` based on your environment:
- Local development: 10-30s
- Cloud/Kubernetes: 1-3 minutes
- Complex environments: 5+ minutes

### Issue: High retry frequency causing load
**Symptoms**: Excessive broker requests, performance issues
**Solution**: Adjust `topic.retry.interval`:
```sql
-- Too aggressive (avoid)
WITH ("topic.retry.interval" = "100ms")

-- Better for most cases
WITH ("topic.retry.interval" = "5s")

-- Conservative for high-load systems
WITH ("topic.retry.interval" = "30s")
```

### Issue: Mixed error types (not all are topic-missing)
**Problem**: Network issues, authentication failures mixed with missing topics
**Solution**: Check error patterns and adjust retry strategy:

```rust
// Enhanced error categorization needed
match error {
    ConsumerError::KafkaError(kafka_err) => {
        match kafka_err {
            KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownTopicOrPartition) => {
                // Topic missing - retry makes sense
            }
            KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerNotAvailable) => {
                // Infrastructure issue - different retry strategy
            }
            _ => // Handle other cases
        }
    }
}
```

## Performance Optimization

### Recommended Configurations by Environment

| Environment | wait.timeout | retry.interval | Rationale |
|-------------|--------------|----------------|-----------|
| Local Dev | 30s | 2s | Fast feedback, topics created manually |
| CI/CD | 1m | 5s | Automated topic creation may take time |
| Staging | 2m | 10s | May have resource constraints |
| Production | 5m | 30s | Conservative, avoid overwhelming brokers |

### Monitoring and Alerting

Add these metrics to your monitoring:
- `kafka.topic.retry_duration` - How long retries take
- `kafka.topic.retry_count` - Number of retry attempts
- `kafka.topic.success_rate` - Success rate after retries
- `kafka.topic.timeout_rate` - Rate of ultimate failures