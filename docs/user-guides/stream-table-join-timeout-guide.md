# Timeout Configuration Guide

**Version**: 1.0
**Last Updated**: September 28, 2025
**Status**: Production Ready

## Overview

This guide covers timeout configuration in Velostream for stream-table joins, data source connections, and table loading operations. Proper timeout configuration ensures reliable operations and prevents system hangs.

## Table of Contents

- [Understanding Timeouts](#understanding-timeouts)
- [Table Loading Timeouts](#table-loading-timeouts)
- [Stream-Table Join Timeouts](#stream-table-join-timeouts)
- [Data Source Timeouts](#data-source-timeouts)
- [Progress Monitoring Timeouts](#progress-monitoring-timeouts)
- [Production Configuration](#production-configuration)
- [Troubleshooting](#troubleshooting)
- [Configuration Reference](#configuration-reference)

## Understanding Timeouts

### Timeout Hierarchy

Velostream uses a hierarchical timeout system:

```
Global Timeouts
├── Table Coordination Timeouts
│   ├── Table Ready Wait (60s default)
│   └── Progress Monitoring (300s default)
├── Data Source Timeouts
│   ├── Kafka Topic Wait (0s default - no wait)
│   ├── File Source Wait (0s default - no wait)
│   └── Connection Timeouts (30s default)
└── Stream Processing Timeouts
    ├── Join Operation Timeout (10s default)
    └── Batch Processing Timeout (5s default)
```

### Timeout Principles

1. **Fail Fast by Default**: Short timeouts prevent hanging operations
2. **Configurable per Use Case**: Different timeouts for different scenarios
3. **Graceful Degradation**: Timeout leads to fallback strategies
4. **Clear Error Messages**: Timeout errors include configuration guidance

## Table Loading Timeouts

### Stream-Table Coordination

Configure how long streams wait for tables to be ready:

```sql
-- Stream waits up to 60 seconds for user_profiles table
CREATE STREAM enriched_events AS
SELECT
    e.event_id,
    e.user_id,
    u.name,
    u.tier
FROM kafka_events e
JOIN user_profiles u ON e.user_id = u.user_id
WITH ("table.wait.timeout" = "60s");
```

### Global Table Wait Configuration

Set system-wide defaults in your configuration:

```yaml
# config/velostream.yaml
table_coordination:
  default_wait_timeout: "60s"  # Default wait for all tables
  max_wait_timeout: "300s"     # Maximum allowed wait time
  health_check_interval: "5s"  # How often to check table readiness
```

### Per-Table Timeout Configuration

```rust
use velostream::velostream::server::table_registry::TableRegistry;
use std::time::Duration;

let registry = TableRegistry::new();

// Wait up to 2 minutes for large table
let ready_status = registry
    .wait_for_table_ready("large_dataset", Duration::from_secs(120))
    .await?;

match ready_status {
    TableReadyStatus::Ready => println!("Table ready for joins"),
    TableReadyStatus::Timeout => println!("Table still loading after 2 minutes"),
    TableReadyStatus::Error(e) => println!("Table failed to load: {}", e),
}
```

### Multiple Table Coordination

```rust
// Wait for multiple tables with different timeouts
let table_configs = vec![
    ("user_profiles", Duration::from_secs(30)),   // Quick lookup table
    ("transaction_history", Duration::from_secs(300)), // Large historical data
    ("current_balances", Duration::from_secs(60)),     // Medium-sized table
];

let results = registry
    .wait_for_tables_ready(table_configs)
    .await?;

for (table_name, status) in results {
    match status {
        TableReadyStatus::Ready => println!("{} is ready", table_name),
        TableReadyStatus::Timeout => println!("{} timed out", table_name),
        TableReadyStatus::Error(e) => println!("{} failed: {}", table_name, e),
    }
}
```

## Stream-Table Join Timeouts

### Join Operation Timeouts

Configure timeouts for individual join operations:

```sql
-- Set join timeout for each operation
CREATE STREAM real_time_trades AS
SELECT
    t.trade_id,
    t.symbol,
    t.quantity,
    p.current_price,
    l.max_position
FROM kafka_trades t
LEFT JOIN price_cache p ON t.symbol = p.symbol
LEFT JOIN position_limits l ON t.trader_id = l.trader_id
WITH (
    "join.timeout" = "10s",           -- Individual join timeout
    "join.retry.max" = "3",           -- Retry failed joins
    "join.retry.delay" = "1s"         -- Delay between retries
);
```

### Batch Join Timeouts

For high-throughput scenarios with batch processing:

```sql
-- Configure batch join timeouts
CREATE STREAM batch_enriched AS
SELECT * FROM events e
JOIN user_data u ON e.user_id = u.user_id
WITH (
    "batch.size" = "1000",            -- Process 1000 records per batch
    "batch.timeout" = "5s",           -- Max time per batch
    "batch.join.timeout" = "2s"       -- Max time per join within batch
);
```

### Join Fallback Configuration

```rust
use velostream::velostream::server::graceful_degradation::*;

// Configure fallback strategy with timeouts
let join_config = StreamTableJoinConfig {
    join_timeout: Duration::from_secs(10),
    fallback_strategy: TableMissingDataStrategy::WaitAndRetry {
        max_retries: 3,
        delay: Duration::from_secs(1),
    },
    circuit_breaker: Some(CircuitBreakerConfig {
        failure_threshold: 5,
        recovery_timeout: Duration::from_secs(60),
        failure_rate_window: Duration::from_secs(60),
        min_calls_in_window: 10,
        failure_rate_threshold: 50.0,
    }),
};
```

## Data Source Timeouts

### Kafka Source Timeouts

Configure timeouts for Kafka topics that may not exist yet:

```sql
-- Wait for Kafka topic to be created
CREATE TABLE user_events AS
SELECT * FROM kafka_user_events
WITH (
    "topic.wait.timeout" = "30s",     -- Wait up to 30 seconds for topic
    "topic.retry.interval" = "5s",    -- Check every 5 seconds
    "consumer.timeout" = "10s",       -- Kafka consumer timeout
    "connection.timeout" = "15s"      -- Kafka connection timeout
);
```

### File Source Timeouts

Wait for files to appear or patterns to match:

```sql
-- Wait for file to be created
CREATE TABLE batch_data AS
SELECT * FROM file:///data/input/batch-2025-09-28.csv
WITH (
    "file.wait.timeout" = "300s",     -- Wait up to 5 minutes for file
    "file.retry.interval" = "10s",    -- Check every 10 seconds
    "file.read.timeout" = "60s"       -- File read timeout
);

-- Wait for pattern match
CREATE TABLE log_files AS
SELECT * FROM file:///logs/app-*.json
WITH (
    "file.wait.timeout" = "60s",      -- Wait for matching files
    "file.retry.interval" = "5s",     -- Check frequently
    "pattern.match.min" = "1"         -- Require at least 1 matching file
);
```

### HTTP/REST Source Timeouts

```sql
-- Configure HTTP source timeouts
CREATE TABLE api_data AS
SELECT * FROM http://api.example.com/data
WITH (
    "http.connect.timeout" = "10s",   -- Connection establishment
    "http.read.timeout" = "30s",      -- Response read timeout
    "http.retry.timeout" = "120s",    -- Total retry window
    "http.retry.interval" = "15s"     -- Delay between retries
);
```

## Progress Monitoring Timeouts

### Progress Streaming Timeouts

Configure timeouts for progress monitoring WebSocket connections:

```yaml
# config/progress-monitoring.yaml
progress_streaming:
  connection_timeout: "30s"          # WebSocket connection timeout
  heartbeat_interval: "10s"          # Keep-alive heartbeat
  max_connection_idle: "300s"        # Close idle connections after 5 minutes
  update_interval: "1s"              # Progress update frequency
```

### Health Check Timeouts

```yaml
health_monitoring:
  check_interval: "5s"               # How often to run health checks
  response_timeout: "2s"             # Max time for health check response
  critical_threshold: "30s"          # Mark as critical after 30s
  warning_threshold: "10s"           # Mark as warning after 10s
```

### Progress Tracking Timeouts

```rust
// Configure progress tracking with timeouts
let tracker = registry
    .start_progress_tracking("slow_table".to_string(), Some(1_000_000))
    .await;

// Set timeout for progress updates
tracker.set_update_timeout(Duration::from_secs(30)).await;

// Set timeout for completion
tracker.set_completion_timeout(Duration::from_secs(3600)).await; // 1 hour max
```

## Production Configuration

### Environment-Specific Timeouts

#### Development Environment
```yaml
# config/dev.yaml
timeouts:
  table_wait: "30s"         # Shorter waits for development
  join_timeout: "5s"        # Quick feedback
  data_source: "15s"        # Fast failure detection
  progress_update: "2s"     # Frequent updates
```

#### Staging Environment
```yaml
# config/staging.yaml
timeouts:
  table_wait: "60s"         # Production-like timing
  join_timeout: "10s"       # Realistic timeouts
  data_source: "30s"        # Allow for network variance
  progress_update: "5s"     # Moderate update frequency
```

#### Production Environment
```yaml
# config/production.yaml
timeouts:
  table_wait: "120s"        # Allow for large table loads
  join_timeout: "15s"       # Conservative join timeouts
  data_source: "60s"        # Account for network issues
  progress_update: "10s"    # Reduce monitoring overhead
  circuit_breaker: "300s"   # Long recovery windows
```

### Load-Based Configuration

Adjust timeouts based on expected load:

```rust
// High-throughput configuration
let high_volume_config = TimeoutConfig {
    table_ready_wait: Duration::from_secs(300),    // Allow for large loads
    join_operation: Duration::from_secs(5),        // Quick joins
    batch_processing: Duration::from_secs(10),     // Larger batches
    circuit_breaker_recovery: Duration::from_secs(600), // Long recovery
};

// Low-latency configuration
let low_latency_config = TimeoutConfig {
    table_ready_wait: Duration::from_secs(30),     // Fast startup
    join_operation: Duration::from_secs(2),        // Ultra-quick joins
    batch_processing: Duration::from_secs(1),      // Small batches
    circuit_breaker_recovery: Duration::from_secs(60), // Quick recovery
};
```

## Troubleshooting

### Common Timeout Issues

#### 1. Table Coordination Timeouts

**Symptom**: `TableReadyTimeout` errors when starting streams

```
Error: Table 'user_profiles' not ready after 60s timeout
```

**Solutions**:
```sql
-- Option 1: Increase timeout for large tables
CREATE STREAM events AS
SELECT * FROM kafka_events e
JOIN user_profiles u ON e.user_id = u.user_id
WITH ("table.wait.timeout" = "300s");  -- 5 minutes

-- Option 2: Use graceful degradation
CREATE STREAM events AS
SELECT * FROM kafka_events e
LEFT JOIN user_profiles u ON e.user_id = u.user_id
WITH (
    "table.wait.timeout" = "60s",
    "fallback.strategy" = "EmitWithNulls"  -- Continue without enrichment
);
```

#### 2. Join Operation Timeouts

**Symptom**: `JoinTimeout` errors during stream processing

```
Error: Join operation timed out after 10s
```

**Solutions**:
```rust
// Check table performance
let table_stats = registry.get_table_stats("slow_table").await;
println!("Lookup time: {}ms", table_stats.avg_lookup_time.as_millis());

// Optimize table implementation
if table_stats.avg_lookup_time > Duration::from_millis(100) {
    // Consider using OptimizedTableImpl for O(1) lookups
    println!("Consider optimizing table for faster lookups");
}
```

#### 3. Data Source Timeouts

**Symptom**: `SourceTimeout` errors during table creation

```
Error: Kafka topic 'user_events' not found after 30s
```

**Solutions**:
```bash
# Check if topic exists
kafka-topics --list --bootstrap-server localhost:9092 | grep user_events

# Create topic if missing
kafka-topics --create --topic user_events --partitions 3 --replication-factor 1

# Or use retry configuration
```

### Timeout Debugging

#### Enable Timeout Logging

```yaml
# config/logging.yaml
logging:
  level: DEBUG
  loggers:
    "velostream::timeouts": DEBUG
    "velostream::table_coordination": DEBUG
    "velostream::join_operations": DEBUG
```

#### Monitor Timeout Metrics

```bash
# Check timeout statistics
curl http://localhost:8080/health/metrics | jq '{
  table_timeouts: .timeouts.table_coordination,
  join_timeouts: .timeouts.join_operations,
  source_timeouts: .timeouts.data_sources
}'
```

#### Debug Slow Operations

```rust
// Enable detailed timing for debugging
let debug_config = ProcessorConfig {
    enable_timing: true,
    timeout_logging: true,
    performance_warnings: true,
};

// Check operation timing
let timing_report = processor.get_timing_report();
for (operation, duration) in timing_report {
    if duration > Duration::from_millis(100) {
        println!("Slow operation: {} took {}ms", operation, duration.as_millis());
    }
}
```

### Performance Impact of Timeouts

#### Timeout vs Performance Trade-offs

| Timeout Type | Short Timeout | Long Timeout |
|--------------|---------------|--------------|
| **Table Wait** | Fast failure, may miss data | Slower startup, complete data |
| **Join Ops** | Low latency, may drop records | Higher latency, complete joins |
| **Data Source** | Quick error detection | Better reliability |

#### Monitoring Timeout Impact

```bash
# Monitor timeout-related metrics
curl http://localhost:8080/health/prometheus | grep timeout

# Example output:
# velostream_timeouts_total{type="table_wait"} 12
# velostream_timeouts_total{type="join_operation"} 3
# velostream_timeout_duration_seconds{type="table_wait",quantile="0.95"} 45.2
```

## Configuration Reference

### Table Coordination Timeouts

| Configuration | Default | Range | Description |
|---------------|---------|-------|-------------|
| `table.wait.timeout` | `60s` | `0s-3600s` | Wait for table readiness |
| `table.health.check.interval` | `5s` | `1s-60s` | Health check frequency |
| `table.ready.poll.interval` | `1s` | `100ms-10s` | Readiness polling |

### Join Operation Timeouts

| Configuration | Default | Range | Description |
|---------------|---------|-------|-------------|
| `join.timeout` | `10s` | `1s-300s` | Individual join timeout |
| `batch.join.timeout` | `5s` | `1s-60s` | Batch join timeout |
| `join.retry.delay` | `1s` | `100ms-30s` | Retry delay |

### Data Source Timeouts

| Configuration | Default | Range | Description |
|---------------|---------|-------|-------------|
| `topic.wait.timeout` | `0s` | `0s-3600s` | Kafka topic wait |
| `file.wait.timeout` | `0s` | `0s-3600s` | File availability wait |
| `connection.timeout` | `30s` | `5s-300s` | Connection establishment |

### Progress Monitoring Timeouts

| Configuration | Default | Range | Description |
|---------------|---------|-------|-------------|
| `progress.update.interval` | `1s` | `100ms-60s` | Update frequency |
| `progress.connection.timeout` | `30s` | `5s-300s` | WebSocket timeout |
| `health.check.timeout` | `2s` | `100ms-30s` | Health check timeout |

### Duration Format

Velostream supports these duration formats:

```
"30s"     → 30 seconds
"5m"      → 5 minutes
"2h"      → 2 hours
"1.5m"    → 90 seconds
"500ms"   → 500 milliseconds
"0"       → No timeout (infinite wait)
```

## Best Practices

### 1. Environment-Specific Configuration

- **Development**: Short timeouts for fast feedback
- **Testing**: Medium timeouts for realistic scenarios
- **Production**: Conservative timeouts for reliability

### 2. Monitoring and Alerting

- Monitor timeout rates and adjust accordingly
- Alert on high timeout rates (>5% of operations)
- Track timeout duration percentiles

### 3. Graceful Degradation

- Always configure fallback strategies for timeouts
- Use circuit breakers for repeated timeout scenarios
- Provide clear error messages with remediation steps

### 4. Performance Optimization

- Optimize slow operations rather than just increasing timeouts
- Use OptimizedTableImpl for fast table lookups
- Consider async operations for better concurrency

## Conclusion

Proper timeout configuration is crucial for reliable stream-table join operations. Key principles:

- **Start Conservative**: Use shorter timeouts initially, increase as needed
- **Monitor Actively**: Track timeout rates and performance impact
- **Plan for Failure**: Always have fallback strategies
- **Test Thoroughly**: Validate timeout behavior under load

For related documentation:
- [Progress Monitoring Guide](progress-monitoring-guide.md)
- [Graceful Degradation Guide](graceful-degradation-guide.md)
- [Production Deployment Guide](production-deployment-guide.md)