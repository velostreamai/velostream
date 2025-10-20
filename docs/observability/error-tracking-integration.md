# Error Tracking Integration Guide

## Overview

Velostream now provides comprehensive error message tracking with integration into Prometheus and Grafana for real-time visibility into system errors. This document describes how the error tracking system works end-to-end, from capture to visualization.

## Architecture

### Error Message Capture

Errors are captured and tracked in a **rolling buffer** maintained by the `ErrorMessageBuffer` struct:

- **Last 10 Messages**: The buffer maintains a FIFO queue of the most recent unique error messages
- **Message Counting**: Each unique error message tracks its occurrence count
- **Timestamp Tracking**: Each error entry includes a Unix timestamp for when it occurred
- **Thread-Safe Access**: Uses `Arc<Mutex<>>` for safe concurrent access across async contexts

### Metrics Exposure to Prometheus

Three Prometheus gauges expose error tracking data for monitoring:

1. **`velo_error_messages_total`** (gauge)
   - **Description**: Total number of error messages recorded (cumulative)
   - **Type**: Gauge (increases monotonically)
   - **Use Case**: Track overall error frequency and trends

2. **`velo_unique_error_types`** (gauge)
   - **Description**: Number of unique error message types in the system
   - **Type**: Gauge (reflects current diversity of errors)
   - **Use Case**: Identify if error types are proliferating (potential system stability issue)

3. **`velo_buffered_error_messages`** (gauge)
   - **Description**: Number of error messages in current rolling buffer (max 10)
   - **Type**: Gauge (reflects recent error activity)
   - **Use Case**: Track if error buffer is full (active error generation)

### Data Flow

```
┌─────────────────┐
│ Error Occurs    │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────┐
│ record_error_message()      │ ◄─── MetricsProvider API
│ or                          │
│ record_sql_query_with_error()
└────────┬────────────────────┘
         │
         ▼
┌───────────────────────────────┐
│ ErrorMessageBuffer            │
│ - Rolling FIFO queue (10 max) │
│ - Message occurrence counts   │
│ - Timestamps                  │
└────────┬──────────────────────┘
         │
         ▼
┌────────────────────────────┐
│ sync_error_metrics()       │ ◄─── Call to update Prometheus gauges
│ Updates Prometheus Gauges  │
└────────┬───────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Prometheus Scrape       │ ◄─── /metrics endpoint
│ Exports Gauges          │
└────────┬────────────────┘
         │
         ▼
┌──────────────────────┐
│ Grafana Dashboard    │ ◄─── Queries Prometheus
│ Error Tracking Panel │
└──────────────────────┘
```

## Integration Points

### 1. Capturing Errors

Errors can be captured through two methods:

**Method A: Direct Error Recording**
```rust
metrics_provider.record_error_message("Connection timeout".to_string());
```

**Method B: SQL Query Execution with Error**
```rust
metrics_provider.record_sql_query_with_error(
    "select",
    Duration::from_millis(100),
    false,  // query failed
    0,
    Some("Database offline".to_string()),
);
```

### 2. Syncing Metrics to Prometheus

The metrics must be synchronized to Prometheus at regular intervals (e.g., before Prometheus scrape):

```rust
// This should be called in your metrics scrape handler
metrics_provider.sync_error_metrics();
```

This call:
- Reads current error tracking state from `ErrorMessageBuffer`
- Updates the three Prometheus gauges with current values
- Ensures metrics reflect the most recent error activity

### 3. Prometheus Scrape Configuration

Ensure your `prometheus.yml` includes the Velostream metrics endpoint:

```yaml
scrape_configs:
  - job_name: 'velostream'
    static_configs:
      - targets: ['localhost:9090']  # Adjust port as needed
    metrics_path: '/metrics'
    scrape_interval: 15s
```

## Grafana Dashboard

### Location

```
demo/trading/monitoring/grafana/dashboards/velostream-error-tracking.json
```

### Dashboard Panels

#### 1. Total Error Count (Stat Panel)
- **Metric**: `velo_error_messages_total`
- **Display**: Current cumulative error count
- **Color Thresholds**:
  - Green: 0-4 errors
  - Yellow: 5-9 errors
  - Orange: 10-19 errors
  - Red: 20+ errors

#### 2. Unique Error Types (Stat Panel)
- **Metric**: `velo_unique_error_types`
- **Display**: Number of distinct error message types
- **Color Thresholds**:
  - Green: 1 type
  - Yellow: 2-3 types
  - Orange: 4-7 types
  - Red: 8+ types
- **Insight**: Helps identify system instability (many error types) vs. single recurring issue

#### 3. Buffered Error Messages (Stat Panel)
- **Metric**: `velo_buffered_error_messages`
- **Display**: How many recent errors are in the rolling buffer
- **Color Thresholds**:
  - Green: 0-4 messages
  - Yellow: 5-7 messages
  - Orange: 8-9 messages
  - Red: 10 messages (buffer full)
- **Insight**: Full buffer indicates active recent error generation

#### 4. Error Tracking Over Time (Time Series)
- **Metrics**: `velo_error_messages_total` and `velo_unique_error_types`
- **X-Axis**: Time
- **Y-Axis**: Count
- **Purpose**: Visualize error trends and detect sudden spikes

#### 5. Last 10 Error Messages (Table)
- **Source**: Error message buffer query (requires custom data)
- **Columns**: Error Message, Occurrence Count
- **Purpose**: Quick reference for most recent errors
- **Note**: This panel may require additional integration with query-based metrics

## Usage Examples

### Example 1: Basic Error Tracking

```rust
use velostream::velostream::observability::MetricsProvider;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let metrics = MetricsProvider::new(config).await.unwrap();

    // Record an error
    metrics.record_error_message("Connection failed".to_string());
    metrics.record_error_message("Connection failed".to_string());  // Same error
    metrics.record_error_message("Timeout error".to_string());

    // Before Prometheus scrape, sync metrics
    metrics.sync_error_metrics();

    // Now Prometheus can read the updated gauges
    let metrics_text = metrics.get_metrics_text().unwrap();
    println!("{}", metrics_text);
}
```

### Example 2: Query Error Tracking

```rust
metrics.record_sql_query_with_error(
    "stream_processing",
    Duration::from_millis(500),
    false,  // failed
    100,
    Some("Batch processing failed: memory exhausted".to_string()),
);

metrics.sync_error_metrics();
```

### Example 3: Error Statistics Retrieval

```rust
// Get comprehensive error statistics
if let Some(stats) = metrics.get_error_stats() {
    println!("Total errors: {}", stats.total_errors);
    println!("Unique types: {}", stats.unique_errors);

    // Get top 5 most common errors
    let top_errors = stats.get_top_errors(5);
    for (msg, count) in top_errors {
        println!("{}: {} occurrences", msg, count);
    }
}
```

## Key Features

### 1. Rolling Buffer (Last 10 Messages)
- **Benefit**: Captures recent errors without unbounded memory growth
- **FIFO Eviction**: Oldest messages are automatically removed when buffer is full
- **Duplicate Handling**: Same error multiple times increments count, doesn't duplicate buffer entry

### 2. Message Counting
- **Total Errors**: Cumulative count since application startup (never decreases)
- **Per-Message Counts**: Track occurrence frequency of each unique error message
- **Frequency Analysis**: Identify which errors are most common

### 3. Timestamp Tracking
- **Per-Entry**: Each error message includes Unix timestamp of when it occurred
- **Resolution**: Second-level precision (sufficient for error analysis)
- **Use Case**: Correlate errors with other system events

### 4. Thread-Safe Access
- **Concurrent Safety**: Arc<Mutex<>> ensures safe access across async tasks
- **No Deadlocks**: Lock is held briefly during operations
- **Performance**: Efficient string cloning for message deduplication

## Performance Considerations

### Memory Usage
- **Buffer Size**: Fixed at 10 entries (bounded memory)
- **Per-Entry Overhead**: ~100-500 bytes depending on message length
- **Maximum Buffer**: ~5-50 KB for typical error messages

### CPU Impact
- **Recording**: O(1) - HashMap lookup + string comparison
- **Syncing**: O(1) - Read three counters and update gauges
- **Negligible**: Error tracking has minimal performance impact

## Troubleshooting

### Metrics Not Appearing in Prometheus

**Symptoms**: `velo_error_messages_total` gauge doesn't appear in Prometheus

**Solution**: Ensure `sync_error_metrics()` is called before Prometheus scrapes

```rust
// In your metrics scrape handler:
pub async fn metrics_handler(metrics: &MetricsProvider) -> Result<String, Error> {
    metrics.sync_error_metrics();  // Update all gauges from error buffer
    metrics.get_metrics_text()
}
```

### Buffer Always Full

**Symptoms**: `velo_buffered_error_messages` always at 10

**Cause**: Application continuously generating errors faster than they age out

**Solution**: Investigate root cause - may indicate:
- Disk I/O failures
- Network connectivity issues
- Resource exhaustion
- Application bugs

### Wrong Error Counts

**Symptoms**: Metrics don't match expected error count

**Verify**:
1. Are you calling `sync_error_metrics()` regularly?
2. Are errors being properly caught and recorded?
3. Check `get_error_stats()` to verify internal buffer state

## Best Practices

### 1. Call sync_error_metrics() Periodically
```rust
// Before Prometheus scrape
metrics.sync_error_metrics();
```

### 2. Use record_sql_query_with_error() for Query Errors
```rust
// Captures both metrics AND error message
metrics.record_sql_query_with_error(
    query_type,
    duration,
    success,
    record_count,
    error_message,
);
```

### 3. Implement Alerting
```
alert_rule: error_rate_too_high
  condition: velo_unique_error_types > 5
  duration: 5m
  action: notify_ops_team
```

### 4. Monitor Buffer Fullness
```
alert_rule: error_buffer_full
  condition: velo_buffered_error_messages == 10
  duration: 1m
  action: investigate_active_errors
```

## API Reference

### MetricsProvider Methods

#### `record_error_message(message: String)`
Record an error message without query metrics

#### `get_error_stats() -> Option<ErrorStats>`
Get comprehensive error statistics

#### `get_top_errors(limit: usize) -> Vec<(String, u64)>`
Get top N most common errors sorted by frequency

#### `get_error_messages() -> Vec<ErrorEntry>`
Get all messages in current buffer

#### `get_total_errors() -> u64`
Get cumulative error count

#### `get_unique_error_types() -> usize`
Get count of unique error message types

#### `get_buffered_error_count() -> usize`
Get number of messages in rolling buffer

#### `sync_error_metrics()`
Update Prometheus gauges with current error state

#### `reset_error_tracking()`
Clear all error tracking data

## Related Documentation

- [Observability Module](../observability-module.md)
- [Prometheus Metrics Configuration](prometheus-configuration.md)
- [Grafana Dashboard Setup](grafana-setup.md)
- [FR-073: SQL-Native Observability](../../features/FR-073-sql-native-observability.md)
