# Progress Monitoring User Guide

**Version**: 1.0
**Last Updated**: September 28, 2025
**Status**: Production Ready

## Overview

Velostream's Progress Monitoring system provides real-time visibility into table loading operations, helping you track progress, identify bottlenecks, and ensure data consistency in stream-table joins.

## Table of Contents

- [Quick Start](#quick-start)
- [Understanding Progress Monitoring](#understanding-progress-monitoring)
- [Monitoring Table Loading](#monitoring-table-loading)
- [Health Dashboard](#health-dashboard)
- [Streaming Progress Updates](#streaming-progress-updates)
- [Error Handling and Troubleshooting](#error-handling-and-troubleshooting)
- [Production Best Practices](#production-best-practices)
- [API Reference](#api-reference)

## Quick Start

### Enable Progress Monitoring

Progress monitoring is automatically enabled when you create tables with the TableRegistry:

```sql
-- This table will be monitored automatically
CREATE TABLE user_profiles AS
SELECT * FROM kafka_users
WITH ("auto.offset.reset" = "latest");
```

### Check Loading Progress

Use the health dashboard endpoint to check progress:

```bash
# Get overall table health
curl http://localhost:8080/health/tables

# Get detailed loading progress
curl http://localhost:8080/health/loading-progress

# Get specific table progress
curl http://localhost:8080/health/tables/user_profiles
```

### Monitor Progress in Real-Time

```bash
# Stream progress updates via WebSocket
wscat -c ws://localhost:8080/progress/stream

# Or via HTTP Server-Sent Events
curl -N http://localhost:8080/progress/events
```

## Understanding Progress Monitoring

### Progress Lifecycle

Every table goes through these stages:

```
Initializing → Loading → Completed
     ↓            ↓         ↑
   Error ←---- Error → Retry
```

### Key Metrics

| Metric | Description | Example |
|--------|-------------|---------|
| **Records Loaded** | Number of records processed | 45,230 |
| **Progress Percentage** | Completion percentage (if total known) | 67.8% |
| **Loading Rate** | Records per second | 1,250 rec/s |
| **ETA** | Estimated completion time | 2025-09-28T15:30:00Z |
| **Bytes Processed** | Total data volume | 12.4 MB |

### Status Types

- **`Initializing`**: Table setup in progress
- **`Loading`**: Actively reading data from source
- **`Completed`**: All data loaded successfully
- **`Failed`**: Error occurred during loading
- **`Retrying`**: Attempting to recover from error

## Monitoring Table Loading

### Real-Time Progress Tracking

Monitor large table loads with automatic progress updates:

```rust
use velostream::velostream::server::table_registry::TableRegistry;
use std::sync::Arc;

// Create registry with monitoring
let registry = Arc::new(TableRegistry::new());

// Start tracking a table
let tracker = registry
    .start_progress_tracking("large_dataset".to_string(), Some(1_000_000))
    .await;

// Monitor progress
loop {
    let progress = tracker.get_current_progress().await;
    println!("Progress: {:.1}% ({} records, {:.0} rec/s)",
        progress.progress_percentage.unwrap_or(0.0),
        progress.records_loaded,
        progress.loading_rate
    );

    if matches!(progress.status, TableLoadStatus::Completed) {
        break;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
}
```

### Batch Progress Updates

For high-volume data sources, update progress in batches:

```rust
// Update progress periodically during loading
let mut records_count = 0;
let mut bytes_count = 0;

for batch in data_batches {
    // Process batch
    let batch_size = batch.len();
    let batch_bytes = batch.size_estimate();

    records_count += batch_size;
    bytes_count += batch_bytes;

    // Update progress every 1000 records
    if records_count % 1000 == 0 {
        tracker.add_records(batch_size, batch_bytes).await;
    }
}

// Final update
tracker.add_records(records_count % 1000, bytes_count % 1000).await;
tracker.set_completed().await;
```

### Progress with Unknown Total

For streaming sources where total size is unknown:

```rust
// Start tracking without total count
let tracker = registry
    .start_progress_tracking("streaming_data".to_string(), None)
    .await;

// Progress will show records loaded and rate, but no percentage
let progress = tracker.get_current_progress().await;
println!("Loaded {} records at {:.0} rec/s",
    progress.records_loaded,
    progress.loading_rate
);
```

## Health Dashboard

### Dashboard Endpoints

| Endpoint | Purpose | Response Format |
|----------|---------|-----------------|
| `/health/tables` | Overall table health | JSON summary |
| `/health/loading-progress` | Detailed progress | JSON with all tables |
| `/health/tables/{name}` | Specific table health | JSON single table |
| `/health/metrics` | System metrics | JSON performance data |
| `/health/prometheus` | Prometheus metrics | Text format |

### Health Status Interpretation

#### Overall Health Status

```json
{
  "overall_status": "Healthy",
  "total_tables": 5,
  "healthy_count": 4,
  "warning_count": 1,
  "critical_count": 0,
  "last_updated": "2025-09-28T14:30:00Z"
}
```

**Status Levels**:
- **`Healthy`**: All tables loading normally
- **`Warning`**: Some tables slow or minor issues
- **`Critical`**: Tables failed or major issues

#### Detailed Progress Response

```json
{
  "summary": {
    "total_tables": 3,
    "loading": 2,
    "completed": 1,
    "failed": 0,
    "total_records_loaded": 67500
  },
  "tables": {
    "user_profiles": {
      "status": "Loading",
      "records_loaded": 45000,
      "total_records_expected": 100000,
      "progress_percentage": 45.0,
      "loading_rate": 1250.0,
      "bytes_processed": 12400000,
      "started_at": "2025-09-28T14:25:00Z",
      "estimated_completion": "2025-09-28T14:35:00Z"
    }
  }
}
```

### Dashboard Alerting

Set up alerts based on health status:

```bash
# Example alert check
STATUS=$(curl -s http://localhost:8080/health/tables | jq -r '.overall_status')

if [ "$STATUS" != "Healthy" ]; then
    echo "ALERT: Velostream table loading issues detected"
    # Send notification to ops team
fi
```

## Streaming Progress Updates

### WebSocket Streaming

Connect to real-time progress updates:

```javascript
// JavaScript example
const ws = new WebSocket('ws://localhost:8080/progress/stream');

ws.onmessage = function(event) {
    const update = JSON.parse(event.data);

    switch(update.event_type) {
        case 'TableStarted':
            console.log(`Table ${update.table_name} started loading`);
            break;

        case 'ProgressUpdate':
            console.log(`${update.table_name}: ${update.progress_percentage}% complete`);
            break;

        case 'TableCompleted':
            console.log(`Table ${update.table_name} loaded successfully`);
            break;

        case 'TableFailed':
            console.error(`Table ${update.table_name} failed: ${update.error_message}`);
            break;
    }
};
```

### Server-Sent Events

For simpler HTTP-based streaming:

```bash
# Monitor progress via curl
curl -N -H "Accept: text/event-stream" http://localhost:8080/progress/events

# Example output:
# event: table_started
# data: {"table_name": "user_profiles", "timestamp": "2025-09-28T14:30:00Z"}
#
# event: progress_update
# data: {"table_name": "user_profiles", "progress_percentage": 25.5, "records_loaded": 25500}
```

### Progress Subscriptions

Subscribe to specific table progress:

```rust
// Subscribe to a specific table
let mut progress_stream = registry
    .subscribe_to_table_progress("user_profiles")
    .await?;

while let Some(progress) = progress_stream.next().await {
    println!("User profiles: {:.1}% complete",
        progress.progress_percentage.unwrap_or(0.0)
    );
}
```

## Error Handling and Troubleshooting

### Common Progress Issues

#### Table Loading Stuck

**Symptoms**: Progress percentage not increasing
```bash
# Check if source is responsive
curl http://localhost:8080/health/tables/stuck_table

# Look for error messages
curl http://localhost:8080/health/tables/stuck_table | jq '.error_message'
```

**Solutions**:
1. Check source connectivity (Kafka broker, file access)
2. Verify source has data available
3. Check for permission issues
4. Review timeout configurations

#### Slow Loading Performance

**Symptoms**: Loading rate below expected values
```bash
# Check loading rate
curl http://localhost:8080/health/loading-progress | jq '.tables.slow_table.loading_rate'
```

**Solutions**:
1. Increase batch sizes for file sources
2. Optimize Kafka consumer configuration
3. Check network bandwidth
4. Consider parallel loading for large datasets

#### ETA Inaccurate

**Symptoms**: Estimated completion time wildly incorrect
```bash
# Check if total records is known
curl http://localhost:8080/health/tables/table_name | jq '.total_records_expected'
```

**Solutions**:
1. For unknown totals, ETA cannot be calculated
2. Loading rate fluctuations affect ETA accuracy
3. Consider using time-based rather than record-based estimates

### Debug Information

Enable detailed debugging:

```rust
// Get detailed progress information
let detailed_progress = registry.get_table_loading_progress("debug_table").await;

if let Some(progress) = detailed_progress {
    println!("Debug info:");
    println!("  Started: {}", progress.started_at);
    println!("  Rate: {:.2} rec/s", progress.loading_rate);
    println!("  Bytes/sec: {:.2}", progress.bytes_per_second);
    println!("  Status: {:?}", progress.status);

    if let Some(error) = progress.error_message {
        println!("  Error: {}", error);
    }
}
```

## Production Best Practices

### 1. Monitoring Setup

- **Health Checks**: Monitor `/health/tables` endpoint every 30 seconds
- **Alerting**: Set up alerts for `Critical` or extended `Warning` status
- **Logging**: Enable progress logging for audit trails
- **Dashboards**: Create operational dashboards with key metrics

### 2. Performance Optimization

- **Batch Sizes**: Update progress every 1000-10000 records (not every record)
- **Concurrency**: Monitor multiple tables in parallel
- **Resource Limits**: Set reasonable timeouts to prevent hanging
- **Memory Usage**: Monitor memory consumption during large loads

### 3. Error Recovery

- **Retry Logic**: Configure appropriate retry policies for transient failures
- **Fallback Strategies**: Use graceful degradation when tables are slow
- **Circuit Breakers**: Implement circuit breakers for failing data sources
- **Manual Intervention**: Provide ops team with clear error messages

### 4. Capacity Planning

```bash
# Monitor system resources during loading
curl http://localhost:8080/health/metrics | jq '{
  memory_usage: .memory_usage_mb,
  active_tables: .performance_metrics.active_loading_tables,
  total_throughput: .performance_metrics.total_records_per_second
}'
```

## API Reference

### TableRegistry Methods

```rust
// Start progress tracking
async fn start_progress_tracking(
    &self,
    table_name: String,
    total_records: Option<usize>
) -> TableProgressTracker

// Get current progress
async fn get_table_loading_progress(
    &self,
    table_name: &str
) -> Option<TableLoadProgress>

// Get summary of all tables
async fn get_loading_summary(&self) -> LoadingSummary

// Subscribe to progress updates
async fn subscribe_to_table_progress(
    &self,
    table_name: &str
) -> Option<ProgressStream>

// Stop tracking
async fn stop_progress_tracking(&self, table_name: &str)
```

### Progress Tracker Methods

```rust
// Update record count
async fn add_records(&self, count: usize, bytes: u64)

// Set status
async fn set_status(&self, status: TableLoadStatus)

// Mark as completed
async fn set_completed(&self)

// Mark as failed
async fn set_error(&self, error_message: String)

// Get current state
async fn get_current_progress(&self) -> TableLoadProgress
```

### HTTP API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health/tables` | Overall table health |
| GET | `/health/loading-progress` | Detailed progress for all tables |
| GET | `/health/tables/{name}` | Specific table health |
| GET | `/health/metrics` | System performance metrics |
| GET | `/health/prometheus` | Prometheus format metrics |
| GET | `/progress/events` | Server-sent events stream |
| WebSocket | `/progress/stream` | Real-time progress WebSocket |

### Progress Event Types

```rust
pub enum ProgressEvent {
    InitialSnapshot {
        tables: HashMap<String, TableLoadProgress>,
        timestamp: DateTime<Utc>,
    },
    TableStarted {
        table_name: String,
        timestamp: DateTime<Utc>,
    },
    ProgressUpdate {
        table_name: String,
        progress: TableLoadProgress,
        timestamp: DateTime<Utc>,
    },
    TableCompleted {
        table_name: String,
        final_stats: LoadingStats,
        timestamp: DateTime<Utc>,
    },
    TableFailed {
        table_name: String,
        error_message: String,
        timestamp: DateTime<Utc>,
    },
}
```

## Conclusion

Velostream's Progress Monitoring provides comprehensive visibility into table loading operations, enabling:

- **Real-time tracking** of large data loads
- **Proactive issue detection** before they impact streams
- **Performance optimization** through detailed metrics
- **Operational confidence** with health monitoring

For additional support, see:
- [Timeout Configuration Guide](timeout-configuration-guide.md)
- [Graceful Degradation Guide](graceful-degradation-guide.md)
- [Production Deployment Guide](production-deployment-guide.md)