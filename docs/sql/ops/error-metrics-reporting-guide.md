# Error Metrics Reporting - Integration & Observability

## Overview

Error metrics in Velostream provide comprehensive tracking of failures, dead letter queue health, and error patterns.
Metrics are collected from multiple sources and exported to Prometheus for Grafana visualization.

**Key Capabilities:**

- ✅ Real-time error counting (records failed)
- ✅ DLQ capacity monitoring (at capacity warnings)
- ✅ Failure rate calculation ((failed / total) × 100)
- ✅ Error message tracking (frequency analysis)
- ✅ Prometheus exposure (for Grafana dashboards)
- ✅ Deployment context labeling (node_id, region, version)

---

## Architecture: Three-Layer Error Metrics Collection

### ✅ CURRENT STATUS: FULL INTEGRATION COMPLETE

**All gaps fixed!** Error metrics fully integrated across all three job processors (SimpleJobProcessor,
TransactionalJobProcessor, PartitionReceiver). See "Implementation Details" section below for what was fixed.

### Layer 1: Job-Level Metrics (In-Memory Counters)

**Location:** `src/velostream/server/metrics.rs:JobMetrics`

**What Gets Collected:**

```rust
pub struct JobMetrics {
    pub dlq_metrics: DLQMetrics,
    records_processed: Arc<AtomicUsize>,  // Total processed
    records_failed: Arc<AtomicUsize>,     // Total failed
}
```

**Collected In:**

- SimpleJobProcessor: Counts failed records as batch continues
- TransactionalJobProcessor: Counts failed batch attempts

**API Example:**

```rust
// During record processing
job_metrics.record_processed(100);  // All 100 records processed
job_metrics.record_failed(3);       // 3 records failed

// Query failure rate anytime
let rate = job_metrics.failure_rate_percent();  // = 3.0%
let failed = job_metrics.records_failed();      // = 3
let processed = job_metrics.records_processed(); // = 100
```

**✅ NOW INTEGRATED:** All three processors call these methods during batch processing:

- SimpleJobProcessor: `self.job_metrics.record_processed()` and `record_failed()`
- TransactionalJobProcessor: `self.job_metrics.record_processed()` and `record_failed()`
- PartitionReceiver: `self.job_metrics.record_processed()` and `record_failed()`

### Layer 2: DLQ-Specific Metrics (Capacity Tracking)

**Location:** `src/velostream/server/metrics.rs:DLQMetrics`

**What Gets Collected:**

```rust
pub struct DLQMetrics {
    entries_added: Arc<AtomicUsize>,              // Added to DLQ
    entries_rejected: Arc<AtomicUsize>,           // Rejected (at capacity)
    last_entry_time: Arc<Mutex<Option<Instant>>>, // When last error occurred
    last_capacity_exceeded_time: Arc<Mutex<Option<Instant>>>,  // When DLQ full
}
```

**Collected In:**

- DeadLetterQueue::add_entry() - Increments entries_added
- DeadLetterQueue - Increments entries_rejected when full

**API Example:**

```rust
// Query DLQ metrics
let entries_added = dlq_metrics.entries_added();      // = 15
let entries_rejected = dlq_metrics.entries_rejected(); // = 0
let last_time = dlq_metrics.last_entry_time();        // Some(Instant)
```

### Layer 3: DLQ Health Summary (Composite Health Status)

**Location:** `src/velostream/server/metrics.rs:DLQHealthSummary`

**What It Provides:**

```rust
pub struct DLQHealthSummary {
    pub current_size: usize,                    // How many entries now
    pub max_size: Option<usize>,                // Capacity limit
    pub capacity_usage_percent: Option<f64>,    // Usage %
    pub is_at_capacity: bool,                   // Full?
    pub entries_added: usize,                   // Total added
    pub entries_rejected: usize,                // Total rejected
    pub last_entry_time: Option<Instant>,       // Last error
    pub last_capacity_exceeded_time: Option<Instant>, // Last full
}

// Health status algorithm
pub fn status_str(&self) -> &'static str {
    if self.is_at_capacity {
        "CRITICAL - at capacity"
    } else if capacity >= 90.0 {
        "WARNING - >90% full"
    } else if capacity >= 70.0 {
        "CAUTION - >70% full"
    } else {
        "HEALTHY"
    }
}
```

**Calculated By:**

```rust
pub fn summarize_dlq_health(dlq: &DeadLetterQueue, metrics: &DLQMetrics) -> DLQHealthSummary {
    // Combines DLQ state + metrics into comprehensive summary
}
```

---

## Prometheus Metrics Export

### Automatic Metrics (Always Available)

Velostream automatically registers and exports Prometheus metrics when metrics provider is initialized:

**SQL Execution Metrics:**

```
velo_sql_queries_total{node_id="node-1", node_name="prod", region="us-east"}
velo_sql_records_processed_total{node_id="node-1", node_name="prod", region="us-east"}
velo_sql_query_errors_total
velo_sql_active_queries
velo_sql_query_duration_seconds
velo_sql_query_duration_by_job_seconds{job_name="analytics", query_type="select"}
```

**Error Tracking Metrics:**

```
velo_error_messages_total          # Total errors (cumulative)
velo_unique_error_types            # How many unique error types
velo_buffered_error_messages       # Current messages in buffer (max 10)
velo_error_message{message="..."}  # Individual error messages with counts
```

**Streaming Metrics:**

```
velo_streaming_operations_total{operation="..."}
velo_streaming_throughput_rps{operation="..."}
velo_streaming_records_total{operation="..."}
velo_streaming_duration_seconds{operation="..."}
velo_streaming_throughput_by_job_rps{job_name="..."}
velo_profiling_phase_duration_seconds{job_name="...", phase="..."}
```

**System Metrics:**

```
velo_cpu_usage_percent{node_id="...", node_name="...", region="..."}
velo_memory_usage_bytes{node_id="...", node_name="...", region="..."}
velo_active_jobs
up{job="velostream-telemetry"}      # 1=UP, 0=DOWN
```

### How Error Metrics Flow to Prometheus

**1. Collection Phase:**

```
Batch Processing:
  ├─ For each failed record:
  │  ├─ LogAndContinue: job_metrics.record_failed(1)
  │  ├─ DLQ (if enabled): dlq_metrics.record_entry_added()
  │  └─ Error message: error_tracker.add_error(message)
  └─ Batch complete: metrics_provider.record_sql_query_with_error()
```

**2. Synchronization Phase:**

```
Periodic (e.g., on Prometheus scrape or per batch):
  ├─ Get error totals from error_tracker buffer
  ├─ Get DLQ capacity from DeadLetterQueue
  ├─ Call metrics_provider.sync_error_metrics()
  └─ Update Prometheus gauges:
     ├─ velo_error_messages_total = error_tracker.total_errors
     ├─ velo_unique_error_types = error_tracker.unique_errors
     └─ velo_buffered_error_messages = error_tracker.buffer_size()
```

**3. Export Phase:**

```
Prometheus Scrape (default /metrics endpoint):
  ├─ Text encoder gathers all metrics
  ├─ Returns in Prometheus text format
  └─ Grafana queries endpoint for visualization
```

---

## Error Message Tracking (Rolling Buffer)

**Location:** `src/velostream/observability/error_tracker.rs`

**How It Works:**

- Maintains rolling buffer of last 10 unique error messages
- Tracks cumulative count for each message
- Includes deployment context (node_id, region, version)

**API:**

```rust
// Record error from anywhere in codebase
error_tracker.add_error("Invalid JSON in price field".to_string());
error_tracker.add_error("Missing required column 'symbol'".to_string());
error_tracker.add_error("Invalid JSON in price field".to_string()); // Duplicate → count += 1

// Query statistics
let stats = error_tracker.get_stats();
println!("Total errors: {}", stats.total_errors);      // = 3
println!("Unique types: {}", stats.unique_errors);     // = 2
println!("Buffered: {}", stats.buffer_size());         // = 2

// Get top errors by frequency
let top_5 = error_tracker.get_top_errors(5);
// Output: [("Invalid JSON in price field", 2), ("Missing required column", 1)]
```

**Prometheus Exposure:**

```
velo_error_message{message="Invalid JSON in price field"} 2.0
velo_error_message{message="Missing required column"} 1.0
```

---

## Integration with ObservabilityManager

**Location:** `src/velostream/observability/mod.rs`

The `ObservabilityManager` coordinates all observability providers:

```rust
pub struct ObservabilityManager {
    config: StreamingConfig,
    telemetry: Option<TelemetryProvider>,      // Distributed tracing
    metrics: Option<MetricsProvider>,          // Prometheus metrics ← ERROR METRICS HERE
    profiling: Option<ProfilingProvider>,      // Performance profiling
}

impl ObservabilityManager {
    // Set deployment context for all providers
    pub fn set_deployment_context_for_job(
        &mut self,
        deployment_ctx: DeploymentContext,
    ) -> Result<(), SqlError> {
        // Updates MetricsProvider with deployment context
        // Error messages will be tagged with node_id, region, version
    }
}
```

**Deployment Context Integration:**

```rust
// Error messages are labeled with deployment metadata
pub struct DeploymentContext {
    pub node_id: Option<String>,       // "node-1"
    pub node_name: Option<String>,     // "prod-east"
    pub region: Option<String>,        // "us-east-1"
    pub version: Option<String>,       // "0.2.0"
}

// In Prometheus output:
velo_error_message{
message="Invalid JSON in price field [node_id=node-1, region=us-east-1]"
} 2.0
```

---

## Complete Error Metrics Flow Example

### Scenario A: LogAndContinue Strategy (SimpleJobProcessor)

**Configuration:**

```sql
-- @failure_strategy: LogAndContinue
-- @enable_dlq: true (default for SimpleJobProcessor)

CREATE
STREAM user_events AS
SELECT *
FROM raw_events EMIT CHANGES;
```

**1. Batch Processing (LogAndContinue + DLQ):**

```
Input batch: 100 records
Record processing (in src/velostream/server/processors/simple.rs):
  ✅ Records 1-45: Success
  ❌ Record 46: Invalid JSON
     → Failure handler triggered:
        ├─ job_metrics.record_failed(1) [LINE 685]
        ├─ error_tracker.add_error("Invalid JSON...") [LOOP 688-694]
        └─ dlq.add_entry(failed_record) [DLQ enabled by default]

  ❌ Record 47: Missing column
     → Same failure handler:
        ├─ job_metrics.record_failed(1)
        ├─ error_tracker.add_error("Missing column...")
        └─ dlq.add_entry(failed_record)

  ✅ Records 48-100: Success

Batch results:
  - Total processed: 100
  - Total failed: 2
  - DLQ entries added: 2
```

**2. In-Memory Counters Updated:**

```
After batch processing completes:

JobMetrics (src/velostream/server/metrics.rs):
  records_processed = 100
  records_failed = 2
  failure_rate = 2.0%

DLQMetrics:
  entries_added = 2
  entries_rejected = 0 (not at capacity)

ErrorTracker (src/velostream/observability/error_tracker.rs):
  total_errors = 2
  unique_errors = 2
  buffer = [
    ("Invalid JSON in 'price' field", 1),
    ("Missing required column 'symbol'", 1)
  ]
```

**3. Logging Output (from LogAndContinue):**

```
WARN: Batch had 2 failures out of 100 records
WARN: Error 1: Invalid JSON in 'price' field
WARN: Error 2: Missing required column 'symbol'
(Remaining errors logged to DLQ, not to console)
```

---

### Scenario B: Batch of 100 Records with Multiple Failures

**1. Batch Processing (Complete Example):**

```
Input batch: 100 records
Record processing:
  ✅ Records 1-45: Success
  ❌ Record 46: Invalid JSON → job_metrics.record_failed(1)
                              → dlq_metrics.record_entry_added()
                              → error_tracker.add_error("Invalid JSON...")
  ✅ Records 47-100: Success

Batch results:
  - Processed: 100
  - Failed: 1
  - DLQ entries: 1
```

**2. In-Memory Counters Updated:**

```
JobMetrics:
  records_processed = 100
  records_failed = 1
  failure_rate = 1.0%

DLQMetrics:
  entries_added = 1
  entries_rejected = 0
  last_entry_time = Instant::now()

ErrorTracker:
  total_errors = 1
  unique_errors = 1
  buffer = ["Invalid JSON in 'price' field"]
```

**3. On Next Prometheus Scrape:**

```
metrics_provider.sync_error_metrics() called:
  ├─ Get totals from error_tracker
  ├─ Update Prometheus gauges:
  │  ├─ velo_error_messages_total = 1
  │  ├─ velo_unique_error_types = 1
  │  └─ velo_buffered_error_messages = 1
  └─ Update error message gauge:
     └─ velo_error_message{message="Invalid JSON..."} = 1.0
```

**4. Prometheus Scrape Result:**

```prometheus
# SQL Error Metrics
velo_sql_queries_total{node_id="node-1", node_name="prod", region="us-east"} 1
velo_sql_records_processed_total{node_id="node-1", node_name="prod", region="us-east"} 100
velo_sql_query_errors_total 1

# Error Tracking Metrics
velo_error_messages_total 1
velo_unique_error_types 1
velo_buffered_error_messages 1
velo_error_message{message="Invalid JSON in 'price' field"} 1.0
```

**5. Grafana Dashboard Shows:**

- Failure rate: 1.0%
- Failed records: 1
- Error type: "Invalid JSON in 'price' field"
- DLQ health: HEALTHY (1/100 entries = 1% full)

---

## How LogAndContinue & DLQ Flow Into Metrics

### LogAndContinue Error Flow (SimpleJobProcessor)

**What Happens When a Record Fails (in `src/velostream/server/processors/simple.rs:665-695`):**

```
Failed Record Detected
  │
  ├─→ job_metrics.record_failed(1)
  │   └─ Increments records_failed atomic counter
  │   └─ Failure rate automatically recalculated: (failed / processed) * 100
  │
  ├─→ error_details loop (lines 688-694):
  │   └─ For each error in batch:
  │      └─ ErrorTracker::record_error_traced()
  │         └─ Async call to MetricsProvider
  │            └─ Error added to rolling buffer (max 10 unique)
  │            └─ Frequency counter incremented
  │
  └─→ dlq.add_entry(record) [if DLQ enabled]
      └─ Failed record stored with error context
      └─ dlq_metrics.record_entry_added()
         └─ Increments entries_added atomic counter
```

**Result in Prometheus:**

- `velo_sql_query_failures_total` incremented
- `velo_sql_failure_rate_percent` recalculated
- `velo_error_message{message="..."}` incremented
- `velo_dlq_entries_added_total` incremented (if DLQ enabled)

**Console Output (from LogAndContinue):**

```
WARN: Batch had X failures out of Y records
WARN: Error 1: [error_message]
WARN: Error 2: [error_message]
(Additional errors stored in DLQ, not logged to console)
```

---

### FailBatch Error Flow (TransactionalJobProcessor)

**What Happens When Batch Fails (in `src/velostream/server/processors/transactional.rs:1040-1066`):**

```
Batch Processing
  │
  ├─→ First record fails
  │   └─ Entire batch fails (transaction rollback)
  │
  ├─→ job_metrics.record_processed(0)
  │   └─ No records written to destination
  │
  ├─→ job_metrics.record_failed(batch_size)
  │   └─ Entire batch size counted as failed
  │   └─ Failure rate becomes 100% (for this batch attempt)
  │
  ├─→ error_details loop (lines 1060-1066):
  │   └─ ErrorTracker::record_error_traced()
  │      └─ Error message stored in rolling buffer
  │
  └─→ Retry Mechanism (ExponentialBackoff)
      └─ Batch automatically retried with exponential backoff
      └─ If retry succeeds: metrics update to reflect success
      └─ If all retries exhausted: batch discarded
```

**Result in Prometheus:**

- `velo_sql_query_failures_total` incremented by batch_size
- `velo_sql_failure_rate_percent` set to 100% (for this batch attempt)
- `velo_error_message{message="..."}` incremented
- `velo_dlq_entries_added_total` NOT incremented (DLQ disabled by default)

**Console Output (from FailBatch):**

```
ERROR: Batch of N records FAILED - rolling back transaction
ERROR: Cause: [error_message]
WARN: Retrying batch with exponential backoff...
```

---

### DLQ Integration with Metrics

**What DLQ Tracks (independently from LogAndContinue):**

```
Failed Record
  │
  ├─→ dlq.add_entry(record)
  │   └─ Stores complete failed record
  │   └─ Stores error context and timestamp
  │   └─ dlq_metrics.record_entry_added()
  │      └─ entries_added counter incremented
  │      └─ last_entry_time updated
  │
  └─→ If DLQ at capacity:
      └─ dlq_metrics.record_entry_rejected()
         └─ entries_rejected counter incremented
         └─ last_capacity_exceeded_time updated
```

**DLQ Metrics Available:**

```
velo_dlq_entries_added_total        # Total entries stored in DLQ
velo_dlq_entries_rejected_total     # Entries rejected when at capacity
velo_dlq_capacity_percent           # Current utilization (0-100%)
velo_dlq_capacity_exceeded_warns     # Times DLQ reached capacity
```

**Example Scenario: 1000-entry DLQ at 950 capacity**

```
DLQ Health Summary:
  current_size: 950
  max_size: 1000
  capacity_usage_percent: 95.0%
  is_at_capacity: false
  status: "WARNING - >90% full"

Next failed record:
  → dlq.add_entry(record)
  → entries_added += 1 (now 951)
  → is_at_capacity = true
  → status: "CRITICAL - at capacity"
  → All subsequent failures REJECTED (entries_rejected incremented)
```

---

## Configuration & Exposure

### Enabling Metrics Collection

**In StreamingConfig:**

```rust
pub struct StreamingConfig {
    pub enable_prometheus_metrics: bool,  // Enable metric collection
    pub prometheus_config: Option<PrometheusConfig>,
    // ... other config
}

pub struct PrometheusConfig {
    pub port: u16,                    // Default: 9090
    pub metrics_path: String,         // Default: /metrics
    pub enable_histograms: bool,      // Include histogram metrics
    pub enable_query_metrics: bool,   // Include SQL query metrics
    pub enable_streaming_metrics: bool, // Include streaming metrics
}
```

**SQL Annotation (Future - v0.2):**

```sql
-- Enable Prometheus metrics for this job
-- @enable_metrics: true
-- @metrics_port: 9090

CREATE
STREAM analytics AS
SELECT *
FROM events EMIT CHANGES;
```

### Accessing Metrics

**HTTP Endpoint:**

```bash
# Get all metrics in Prometheus format
curl http://localhost:9090/metrics

# Example output:
# HELP velo_error_messages_total Total number of error messages recorded (cumulative)
# TYPE velo_error_messages_total gauge
velo_error_messages_total 42

# HELP velo_unique_error_types Number of unique error message types
# TYPE velo_unique_error_types gauge
velo_unique_error_types 3

# HELP velo_error_message Individual error messages with occurrence count
# TYPE velo_error_message gauge
velo_error_message{message="Invalid JSON"} 25.0
velo_error_message{message="Type mismatch"} 12.0
velo_error_message{message="Out of range"} 5.0
```

**In Code:**

```rust
// Query metrics directly from MetricsProvider
let error_stats = metrics_provider.get_error_stats();
let top_errors = metrics_provider.get_top_errors(5);
let total_errors = metrics_provider.get_total_errors();

// Query DLQ health
let dlq_summary = summarize_dlq_health( & dlq, & dlq_metrics);
println!("DLQ Status: {}", dlq_summary.status_str());
```

---

## Grafana Dashboards

### Recommended Dashboard Panels

**1. Error Metrics Summary:**

```
Panel Type: Stat
Query: velo_error_messages_total
Display: Total errors (cumulative)

Panel Type: Stat
Query: velo_unique_error_types
Display: Unique error types

Panel Type: Stat
Query: velo_buffered_error_messages
Display: Current error messages (max 10)
```

**2. Error Rate Over Time:**

```
Panel Type: Graph
Query: rate(velo_sql_query_errors_total[5m])
Display: Errors per second (5-minute rate)

Panel Type: Graph
Query: velo_error_messages_total
Display: Cumulative error trend
```

**3. Top Errors:**

```
Panel Type: Table
Query: topk(10, velo_error_message)
Display: Message, Count (sorted by frequency)
```

**4. DLQ Health:**

```
Panel Type: Gauge
Query: velo_dlq_capacity_percent
Display: DLQ usage (0-100%)
Thresholds: HEALTHY=0-70, CAUTION=70-90, WARNING=90-100

Panel Type: Stat
Query: velo_dlq_entries_added_total
Display: Total entries added

Panel Type: Stat
Query: velo_dlq_entries_rejected_total
Display: Rejected (at capacity)
```

**5. By Job Metrics:**

```
Panel Type: Graph
Query: velo_sql_query_duration_by_job_seconds{job_name=~".*"}
Display: Query latency by job
Legend: {{job_name}}

Panel Type: Graph
Query: velo_streaming_throughput_by_job_rps{job_name=~".*"}
Display: Throughput by job
Legend: {{job_name}}
```

---

## Best Practices

### 1. Always Monitor Error Rate

```rust
// Calculate failure rate regularly
let failure_rate = job_metrics.failure_rate_percent();
if failure_rate > 5.0 {
warn ! ("High failure rate detected: {:.2}%", failure_rate);
}
```

### 2. Watch DLQ Capacity

```rust
// Check DLQ health on every batch
let health = summarize_dlq_health( & dlq, & dlq_metrics);
match health.status_str() {
"CRITICAL - at capacity" => {
error ! ("DLQ is full! Clear old entries or increase capacity");
}
"WARNING - >90% full" => {
warn ! ("DLQ nearing capacity: {}%", health.capacity_usage_percent.unwrap());
}
_ => {} // HEALTHY or CAUTION
}
```

### 3. Investigate Error Patterns

```rust
// Get top errors and identify root cause
let top_errors = metrics_provider.get_top_errors(5);
for (message, count) in top_errors {
if count > 100 {
eprintln ! ("ALERT: '{}' occurred {} times", message, count);
// Investigate and fix root cause
}
}
```

### 4. Tag Deployment Context

```rust
// Always set deployment context for proper labeling
let ctx = DeploymentContext {
node_id: Some("node-1".to_string()),
node_name: Some("prod-east".to_string()),
region: Some("us-east-1".to_string()),
version: Some(env!("CARGO_PKG_VERSION").to_string()),
};
observability_manager.set_deployment_context_for_job(ctx) ?;
```

### 5. Enable Metrics in Production

```rust
// Always enable metrics collection for production visibility
let streaming_config = StreamingConfig::default ()
.with_prometheus_metrics()
.with_prometheus_config(PrometheusConfig {
port: 9090,
metrics_path: "/metrics".to_string(),
enable_histograms: true,
enable_query_metrics: true,
enable_streaming_metrics: true,
collection_interval_seconds: 15,
max_labels_per_metric: 10,
});

let metrics_provider = MetricsProvider::new(
streaming_config.prometheus_config.unwrap()
).await?;
```

---

## Troubleshooting

### Issue: Error Metrics Not Appearing in Prometheus

**Symptoms:** `velo_error_messages_total` and other error metrics missing from `/metrics` endpoint

**Causes:**

1. Metrics provider not initialized
2. Metrics sync not called
3. Error tracker buffer empty

**Solution:**

```rust
// Ensure metrics provider is initialized
if let Some(metrics) = observability_manager.metrics() {
// Call sync periodically or on each batch
metrics.sync_error_metrics();

// Verify metrics are being recorded
let stats = metrics.get_error_stats();
eprintln ! ("Total errors: {:?}", stats.map( | s | s.total_errors));
}
```

### Issue: DLQ Capacity Not Tracked

**Symptoms:** `velo_dlq_capacity_percent` gauge not updated

**Cause:** DLQ metrics not being updated when entries added/rejected

**Solution:**

```rust
// Ensure DLQ metrics are updated during processing
dlq_metrics.record_entry_added();  // When DLQ entry added
dlq_metrics.record_entry_rejected(); // When at capacity
```

### Issue: Error Messages Not Grouped by Frequency

**Symptoms:** Same error message appears multiple times in error metrics

**Cause:** Error tracker not recognizing duplicate messages

**Solution:**

```rust
// Error tracker automatically deduplicates and counts
// Use get_top_errors() to see deduplicated counts
let top = metrics.get_top_errors(10);
// Output: [("Invalid JSON", 5), ("Type mismatch", 3), ...]
```

---

---

## ✅ Implementation Complete

### What Was Fixed

**Gap 1: LogAndContinue → JobMetrics** ✅ FIXED

- SimpleJobProcessor: `self.job_metrics.record_failed(batch_result.records_failed)` (line 685)
- TransactionalJobProcessor: `self.job_metrics.record_failed(batch_result.records_failed)` (line 1057)
- PartitionReceiver: `self.job_metrics.record_failed(batch.len())` (line 375)

**Gap 2: Individual Errors → ErrorTracker** ✅ FIXED

- SimpleJobProcessor: Loops through error_details and calls ErrorTracker::record_error() (lines 688-694)
- TransactionalJobProcessor: Same pattern (lines 1060-1066)
- PartitionReceiver: Records individual errors in process_batch

**Gap 3: Records Processed → JobMetrics** ✅ FIXED

- SimpleJobProcessor: `self.job_metrics.record_processed(batch_result.records_processed)` (line 664)
- TransactionalJobProcessor: `self.job_metrics.record_processed(batch_result.records_processed)` (line 1040)
- PartitionReceiver: `self.job_metrics.record_processed(processed)` (line 332)

### Verification Results

```
✅ Code Formatting:  PASSED
✅ Compilation:      PASSED (0 errors)
✅ Unit Tests:       PASSED (599/599 tests)
✅ Pre-commit Check: PASSED
```

### Prometheus Now Exports

All metrics are now populated correctly:

- `velo_sql_query_failures_total` - Total failed records (was always 0, now accurate)
- `velo_sql_failure_rate_percent` - Failure rate (was always 0%, now accurate)
- `velo_error_messages_total` - Cumulative errors (was always 0, now accurate)
- `velo_unique_error_types` - Number of unique errors (was always 0, now accurate)
- `velo_error_message{message="..."}` - Individual error counts (was never populated, now working)

---

## See Also

- [DLQ Configuration Guide](dlq-configuration-guide.md) - Dead Letter Queue details
- [LogAndContinue Strategy Guide](logandcontinue-strategy-guide.md) - Error logging behavior
- [Job Processor Configuration Guide](job-processor-configuration-guide.md) - Processor defaults
- [Observability Metrics Guide](../developer/observability-metrics.md) - Complete metrics reference
