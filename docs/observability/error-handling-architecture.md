# Velostream Error Handling Architecture Analysis

## Executive Summary

This document provides a comprehensive analysis of how Velostream handles errors, where they are logged/exposed, and how error messages flow through the system. The investigation reveals that **velo_deployment.log is NOT a file written by the codebase** - instead, errors follow a sophisticated multi-layered observability architecture.

---

## 1. WHERE ERRORS ARE LOGGED

### 1.1 Standard Rust Logging (Primary)

**Location**: Throughout the codebase using Rust `log` crate macros

**Files Using Logging**:
- `/src/velostream/server/stream_job_server.rs` (lines 25+)
- `/src/velostream/server/processors/simple.rs`
- `/src/velostream/server/processors/transactional.rs`
- `/src/velostream/datasource/kafka/reader.rs`
- And 393+ total occurrences across the codebase

**Logging Levels**:
```rust
log::error!()   // Critical failures
log::warn!()    // Non-critical issues
log::info!()    // Informational messages
log::debug!()   // Detailed diagnostics
log::trace!()   // Most verbose
```

**Example from stream_job_server.rs**:
```rust
// Line 742-745
error!("Job '{}' failed (transactional): {:?}", job_name, e);

// Line 360-361
info!("Deploying job '{}' version '{}' on topic '{}': {}",
      name, version, topic, query);
```

### 1.2 In-Memory Error Buffer (Error Tracker)

**Location**: `src/velostream/observability/error_tracker.rs`

**Components**:
- `ErrorMessageBuffer` - Rolling FIFO buffer storing last 10 error messages
- `ErrorEntry` - Individual error entries with timestamps and context
- `DeploymentContext` - Metadata (node_id, node_name, region, version)

**Key Features**:
```rust
pub struct ErrorMessageBuffer {
    messages: Vec<ErrorEntry>,              // Last 10 errors (FIFO)
    message_counts: HashMap<String, u64>,  // Count per unique message
    total_errors: u64,                      // Cumulative counter
    node_id: Option<String>,                // Node context
    deployment_context: Option<DeploymentContext>
}

pub struct ErrorEntry {
    pub message: String,
    pub timestamp: u64,                     // Unix timestamp (seconds)
    pub count: u64,                         // Occurrences of this message
    pub node_id: Option<String>,
    pub deployment_context: Option<DeploymentContext>,
}
```

**Properties**:
- Maximum capacity: 10 messages (oldest automatically evicted)
- Duplicate handling: Same message increments count, doesn't duplicate entry
- Thread-safe: Uses `Arc<Mutex<>>`
- Timestamp: Per-error Unix timestamp (second precision)

### 1.3 Prometheus Metrics Exposure

**Location**: `src/velostream/observability/metrics.rs`

**Error Tracking Metrics** (Lines 1070-1120):

```rust
// Total cumulative errors
pub metric: velo_error_messages_total (IntGauge)

// Count of unique error message types
pub metric: velo_unique_error_types (IntGauge)

// Current messages in rolling buffer (0-10)
pub metric: velo_buffered_error_messages (IntGauge)

// Individual error messages with occurrence counts
pub metric: velo_error_message (GaugeVec with label "message")
```

**Metrics Provider Methods** (Lines 875-920):
```rust
pub fn record_error_message(&self, message: String)
pub fn get_error_stats() -> Option<ErrorStats>
pub fn get_top_errors(limit: usize) -> Vec<(String, u64)>
pub fn get_error_messages() -> Vec<ErrorEntry>
pub fn sync_error_metrics()  // Updates Prometheus gauges
```

---

## 2. ERROR TRACKING FLOW

### 2.1 Complete Data Flow Path

```
┌──────────────────────────────┐
│ 1. Error Occurs in Job       │
│    (Connection failure,      │
│     Query timeout, etc.)     │
└─────────────┬────────────────┘
              │
              ▼
┌──────────────────────────────┐
│ 2. Error Captured at Source  │
│    - stream_job_server.rs    │
│    - error_tracking_helper.rs│
│    - processors (simple/tx)  │
└─────────────┬────────────────┘
              │
              ▼
┌──────────────────────────────┐
│ 3a. Standard Log Output      │
│    - log::error!() macro     │
│    - Goes to stderr/loggers  │
│    - No file by default      │
└─────────────┬────────────────┘
              │
              ├─────────────┬──────────────┬──────────────┐
              │             │              │              │
              ▼             ▼              ▼              ▼
         Console       File Logger    Syslog        Cloud Logging
         (stderr)    (if configured) (if enabled)  (if integrated)
```

```
┌──────────────────────────────┐
│ 3b. Error Buffer Tracking    │
│    - record_error_message()  │
│    - ErrorMessageBuffer      │
│    - Last 10 errors + counts │
└─────────────┬────────────────┘
              │
              ▼
┌──────────────────────────────┐
│ 4. Metrics Recording         │
│    - sync_error_metrics()    │
│    - Updates Prometheus      │
│      gauges                  │
└─────────────┬────────────────┘
              │
              ▼
┌──────────────────────────────┐
│ 5. Prometheus Scrape         │
│    - /metrics endpoint       │
│    - Exports Prometheus text │
│    - Format: EXPOSITION      │
└─────────────┬────────────────┘
              │
              ▼
┌──────────────────────────────┐
│ 6. Monitoring Systems        │
│    - Prometheus DB           │
│    - Grafana dashboards      │
│    - Alerting rules          │
│    - Error tracking panel    │
└──────────────────────────────┘
```

### 2.2 Error Entry Point Example (from stream_job_server.rs)

**Scenario**: Job deployment fails

```rust
// Line 741-746
match processor.process_multi_job(...).await {
    Ok(stats) => {
        info!("Job '{}' completed successfully (transactional): {:?}", 
              job_name, stats);
    }
    Err(e) => {
        error!("Job '{}' failed (transactional): {:?}", job_name, e);
        // Error is logged here via log::error!()
    }
}
```

**Flow**:
1. `error!()` macro logs to standard logging system
2. If observability manager exists, error also recorded via:
   ```rust
   ErrorTracker::record_error(
       &observability_for_spawn,
       &job_name,
       error_message
   )
   ```
3. Error message added to `ErrorMessageBuffer` in `MetricsProvider`
4. Available for Prometheus export on next scrape

---

## 3. ERROR HANDLING IN JOB SERVER

### 3.1 Error Recording Helper (error_tracking_helper.rs)

**Location**: `src/velostream/server/processors/error_tracking_helper.rs`

```rust
pub struct ErrorTracker;

impl ErrorTracker {
    /// Record an error message to the metrics system asynchronously
    pub fn record_error(
        observability: &Option<SharedObservabilityManager>,
        job_name: &str,
        error_message: String,
    ) {
        if let Some(ref obs_manager) = observability {
            let manager = obs_manager.clone();
            let prefixed_message = format!("[{}] {}", job_name, error_message);
            tokio::spawn(async move {
                let manager_read = manager.read().await;
                if let Some(metrics) = manager_read.metrics() {
                    metrics.record_error_message(prefixed_message);
                }
            });
        }
    }
}
```

**Key Points**:
- Spawns background task to avoid blocking processor
- Prefixes error with job name `[job_name] error message`
- Records to MetricsProvider's error buffer
- Errors visible in Grafana's Error Tracking dashboard

### 3.2 Integration with Processors

**Simple Processor** (`src/velostream/server/processors/simple.rs`):
- Records errors via `ErrorTracker::record_error()`
- Logs errors with `error!()` macro
- Continues processing (fail-safe design)

**Transactional Processor** (`src/velostream/server/processors/transactional.rs`):
- Records errors before rolling back
- Ensures error visibility even on transaction rollback
- Similar error tracking mechanism

---

## 4. OBSERVABILITY MANAGER INTEGRATION

### 4.1 Initialization in Stream Job Server

**Location**: Lines 134-173, 175-239

```rust
// Phase 4: Initialize shared observability manager
let observability = if enable_monitoring {
    use crate::velostream::observability::ObservabilityManager;
    
    let streaming_config = StreamingConfig::default()
        .with_prometheus_metrics();
    
    let mut obs_manager = ObservabilityManager::from_streaming_config(
        streaming_config
    );
    
    match obs_manager.initialize().await {
        Ok(()) => {
            info!("✅ Server-level observability initialized");
            Some(Arc::new(RwLock::new(obs_manager)))
        }
        Err(e) => {
            warn!("⚠️ Failed to initialize observability: {}", e);
            None
        }
    }
} else {
    None
};
```

### 4.2 Deployment Context Setting

**Location**: Lines 554-573

```rust
// Initialize deployment context for error tracking
if let Some(ref obs_manager) = observability_manager {
    let deployment_ctx = Self::build_deployment_context(&name, &version);
    if let Ok(mut obs_lock) = obs_manager.try_write() {
        match obs_lock.set_deployment_context_for_job(deployment_ctx.clone()) {
            Ok(()) => {
                info!("Job '{}': Deployment context initialized 
                      (node_id={:?}, region={:?}, version={})",
                      name, deployment_ctx.node_id, 
                      deployment_ctx.region,
                      deployment_ctx.version.as_deref()
                      .unwrap_or("unknown"));
            }
            Err(e) => {
                warn!("Job '{}': Failed to set deployment context: {}", 
                      name, e);
            }
        }
    }
}
```

**Context Sources** (Lines 1628-1656):
```rust
fn build_deployment_context(job_name: &str, version: &str) 
    -> DeploymentContext 
{
    // Priority order:
    let node_id = env::var("NODE_ID")
        .or_else(|| env::var("HOSTNAME"))
        .or_else(|| env::var("POD_NAME"));
    
    let node_name = env::var("NODE_NAME")
        .or_else(|| env::var("SERVICE_NAME"));
    
    let region = env::var("AWS_REGION")
        .or_else(|| env::var("REGION"))
        .or_else(|| env::var("DEPLOYMENT_REGION"));
    
    let app_version = if version.is_empty() {
        env::var("APP_VERSION").ok()
    } else {
        Some(version.to_string())
    };
    
    DeploymentContext {
        node_id, node_name, region, version: app_version
    }
}
```

---

## 5. PROMETHEUS METRICS ENDPOINTS

### 5.1 Available Metrics

**Error Tracking Metrics** (exposed at `/metrics` endpoint):

```prometheus
# Total errors recorded (cumulative)
velo_error_messages_total 42

# Unique error message types
velo_unique_error_types 3

# Messages in current rolling buffer
velo_buffered_error_messages 5

# Individual error messages (with message label)
velo_error_message{message="Connection timeout"} 15
velo_error_message{message="Query execution failed"} 18
velo_error_message{message="Memory limit exceeded"} 9
```

### 5.2 Metrics Sync Mechanism

**Call Pattern**:
```rust
// In metrics scrape handler
pub async fn metrics_handler(
    metrics: &MetricsProvider
) -> Result<String, Error> {
    // Update Prometheus gauges from error buffer
    metrics.sync_error_metrics();
    
    // Return metrics text
    metrics.get_metrics_text()
}
```

**What sync_error_metrics() does** (Lines 926-949):
```rust
pub fn sync_error_metrics(&self) {
    if self.active {
        let total_errors = self.get_total_errors();
        let unique_types = self.get_unique_error_types();
        let buffered_count = self.get_buffered_error_count();
        
        // Update Prometheus gauges
        self.sql_metrics.update_error_gauges(
            total_errors,
            unique_types,
            buffered_count
        );
        
        // Update individual error message metrics
        if let Ok(tracker) = self.error_tracker.lock() {
            let message_counts = tracker.get_message_counts();
            self.sql_metrics.update_error_message_metrics(
                &message_counts
            );
        }
    }
}
```

---

## 6. GRAFANA DASHBOARD INTEGRATION

### 6.1 Dashboard Location

```
demo/trading/monitoring/grafana/dashboards/velostream-error-tracking.json
```

### 6.2 Dashboard Panels

**Panel 1: Total Error Count (Stat)**
- Metric: `velo_error_messages_total`
- Shows: Cumulative error count
- Thresholds: Green (0-4), Yellow (5-9), Orange (10-19), Red (20+)

**Panel 2: Unique Error Types (Stat)**
- Metric: `velo_unique_error_types`
- Shows: How many different error types
- Indicates: System stability (high diversity = unstable)

**Panel 3: Buffered Errors (Stat)**
- Metric: `velo_buffered_error_messages`
- Shows: Recent error activity (0-10 scale)
- Red when full: Indicates active error generation

**Panel 4: Error Trends (Time Series)**
- Metrics: `velo_error_messages_total`, `velo_unique_error_types`
- Shows: Error trends over time
- Helps: Detect sudden spikes or patterns

**Panel 5: Error Message Details (Table)**
- Source: Error message buffer (custom query)
- Shows: Last error messages with counts
- Useful: Quick diagnosis of current issues

---

## 7. FILE LOGGING INVESTIGATION

### 7.1 velo_deployment.log - NOT Written by Velostream

**Finding**: The file `velo_deployment.log` is **NOT** created or written to by the Velostream codebase.

**Evidence**:
- No occurrences of `velo_deployment.log` in source code
- No file write operations targeting deployment logs
- No logger configuration writing to `.log` files
- Logging uses Rust `log` crate (facade pattern)

### 7.2 Where velo_deployment.log Comes From

**Possible Sources**:

1. **External Logger Configuration**
   - If `env_logger` or `tracing_subscriber` is configured in `main.rs`/binary
   - Could write to files based on environment variable
   - Not visible in library code

2. **Docker/Container Logging**
   - Container runtime might create logs
   - Not application code responsibility

3. **System Administrator Setup**
   - Operational setup for production deployments
   - Log rotation, centralized logging, etc.

4. **Testing/Demo Infrastructure**
   - `demo/trading/` scripts might create logs
   - CI/CD pipeline configuration

### 7.3 Actual Error Output Destinations

**Current Architecture**:
1. Rust `log` crate → Logger implementation (env_logger, tracing, etc.)
2. Logger output → stdout/stderr (default)
3. Container runtime → Docker logs or systemd journal
4. Logs → Centralized logging system (ELK, Datadog, etc.)

**Not**:
- Direct file writing to `velo_deployment.log`
- Application doesn't manage log file rotation
- No log file path configuration in codebase

---

## 8. RECOMMENDED ERROR MONITORING PATTERNS

### 8.1 Check Recent Errors Programmatically

```rust
// Get error statistics
if let Some(stats) = metrics_provider.get_error_stats() {
    println!("Total errors: {}", stats.total_errors);
    println!("Unique types: {}", stats.unique_errors);
    
    // Get top 5 most common errors
    for (msg, count) in stats.get_top_errors(5) {
        println!("{}: {} occurrences", msg, count);
    }
}

// Get all buffered error messages
let errors = metrics_provider.get_error_messages();
for entry in errors {
    println!("[{}] {} (count: {})",
             entry.timestamp,
             entry.message,
             entry.count);
}
```

### 8.2 Query Prometheus Directly

```bash
# Get current error count
curl 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=velo_error_messages_total'

# Get unique error types over last 5 minutes
curl 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=rate(velo_unique_error_types[5m])'

# Get errors by type
curl 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=velo_error_message'
```

### 8.3 Set Up Grafana Alerting

```yaml
# Alert: High error rate
alert_rule: high_error_rate
  condition: velo_error_messages_total > 50
  duration: 5m
  action: notify_ops_team

# Alert: Many error types
alert_rule: system_instability
  condition: velo_unique_error_types > 5
  duration: 5m
  action: page_on_call_engineer

# Alert: Buffer full (active errors)
alert_rule: active_errors
  condition: velo_buffered_error_messages >= 10
  duration: 1m
  action: investigate_immediately
```

---

## 9. ERROR TRACKING API REFERENCE

### 9.1 ErrorMessageBuffer Methods

```rust
// Create buffer
let mut buffer = ErrorMessageBuffer::new();

// Add error (with deduplication)
buffer.add_error("Connection timeout".to_string());

// Get statistics
let stats = buffer.get_stats();
// Returns: ErrorStats {
//   total_errors: u64,
//   unique_errors: usize,
//   buffered_messages: usize,
//   message_counts: HashMap<String, u64>
// }

// Get all messages
let messages = buffer.get_messages();

// Get specific message count
let count = buffer.get_message_count("Connection timeout");

// Get top N errors
let top = stats.get_top_errors(5);

// Check if full
if buffer.is_full() { /* 10 messages */ }

// Reset all
buffer.reset();
```

### 9.2 MetricsProvider Error Methods

```rust
// Record error
metrics_provider.record_error_message("Error text".to_string());

// Get statistics
if let Some(stats) = metrics_provider.get_error_stats() {
    println!("Total: {}, Unique: {}", 
             stats.total_errors, stats.unique_errors);
}

// Get top errors
let top = metrics_provider.get_top_errors(5);

// Get messages
let msgs = metrics_provider.get_error_messages();

// Sync to Prometheus (IMPORTANT)
metrics_provider.sync_error_metrics();

// Individual counters
let total = metrics_provider.get_total_errors();
let unique = metrics_provider.get_unique_error_types();
let buffered = metrics_provider.get_buffered_error_count();

// Reset all
metrics_provider.reset_error_tracking();
```

### 9.3 ErrorTracker Helper

```rust
// Record error with job context
ErrorTracker::record_error(
    &observability_manager,
    "job_name",
    "Error message".to_string()
);
// Results in: "[job_name] Error message" in metrics
```

---

## 10. SUMMARY TABLE

| Aspect | Details |
|--------|---------|
| **Primary Logging** | Rust `log` crate macros (error!, warn!, info!, debug!) |
| **Error Buffer** | Last 10 messages with counts (ErrorMessageBuffer) |
| **Metrics Exposure** | Prometheus metrics via MetricsProvider |
| **Metrics Endpoint** | `/metrics` (text/plain Prometheus format) |
| **File Logging** | Not in Velostream code; configured externally |
| **velo_deployment.log** | Not written by Velostream application |
| **Error Entry Points** | stream_job_server, processors, error_tracking_helper |
| **Deployment Context** | Environment variables (NODE_ID, REGION, etc.) |
| **Dashboard** | Grafana (demo/trading/monitoring/grafana/) |
| **Metrics Sync** | Must call sync_error_metrics() before scrape |
| **Memory Bounded** | Buffer max 10 entries (~5-50 KB) |
| **Thread-Safe** | Arc<Mutex<>> throughout |
| **Performance Impact** | Negligible (O(1) operations) |

---

## 11. KEY FILES REFERENCE

| File | Purpose |
|------|---------|
| `/src/velostream/observability/error_tracker.rs` | ErrorMessageBuffer, ErrorEntry, DeploymentContext |
| `/src/velostream/observability/metrics.rs` | MetricsProvider, Prometheus metrics integration |
| `/src/velostream/server/processors/error_tracking_helper.rs` | ErrorTracker helper for recording errors |
| `/src/velostream/server/stream_job_server.rs` | Job deployment, error logging, observability setup |
| `/docs/observability/error-tracking-integration.md` | Comprehensive error tracking documentation |
| `demo/trading/monitoring/grafana/` | Grafana dashboard configuration |

---

## Conclusion

Velostream's error handling follows a sophisticated multi-layered architecture:

1. **Standard Logging**: Via Rust `log` crate throughout the codebase
2. **Error Buffer**: In-memory rolling buffer of last 10 errors with counts
3. **Metrics**: Prometheus metrics for error tracking (total, unique, buffered)
4. **Observability**: Integration with monitoring dashboards (Grafana)
5. **Deployment Context**: Rich metadata (node_id, region, version) attached to errors

The system is **NOT** designed to write to `velo_deployment.log` file. Instead, errors are:
- Logged via standard logging (configurable destination)
- Tracked in memory for real-time visibility
- Exposed via Prometheus for monitoring
- Visualized in Grafana dashboards

For production deployments, errors are monitored through:
- Prometheus metrics queries
- Grafana error tracking dashboard
- Alerting rules based on error thresholds
- Log aggregation (external systems)
