# Dead Letter Queue & Metrics Configuration Guide

## Overview

This guide covers the Dead Letter Queue (DLQ) implementation in Velostream, including current capabilities and operational patterns for managing failed records, monitoring health, and implementing recovery strategies.

## Table of Contents

1. [Current DLQ Implementation](#current-dlq-implementation)
2. [Configuration](#configuration)
3. [Operational Patterns](#operational-patterns)
4. [Monitoring and Health](#monitoring-and-health)
5. [Recovery Strategies](#recovery-strategies)
6. [Examples](#examples)
7. [Future Enhancements](#future-enhancements)

---

## Current DLQ Implementation

### What is the Dead Letter Queue?

The Dead Letter Queue (DLQ) is an in-memory collection of failed records that preserves:
- The original record data
- Complete error context (message, type, source)
- Recoverable flag (indicates if retry is possible)
- Timestamp (for investigation)

### Key Features

**Size Management**
- Optional capacity limits prevent unbounded memory growth
- Atomic operations for lock-free capacity checks
- Capacity tracking with percentage metrics

**Thread-Safe Operations**
- All DLQ operations are async-safe via Tokio
- Mutex-protected entry storage with cloning for retrieval
- Atomic flags for fast capacity checks

**Processor Integration**
```
SimpleJobProcessor      ✅ DLQ Enabled (LogAndContinue)
PartitionReceiver       ✅ DLQ Enabled (per-partition)
TransactionalJobProcessor ❌ DLQ Disabled (FailBatch strategy)
```

**Error Context Preservation**
Each DLQ entry contains:
```rust
pub struct DLQEntry {
    pub record: StreamRecord,           // Original data
    pub error_message: String,          // Error description
    pub record_index: usize,            // Position in batch
    pub recoverable: bool,              // Can be retried
    pub timestamp: Instant,             // When error occurred
}
```

### When Records Enter DLQ

**LogAndContinue Strategy** (Default)
- Individual record processing fails
- Record added to DLQ
- Processing continues with next record

**SendToDLQ Strategy**
- Batch processing fails
- Failed records sent to DLQ
- Batch continues

**FailBatch Strategy** (Transactional)
- Any record fails → entire batch fails
- DLQ disabled (batch is rolled back)
- No DLQ entry created

**RetryWithBackoff Strategy**
- Record fails initial processing
- Automatically retried with backoff
- If max retries exceeded, added to DLQ

---

## Configuration

### Enabling/Disabling DLQ

Via `JobProcessingConfig`:

```rust
use velostream::velostream::server::processors::{JobProcessingConfig, FailureStrategy};
use std::time::Duration;

// Enable DLQ with unlimited size (default)
let config = JobProcessingConfig {
    enable_dlq: true,
    dlq_max_size: None,
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};

// Enable with size limit
let config = JobProcessingConfig {
    enable_dlq: true,
    dlq_max_size: Some(10_000),  // Max 10K entries
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};

// Disable DLQ
let config = JobProcessingConfig {
    enable_dlq: false,
    dlq_max_size: None,
    ..Default::default()
};
```

### Size Limit Guidelines

**Development**
```rust
dlq_max_size: Some(100)  // Small for quick testing
```

**Standard Production**
```rust
dlq_max_size: Some(10_000)  // 10K entries (~15-50 MB depending on record size)
```

**High-Volume Systems**
```rust
dlq_max_size: Some(100_000)  // 100K entries for very high throughput
```

**Unlimited (Not Recommended)**
```rust
dlq_max_size: None  // Can grow unbounded
```

### Processor-Specific Configuration

**SimpleJobProcessor**
```rust
let config = JobProcessingConfig {
    enable_dlq: true,  // Default: enabled
    dlq_max_size: Some(10_000),
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};
```

**PartitionReceiver**
```rust
let config = JobProcessingConfig {
    enable_dlq: true,  // Default: enabled
    dlq_max_size: Some(2_000),  // Smaller per-partition limit
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};
```

**TransactionalJobProcessor**
```rust
let config = JobProcessingConfig {
    enable_dlq: false,  // Default: disabled (by design)
    dlq_max_size: None,
    failure_strategy: FailureStrategy::FailBatch,
    use_transactions: true,
    ..Default::default()
};
```

---

## Operational Patterns

### Accessing DLQ

```rust
use velostream::velostream::server::processors::ObservabilityWrapper;

let wrapper = ObservabilityWrapper::with_dlq();

// Check if enabled
if wrapper.has_dlq() {
    println!("DLQ is enabled");
}

// Get DLQ reference
if let Some(dlq) = wrapper.dlq() {
    // Perform operations
}
```

### Common Operations

**Add Entry**
```rust
if let Some(dlq) = wrapper.dlq() {
    let added = dlq.add_entry(
        record,
        "SQL execution error".to_string(),
        record_index,
        true  // recoverable
    ).await;

    if !added {
        log::error!("DLQ at capacity, entry rejected");
    }
}
```

**Get Entries**
```rust
if let Some(dlq) = wrapper.dlq() {
    let entries = dlq.get_entries().await;
    for entry in entries {
        println!("Failed record {}: {}",
                 entry.record_index,
                 entry.error_message);
    }
}
```

**Check Capacity**
```rust
if let Some(dlq) = wrapper.dlq() {
    let size = dlq.current_size();
    let usage = dlq.capacity_usage_percent();
    let at_capacity = dlq.is_at_capacity();

    if at_capacity {
        log::warn!("DLQ at maximum capacity!");
    }
}
```

**Clear DLQ**
```rust
if let Some(dlq) = wrapper.dlq() {
    dlq.clear().await;
    dlq.reset_max_size_flag();
}
```

**Print Entries**
```rust
if let Some(dlq) = wrapper.dlq() {
    dlq.print_entries().await;
}
```

---

## Monitoring and Health

### Health Metrics

```rust
use velostream::velostream::server::metrics::{
    DLQMetrics, DLQHealthSummary, summarize_dlq_health
};

let dlq_metrics = DLQMetrics::new();

// Record operations
dlq_metrics.record_entry_added();
dlq_metrics.record_entry_rejected();

// Get health summary
let health = summarize_dlq_health(&dlq, &dlq_metrics);

// Check health
if health.is_healthy() {
    println!("DLQ Status: {}", health.status_str());
} else {
    log::warn!("DLQ Status: {}", health.status_str());
}
```

### Health Status Levels

| Status | Capacity | Action |
|--------|----------|--------|
| HEALTHY | < 70% | Normal operation |
| CAUTION | 70-90% | Monitor closely, plan cleanup |
| WARNING | > 90% | Start recovery soon |
| CRITICAL | 100% | **Stop processing, implement recovery** |

### Available Metrics

```rust
let health = summarize_dlq_health(&dlq, &metrics);

// Current state
println!("Current Size: {}", health.current_size);
println!("Max Size: {:?}", health.max_size);
println!("Capacity: {:.1}%", health.capacity_usage_percent.unwrap_or(0.0));
println!("At Capacity: {}", health.is_at_capacity);

// History
println!("Entries Added: {}", health.entries_added);
println!("Entries Rejected: {}", health.entries_rejected);
println!("Last Entry: {:?}", health.last_entry_time);
println!("Last Capacity Hit: {:?}", health.last_capacity_exceeded_time);
```

### Monitoring Implementation

```rust
// Periodic health check (e.g., every 5 minutes)
tokio::spawn({
    let dlq = dlq.clone();
    let metrics = metrics.clone();
    async move {
        loop {
            let health = summarize_dlq_health(&dlq, &metrics);
            if !health.is_healthy() {
                log::warn!("DLQ Health: {} ({:.1}%)",
                          health.status_str(),
                          health.capacity_usage_percent.unwrap_or(0.0));
            }
            tokio::time::sleep(Duration::from_secs(300)).await;
        }
    }
});
```

---

## Recovery Strategies

### Strategy 1: In-Process Batch Recovery

```rust
async fn recover_from_dlq(
    dlq: &DeadLetterQueue,
    processor: &SimpleJobProcessor,
) -> Result<()> {
    let entries = dlq.get_entries().await;
    let mut recovered = 0;
    let mut still_failing = 0;

    for entry in entries {
        match processor.process_record(entry.record.clone()).await {
            Ok(_) => recovered += 1,
            Err(_) => still_failing += 1,
        }
    }

    println!("Recovery: {} recovered, {} still failing",
             recovered, still_failing);

    // Clear DLQ after processing
    dlq.clear().await;
    dlq.reset_max_size_flag();

    Ok(())
}
```

### Strategy 2: Export for External Processing

```rust
use std::fs::File;
use std::io::Write;

async fn export_dlq_to_file(
    dlq: &DeadLetterQueue,
    path: &str,
) -> Result<()> {
    let entries = dlq.get_entries().await;
    let mut file = File::create(path)?;

    writeln!(file, "record_index,error_message,recoverable")?;
    for entry in entries {
        writeln!(
            file,
            "{},\"{}\",{}",
            entry.record_index,
            entry.error_message.replace("\"", "\"\""),
            entry.recoverable
        )?;
    }

    Ok(())
}
```

### Strategy 3: Capacity Alert & Recovery

```rust
async fn check_dlq_capacity(
    dlq: &DeadLetterQueue,
    metrics: &DLQMetrics,
) {
    let health = summarize_dlq_health(dlq, metrics);

    if health.is_at_capacity {
        log::error!("🚨 CRITICAL: DLQ at maximum capacity!");
        log::error!("Current: {}/{}",
                  health.current_size,
                  health.max_size.unwrap_or(0));
        // Trigger incident response
    } else if let Some(percent) = health.capacity_usage_percent {
        if percent > 80.0 {
            log::warn!("⚠️  DLQ {:.1}% full", percent);
        }
    }

    if health.entries_rejected > 0 {
        log::warn!("⚠️  {} entries rejected from DLQ",
                  health.entries_rejected);
    }
}
```

---

## Examples

### Example 1: Basic DLQ Setup

```rust
use velostream::velostream::server::processors::{
    JobProcessingConfig, SimpleJobProcessor, FailureStrategy
};

let config = JobProcessingConfig {
    enable_dlq: true,
    dlq_max_size: Some(10_000),
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};

// Processor automatically uses DLQ
let processor = SimpleJobProcessor::new(query, config, ...)?;
```

### Example 2: High-Volume with Monitoring

```rust
let config = JobProcessingConfig {
    max_batch_size: 10_000,
    enable_dlq: true,
    dlq_max_size: Some(50_000),
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};

let dlq_metrics = DLQMetrics::new();

// Background monitoring
tokio::spawn({
    let dlq = dlq.clone();
    let metrics = dlq_metrics.clone();
    async move {
        loop {
            let health = summarize_dlq_health(&dlq, &metrics);
            if !health.is_healthy() {
                log::warn!("DLQ: {}", health.status_str());
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
});
```

### Example 3: Per-Partition Configuration

```rust
fn create_partition_config(partition_id: usize) -> JobProcessingConfig {
    JobProcessingConfig {
        max_batch_size: 500,
        enable_dlq: true,
        dlq_max_size: Some(2_000),  // Smaller per partition
        failure_strategy: FailureStrategy::LogAndContinue,
        ..Default::default()
    }
}
```

---

## Future Enhancements

The following features are planned for future releases:

### 1. Persistent DLQ Storage

```rust
// Future API (not implemented)
let dlq = DeadLetterQueue::builder()
    .storage(StorageType::Kafka("dlq-topic"))
    .max_size(1_000_000)
    .retention_days(30)
    .build()?;
```

### 2. Error Categorization

```rust
// Future API (not implemented)
pub enum ErrorCategory {
    Transient,      // Network issues, timeouts
    Parsing,        // Data format issues
    Validation,     // Business rule violations
    Permanent,      // Unrecoverable errors
}
```

### 3. Automatic Retry with Backoff

```rust
// Current: Manual retry
// Future: Automatic with exponential backoff
let dlq = DeadLetterQueue::builder()
    .retry_strategy(RetryStrategy::ExponentialBackoff {
        max_attempts: 5,
        initial_delay: Duration::from_millis(1000),
        max_delay: Duration::from_hours(1),
        multiplier: 2.0,
    })
    .build()?;
```

### 4. Metrics Integration

```rust
// Future: Prometheus/Grafana metrics export
pub struct DLQMetricsCollector {
    entries_added: Counter,
    entries_rejected: Counter,
    queue_size: Gauge,
    capacity_percent: Gauge,
}
```

---

## Best Practices

1. **Size Limits**: Set appropriate limits based on memory availability
2. **Monitoring**: Regular health checks prevent surprises
3. **Recovery**: Implement recovery processes, don't let DLQ grow unbounded
4. **Cleanup**: Periodically clear processed entries
5. **Alerting**: Alert at 80% capacity, before critical 100%
6. **Logging**: Comprehensive logging of all DLQ operations
7. **Testing**: Test recovery paths in integration tests
8. **Documentation**: Document DLQ behavior for your specific pipelines

---

## See Also

- [ObservabilityWrapper Documentation](../../../src/velostream/server/processors/observability_wrapper.rs) - API reference
- [DeadLetterQueue Implementation](../../../src/velostream/server/processors/common.rs) - Core DLQ implementation
- [DLQ Metrics](../../../src/velostream/server/metrics.rs) - Health monitoring and metrics
- [Job Processor Architecture](./multi-source-sink-guide.md) - Processor design
- [Simple Job Processor](../../../src/velostream/server/processors/simple_job_processor.rs) - Non-transactional processor with DLQ support

