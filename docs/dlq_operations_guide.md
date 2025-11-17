# Dead Letter Queue (DLQ) Operations Guide

## Overview

The Dead Letter Queue (DLQ) is a critical component of Velostream's error handling and recovery system. It collects failed records for inspection, debugging, and recovery, enabling non-destructive error handling in streaming data pipelines.

## Concepts

### What is a DLQ?

A Dead Letter Queue is a designated location where failed records are stored after processing fails. Unlike traditional error handling that logs and discards failed records, a DLQ preserves the actual record data along with comprehensive error context, allowing operators to:

- **Inspect** what went wrong
- **Debug** data quality issues
- **Recover** from failures through re-processing
- **Monitor** error patterns
- **Audit** failures for compliance

### When Records Enter DLQ

Records are added to the DLQ in specific scenarios depending on the failure strategy:

**LogAndContinue Strategy** (Default for SimpleJobProcessor and PartitionReceiver):
- Individual record processing fails (SQL error, type mismatch, etc.)
- Record is added to DLQ
- Processing continues with next record

**SendToDLQ Strategy**:
- Batch processing fails
- Failed records are sent to DLQ
- Batch continues with next batch

**FailBatch Strategy** (TransactionalJobProcessor):
- Any record fails, entire batch fails
- **DLQ is disabled** - batch is rolled back
- No DLQ entry created (by design)

**RetryWithBackoff Strategy**:
- Record fails initial processing
- Automatically retried with exponential backoff
- If max retries exceeded, record sent to DLQ

### Failure Types

Records reach DLQ for various failure types:

1. **SQL Execution Errors**: Query failed during execution
2. **Type Conversion Errors**: Field value cannot be converted to target type
3. **Schema Validation Errors**: Record doesn't match expected schema
4. **Aggregation Errors**: Window/aggregation computation failed
5. **Serialization Errors**: Failed to serialize record for output
6. **Resource Errors**: Connection lost, timeout occurred
7. **Recoverable Errors**: Errors that may succeed on retry

## Configuration

### Enabling/Disabling DLQ

```rust
use velostream::velostream::server::processors::{JobProcessingConfig, FailureStrategy};
use std::time::Duration;

// Enable DLQ (default for most strategies)
let config = JobProcessingConfig {
    enable_dlq: true,  // Default: true
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};

// Disable DLQ
let config = JobProcessingConfig {
    enable_dlq: false,
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};
```

### Setting DLQ Size Limits

```rust
// Unlimited DLQ (default)
let config = JobProcessingConfig {
    dlq_max_size: None,  // No limit
    ..Default::default()
};

// Limited DLQ to 10,000 entries
let config = JobProcessingConfig {
    dlq_max_size: Some(10_000),  // 10k entry limit
    ..Default::default()
};
```

### Creating DLQ with Capacity

```rust
use velostream::velostream::server::processors::DeadLetterQueue;

// Unlimited DLQ
let dlq = DeadLetterQueue::new();

// Limited DLQ to 5,000 entries
let dlq = DeadLetterQueue::with_max_size(5_000);
```

## Operations

### Monitoring DLQ Health

```rust
use velostream::velostream::server::metrics::{DLQMetrics, summarize_dlq_health};

let dlq = DeadLetterQueue::with_max_size(1_000);
let metrics = DLQMetrics::new();

// Get health summary
let health = summarize_dlq_health(&dlq, &metrics);

// Check status
if health.is_healthy() {
    println!("DLQ Status: {}", health.status_str());  // "HEALTHY"
} else {
    eprintln!("DLQ Status: {}", health.status_str());  // "WARNING - >90% full"
}

// Get detailed metrics
println!("Current Size: {}", health.current_size);
println!("Max Size: {:?}", health.max_size);
println!("Capacity: {:.1}%", health.capacity_usage_percent.unwrap_or(0.0));
println!("Entries Added: {}", health.entries_added);
println!("Entries Rejected: {}", health.entries_rejected);
```

### Checking DLQ Capacity

```rust
// Get current size
let current = dlq.current_size();  // Fast, lock-free

// Get max size
if let Some(max) = dlq.max_size() {
    println!("Max capacity: {} entries", max);
}

// Get capacity percentage
if let Some(percent) = dlq.capacity_usage_percent() {
    println!("DLQ is {:.1}% full", percent);
}

// Check if at capacity
if dlq.is_at_capacity() {
    eprintln!("DLQ has reached maximum capacity!");
}
```

### Accessing DLQ Entries

```rust
// Get all entries (blocking operation)
let entries = dlq.get_entries().await;
for entry in entries {
    println!("Failed Record:");
    println!("  Index: {}", entry.record_index);
    println!("  Error: {}", entry.error_message);
    println!("  Recoverable: {}", entry.recoverable);
    println!("  Timestamp: {:?}", entry.timestamp);
}

// Get entry count
let count = dlq.len().await;
println!("DLQ contains {} failed records", count);

// Check if empty
if dlq.is_empty().await {
    println!("DLQ is empty - no failures");
}
```

### Clearing DLQ

```rust
// Clear all entries and reset counters
dlq.clear().await;

// Reset capacity flag (after clearing)
dlq.reset_max_size_flag();

// Verify
assert!(dlq.is_empty().await);
assert!(!dlq.is_at_capacity());
```

### Printing DLQ Contents

```rust
// Pretty-print all DLQ entries
dlq.print_entries().await;

// Output example:
// ╔════════════════════════════════════════════════════════╗
// ║ DEAD LETTER QUEUE - 5 failed records               ║
// ╚════════════════════════════════════════════════════════╝
//
// DLQ Entry 1:
//   Record Index:     0
//   Error Message:    SQL execution error: Column 'price' not found
//   Recoverable:      true
//   Record Fields:    ["id", "symbol", "timestamp"]
//
// DLQ Entry 2:
//   Record Index:     5
//   Error Message:    Type conversion failed for field 'quantity'
//   Recoverable:      false
//   Record Fields:    ["id", "symbol", "price", "quantity"]
```

## Metrics and Monitoring

### Available Metrics

```rust
use velostream::velostream::server::metrics::{DLQMetrics, JobMetrics};

let metrics = DLQMetrics::new();
let job_metrics = JobMetrics::new();

// DLQ Metrics
metrics.record_entry_added();       // Record successful DLQ entry
metrics.record_entry_rejected();    // Record capacity rejection

println!("Entries Added: {}", metrics.entries_added());
println!("Entries Rejected: {}", metrics.entries_rejected());
println!("Last Entry Time: {:?}", metrics.last_entry_time());
println!("Last Capacity Hit: {:?}", metrics.last_capacity_exceeded_time());

// Job Metrics
job_metrics.record_processed(100);
job_metrics.record_failed(5);

println!("Records Processed: {}", job_metrics.records_processed());
println!("Records Failed: {}", job_metrics.records_failed());
println!("Failure Rate: {:.2}%", job_metrics.failure_rate_percent());

// DLQ Health
println!("DLQ Status: {}", health.status_str());
// Possible values:
// - "HEALTHY"             : < 70% capacity
// - "CAUTION - >70% full" : 70-90% capacity
// - "WARNING - >90% full" : > 90% capacity
// - "CRITICAL - at capacity" : at maximum capacity
```

### Health Status Interpretation

| Status | Capacity | Action |
|--------|----------|--------|
| HEALTHY | < 70% | Normal operation |
| CAUTION | 70-90% | Monitor closely, plan cleanup |
| WARNING | > 90% | Start recovery process soon |
| CRITICAL | 100% | **Stop processing**, implement recovery immediately |

## Recovery Strategies

### Strategy 1: Process DLQ Records in Batch

After DLQ fills with failed records, batch re-process them:

```rust
async fn recover_from_dlq(
    dlq: &DeadLetterQueue,
    processor: &SimpleJobProcessor,
) -> Result<()> {
    let entries = dlq.get_entries().await;
    let recovered_count = 0;
    let still_failing_count = 0;

    for entry in entries {
        match processor.process_record(entry.record.clone()).await {
            Ok(_) => {
                // Record now processes successfully!
                recovered_count += 1;
            }
            Err(_) => {
                // Still failing, keep for investigation
                still_failing_count += 1;
            }
        }
    }

    println!("Recovery: {} recovered, {} still failing",
             recovered_count, still_failing_count);

    // Clear DLQ after processing
    dlq.clear().await;
    dlq.reset_max_size_flag();

    Ok(())
}
```

### Strategy 2: Export DLQ for External Processing

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

### Strategy 3: Alert on Capacity

```rust
use velostream::velostream::server::metrics::summarize_dlq_health;

async fn check_dlq_alerts(
    dlq: &DeadLetterQueue,
    metrics: &DLQMetrics,
    alert_threshold: Option<usize>,
) {
    let health = summarize_dlq_health(dlq, metrics);

    // Alert on capacity
    if health.is_at_capacity {
        eprintln!("🚨 ALERT: DLQ at maximum capacity!");
        eprintln!("   Current: {}/{}",
                  health.current_size,
                  health.max_size.unwrap_or(0));
        // Trigger incident response
    }

    // Alert on high percentage
    if let Some(percent) = health.capacity_usage_percent {
        if percent > 80.0 {
            eprintln!("⚠️  WARNING: DLQ {:.1}% full", percent);
        }
    }

    // Alert if rejection happening
    if health.entries_rejected > 0 {
        eprintln!("⚠️  WARNING: {} entries rejected from DLQ",
                  health.entries_rejected);
    }
}
```

## Best Practices

### 1. Set Appropriate Size Limits

```rust
// Small limit for strict failure tracking
let small_dlq = DeadLetterQueue::with_max_size(100);

// Large limit for high-volume streaming
let large_dlq = DeadLetterQueue::with_max_size(100_000);

// Unlimited for non-critical pipelines
let unlimited_dlq = DeadLetterQueue::new();
```

### 2. Monitor DLQ Regularly

```rust
// Periodic health check (e.g., every 5 minutes)
tokio::spawn(async {
    loop {
        let health = summarize_dlq_health(&dlq, &metrics);
        if !health.is_healthy() {
            log::warn!("DLQ health degraded: {}", health.status_str());
        }
        tokio::time::sleep(Duration::from_secs(300)).await;
    }
});
```

### 3. Implement Capacity Handling

```rust
// Before adding entry, check capacity
if dlq.is_at_capacity() {
    log::error!("DLQ at capacity, cannot accept more failures");
    // Option 1: Reject record processing
    // Option 2: Clear old entries
    // Option 3: Export and reset DLQ
} else {
    // Safe to add entry
    let added = dlq.add_entry(
        record, error_msg, index, recoverable
    ).await;

    if added {
        metrics.record_entry_added();
    } else {
        metrics.record_entry_rejected();
    }
}
```

### 4. Log DLQ Activities

```rust
// In SimpleJobProcessor or PartitionReceiver
if let Some(dlq) = self.observability_wrapper.dlq() {
    match dlq.add_entry(...).await {
        true => {
            debug!("DLQ Entry Added - Record {}: {}",
                   record_index, error_message);
        }
        false => {
            error!("DLQ Entry Rejected - At Capacity! Record {}",
                   record_index);
        }
    }
} else {
    error!("DLQ not available, logging fallback: {}",
           error_message);
}
```

### 5. Clean Up Periodically

```rust
// After processing DLQ entries
async fn cleanup_dlq_after_recovery(dlq: &DeadLetterQueue) {
    dlq.clear().await;
    dlq.reset_max_size_flag();
    log::info!("DLQ cleared and reset");
}
```

## Troubleshooting

### Issue: DLQ Reaching Capacity Quickly

**Symptoms**: High rate of entries rejected, capacity exceeded frequently

**Causes**:
- High error rate in processing
- DLQ size limit too small
- Upstream data quality issues

**Solutions**:
1. Increase DLQ size limit
2. Investigate and fix error source
3. Review data quality
4. Implement recovery process

### Issue: DLQ Growing Unbounded

**Symptoms**: Memory usage increasing, process slowing down

**Causes**:
- No size limit set
- DLQ never cleared
- Continuous processing errors

**Solutions**:
1. Set appropriate `dlq_max_size`
2. Implement periodic cleanup
3. Export and analyze DLQ before clearing
4. Fix underlying data issues

### Issue: Missing DLQ Entries

**Symptoms**: Expected errors not in DLQ

**Causes**:
- DLQ disabled in config
- At capacity (entries rejected)
- FailBatch strategy in use

**Solutions**:
1. Check `enable_dlq: true` in config
2. Check capacity and increase if needed
3. Verify failure strategy
4. Check metrics for rejections

### Issue: High Memory Usage

**Symptoms**: Process memory growing continuously

**Causes**:
- Very large DLQ entries
- Many entries in memory
- No cleanup process

**Solutions**:
1. Reduce `dlq_max_size`
2. Implement periodic export and clear
3. Move DLQ entries to persistent storage
4. Use file-based DLQ instead of in-memory

## Advanced: Custom DLQ Implementation

For specialized needs, you can extend DLQ functionality:

```rust
use velostream::velostream::server::processors::DeadLetterQueue;

/// Custom DLQ with persistence
pub struct PersistentDLQ {
    memory_dlq: DeadLetterQueue,
    db_connection: Arc<DatabaseConnection>,
}

impl PersistentDLQ {
    pub async fn add_entry_with_persistence(
        &self,
        record: StreamRecord,
        error: String,
        index: usize,
        recoverable: bool,
    ) -> Result<()> {
        // Add to memory DLQ
        if !self.memory_dlq.add_entry(
            record.clone(), error.clone(), index, recoverable
        ).await {
            return Err("DLQ full".into());
        }

        // Also persist to database
        self.db_connection.insert_dlq_entry(
            record, error, index, recoverable
        ).await?;

        Ok(())
    }
}
```

## Summary

The Dead Letter Queue is a powerful tool for building robust, observable streaming pipelines. By properly configuring, monitoring, and managing the DLQ, you can:

- **Catch failures** without losing data
- **Debug issues** with full context
- **Recover gracefully** from errors
- **Monitor health** proactively
- **Maintain quality** with visibility

For more information, see:
- [DLQ Configuration Guide](./docs/dlq_configuration_guide.md)
- [DLQ Architecture](./docs/dlq_architecture.md)
- [Metrics Reference](./docs/metrics_reference.md)
