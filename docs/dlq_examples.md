# DLQ Configuration Examples

This document provides practical examples of DLQ configuration for different scenarios.

## Table of Contents

1. [Basic Setup](#basic-setup)
2. [Production Scenarios](#production-scenarios)
3. [Development & Testing](#development--testing)
4. [Advanced Patterns](#advanced-patterns)
5. [Recovery Workflows](#recovery-workflows)

## Basic Setup

### Example 1: Simple DLQ with Default Configuration

```rust
use velostream::velostream::server::processors::{JobProcessingConfig, FailureStrategy};
use std::time::Duration;

// Create config with defaults and DLQ enabled
let config = JobProcessingConfig::default();
assert!(config.enable_dlq);  // DLQ enabled by default
assert_eq!(config.dlq_max_size, None);  // Unlimited size
```

### Example 2: DLQ with Size Limit

```rust
let config = JobProcessingConfig {
    max_batch_size: 100,
    batch_timeout: Duration::from_millis(1000),
    use_transactions: false,
    failure_strategy: FailureStrategy::LogAndContinue,
    max_retries: 10,
    retry_backoff: Duration::from_millis(5000),
    log_progress: true,
    progress_interval: 10,
    empty_batch_count: 1000,
    wait_on_empty_batch_ms: 1000,
    enable_dlq: true,
    dlq_max_size: Some(10_000),  // Limit to 10K entries
};
```

### Example 3: Minimal DLQ Configuration

```rust
let config = JobProcessingConfig {
    enable_dlq: true,
    dlq_max_size: Some(5_000),
    ..Default::default()
};
```

## Production Scenarios

### Scenario 1: High-Volume Data Streaming

**Requirements**:
- Handle high error rates gracefully
- Monitor DLQ health continuously
- Automatic capacity management

```rust
use velostream::velostream::server::processors::{
    JobProcessingConfig, FailureStrategy, DeadLetterQueue
};
use velostream::velostream::server::metrics::{DLQMetrics, JobMetrics};
use std::time::Duration;

// Configuration for high-volume scenario
let config = JobProcessingConfig {
    max_batch_size: 10_000,        // Large batches
    batch_timeout: Duration::from_secs(2),
    use_transactions: false,
    failure_strategy: FailureStrategy::LogAndContinue,
    max_retries: 3,
    retry_backoff: Duration::from_millis(100),
    log_progress: true,
    progress_interval: 100,
    empty_batch_count: 100,
    wait_on_empty_batch_ms: 100,
    enable_dlq: true,
    dlq_max_size: Some(50_000),    // Large DLQ for volume
};

// Setup metrics monitoring
let dlq = DeadLetterQueue::with_max_size(50_000);
let dlq_metrics = DLQMetrics::new();
let job_metrics = JobMetrics::new();

// Background health monitoring
tokio::spawn({
    let dlq = dlq.clone();
    let metrics = dlq_metrics.clone();
    async move {
        loop {
            let health = summarize_dlq_health(&dlq, &metrics);
            if !health.is_healthy() {
                log::warn!("DLQ health: {} ({:.1}%)",
                          health.status_str(),
                          health.capacity_usage_percent.unwrap_or(0.0));
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
});
```

### Scenario 2: Financial Transaction Processing

**Requirements**:
- Zero data loss
- Exact error tracking
- Audit trail for compliance

```rust
use velostream::velostream::server::processors::FailureStrategy;

// Transaction processing configuration
let config = JobProcessingConfig {
    max_batch_size: 1_000,
    batch_timeout: Duration::from_millis(500),
    use_transactions: false,  // LogAndContinue preferred for recovery
    failure_strategy: FailureStrategy::LogAndContinue,
    max_retries: 5,
    retry_backoff: Duration::from_millis(1000),
    log_progress: true,
    progress_interval: 10,
    empty_batch_count: 500,
    wait_on_empty_batch_ms: 500,
    enable_dlq: true,
    dlq_max_size: Some(10_000),  // Moderate limit for audit
};

// Implement audit logging
async fn setup_audit_logging(dlq: &DeadLetterQueue) {
    tokio::spawn({
        let dlq = dlq.clone();
        async move {
            loop {
                let entries = dlq.get_entries().await;
                for entry in entries {
                    // Log to audit system
                    log::info!("AUDIT: Failed transaction at index {}. Error: {}. Recoverable: {}",
                              entry.record_index,
                              entry.error_message,
                              entry.recoverable);
                }
                tokio::time::sleep(Duration::from_secs(300)).await;
            }
        }
    });
}
```

### Scenario 3: Multi-Partition Data Processing

**Requirements**:
- Per-partition error isolation
- Prevent cascade failures
- Quick recovery

```rust
// Configuration for each partition
fn create_partition_config(partition_id: usize) -> JobProcessingConfig {
    JobProcessingConfig {
        max_batch_size: 500,
        batch_timeout: Duration::from_millis(1000),
        use_transactions: false,
        failure_strategy: FailureStrategy::LogAndContinue,
        max_retries: 3,
        retry_backoff: Duration::from_millis(500),
        log_progress: true,
        progress_interval: 50,
        empty_batch_count: 100,
        wait_on_empty_batch_ms: 500,
        enable_dlq: true,
        dlq_max_size: Some(2_000),  // Smaller per partition
    }
}

// Create per-partition DLQ
async fn setup_partition_dlqs(partition_count: usize) {
    let dlqs: Vec<_> = (0..partition_count)
        .map(|_| DeadLetterQueue::with_max_size(2_000))
        .collect();

    // Monitor all partitions
    for (pid, dlq) in dlqs.iter().enumerate() {
        tokio::spawn({
            let dlq = dlq.clone();
            async move {
                loop {
                    let size = dlq.len().await;
                    if size > 1_800 {  // Alert at 90%
                        log::warn!("Partition {}: DLQ {}/{} entries",
                                  pid, size, 2_000);
                    }
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
        });
    }
}
```

## Development & Testing

### Scenario 4: Local Development

**Requirements**:
- Minimal resource usage
- Easy debugging
- Fast failure tracking

```rust
let config = JobProcessingConfig {
    max_batch_size: 10,          // Small batches
    batch_timeout: Duration::from_millis(100),
    use_transactions: false,
    failure_strategy: FailureStrategy::LogAndContinue,
    max_retries: 1,              // Quick failures for debugging
    retry_backoff: Duration::from_millis(10),
    log_progress: true,
    progress_interval: 1,        // Log every batch
    empty_batch_count: 10,
    wait_on_empty_batch_ms: 10,
    enable_dlq: true,
    dlq_max_size: Some(100),     // Small DLQ for dev
};
```

### Scenario 5: Unit Testing

**Requirements**:
- Predictable behavior
- Easy assertion
- No side effects

```rust
#[tokio::test]
async fn test_dlq_capacity_tracking() {
    use velostream::velostream::server::processors::DeadLetterQueue;
    use velostream::velostream::sql::execution::StreamRecord;
    use std::collections::HashMap;

    let dlq = DeadLetterQueue::with_max_size(3);
    let mut record = StreamRecord::new(HashMap::new());

    // Fill DLQ to capacity
    assert!(dlq.add_entry(record.clone(), "error 1".to_string(), 0, true).await);
    assert!(dlq.add_entry(record.clone(), "error 2".to_string(), 1, true).await);
    assert!(dlq.add_entry(record.clone(), "error 3".to_string(), 2, true).await);

    // Should reject at capacity
    assert!(!dlq.add_entry(record.clone(), "error 4".to_string(), 3, true).await);
    assert!(dlq.is_at_capacity());
    assert_eq!(dlq.len().await, 3);

    // Clear and verify reset
    dlq.clear().await;
    dlq.reset_max_size_flag();
    assert!(!dlq.is_at_capacity());
    assert_eq!(dlq.len().await, 0);
}
```

### Scenario 6: Integration Testing

**Requirements**:
- Test error scenarios
- Verify recovery paths
- Check metrics

```rust
#[tokio::test]
async fn test_dlq_integration_with_processor() {
    use velostream::velostream::server::metrics::DLQMetrics;

    let dlq = DeadLetterQueue::with_max_size(100);
    let metrics = DLQMetrics::new();
    let config = JobProcessingConfig {
        enable_dlq: true,
        dlq_max_size: Some(100),
        ..Default::default()
    };

    // Simulate processor adding entries
    for i in 0..50 {
        let _ = dlq.add_entry(
            record.clone(),
            format!("Error {}", i),
            i,
            true
        ).await;
        metrics.record_entry_added();
    }

    // Verify metrics
    assert_eq!(metrics.entries_added(), 50);
    assert_eq!(dlq.len().await, 50);

    // Verify capacity
    assert!(!dlq.is_at_capacity());
    assert_eq!(dlq.capacity_usage_percent(), Some(50.0));

    // Verify recovery
    let entries = dlq.get_entries().await;
    assert_eq!(entries.len(), 50);
}
```

## Advanced Patterns

### Pattern 1: Adaptive Capacity Based on Error Rate

```rust
async fn adjust_dlq_capacity(
    dlq: &DeadLetterQueue,
    metrics: &JobMetrics,
    error_rate: f64,
) {
    // If error rate > 5%, increase monitoring frequency
    if error_rate > 5.0 {
        log::warn!("High error rate: {:.2}%. Increase DLQ capacity.", error_rate);
    }
}
```

### Pattern 2: Persistent DLQ Backup

```rust
async fn backup_dlq_entries(
    dlq: &DeadLetterQueue,
    backup_dir: &str,
) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let entries = dlq.get_entries().await;
    let backup_file = format!("{}/dlq_backup_{}.json",
                              backup_dir,
                              std::time::SystemTime::now()
                                  .duration_since(std::time::UNIX_EPOCH)?
                                  .as_secs());

    let mut file = File::create(&backup_file)?;
    let json = serde_json::to_string(&entries)?;
    file.write_all(json.as_bytes())?;

    log::info!("DLQ backed up to {}", backup_file);
    Ok(())
}
```

### Pattern 3: Time-Based DLQ Cleanup

```rust
async fn cleanup_old_dlq_entries(
    dlq: &DeadLetterQueue,
    max_age: Duration,
) {
    let entries = dlq.get_entries().await;
    let now = Instant::now();
    let old_entries: Vec<_> = entries.iter()
        .filter(|e| now.duration_since(e.timestamp) > max_age)
        .collect();

    log::info!("Found {} DLQ entries older than {:?}",
              old_entries.len(), max_age);

    // Process old entries
    // Then clear
    dlq.clear().await;
    dlq.reset_max_size_flag();
}
```

### Pattern 4: DLQ with Prioritization

```rust
async fn prioritize_dlq_recovery(
    dlq: &DeadLetterQueue,
) -> (Vec<DLQEntry>, Vec<DLQEntry>) {
    let entries = dlq.get_entries().await;

    // Separate recoverable and non-recoverable
    let (recoverable, non_recoverable): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|e| e.recoverable);

    // Process recoverable first (higher priority)
    (recoverable, non_recoverable)
}
```

## Recovery Workflows

### Workflow 1: Batch Recovery with Verification

```rust
async fn recover_dlq_with_verification(
    dlq: &DeadLetterQueue,
    processor: &SimpleJobProcessor,
) -> Result<RecoveryStats> {
    let entries = dlq.get_entries().await;
    let mut recovered = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    for entry in entries {
        match processor.process_record(entry.record.clone()).await {
            Ok(_) => {
                recovered += 1;
                log::info!("Recovered entry {}", entry.record_index);
            }
            Err(e) => {
                failed += 1;
                errors.push((entry.record_index, e.to_string()));
                log::warn!("Still failing entry {}: {}", entry.record_index, e);
            }
        }
    }

    // Backup failures for analysis
    if !errors.is_empty() {
        let json = serde_json::to_string(&errors)?;
        std::fs::write("recovery_failures.json", json)?;
    }

    // Clear DLQ
    dlq.clear().await;
    dlq.reset_max_size_flag();

    Ok(RecoveryStats {
        total: entries.len(),
        recovered,
        failed,
        errors,
    })
}

#[derive(Debug)]
struct RecoveryStats {
    total: usize,
    recovered: usize,
    failed: usize,
    errors: Vec<(usize, String)>,
}
```

### Workflow 2: Export and Process Externally

```rust
async fn export_dlq_for_external_processing(
    dlq: &DeadLetterQueue,
    export_format: ExportFormat,
) -> Result<String> {
    let entries = dlq.get_entries().await;

    let output = match export_format {
        ExportFormat::CSV => export_as_csv(&entries)?,
        ExportFormat::JSON => export_as_json(&entries)?,
        ExportFormat::Parquet => export_as_parquet(&entries)?,
    };

    // Save export
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let filename = format!("dlq_export_{}.{}", timestamp, export_format.extension());
    std::fs::write(&filename, &output)?;

    // Clear DLQ after successful export
    dlq.clear().await;
    dlq.reset_max_size_flag();

    Ok(filename)
}

enum ExportFormat {
    CSV,
    JSON,
    Parquet,
}

impl ExportFormat {
    fn extension(&self) -> &'static str {
        match self {
            Self::CSV => "csv",
            Self::JSON => "json",
            Self::Parquet => "parquet",
        }
    }
}
```

### Workflow 3: Incremental Recovery Loop

```rust
async fn incremental_dlq_recovery(
    dlq: &DeadLetterQueue,
    processor: &SimpleJobProcessor,
    batch_size: usize,
) -> Result<()> {
    loop {
        let entries = dlq.get_entries().await;
        if entries.is_empty() {
            log::info!("DLQ empty, recovery complete");
            break;
        }

        // Process in batches
        for chunk in entries.chunks(batch_size) {
            for entry in chunk {
                match processor.process_record(entry.record.clone()).await {
                    Ok(_) => {
                        log::debug!("Recovered: {}", entry.record_index);
                    }
                    Err(e) => {
                        log::warn!("Still failing: {}. Error: {}",
                                  entry.record_index, e);
                    }
                }
            }

            // Check capacity between batches
            let health = summarize_dlq_health(&dlq, &metrics);
            log::info!("Recovery progress: {} entries",
                      health.current_size);

            // Rate limiting
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Clear after each successful pass
        dlq.clear().await;
        dlq.reset_max_size_flag();

        // Wait before next cycle
        tokio::time::sleep(Duration::from_secs(60)).await;
    }

    Ok(())
}
```

## Summary

These examples demonstrate:
- **Basic setup**: Simple DLQ configurations
- **Production**: Handling real-world scenarios
- **Testing**: Verification and validation patterns
- **Advanced**: Custom behaviors and optimization
- **Recovery**: Different approaches to processing failed records

For more information, see:
- [DLQ Operations Guide](./dlq_operations_guide.md)
- [DLQ Configuration Guide](./dlq_configuration_guide.md)
