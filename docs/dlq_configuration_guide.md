# DLQ Configuration Guide

## JobProcessingConfig DLQ Settings

The `JobProcessingConfig` struct provides comprehensive DLQ configuration options:

### Configuration Fields

```rust
pub struct JobProcessingConfig {
    // ... other fields ...

    /// Enable Dead Letter Queue for failed records
    /// Default: true for most strategies, false for TransactionalJobProcessor
    pub enable_dlq: bool,

    /// Maximum size limit for the Dead Letter Queue
    /// If None, no limit is enforced. If Some(max), DLQ rejects entries when full.
    /// Default: None (unlimited)
    pub dlq_max_size: Option<usize>,
}
```

### Basic Configuration Examples

#### Example 1: Enable DLQ with Unlimited Size (Default)

```rust
use velostream::velostream::server::processors::JobProcessingConfig;

let config = JobProcessingConfig {
    enable_dlq: true,
    dlq_max_size: None,  // Unlimited
    ..Default::default()
};
```

#### Example 2: Enable DLQ with Size Limit

```rust
let config = JobProcessingConfig {
    enable_dlq: true,
    dlq_max_size: Some(10_000),  // Maximum 10,000 entries
    ..Default::default()
};
```

#### Example 3: Disable DLQ

```rust
let config = JobProcessingConfig {
    enable_dlq: false,
    dlq_max_size: None,
    ..Default::default()
};
```

#### Example 4: Complete Configuration

```rust
use std::time::Duration;
use velostream::velostream::server::processors::{
    JobProcessingConfig, FailureStrategy
};

let config = JobProcessingConfig {
    max_batch_size: 1000,
    batch_timeout: Duration::from_secs(5),
    use_transactions: false,
    failure_strategy: FailureStrategy::LogAndContinue,
    max_retries: 3,
    retry_backoff: Duration::from_millis(1000),
    log_progress: true,
    progress_interval: 100,
    empty_batch_count: 100,
    wait_on_empty_batch_ms: 500,
    enable_dlq: true,           // DLQ enabled
    dlq_max_size: Some(5_000),  // 5K entry limit
};
```

## Processor-Specific DLQ Configuration

### SimpleJobProcessor

SimpleJobProcessor processes records individually, making DLQ ideal:

```rust
use velostream::velostream::server::processors::SimpleJobProcessor;

let config = JobProcessingConfig {
    enable_dlq: true,  // Default: enabled
    dlq_max_size: Some(10_000),  // Recommended: moderate limit
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};

let processor = SimpleJobProcessor::new(query, config, ...)?;
```

**Recommendation**: Enabled with moderate size limit (1K-100K)

### TransactionalJobProcessor

TransactionalJobProcessor uses FailBatch strategy, DLQ disabled by design:

```rust
use velostream::velostream::server::processors::TransactionalJobProcessor;

let config = JobProcessingConfig {
    enable_dlq: false,  // Default: disabled (by design)
    dlq_max_size: None,
    failure_strategy: FailureStrategy::FailBatch,
    use_transactions: true,
    ..Default::default()
};

let processor = TransactionalJobProcessor::new(query, config, ...)?;
```

**Why disabled**: FailBatch rolls back entire batch on any error. Failed records aren't written, so DLQ would be empty.

### PartitionReceiver

PartitionReceiver processes each partition independently:

```rust
let config = JobProcessingConfig {
    enable_dlq: true,  // Default: enabled for debugging
    dlq_max_size: Some(1_000),  // Smaller limit for per-partition failures
    failure_strategy: FailureStrategy::LogAndContinue,
    ..Default::default()
};
```

**Recommendation**: Enabled with smaller limit per partition (500-5K)

## DeadLetterQueue Creation

### Creating DLQ with Configuration

```rust
use velostream::velostream::server::processors::DeadLetterQueue;

// No size limit (grows unbounded)
let dlq = DeadLetterQueue::new();

// Size-limited DLQ
let dlq = DeadLetterQueue::with_max_size(10_000);

// From JobProcessingConfig
let dlq = if let Some(max_size) = config.dlq_max_size {
    DeadLetterQueue::with_max_size(max_size)
} else {
    DeadLetterQueue::new()
};
```

## Size Limit Configuration

### Choosing Appropriate Size Limits

Size limits depend on:
1. **Memory constraints**: Each DLQEntry stores full record + error message
2. **Error rate**: Higher error rates → larger needed limits
3. **Recovery frequency**: How often you process DLQ
4. **Record size**: Larger records need smaller limits for same memory

### Size Limit Examples

#### Small (Testing/Development)

```rust
let config = JobProcessingConfig {
    dlq_max_size: Some(100),  // 100 entries
    ..Default::default()
};
```

**Use case**: Local testing, development environments

#### Medium (Standard)

```rust
let config = JobProcessingConfig {
    dlq_max_size: Some(10_000),  // 10K entries
    ..Default::default()
};
```

**Use case**: Production systems, moderate data volumes

#### Large (High-Volume)

```rust
let config = JobProcessingConfig {
    dlq_max_size: Some(100_000),  // 100K entries
    ..Default::default()
};
```

**Use case**: High-throughput systems, large record sizes acceptable

#### Unlimited

```rust
let config = JobProcessingConfig {
    dlq_max_size: None,  // No limit
    ..Default::default()
};
```

**Use case**: Non-critical pipelines, sufficient memory available

### Memory Impact

Rough memory estimate per DLQEntry:
- Base: ~200 bytes (metadata)
- StreamRecord: ~200-2000+ bytes
- Error message: ~100-1000 bytes

**Example calculation for 10K entries**:
```
10,000 entries × 1,500 bytes avg = 15 MB
```

## Monitoring Configuration

Enable DLQ metrics through configuration:

```rust
use velostream::velostream::server::metrics::{DLQMetrics, JobMetrics};

let dlq_metrics = DLQMetrics::new();
let job_metrics = JobMetrics::new();

// Enable DLQ in config
let config = JobProcessingConfig {
    enable_dlq: true,
    dlq_max_size: Some(10_000),
    ..Default::default()
};

// Use metrics to track DLQ health
let health = summarize_dlq_health(&dlq, &dlq_metrics);
println!("Status: {}", health.status_str());
```

## Configuration for Different Strategies

### LogAndContinue (Default)

```rust
use velostream::velostream::server::processors::FailureStrategy;

let config = JobProcessingConfig {
    failure_strategy: FailureStrategy::LogAndContinue,
    enable_dlq: true,  // DLQ essential for recovery
    dlq_max_size: Some(10_000),
    ..Default::default()
};
```

**DLQ Behavior**: Individual failed records added to DLQ

### SendToDLQ

```rust
let config = JobProcessingConfig {
    failure_strategy: FailureStrategy::SendToDLQ,
    enable_dlq: true,  // Required for this strategy
    dlq_max_size: Some(5_000),  // Batch-level failures
    ..Default::default()
};
```

**DLQ Behavior**: Failed batches sent to DLQ

### FailBatch

```rust
let config = JobProcessingConfig {
    failure_strategy: FailureStrategy::FailBatch,
    enable_dlq: false,  // Never use with FailBatch
    dlq_max_size: None,
    use_transactions: true,
    ..Default::default()
};
```

**DLQ Behavior**: Disabled (batch rolled back)

### RetryWithBackoff

```rust
let config = JobProcessingConfig {
    failure_strategy: FailureStrategy::RetryWithBackoff,
    enable_dlq: true,  // For records exceeding max_retries
    dlq_max_size: Some(1_000),  // Only unhealthy records
    max_retries: 5,
    retry_backoff: Duration::from_millis(500),
    ..Default::default()
};
```

**DLQ Behavior**: Records added after max retries exceeded

## Runtime Configuration Updates

### Check Current Configuration

```rust
let config = JobProcessingConfig::default();
println!("DLQ Enabled: {}", config.enable_dlq);
println!("DLQ Max Size: {:?}", config.dlq_max_size);
```

### Build Configuration from Defaults

```rust
use velostream::velostream::server::processors::JobProcessingConfig;

// Start with defaults, override specific fields
let mut config = JobProcessingConfig::default();
config.dlq_max_size = Some(50_000);
config.max_retries = 5;
```

## Configuration Validation

### Best Practices

```rust
fn validate_dlq_config(config: &JobProcessingConfig) -> Result<()> {
    // Check 1: FailBatch shouldn't have DLQ enabled
    if config.failure_strategy == FailureStrategy::FailBatch
        && config.enable_dlq
    {
        return Err("FailBatch strategy should not enable DLQ".into());
    }

    // Check 2: Size limit should be reasonable
    if let Some(max) = config.dlq_max_size {
        if max == 0 {
            return Err("DLQ size limit must be > 0".into());
        }
        if max > 1_000_000 {
            eprintln!("WARNING: Very large DLQ limit, check memory availability");
        }
    }

    // Check 3: Retry config consistency
    if config.max_retries > 0 && config.failure_strategy == FailureStrategy::LogAndContinue {
        // Retries make sense with LogAndContinue
    }

    Ok(())
}
```

## Configuration in YAML (Future)

Planned YAML configuration support:

```yaml
job_processing:
  max_batch_size: 1000
  batch_timeout_ms: 5000
  use_transactions: false
  failure_strategy: "LogAndContinue"
  max_retries: 3
  retry_backoff_ms: 1000

  # DLQ Configuration
  dlq:
    enabled: true
    max_size: 10000  # entries
    monitor_health: true
    alert_at_capacity: true
```

## Troubleshooting Configuration

### DLQ Not Working (Not Capturing Failures)

**Check 1**: Verify enabled
```rust
if !config.enable_dlq {
    // DLQ disabled, enable it
    config.enable_dlq = true;
}
```

**Check 2**: Verify strategy compatibility
```rust
// LogAndContinue and SendToDLQ work best
if config.failure_strategy == FailureStrategy::FailBatch {
    eprintln!("DLQ won't work with FailBatch strategy");
}
```

**Check 3**: Verify processor supports DLQ
```rust
// SimpleJobProcessor ✓
// PartitionReceiver ✓
// TransactionalJobProcessor ✗ (by design)
```

### DLQ Capacity Issues

**Check 1**: Size limit set correctly
```rust
if let Some(max) = config.dlq_max_size {
    println!("DLQ limited to {} entries", max);
} else {
    println!("DLQ unlimited");
}
```

**Check 2**: Entries being rejected
```rust
let metrics = DLQMetrics::new();
if metrics.entries_rejected() > 0 {
    eprintln!("DLQ rejecting entries - increase size limit");
    eprintln!("Rejected: {}", metrics.entries_rejected());
}
```

**Check 3**: Memory pressure
```rust
// Monitor memory and adjust max_size accordingly
let health = summarize_dlq_health(&dlq, &metrics);
if health.is_at_capacity {
    eprintln!("DLQ at capacity - reduce max_size or implement recovery");
}
```

## Summary

Key configuration points:

1. **Enable/Disable**: `enable_dlq` boolean flag
2. **Size Management**: `dlq_max_size` optional usize
3. **Strategy Alignment**: Choose compatible failure strategy
4. **Monitoring**: Use DLQMetrics for health tracking
5. **Validation**: Check configuration consistency
6. **Memory**: Size limits prevent unbounded growth

For operational details, see [DLQ Operations Guide](./dlq_operations_guide.md).
