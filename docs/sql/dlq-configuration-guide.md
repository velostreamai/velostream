# Dead Letter Queue (DLQ) Configuration Guide

## Overview

The Dead Letter Queue (DLQ) is a feature that captures failed records during stream processing for inspection, debugging, and recovery. This guide explains DLQ behavior, configuration, and best practices.

---

## What is the Dead Letter Queue?

A Dead Letter Queue is a separate queue that stores records that failed processing. Instead of losing error information or crashing the entire job, failed records are captured with:
- The original record data
- Error message explaining what went wrong
- Record index and timing information
- Whether the error is recoverable

### DLQ Entry Structure

```rust
pub struct DLQEntry {
    pub record: StreamRecord,           // The failed record with all fields
    pub error_message: String,          // Human-readable error description
    pub record_index: usize,            // Position in the batch
    pub recoverable: bool,              // Whether retrying might succeed
    pub timestamp: Instant,             // When the failure occurred
}
```

---

## Default Behavior by Processor Type

### SimpleJobProcessor: DLQ ENABLED by Default ✅

**Default Configuration:**
```rust
SimpleJobProcessor {
    enable_dlq: true,  // ← Enabled by default
    dlq_max_size: Some(100),  // Max 100 entries
}
```

**Why DLQ is enabled:**
- Uses `LogAndContinue` failure strategy by default
- Individual records can fail while batch continues
- DLQ captures these individual failures for analysis
- Supports error recovery and debugging in production

**Example Behavior:**
```
Batch of 100 records arrives:
  Records 1-45: ✅ Process successfully
  Record 46: ❌ Invalid JSON - Added to DLQ, batch continues
  Records 47-100: ✅ Process successfully

Result: 99 records processed, 1 in DLQ
```

### TransactionalJobProcessor: DLQ DISABLED by Default ❌

**Default Configuration:**
```rust
TransactionalJobProcessor {
    enable_dlq: false,  // ← Disabled by default
    failure_strategy: FailureStrategy::FailBatch,
}
```

**Why DLQ is disabled:**
- Uses `FailBatch` failure strategy by default
- **Any record failure causes entire batch to rollback**
- No records from the failed batch are written to sink
- DLQ would remain empty (no failed records to capture)
- Recovery is at batch-level (retry whole batch), not record-level

**Example Behavior:**
```
Batch of 100 records arrives:
  Records 1-45: ✅ Process successfully (staged)
  Record 46: ❌ Invalid JSON - Entire batch rolled back!
  Records 47-100: ⏸️ Never processed

Result: All 100 records rolled back, batch retried
DLQ: Empty (no individual record failures captured)
```

---

## DLQ Configuration

### Enabling DLQ

#### For SimpleJobProcessor (Enabled by Default)

```sql
-- DLQ is already enabled, no configuration needed
-- @job_mode: simple
CREATE STREAM with_dlq AS
SELECT * FROM events
EMIT CHANGES;
```

To explicitly enable (when annotation support is available):
```sql
-- @enable_dlq: true  -- Already default, but explicit is fine
CREATE STREAM with_dlq AS
SELECT * FROM events EMIT CHANGES;
```

#### For TransactionalJobProcessor (Disabled by Default)

For debugging, you can enable DLQ even though batch rollback is used:

```rust
// Explicitly enable DLQ for debugging (code configuration)
TransactionalJobProcessor::new(JobProcessingConfig {
    use_transactions: true,
    enable_dlq: true,  // Explicitly enable for debugging
    ..Default::default()
})
```

Future SQL annotation (when implemented):
```sql
-- @enable_dlq: true
-- @job_mode: transactional
CREATE STREAM trans_with_dlq AS
SELECT * FROM events EMIT CHANGES;
```

---

### Disabling DLQ

To disable DLQ for maximum throughput:

```rust
// Code configuration
SimpleJobProcessor::new(JobProcessingConfig {
    enable_dlq: false,  // Disable for ultra-high throughput
    ..Default::default()
})
```

Future SQL annotation (when implemented):
```sql
-- @enable_dlq: false
-- @job_mode: simple
CREATE STREAM ultra_fast AS
SELECT * FROM events EMIT CHANGES;
```

---

### Setting DLQ Size Limit

Control maximum number of entries stored in DLQ:

**Current:** Only via code configuration
```rust
JobProcessingConfig {
    dlq_max_size: Some(500),  // Store up to 500 entries
    ..Default::default()
}
```

**Future SQL annotation (planned):**
```sql
-- @dlq_max_size: 500
CREATE STREAM debug_stream AS
SELECT * FROM events EMIT CHANGES;
```

---

## DLQ Behavior in Detail

### When Records Enter the DLQ

**SimpleJobProcessor (LogAndContinue):**
- Record fails SQL execution (e.g., column doesn't exist)
- Record fails serialization (e.g., invalid JSON conversion)
- Record fails filtering/transformation logic
- **→ Added to DLQ, batch continues with other records**

**TransactionalJobProcessor (FailBatch):**
- Same failure types occur
- **→ Entire batch rolled back, individual records NOT added to DLQ**
- Batch is retried from the source

### Capacity & Overflow

When DLQ reaches max size (default: 100 entries):

```rust
if dlq_entry_count >= max_size {
    new_entry_rejected = true;
    log_debug("DLQ at capacity: 100/100. Rejecting new entry.");
}
```

**Behavior:**
- New failed records are **rejected** (not added to DLQ)
- Processing continues (no crash)
- `is_at_capacity()` flag set to true
- Log message emitted (debug level)

**Recommendation:** Monitor DLQ capacity and clear when full

---

## Accessing DLQ Data

### Via Code (Current)

```rust
// Get all DLQ entries
let entries = processor.dlq.get_entries().await;
println!("Failed records: {}", entries.len());

for entry in entries {
    println!("Record {}: {}", entry.record_index, entry.error_message);
}

// Print formatted DLQ contents
processor.dlq.print_entries().await;

// Clear DLQ when done investigating
processor.dlq.clear().await;

// Check capacity
if processor.dlq.is_at_capacity() {
    println!("DLQ is full! Need to clear old entries.");
}
```

### Via Observability APIs (Future)

Once integration is complete:
```rust
// Query DLQ via metrics/telemetry system
let dlq_entries = observability_manager.get_dlq_entries("job_id");
let dlq_size = observability_manager.get_dlq_size("job_id");
let dlq_at_capacity = observability_manager.is_dlq_full("job_id");
```

### Via Kafka Topic (Future)

Route DLQ entries to a Kafka topic for downstream processing:
```sql
-- PLANNED: Route failed records to DLQ topic
CREATE STREAM dlq_processor AS
SELECT * FROM main_stream
WITH ('dlq.sink' = 'kafka://dlq-topic');
```

---

## DLQ Use Cases

### 1. Debugging Failed Records

Find what fields caused failures and fix them:

```rust
// Check DLQ to understand failure pattern
for entry in dlq.get_entries().await {
    println!("Record {}: {:?}", entry.record_index, entry.record.fields);
    println!("Error: {}", entry.error_message);
    // → "Invalid JSON in price field"
    // → Check the value of 'price' field in this record
}
```

### 2. Data Quality Monitoring

Track how many records fail per error type:

```rust
let dlq_entries = dlq.get_entries().await;
let errors_by_type = dlq_entries
    .iter()
    .map(|e| &e.error_message)
    .collect::<std::collections::HashSet<_>>();

for error in errors_by_type {
    let count = dlq_entries
        .iter()
        .filter(|e| &e.error_message == error)
        .count();
    println!("{}: {} occurrences", error, count);
}
```

### 3. Selective Recovery

Re-process failed records after fixing the issue:

```rust
// 1. Identify the problem from DLQ
let failed_records = dlq.get_entries().await;
// → Saw that "price" field has invalid values for records 45-67

// 2. Fix the source or transformation logic

// 3. Clear DLQ and restart processing
dlq.clear().await;
// → Records will be re-processed with fixed logic
```

### 4. Post-Incident Analysis

Investigate what went wrong during an outage:

```rust
// DLQ persists in-memory during job lifetime
// After an incident, query DLQ before restarting job
let incident_failures = dlq.get_entries().await;

for entry in incident_failures {
    eprintln!("Failed at {}: {}",
        entry.timestamp,
        entry.error_message
    );
    eprintln!("Recoverable: {}", entry.recoverable);
}

// Then clear and restart
dlq.clear().await;
```

---

## Performance Impact

### DLQ Overhead

DLQ has minimal overhead when disabled or lightly used:

| Scenario | Overhead | Impact |
|----------|----------|--------|
| DLQ disabled (`enable_dlq: false`) | 0% | No overhead |
| DLQ enabled, no failures | <1% | Minimal (capacity check only) |
| DLQ enabled, 1% failure rate | 1-2% | One entry per 100 records |
| DLQ enabled, at capacity | 1-2% | Rejecting new entries (no allocation) |

### Memory Usage

```
DLQ Entry size ≈ 1KB-10KB (depending on record size)
Max entries (default): 100
Max memory (default): 100KB-1MB

With custom size limit:
- dlq_max_size: 500 → 500KB-5MB
- dlq_max_size: 1000 → 1MB-10MB
```

---

## Best Practices

### 1. Enable DLQ for SimpleJobProcessor (Default)

```sql
-- ✅ RECOMMENDED: Keep DLQ enabled
-- @job_mode: simple
CREATE STREAM standard AS
SELECT * FROM events EMIT CHANGES;
```

**Why:**
- Minimal overhead (<1%)
- Captures errors for debugging
- Helps identify data quality issues

### 2. Monitor DLQ Capacity

```rust
// Check periodically
if dlq.is_at_capacity() {
    warn!("DLQ full! Clear old entries or increase dlq_max_size");
    dlq.clear().await;  // Clear to allow new entries
}
```

### 3. Size DLQ Appropriately

```rust
// Balance memory usage vs. debugging information
JobProcessingConfig {
    dlq_max_size: Some(500),  // For high-error scenarios
    // or
    dlq_max_size: Some(50),   // For low-error, memory-constrained
    ..Default::default()
}
```

### 4. Disable DLQ Only for Ultra-High Throughput

```sql
-- ❌ ONLY if throughput is critical
-- @enable_dlq: false
-- @job_mode: simple
CREATE STREAM ultra_fast AS
SELECT * FROM events EMIT CHANGES;
```

**Tradeoff:** Lose error tracking for ~1-2% throughput gain

### 5. Plan for DLQ Persistence

Current: DLQ is in-memory (lost on job restart)

Future plan: Option to write failed records to Kafka topic for durability

```sql
-- PLANNED: Persist DLQ to Kafka
-- @dlq_sink: 'kafka://dlq-topic'
CREATE STREAM main AS
SELECT * FROM events EMIT CHANGES;
```

---

## Troubleshooting

### DLQ is Empty but Errors Are Occurring

**Likely Cause:** DLQ is disabled
```rust
// Check if enable_dlq is false
if !config.enable_dlq {
    // Enable DLQ
    config.enable_dlq = true;
}
```

### DLQ Keeps Reaching Capacity

**Symptoms:** Frequent "DLQ at capacity" log messages

**Solutions:**
```rust
// 1. Increase size limit
JobProcessingConfig {
    dlq_max_size: Some(1000),  // Increase from default 100
    ..Default::default()
}

// 2. Clear DLQ more frequently
if dlq.len().await > 50 {  // Clear when 50% full
    dlq.clear().await;
}

// 3. Fix the underlying data quality issue
// → Address whatever is causing so many failures
```

### DLQ Size Is Growing Too Large

**Symptoms:** Memory usage increasing, DLQ entries not being cleared

**Solutions:**
```rust
// 1. Investigate failure root cause
let failures = dlq.get_entries().await;
// Group by error type, find pattern

// 2. Fix the source data or transformation
// 3. Clear DLQ
dlq.clear().await;

// 4. Consider if DLQ is right for this workload
// If failures are expected/normal, disabling DLQ may be appropriate
```

### TransactionalJobProcessor: DLQ Always Empty

**Expected Behavior:** DLQ remains empty because FailBatch strategy rolls back entire batches

**To Debug Failures:**
```rust
// With TransactionalJobProcessor:
// - Failed batch is logged
// - Entire batch retried
// - No individual record errors captured in DLQ

// To see what failed:
// Check application logs for "Batch X failed: [error]"
// Or enable DLQ explicitly (for debugging only):
TransactionalJobProcessor::new(JobProcessingConfig {
    enable_dlq: true,  // Explicit: for debugging only
    ..Default::default()
})
```

---

## DLQ Specification

### Public API

```rust
pub struct DeadLetterQueue {
    // Queue entries (read-only externally)
    pub entries: Arc<Mutex<Vec<DLQEntry>>>,

    // Max size configuration
    max_size: Option<usize>,

    // Status flags
    max_size_reached: Arc<AtomicBool>,
    entry_count: Arc<AtomicUsize>,
}

impl DeadLetterQueue {
    pub fn new() -> Self;
    pub fn with_max_size(max_size: usize) -> Self;

    pub async fn add_entry(
        &self,
        record: StreamRecord,
        error_message: String,
        record_index: usize,
        recoverable: bool,
    ) -> bool;  // Returns false if at capacity

    pub async fn get_entries(&self) -> Vec<DLQEntry>;
    pub async fn len(&self) -> usize;
    pub async fn is_empty(&self) -> bool;
    pub async fn clear(&self);

    pub fn max_size(&self) -> Option<usize>;
    pub fn is_at_capacity(&self) -> bool;
    pub fn reset_max_size_flag(&self);
}
```

---

## See Also

- [Job Processor Configuration Guide](./job-processor-configuration-guide.md) - Complete processor configuration
- [Job Annotations Guide](./job-annotations-guide.md) - Current SQL annotations
- [Future Annotations Roadmap](./future-annotations-roadmap.md) - Planned `@enable_dlq` and `@dlq_max_size` annotations
- [Failure Handling Strategy Guide](../developer/failure-strategies.md) - Planned document
