# LogAndContinue Strategy - Error Logging & DLQ Behavior

## Overview

`LogAndContinue` is the default failure strategy for SimpleJobProcessor. When records fail during processing, the strategy logs the error, adds it to the Dead Letter Queue (if enabled), and continues processing the remaining records in the batch.

**Key Characteristics:**
- ✅ Continues batch processing even when records fail
- ✅ Logs all failure details at WARN/INFO levels
- ✅ Stores failed records in DLQ (if enabled)
- ✅ Best-effort semantics (some records may be lost on error)
- ✅ Optimized for throughput

---

## Logging Behavior

LogAndContinue logs errors at multiple levels with specific patterns. This guide explains exactly what gets logged and where.

### Batch-Level Summary Log (WARN)

When a batch contains any failures, a batch-level summary is logged:

```log
[WARN] Job 'market_analysis': Source 'kafka_input' had 3 failures (continuing)
```

**Logged Once Per Batch**
- Log Level: **WARN**
- Frequency: Once when batch has failures
- Location: `src/velostream/server/processors/simple.rs:process_batch()`

**Information Provided:**
- Job name
- Source name
- Number of failed records
- Processing continues (not stopping)

### Individual Record Error Logs (WARN)

For the **first 3 errors in a batch**, individual error details are logged:

```log
[WARN] Job 'market_analysis' failed to process record 5: Invalid JSON in 'price' field [Recoverable: true]
[WARN] Job 'market_analysis' failed to process record 12: Missing required column 'symbol' [Recoverable: false]
[WARN] Job 'market_analysis' failed to process record 27: Column 'timestamp' not found [Recoverable: false]
```

**Logged For:** First 3 errors only per batch
- Log Level: **WARN**
- Details:
  - Record index (position in batch)
  - Error message (from SQL execution engine)
  - Whether error is recoverable

**Why Only First 3?**
- Prevents log spam in high-error scenarios
- Provides representative sampling of errors
- If you see 3 different errors, you know the pattern
- Full error details available in DLQ

**Location:** `src/velostream/server/processors/common.rs:process_batch()`

### DLQ Fallback Logs (INFO or WARN)

If DLQ is **disabled**, individual errors are logged instead:

```log
[INFO] Failed Record 5: Invalid JSON in 'price' field
[INFO] Failed Record 12: Missing required column 'symbol'
[INFO] Failed Record 27: Column 'timestamp' not found
```

**Logged When:** DLQ is disabled (`enable_dlq: false`)
- Log Level: **INFO** or **WARN** (depending on context)
- For Each Failed Record
- Alternative to DLQ storage

**Location:** `src/velostream/server/processors/simple.rs`

---

## Complete Error Information Flow

### What Gets Logged

LogAndContinue logs **three types of information**:

#### 1. Batch-Level Summary
```rust
warn!(
    "Job '{}': Source '{}' had {} failures (continuing)",
    job_name, source_name, batch_result.records_failed
);
```
- Count of failed records in batch
- Processing continues
- No individual error details

#### 2. Individual Record Details (First 3 Only)
```rust
warn!(
    "Job '{}' failed to process record {}: {} [Recoverable: {}]",
    job_name, index, detailed_msg, recoverable
);
```
- Record position/index
- Error message
- Recoverable flag (true/false)

#### 3. DLQ Entry (If Enabled)
```rust
dlq.add_entry(
    record,                 // Full record data
    error_message,          // Error message
    record_index,           // Index
    recoverable             // Recoverable flag
).await;
```
- Complete failed record
- Error message
- Timestamp
- Recoverable flag

### What Doesn't Get Logged

LogAndContinue **does NOT log:**
- ❌ Records 4+ in a batch (to prevent spam)
- ❌ Detailed stack traces
- ❌ Internal SQL engine state
- ❌ Query execution context

**These are available in DLQ for inspection.**

---

## Example: Complete Error Tracking

### Scenario: Batch of 100 Records, 5 Failures

```
Input: 100 records from kafka_topic

Record Processing:
  1-4:    ✅ Success
  5:      ❌ Error: Invalid JSON in 'price' → LOGGED (1st error)
  6-11:   ✅ Success
  12:     ❌ Error: Missing 'symbol' → LOGGED (2nd error)
  13-26:  ✅ Success
  27:     ❌ Error: 'timestamp' not found → LOGGED (3rd error)
  28-48:  ✅ Success
  49:     ❌ Error: Invalid type conversion → NOT LOGGED (4th error)
  50-95:  ✅ Success
  96:     ❌ Error: Out of range value → NOT LOGGED (5th error)
  97-100: ✅ Success

Batch Result:
  - Processed: 95 records
  - Failed: 5 records
```

### Log Output

**Batch-Level Summary (1 log entry):**
```
[WARN] Job 'analytics': Source 'kafka_topic' had 5 failures (continuing)
```

**Individual Errors (3 log entries, first 3 only):**
```
[WARN] Job 'analytics' failed to process record 5: Invalid JSON in 'price' [Recoverable: true]
[WARN] Job 'analytics' failed to process record 12: Missing required column 'symbol' [Recoverable: false]
[WARN] Job 'analytics' failed to process record 27: Column 'timestamp' not found [Recoverable: false]
```

**DLQ Entries (5 entries if DLQ enabled):**
```
DLQ Entry 1: Record 5, Error: "Invalid JSON in 'price'", Fields: {...}
DLQ Entry 2: Record 12, Error: "Missing required column 'symbol'", Fields: {...}
DLQ Entry 3: Record 27, Error: "Column 'timestamp' not found", Fields: {...}
DLQ Entry 4: Record 49, Error: "Invalid type conversion", Fields: {...}
DLQ Entry 5: Record 96, Error: "Out of range value", Fields: {...}
```

**Summary:**
- 4 log messages total (1 batch summary + 3 individual errors)
- 5 DLQ entries with complete record data
- 95 records successfully processed
- Processing continues

---

## Controlling Logging Level

### Log Levels Used

LogAndContinue uses these log levels consistently:

| Log Type | Level | When |
|----------|-------|------|
| Batch summary | WARN | Any failures in batch |
| Individual errors | WARN | First 3 errors per batch |
| DLQ disabled fallback | INFO | All errors when no DLQ |

### How to Enable/Disable Logs

**Enable all logs (production):**
```bash
RUST_LOG=warn ./your_app
```

**See detailed error info:**
```bash
RUST_LOG=info ./your_app
```

**Disable batch warnings (not recommended):**
```bash
RUST_LOG=error ./your_app  # Only errors, no warnings
```

---

## Error Details Captured

For each failed record, LogAndContinue captures:

### In Logs
- Record index
- Error message
- Recoverable flag

### In DLQ (If Enabled)
- **Record index** - Position in batch
- **Error message** - Detailed description
- **Recoverable** - Whether retrying might help
- **Record data** - All field values
- **Timestamp** - When failure occurred

### Example DLQ Entry

```rust
DLQEntry {
    record: StreamRecord {
        fields: {
            "id": Integer(45),
            "symbol": String("AAPL"),
            "price": String("invalid_json"),  // ← Problem field
            "timestamp": Timestamp(2024-11-18T10:30:00),
        },
        partition: 0,
        offset: 1045,
    },
    error_message: "Invalid JSON in 'price' field".to_string(),
    record_index: 5,
    recoverable: true,
    timestamp: Instant::now(),
}
```

---

## Configuration Impact on Logging

### With DLQ Enabled (Default)

```rust
JobProcessingConfig {
    failure_strategy: FailureStrategy::LogAndContinue,
    enable_dlq: true,  // ← Enabled
    dlq_max_size: Some(100),
    ..Default::default()
}
```

**Behavior:**
```
Batch summary log
    ↓
First 3 errors logged (WARN)
    ↓
All failed records added to DLQ
    ↓
Query DLQ later for full error details
```

### With DLQ Disabled

```rust
JobProcessingConfig {
    failure_strategy: FailureStrategy::LogAndContinue,
    enable_dlq: false,  // ← Disabled
    ..Default::default()
}
```

**Behavior:**
```
Batch summary log
    ↓
First 3 errors logged (WARN)
    ↓
All failures logged individually (INFO level)
    ↓
No DLQ storage
```

**Result:** More verbose logging, less convenient error inspection

---

## Comparing Strategies

### LogAndContinue vs FailBatch

| Aspect | LogAndContinue | FailBatch |
|--------|----------------|-----------|
| **Logging** | Batch summary + 1st 3 errors | Batch error only |
| **DLQ** | Individual records | Entire batch (no use) |
| **Throughput** | High (no retries) | Lower (batch retry) |
| **Data Loss** | Possible (failed records skipped) | No (all-or-nothing) |
| **Use Case** | Analytics, logs, metrics | Financial, critical data |

**Example:**
```
LogAndContinue (100 records, 3 fail):
  [WARN] Batch had 3 failures
  [WARN] Record 5 error
  [WARN] Record 12 error
  [WARN] Record 27 error
  Result: 97 records in sink, 3 in DLQ

FailBatch (100 records, 3 fail):
  [ERROR] Batch failed: record 5 error
  [INFO] Retrying entire batch
  Result: All 100 records retried
```

---

## Best Practices

### 1. Always Use DLQ with LogAndContinue

```rust
// ✅ RECOMMENDED
JobProcessingConfig {
    failure_strategy: FailureStrategy::LogAndContinue,
    enable_dlq: true,  // Capture all failures
    ..Default::default()
}
```

**Why:** See all error details later, not just first 3 in logs

### 2. Monitor Batch Failure Warnings

```bash
# Watch for batch failure patterns
grep "had.*failures" application.log

# Sample output:
# [WARN] Job 'analytics': had 5 failures
# [WARN] Job 'analytics': had 2 failures
# [WARN] Job 'analytics': had 12 failures  ← Increasing!
```

### 3. Regularly Check DLQ

```rust
// Inspect DLQ entries periodically
let dlq_entries = processor.dlq.get_entries().await;
println!("Failed records this batch: {}", dlq_entries.len());

// Group by error type
let mut errors_by_type = HashMap::new();
for entry in dlq_entries {
    *errors_by_type.entry(entry.error_message).or_insert(0) += 1;
}

// Identify patterns
for (error, count) in errors_by_type {
    if count > 10 {
        eprintln!("ALERT: {} errors occurred {} times!", error, count);
    }
}
```

### 4. Act on Error Patterns

When you see repeated errors in logs/DLQ:

```
Pattern: "Invalid JSON in 'price' field" (occurs 30 times)

Investigation:
  1. Check what values are in the DLQ price field
  2. Determine if data source is corrupted
  3. Fix the source or add validation

Action:
  1. Fix the root cause
  2. Clear DLQ
  3. Resume processing
```

---

## Troubleshooting

### Issue: Not Seeing Expected Error Logs

**Problem:** Batch failures occur but no WARN logs appear

**Causes:**
1. Log level not set to WARN or higher
2. Errors occur but are < 3 per batch (early exit in loop)
3. DLQ enabled but you're only looking at first 3 logs

**Solution:**
```bash
# Ensure log level includes WARN
RUST_LOG=warn,info ./app

# Or check DLQ instead of logs
dlq.print_entries().await;
```

### Issue: DLQ Growing Too Large

**Problem:** DLQ capacity exceeded, new entries rejected

**Symptoms:**
```log
[DEBUG] DLQ at capacity: 100/100 entries. Rejecting new entry.
```

**Solution:**
```rust
// Investigate root cause
let failures = dlq.get_entries().await;
let errors_by_type = group_by_error_type(failures);
// → Find that "Invalid JSON" is 80% of failures

// Fix the source
// → Add data validation upstream

// Clear DLQ and continue
dlq.clear().await;
```

### Issue: Mixed Log Levels Confusing

**Problem:** Some errors at WARN, some at INFO

**Cause:** DLQ disabled, using INFO fallback logs

**Solution:**
```rust
// Enable DLQ (recommended)
enable_dlq: true,

// Use WARN level for logs
RUST_LOG=warn
```

---

## Log Output Examples

### High-Quality Data (No Errors)

```log
[INFO] Job 'analytics': Processed batch 45: 1000 records/sec
[INFO] Job 'analytics': Processed batch 46: 998 records/sec
[INFO] Job 'analytics': Processed batch 47: 1002 records/sec
```

### Data Quality Issues (Few Errors)

```log
[INFO] Job 'analytics': Processed batch 45: 987 records/sec
[WARN] Job 'analytics': Source 'kafka_input' had 13 failures (continuing)
[WARN] Job 'analytics' failed to process record 23: Invalid JSON [Recoverable: true]
[WARN] Job 'analytics' failed to process record 56: Type mismatch [Recoverable: false]
[WARN] Job 'analytics' failed to process record 91: Out of range [Recoverable: true]
[INFO] Job 'analytics': Processed batch 46: 987 records/sec
```

### Data Quality Crisis (Many Errors)

```log
[WARN] Job 'analytics': Source 'kafka_input' had 234 failures (continuing)
[WARN] Job 'analytics' failed to process record 1: Invalid JSON [Recoverable: true]
[WARN] Job 'analytics' failed to process record 4: Invalid JSON [Recoverable: true]
[WARN] Job 'analytics' failed to process record 8: Invalid JSON [Recoverable: true]
[WARN] DLQ at capacity: 100/100 entries. Rejecting new entry.
[WARN] Job 'analytics': Source 'kafka_input' had 267 failures (continuing)
[WARN] Job 'analytics' failed to process record 2: Invalid JSON [Recoverable: true]
...
```

In this case:
- **Stop processing**
- **Investigate DLQ for pattern** (all "Invalid JSON")
- **Fix the data source**
- **Clear DLQ and restart**

---

## See Also

- [DLQ Configuration Guide](./dlq-configuration-guide.md) - Complete DLQ reference
- [Job Processor Configuration Guide](job-processor-configuration-guide.md) - Failure strategies explained
- [Future Annotations Roadmap](./future-annotations-roadmap.md) - Planned logging control annotations
