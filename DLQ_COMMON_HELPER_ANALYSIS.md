# DLQ Common Helper Logic Analysis

## Current State by Processor

### SimpleJobProcessor ✅ (DLQ Enabled)
- **File**: `src/velostream/server/processors/simple.rs` (lines 641-714)
- **Strategy**: `SendToDLQ` - Adds individual failed records to DLQ
- **Error Handling**: Implemented with error logging and fallback
- **Pattern**:
  ```rust
  if let Some(dlq) = self.observability_wrapper.dlq() {
      for error in &batch_result.error_details {
          // Create DLQ record with error context
          // Call dlq.add_entry() with error handling
      }
  }
  ```

### TransactionalJobProcessor ❌ (DLQ Disabled)
- **File**: `src/velostream/server/processors/transactional.rs` (lines 1062-1072)
- **Strategy**: `FailBatch` - Rolls back entire batch on error
- **DLQ Status**: **Explicitly disabled** (by design)
- **Rationale**:
  - FailBatch rolls back entire batch
  - Failed records are NOT written to sink
  - DLQ only useful for partial success (LogAndContinue)
  - SendToDLQ case is handled with warning (treats as FailBatch)
- **Design Note**: This is intentional - no DLQ needed for transactional mode

### PartitionReceiver ✅ (DLQ Enabled)
- **File**: `src/velostream/server/v2/partition_receiver.rs`
- **Patterns**:
  - **Batch-level**: Lines 293-316 (max retries exceeded)
  - **Record-level**: Lines 438-466 (individual record errors)
- **Error Handling**: Implemented with panic detection and logging
- **Pattern**:
  ```rust
  if let Some(dlq) = self.observability_wrapper.dlq() {
      // Build error context
      // Call dlq.add_entry() with error handling
      // Handle panic with catch_unwind + block_on
  }
  ```

### AdaptiveJobProcessor ⚠️ (Not Yet Implemented)
- **File**: `src/velostream/server/v2/adaptive_job_processor.rs`
- **Status**: No DLQ handling found
- **Need**: To be determined based on failure strategy

---

## Common Patterns Identified

### Pattern 1: Building DLQ Record with Error Context
```rust
let mut record_data = std::collections::HashMap::new();
record_data.insert("error".to_string(), FieldValue::String(error_message));
record_data.insert("source".to_string(), FieldValue::String(source_name));
// Optional fields: partition_id, batch_size, etc.
let dlq_record = StreamRecord::new(record_data);
```

### Pattern 2: Error Handling Around add_entry
Both SimpleJobProcessor and PartitionReceiver implement:
```rust
if let Some(dlq) = self.observability_wrapper.dlq() {
    match catch_unwind/await_call {
        Ok(_) => debug!("DLQ entry added"),
        Err(_) => error!("Failed to add DLQ entry"),
    }
} else {
    warn!("DLQ not enabled, logging fallback");
}
```

### Pattern 3: Context-Specific DLQ Entries
Different processors add different context fields:
- SimpleJobProcessor: `error`, `source`
- PartitionReceiver (record): `error`, `partition_id`
- PartitionReceiver (batch): `error`, `partition_id`, `batch_size`

---

## Extraction Options

### Option A: DLQHelper Trait (Recommended)
Create a trait with default implementations for common operations:

```rust
pub trait DLQErrorHandler {
    fn create_dlq_record(
        &self,
        error_message: String,
        context: HashMap<String, FieldValue>,
    ) -> StreamRecord {
        let mut record_data = context;
        record_data.insert("error".to_string(), FieldValue::String(error_message));
        StreamRecord::new(record_data)
    }

    async fn write_dlq_entry_with_context(
        &self,
        dlq: Option<&Arc<DeadLetterQueue>>,
        record: StreamRecord,
        error_msg: String,
        record_index: usize,
        recoverable: bool,
        fallback_log: impl Fn(),
    ) -> bool {
        if let Some(dlq) = dlq {
            // Common error handling
            match catch_unwind(...) {
                Ok(_) => {
                    debug!("DLQ entry added");
                    true
                }
                Err(_) => {
                    error!("Failed to add DLQ entry");
                    fallback_log();
                    false
                }
            }
        } else {
            fallback_log();
            false
        }
    }
}

// Impl for processors
impl DLQErrorHandler for SimpleJobProcessor {}
impl DLQErrorHandler for PartitionReceiver {}
```

**Pros**:
- Default implementations reduce duplication
- Processors can override if needed
- Type-safe and compile-time checked
- Flexible for context-specific variations

**Cons**:
- Async trait complexity (requires async_trait crate)
- May be over-engineered if variations are significant

---

### Option B: DLQHelper Utility Module (Simpler)
Create a `dlq_helper.rs` module with utility functions:

```rust
// src/velostream/server/processors/dlq_helper.rs

pub struct DLQEntryBuilder {
    record_data: HashMap<String, FieldValue>,
    error_message: String,
}

impl DLQEntryBuilder {
    pub fn new(error_message: String) -> Self {
        Self {
            record_data: HashMap::new(),
            error_message,
        }
    }

    pub fn with_source(mut self, source: String) -> Self {
        self.record_data.insert(
            "source".to_string(),
            FieldValue::String(source),
        );
        self
    }

    pub fn with_partition_id(mut self, partition_id: usize) -> Self {
        self.record_data.insert(
            "partition_id".to_string(),
            FieldValue::Integer(partition_id as i64),
        );
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.record_data.insert(
            "batch_size".to_string(),
            FieldValue::Integer(batch_size as i64),
        );
        self
    }

    pub fn build(mut self) -> StreamRecord {
        self.record_data.insert(
            "error".to_string(),
            FieldValue::String(self.error_message),
        );
        StreamRecord::new(self.record_data)
    }
}

pub async fn try_add_dlq_entry(
    dlq: Option<&Arc<DeadLetterQueue>>,
    record: StreamRecord,
    error_msg: String,
    index: usize,
    recoverable: bool,
) -> bool {
    if let Some(dlq) = dlq {
        match catch_unwind(...) {
            Ok(_) => true,
            Err(_) => {
                error!("Failed to add DLQ entry");
                false
            }
        }
    } else {
        false
    }
}
```

**Pros**:
- Simple builder pattern for DLQ records
- Easy to understand and maintain
- Reduces code duplication
- No async complexity

**Cons**:
- Error handling still needs to be in each processor
- Less type safety

---

### Option C: Minimal - Just Document Pattern
Keep current implementation as-is but document the pattern:
- Add comment block explaining the DLQ pattern
- Reference this pattern in each processor
- Keep flexibility for processor-specific needs

**Pros**:
- Minimal code changes
- Maximum flexibility for each processor

**Cons**:
- Code duplication remains
- Pattern consistency depends on developer discipline

---

## TransactionalJobProcessor: No Change Needed

**Current Design is Correct**:
1. TransactionalJobProcessor uses `FailBatch` strategy
2. FailBatch rolls back entire batch on ANY error
3. Failed records are **NOT** written to sink
4. DLQ is intentionally disabled (`with_dlq(false)`)
5. SendToDLQ case is handled as FailBatch (with warning)

**Why No DLQ for Transactional**:
- DLQ is for partial failures (LogAndContinue)
- Transactional mode is all-or-nothing (FailBatch)
- No point storing failed records that were rolled back
- Existing comment in code explains this well (lines 50-54)

---

## Recommendation

### Implement Option B: DLQHelper Module

**Rationale**:
1. Reduces code duplication without over-engineering
2. Improves maintainability with builder pattern
3. Centralizes error handling logic
4. Easier to add new processors later
5. No async trait complexity
6. Keeps each processor's error handling visible

### Implementation Steps:
1. Create `src/velostream/server/processors/dlq_helper.rs`
2. Implement `DLQEntryBuilder` (fluent interface)
3. Implement `try_add_dlq_entry()` helper
4. Refactor SimpleJobProcessor to use builder
5. Refactor PartitionReceiver to use builder
6. No changes to TransactionalJobProcessor (by design)
7. Add tests for DLQEntryBuilder
8. Document pattern in inline comments

### Files to Modify:
- `src/velostream/server/processors/mod.rs` (add dlq_helper module)
- `src/velostream/server/processors/simple.rs` (use builder)
- `src/velostream/server/v2/partition_receiver.rs` (use builder)

### Files to Create:
- `src/velostream/server/processors/dlq_helper.rs` (new helper module)

---

## Notes on AdaptiveJobProcessor

Once AdaptiveJobProcessor implements DLQ handling, it should:
1. Use the same DLQEntryBuilder pattern
2. Define its context fields clearly (similar to others)
3. Implement error handling with catch_unwind
4. Consider which failure strategy it uses (SendToDLQ vs FailBatch)

