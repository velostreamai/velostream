# Phase 6.7: Execute With Record Sync Conversion Architecture

## Executive Summary

This document provides a comprehensive analysis of the architecture needed to convert `execute_with_record()` from async to sync. The conversion involves understanding the channel patterns, output routing, and state management to enable lock-free synchronous batch processing at the partition level.

## Current Architecture (Phase 6.6)

### 1. Output Sender/Receiver Channel Pattern

**Creation Pattern:**
```rust
// Created at engine initialization time (stream_job_server.rs)
let (output_sender, output_receiver) = mpsc::unbounded_channel::<StreamRecord>();
let mut engine = StreamExecutionEngine::new(output_sender);
engine.set_output_receiver(output_receiver);
```

**Key Points:**
- `output_sender`: Cloned and owned by engine
- `output_receiver`: Optional, set separately via `set_output_receiver()`
- Channel is unbounded (no backpressure)
- Created BEFORE engine initialization

### 2. How Output Flows Through Channel

**Within execute_with_record():**

```rust
// In execute_internal(), after processing result
if let Some(result) = result {
    // Send to BOTH message channel and output channel
    self.message_sender.send(ExecutionMessage::QueryResult { ... })?;
    self.output_sender.send(result)?;  // <-- Channel send
}

// For EMIT CHANGES queries in common.rs
let mut temp_receiver = {
    let mut engine_lock = engine.write().await;
    engine_lock.take_output_receiver()  // <-- Takes ownership
};

// Process batch WITHOUT holding lock
for (index, record) in batch.into_iter().enumerate() {
    let mut ctx = processor_context_arc.lock().unwrap();
    match QueryProcessor::process_query(query, &record, &mut ctx) {
        Ok(result) => {
            if let Some(output) = result.record {
                let _ = output_sender.send(output);  // <-- Channel send
            }
        }
        Err(e) => { /* error handling */ }
    }
}

// After batch processing, collect emitted results
if let Some(rx) = &mut temp_receiver {
    while let Ok(emitted_record) = rx.try_recv() {
        output_records.push(Arc::new(emitted_record));
    }
}

// Return receiver to engine
if let Some(rx) = temp_receiver {
    let mut engine_lock = engine.write().await;
    engine_lock.return_output_receiver(rx);
}
```

### 3. Call Sites of execute_with_record()

#### Primary Call Sites:

1. **partition_receiver.rs (Phase 6.6 - Key for Phase 6.7)**
   ```rust
   // In PartitionReceiver::process_batch()
   async fn process_batch(&mut self, batch: &[StreamRecord]) -> Result<usize, SqlError> {
       for record in batch {
           match self
               .execution_engine
               .execute_with_record(&self.query, record)
               .await  // <-- Async call
           {
               Ok(_) => { processed += 1; }
               Err(e) => { /* error handling */ }
           }
       }
       Ok(processed)
   }
   ```

2. **common.rs (process_batch_with_output)**
   - Not directly called; uses QueryProcessor::process_query instead for better performance
   - Comments note that execute_with_record() is used for EMIT CHANGES
   - process_batch_with_output provides 2x+ improvement over direct execute_with_record calls

3. **partition_manager.rs**
   ```rust
   engine.execute_with_record(query, &record).await?;
   ```

4. **bin/sql_batch.rs**
   ```rust
   engine.execute_with_record(parsed_query, &record).await?;
   ```

5. **Tests and Examples** (numerous)
   - Most tests create engine with channel and ignore receiver
   - Used in performance benchmarks

### 4. Output Sender Access Patterns

**Current Mechanisms:**

1. **Direct Sending in execute_internal()**
   ```rust
   self.output_sender.send(result)?;
   ```

2. **Cloned for Batch Processing**
   ```rust
   let output_sender = {
       let engine_lock = engine.read().await;
       engine_lock.get_output_sender_for_batch()  // Clone sender
   };
   // Later, use without lock:
   let _ = output_sender.send(output);
   ```

3. **Try-Receive Pattern (FR-082 Phase 5)**
   ```rust
   pub fn try_receive_output(&mut self) -> Result<StreamRecord, TryRecvError> {
       if let Some(receiver) = &mut self.output_receiver {
           receiver.try_recv()
       } else {
           Err(TryRecvError::Disconnected)
       }
   }
   ```

### 5. Why Channel Pattern Exists

**Purpose:**
- EMIT CHANGES support: Results emit through channel, not return values
- Streaming semantics: One input can produce multiple outputs
- Decoupling: Engine logic separate from result consumption

**Design Trade-offs:**
- Async channel adds overhead
- Lock contention when accessing sender
- Unbounded queue potential memory risk
- Mixed patterns: Some paths use QueryProcessor directly (faster), others use execute_with_record

## Phase 6.7 Conversion Strategy

### Problem Statement

`execute_with_record()` is async but partition receivers need synchronous execution:
- Eliminates async/await overhead (~5-10%)
- Enables lock-free state access
- Reduces architectural indirection
- Target: 2-3x performance improvement when combined with all optimizations

### Key Insight: Two Output Modes

**Mode 1: Direct Return Value (Best for Sync)**
```rust
// What we want to return for non-EMIT-CHANGES queries
fn execute_with_record_sync(
    &mut self,
    query: &StreamingQuery,
    stream_record: &StreamRecord,
) -> Result<Option<StreamRecord>, SqlError> {
    // Process query
    // Return result directly
    Ok(Some(result_record))
}
```

**Mode 2: Channel Emission (For EMIT CHANGES)**
```rust
// Keep async for EMIT CHANGES-only path
async fn execute_with_record_emit(
    &mut self,
    query: &StreamingQuery,
    stream_record: &StreamRecord,
) -> Result<(), SqlError> {
    // Process query
    // Send results through channel
    self.output_sender.send(result)?;
    Ok(())
}
```

### Architecture Changes Needed

#### 1. New Sync Method

**Location:** `src/velostream/sql/execution/engine.rs`

```rust
/// Synchronous version of execute_with_record for partition-level processing
/// 
/// ## Usage
/// 
/// For non-EMIT-CHANGES queries: Returns result directly
/// For EMIT-CHANGES queries: Use async version (execute_with_record_async)
///
/// ## Returns
/// 
/// - `Ok(Some(record))` - Query produced output
/// - `Ok(None)` - Query executed but produced no output
/// - `Err(SqlError)` - Error during execution
pub fn execute_with_record_sync(
    &mut self,
    query: &StreamingQuery,
    stream_record: &StreamRecord,
) -> Result<Option<StreamRecord>, SqlError> {
    // Implementation details follow
}
```

#### 2. QueryProcessor Direct Use (Already Works)

The key insight is that `QueryProcessor::process_query()` is ALREADY synchronous and returns `Result<QueryProcessorResult>` with optional record.

```rust
// This is synchronous and available today
pub fn process_query(
    query: &StreamingQuery,
    record: &StreamRecord,
    context: &mut ProcessorContext,
) -> Result<QueryProcessorResult, SqlError>
```

**Benefits:**
- No channel overhead
- Direct return value
- State in Arc<Mutex<ProcessorContext>>
- Already used by process_batch_with_output() for 2x improvement

#### 3. State Management Pattern

**Current Pattern (Phase 6.5+):**
```rust
// In process_batch_with_output, state lives in QueryExecution
let processor_context_arc = {
    let mut engine_lock = engine.write().await;
    if let Some(context_arc) = engine_lock.ensure_query_execution(&query) {
        context_arc
    } else {
        return error;
    }
};

// Process records while holding Mutex lock only during processing
for record in batch {
    let mut ctx = processor_context_arc.lock().unwrap();
    let result = QueryProcessor::process_query(query, &record, &mut ctx)?;
    // Use result...
}
```

**For Sync Version:**
- Same pattern works! QueryProcessor is already sync
- Just call directly from sync code
- No async/await overhead

### Implementation Plan

#### Phase 6.7a: Add sync_execute_with_record()

```rust
pub fn execute_with_record_sync(
    &mut self,
    query: &StreamingQuery,
    stream_record: &StreamRecord,
) -> Result<Option<StreamRecord>, SqlError> {
    // Step 1: Clone record if needed (windowed queries only)
    let record_to_process = if let StreamingQuery::Select {
        window: Some(_), ..
    } = query {
        // Windowed - may need modification
        stream_record.clone()
    } else {
        // Non-windowed - use as-is
        stream_record.clone()
    };

    // Step 2: Generate consistent query ID
    let query_id = self.generate_query_id(query);

    // Step 3: Lazy initialize QueryExecution if needed
    if !self.active_queries.contains_key(&query_id) {
        self.init_query_execution(query.clone());
    }

    // Step 4: Get Arc<Mutex<ProcessorContext>>
    let processor_context_arc = self
        .get_query_execution(&query_id)
        .map(|execution| Arc::clone(&execution.processor_context))
        .ok_or_else(|| SqlError::ExecutionError {
            message: format!("Failed to initialize query execution"),
            query: None,
        })?;

    // Step 5: Set streaming config on context
    {
        let mut ctx = processor_context_arc.lock().unwrap();
        ctx.streaming_config = Some(self.config.clone());
    }

    // Step 6: Process using QueryProcessor (synchronous!)
    let mut ctx = processor_context_arc.lock().unwrap();
    let result = QueryProcessor::process_query(query, &record_to_process, &mut ctx)?;

    // Step 7: Return result directly (no channel!)
    Ok(result.record)
}
```

#### Phase 6.7b: Update PartitionReceiver

**File:** `src/velostream/server/v2/partition_receiver.rs`

```rust
// Change from:
async fn process_batch(&mut self, batch: &[StreamRecord]) -> Result<usize, SqlError> {
    for record in batch {
        match self
            .execution_engine
            .execute_with_record(&self.query, record)
            .await  // <-- REMOVE .await
        {
            Ok(_) => { processed += 1; }
            Err(e) => { /* ... */ }
        }
    }
    Ok(processed)
}

// To:
fn process_batch(&mut self, batch: &[StreamRecord]) -> Result<usize, SqlError> {
    for record in batch {
        match self
            .execution_engine
            .execute_with_record_sync(&self.query, record)
            // <-- Sync call, no .await
        {
            Ok(_) => { processed += 1; }
            Err(e) => { /* ... */ }
        }
    }
    Ok(processed)
}
```

Then change `run()` from async to blocking:

```rust
// Change from:
pub async fn run(&mut self) -> Result<(), SqlError> {
    loop {
        match self.receiver.recv().await {  // <-- Await on channel
            Some(batch) => {
                self.process_batch(&batch).await?;  // <-- Await on process
            }
            None => { break; }
        }
    }
    Ok(())
}

// To (using tokio::task::block_in_place or sync channels):
pub async fn run(&mut self) -> Result<(), SqlError> {
    // Option 1: Use sync channel at this boundary
    // Option 2: Use tokio::task::block_in_place inside async
    loop {
        match self.receiver.recv().await {
            Some(batch) => {
                // Use block_in_place to run sync code in async context
                self.process_batch(&batch)?;  // <-- Sync call
            }
            None => { break; }
        }
    }
    Ok(())
}
```

#### Phase 6.7c: Handle EMIT CHANGES Path

**For EMIT CHANGES queries**, continue using the async path or convert to:

```rust
pub fn execute_with_record_emit_changes(
    &mut self,
    query: &StreamingQuery,
    stream_record: &StreamRecord,
) -> Result<(), SqlError> {
    // Same as Phase 6.7a but send results through channel
    // instead of returning
    
    let record_to_process = stream_record.clone();
    let query_id = self.generate_query_id(query);
    
    if !self.active_queries.contains_key(&query_id) {
        self.init_query_execution(query.clone());
    }

    let processor_context_arc = self
        .get_query_execution(&query_id)
        .map(|execution| Arc::clone(&execution.processor_context))
        .ok_or_else(|| SqlError::ExecutionError { /* ... */ })?;

    {
        let mut ctx = processor_context_arc.lock().unwrap();
        ctx.streaming_config = Some(self.config.clone());
    }

    // Process and emit through channel
    let mut ctx = processor_context_arc.lock().unwrap();
    let result = QueryProcessor::process_query(query, &record_to_process, &mut ctx)?;
    
    if let Some(output) = result.record {
        self.output_sender.send(output)?;
    }
    
    Ok(())
}
```

### Channel Elimination Benefits

**For Non-EMIT-CHANGES (Common Case):**
- Remove channel send overhead
- Remove channel allocation/deallocation
- Direct return to caller
- No receiver setup needed
- ~10-20% improvement expected

**For EMIT-CHANGES (Optional Path):**
- Can keep async version if needed
- Or use sync with explicit channel sends
- Isolated to specific query types

## Key Call Sites to Update

### Must Update:
1. **partition_receiver.rs** - Primary target for Phase 6.7
   - Change process_batch from async to sync
   - Use execute_with_record_sync
   - May need to adjust run() if receiver is async

2. **common.rs** - Already optimized
   - Can continue using QueryProcessor directly
   - Or update to use execute_with_record_sync
   - Already has 2x improvement via direct processor use

### Can Keep As-Is:
1. **sql_batch.rs** - Less critical path
2. **Tests** - Lower priority, can add both methods
3. **Examples** - Optional to update

## Output Receiver Lifecycle

**Current Pattern (With Channel):**
```
1. Create channel: (output_sender, output_receiver)
2. Create engine with output_sender
3. set_output_receiver(receiver) - optional, for EMIT CHANGES
4. Optionally: take_output_receiver() for draining
5. process_batch_with_output() collects results from channel
```

**New Pattern (Sync Return):**
```
1. Create engine (no output_sender/receiver needed for most cases)
2. Call execute_with_record_sync() -> Option<StreamRecord>
3. Direct result handling, no channel drain
4. For EMIT CHANGES only: use async path or explicit channel sends
```

## Performance Impact Analysis

**Async/Await Overhead (Current):**
- State machine creation: ~5-10%
- Executor polling: ~2-5%
- Channel operations: ~5-10% (send/recv)
- **Total: 12-25% overhead**

**Sync Version Benefits:**
- Direct function calls
- No state machine
- No executor involvement
- Direct return values
- Lock-free state access (Arc<Mutex> only during critical section)

**Expected Performance Improvement:**
- Phase 6.7 alone: ~15% (removing execute_with_record async)
- Combined with Phase 6.6+ optimizations: 2-3x total

## Architecture Diagram

```
┌─────────────────────────────────────────────────┐
│ PartitionReceiver (Phase 6.6)                  │
│ - Owns StreamExecutionEngine directly          │
│ - Owns ProcessorContext                        │
│ - Receives batches via mpsc channel            │
└──────────────┬──────────────────────────────────┘
               │
               │ Loop: receiver.recv().await
               ▼
        ┌──────────────────────┐
        │ process_batch() [NEW]│ ← Sync method
        │ (not async)          │
        └──────────┬───────────┘
                   │
                   │ For each record
                   ▼
        ┌──────────────────────────────┐
        │execute_with_record_sync() [NEW] ← Direct return
        │ Returns Option<StreamRecord>  │
        └──────────┬───────────────────┘
                   │
                   ▼
        ┌──────────────────────────────┐
        │ QueryProcessor::process_query│ (sync!)
        │ Works with Arc<Mutex<Ctx>>  │
        └──────────┬───────────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │ Return Option<StreamRecord>
        │ (NO CHANNEL!)
        └──────────────────────────┘
```

## Backward Compatibility

**Maintaining Both Methods:**

```rust
// Old async method (keep for compatibility)
pub async fn execute_with_record(
    &mut self,
    query: &StreamingQuery,
    stream_record: &StreamRecord,
) -> Result<(), SqlError> {
    // Call sync version and handle via channel
    if let Some(result) = self.execute_with_record_sync(query, stream_record)? {
        self.output_sender.send(result)?;
    }
    Ok(())
}

// New sync method (primary path)
pub fn execute_with_record_sync(
    &mut self,
    query: &StreamingQuery,
    stream_record: &StreamRecord,
) -> Result<Option<StreamRecord>, SqlError> {
    // Implementation
}
```

**Benefits:**
- Existing code continues to work
- New code uses faster sync path
- Gradual migration possible
- Tests can use either method

## Testing Strategy

**Unit Tests:**
1. Test execute_with_record_sync() directly
2. Compare results with async version
3. Verify state management (Arc<Mutex> works correctly)

**Integration Tests:**
1. PartitionReceiver with sync batches
2. GROUP BY and WINDOW state accumulation
3. EMIT CHANGES special handling

**Performance Benchmarks:**
1. Baseline: current execute_with_record
2. Sync version: execute_with_record_sync
3. end-to-end: PartitionReceiver with batches
4. Target: 15% improvement for execute, 2-3x with all optimizations

## Summary: Critical Insights for Phase 6.7

1. **QueryProcessor is Already Sync** - The core processing logic is synchronous, async is just in the wrapper

2. **State in Arc<Mutex> Works Fine Synchronously** - Lock held only during processing, not across await points

3. **Two Output Patterns Needed**:
   - Non-EMIT-CHANGES: Direct return value (best for sync)
   - EMIT-CHANGES: Channel emission (keep optional async path)

4. **PartitionReceiver is the Primary Beneficiary** - It owns engine directly, so can use sync methods

5. **process_batch_with_output Already Provides 2x Improvement** - By using QueryProcessor directly instead of execute_with_record

6. **No Major Architectural Changes Needed** - State management, query execution, processor context all work fine synchronously

7. **Backward Compatibility Easy** - Keep async method, have it call sync internally

## References

- `src/velostream/sql/execution/engine.rs` - execute_with_record, output_sender handling
- `src/velostream/server/v2/partition_receiver.rs` - Primary call site (Phase 6.6)
- `src/velostream/server/processors/common.rs` - process_batch_with_output (already optimized)
- `src/velostream/sql/execution/processors/mod.rs` - QueryProcessor (sync!)

