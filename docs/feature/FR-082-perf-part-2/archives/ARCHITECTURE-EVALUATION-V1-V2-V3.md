# FR-082 Architecture Evaluation: V1 vs V2 vs V3 (Pure STP)

**Date**: November 8, 2025
**Status**: Analysis & Proposal
**Goal**: Identify optimal architecture for 200K+ rec/sec target

---

## Executive Summary

Three architectural approaches evaluated for streaming SQL execution:

| Aspect | V1 | V2 (Current) | V3 (Pure STP) |
|--------|-----|-------------|--------------|
| **Pattern** | Single queue + coordinator | Simplified routing | Independent pipelines |
| **Parallelism** | 0 (sequential) | Inline (no async tasks) | True (tokio::spawn per partition) |
| **Throughput** | 20K rec/sec | 110K rec/sec (5.5x) | Target: 200K+ (10x) |
| **Latency** | High (batch bound) | Medium (inline processing) | Low (independent streams) |
| **Channels** | Main MPSC | None | Per-partition only |
| **Locking** | Per-record Arc<Mutex> | Batch-level Arc<RwLock> | Partition-local only |
| **Lock Contention** | Severe | Moderate | Minimal |

---

## Architecture 1: V1 (Current Production - Single Queue)

### Design
```
DataReader → MPSC Channel → Main Coordinator
                                    ↓
                          QueryProcessor (locked)
                                    ↓
                          DataWriter
```

### Characteristics
- **Single MPSC channel** for all records
- **Per-record Arc<Mutex> lock** on coordinator state
- **Sequential processing** through one executor
- **High contention** on Mutex

### Performance
```
Throughput: ~20K rec/sec (5000 records in 250ms)
Overhead: 95%+ (coordination bound, not query-bound)
Bottleneck: Arc<Mutex> contention in coordinator
```

### Issues
1. ✗ Mutex lock acquired per-record
2. ✗ All records serialize through single lock
3. ✗ No parallelism at all
4. ✗ Cache misses due to lock bouncing
5. ✗ Cannot scale beyond single core

---

## Architecture 2: V2 (Current Simplified - Inline Routing)

### Design
```
DataReader → Batch Read
                ↓
           Inline Processing Loop
           - Read batch
           - ProcessorContext.new()
           - For each record:
             * QueryProcessor::process_query()
             * Write output
           - Update engine state (RwLock write)
```

### Characteristics
- **Single reader/writer** (standard pattern)
- **Inline record processing** (no async tasks spawned)
- **Batch-level Arc<RwLock>** (per-batch state updates)
- **Sequential within batch**, parallel across batches (if multiple threads read concurrently)

### Performance
```
Scenario 0 (Pure SELECT):
  V2@4p: 109,876 rec/sec (45.51ms for 5000 records)
  Speedup: 5.49x vs V1
  Per-core efficiency: 137.2%

Projected across scenarios: 5-6x improvement
```

### Issues
1. ✗ Still sequential record processing
2. ✗ Single batch blocks all output
3. ✗ RwLock contention on engine state
4. ✗ No true parallelism for independent partitions
5. ✗ Limited by single processing thread

### Advantages
1. ✓ No deadlocks (no channels)
2. ✓ Simple implementation
3. ✓ Fast compilation
4. ✓ Proper resource cleanup
5. ✓ Works with existing tests

---

## Architecture 3: V3 (Proposed Pure STP - Independent Pipelines)

### Design: True Single-Threaded Pipeline (STP)

```
Partition 0:
  Reader 0 → Process locally → Write 0

Partition 1:
  Reader 1 → Process locally → Write 1

...

Partition N:
  Reader N → Process locally → Write N

Shared state: Arc<RwLock<StreamExecutionEngine>> only
```

### Key Principles

**1. No Main Thread Bottleneck**
```rust
// Each partition spawned independently
for partition_id in 0..num_partitions {
    tokio::spawn(async move {
        let mut reader = readers[partition_id];
        let mut writer = writers[partition_id];

        loop {
            // Read batch (independent)
            let batch = reader.read().await?;

            // Process records (independent, no channels)
            for record in batch {
                let result = QueryProcessor::process_query(&query, &record, &mut context)?;
                writer.write(result).await?;
            }

            // No synchronization with other partitions
        }
    });
}
```

**2. Per-Partition State Isolation**
- Each partition has own ProcessorContext
- No cross-partition locks during processing
- Only shared state: execution engine (for aggregations)

**3. Zero Inter-Partition Channels**
- Records NOT routed between partitions
- Records NOT collected in main coordinator
- Each partition: read → process → write independently

**4. Load Distribution**
- Records distributed at source (DataReader)
- OR use GROUP BY key hash for consistent routing
- Each partition processes its own records without channels

### Performance Target

```
Expected Scaling:
  V2@1p: ~76K rec/sec (no parallelism overhead)
  V2@4p: ~304K rec/sec (4x linear scaling)
  V2@8p: ~600K+ rec/sec (8x linear scaling)

Why better than V2 current:
  - No inline sequential loop (replaced by N independent loops)
  - Each partition processes independently
  - No channel communication overhead
  - Better CPU cache utilization (smaller working sets)
  - Linear scaling up to number of cores
```

---

## Comparison: Record Flow and Bottlenecks

### V1: Single Queue Pattern
```
Record 1 ──→ MPSC Enqueue
Record 2 ──→ MPSC Enqueue
...        (all blocked if channel full)
Record N ──→ MPSC Enqueue
                    ↓
            Main Coordinator (Arc<Mutex> lock)
                    ↓
            Process Record N-1
            Process Record N
            ...
            (sequential, one at a time)
                    ↓
            DataWriter (may also be locked)

BOTTLENECK: Single Mutex lock serializes ALL processing
```

### V2 Current: Inline Batch Processing
```
DataReader.read() → [Record 1, Record 2, ..., Record 1000]
                    ↓
            Batch Processing Loop
            for record in batch {
              ProcessorContext.process(record)
            }
                    ↓
            RwLock::write() ← Update engine state
                    ↓
            DataWriter outputs

BOTTLENECK: Sequential processing within batch
            RwLock write on engine state
```

### V3 Pure STP: Independent Partitions
```
Partition 0:            Partition 1:            Partition N:
Reader 0               Reader 1                 Reader N
  ↓                      ↓                        ↓
Read batch 0         Read batch 1             Read batch N
  ↓                      ↓                        ↓
Process records      Process records          Process records
  ↓                      ↓                        ↓
Write outputs        Write outputs            Write outputs
  ↓                      ↓                        ↓
(no channels, no inter-partition sync)

Shared: Arc<RwLock<Engine>> for aggregations only
        (read-heavy, minimal contention)

BOTTLENECK: None (independent pipelines)
            Only RwLock contention for aggregation state updates
```

---

## Implementation Approach: V3 Pure STP

### Step 1: Remove Channel Routing

**Current V2** (simplified inline):
```rust
async fn process_job(
    mut reader,
    mut writer,
    engine,
    query,
) {
    loop {
        let batch = reader.read().await?;  // Single reader
        // Process batch
        for record in batch {
            QueryProcessor::process_query(...)?;
        }
        writer.write(...).await?;  // Single writer
    }
}
```

**Target V3** (pure STP):
```rust
// Instead of single reader/writer, expect N readers/writers
// OR use process_multi_job() which already implements this!

pub async fn process_multi_job(
    readers: HashMap<String, Box<dyn DataReader>>,  // N readers
    writers: HashMap<String, Box<dyn DataWriter>>,  // N writers
    engine,
    query,
) {
    // Already implemented!
    // Creates independent partition pipelines
    // Each spawned with tokio::spawn
    // No coordination channels
}
```

### Step 2: Optimize Engine State Access

**Current approach** (batch-level locks):
```rust
for record in batch {
    // Read lock for state retrieval
    let state = engine.read().await;

    // Process record
    let result = QueryProcessor::process_query(&query, &record, &state)?;

    // Write lock for state update (serialized)
}
// Write lock here too
```

**Optimized V3 approach** (amortized locking):
```rust
// Single batch start
let state = engine.read().await;
let (group_states, window_states) = (state.get_group_states(), state.get_window_states());
drop(state);  // Release read lock early

// Process entire batch without locks
for record in batch {
    QueryProcessor::process_query(&query, &record, &mut context)?;
}

// Single batch end
let mut state = engine.write().await;
state.set_group_states(context.group_by_states);
state.set_window_states(context.persistent_window_states);
```

This reduces lock acquisitions from `batch_size` to just 2 per batch!

### Step 3: Parallel Partition Pipelines

The existing `process_multi_job()` already does this:

```rust
for partition_id in 0..num_partitions {
    let reader = readers[partition_id];
    let writer = writers[partition_id];

    let handle = tokio::spawn(async move {
        // Independent pipeline
        loop {
            let batch = reader.read().await?;
            // Process with no inter-partition sync
            // Write results
        }
    });
}
```

---

## Router and Channel Re-evaluation

### Current Router Pattern (V2)
- Records routed to partitions via MPSC channel
- Main coordinator receives all records, sends to partitions
- **Problem**: Main thread becomes bottleneck

### Pure STP Pattern (V3)
- **No routing channels** between main and partitions
- **Instead**: Each partition reads independently

**Two Options**:

**Option A: Per-Partition Readers**
- DataSource provides N independent readers
- Each partition: `reader.read()` → process → write
- No cross-partition communication
- Best for Kafka where partitions naturally exist

**Option B: Central Reader with Hash Routing**
- Single DataReader, but partition reading logic
- Hash records by GROUP BY key at source
- Route to appropriate partition reader
- Done at read time, not in coordinator

**Option C: Central Reader with Async Channels (Current)**
- Main thread reads all records
- Routes to partition channels
- Partitions read from channels
- **This is what we have now** - has contention

**Recommendation**: Avoid Option C. Prefer A or B for pure STP.

---

## Performance Projections

### V1 (Baseline)
```
Single core:  20,183 rec/sec (5000 records, 247ms)
Overhead:     95% (coordination bound)
Bottleneck:   Arc<Mutex> per record
```

### V2 (Current - Scenario 0)
```
Inline routing (super-linear):  109,876 rec/sec
Speedup vs V1:                  5.49x
Per-core efficiency:            137.2%
Note: Benefits from cache effects in small dataset
```

### V3 (Proposed - Pure STP)
```
Expected per partition:    ~75K rec/sec  (same as V1, minimal overhead)
Expected @8 partitions:    ~600K rec/sec (linear 8x scaling)

Conservative estimate:     5-10x vs V2 (if we lose cache benefits)
Optimistic estimate:       1.5-2x vs V2 (if we gain parallelism benefits)

Target: 200K+ rec/sec on 8 cores
```

### Real-World Scenario Comparison

| Scenario | V1 Baseline | V2 (5.5x) | V3 (8x) | Target |
|----------|-------------|-----------|---------|--------|
| 0: SELECT | 20K | 110K | 160K | 200K |
| 1: ROWS WINDOW | 20K | 110K | 160K | 200K |
| 2: GROUP BY | 23K | 126K | 184K | 200K |
| 3a: TUMBLING | 23K | 126K | 184K | 200K |
| 3b: EMIT CHANGES | 23K | 126K | 184K | 200K |

---

## Latency Considerations

### V1: Per-Record Latency
```
Record latency = Queue wait + Lock acquisition + Processing + Queue write
Typical: 10-50ms per record
Jitter: High (lock contention)
```

### V2: Batch Latency
```
Record latency = Batch wait + Processing + Lock + Write
Typical: 5-20ms per record (batched)
Jitter: Medium (batch boundary effects)
```

### V3: Pure STP Latency
```
Record latency = Processing only (no queue, no locks)
Typical: 1-10μs per record (theoretical, very fast)
Jitter: Very low (independent pipelines)
Trade-off: Slightly higher throughput latency (tail latency might increase)
```

---

## Migration Path: V1 → V2 → V3

### Phase 6.3 ✅ (Completed)
- Implement V2 with simplified inline routing
- Establish baseline: 5.5x speedup
- Proper JobExecutionStats tracking
- Compatible with all scenario tests

### Phase 6.4 (Next)
- Optimize engine state locking (amortized locks)
- Reduce read/write lock acquisitions
- Target: Additional 2x improvement (5.5x → 11x)

### Phase 6.5 (Future)
- Implement pure STP with per-partition pipelines
- Remove inter-partition channels
- Use `process_multi_job()` or variants
- Target: Full 200K+ rec/sec

---

## Recommendation

### Immediate Action
1. **Keep V2 as foundation** (5.5x improvement proven)
2. **Optimize engine state locking** (low effort, 2x gain)
3. **Complete scenario testing** (finalize V2 baseline)

### Next Phase
1. **Implement V3 pure STP** for production deployment
2. **Benchmark latency** (p50, p99 metrics)
3. **Evaluate tradeoffs** (throughput vs latency)

### Key Insight
Pure STP architecture eliminates **all coordination overhead** by making partitions completely independent. Each partition becomes a single-threaded pipeline, but N pipelines run in parallel via tokio::spawn.

**This is the path to 200K+ rec/sec on 8 cores.**

---

## Code Location References

- V2 Implementation: `src/velostream/server/v2/job_processor_v2.rs:110-228`
- V3 Foundation: `src/velostream/server/v2/coordinator.rs:763-910` (process_multi_job)
- Partition Pipeline: `src/velostream/server/v2/coordinator.rs:923-1044`

---

## Questions for Evaluation

1. **Latency vs Throughput**: Do we prioritize extreme throughput (V3) or lower latency (V2)?
2. **Reader Distribution**: Should each partition have separate reader, or use central reader with hashing?
3. **State Consistency**: For GROUP BY aggregations across partitions, how do we handle state updates?
4. **Monitoring**: What metrics should we track (p50, p99 latency vs throughput)?
5. **Backwards Compatibility**: Can we support both V2 and V3 simultaneously during migration?
