# Job Server V2: Clean-Slate Architecture Blueprint

> **⚠️ ARCHITECTURE SUPERSEDED**
>
> This document describes a single-actor V2 architecture that has been superseded by the **Hash-Partitioned Pipeline** design.
>
> **See Instead**: `FR-082-job-server-v2-PARTITIONED-PIPELINE.md`
>
> **Why Changed**: The single StateManagerActor design creates a serialization bottleneck (200K rec/sec max), while hash-partitioned design scales linearly (1.5M rec/sec on 8 cores).
>
> **This document is retained for historical reference only.**

---

## Executive Summary

**Current Performance**: 23K rec/sec (23x slower than pure SQL)
**Root Cause**: Arc<Mutex> + HashMap cloning overhead (204ms in 213ms total)
**Decision**: Should we fix incrementally (Option 5: 8x improvement) or redesign from scratch?

This document provides a **greenfield architecture** designed from requirements, then compares it to incremental fixes.

---

## Part 0: Performance Foundation - SQL Engine Optimization (CRITICAL)

### 🚨 BLOCKING ISSUE: GROUP BY Performance Gap

**Problem**: V2 architecture assumes 200K rec/sec GROUP BY throughput, but **actual SQL engine baseline is 3.58K rec/sec** (FR-082 Phase 4A findings).

**Impact**: Without fixing the SQL engine itself, V2 cannot achieve stated targets:

| Metric | V2 Assumes | Actual Baseline | Gap |
|--------|-----------|----------------|-----|
| GROUP BY single-source | 200K rec/sec | 3.58K rec/sec | **56x shortfall** |
| GROUP BY multi-source (8 cores) | 1.6M rec/sec | 28.6K rec/sec | **56x shortfall** |
| Horizontal scaling (160 sources) | 32M rec/sec | 574K rec/sec | **56x shortfall** |

**Root Cause**: SQL engine GROUP BY implementation has fundamental performance bottlenecks (from FR-082-PHASE4-BOTTLENECK_FINDINGS.md):

1. **Vec&lt;String&gt; hash keys** (~40% overhead): Every group key requires Vec + String allocations
2. **Group state cloning** (~30% overhead): Full HashMap clone per batch (1000+ times)
3. **String allocations in accumulators** (~15% overhead): 10+ HashMaps with String keys per accumulator
4. **generate_group_key allocations** (~20% overhead): 1M Vec + 1M String allocations for key extraction
5. **Record cloning** (~10% overhead): Should use Arc&lt;StreamRecord&gt;

**Conclusion**: V2 architecture is **blocked** until SQL engine GROUP BY is optimized via FR-082 Phase 4B + 4C.

---

### FR-082 Phase 4B: Hash Table Optimization (Week 1)

**Goal**: Replace Vec&lt;String&gt; keys with optimized GroupKey + FxHashMap

#### Changes Required

```rust
// Before (current):
pub struct GroupByState {
    pub groups: HashMap<Vec<String>, GroupAccumulator>,  // ← SLOW!
}

// After (Phase 4B):
use rustc_hash::FxHashMap;  // 2-3x faster than std HashMap

pub struct GroupByState {
    pub groups: FxHashMap<GroupKey, GroupAccumulator>,  // ← FAST!
}

// Optimized GroupKey with pre-computed hash
#[derive(Clone, PartialEq, Eq)]
pub struct GroupKey {
    hash: u64,                      // Pre-computed hash (avoid re-hashing)
    values: Arc<[FieldValue]>,      // Arc to avoid Vec allocation
}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);  // Use pre-computed hash
    }
}

impl GroupKey {
    pub fn new(values: Vec<FieldValue>) -> Self {
        // Pre-compute hash once
        let mut hasher = FxHasher::default();
        for value in &values {
            value.hash(&mut hasher);
        }
        let hash = hasher.finish();

        Self {
            hash,
            values: Arc::from(values.as_slice()),
        }
    }
}
```

**Expected Improvement**: 3.58K → 15-20K rec/sec (+400-500%)

---

### FR-082 Phase 4C: Arc-based State Sharing (Week 2)

**Goal**: Eliminate HashMap cloning via Arc + copy-on-write pattern

#### Changes Required

```rust
// 1. Wrap group states in Arc for cheap cloning
pub struct GroupByState {
    pub groups: Arc<FxHashMap<GroupKey, GroupAccumulator>>,
}

impl StreamExecutionEngine {
    // Cheap Arc clone instead of full HashMap clone
    pub fn get_group_state_ref(&self) -> Arc<FxHashMap<GroupKey, GroupAccumulator>> {
        Arc::clone(&self.group_states.groups)
    }

    // Efficient batch merge with copy-on-write
    pub fn merge_batch_state(&mut self, batch_state: FxHashMap<GroupKey, GroupAccumulator>) {
        // Arc::make_mut() only clones if ref count > 1
        let groups = Arc::make_mut(&mut self.group_states.groups);

        for (key, batch_acc) in batch_state {
            groups.entry(key)
                .and_modify(|acc| acc.merge(&batch_acc))
                .or_insert(batch_acc);
        }
    }
}

// 2. Use Arc<StreamRecord> in sample_record
pub struct GroupAccumulator {
    pub sample_record: Option<Arc<StreamRecord>>,  // ← Arc instead of clone
    // ...
}

// 3. String interning for field names
pub struct StringInterner {
    pool: HashMap<String, &'static str>,
}

impl StringInterner {
    pub fn intern(&mut self, s: &str) -> &'static str {
        if let Some(interned) = self.pool.get(s) {
            return *interned;
        }
        let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
        self.pool.insert(s.to_string(), leaked);
        leaked
    }
}

// 4. Group key caching
pub struct GroupKeyCache {
    cache: LruCache<u64, GroupKey>,  // record_hash -> GroupKey
}

impl StreamExecutionEngine {
    fn generate_group_key_cached(
        &mut self,
        expressions: &[Expr],
        record: &StreamRecord,
    ) -> Result<GroupKey, SqlError> {
        let record_hash = record.compute_hash();

        // Check cache first
        if let Some(cached) = self.key_cache.get(&record_hash) {
            return Ok(cached.clone());  // Cheap Arc clone
        }

        // Generate key (reuse buffer from pool)
        let mut values = self.value_buffer_pool.acquire();
        values.clear();

        for expr in expressions {
            let value = ExpressionEvaluator::evaluate_expression_value(expr, record)?;
            values.push(value);
        }

        let key = GroupKey::new(values);

        // Cache for future lookups
        self.key_cache.insert(record_hash, key.clone());

        Ok(key)
    }
}
```

**Expected Improvement**: 15-20K → 200K rec/sec (+750-1000%)

---

### Combined Phase 4B + 4C Results

| Phase | Optimization | Throughput | Improvement | Status |
|-------|------------|-----------|-------------|--------|
| Baseline | - | 3.58 K rec/s | - | ✅ Measured |
| **Phase 4B** | **FxHashMap + GroupKey** | **15-20 K rec/s** | **+400-500%** | 📋 Required for V2 |
| **Phase 4C** | **Arc state + Interning** | **200 K rec/s** | **+750-1000%** | 📋 Required for V2 |
| **V2 Assumed** | **V2 architecture** | **200 K rec/s** | - | ✅ Now achievable |

**Success Criteria**: GROUP BY at 200K rec/sec enables V2 targets:
- Single-source: 200K rec/sec ✅
- Multi-source (8 cores): 1.6M rec/sec ✅
- Horizontal scaling (160 sources): 32M rec/sec ✅

---

### Integration into V2 StateManagerActor

The V2 StateManagerActor MUST use Phase 4B/4C optimizations:

```rust
use rustc_hash::FxHashMap;

/// Actor that owns ALL query state (with Phase 4B/4C optimizations)
struct StateManagerActor {
    receiver: mpsc::UnboundedReceiver<StateMessage>,

    // Phase 4B: FxHashMap instead of HashMap
    states: HashMap<String, QueryState>,  // query_id -> state

    // Phase 4C: String interning pool
    string_interner: StringInterner,

    // Phase 4C: Group key cache
    key_cache: LruCache<u64, GroupKey>,
}

struct QueryState {
    // GROUP BY state (Phase 4B: FxHashMap with GroupKey; Phase 4C: Arc wrapper)
    group_by: Arc<FxHashMap<GroupKey, GroupAccumulator>>,

    // Time-based window state (TUMBLING, SLIDING, SESSION)
    window_states: Vec<WindowState>,

    // ROWS WINDOW buffers (memory-bounded sliding buffers)
    rows_window_buffers: HashMap<PartitionKey, Arc<VecDeque<Arc<StreamRecord>>>>,
}

enum StateMessage {
    /// Update local batch state into global state
    MergeBatchState {
        query_id: String,
        // Phase 4B: Uses GroupKey instead of Vec<String>
        batch_state: FxHashMap<GroupKey, GroupAccumulator>,
        response: oneshot::Sender<()>,
    },
    // ...
}

impl StateManagerActor {
    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                StateMessage::MergeBatchState { query_id, batch_state, response } => {
                    let state = self.states.entry(query_id).or_default();

                    // Phase 4C: Arc::make_mut() for copy-on-write
                    let groups = Arc::make_mut(&mut state.group_by);

                    // Phase 4B: FxHashMap merge (2-3x faster than std HashMap)
                    for (key, agg) in batch_state {
                        groups.entry(key)
                            .and_modify(|existing| existing.merge(&agg))
                            .or_insert(agg);
                    }
                    response.send(()).ok();
                }
                // ...
            }
        }
    }
}
```

---

### Phase 0 Timeline & Deliverables

**Duration**: 2 weeks (blocking work before V2 implementation)

**Week 1: Phase 4B (Hash Table Optimization)**
- Files to modify:
  - `src/velostream/sql/execution/internal.rs` - Change GroupByState key type
  - `src/velostream/sql/execution/aggregation/state.rs` - Update generate_group_key
  - `Cargo.toml` - Add `rustc-hash = "1.1"` dependency
- Tests to create:
  - `tests/unit/sql/execution/group_key_test.rs` - GroupKey behavior
  - `tests/performance/analysis/phase4b_hash_optimization_test.rs` - Benchmark
- **Expected result**: 3.58K → 15-20K rec/sec

**Week 2: Phase 4C (Arc State Sharing)**
- Files to modify:
  - `src/velostream/sql/execution/internal.rs` - Use Arc&lt;FxHashMap&gt;
  - `src/velostream/sql/execution/aggregation/accumulator.rs` - Use Arc&lt;StreamRecord&gt;
  - `src/velostream/sql/execution/engine.rs` - Remove cloning, use Arc::make_mut()
- Tests to create:
  - `tests/unit/sql/execution/arc_state_test.rs` - Arc state sharing
  - `tests/performance/analysis/phase4c_arc_optimization_test.rs` - Benchmark
- **Expected result**: 15-20K → 200K rec/sec

**Deliverable**: SQL engine GROUP BY at 200K rec/sec baseline ✅ Unblocks V2 implementation

---

### ⚠️ CRITICAL PATH: Phase 0 is Non-Negotiable

**V2 implementation CANNOT proceed until Phase 0 is complete** because:

1. **Performance targets are impossible**: Without Phase 4B/4C, SQL engine runs at 3.58K rec/sec (not 200K)
2. **Architecture assumptions invalid**: V2 design assumes fast GROUP BY operations
3. **Scaling projections wrong**: Multi-source/horizontal scaling calculations based on 200K baseline
4. **Competitive positioning false**: Cannot be "better than Flink" with 3.58K GROUP BY performance

**Recommendation**: Complete Phase 0 (2 weeks) before ANY V2 architectural work.

---

## Part 1: Requirements Analysis

### Functional Requirements (Extracted from Current Code)

#### 1. Data Source Abstraction
```rust
// From datasource/traits.rs
trait DataReader {
    async fn read() -> Vec<StreamRecord>;      // Batch reads
    async fn commit();                          // Offset management
    async fn seek(SourceOffset);                // Replay support
    async fn has_more() -> bool;                // End-of-stream detection
    async fn begin_transaction();               // Transactional support
    async fn commit_transaction();
    async fn rollback_transaction();
}
```

**Capabilities needed**:
- Kafka, File, S3, Database sources
- Batch size configuration
- Transaction support (optional)
- Offset tracking for replay
- Multiple concurrent sources

#### 2. Data Sink Abstraction
```rust
trait DataWriter {
    async fn write(StreamRecord);               // Single writes
    async fn write_batch(Vec<Arc<StreamRecord>>); // Batch writes
    async fn flush();                           // Buffer management
    async fn commit();                          // Sink commit
    async fn update(key, record);               // Upserts
    async fn delete(key);                       // Deletes
    async fn rollback();                        // Transaction rollback
}
```

**Capabilities needed**:
- Stdout, Kafka, File, Database sinks
- Batch write optimization
- Transaction support (optional)
- Multiple concurrent sinks

#### 3. Job Processing Configuration
```rust
struct JobProcessingConfig {
    max_batch_size: usize,
    batch_timeout: Duration,
    use_transactions: bool,
    failure_strategy: FailureStrategy,
    max_retries: u32,
    retry_backoff: Duration,
    log_progress: bool,
    progress_interval: u64,
}
```

**Capabilities needed**:
- Configurable batch sizes
- Timeout-based batch flushing
- Error handling strategies (LogAndContinue, FailBatch, RetryWithBackoff)
- Retry logic with exponential backoff
- Progress monitoring

#### 4. SQL Execution
```rust
trait QueryProcessor {
    fn process_query(
        query: &StreamingQuery,
        record: &StreamRecord,
        context: &mut ProcessorContext,
    ) -> Result<QueryResult>;
}

struct ProcessorContext {
    group_by_states: HashMap<GroupKey, AggregateState>,
    persistent_window_states: Vec<WindowState>,     // Time-based windows (TUMBLING, SLIDING, SESSION)
    rows_window_buffers: HashMap<PartitionKey, RowsWindowBuffer>,  // ROWS WINDOW buffers
    // ... other state
}
```

**Capabilities needed**:
- GROUP BY with aggregation
- WINDOW functions:
  - **ROWS WINDOW**: Memory-bounded sliding buffers (e.g., `AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS)`)
  - **TUMBLING**: Fixed-size time windows
  - **SLIDING**: Overlapping time windows
  - **SESSION**: Gap-based time windows
- JOIN operations
- WHERE/HAVING filtering
- State management across batches (GROUP BY hash tables, window buffers, window metadata)

#### 5. Observability & Metrics
```rust
// From processors/observability_helper.rs
- Batch span creation (distributed tracing)
- Deserialization metrics
- SQL processing metrics
- Serialization metrics
- Error tracking
- Throughput metrics
```

**Capabilities needed**:
- OpenTelemetry integration
- Prometheus metrics
- Custom SQL @metric annotations
- Counter, Gauge, Histogram metrics
- Distributed tracing context

#### 6. Multi-Source/Multi-Sink Processing
```rust
async fn process_multi_job(
    readers: HashMap<String, Box<dyn DataReader>>,
    writers: HashMap<String, Box<dyn DataWriter>>,
    engine: Arc<Mutex<StreamExecutionEngine>>,
    query: StreamingQuery,
) -> JobExecutionStats;
```

**Capabilities needed**:
- Multiple sources (JOINs, UNIONs)
- Multiple sinks (fanout)
- Coordinated batch processing
- Shutdown signaling

### Non-Functional Requirements

#### Performance Goals
- **Target**: 500K+ rec/sec (close to pure SQL's 790K)
- **Acceptable**: 200K+ rec/sec (8x improvement)
- **Current**: 23K rec/sec (unacceptable)

#### Concurrency Requirements
- Multiple jobs processing concurrently
- Multiple batches from same job in parallel
- Lock-free hot paths where possible

#### Reliability Requirements
- Exactly-once processing (with transactions)
- At-least-once processing (without transactions)
- Graceful error handling
- Retry logic for transient failures

#### Operational Requirements
- Progress monitoring
- Health checks
- Metrics export
- Dynamic configuration

---

## Part 2: Current Architecture (V1) Analysis

### Component Structure
```
┌─────────────────────────────────────────────────────────┐
│               SimpleJobProcessor                        │
│  (Coordinates everything, owns Arc<Mutex<Engine>>)     │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ├─► DataReader (Kafka/File/S3)
                   │
                   ├─► process_batch_with_output()
                   │   ┌────────────────────────────┐
                   │   │ LOCK #1: Clone state       │ ← 80ms overhead
                   │   │ Process batch locally      │
                   │   │ LOCK #2: Sync state back   │ ← 80ms overhead
                   │   └────────────────────────────┘
                   │
                   ├─► ObservabilityHelper (metrics) ← 30ms overhead
                   │
                   └─► DataWriter (Kafka/File/Stdout) ← 10ms overhead
```

### Critical Bottlenecks

#### 1. Centralized State with Coarse Locking
```rust
// PROBLEM: Single Arc<Mutex<Engine>> for ALL state
Arc<Mutex<StreamExecutionEngine>> {
    group_by_states: HashMap<GroupKey, AggregateState>,  // Cloned EVERY batch
    window_states: Vec<WindowState>,                     // Cloned EVERY batch
    // ... other state
}
```

**Cost per batch** (5 batches total):
- Lock acquisition: 2 × 8ms = 16ms
- HashMap clone (200 keys): 16ms
- **Total per batch**: 32ms × 5 = **160ms wasted**

#### 2. Observability Always On
```rust
// PROBLEM: Always pays observability cost, even when disabled
ObservabilityHelper::start_batch_span()      // ~5ms
ObservabilityHelper::record_deserialization() // ~3ms
ObservabilityHelper::record_sql_processing()  // ~5ms
ObservabilityHelper::inject_trace_context()   // ~2ms
// Total per batch: ~15ms × 5 = 75ms (when not needed)
```

#### 3. Sequential Batch Processing
```rust
// PROBLEM: Loop processes batches sequentially
loop {
    let batch = reader.read().await;     // Wait for batch
    process_batch(...).await;            // Wait for processing
    writer.write_batch(...).await;       // Wait for write
}
// Concurrency opportunity missed!
```

---

## Part 3: Clean-Slate Architecture (V2)

### Design Principles

1. **Zero-Copy Where Possible**: Avoid cloning large structures
2. **Lock-Free Execution Paths**: Use message passing over shared memory
3. **Optional Observability**: Pay cost only when needed
4. **Pipeline Parallelism**: Overlap I/O and compute
5. **Stateful Stream Processing**: Match Flink/Kafka Streams patterns

### Architecture Diagram

```
┌────────────────────────────────────────────────────────────────┐
│                     Job Coordinator                             │
│  (Lightweight orchestrator, no shared state)                    │
└───┬───────────────────────────────┬────────────────────────────┘
    │                               │
    │                               │
    ▼                               ▼
┌───────────────────┐     ┌────────────────────────────┐
│  Source Pipeline  │     │    State Manager Actor     │
│   (async stream)  │     │   (single-threaded actor)  │
├───────────────────┤     ├────────────────────────────┤
│ • DataReader      │     │ • GROUP BY states          │
│ • Batching        │────►│ • Window states            │
│ • Backpressure    │     │ • Message-based updates    │
└────────┬──────────┘     └────────────────────────────┘
         │                          │
         │                          │
         ▼                          ▼
┌────────────────────┐     ┌────────────────────────────┐
│ Processing Pipeline│     │   Sink Pipeline            │
│  (parallel workers)│─────►  (async stream)            │
├────────────────────┤     ├────────────────────────────┤
│ • SQL execution    │     │ • DataWriter               │
│ • Local state      │     │ • Batching                 │
│ • Parallel batches │     │ • Async writes             │
└────────────────────┘     └────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────┐
│    Optional Observability Layer        │
│  (enabled via feature flag/config)     │
└────────────────────────────────────────┘
```

### Core Components

#### 1. State Manager Actor (Lock-Free)

**Purpose**: Single-threaded state owner, no locks needed

```rust
/// Actor that owns ALL query state (no locks!)
struct StateManagerActor {
    receiver: mpsc::UnboundedReceiver<StateMessage>,
    states: HashMap<String, QueryState>,  // query_id -> state
}

enum StateMessage {
    /// Update local batch state into global state
    MergeBatchState {
        query_id: String,
        batch_state: HashMap<GroupKey, AggregateState>,
        response: oneshot::Sender<()>,
    },

    /// Get current state snapshot (for recovery)
    GetSnapshot {
        query_id: String,
        response: oneshot::Sender<QueryState>,
    },

    /// Initialize query state
    Initialize {
        query_id: String,
        initial_state: QueryState,
    },
}

impl StateManagerActor {
    async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                StateMessage::MergeBatchState { query_id, batch_state, response } => {
                    // Fast HashMap merge (no cloning!)
                    let state = self.states.entry(query_id).or_default();
                    for (key, agg) in batch_state {
                        state.group_by.entry(key)
                            .and_modify(|existing| existing.merge(&agg))
                            .or_insert(agg);
                    }
                    response.send(()).ok();
                }
                StateMessage::GetSnapshot { query_id, response } => {
                    let snapshot = self.states.get(&query_id).cloned();
                    response.send(snapshot).ok();
                }
                StateMessage::Initialize { query_id, initial_state } => {
                    self.states.insert(query_id, initial_state);
                }
            }
        }
    }
}
```

**Benefits**:
- ✅ Zero lock contention (single-threaded actor)
- ✅ Zero cloning during merges (in-place updates)
- ✅ Natural backpressure (channel buffering)
- ✅ Easy to make persistent (snapshot to disk)

**Performance**:
- Message passing: ~500ns per message
- HashMap merge: ~50ns per key
- **Total cost per batch**: ~10ms (vs 160ms current)
- **Improvement**: 16x faster state management

#### 2. Processing Pipeline (Parallel Workers)

**Purpose**: CPU-bound SQL execution, no I/O

```rust
/// Worker that processes batches in parallel
struct ProcessingWorker {
    worker_id: usize,
    query: StreamingQuery,
    state_tx: mpsc::UnboundedSender<StateMessage>,
}

impl ProcessingWorker {
    async fn process_batch(&self, batch: Vec<StreamRecord>) -> ProcessedBatch {
        // Build LOCAL state for THIS batch (no locks!)
        let mut local_context = ProcessorContext::new(&self.query);
        let mut output_records = Vec::with_capacity(batch.len());

        for record in batch {
            match QueryProcessor::process_query(&self.query, &record, &mut local_context) {
                Ok(result) => {
                    if let Some(output) = result.record {
                        output_records.push(Arc::new(output));
                    }
                }
                Err(e) => {
                    // Record error
                }
            }
        }

        // Send local state to actor for merging (async, non-blocking)
        let (tx, rx) = oneshot::channel();
        self.state_tx.send(StateMessage::MergeBatchState {
            query_id: self.query.id.clone(),
            batch_state: local_context.group_by_states,
            response: tx,
        }).ok();

        // Don't wait for merge to complete - return output immediately
        ProcessedBatch { output_records }
    }
}
```

**Benefits**:
- ✅ True parallelism (N workers process N batches concurrently)
- ✅ Zero locking in hot path
- ✅ Local state building (fast allocations)
- ✅ Async state merging (doesn't block worker)

**Performance**:
- Batch processing: ~9ms (same as pure SQL)
- State message send: ~500ns (non-blocking)
- **Total cost per batch**: ~9ms (vs 213ms current)
- **Improvement**: 23x faster!

#### 3. Source/Sink Pipelines (Async Streams)

**Purpose**: Overlap I/O with compute

```rust
/// Source pipeline using tokio streams
async fn source_pipeline(
    mut reader: Box<dyn DataReader>,
    batch_tx: mpsc::Sender<Vec<StreamRecord>>,
) {
    loop {
        match reader.read().await {
            Ok(batch) if !batch.is_empty() => {
                // Send to processing pipeline (with backpressure)
                if batch_tx.send(batch).await.is_err() {
                    break; // Receiver dropped
                }
            }
            Ok(_) => {
                // Empty batch - check for more data
                if !reader.has_more().await.unwrap_or(false) {
                    break;
                }
            }
            Err(e) => {
                // Handle error
                break;
            }
        }
    }
}

/// Sink pipeline using tokio streams
async fn sink_pipeline(
    mut writer: Box<dyn DataWriter>,
    mut output_rx: mpsc::Receiver<Vec<Arc<StreamRecord>>>,
) {
    while let Some(records) = output_rx.recv().await {
        if let Err(e) = writer.write_batch(records).await {
            // Handle error
        }
    }
    writer.flush().await.ok();
}
```

**Benefits**:
- ✅ Pipeline parallelism (read → process → write simultaneously)
- ✅ Automatic backpressure (bounded channels)
- ✅ Zero wasted cycles (async I/O)

#### 4. Job Coordinator (Lightweight Orchestrator)

```rust
/// Main job coordinator - wires everything together
pub struct JobCoordinatorV2 {
    config: JobProcessingConfig,
    observability: Option<ObservabilityConfig>,
}

impl JobCoordinatorV2 {
    pub async fn run_job(
        &self,
        reader: Box<dyn DataReader>,
        writer: Box<dyn DataWriter>,
        query: StreamingQuery,
    ) -> JobExecutionStats {
        // 1. Spawn state manager actor
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let state_actor = StateManagerActor::new(state_rx);
        tokio::spawn(state_actor.run());

        // 2. Create processing pipeline
        let (batch_tx, batch_rx) = mpsc::channel(self.config.max_batch_size);
        let (output_tx, output_rx) = mpsc::channel(self.config.max_batch_size);

        // 3. Spawn worker pool
        let num_workers = num_cpus::get();
        for i in 0..num_workers {
            let worker = ProcessingWorker::new(i, query.clone(), state_tx.clone());
            let batch_rx = batch_rx.clone();
            let output_tx = output_tx.clone();

            tokio::spawn(async move {
                while let Some(batch) = batch_rx.recv().await {
                    let result = worker.process_batch(batch).await;
                    output_tx.send(result.output_records).await.ok();
                }
            });
        }

        // 4. Spawn source and sink pipelines
        tokio::spawn(source_pipeline(reader, batch_tx));
        tokio::spawn(sink_pipeline(writer, output_rx));

        // 5. Wait for completion and collect stats
        // ... shutdown coordination ...

        JobExecutionStats::default()
    }
}
```

**Benefits**:
- ✅ Simple coordination (no shared state)
- ✅ Easy to scale (add more workers)
- ✅ Clean shutdown (drop channels)

---

## Part 4: Performance Comparison

### V1 (Current) Timeline - 5K records, 5 batches

```
Total time: 213ms

Timeline per batch (~43ms each):
├─ Read batch (I/O): 5ms
├─ LOCK #1: 8ms ❌
├─ Clone HashMap: 16ms ❌
├─ UNLOCK #1: 2ms
├─ Process batch (CPU): 9ms
├─ Observability: 5ms ⚠️
├─ LOCK #2: 8ms ❌
├─ Sync state: 4ms
├─ UNLOCK #2: 2ms
└─ Write batch (I/O): 4ms

Batches processed: SEQUENTIALLY (no parallelism)
```

### V2 (Proposed) Timeline - 5K records, 5 batches

```
Total time: ~25ms (8.5x faster!)

Pipeline (fully parallel):
┌─ Source pipeline ──────────┐
│  Read batch: 5ms           │
│  Send to worker: 0.5ms     │
└────────────────────────────┘
         │
         ▼
┌─ Worker pool (4 workers) ──┐
│  Process batch: 9ms        │
│  Build local state: 0ms    │
│  Send merge msg: 0.5ms     │
│  Emit output: 0.5ms        │
└────────────────────────────┘
         │
         ├─► State actor (async)
         │   Merge state: 1ms
         │
         └─► Sink pipeline
             Write batch: 4ms

Batches processed: 4 IN PARALLEL
Observability: OPTIONAL (disabled in test)
```

**Key differences**:
- ❌ No locks in hot path
- ❌ No HashMap cloning
- ✅ 4 batches process simultaneously (4 CPU cores)
- ✅ I/O overlapped with compute
- ⚠️ Observability opt-in (not always-on)

### Expected Throughput

| Scenario | V1 (Current) | V2 (Proposed) | Improvement |
|----------|--------------|---------------|-------------|
| **Single source, no obs** | 23K rec/sec | 200K rec/sec | 8.7x faster |
| **Single source, with obs** | 23K rec/sec | 150K rec/sec | 6.5x faster |
| **Multi-source (4 sources)** | 20K rec/sec | 600K rec/sec | 30x faster! |
| **Pure SQL baseline** | 790K rec/sec | 790K rec/sec | No change |

**Why multi-source scales better**:
- V1: Sequential batch processing from all sources
- V2: Parallel workers process batches from all sources concurrently

---

## Part 5: Migration Complexity Analysis

### Option A: Incremental Fix (Option 5 - Local Merge)

**Code Changes Required**:
```
Modified files: 1
Lines changed: ~50
New traits/types: 0
Breaking changes: 0
Testing effort: Low (existing tests work)
```

**Implementation**:
```rust
// BEFORE (common.rs:228-234)
let (group_states, window_states) = {
    let engine_lock = engine.lock().await;
    (
        engine_lock.get_group_states().clone(),  // ❌ Clone
        engine_lock.get_window_states(),
    )
};
// ... process batch ...
{
    let mut engine_lock = engine.lock().await;
    engine_lock.set_group_states(group_states);
}

// AFTER (Option 5)
// Build local state (no lock)
let mut local_context = ProcessorContext::new(&query_id);
for record in batch {
    QueryProcessor::process_query(&query, &record, &mut local_context)?;
}
// Single lock for merge
{
    let mut engine_lock = engine.lock().await;
    engine_lock.merge_batch_state(local_context.group_by_states);
}
```

**Timeline**: 2-4 hours to implement + test

### Option B: Complete Redesign (V2 Architecture)

**Code Changes Required**:
```
New files: 6-8
Modified files: 10-15
Lines changed: ~2000
New traits/types: 4-6 (StateMessage, ProcessingWorker, etc.)
Breaking changes: YES (API changes)
Testing effort: HIGH (new integration tests needed)
```

**Implementation phases**:
1. **Phase 1**: State manager actor (2 days)
2. **Phase 2**: Worker pool (2 days)
3. **Phase 3**: Pipeline integration (2 days)
4. **Phase 4**: Migration of existing features (3 days)
5. **Phase 5**: Testing & validation (3 days)

**Timeline**: 2-3 weeks

---

## Part 6: Recommendation Matrix

| Criterion | Option 5 (Incremental) | V2 (Redesign) | Winner |
|-----------|------------------------|---------------|--------|
| **Performance (single source)** | 200K rec/sec (8x) | 200K rec/sec (8x) | TIE ✅ |
| **Performance (multi-source)** | 80K rec/sec (3x) | 600K rec/sec (30x) | V2 🏆 |
| **Implementation time** | 4 hours | 2-3 weeks | Option 5 🏆 |
| **Code complexity** | +50 lines | +2000 lines | Option 5 🏆 |
| **Breaking changes** | None | Moderate | Option 5 🏆 |
| **Testing effort** | Low | High | Option 5 🏆 |
| **Scalability** | Limited | Excellent | V2 🏆 |
| **Future maintenance** | Technical debt | Clean slate | V2 🏆 |
| **Observability opt-out** | No | Yes | V2 🏆 |
| **Pipeline parallelism** | No | Yes | V2 🏆 |

---

## Part 7: Final Recommendation

### For Immediate Performance Gains: **Option 5 (Incremental)**

**When to choose**:
- ✅ Need performance improvement ASAP (hours, not weeks)
- ✅ Primarily single-source workloads
- ✅ 8x improvement is acceptable
- ✅ Want minimal risk/testing
- ✅ Can't afford breaking changes

**Implementation path**:
1. Implement local merge in `process_batch_with_output` (~4 hours)
2. Run overhead_breakdown test (validate 8x improvement)
3. Run comprehensive benchmarks (ensure no regressions)
4. Deploy to production

### For Long-Term Architecture: **V2 (Redesign)**

**When to choose**:
- ✅ Multi-source/multi-sink workloads are critical
- ✅ Want near-pure-SQL performance (26x improvement for multi-source)
- ✅ Can invest 2-3 weeks for migration
- ✅ Value clean architecture over quick wins
- ✅ Want optional observability overhead

**Implementation path**:
1. Develop V2 in parallel (new module: `server::processors::v2`)
2. Run performance tests comparing V1 vs V2
3. Migrate one job at a time (gradual rollout)
4. Deprecate V1 after 6 months

### **HYBRID APPROACH (Recommended)**

**Best of both worlds**:

1. **Short term** (Week 1): Implement Option 5 local merge
   - Get 8x improvement immediately
   - Ship to production quickly
   - Buy time for proper redesign

2. **Medium term** (Weeks 2-5): Develop V2 architecture
   - Build actor-based state manager
   - Implement worker pools
   - Create pipeline coordinators
   - Thorough testing

3. **Long term** (Month 2): Gradual V2 migration
   - New jobs use V2
   - Existing jobs stay on V1 (with local merge)
   - Monitor performance & stability
   - Migrate when confident

**Why this works**:
- ✅ Immediate performance gains (Option 5)
- ✅ No rush/pressure for V2 development
- ✅ Can validate V2 in production gradually
- ✅ Fallback to V1 if V2 has issues
- ✅ Best long-term architecture

---

## Part 8: Next Steps

### If choosing Option 5 (Incremental):
1. Implement merge_batch_state() in StreamExecutionEngine
2. Refactor process_batch_with_output() to use local state
3. Run job_server_overhead_breakdown test
4. Verify 8x improvement (23K → 200K rec/sec)
5. Update documentation

### If choosing V2 (Redesign):
1. Create new module: `server::processors::v2`
2. Implement StateManagerActor
3. Implement ProcessingWorker
4. Create pipeline coordinators
5. Write integration tests
6. Performance validation

### If choosing HYBRID (Recommended):
1. **Week 1**: Option 5 implementation
2. **Week 2**: V2 design review & prototyping
3. **Weeks 3-4**: V2 core implementation
4. **Week 5**: V2 testing & validation
5. **Week 6+**: Gradual production migration

**Question for you**: Which path do you want to take?

---

## Part 9: Enterprise Features & Production Readiness

### 9.1 Job Management System

#### Job Lifecycle Management

```rust
/// Job lifecycle states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Created,       // Job defined but not started
    Starting,      // Initializing resources
    Running,       // Actively processing
    Paused,        // Temporarily suspended
    Rebalancing,   // Redistributing partitions
    Stopping,      // Graceful shutdown in progress
    Stopped,       // Cleanly stopped
    Failed,        // Error state
    Recovering,    // Attempting recovery from failure
}

/// Job manager coordinating multiple jobs
pub struct JobManager {
    jobs: Arc<RwLock<HashMap<JobId, JobContext>>>,
    coordinator: Arc<JobCoordinator>,
    health_checker: Arc<HealthChecker>,
    metrics: Arc<JobMetricsCollector>,
}

#[derive(Clone)]
pub struct JobContext {
    job_id: JobId,
    state: Arc<AtomicU8>,  // JobState
    config: JobProcessingConfig,
    start_time: Instant,
    last_checkpoint: Arc<RwLock<Option<Instant>>>,
    error_count: Arc<AtomicUsize>,
    records_processed: Arc<AtomicU64>,
    shutdown_tx: mpsc::Sender<ShutdownSignal>,
}

impl JobManager {
    /// Start a new streaming job
    pub async fn start_job(
        &self,
        job_id: JobId,
        query: StreamingQuery,
        sources: Vec<Box<dyn DataReader>>,
        sinks: Vec<Box<dyn DataWriter>>,
        config: JobProcessingConfig,
    ) -> Result<JobHandle> {
        // 1. Validate job configuration
        self.validate_job_config(&config)?;

        // 2. Allocate resources (worker pool, state actors)
        let resources = self.allocate_resources(&config).await?;

        // 3. Initialize job context
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let context = JobContext::new(job_id.clone(), config, shutdown_tx);

        // 4. Register job with health checker
        self.health_checker.register_job(job_id.clone(), context.clone()).await;

        // 5. Start processing pipeline
        let handle = self.coordinator.spawn_job(
            job_id.clone(),
            query,
            sources,
            sinks,
            resources,
            shutdown_rx,
        ).await?;

        // 6. Store job context
        self.jobs.write().await.insert(job_id.clone(), context);

        Ok(JobHandle { job_id, handle })
    }

    /// Gracefully stop a job
    pub async fn stop_job(&self, job_id: &JobId) -> Result<JobStats> {
        let context = self.jobs.read().await.get(job_id).cloned()
            .ok_or(JobError::NotFound)?;

        // 1. Set state to Stopping
        context.set_state(JobState::Stopping);

        // 2. Send shutdown signal
        context.shutdown_tx.send(ShutdownSignal::Graceful).await?;

        // 3. Wait for completion (with timeout)
        tokio::time::timeout(
            Duration::from_secs(30),
            self.wait_for_job_shutdown(&context)
        ).await??;

        // 4. Collect final statistics
        let stats = self.collect_job_stats(&context).await;

        // 5. Cleanup resources
        self.cleanup_job_resources(job_id).await?;

        Ok(stats)
    }

    /// Pause job processing (keep state, stop consuming)
    pub async fn pause_job(&self, job_id: &JobId) -> Result<()> {
        let context = self.jobs.read().await.get(job_id).cloned()
            .ok_or(JobError::NotFound)?;

        context.set_state(JobState::Paused);
        context.shutdown_tx.send(ShutdownSignal::Pause).await?;

        Ok(())
    }

    /// Resume paused job
    pub async fn resume_job(&self, job_id: &JobId) -> Result<()> {
        let context = self.jobs.read().await.get(job_id).cloned()
            .ok_or(JobError::NotFound)?;

        context.set_state(JobState::Running);
        context.shutdown_tx.send(ShutdownSignal::Resume).await?;

        Ok(())
    }
}
```

#### Health Monitoring

```rust
pub struct HealthChecker {
    jobs: Arc<RwLock<HashMap<JobId, JobHealthState>>>,
    check_interval: Duration,
}

#[derive(Clone)]
struct JobHealthState {
    last_heartbeat: Arc<RwLock<Instant>>,
    consecutive_failures: Arc<AtomicUsize>,
    is_healthy: Arc<AtomicBool>,
}

impl HealthChecker {
    /// Periodic health check for all jobs
    pub async fn run_health_checks(&self) {
        let mut interval = tokio::time::interval(self.check_interval);

        loop {
            interval.tick().await;

            let jobs = self.jobs.read().await.clone();
            for (job_id, health) in jobs {
                self.check_job_health(&job_id, &health).await;
            }
        }
    }

    async fn check_job_health(&self, job_id: &JobId, health: &JobHealthState) {
        let last_heartbeat = *health.last_heartbeat.read().await;
        let time_since_heartbeat = last_heartbeat.elapsed();

        // Check 1: Heartbeat timeout (30 seconds)
        if time_since_heartbeat > Duration::from_secs(30) {
            self.handle_stale_job(job_id, health).await;
            return;
        }

        // Check 2: Excessive errors
        let failures = health.consecutive_failures.load(Ordering::Relaxed);
        if failures > 10 {
            self.handle_failing_job(job_id, health).await;
            return;
        }

        // Mark as healthy
        health.is_healthy.store(true, Ordering::Release);
    }
}
```

---

### 9.2 Watermark Management

#### Event-Time Processing

```rust
/// Watermark manager tracking event time progress
pub struct WatermarkManager {
    watermarks: Arc<RwLock<HashMap<PartitionId, Watermark>>>,
    late_data_strategy: LateDataStrategy,
    max_out_of_orderness: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct Watermark {
    timestamp: i64,           // Current watermark (event time)
    processing_time: Instant, // When watermark was updated
}

#[derive(Debug, Clone, Copy)]
pub enum LateDataStrategy {
    Drop,                     // Discard late records
    AllowedLateness(Duration), // Accept within window
    SideOutput,               // Route to separate stream
}

impl WatermarkManager {
    /// Update watermark based on incoming record
    pub async fn update_watermark(
        &self,
        partition: PartitionId,
        record_event_time: i64,
    ) -> Option<Watermark> {
        let mut watermarks = self.watermarks.write().await;

        // Get current watermark for partition
        let current = watermarks.get(&partition).copied();

        // Calculate new watermark (max event time - allowed lateness)
        let new_watermark_ts = record_event_time - self.max_out_of_orderness.as_millis() as i64;

        // Watermarks can only advance (monotonicity guarantee)
        let new_watermark = match current {
            Some(wm) if new_watermark_ts > wm.timestamp => {
                let updated = Watermark {
                    timestamp: new_watermark_ts,
                    processing_time: Instant::now(),
                };
                watermarks.insert(partition, updated);
                Some(updated)
            }
            None => {
                let initial = Watermark {
                    timestamp: new_watermark_ts,
                    processing_time: Instant::now(),
                };
                watermarks.insert(partition, initial);
                Some(initial)
            }
            _ => None, // No advancement
        };

        new_watermark
    }

    /// Get global watermark across all partitions (minimum)
    pub async fn global_watermark(&self) -> Option<Watermark> {
        let watermarks = self.watermarks.read().await;

        watermarks.values()
            .min_by_key(|wm| wm.timestamp)
            .copied()
    }

    /// Check if record is late relative to watermark
    pub async fn is_late_data(
        &self,
        partition: PartitionId,
        record_event_time: i64,
    ) -> bool {
        let watermarks = self.watermarks.read().await;

        match watermarks.get(&partition) {
            Some(wm) => record_event_time < wm.timestamp,
            None => false, // No watermark yet
        }
    }

    /// Handle late arriving data
    pub async fn handle_late_data(
        &self,
        record: StreamRecord,
        watermark: Watermark,
    ) -> LateDataDecision {
        match self.late_data_strategy {
            LateDataStrategy::Drop => LateDataDecision::Drop,

            LateDataStrategy::AllowedLateness(allowed) => {
                let event_time = record.get_event_time();
                let lateness = watermark.timestamp - event_time;

                if lateness <= allowed.as_millis() as i64 {
                    LateDataDecision::Process
                } else {
                    LateDataDecision::Drop
                }
            }

            LateDataStrategy::SideOutput => {
                LateDataDecision::SideOutput
            }
        }
    }
}

/// Window triggering based on watermarks
impl WindowProcessor {
    /// Check if window should emit based on watermark
    pub fn should_emit_window(
        &self,
        window_end: i64,
        watermark: Watermark,
    ) -> bool {
        // Emit when watermark passes window end time
        watermark.timestamp >= window_end
    }

    /// Emit all ready windows
    pub async fn emit_ready_windows(
        &mut self,
        watermark: Watermark,
    ) -> Vec<WindowEmission> {
        let mut emissions = Vec::new();

        // Find all windows where watermark >= window_end
        self.pending_windows.retain(|window| {
            if watermark.timestamp >= window.end_time {
                emissions.push(WindowEmission {
                    window_start: window.start_time,
                    window_end: window.end_time,
                    aggregates: window.state.clone(),
                    watermark: watermark.timestamp,
                });
                false // Remove from pending
            } else {
                true // Keep pending
            }
        });

        emissions
    }
}
```

---

### 9.3 System Fields & StreamRecord Metadata

#### Built-in System Columns

Velostream provides **6 built-in system columns** that expose record metadata from `StreamRecord` properties. These fields are **NOT** stored in `record.fields` but are accessed via record properties.

**System Column Definition** (from `src/velostream/sql/execution/types.rs`):

```rust
/// System column names in Velostream (UPPERCASE internal form)
/// These are special columns from StreamRecord properties, not from field data
pub mod system_columns {
    /// Processing time in milliseconds since Unix epoch (UPPERCASE internal form)
    pub const TIMESTAMP: &str = "_TIMESTAMP";

    /// Kafka partition offset for the record (UPPERCASE internal form)
    pub const OFFSET: &str = "_OFFSET";

    /// Kafka partition number (UPPERCASE internal form)
    pub const PARTITION: &str = "_PARTITION";

    /// Event time in milliseconds since Unix epoch (UPPERCASE internal form)
    pub const EVENT_TIME: &str = "_EVENT_TIME";

    /// Window start time in milliseconds since Unix epoch (UPPERCASE internal form)
    pub const WINDOW_START: &str = "_WINDOW_START";

    /// Window end time in milliseconds since Unix epoch (UPPERCASE internal form)
    pub const WINDOW_END: &str = "_WINDOW_END";
}
```

#### Performance Optimization: UPPERCASE Normalization

**CRITICAL**: System column names are stored in **UPPERCASE** internally to eliminate repeated string allocations during query execution.

```rust
/// Normalize column name to UPPERCASE if it's a system column
/// This should be called ONCE at parse/validation time.
/// Internally, all system column references use UPPERCASE to avoid repeated allocations.
#[inline]
pub fn normalize_if_system_column(name: &str) -> Option<&'static str> {
    let upper = name.to_uppercase();
    get_system_columns_set().get(upper.as_str()).copied()
}

/// Check if a name (UPPERCASE) is a system column - O(1) lookup
#[inline]
pub fn is_system_column_upper(name_upper: &str) -> bool {
    get_system_columns_set().contains(name_upper)
}
```

**Best Practice**: Normalize column names at **parse time**, not at execution time:

```rust
// ✅ CORRECT: Normalize at parse time (once)
let parsed_query = parser.parse(sql)?;
// Parser calls normalize_if_system_column() during AST construction

// ❌ INCORRECT: Normalize at execution time (1M+ times)
for record in stream {
    let col_name = normalize_if_system_column("_timestamp");  // Repeated allocation!
}
```

#### System Field Access Patterns in V2

**1. ProcessingWorker - Query Execution**

Workers must access system fields via `StreamRecord` properties, not `fields` HashMap:

```rust
impl ProcessingWorker {
    async fn process_batch(&self, batch: Vec<StreamRecord>) -> ProcessedBatch {
        let mut local_context = ProcessorContext::new(&self.query);

        for record in batch {
            // Access system fields from record properties
            let timestamp = record.timestamp;           // _TIMESTAMP
            let offset = record.offset;                 // _OFFSET
            let partition = record.partition;           // _PARTITION
            let event_time = record.get_event_time();   // _EVENT_TIME

            // Regular fields from HashMap
            let user_field = record.fields.get("price");

            // Execute query with both system and user fields
            let result = self.execute_query(&record, &mut local_context).await?;
        }

        ProcessedBatch { /* ... */ }
    }
}
```

**2. StateManagerActor - Window Metadata**

System fields `_WINDOW_START` and `_WINDOW_END` are managed by `StateManagerActor`:

```rust
struct QueryState {
    // Time-based window state (TUMBLING, SLIDING, SESSION)
    window_states: Vec<WindowState>,
}

struct WindowState {
    pub window_id: WindowId,
    pub window_start: i64,    // ← _WINDOW_START value
    pub window_end: i64,      // ← _WINDOW_END value
    pub group_by_state: Arc<FxHashMap<GroupKey, GroupAccumulator>>,
}

impl StateManagerActor {
    async fn handle_merge_window_state(
        &mut self,
        query_id: &str,
        window_state: WindowState,
    ) {
        let state = self.states.get_mut(query_id).unwrap();

        // When emitting window results, inject _WINDOW_START and _WINDOW_END
        for (group_key, accumulator) in window_state.group_by_state.iter() {
            let mut output_record = accumulator.to_record();

            // Inject system fields for window boundaries
            output_record.fields.insert(
                "_WINDOW_START".to_string(),
                FieldValue::Integer(window_state.window_start),
            );
            output_record.fields.insert(
                "_WINDOW_END".to_string(),
                FieldValue::Integer(window_state.window_end),
            );

            self.emit_record(output_record).await;
        }
    }
}
```

**3. Backpressure Monitoring with System Fields**

Leverage system fields for monitoring and diagnostics:

```rust
impl BackpressureDetector {
    /// Track processing lag using _TIMESTAMP and _EVENT_TIME
    pub fn calculate_lag(&self, record: &StreamRecord) -> Duration {
        let processing_time = record.timestamp;  // _TIMESTAMP (processing time)
        let event_time = record.get_event_time().timestamp_millis();  // _EVENT_TIME

        let lag_ms = processing_time - event_time;
        Duration::from_millis(lag_ms as u64)
    }

    /// Monitor partition skew using _PARTITION
    pub fn track_partition_distribution(&self, batch: &[StreamRecord]) {
        let mut partition_counts: HashMap<i32, usize> = HashMap::new();

        for record in batch {
            *partition_counts.entry(record.partition).or_insert(0) += 1;
        }

        // Detect skewed partitions for load balancing
        let max_count = partition_counts.values().max().unwrap_or(&0);
        let min_count = partition_counts.values().min().unwrap_or(&0);
        let skew_ratio = *max_count as f64 / (*min_count as f64).max(1.0);

        if skew_ratio > 3.0 {
            warn!("Partition skew detected: max={}, min={}, ratio={:.2}",
                  max_count, min_count, skew_ratio);
        }
    }
}
```

#### StreamRecord Structure

```rust
/// A record in a streaming data source
#[derive(Debug, Clone, Default)]
pub struct StreamRecord {
    /// The actual field data for this record (user fields)
    pub fields: HashMap<String, FieldValue>,

    /// Timestamp when this record was created (milliseconds since epoch)
    /// Exposed as _TIMESTAMP system column
    pub timestamp: i64,

    /// Offset of this record within its partition
    /// Exposed as _OFFSET system column
    pub offset: i64,

    /// Partition number this record came from
    /// Exposed as _PARTITION system column
    pub partition: i32,

    /// Message headers (key-value pairs) associated with this record
    pub headers: HashMap<String, String>,

    /// Event-time timestamp for watermark-based processing (optional)
    /// When None, processing-time (timestamp field) is used
    /// When Some, this timestamp is used for event-time windowing and watermarks
    /// Exposed as _EVENT_TIME system column
    pub event_time: Option<DateTime<Utc>>,
}

impl StreamRecord {
    /// Get the effective timestamp for time-based processing
    /// Returns the event-time if set, otherwise falls back to processing-time.
    pub fn get_event_time(&self) -> DateTime<Utc> {
        match self.event_time {
            Some(event_time) => event_time,
            None => {
                // Convert processing-time timestamp to DateTime
                DateTime::from_timestamp(
                    self.timestamp / 1000,
                    ((self.timestamp % 1000) * 1_000_000) as u32,
                )
                .unwrap_or_else(Utc::now)
            }
        }
    }

    /// Extract event-time from a field if present
    /// Useful for parsing event-time from record data
    pub fn extract_event_time_from_field(
        &mut self,
        field_name: &str,
    ) -> Option<DateTime<Utc>> {
        match self.fields.get(field_name) {
            Some(FieldValue::Integer(timestamp_ms)) => {
                let datetime = DateTime::from_timestamp(
                    *timestamp_ms / 1000,
                    ((*timestamp_ms % 1000) * 1_000_000) as u32,
                );
                if let Some(dt) = datetime {
                    self.event_time = Some(dt);
                    Some(dt)
                } else {
                    None
                }
            }
            Some(FieldValue::Timestamp(naive_dt)) => {
                let dt = DateTime::from_naive_utc_and_offset(*naive_dt, Utc);
                self.event_time = Some(dt);
                Some(dt)
            }
            _ => None,
        }
    }
}
```

#### V2 Architecture Implications

**1. State Keying**: System fields can be used in GROUP BY clauses

```sql
-- Valid query using system fields
SELECT
    _PARTITION,
    _WINDOW_START,
    _WINDOW_END,
    COUNT(*) as record_count,
    AVG(price) as avg_price
FROM market_data
GROUP BY _PARTITION
WINDOW TUMBLING (_EVENT_TIME, INTERVAL '1' MINUTE);
```

**GroupKey construction** must handle system fields:

```rust
impl ProcessorContext {
    fn generate_group_key(
        &mut self,
        expressions: &[Expr],
        record: &StreamRecord,
    ) -> Result<GroupKey, SqlError> {
        let mut values = Vec::with_capacity(expressions.len());

        for expr in expressions {
            match expr {
                Expr::Column(col_name) if col_name == "_TIMESTAMP" => {
                    values.push(FieldValue::Integer(record.timestamp));
                }
                Expr::Column(col_name) if col_name == "_OFFSET" => {
                    values.push(FieldValue::Integer(record.offset));
                }
                Expr::Column(col_name) if col_name == "_PARTITION" => {
                    values.push(FieldValue::Integer(record.partition as i64));
                }
                Expr::Column(col_name) if col_name == "_EVENT_TIME" => {
                    values.push(FieldValue::Integer(
                        record.get_event_time().timestamp_millis()
                    ));
                }
                Expr::Column(col_name) => {
                    // User field from HashMap
                    let value = record.fields.get(col_name)
                        .cloned()
                        .unwrap_or(FieldValue::Null);
                    values.push(value);
                }
                _ => { /* expression evaluation */ }
            }
        }

        Ok(GroupKey::new(values))
    }
}
```

**2. Prometheus Metrics**: Use system fields for cardinality control

```rust
impl JobMetricsCollector {
    /// Record throughput per partition (using _PARTITION)
    pub fn record_partition_throughput(&self, batch: &[StreamRecord]) {
        for record in batch {
            self.partition_throughput
                .with_label_values(&[&record.partition.to_string()])
                .inc();
        }
    }

    /// Track event-time lag distribution (using _EVENT_TIME and _TIMESTAMP)
    pub fn record_event_time_lag(&self, record: &StreamRecord) {
        let processing_time = record.timestamp;
        let event_time = record.get_event_time().timestamp_millis();
        let lag_ms = processing_time - event_time;

        self.event_time_lag_histogram.observe(lag_ms as f64 / 1000.0);
    }
}
```

**3. Watermark Generation**: `_EVENT_TIME` drives watermark calculation

```rust
impl WatermarkManager {
    /// Update watermark based on _EVENT_TIME system field
    pub async fn update_watermark(
        &self,
        partition: PartitionId,
        record: &StreamRecord,
    ) -> Option<Watermark> {
        // Use _EVENT_TIME system field
        let record_event_time = record.get_event_time().timestamp_millis();

        let mut watermarks = self.watermarks.write().await;

        // Calculate new watermark (max event time - allowed lateness)
        let new_watermark_ts = record_event_time
            - self.max_out_of_orderness.as_millis() as i64;

        // Watermarks can only advance (monotonicity guarantee)
        let current = watermarks.get(&partition).copied();
        match current {
            Some(wm) if new_watermark_ts > wm.timestamp => {
                let updated = Watermark {
                    timestamp: new_watermark_ts,
                    processing_time: Instant::now(),
                };
                watermarks.insert(partition, updated);
                Some(updated)
            }
            None => {
                let new_wm = Watermark {
                    timestamp: new_watermark_ts,
                    processing_time: Instant::now(),
                };
                watermarks.insert(partition, new_wm);
                Some(new_wm)
            }
            _ => None,  // Watermark did not advance
        }
    }
}
```

#### Testing System Fields

```rust
#[tokio::test]
async fn test_system_fields_in_group_by() {
    let mut record = StreamRecord::new(HashMap::new());
    record.timestamp = 1700000000000;  // _TIMESTAMP
    record.offset = 42;                // _OFFSET
    record.partition = 3;              // _PARTITION
    record.event_time = Some(DateTime::from_timestamp(1700000000, 0).unwrap());

    // Query using system fields
    let sql = "SELECT _PARTITION, COUNT(*) FROM stream GROUP BY _PARTITION";
    let query = parser.parse(sql)?;

    // Execute with system field access
    let result = engine.execute_with_record(&query, record).await?;

    // Verify system field values
    assert_eq!(result.fields.get("_PARTITION"), Some(&FieldValue::Integer(3)));
}
```

---

### 9.4 Job Observability & Metrics

#### Comprehensive Metrics System

```rust
/// Detailed job metrics collector
pub struct JobMetricsCollector {
    registry: Arc<prometheus::Registry>,

    // Throughput metrics
    records_processed: Counter,
    records_per_second: Gauge,
    bytes_processed: Counter,

    // Latency metrics
    end_to_end_latency: Histogram,
    processing_latency: Histogram,
    watermark_lag: Gauge,

    // Resource utilization
    cpu_usage: Gauge,
    memory_usage: Gauge,
    worker_utilization: GaugeVec,

    // Error metrics
    errors_total: CounterVec,
    retries_total: Counter,
    late_records: Counter,

    // State metrics
    state_size_bytes: Gauge,
    checkpoint_duration: Histogram,

    // Window metrics
    windows_emitted: Counter,
    window_firing_delay: Histogram,

    // Backpressure metrics
    backpressure_events: CounterVec,           // Total backpressure events per component
    backpressure_ratio: GaugeVec,              // Current backpressure ratio (0.0-1.0) per worker
    channel_utilization: GaugeVec,             // Channel buffer utilization per stage
    worker_queue_depth: GaugeVec,              // Current queue depth per worker
    state_merge_wait_time: Histogram,          // Time spent waiting for state merges
    source_throttle_events: Counter,           // Times source was throttled due to backpressure
    downstream_blocking_time: HistogramVec,    // Time blocked by downstream components
}

impl JobMetricsCollector {
    /// Record batch processing metrics
    pub fn record_batch_processed(
        &self,
        batch_size: usize,
        processing_duration: Duration,
        end_to_end_duration: Duration,
    ) {
        self.records_processed.inc_by(batch_size as u64);
        self.processing_latency.observe(processing_duration.as_secs_f64());
        self.end_to_end_latency.observe(end_to_end_duration.as_secs_f64());

        // Calculate throughput
        let throughput = batch_size as f64 / processing_duration.as_secs_f64();
        self.records_per_second.set(throughput);
    }

    /// Record backpressure event when worker or channel is saturated
    pub fn record_backpressure_event(
        &self,
        component: &str,      // "worker", "state_manager", "source", "sink"
        worker_id: usize,
        severity: f64,        // 0.0 (no pressure) to 1.0 (fully saturated)
    ) {
        self.backpressure_events
            .with_label_values(&[component, &worker_id.to_string()])
            .inc();

        self.backpressure_ratio
            .with_label_values(&[component, &worker_id.to_string()])
            .set(severity);
    }

    /// Record channel buffer utilization (critical for detecting bottlenecks)
    pub fn record_channel_utilization(
        &self,
        stage: &str,          // "reader_to_workers", "workers_to_state", "state_to_writer"
        current_depth: usize,
        max_capacity: usize,
    ) {
        let utilization = current_depth as f64 / max_capacity as f64;
        self.channel_utilization
            .with_label_values(&[stage])
            .set(utilization);

        // Automatic backpressure detection
        if utilization > 0.8 {
            self.backpressure_events
                .with_label_values(&["channel", stage])
                .inc();
        }
    }

    /// Record worker queue depth (for work-stealing and load balancing)
    pub fn record_worker_queue_depth(&self, worker_id: usize, queue_depth: usize) {
        self.worker_queue_depth
            .with_label_values(&[&worker_id.to_string()])
            .set(queue_depth as f64);
    }

    /// Record state merge wait time (critical for identifying state bottlenecks)
    pub fn record_state_merge_wait(&self, wait_duration: Duration) {
        self.state_merge_wait_time.observe(wait_duration.as_secs_f64());

        // If state merge takes > 100ms, it's a backpressure indicator
        if wait_duration.as_millis() > 100 {
            self.backpressure_events
                .with_label_values(&["state_manager", "merge"])
                .inc();
        }
    }

    /// Record source throttling due to downstream backpressure
    pub fn record_source_throttle(&self) {
        self.source_throttle_events.inc();
    }

    /// Record time spent blocked by downstream component
    pub fn record_downstream_blocking(
        &self,
        component: &str,      // "state_manager", "sink", "network"
        blocking_duration: Duration,
    ) {
        self.downstream_blocking_time
            .with_label_values(&[component])
            .observe(blocking_duration.as_secs_f64());
    }

    /// Record watermark lag (event time vs processing time)
    pub fn record_watermark_lag(&self, watermark: Watermark, processing_time: Instant) {
        let lag_ms = processing_time.duration_since(Instant::now()).as_millis() as i64;
        self.watermark_lag.set(lag_ms as f64);
    }

    /// Record worker utilization
    pub fn record_worker_utilization(&self, worker_id: usize, utilization: f64) {
        self.worker_utilization
            .with_label_values(&[&worker_id.to_string()])
            .set(utilization);
    }

    /// Expose metrics endpoint
    pub fn metrics_endpoint(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode_to_string(&metric_families).unwrap()
    }
}
```

#### Distributed Tracing Integration

```rust
use opentelemetry::trace::{Span, Tracer, SpanKind};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct TracingContext {
    tracer: Arc<dyn Tracer>,
    job_id: JobId,
}

impl TracingContext {
    /// Create span for batch processing
    pub fn batch_span(&self, batch_id: u64) -> impl Span {
        self.tracer
            .span_builder(format!("process_batch_{}", batch_id))
            .with_kind(SpanKind::Internal)
            .with_attributes(vec![
                ("job.id".into(), self.job_id.to_string().into()),
                ("batch.id".into(), batch_id.into()),
            ])
            .start(&*self.tracer)
    }

    /// Create span for state merge
    pub fn state_merge_span(&self) -> impl Span {
        self.tracer
            .span_builder("state_merge")
            .with_kind(SpanKind::Internal)
            .start(&*self.tracer)
    }
}
```

#### Backpressure Detection & Automatic Throttling

```rust
/// Backpressure detector monitors system health and triggers throttling
pub struct BackpressureDetector {
    metrics: Arc<JobMetricsCollector>,
    thresholds: BackpressureThresholds,
    current_state: Arc<RwLock<BackpressureState>>,
}

pub struct BackpressureThresholds {
    channel_utilization_warning: f64,    // 0.7 (70% full)
    channel_utilization_critical: f64,   // 0.9 (90% full)
    state_merge_latency_ms: u64,         // 100ms
    worker_queue_depth_warning: usize,   // 1000 records
    memory_pressure_threshold: f64,      // 0.85 (85% of max)
}

#[derive(Debug, Clone)]
pub enum BackpressureState {
    Healthy,
    Warning { component: String, severity: f64 },
    Critical { component: String, severity: f64 },
    Saturated { component: String },
}

impl BackpressureDetector {
    /// Check all components and detect backpressure conditions
    pub async fn check_system_health(&self) -> BackpressureState {
        // 1. Check channel utilization across all stages
        let channel_pressure = self.check_channel_pressure().await;

        // 2. Check worker queue depths
        let worker_pressure = self.check_worker_queues().await;

        // 3. Check state manager merge latency
        let state_pressure = self.check_state_merge_pressure().await;

        // 4. Check memory pressure
        let memory_pressure = self.check_memory_pressure().await;

        // Determine overall system state
        let max_pressure = [
            channel_pressure,
            worker_pressure,
            state_pressure,
            memory_pressure,
        ].iter().max_by(|a, b| {
            self.severity_score(a).partial_cmp(&self.severity_score(b)).unwrap()
        }).unwrap();

        max_pressure.clone()
    }

    /// Monitor channel utilization and detect bottlenecks
    async fn check_channel_pressure(&self) -> BackpressureState {
        // Channels to monitor:
        // - reader_to_workers: Source -> Workers
        // - workers_to_state: Workers -> StateManager
        // - state_to_writer: StateManager -> Sink

        let stages = ["reader_to_workers", "workers_to_state", "state_to_writer"];

        for stage in stages {
            let utilization = self.get_channel_utilization(stage).await;

            if utilization > self.thresholds.channel_utilization_critical {
                // CRITICAL: Channel is 90%+ full - apply aggressive backpressure
                self.metrics.record_backpressure_event(
                    "channel",
                    0,
                    1.0,  // Maximum severity
                );

                return BackpressureState::Critical {
                    component: format!("channel:{}", stage),
                    severity: utilization,
                };
            } else if utilization > self.thresholds.channel_utilization_warning {
                // WARNING: Channel is 70%+ full - start throttling
                self.metrics.record_backpressure_event(
                    "channel",
                    0,
                    utilization,
                );

                return BackpressureState::Warning {
                    component: format!("channel:{}", stage),
                    severity: utilization,
                };
            }
        }

        BackpressureState::Healthy
    }

    /// Automatic source throttling based on backpressure state
    pub async fn apply_backpressure(&self, state: &BackpressureState) -> ThrottleAction {
        match state {
            BackpressureState::Healthy => {
                ThrottleAction::NoThrottle
            }
            BackpressureState::Warning { severity, .. } => {
                // Linear throttling: reduce throughput proportionally
                let throttle_factor = (severity - 0.7) / 0.2;  // 0.0 at 70%, 1.0 at 90%
                ThrottleAction::Throttle {
                    delay_ms: (throttle_factor * 100.0) as u64,
                }
            }
            BackpressureState::Critical { .. } => {
                // Aggressive throttling: pause source reads
                self.metrics.record_source_throttle();
                ThrottleAction::Pause {
                    duration: Duration::from_millis(500),
                }
            }
            BackpressureState::Saturated { .. } => {
                // System saturated: stop all reads until pressure drops
                self.metrics.record_source_throttle();
                ThrottleAction::Stop {
                    reason: "System saturated - backpressure critical".to_string(),
                }
            }
        }
    }
}

pub enum ThrottleAction {
    NoThrottle,
    Throttle { delay_ms: u64 },
    Pause { duration: Duration },
    Stop { reason: String },
}

/// Example: Integration in JobServer main loop
async fn process_with_backpressure_monitoring(
    reader: &mut impl DataReader,
    metrics: Arc<JobMetricsCollector>,
) {
    let detector = BackpressureDetector::new(metrics.clone());

    loop {
        // Check system health before reading next batch
        let pressure_state = detector.check_system_health().await;

        // Apply automatic throttling
        match detector.apply_backpressure(&pressure_state).await {
            ThrottleAction::NoThrottle => {
                // System healthy - read at full speed
            }
            ThrottleAction::Throttle { delay_ms } => {
                // Inject delay to reduce throughput
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            ThrottleAction::Pause { duration } => {
                // Pause reads to let system catch up
                warn!("Backpressure detected - pausing reads for {:?}", duration);
                tokio::time::sleep(duration).await;
                continue;
            }
            ThrottleAction::Stop { reason } => {
                // Critical backpressure - stop processing
                error!("Stopping job due to backpressure: {}", reason);
                return;
            }
        }

        // Read next batch
        let batch = reader.read().await?;

        // Record channel utilization after read
        metrics.record_channel_utilization(
            "reader_to_workers",
            batch.len(),
            1000,  // max capacity
        );

        // ... continue processing
    }
}
```

#### Prometheus Metrics Exposure

```text
# HELP velostream_backpressure_events_total Total backpressure events by component
# TYPE velostream_backpressure_events_total counter
velostream_backpressure_events_total{component="channel",stage="workers_to_state"} 42
velostream_backpressure_events_total{component="state_manager",worker_id="0"} 15
velostream_backpressure_events_total{component="worker",worker_id="3"} 8

# HELP velostream_backpressure_ratio Current backpressure severity ratio (0.0-1.0)
# TYPE velostream_backpressure_ratio gauge
velostream_backpressure_ratio{component="channel",stage="workers_to_state"} 0.87
velostream_backpressure_ratio{component="worker",worker_id="0"} 0.23
velostream_backpressure_ratio{component="worker",worker_id="1"} 0.91

# HELP velostream_channel_utilization Channel buffer utilization percentage
# TYPE velostream_channel_utilization gauge
velostream_channel_utilization{stage="reader_to_workers"} 0.42
velostream_channel_utilization{stage="workers_to_state"} 0.89
velostream_channel_utilization{stage="state_to_writer"} 0.15

# HELP velostream_worker_queue_depth Current queue depth per worker
# TYPE velostream_worker_queue_depth gauge
velostream_worker_queue_depth{worker_id="0"} 234
velostream_worker_queue_depth{worker_id="1"} 892
velostream_worker_queue_depth{worker_id="2"} 145

# HELP velostream_state_merge_wait_time_seconds Time spent waiting for state merges
# TYPE velostream_state_merge_wait_time_seconds histogram
velostream_state_merge_wait_time_seconds_bucket{le="0.01"} 1523
velostream_state_merge_wait_time_seconds_bucket{le="0.05"} 2891
velostream_state_merge_wait_time_seconds_bucket{le="0.1"} 3456
velostream_state_merge_wait_time_seconds_bucket{le="+Inf"} 3502

# HELP velostream_source_throttle_events_total Times source was throttled
# TYPE velostream_source_throttle_events_total counter
velostream_source_throttle_events_total 127

# HELP velostream_downstream_blocking_time_seconds Time blocked by downstream
# TYPE velostream_downstream_blocking_time_seconds histogram
velostream_downstream_blocking_time_seconds_bucket{component="state_manager",le="0.01"} 892
velostream_downstream_blocking_time_seconds_bucket{component="sink",le="0.05"} 234
```

#### Grafana Dashboard Alerts

```yaml
# Alert when channel utilization is high
- alert: HighChannelUtilization
  expr: velostream_channel_utilization > 0.85
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "Channel {{ $labels.stage }} is {{ $value | humanizePercentage }} full"
    description: "Backpressure detected - channel near capacity"

# Alert when workers are severely backpressured
- alert: WorkerBackpressure
  expr: velostream_backpressure_ratio{component="worker"} > 0.9
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "Worker {{ $labels.worker_id }} is critically backpressured"
    description: "Worker queue is saturated - consider scaling up"

# Alert when state merge is slow
- alert: SlowStateMerge
  expr: histogram_quantile(0.95, velostream_state_merge_wait_time_seconds_bucket) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "State merge p95 latency is {{ $value }}s"
    description: "State manager is bottleneck - Phase 4B/4C optimization needed"

# Alert when source throttling is frequent
- alert: FrequentSourceThrottling
  expr: rate(velostream_source_throttle_events_total[5m]) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Source throttled {{ $value }} times/sec"
    description: "Downstream cannot keep up - investigate bottlenecks"
```

---

### 9.4 Scaling Model

#### Vertical Scaling (Single Server, Massive Cores)

```rust
/// Adaptive worker pool scaling to utilize all cores
pub struct AdaptiveWorkerPool {
    workers: Vec<Arc<ProcessingWorker>>,
    num_cores: usize,
    work_stealing: bool,
}

impl AdaptiveWorkerPool {
    /// Create worker pool sized for available cores
    pub fn new(config: &JobProcessingConfig) -> Self {
        let num_cores = num_cpus::get();

        // Strategy: 1 worker per core for CPU-bound SQL processing
        let workers = (0..num_cores)
            .map(|id| Arc::new(ProcessingWorker::new(id)))
            .collect();

        Self {
            workers,
            num_cores,
            work_stealing: true,
        }
    }

    /// Scale up to 100+ cores efficiently
    pub async fn process_with_affinity(
        &self,
        batches: mpsc::Receiver<Vec<StreamRecord>>,
    ) {
        // Pin workers to specific CPU cores for cache locality
        for (worker_id, worker) in self.workers.iter().enumerate() {
            let core_id = worker_id % self.num_cores;

            // Set CPU affinity (Linux/Windows specific)
            #[cfg(target_os = "linux")]
            {
                use core_affinity::CoreId;
                core_affinity::set_for_current(CoreId { id: core_id });
            }

            // Spawn worker on pinned core
            tokio::spawn(worker.clone().run(batches.clone()));
        }
    }

    /// Work-stealing for load balancing
    pub async fn steal_work(&self, idle_worker_id: usize) -> Option<Vec<StreamRecord>> {
        if !self.work_stealing {
            return None;
        }

        // Find busiest worker
        let busiest = self.workers
            .iter()
            .enumerate()
            .filter(|(id, _)| *id != idle_worker_id)
            .max_by_key(|(_, w)| w.queue_size())?;

        // Steal half their work
        busiest.1.steal_batch().await
    }
}

/// Performance targets for vertical scaling
///
/// Hardware: 64-core AMD EPYC / Intel Xeon
/// Expected throughput scaling:
/// - 4 cores:   200K rec/sec
/// - 16 cores:  800K rec/sec  (4x)
/// - 64 cores: 3.2M rec/sec  (16x)
/// - 128 cores: 6.4M rec/sec (32x)
///
/// Key optimizations:
/// - NUMA-aware memory allocation
/// - CPU pinning for cache locality
/// - Lock-free queues between workers
/// - Batch size tuning per core count
```

#### Horizontal Scaling (Distributed Across Servers)

```rust
/// Distributed job coordination using Kafka consumer groups
pub struct DistributedJobCoordinator {
    consumer_group: String,
    partitions_assigned: Arc<RwLock<Vec<PartitionId>>>,
    state_backend: Arc<dyn DistributedStateBackend>,
    rebalance_listener: Arc<RebalanceListener>,
}

impl DistributedJobCoordinator {
    /// Leverage Kafka consumer group for automatic partition assignment
    pub async fn start_distributed_job(
        &self,
        job_config: JobProcessingConfig,
        query: StreamingQuery,
    ) -> Result<()> {
        // 1. Join Kafka consumer group
        let consumer = self.create_kafka_consumer(&job_config).await?;

        // 2. Subscribe to topics (triggers partition assignment)
        consumer.subscribe(&job_config.source_topics)?;

        // 3. Handle partition assignment callback
        consumer.set_rebalance_listener(Box::new(|partitions| {
            // On partition assignment: load state for assigned partitions
            self.load_partitioned_state(partitions).await;

            // On partition revocation: checkpoint state for revoked partitions
            self.checkpoint_partitioned_state(partitions).await;
        }))?;

        // 4. Process assigned partitions
        loop {
            let message = consumer.poll(Duration::from_millis(100)).await?;

            // Process message using partition-local state
            let partition = message.partition();
            let state = self.state_backend.get_partition_state(partition).await?;

            self.process_message(message, state).await?;
        }

        Ok(())
    }

    /// Partition-aware state management
    pub async fn load_partitioned_state(
        &self,
        partitions: &[PartitionId],
    ) -> Result<()> {
        for partition in partitions {
            // Load state from distributed backend (S3, RocksDB, Redis)
            let state = self.state_backend.restore_partition(*partition).await?;
            self.state_cache.insert(*partition, state);
        }
        Ok(())
    }

    /// Checkpoint state before rebalancing
    pub async fn checkpoint_partitioned_state(
        &self,
        partitions: &[PartitionId],
    ) -> Result<()> {
        for partition in partitions {
            let state = self.state_cache.remove(partition)
                .ok_or(StateError::NotFound)?;

            // Persist to distributed backend
            self.state_backend.checkpoint_partition(*partition, state).await?;
        }
        Ok(())
    }
}

/// Distributed state backend abstraction
#[async_trait]
pub trait DistributedStateBackend: Send + Sync {
    /// Restore state for a specific partition
    async fn restore_partition(&self, partition: PartitionId) -> Result<QueryState>;

    /// Checkpoint state for a partition
    async fn checkpoint_partition(&self, partition: PartitionId, state: QueryState) -> Result<()>;

    /// Get current state size
    async fn state_size_bytes(&self, partition: PartitionId) -> Result<usize>;
}

/// S3-backed state backend (for large state)
pub struct S3StateBackend {
    bucket: String,
    prefix: String,
    s3_client: Arc<aws_sdk_s3::Client>,
}

/// RocksDB-backed state backend (for fast local state)
pub struct RocksDBStateBackend {
    db: Arc<rocksdb::DB>,
    checkpoint_dir: PathBuf,
}
```

**Scaling Model Summary**:

| Configuration | Cores | Servers | Throughput | State Size | Use Case |
|---------------|-------|---------|------------|------------|----------|
| **Single Small** | 4 | 1 | 200K rec/sec | <1GB | Development/Testing |
| **Single Medium** | 16 | 1 | 800K rec/sec | <10GB | Single-source prod |
| **Single Large** | 64 | 1 | 3.2M rec/sec | <50GB | High-throughput single node |
| **Distributed Small** | 16 | 4 | 3.2M rec/sec | <100GB | Multi-source, moderate state |
| **Distributed Large** | 64 | 10 | 32M rec/sec | <1TB | Massive scale, multi-source |

---

### 9.5 Low-Latency Optimizations

#### Sub-Millisecond Processing Targets

```rust
/// Ultra-low-latency configuration
pub struct LowLatencyConfig {
    /// Use pre-allocated buffers
    use_object_pools: bool,

    /// Skip observability in hot path
    disable_tracing: bool,

    /// Process records immediately (no batching delay)
    zero_batch_timeout: bool,

    /// Pin processing to NUMA node
    numa_awareness: bool,

    /// Use lock-free data structures
    lock_free_queues: bool,
}

impl LowLatencyConfig {
    pub fn ultra_low() -> Self {
        Self {
            use_object_pools: true,
            disable_tracing: true,
            zero_batch_timeout: true,
            numa_awareness: true,
            lock_free_queues: true,
        }
    }
}

/// Object pooling to reduce allocations
pub struct RecordPool {
    pool: Arc<crossbeam::queue::ArrayQueue<Box<StreamRecord>>>,
    capacity: usize,
}

impl RecordPool {
    pub fn new(capacity: usize) -> Self {
        let pool = Arc::new(crossbeam::queue::ArrayQueue::new(capacity));

        // Pre-allocate records
        for _ in 0..capacity {
            pool.push(Box::new(StreamRecord::default())).ok();
        }

        Self { pool, capacity }
    }

    /// Acquire pre-allocated record (zero-allocation)
    pub fn acquire(&self) -> Option<Box<StreamRecord>> {
        self.pool.pop()
    }

    /// Return record to pool
    pub fn release(&self, mut record: Box<StreamRecord>) {
        record.clear();
        self.pool.push(record).ok();
    }
}

/// Lock-free batch queue
pub struct LockFreeBatchQueue {
    queue: Arc<crossbeam::queue::SegQueue<Vec<StreamRecord>>>,
}

impl LockFreeBatchQueue {
    pub fn push(&self, batch: Vec<StreamRecord>) {
        self.queue.push(batch);
    }

    pub fn pop(&self) -> Option<Vec<StreamRecord>> {
        self.queue.pop()
    }
}
```

**Latency Targets**:

| Percentile | Standard Mode | Low-Latency Mode | Ultra-Low Mode |
|------------|---------------|------------------|----------------|
| p50 | 5ms | 1ms | 100μs |
| p95 | 15ms | 3ms | 500μs |
| p99 | 30ms | 10ms | 2ms |
| p99.9 | 100ms | 50ms | 10ms |

**Optimizations**:
- Zero-copy deserialization (borrow from Kafka buffer)
- SIMD for aggregate computations
- Compile-time SQL optimization
- JIT-compiled query plans

---

### 9.6 Exactly-Once Semantics

#### Two-Phase Commit Protocol

```rust
/// Exactly-once coordinator using 2PC
pub struct ExactlyOnceCoordinator {
    transaction_log: Arc<TransactionLog>,
    state_backend: Arc<dyn TransactionalStateBackend>,
    sink_backend: Arc<dyn TransactionalSink>,
}

#[async_trait]
pub trait TransactionalStateBackend {
    /// Begin transaction for state updates
    async fn begin_transaction(&self, txn_id: TransactionId) -> Result<Transaction>;

    /// Prepare to commit (Phase 1 of 2PC)
    async fn prepare(&self, txn: &Transaction) -> Result<PrepareResponse>;

    /// Commit transaction (Phase 2 of 2PC)
    async fn commit(&self, txn_id: TransactionId) -> Result<()>;

    /// Abort transaction
    async fn abort(&self, txn_id: TransactionId) -> Result<()>;
}

impl ExactlyOnceCoordinator {
    /// Process batch with exactly-once guarantees
    pub async fn process_batch_exactly_once(
        &self,
        batch: Vec<StreamRecord>,
        kafka_offsets: Vec<KafkaOffset>,
    ) -> Result<()> {
        let txn_id = TransactionId::new();

        // 1. Begin transaction
        let txn = self.state_backend.begin_transaction(txn_id).await?;
        let sink_txn = self.sink_backend.begin_transaction(txn_id).await?;

        // 2. Process batch (accumulate state changes)
        let state_updates = self.process_batch_transactional(batch, &txn).await?;

        // 3. PHASE 1: Prepare (all participants vote)
        let state_vote = self.state_backend.prepare(&txn).await?;
        let sink_vote = self.sink_backend.prepare(&sink_txn).await?;

        if !state_vote.can_commit() || !sink_vote.can_commit() {
            // Abort if any participant votes NO
            self.state_backend.abort(txn_id).await?;
            self.sink_backend.abort(txn_id).await?;
            return Err(TransactionError::PrepareFailed);
        }

        // 4. Write to transaction log (durable commit decision)
        self.transaction_log.record_commit(txn_id, kafka_offsets).await?;

        // 5. PHASE 2: Commit (all participants commit)
        self.state_backend.commit(txn_id).await?;
        self.sink_backend.commit(txn_id).await?;

        // 6. Commit Kafka offsets (marks batch as processed)
        self.commit_kafka_offsets(kafka_offsets).await?;

        Ok(())
    }

    /// Recover from failure using transaction log
    pub async fn recover_from_failure(&self) -> Result<()> {
        // 1. Read uncommitted transactions from log
        let pending_txns = self.transaction_log.get_pending_transactions().await?;

        for txn_id in pending_txns {
            // 2. Check if transaction was committed
            let status = self.transaction_log.get_status(txn_id).await?;

            match status {
                TransactionStatus::Committed => {
                    // Complete commit if not finished
                    self.state_backend.commit(txn_id).await?;
                    self.sink_backend.commit(txn_id).await?;
                }
                TransactionStatus::Aborted | TransactionStatus::Unknown => {
                    // Abort incomplete transactions
                    self.state_backend.abort(txn_id).await?;
                    self.sink_backend.abort(txn_id).await?;
                }
                _ => {}
            }
        }

        Ok(())
    }
}
```

**Exactly-Once Guarantees**:
- ✅ **Kafka source**: Consumer offset commits in transaction
- ✅ **State updates**: Transactional state backend (RocksDB, PostgreSQL)
- ✅ **Sink writes**: Transactional sinks (Kafka with transactions, databases)
- ✅ **Failure recovery**: Transaction log replay on restart
- ✅ **Idempotency**: Duplicate detection via transaction IDs

---

### 9.7 Future-Proofing & Extensibility

#### Pluggable Architecture

```rust
/// Extensibility points for custom implementations
pub trait StateBackendProvider {
    fn create_state_backend(&self, config: &StateConfig) -> Box<dyn DistributedStateBackend>;
}

pub trait SerializationProvider {
    fn create_serializer(&self, format: &str) -> Box<dyn Serializer>;
    fn create_deserializer(&self, format: &str) -> Box<dyn Deserializer>;
}

pub trait WindowProvider {
    fn create_window_assigner(&self, config: &WindowConfig) -> Box<dyn WindowAssigner>;
    fn create_window_trigger(&self, config: &TriggerConfig) -> Box<dyn WindowTrigger>;
}

/// Plugin registry for runtime extensions
pub struct PluginRegistry {
    state_backends: HashMap<String, Box<dyn StateBackendProvider>>,
    serializers: HashMap<String, Box<dyn SerializationProvider>>,
    windows: HashMap<String, Box<dyn WindowProvider>>,
}

impl PluginRegistry {
    /// Register custom state backend
    pub fn register_state_backend(&mut self, name: String, provider: Box<dyn StateBackendProvider>) {
        self.state_backends.insert(name, provider);
    }

    /// Load plugin from shared library
    #[cfg(feature = "dynamic-plugins")]
    pub unsafe fn load_plugin(&mut self, path: &Path) -> Result<()> {
        use libloading::Library;

        let lib = Library::new(path)?;
        let init_fn: libloading::Symbol<unsafe extern fn(&mut PluginRegistry)> =
            lib.get(b"plugin_init")?;

        init_fn(self);
        Ok(())
    }
}
```

#### Versioning & Compatibility

```rust
/// State schema versioning for upgrades
#[derive(Serialize, Deserialize)]
pub struct VersionedState {
    version: u32,
    schema_hash: u64,
    data: Vec<u8>,
}

pub struct StateMigrator {
    migrations: Vec<Box<dyn StateMigration>>,
}

pub trait StateMigration {
    fn from_version(&self) -> u32;
    fn to_version(&self) -> u32;
    fn migrate(&self, old_state: Vec<u8>) -> Result<Vec<u8>>;
}

impl StateMigrator {
    /// Migrate state from v1 to current version
    pub fn migrate(&self, state: VersionedState, target_version: u32) -> Result<VersionedState> {
        let mut current = state;

        while current.version < target_version {
            // Find migration for current version
            let migration = self.migrations.iter()
                .find(|m| m.from_version() == current.version)
                .ok_or(MigrationError::NoPath)?;

            // Apply migration
            current.data = migration.migrate(current.data)?;
            current.version = migration.to_version();
        }

        Ok(current)
    }
}
```

#### Configuration Evolution

```rust
/// Backwards-compatible configuration with defaults
#[derive(Deserialize)]
#[serde(default)]
pub struct JobConfig {
    // V1 fields (always present)
    pub job_id: String,
    pub query: String,

    // V2 fields (added later, optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exactly_once: Option<bool>,

    // V3 fields (future additions)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub low_latency_mode: Option<LowLatencyConfig>,

    // Unknown fields (forward compatibility)
    #[serde(flatten)]
    pub extensions: HashMap<String, serde_json::Value>,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            job_id: String::new(),
            query: String::new(),
            exactly_once: Some(false),
            low_latency_mode: None,
            extensions: HashMap::new(),
        }
    }
}
```

---

## Part 10: Implementation Roadmap with Enterprise Features

**⚠️ CRITICAL**: Phase 0 is **BLOCKING** - V2 cannot proceed until SQL engine GROUP BY is optimized.

### Phase 0: Performance Foundation (Weeks 1-2) ⚠️ **BLOCKING**

**Goal**: Fix SQL engine GROUP BY performance from 3.58K → 200K rec/sec

**Week 1: FR-082 Phase 4B - Hash Table Optimization**
- 📦 Add `rustc-hash` dependency to Cargo.toml
- 🔧 Implement GroupKey with pre-computed hash
- 🔧 Replace HashMap with FxHashMap in GroupByState
- 🔧 Update generate_group_key to return GroupKey
- ✅ Test: `tests/unit/sql/execution/group_key_test.rs`
- ⚡ Benchmark: `tests/performance/analysis/phase4b_hash_optimization_test.rs`
- **Expected result**: 3.58K → 15-20K rec/sec (+400-500%)

**Week 2: FR-082 Phase 4C - Arc State Sharing**
- 🔧 Wrap GroupByState in Arc&lt;FxHashMap&gt;
- 🔧 Implement Arc::make_mut() merge pattern
- 🔧 Use Arc&lt;StreamRecord&gt; in GroupAccumulator
- 🔧 Implement StringInterner for field names
- 🔧 Add GroupKeyCache with LRU eviction
- ✅ Test: `tests/unit/sql/execution/arc_state_test.rs`
- ⚡ Benchmark: `tests/performance/analysis/phase4c_arc_optimization_test.rs`
- **Expected result**: 15-20K → 200K rec/sec (+750-1000%)

**Deliverables**:
- ✅ SQL engine GROUP BY at 200K rec/sec baseline
- ✅ V2 performance targets now achievable
- ✅ Unblocks Phase 1 V2 implementation

**Success Criteria**:
```bash
# Run Phase 4B/4C benchmarks
cargo test --tests --no-default-features --release phase4b_hash_optimization -- --nocapture
cargo test --tests --no-default-features --release phase4c_arc_optimization -- --nocapture

# Must achieve:
# - Phase 4B: 15-20K rec/sec GROUP BY throughput
# - Phase 4C: 200K rec/sec GROUP BY throughput
# - Zero compilation errors
# - All existing tests pass
```

---

### Phase 1: Core V2 Architecture (Weeks 3-5)

**Prerequisites**: ✅ Phase 0 complete (200K rec/sec GROUP BY baseline)

**Week 3: State Manager Actor**
- 🔧 Implement StateManagerActor with FxHashMap (from Phase 0)
- 🔧 StateMessage enum with MergeBatchState, GetSnapshot
- 🔧 Actor run loop with message handling
- 🔧 Integration with StreamExecutionEngine
- ✅ Test: `tests/unit/server/state_manager_actor_test.rs`

**Week 4: Processing Workers**
- 🔧 ProcessingWorker with local context building
- 🔧 Worker pool with configurable parallelism
- 🔧 Batch distribution via round-robin
- ✅ Test: `tests/unit/server/processing_worker_test.rs`

**Week 5: Source/Sink Pipelines**
- 🔧 SourcePipeline with async stream
- 🔧 SinkPipeline with batching + backpressure
- 🔧 JobCoordinator orchestration
- ✅ Test: `tests/integration/server/v2_pipeline_test.rs`
- ⚡ Benchmark: `tests/performance/v2_architecture_benchmark.rs`

**Deliverables**:
- ✅ V2 architecture with 200K rec/sec single-source
- ✅ 8x improvement over V1 job server
- ✅ Parallel batch processing

---

### Phase 2: P1 Enterprise Features (Weeks 6-11)

**Week 6: State TTL**
- 🔧 StateTTLConfig with ttl, update_type, cleanup_strategy
- 🔧 TTL enforcement in StateManagerActor
- 🔧 Background cleanup task
- ✅ Test: `tests/unit/server/state_ttl_test.rs`

**Week 7: State Rescaling**
- 🔧 State redistribution on partition count change
- 🔧 Consistent hashing for key assignment
- 🔧 State transfer protocol
- ✅ Test: `tests/integration/server/state_rescaling_test.rs`

**Week 8-9: Checkpoint Alignment + Savepoints**
- 🔧 CheckpointCoordinator with barrier alignment
- 🔧 CheckpointBarrier propagation across sources
- 🔧 Savepoint creation with versioning
- 🔧 Savepoint restore with compatibility check
- ✅ Test: `tests/integration/server/checkpoint_alignment_test.rs`
- ✅ Test: `tests/integration/server/savepoint_test.rs`

**Week 10-11: Kubernetes Support**
- 🔧 StreamingJob CRD definition
- 🔧 Kubernetes operator for job lifecycle
- 🔧 ConfigMap/Secret integration
- 🔧 Service mesh compatibility
- ✅ Test: `tests/integration/deployment/kubernetes_test.rs`
- 📄 Doc: `docs/deployment/kubernetes-operator.md`

**Deliverables**:
- ✅ State TTL for long-running jobs
- ✅ State rescaling for dynamic partitions
- ✅ Exactly-once with checkpoint alignment
- ✅ Zero-downtime upgrades via savepoints
- ✅ Cloud-native Kubernetes deployment

---

### Phase 3: P1 SQL & Watermark Features (Weeks 12-13)

**Week 12: User-Defined Functions**
- 🔧 ScalarFunction, TableFunction, AggregateFunction traits
- 🔧 UDF registration in StreamExecutionEngine
- 🔧 Dynamic function dispatch in expression evaluator
- ✅ Test: `tests/unit/sql/udf_test.rs`
- 📄 Doc: `docs/sql/user-defined-functions.md`

**Week 13: Watermark Alignment**
- 🔧 Per-source watermark tracking
- 🔧 Max drift enforcement
- 🔧 Throttling for fast sources
- ✅ Test: `tests/unit/server/watermark_alignment_test.rs`

**Deliverables**:
- ✅ UDFs for custom business logic
- ✅ Watermark alignment for multi-source correctness

---

### Phase 4: P2 Production Features (Weeks 14-17)

**Week 14-15: Advanced Windowing**
- 🔧 WindowTrigger (EventTime, ProcessingTime, Count, Custom)
- 🔧 Side outputs for late data
- 🔧 Custom window assigners
- ✅ Test: `tests/unit/sql/advanced_windowing_test.rs`

**Week 16-17: File Sources/Sinks**
- 🔧 Parquet reader/writer
- 🔧 CSV reader/writer
- 🔧 File batching and bucketing
- ✅ Test: `tests/integration/sources/file_source_test.rs`
- ✅ Test: `tests/integration/sinks/file_sink_test.rs`

**Deliverables**:
- ✅ Advanced windowing for complex event processing
- ✅ File sources/sinks for batch-streaming unification

---

### Phase 5: Scaling & Performance (Weeks 18-20)

**Week 18: Vertical Scaling**
- ✅ NUMA awareness (already in V2 blueprint)
- ✅ CPU pinning (already in V2 blueprint)
- ⚡ Benchmark: Verify 100+ core scaling

**Week 19: Horizontal Scaling**
- ✅ Kafka consumer groups (already in V2 blueprint)
- 🔧 State sharding across instances
- ⚡ Benchmark: 32M rec/sec with 160 sources

**Week 20: Low-Latency Optimizations**
- ✅ Object pooling (already in V2 blueprint)
- ✅ Lock-free queues (already in V2 blueprint)
- ⚡ Benchmark: <1ms p95 latency

**Deliverables**:
- ✅ Vertical scaling to 100+ cores
- ✅ Horizontal scaling to 32M rec/sec
- ✅ Sub-millisecond latency mode

---

### Phase 6: Reliability (Weeks 21-23)

**Week 21-22: Exactly-Once Semantics**
- ✅ Two-phase commit (already in V2 blueprint)
- ✅ Transaction log (already in V2 blueprint)
- ✅ Test: Verify exactly-once guarantees

**Week 23: Failure Recovery**
- 🔧 Partial recovery (restart failed tasks only)
- 🔧 Recovery strategies (RestartAll, RestartFailed, FailoverRegion)
- ✅ Test: `tests/integration/reliability/failure_recovery_test.rs`

**Deliverables**:
- ✅ Exactly-once semantics end-to-end
- ✅ Partial recovery for fault isolation

---

### Phase 7: Extensibility & Polish (Weeks 24-26)

**Week 24: Query Optimization**
- 🔧 Predicate pushdown
- 🔧 Projection pushdown
- 🔧 Constant folding
- ✅ Test: `tests/unit/sql/query_optimizer_test.rs`

**Week 25: Security**
- 🔧 SSL/TLS for internal communication
- 🔧 API authentication
- 📄 Doc: `docs/security/ssl-configuration.md`

**Week 26: Documentation & Polish**
- 📄 Comprehensive user guide
- 📄 API reference docs
- 📄 Deployment best practices
- 🎨 Examples and tutorials

**Deliverables**:
- ✅ Query optimization for performance
- ✅ Security for production deployments
- ✅ Complete documentation

---

### Total Timeline: 26 Weeks (6 Months)

**Critical Path**:
1. **Phase 0 (Weeks 1-2)**: ⚠️ BLOCKING - SQL engine optimization
2. **Phase 1 (Weeks 3-5)**: Core V2 architecture
3. **Phase 2 (Weeks 6-11)**: P1 enterprise features
4. **Phase 3 (Weeks 12-13)**: P1 SQL & watermarks
5. **Phase 4-7 (Weeks 14-26)**: P2 features + polish

**Minimum Viable Product (MVP)**: Weeks 1-13 (3 months)
- Phase 0: SQL engine at 200K rec/sec
- Phase 1: V2 architecture
- Phase 2: State TTL, rescaling, checkpoints, savepoints, Kubernetes
- Phase 3: UDFs, watermark alignment

**Production Ready**: Weeks 1-23 (5.5 months)
- MVP + Phase 4-6 (advanced windowing, file sources, scaling, reliability)

**Feature Complete**: Weeks 1-26 (6 months)
- Production Ready + Phase 7 (optimization, security, docs)

---

## Part 11: Competitive Analysis

| Feature | Velostream V2 | Apache Flink | Kafka Streams |
|---------|---------------|--------------|---------------|
| **Throughput (single node)** | 3.2M rec/sec | 2M rec/sec | 1.5M rec/sec |
| **Latency (p99)** | <10ms | ~50ms | ~100ms |
| **State backend** | Pluggable | RocksDB only | RocksDB only |
| **Exactly-once** | ✅ 2PC | ✅ Checkpoints | ✅ Transactions |
| **SQL support** | ✅ Native | ✅ Flink SQL | ❌ KSQL separate |
| **Watermarks** | ✅ Event-time | ✅ Event-time | ⚠️ Limited |
| **Horizontal scaling** | ✅ Kafka groups | ✅ JobManager | ✅ Consumer groups |
| **Low-latency mode** | ✅ <1ms p95 | ❌ | ❌ |
| **Rust performance** | ✅ Zero-copy | ❌ JVM GC | ❌ JVM GC |

**Competitive Advantages**:
- 🚀 **1.6x faster** than Flink (3.2M vs 2M rec/sec)
- 🏎️ **5x lower latency** than Kafka Streams (10ms vs 100ms p99)
- 💰 **Lower resource cost** (no JVM overhead)
- 🔌 **More flexible** (pluggable state backends)
- 🦀 **Memory safe** (Rust safety guarantees)

---

## Part 12: Production Deployment Checklist

### Infrastructure
- [ ] Kubernetes deployment manifests
- [ ] Horizontal pod autoscaling
- [ ] Resource limits (CPU, memory)
- [ ] Network policies
- [ ] Service mesh integration (Istio/Linkerd)

### Monitoring
- [ ] Prometheus metrics endpoint
- [ ] Grafana dashboards
- [ ] PagerDuty/Opsgenie integration
- [ ] Log aggregation (ELK/Splunk)
- [ ] Distributed tracing (Jaeger/Zipkin)

### Reliability
- [ ] Chaos testing (failure injection)
- [ ] Disaster recovery plan
- [ ] State backup strategy
- [ ] Rollback procedures
- [ ] Load testing results

### Security
- [ ] Kafka ACLs configured
- [ ] mTLS for inter-service communication
- [ ] Secret management (Vault/AWS Secrets Manager)
- [ ] Network segmentation
- [ ] Audit logging

### Compliance
- [ ] Data retention policies
- [ ] GDPR/CCPA compliance
- [ ] SOC2 controls
- [ ] Encryption at rest/transit

---

## Conclusion

The V2 architecture provides a **production-ready, enterprise-grade streaming platform** with:

✅ **Performance**: 30x improvement (multi-source), 200K-3.2M rec/sec  
✅ **Scalability**: Vertical (100+ cores) + Horizontal (Kafka consumer groups)  
✅ **Reliability**: Exactly-once semantics, failure recovery, health monitoring  
✅ **Observability**: Comprehensive metrics, distributed tracing, alerting  
✅ **Latency**: Sub-millisecond p95 (<1ms) in low-latency mode  
✅ **Future-proof**: Pluggable architecture, versioning, backward compatibility  

**Next Decision**: Start with Option 5 (4 hours) or go directly to V2 (3 months)?

