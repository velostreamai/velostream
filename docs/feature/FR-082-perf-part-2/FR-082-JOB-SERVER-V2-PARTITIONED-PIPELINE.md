# Job Server V2: Hash-Partitioned Pipeline Architecture

## Executive Summary

**Current V1 Performance**: 23K rec/sec (33x slower than pure SQL: 790K rec/sec)

**V2 Design Goal**: Hash-partitioned state managers for **ultra-low-latency, true parallelism, linear scaling**

**Key Innovation**: Partition state across N managers (one per CPU core) using GROUP BY key hashing

**Target Performance**: 1.5M rec/sec on 8 cores (65x improvement over V1)

---

## Architecture Philosophy

### Hash-Partitioned Pipeline Design

```
              Hash(group_key) % N
              ┌─────────────────┐
Source ──► Router              │
              └─────────────────┘
                     │
         ┌───────────┼───────────┐
         │           │           │
         ▼           ▼           ▼
    Partition 0  Partition 1  Partition N-1
    [State Mgr]  [State Mgr]  [State Mgr]
    [200K r/s]   [200K r/s]   [200K r/s]
         │           │           │
         └───────────┼───────────┘
                     ▼
                  Output
             N × 200K rec/sec
```

**Result**: N cores = N × 200K rec/sec (linear scaling)

### Why This Architecture Works

**GROUP BY Queries Have Natural Partitioning**:
- Each group key maps to exactly ONE partition (consistent hashing)
- Partitions operate **independently** (no cross-partition coordination)
- No locks, no contention, perfect CPU cache locality

**Example**: GROUP BY trader_id, symbol
```sql
SELECT trader_id, symbol, COUNT(*), AVG(price)
FROM trades
GROUP BY trader_id, symbol
```

Record with `(trader_id=T1, symbol=SYM1)` → always routes to Partition 5
- All aggregations for (T1, SYM1) happen in Partition 5 only
- Zero coordination with other partitions
- Lock-free state updates

---

## Part 0: Performance Foundation (Same as Previous)

### FR-082 Phase 4B + 4C: SQL Engine Optimization

**Critical Prerequisites**: V2 requires Phase 4B/4C optimizations for 200K rec/sec per-partition baseline

**Phase 4B** (Week 1): FxHashMap + GroupKey with pre-computed hash
- Target: 3.58K → 15-20K rec/sec

**Phase 4C** (Week 2): Arc<FxHashMap> + Arc<StreamRecord> + string interning
- Target: 15-20K → 200K rec/sec

**Status**: Must be completed BEFORE starting V2 implementation

See original blueprint Part 0 for full details.

---

## Part 1: Hash-Partitioned Architecture

### Core Design Principles

1. **Hash-Based Partition Routing**: GROUP BY keys deterministically route to partitions
2. **Lock-Free State Per Partition**: Each partition owns its state (no Arc<Mutex>)
3. **CPU Core Affinity**: Pin each partition to a dedicated CPU core
4. **Continuous Streaming**: Process records individually (not batched) for ultra-low-latency
5. **Backpressure Propagation**: Per-partition channel monitoring with automatic throttling

### System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                        Data Source Layer                          │
│                    (Kafka, File, Mock, etc.)                      │
└────────────────────────────────┬─────────────────────────────────┘
                                 │ StreamRecord stream
                                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Hash-Based Router                             │
│                                                                    │
│  fn route(record: &StreamRecord, query: &Query) -> usize {       │
│      let group_key = extract_group_key(record, query);           │
│      hash(group_key) % num_partitions                            │
│  }                                                                │
└───┬──────────────┬──────────────┬──────────────┬─────────────────┘
    │              │              │              │
    │ Partition 0  │ Partition 1  │ Partition 2  │ Partition N-1
    ▼              ▼              ▼              ▼
┌────────┐    ┌────────┐    ┌────────┐    ┌────────┐
│ STATE  │    │ STATE  │    │ STATE  │    │ STATE  │
│ MANAGER│    │ MANAGER│    │ MANAGER│    │ MANAGER│
│        │    │        │    │        │    │        │
│ Core 0 │    │ Core 1 │    │ Core 2 │    │ Core N │
│ Pinned │    │ Pinned │    │ Pinned │    │ Pinned │
└────┬───┘    └────┬───┘    └────┬───┘    └────┬───┘
     │             │             │             │
     │  Per-partition GROUP BY state (FxHashMap)
     │  No locks! No contention!
     │             │             │             │
     └─────────────┴─────────────┴─────────────┘
                         │
                         ▼
              ┌────────────────────┐
              │   Output Merger    │
              │  (if ordering req) │
              └──────────┬─────────┘
                         │
                         ▼
              ┌────────────────────┐
              │   Data Sink Layer  │
              │   (Kafka, etc.)    │
              └────────────────────┘
```

---

## Part 2: Core Components

### 1. Hash Router (Partition Assignment)

**Purpose**: Deterministically route records to partitions based on GROUP BY keys

```rust
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};

/// Routes records to partitions using consistent hashing
pub struct HashRouter {
    num_partitions: usize,
    query: StreamingQuery,
}

impl HashRouter {
    pub fn new(num_partitions: usize, query: StreamingQuery) -> Self {
        Self { num_partitions, query }
    }

    /// Determine which partition this record belongs to
    pub fn route(&self, record: &StreamRecord) -> usize {
        // Extract GROUP BY columns from query
        let group_by_columns = self.query.get_group_by_columns();

        if group_by_columns.is_empty() {
            // No GROUP BY: round-robin or single partition
            return 0;
        }

        // Build group key from record
        let mut hasher = FxHasher::default();
        for col in &group_by_columns {
            if let Some(value) = record.fields.get(col) {
                value.hash(&mut hasher);
            }
        }

        let hash = hasher.finish();
        (hash % self.num_partitions as u64) as usize
    }

    /// Route a batch of records (returns Vec<Vec<StreamRecord>>)
    pub fn route_batch(&self, records: Vec<StreamRecord>) -> Vec<Vec<StreamRecord>> {
        let mut partitions: Vec<Vec<StreamRecord>> =
            (0..self.num_partitions).map(|_| Vec::new()).collect();

        for record in records {
            let partition_id = self.route(&record);
            partitions[partition_id].push(record);
        }

        partitions
    }
}
```

**Performance**:
- Hash computation: ~50ns per record
- Partition assignment: O(1)
- **Total routing overhead**: <100ns per record (negligible)

**Key Properties**:
- **Deterministic**: Same group key → same partition (always)
- **Load Balancing**: FxHash provides good distribution
- **Partition Affinity**: Each trader_id/symbol pair stays in one partition

---

### 2. Partition State Manager (Lock-Free Per-Partition)

**Purpose**: Each partition owns its state independently (no locks, no coordination)

```rust
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use std::sync::Arc;

/// Single-partition state manager (no locks needed!)
pub struct PartitionStateManager {
    partition_id: usize,
    receiver: mpsc::UnboundedReceiver<PartitionMessage>,

    // Per-partition state (Phase 4B/4C optimizations)
    query_states: FxHashMap<String, PartitionQueryState>,

    // Observability
    metrics: PartitionMetrics,

    // System fields tracking
    watermark_manager: WatermarkManager,
}

pub struct PartitionQueryState {
    query_id: String,

    // Phase 4B: FxHashMap with GroupKey
    // Phase 4C: Arc wrapper for cheap cloning
    group_by_state: Arc<FxHashMap<GroupKey, GroupAccumulator>>,

    // Window state (for TUMBLING/SLIDING/SESSION windows)
    window_states: Vec<WindowState>,

    // ROWS WINDOW buffers (Scenario 1)
    rows_window_buffers: FxHashMap<PartitionKey, VecDeque<Arc<StreamRecord>>>,
}

pub enum PartitionMessage {
    /// Process a single record (ultra-low-latency mode)
    ProcessRecord {
        query_id: String,
        record: StreamRecord,
        response: oneshot::Sender<Option<Arc<StreamRecord>>>,
    },

    /// Process a batch (higher throughput mode)
    ProcessBatch {
        query_id: String,
        records: Vec<StreamRecord>,
        response: oneshot::Sender<Vec<Arc<StreamRecord>>>,
    },

    /// Get partition state snapshot (for recovery/debugging)
    GetSnapshot {
        query_id: String,
        response: oneshot::Sender<Arc<FxHashMap<GroupKey, GroupAccumulator>>>,
    },

    /// Shutdown this partition
    Shutdown,
}

impl PartitionStateManager {
    pub async fn run(mut self) {
        // Pin this task to a specific CPU core for cache locality
        #[cfg(target_os = "linux")]
        {
            use core_affinity::CoreId;
            let core_id = CoreId { id: self.partition_id };
            core_affinity::set_for_current(core_id);
        }

        while let Some(msg) = self.receiver.recv().await {
            match msg {
                PartitionMessage::ProcessRecord { query_id, record, response } => {
                    let start = std::time::Instant::now();

                    // Get or create query state
                    let state = self.query_states.entry(query_id.clone())
                        .or_insert_with(|| PartitionQueryState::new(query_id));

                    // Process record through SQL engine (lock-free!)
                    let result = self.process_record_internal(state, record).await;

                    // Update metrics
                    self.metrics.record_processing_time(start.elapsed());
                    self.metrics.increment_records_processed();

                    response.send(result).ok();
                }

                PartitionMessage::ProcessBatch { query_id, records, response } => {
                    let start = std::time::Instant::now();

                    let state = self.query_states.entry(query_id.clone())
                        .or_insert_with(|| PartitionQueryState::new(query_id));

                    let mut results = Vec::with_capacity(records.len());
                    for record in records {
                        if let Some(output) = self.process_record_internal(state, record).await {
                            results.push(output);
                        }
                    }

                    self.metrics.record_processing_time(start.elapsed());
                    self.metrics.add_records_processed(results.len());

                    response.send(results).ok();
                }

                PartitionMessage::GetSnapshot { query_id, response } => {
                    let snapshot = self.query_states.get(&query_id)
                        .map(|s| Arc::clone(&s.group_by_state));
                    response.send(snapshot.unwrap_or_default()).ok();
                }

                PartitionMessage::Shutdown => {
                    break;
                }
            }
        }
    }

    async fn process_record_internal(
        &mut self,
        state: &mut PartitionQueryState,
        record: StreamRecord,
    ) -> Option<Arc<StreamRecord>> {
        // Build group key
        let group_key = self.extract_group_key(&record);

        // Phase 4C: Arc::make_mut for copy-on-write
        let groups = Arc::make_mut(&mut state.group_by_state);

        // Update or insert accumulator (lock-free!)
        let accumulator = groups.entry(group_key)
            .or_insert_with(|| GroupAccumulator::new());

        // Update aggregations
        accumulator.update(&record);

        // Check watermarks for window emission
        if let Some(window_output) = self.check_window_emission(state, &record) {
            return Some(Arc::new(window_output));
        }

        // For EMIT CHANGES, return updated aggregate
        if self.is_emit_changes() {
            return Some(Arc::new(accumulator.to_stream_record()));
        }

        None
    }

    fn extract_group_key(&self, record: &StreamRecord) -> GroupKey {
        // Use system fields if needed (_PARTITION, _TIMESTAMP, etc.)
        // Phase 4C: Use string interner and key cache for efficiency
        // ... implementation ...
        todo!()
    }
}
```

**Benefits**:
- ✅ **Zero lock contention**: Each partition is single-threaded
- ✅ **Zero cross-partition coordination**: No shared state
- ✅ **CPU cache locality**: Core pinning keeps L1/L2 cache hot
- ✅ **Linear scaling**: N partitions = N × throughput
- ✅ **Phase 4B/4C optimized**: Uses FxHashMap + Arc patterns

**Performance Per Partition**:
- Message receive: ~500ns
- GROUP BY update: ~50ns per key (FxHashMap)
- Watermark check: ~100ns
- **Total per-record**: ~1-2µs → **500K-1M rec/sec per partition**

---

### 3. Partitioned Job Coordinator

**Purpose**: Orchestrate N partitions + router + source/sink pipelines

```rust
use tokio::sync::mpsc;
use std::sync::Arc;

pub struct PartitionedJobCoordinator {
    config: PartitionedJobConfig,
    num_partitions: usize,
}

pub struct PartitionedJobConfig {
    /// Number of partitions (default: num_cpus::get())
    pub num_partitions: Option<usize>,

    /// Processing mode: Individual (low-latency) or Batch (high-throughput)
    pub processing_mode: ProcessingMode,

    /// Batch size (only used in Batch mode)
    pub batch_size: usize,

    /// Enable CPU core affinity pinning (Linux only)
    pub enable_core_affinity: bool,

    /// Channel buffer size per partition
    pub partition_buffer_size: usize,

    /// Backpressure configuration
    pub backpressure_config: BackpressureConfig,
}

pub enum ProcessingMode {
    /// Process records individually (ultra-low-latency: p95 <1ms)
    Individual,

    /// Process records in batches (higher throughput)
    Batch { size: usize },
}

impl PartitionedJobCoordinator {
    pub async fn run_job(
        &self,
        reader: Box<dyn DataReader>,
        writer: Option<Box<dyn DataWriter>>,
        query: StreamingQuery,
    ) -> JobExecutionStats {
        let num_partitions = self.config.num_partitions
            .unwrap_or_else(|| num_cpus::get());

        // 1. Create partition managers
        let mut partition_senders = Vec::with_capacity(num_partitions);

        for partition_id in 0..num_partitions {
            let (tx, rx) = mpsc::unbounded_channel();
            let manager = PartitionStateManager::new(partition_id, rx);

            tokio::spawn(async move {
                manager.run().await;
            });

            partition_senders.push(tx);
        }

        // 2. Create hash router
        let router = HashRouter::new(num_partitions, query.clone());

        // 3. Create source pipeline with routing
        let (source_tx, mut source_rx) = mpsc::channel(1000);

        tokio::spawn(async move {
            let mut reader = reader;
            loop {
                match reader.read().await {
                    Ok(batch) if !batch.is_empty() => {
                        for record in batch {
                            if source_tx.send(record).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(_) => {
                        if !reader.has_more().await.unwrap_or(false) {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        // 4. Router task: distribute records to partitions
        let (output_tx, mut output_rx) = mpsc::channel(1000);
        let partition_senders_clone = partition_senders.clone();

        tokio::spawn(async move {
            while let Some(record) = source_rx.recv().await {
                let partition_id = router.route(&record);
                let tx = &partition_senders_clone[partition_id];

                let (response_tx, response_rx) = oneshot::channel();

                // Send to appropriate partition
                tx.send(PartitionMessage::ProcessRecord {
                    query_id: query.id.clone(),
                    record,
                    response: response_tx,
                }).ok();

                // Collect output (non-blocking)
                if let Ok(Some(output)) = response_rx.await {
                    output_tx.send(output).await.ok();
                }
            }
        });

        // 5. Sink pipeline (optional)
        if let Some(mut writer) = writer {
            tokio::spawn(async move {
                while let Some(record) = output_rx.recv().await {
                    writer.write(Arc::try_unwrap(record).unwrap_or_else(|arc| (*arc).clone())).await.ok();
                }
                writer.flush().await.ok();
            });
        }

        // 6. Wait for completion and collect stats
        // ... shutdown coordination ...

        JobExecutionStats::default()
    }
}
```

---

## Part 3: Performance Analysis

### Throughput Scaling Model

**Single Partition Baseline** (with Phase 4C optimizations):
- GROUP BY processing: 200K rec/sec
- Hash computation: ~50ns per record
- Message passing: ~500ns per record
- **Net throughput per partition**: ~200K rec/sec

**Multi-Partition Scaling**:

| Cores | Partitions | Theoretical Throughput | Actual Throughput (est.) | Efficiency |
|-------|------------|------------------------|--------------------------|------------|
| 1     | 1          | 200K rec/sec          | 200K rec/sec            | 100%       |
| 2     | 2          | 400K rec/sec          | 380K rec/sec            | 95%        |
| 4     | 4          | 800K rec/sec          | 750K rec/sec            | 94%        |
| 8     | 8          | 1.6M rec/sec          | 1.48M rec/sec           | 93%        |
| 16    | 16         | 3.2M rec/sec          | 2.88M rec/sec           | 90%        |
| 32    | 32         | 6.4M rec/sec          | 5.44M rec/sec           | 85%        |

**Efficiency Loss Factors**:
- Routing overhead: ~2-3%
- Channel contention: ~2-5% (increases with core count)
- Cache coherency: ~3-7% (increases with core count)
- Load imbalance: ~1-5% (depends on hash distribution)

**Why Linear Scaling Works**:
- Each partition operates independently with its own state
- No cross-partition coordination or locking
- Hash routing ensures consistent partition assignment
- CPU core pinning maximizes cache locality

---

### Latency Analysis (Ultra-Low-Latency Mode)

**Record Processing Pipeline**:

```
Source → Router → Partition Manager → Output
  ↓         ↓            ↓              ↓
100µs     50ns        1-2µs          100µs
```

**Total Latency Breakdown**:

| Stage | Time | Description |
|-------|------|-------------|
| Source read | 100µs | Kafka fetch / file read |
| Router hash | 50ns | GroupKey hash computation |
| Channel send | 500ns | mpsc channel message send |
| Partition wait | 1µs | Queue wait (if backpressure) |
| GROUP BY update | 2µs | FxHashMap entry + aggregation update |
| Window check | 100ns | Watermark comparison |
| Output channel | 500ns | Result channel send |
| Sink write | 100µs | Kafka produce / file write |
| **TOTAL** | **~205µs** | **p50 latency** |

**Latency Percentiles** (Individual mode):
- p50: ~200µs
- p95: ~500µs (with occasional backpressure)
- p99: ~1ms (under load)
- p99.9: ~5ms (GC pauses, OS scheduler)

**Improvement over V1**:
- V2 Partitioned: p95 = 500µs
- V1 Current: p95 = 50-100ms (lock contention)
- **100x latency improvement**

---

## Part 4: System Fields & Built-in Columns

### Integration with Hash Partitioning

**System Fields** (from src/velostream/sql/execution/types.rs):
1. `_TIMESTAMP`: Processing timestamp (i64)
2. `_OFFSET`: Kafka offset (i64)
3. `_PARTITION`: Kafka source partition (i32) - **NOT hash partition ID**
4. `_EVENT_TIME`: Event time for watermarks (i64)
5. `_WINDOW_START`: Window start time (i64)
6. `_WINDOW_END`: Window end time (i64)

**Important**: `_PARTITION` refers to **Kafka source partition**, not hash partition ID!

### System Fields in Partition Routing

```rust
impl HashRouter {
    pub fn route(&self, record: &StreamRecord) -> usize {
        let group_by_columns = self.query.get_group_by_columns();

        let mut hasher = FxHasher::default();
        for col in &group_by_columns {
            // Check if system field (UPPERCASE normalized)
            if system_columns::is_system_column_upper(col) {
                // Access from StreamRecord properties (not HashMap)
                match col.as_str() {
                    "_TIMESTAMP" => record.timestamp.hash(&mut hasher),
                    "_OFFSET" => record.offset.hash(&mut hasher),
                    "_PARTITION" => record.partition.hash(&mut hasher),
                    "_EVENT_TIME" => {
                        if let Some(et) = record.get_event_time() {
                            et.hash(&mut hasher);
                        }
                    }
                    _ => {}, // _WINDOW_START/_WINDOW_END set by engine
                }
            } else {
                // User field from HashMap
                if let Some(value) = record.fields.get(col) {
                    value.hash(&mut hasher);
                }
            }
        }

        (hasher.finish() % self.num_partitions as u64) as usize
    }
}
```

### Watermark Management Per Partition

Each partition maintains its own watermark for window emission:

```rust
impl PartitionStateManager {
    fn update_watermark(&mut self, record: &StreamRecord) {
        // Extract event time from system field
        if let Some(event_time) = record.get_event_time() {
            self.watermark_manager.update(event_time);
        }
    }

    fn check_window_emission(
        &mut self,
        state: &mut PartitionQueryState,
        record: &StreamRecord,
    ) -> Option<StreamRecord> {
        let current_watermark = self.watermark_manager.current_watermark();

        // Check if any windows are ready to emit
        for window_state in &mut state.window_states {
            if window_state.window_end <= current_watermark {
                // Emit window aggregates with system fields
                let mut output = self.build_window_output(window_state);

                // Set system window fields
                output.fields.insert(
                    "_WINDOW_START".to_string(),
                    FieldValue::Integer(window_state.window_start),
                );
                output.fields.insert(
                    "_WINDOW_END".to_string(),
                    FieldValue::Integer(window_state.window_end),
                );

                return Some(output);
            }
        }

        None
    }
}
```

---

## Part 5: Backpressure & Observability

### Per-Partition Backpressure Monitoring

Each partition tracks its own backpressure metrics:

```rust
pub struct PartitionMetrics {
    partition_id: usize,

    // Throughput metrics
    records_processed: AtomicU64,
    processing_time_total_us: AtomicU64,

    // Backpressure metrics (from FR-082 blueprint section 9.3)
    backpressure_events: AtomicU64,
    channel_utilization: AtomicF64,  // 0.0 - 1.0
    queue_depth: AtomicUsize,
    state_merge_wait_time_us: AtomicU64,

    // Latency tracking
    latency_p50_us: AtomicU64,
    latency_p95_us: AtomicU64,
    latency_p99_us: AtomicU64,
}

impl PartitionMetrics {
    pub fn channel_utilization(&self) -> f64 {
        self.queue_depth.load(Ordering::Relaxed) as f64 / 1000.0  // Assuming 1000 buffer size
    }

    pub fn backpressure_state(&self) -> BackpressureState {
        let utilization = self.channel_utilization();

        match utilization {
            u if u < 0.7 => BackpressureState::Healthy,
            u if u < 0.85 => BackpressureState::Warning { severity: u, partition: self.partition_id },
            u if u < 0.95 => BackpressureState::Critical { severity: u, partition: self.partition_id },
            _ => BackpressureState::Saturated { partition: self.partition_id },
        }
    }
}

pub enum BackpressureState {
    Healthy,
    Warning { severity: f64, partition: usize },
    Critical { severity: f64, partition: usize },
    Saturated { partition: usize },
}
```

### Automatic Backpressure Handling

```rust
impl PartitionedJobCoordinator {
    async fn monitor_backpressure(&self, partition_metrics: Vec<Arc<PartitionMetrics>>) {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Check each partition
            for (partition_id, metrics) in partition_metrics.iter().enumerate() {
                let state = metrics.backpressure_state();

                match state {
                    BackpressureState::Warning { severity, .. } => {
                        // Log warning
                        log::warn!(
                            "Partition {} experiencing backpressure ({}%)",
                            partition_id,
                            (severity * 100.0) as u32
                        );
                    }
                    BackpressureState::Critical { severity, .. } => {
                        // Throttle source for this partition
                        self.throttle_source_for_partition(partition_id, severity).await;
                    }
                    BackpressureState::Saturated { .. } => {
                        // Pause source completely for this partition
                        self.pause_source_for_partition(partition_id).await;
                    }
                    BackpressureState::Healthy => {
                        // Resume normal operation
                        self.resume_source_for_partition(partition_id).await;
                    }
                }
            }
        }
    }
}
```

### Prometheus Metrics Exposition

```rust
use prometheus::{Counter, Gauge, Histogram, Registry};

pub struct PartitionPrometheusExporter {
    // Per-partition throughput
    records_processed: Vec<Counter>,

    // Per-partition latency
    latency_histogram: Vec<Histogram>,

    // Per-partition backpressure
    channel_utilization: Vec<Gauge>,
    backpressure_events: Vec<Counter>,

    // Global metrics
    total_throughput: Counter,
    active_partitions: Gauge,
}

impl PartitionPrometheusExporter {
    pub fn export(&self, partition_id: usize, metrics: &PartitionMetrics) {
        // Update partition-specific metrics
        self.records_processed[partition_id]
            .inc_by(metrics.records_processed.load(Ordering::Relaxed));

        self.channel_utilization[partition_id]
            .set(metrics.channel_utilization());

        self.backpressure_events[partition_id]
            .inc_by(metrics.backpressure_events.load(Ordering::Relaxed));

        // Update latency histogram
        self.latency_histogram[partition_id]
            .observe(metrics.latency_p95_us.load(Ordering::Relaxed) as f64 / 1000.0);
    }
}
```

**Grafana Dashboard Query Examples**:

```promql
# Throughput per partition
sum(rate(velostream_partition_records_processed[1m])) by (partition_id)

# Total system throughput
sum(rate(velostream_partition_records_processed[1m]))

# Backpressure detection (critical if >0.85)
velostream_partition_channel_utilization > 0.85

# Partition latency p95
histogram_quantile(0.95, velostream_partition_latency_ms)

# Identify hot partitions (load imbalance)
topk(3, rate(velostream_partition_records_processed[5m]))
```

---

## Part 6: Implementation Roadmap

### Phase 0: SQL Engine Optimization (Prerequisite - 2 weeks)

**Week 1: Phase 4B - Hash Table Optimization**
- ✅ Implement GroupKey with pre-computed hash
- ✅ Replace HashMap with FxHashMap
- ✅ Target: 3.58K → 15-20K rec/sec
- ✅ Test: `cargo test profile_pure_group_by_complex -- --nocapture`

**Week 2: Phase 4C - Arc-based State Sharing**
- ✅ Wrap group states in Arc<FxHashMap>
- ✅ Implement copy-on-write merge pattern
- ✅ Add string interning and key caching
- ✅ Target: 15-20K → 200K rec/sec
- ✅ Test: `cargo test comprehensive_sql_benchmarks -- --nocapture`

**Success Criteria**: Pure GROUP BY achieves 200K rec/sec baseline

---

### Phase 1: Hash Routing Foundation (Week 3)

**Week 3: Hash Router + Partition Manager**
- 🔧 Implement `HashRouter` with GROUP BY key extraction
- 🔧 Implement `PartitionStateManager` with lock-free state
- 🔧 Add CPU core affinity (Linux only)
- 🔧 Create `PartitionMessage` enum
- ✅ Test: `tests/unit/server/v2/hash_router_test.rs`
- ✅ Test: `tests/unit/server/v2/partition_state_manager_test.rs`

**Success Criteria**:
- Hash routing: <100ns per record
- Single partition: 200K rec/sec (matches Phase 4C baseline)
- 4 partitions: 750K+ rec/sec (94% efficiency)

---

### Phase 2: Partitioned Coordinator (Week 4)

**Week 4: Job Coordinator + Multi-Partition Orchestration**
- 🔧 Implement `PartitionedJobCoordinator`
- 🔧 Create router task with record distribution
- 🔧 Implement output merger (if ordering required)
- 🔧 Add partition initialization and shutdown
- ✅ Test: `tests/integration/server/v2/partitioned_job_test.rs`

**Success Criteria**:
- 8 cores: 1.48M rec/sec (93% efficiency)
- Clean shutdown (all partitions drain)
- Correct GROUP BY aggregation results

---

### Phase 3: Backpressure & Observability (Week 5)

**Week 5: Per-Partition Backpressure + Prometheus Metrics**
- 🔧 Implement `PartitionMetrics` with atomic counters
- 🔧 Add backpressure detection per partition
- 🔧 Implement automatic throttling
- 🔧 Create `PartitionPrometheusExporter`
- 🔧 Add Grafana dashboard JSON
- ✅ Test: `tests/unit/server/v2/partition_backpressure_test.rs`

**Success Criteria**:
- Backpressure detection: <1ms lag
- Prometheus scrape: <5ms per scrape
- Grafana dashboards show per-partition metrics

---

### Phase 4: System Fields & Watermarks (Week 6)

**Week 6: System Fields Integration + Watermark Management**
- 🔧 Update `HashRouter` to handle system fields in GROUP BY
- 🔧 Implement per-partition watermark managers
- 🔧 Add `_WINDOW_START` / `_WINDOW_END` field injection
- 🔧 Optimize system column access (UPPERCASE normalization)
- ✅ Test: `tests/unit/server/v2/system_fields_test.rs`

**Success Criteria**:
- System field routing: same hash as user fields
- Watermark emission: correct window boundaries
- No performance regression (<5% overhead)

---

### Phase 5: Advanced Features (Weeks 7-8)

**Week 7: ROWS WINDOW + Late Record Handling**
- 🔧 Implement per-partition ROWS WINDOW buffers
- 🔧 Add late record detection and strategy
- 🔧 Implement EMIT CHANGES mode
- ✅ Test: `tests/unit/server/v2/rows_window_test.rs`

**Week 8: State TTL + Recovery**
- 🔧 Add per-partition state TTL cleanup
- 🔧 Implement state snapshot/restore
- 🔧 Add partition rebalancing (for dynamic scaling)
- ✅ Test: `tests/unit/server/v2/state_recovery_test.rs`

---

## Part 7: Performance Targets

### Single-Source Performance (After Phase 4C)

| Scenario | Phase 4C Baseline | V2 (8 Partitions) | Improvement |
|----------|-------------------|-------------------|-------------|
| **Pure GROUP BY (simple)** | 200K rec/sec | 1.5M rec/sec | 7.5x |
| **Pure GROUP BY (complex)** | 200K rec/sec | 1.5M rec/sec | 7.5x |
| **TUMBLING + GROUP BY** | 200K rec/sec | 1.5M rec/sec | 7.5x |
| **ROWS WINDOW** | TBD | TBD × 7.5 | 7.5x |

### Multi-Source Performance (Horizontal Scaling)

**Scenario**: 20 Kafka sources, 8 cores per machine, 10 machines

| Stage | Throughput | Cumulative |
|-------|-----------|------------|
| **Single partition per source** | 200K rec/sec | 200K |
| **8 partitions (single machine)** | 1.5M rec/sec | 1.5M |
| **20 sources (parallel ingest)** | 30M rec/sec | 30M |
| **10 machines (horizontal scale)** | 300M rec/sec | 300M |

**Improvement over V1**:

| Metric | V1 Current | V2 Partitioned (8 cores) | Improvement |
|--------|-----------|--------------------------|-------------|
| **Single Core** | 23K | 200K | 8.7x |
| **8 Cores** | 23K (no scaling) | 1.5M | 65x |
| **20 Sources × 8 Cores** | 23K | 30M | 1,300x |
| **10 Machines** | 23K | 300M | 13,000x |

---

## Part 8: Architecture Benefits

### Key Advantages of Hash-Partitioned Design

**1. Linear Scalability**
- Each additional core adds ~200K rec/sec throughput
- No coordination overhead between partitions
- Scales horizontally across machines

**2. Ultra-Low Latency**
- No queueing delays (no serialization point)
- CPU core pinning for cache locality
- p95 latency <1ms

**3. Fine-Grained Backpressure**
- Per-partition monitoring and throttling
- Individual partitions can pause without affecting others
- Better resource utilization

**4. Fault Isolation**
- Partition failures don't cascade
- Independent recovery per partition
- Easier debugging and monitoring

**5. Natural Fit for GROUP BY**
- GROUP BY queries have built-in partitioning
- Each group key maps to one partition
- Zero cross-partition communication needed

---

## Part 9: Migration Path from V1

### V1 → V2 Migration Strategy

**Option 1: Drop-in Replacement (Recommended)**

```rust
// V1 Code (current):
let processor = SimpleJobProcessor::new(config);
processor.process_job(reader, writer, engine, query, shutdown_rx).await?;

// V2 Code (partitioned):
let coordinator = PartitionedJobCoordinator::new(config);
coordinator.run_job(reader, writer, query).await?;
```

**Option 2: Feature Flag (Gradual Rollout)**

```rust
let processor: Box<dyn JobProcessor> = if config.use_v2_partitioned {
    Box::new(PartitionedJobCoordinator::new(config))
} else {
    Box::new(SimpleJobProcessor::new(config))
};

processor.process_job(...).await?;
```

**Option 3: Parallel Deployment (A/B Testing)**

```yaml
# Kafka consumer group "velostream-v1"
kafka:
  consumer_group: velostream-v1
  processor: SimpleJobProcessor

# Kafka consumer group "velostream-v2"
kafka:
  consumer_group: velostream-v2
  processor: PartitionedJobCoordinator
  num_partitions: 8
```

---

## Part 10: Query Partitionability Classification

### Understanding Query Types

Not all SQL queries benefit equally from hash partitioning. Based on comprehensive analysis of 18 SQL files in the codebase, we've identified **5 distinct query patterns** with different partitionability characteristics.

**For complete analysis**, see: `FR-082-QUERY-PARTITIONABILITY-ANALYSIS.md`

### Partitionability Summary

| Query Type | Count (18 files) | Partitionable? | Strategy |
|------------|-----------------|---------------|----------|
| **GROUP BY (Simple)** | 8 (44%) | ✅ Yes | Hash by GROUP BY columns |
| **GROUP BY + Windows** | 5 (28%) | ✅ Yes | Hash by GROUP BY columns |
| **Pure SELECT** | 3 (17%) | Trivial | Round-robin or single partition |
| **Global Aggregations** | 2 (11%) | ⚠️ Two-phase | Local aggregate → merge |
| **Complex JOINs** | Mixed | Depends | Co-partitioned / Broadcast / Repartition |

**Key Finding**: **72% of queries** (13/18 files) are **fully partitionable** with linear scaling across N cores.

### Decision Tree for Partition Routing

```
Query Analysis
    │
    ├─ Has GROUP BY?
    │  │
    │  ├─ YES → FULLY PARTITIONABLE ✅
    │  │        Strategy: Hash by GROUP BY columns
    │  │        Expected: Linear scaling (N cores = N × 200K rec/sec)
    │  │        Examples:
    │  │          - GROUP BY customer_id → hash(customer_id) % N
    │  │          - GROUP BY device_id, sensor_type → hash(device_id, sensor_type) % N
    │  │          - GROUP BY hashtag + SESSION window → hash(hashtag) % N
    │  │
    │  └─ NO → Check aggregations
    │         │
    │         ├─ Has aggregations (COUNT, SUM, AVG)?
    │         │  │
    │         │  ├─ YES → TWO-PHASE AGGREGATION ⚠️
    │         │  │        Strategy: Local per-partition → merge stage
    │         │  │        Expected: Input scales linearly, output single row
    │         │  │        Example: SELECT COUNT(*), AVG(price) FROM trades
    │         │  │
    │         │  └─ NO → Pure SELECT/projection
    │         │           Strategy: Round-robin or single partition
    │         │           Expected: Very high (400K+ rec/sec)
    │         │           Example: SELECT * FROM stream WHERE value > 100
    │         │
    │         └─ Has JOIN?
    │            │
    │            ├─ Same join key as partition key? → Co-partitioned JOIN ✅
    │            ├─ Small lookup table? → Broadcast JOIN ✅
    │            └─ Different keys? → Repartition JOIN ⚠️
```

### Query Analyzer Enhancement

Add partitionability detection to query analyzer:

```rust
pub enum PartitionStrategy {
    /// Fully partitionable by GROUP BY columns
    FullyPartitionable {
        partition_keys: Vec<String>,
        expected_throughput: u64,  // Per partition
    },

    /// Requires two-phase aggregation (local → merge)
    TwoPhaseAggregation {
        local_agg: Vec<AggregateFunction>,
        merge_strategy: MergeStrategy,
    },

    /// Simple passthrough (no state)
    Passthrough {
        routing: RoutingStrategy,  // RoundRobin or SinglePartition
    },

    /// Broadcast small lookup table to all partitions
    BroadcastJoin {
        replicated_table: String,
        join_key: Vec<String>,
    },

    /// Requires repartitioning one stream
    RepartitionJoin {
        from_key: Vec<String>,
        to_key: Vec<String>,
    },
}

impl QueryAnalyzer {
    pub fn determine_partition_strategy(&self, query: &StreamingQuery) -> PartitionStrategy {
        if let Some(group_by) = &query.group_by {
            // Category: GROUP BY (with or without windows)
            PartitionStrategy::FullyPartitionable {
                partition_keys: group_by.columns.clone(),
                expected_throughput: 200_000,  // After Phase 4C
            }
        } else if query.has_aggregations() {
            // Category: Global aggregations (no GROUP BY)
            PartitionStrategy::TwoPhaseAggregation {
                local_agg: query.extract_aggregations(),
                merge_strategy: MergeStrategy::Sum,  // or Min/Max/Avg
            }
        } else {
            // Category: Pure SELECT
            PartitionStrategy::Passthrough {
                routing: RoutingStrategy::RoundRobin,
            }
        }
    }
}
```

### Routing Implementation

```rust
impl PartitionedJobCoordinator {
    async fn route_record(
        &self,
        record: StreamRecord,
        strategy: &PartitionStrategy,
    ) -> usize {
        match strategy {
            PartitionStrategy::FullyPartitionable { partition_keys, .. } => {
                // Extract GROUP BY key from record
                let group_key = GroupKey::from_record(&record, partition_keys);
                group_key.hash as usize % self.num_partitions
            }

            PartitionStrategy::TwoPhaseAggregation { .. } => {
                // Round-robin distribution (each partition computes partial aggregate)
                self.round_robin_counter.fetch_add(1, Ordering::Relaxed)
                    % self.num_partitions
            }

            PartitionStrategy::Passthrough { routing } => {
                match routing {
                    RoutingStrategy::RoundRobin => {
                        self.round_robin_counter.fetch_add(1, Ordering::Relaxed)
                            % self.num_partitions
                    }
                    RoutingStrategy::SinglePartition => 0,
                }
            }

            PartitionStrategy::BroadcastJoin { join_key, .. } => {
                // Main stream: hash by join key
                let key = record.extract_join_key(join_key);
                hash(key) as usize % self.num_partitions
            }

            PartitionStrategy::RepartitionJoin { .. } => {
                // Multi-stage pipeline required
                unimplemented!("Repartition JOIN requires multi-pass execution")
            }
        }
    }
}
```

### Performance Expectations by Category

| Category | Partitionable? | Expected Throughput (8 cores) | Scaling Efficiency |
|----------|---------------|-------------------------------|-------------------|
| **GROUP BY (Simple)** | ✅ Yes | 1.5M rec/sec | 93% |
| **GROUP BY + Windows** | ✅ Yes | 1.0M rec/sec | 85-90% |
| **Pure SELECT** | Trivial | 400K+ rec/sec | 100% (stateless) |
| **Global Aggregations** | ⚠️ Two-phase | 1.5M rec/sec input | 93% (phase 1) |
| **Co-partitioned JOIN** | ✅ Yes | 1.2M rec/sec | 85% |
| **Broadcast JOIN** | ✅ Yes | 1.5M rec/sec | 93% |

---

## Part 11: Two-Phase Aggregation Strategy

### Problem: Global Aggregations Without GROUP BY

Some queries need to aggregate over the **entire dataset** without GROUP BY:

```sql
-- Example: Global trade statistics
SELECT
    COUNT(*) as total_trades,
    SUM(volume) as total_volume,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM trades;
```

**Challenge**: No GROUP BY columns to hash by → cannot use hash-partitioned routing directly.

**Solution**: **Two-Phase Aggregation**
1. **Phase 1 (Local)**: Each partition computes partial aggregates independently
2. **Phase 2 (Merge)**: Single merge stage combines partial results into final output

### Two-Phase Execution Model

```
Phase 1: Parallel Local Aggregation
─────────────────────────────────────

Source Stream (1M records)
        │
        ├─ Round-robin distribution
        │
    ┌───┼───┬───────┬───────┐
    │   │   │       │       │
    ▼   ▼   ▼       ▼       ▼
Partition 0  Partition 1  ... Partition 7
[125K rec]   [125K rec]       [125K rec]
    │            │                │
    ▼            ▼                ▼
Partial:     Partial:         Partial:
  count: 125K  count: 125K      count: 125K
  sum: 12.5M   sum: 13.0M       sum: 14.2M
  min: 45.32   min: 42.18       min: 48.91
  max: 189.75  max: 195.23      max: 182.44


Phase 2: Merge Stage
────────────────────

All partial aggregates → Merge Coordinator
    │
    ▼
Final Aggregate:
  count: 1M (sum of counts)
  total: 100M (sum of sums)
  avg: 100.0 (total / count)
  min: 42.18 (min of mins)
  max: 195.23 (max of maxs)
    │
    ▼
Output: Single row
```

### Partial Aggregate Data Structure

```rust
/// Partial aggregate computed by each partition
#[derive(Debug, Clone)]
pub struct PartialAggregate {
    // Count-based aggregations
    pub count: u64,                    // COUNT(*)
    pub non_null_counts: HashMap<String, u64>,  // COUNT(column)

    // Sum-based aggregations (for AVG, SUM)
    pub sums: HashMap<String, f64>,    // SUM(column)

    // Min/Max aggregations
    pub mins: HashMap<String, FieldValue>,  // MIN(column)
    pub maxs: HashMap<String, FieldValue>,  // MAX(column)

    // Advanced aggregations (if needed)
    pub sum_of_squares: HashMap<String, f64>,  // For STDDEV calculation
}

impl PartialAggregate {
    /// Merge two partial aggregates
    pub fn merge(&mut self, other: &PartialAggregate) {
        // Merge counts
        self.count += other.count;

        // Merge non-null counts
        for (col, count) in &other.non_null_counts {
            *self.non_null_counts.entry(col.clone()).or_insert(0) += count;
        }

        // Merge sums
        for (col, sum) in &other.sums {
            *self.sums.entry(col.clone()).or_insert(0.0) += sum;
        }

        // Merge mins
        for (col, min_val) in &other.mins {
            self.mins.entry(col.clone())
                .and_modify(|v| {
                    if other_min < *v {
                        *v = min_val.clone();
                    }
                })
                .or_insert(min_val.clone());
        }

        // Merge maxs (similar to mins)
        for (col, max_val) in &other.maxs {
            self.maxs.entry(col.clone())
                .and_modify(|v| {
                    if other_max > *v {
                        *v = max_val.clone();
                    }
                })
                .or_insert(max_val.clone());
        }
    }

    /// Convert partial aggregate to final aggregate
    pub fn finalize(&self) -> FinalAggregate {
        let mut final_agg = FinalAggregate::new();

        // COUNT(*) → direct copy
        final_agg.insert("count", FieldValue::Integer(self.count as i64));

        // SUM(column) → direct copy
        for (col, sum) in &self.sums {
            final_agg.insert(&format!("sum_{}", col), FieldValue::Float(*sum));
        }

        // AVG(column) → sum / count
        for (col, sum) in &self.sums {
            if let Some(count) = self.non_null_counts.get(col) {
                let avg = sum / (*count as f64);
                final_agg.insert(&format!("avg_{}", col), FieldValue::Float(avg));
            }
        }

        // MIN/MAX → direct copy
        for (col, min_val) in &self.mins {
            final_agg.insert(&format!("min_{}", col), min_val.clone());
        }
        for (col, max_val) in &self.maxs {
            final_agg.insert(&format!("max_{}", col), max_val.clone());
        }

        final_agg
    }
}
```

### Merge Stage Coordinator

```rust
/// Coordinates merging of partial aggregates from all partitions
pub struct MergeStageCoordinator {
    num_partitions: usize,
    partial_results: Vec<Option<PartialAggregate>>,
    output_sender: mpsc::UnboundedSender<StreamRecord>,
}

impl MergeStageCoordinator {
    pub async fn collect_partial_result(
        &mut self,
        partition_id: usize,
        partial: PartialAggregate,
    ) {
        self.partial_results[partition_id] = Some(partial);

        // Check if all partitions have reported
        if self.all_partitions_ready() {
            let final_result = self.merge_all();
            self.emit_final_result(final_result).await;
            self.reset();  // Prepare for next window (if windowed)
        }
    }

    fn all_partitions_ready(&self) -> bool {
        self.partial_results.iter().all(|p| p.is_some())
    }

    fn merge_all(&self) -> PartialAggregate {
        let mut merged = PartialAggregate::default();

        for partial in self.partial_results.iter().filter_map(|p| p.as_ref()) {
            merged.merge(partial);
        }

        merged
    }

    async fn emit_final_result(&self, partial: PartialAggregate) {
        let final_agg = partial.finalize();
        let record = StreamRecord::from_aggregate(final_agg);
        self.output_sender.send(record).await.ok();
    }

    fn reset(&mut self) {
        self.partial_results.clear();
        self.partial_results.resize(self.num_partitions, None);
    }
}
```

### Integration with Partition State Manager

Each partition computes local aggregates:

```rust
impl PartitionStateManager {
    async fn process_record_global_agg(
        &mut self,
        record: StreamRecord,
        query: &StreamingQuery,
    ) {
        // Update local partial aggregate
        self.partial_agg.update(&record, &query.aggregations);

        // For windowed global aggregations, emit when window closes
        if let Some(window) = &query.window {
            if self.watermark_manager.should_emit_window(window) {
                // Send partial aggregate to merge coordinator
                self.merge_tx.send(MergeMessage::PartialResult {
                    partition_id: self.partition_id,
                    partial: self.partial_agg.clone(),
                }).await.ok();

                // Reset for next window
                self.partial_agg.reset();
            }
        }
    }
}
```

### Example: Global AVG with Two-Phase

```sql
SELECT AVG(price) as avg_price FROM trades;
```

**Phase 1 (Per-Partition)**:
```
Partition 0: count=125K, sum=12,500,000
Partition 1: count=130K, sum=13,000,000
Partition 2: count=122K, sum=12,200,000
...
Partition 7: count=128K, sum=12,800,000
```

**Phase 2 (Merge)**:
```rust
// Merge counts: 125K + 130K + 122K + ... = 1,000,000
// Merge sums: 12.5M + 13M + 12.2M + ... = 100,000,000
// Compute AVG: 100,000,000 / 1,000,000 = 100.0
```

**Output**: Single row with `avg_price = 100.0`

### Performance Characteristics

**Phase 1 (Local Aggregation)**:
- **Throughput**: Scales linearly (N partitions = N × 200K rec/sec)
- **Latency**: Same as regular processing (<1ms per record)
- **Memory**: O(1) per partition (single accumulator, no hash table)

**Phase 2 (Merge)**:
- **Throughput**: Trivial (merging 8-32 partial results)
- **Latency**: <1ms for merge operation
- **Memory**: O(num_partitions × aggregate_size) = ~1KB total

**Total Expected Throughput**: 1.5M rec/sec input on 8 cores (same as partitioned GROUP BY)

**Output Rate**: Single row per window (or continuous for non-windowed)

### When to Use Two-Phase Aggregation

**Use when**:
- ✅ Query has aggregations (COUNT, SUM, AVG, MIN, MAX) but **no GROUP BY**
- ✅ Final output is single row (global aggregate)
- ✅ Input stream has high volume (>100K rec/sec)

**Examples**:
```sql
-- Global statistics
SELECT COUNT(*), AVG(price), MIN(price), MAX(price) FROM trades;

-- Global with HAVING (filters on aggregates)
SELECT SUM(amount) as total FROM transactions
HAVING SUM(amount) > 1000000;

-- Nested global aggregations in subqueries
SELECT customer_id, total_spent
FROM customer_totals
WHERE total_spent > (SELECT AVG(total_spent) FROM customer_totals);
```

---

## Part 12: Replicated State Management (Broadcast JOIN)

### Problem: Lookup Tables Needed by All Partitions

Some queries require **reference data** that must be available to **all partitions**:

```sql
-- Example: Enrich sensor readings with device metadata
SELECT
    r.device_id,
    r.sensor_type,
    r.value,
    d.device_name,      -- From lookup table
    d.location,         -- From lookup table
    d.manufacturer      -- From lookup table
FROM sensor_readings r
LEFT JOIN device_metadata d ON r.device_id = d.device_id;
```

**Challenge**: `device_metadata` is a **small, slowly-changing lookup table** - partitioning it would require coordination.

**Solution**: **Replicated State** - Broadcast the entire lookup table to all partitions using `Arc<HashMap>`.

### Replicated State Architecture

```
                  Lookup Table Update
                  (device_metadata)
                          │
                          ▼
              ┌─────────────────────────┐
              │  Replicated State Store │
              │   Arc<HashMap<K, V>>    │
              └─────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
    Partition 0     Partition 1     Partition N
    [Full copy]     [Full copy]     [Full copy]
          │               │               │
          └───────────────┼───────────────┘
                          │
                    Join Locally
            (no cross-partition lookups)
```

**Key Property**: All partitions share the **same Arc pointer** → zero-copy replication.

### Implementation

#### 1. Replicated State Container

```rust
/// Lookup table replicated across all partitions
#[derive(Clone)]
pub struct ReplicatedTable {
    /// Table name
    name: String,

    /// Data: Arc-wrapped HashMap for cheap cloning
    data: Arc<HashMap<GroupKey, StreamRecord>>,

    /// Last update timestamp (for staleness tracking)
    last_update: Instant,

    /// Version number (for consistency checks)
    version: u64,
}

impl ReplicatedTable {
    pub fn new(name: String, data: HashMap<GroupKey, StreamRecord>) -> Self {
        ReplicatedTable {
            name,
            data: Arc::new(data),
            last_update: Instant::now(),
            version: 1,
        }
    }

    /// Update the table (atomic Arc swap)
    pub fn update(&mut self, new_data: HashMap<GroupKey, StreamRecord>) {
        self.data = Arc::new(new_data);
        self.last_update = Instant::now();
        self.version += 1;
    }

    /// Lookup by key (O(1) hash table lookup)
    pub fn get(&self, key: &GroupKey) -> Option<&StreamRecord> {
        self.data.get(key)
    }

    /// Memory size estimate
    pub fn memory_bytes(&self) -> usize {
        self.data.len() * std::mem::size_of::<(GroupKey, StreamRecord)>()
    }
}
```

#### 2. Partition State Manager with Replicated Tables

```rust
pub struct PartitionStateManager {
    partition_id: usize,

    // Per-partition state (hash-partitioned, mutable)
    query_states: HashMap<String, QueryState>,

    // Replicated tables (shared across all partitions, immutable per version)
    replicated_tables: HashMap<String, ReplicatedTable>,
}

impl PartitionStateManager {
    /// Process record with broadcast JOIN
    async fn process_broadcast_join(
        &mut self,
        record: StreamRecord,
        join: &JoinClause,
    ) -> Result<StreamRecord> {
        // Get replicated table
        let lookup_table = self.replicated_tables.get(&join.right_table)
            .ok_or_else(|| anyhow!("Replicated table '{}' not found", join.right_table))?;

        // Extract join key from record
        let join_key = GroupKey::from_record(&record, &join.join_columns);

        // Perform local lookup (O(1) hash table access)
        if let Some(lookup_record) = lookup_table.get(&join_key) {
            // Merge fields from both records
            let joined = record.merge_with(lookup_record);
            Ok(joined)
        } else {
            // LEFT JOIN: Keep record even if no match
            Ok(record)
        }
    }

    /// Update replicated table (called when lookup table changes)
    pub fn update_replicated_table(&mut self, table: ReplicatedTable) {
        info!("Partition {}: Updating replicated table '{}' (version {}, size: {} records, {} KB)",
              self.partition_id, table.name, table.version,
              table.data.len(), table.memory_bytes() / 1024);

        self.replicated_tables.insert(table.name.clone(), table);
    }
}
```

#### 3. Coordinator: Broadcast Updates to All Partitions

```rust
impl PartitionedJobCoordinator {
    /// Update replicated table and broadcast to all partitions
    pub async fn update_replicated_table(
        &self,
        table_name: String,
        data: HashMap<GroupKey, StreamRecord>,
    ) -> Result<()> {
        let table = ReplicatedTable::new(table_name.clone(), data);

        info!("Broadcasting replicated table '{}' to {} partitions (version {}, {} records)",
              table_name, self.num_partitions, table.version, table.data.len());

        // Send to ALL partitions (cheap Arc clone)
        let mut broadcast_tasks = Vec::new();
        for partition_sender in &self.partition_senders {
            let table_clone = table.clone();  // ← Cheap Arc clone!
            let sender = partition_sender.clone();

            broadcast_tasks.push(tokio::spawn(async move {
                sender.send(PartitionMessage::UpdateReplicatedTable(table_clone))
                    .await
            }));
        }

        // Wait for all partitions to acknowledge
        for task in broadcast_tasks {
            task.await??;
        }

        info!("Replicated table '{}' broadcast complete", table_name);
        Ok(())
    }
}
```

### Memory Cost Analysis

**Formula**: `Memory = N_partitions × Table_size`

**Example**: 8 partitions, 10K device records, 1KB per record
- **Total Memory**: 8 × 10K × 1KB = **80MB** (manageable)

**When to Use**:
- ✅ Lookup table is **small** (<10MB per partition)
- ✅ Lookup table is **slowly changing** (updates infrequent)
- ✅ High join throughput needed (local lookups, no RPC)

**When NOT to Use**:
- ❌ Lookup table is **large** (>100MB) → Use external key-value store (Redis, RocksDB)
- ❌ Lookup table changes **frequently** (>1 update/sec) → Use external cache with TTL
- ❌ Lookup table is **unbounded** (continuous stream) → Use co-partitioned JOIN instead

### Alternative: External Lookup (for Large Tables)

If lookup table is too large to replicate:

```rust
impl PartitionStateManager {
    async fn process_external_lookup_join(
        &mut self,
        record: StreamRecord,
        join: &JoinClause,
    ) -> Result<StreamRecord> {
        // Extract join key
        let join_key = GroupKey::from_record(&record, &join.join_columns);

        // Query external key-value store (Redis, RocksDB, etc.)
        let lookup_result = self.external_kv_client
            .get(&join.right_table, &join_key)
            .await?;

        if let Some(lookup_record) = lookup_result {
            Ok(record.merge_with(&lookup_record))
        } else {
            Ok(record)  // LEFT JOIN
        }
    }
}
```

**Trade-off**:
- **Memory**: O(1) per partition (no replication)
- **Latency**: +1-5ms per lookup (network RPC)
- **Throughput**: Limited by RPC rate (10K-100K lookups/sec)

### Performance Comparison: Replicated vs External

| Aspect | Replicated (Arc<HashMap>) | External (Redis/RocksDB) |
|--------|---------------------------|--------------------------|
| **Memory** | O(N_partitions × table_size) | O(table_size) (centralized) |
| **Latency** | <1µs (local hash lookup) | 1-5ms (network RPC) |
| **Throughput** | 1.5M rec/sec (local) | 10K-100K rec/sec (RPC limited) |
| **Consistency** | Eventual (broadcast delay) | Strong (centralized) |
| **Use Case** | Small, stable lookup tables | Large, dynamic lookup tables |

---

## Part 13: Future State Considerations - Join Congestion and Mitigation

### Overview

While the hash-partitioned architecture provides excellent performance for pure GROUP BY aggregations, **stream-stream joins introduce potential congestion risks** when partition keys are skewed or temporal hotspots occur.

This section documents architectural limitations, congestion scenarios, and mitigation strategies for future phases.

---

### Congestion Scenario 1: Skewed Join Keys

**Problem**: Co-partitioned stream-stream joins where a small number of keys dominate traffic

```sql
SELECT o.order_id, c.customer_name, o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY o.customer_id
```

**Example: Enterprise Customer Skew**
```
Real-world distribution:
  - customer_id=1001 (Amazon corporate): 1M orders/day (80% of traffic)
  - customer_id=1002-9999: 250K orders/day (20% of traffic)

Hash-partitioned routing (N=8 partitions):
  Partition 3 (gets customer_id=1001): 800K rec/sec ⚠️ OVERLOADED
  Other 7 partitions:                   35K rec/sec each ✅ IDLE
```

**Impact**:
- Single partition becomes bottleneck
- Overall throughput limited to slowest partition
- 7/8 cores underutilized (87.5% waste)
- Backpressure propagates to upstream sources

**Current Status**: ⚠️ **NOT ADDRESSED IN PHASES 1-2**

---

### Congestion Scenario 2: Temporal Hotspots

**Problem**: Burst traffic affecting all partitions simultaneously

```sql
-- Trading volume spike on market open (9:30 AM)
SELECT symbol, COUNT(*), AVG(price)
FROM trades
WHERE event_time BETWEEN '09:30:00' AND '09:30:01'
GROUP BY symbol
WINDOW TUMBLING (event_time, INTERVAL '1' SECOND)
```

**Example: Market Open Spike**
```
Normal trading hours:
  - 200K trades/sec → 25K rec/sec per partition (8 cores) ✅

Market open (9:30-9:31 AM):
  - 2M trades/sec → 250K rec/sec per partition ⚠️ 10x OVERLOAD

Result:
  - All 8 partitions simultaneously overloaded
  - Backpressure triggers across entire system
  - Messages queue upstream (Kafka consumer lag increases)
  - Recovery takes 5-10 minutes after spike ends
```

**Impact**:
- System-wide congestion (not isolated to single partition)
- Increased latency during burst periods
- Potential message loss if buffers overflow

**Current Status**: ⚠️ **PARTIALLY ADDRESSED IN PHASE 3** (backpressure detection only)

---

### Congestion Scenario 3: Repartition Join Overhead

**Problem**: Multi-stage joins requiring shuffle between stages

```sql
-- Product co-purchase recommendations
SELECT o1.customer_id, COUNT(DISTINCT o2.customer_id) as co_buyers
FROM orders o1
JOIN orders o2 ON o1.product_id = o2.product_id  -- Different partition key!
WHERE o1.customer_id != o2.customer_id
GROUP BY o1.customer_id
```

**Multi-Stage Execution Required**:
```
Stage 1: Initial partitioning by customer_id
  → Process orders, but cannot perform join (partitioned by wrong key)

Stage 2: Repartition by product_id (SHUFFLE REQUIRED)
  → All partitions send data to router
  → Router redistributes by Hash(product_id) % N
  → Network overhead: 1.5M rec/sec × record_size (e.g., 200 bytes = 300 MB/sec)

Stage 3: Perform join locally per partition
  → Join orders by product_id
  → Buffer both sides of join (memory intensive)

Stage 4: Repartition by customer_id for GROUP BY (SECOND SHUFFLE)
  → Another network overhead cycle
  → Aggregate co-buyer counts per customer
```

**Impact**:
- **Throughput**: 500-800K rec/sec (50-60% degradation from 1.5M baseline)
- **Network**: 600 MB/sec shuffle traffic (2 stages × 300 MB/sec)
- **Memory**: 2x state size (buffer both join sides)
- **Latency**: +50-100ms per shuffle stage

**Current Status**: ❌ **NOT IMPLEMENTED** (deferred post-Phase 5)

---

### Mitigation Strategy 1: Salted Key Distribution (Future Enhancement)

**Goal**: Split hot keys across multiple partitions to balance load

```rust
// Detect hot key and split across sub-partitions
pub struct SaltedPartitionStrategy {
    hot_keys: HashSet<GroupKey>,        // Keys exceeding threshold
    salt_factor: usize,                  // Sub-partition count (e.g., 4)
}

impl SaltedPartitionStrategy {
    pub fn route(&self, record: &StreamRecord, base_partitions: usize) -> usize {
        let key = record.extract_group_key();

        if self.hot_keys.contains(&key) {
            // Split hot key across salt_factor sub-partitions
            let salt = record.get_random_salt(self.salt_factor);
            let virtual_key = format!("{}:{}", key, salt);
            hash(virtual_key) % (base_partitions * self.salt_factor)
        } else {
            // Normal hashing for non-hot keys
            hash(key) % base_partitions
        }
    }
}
```

**Example: Amazon Corporate Account Split**
```
Before salting (customer_id=1001 → Partition 3):
  Partition 3: 800K rec/sec ⚠️ OVERLOADED

After salting (customer_id=1001 → Partitions 3, 11, 19, 27):
  Partition 3:  200K rec/sec ✅
  Partition 11: 200K rec/sec ✅
  Partition 19: 200K rec/sec ✅
  Partition 27: 200K rec/sec ✅
```

**Challenges**:
- **Aggregation Complexity**: Final aggregation requires merge across salted partitions
- **Hot Key Detection**: Need runtime profiling to identify skewed keys (99th percentile threshold)
- **Dynamic Adjustment**: Salting must adapt to changing traffic patterns

**Phasing**: Post-Phase 5 enhancement (requires query planner changes)

---

### Mitigation Strategy 2: Adaptive Backpressure (Phase 3)

**Goal**: Detect partition overload and throttle upstream sources

```rust
pub struct PartitionMetrics {
    records_per_second: AtomicUsize,
    queue_depth: AtomicUsize,
    processing_latency_p99: AtomicU64,
}

impl PartitionStateManager {
    fn check_backpressure(&self) -> BackpressureSignal {
        let metrics = &self.metrics;

        // Detect overload conditions
        let overloaded =
            metrics.queue_depth.load(Ordering::Relaxed) > 10_000 ||
            metrics.processing_latency_p99.load(Ordering::Relaxed) > 100; // ms

        if overloaded {
            BackpressureSignal::Throttle {
                target_rate: self.capacity * 0.8,  // 80% capacity
                reason: "Partition overload detected".to_string(),
            }
        } else {
            BackpressureSignal::None
        }
    }
}
```

**Behavior**:
- Monitor per-partition queue depth and latency
- Signal router to reduce ingestion rate for overloaded partitions
- Gradual throttling (80% → 60% → 40% capacity) to prevent cascading failures
- Automatic recovery when metrics return to normal

**Limitations**:
- Does not fix root cause (skewed keys still overload partition)
- Upstream Kafka lag increases during throttling
- Reduces overall system throughput to slowest partition

**Phasing**: ✅ **Phase 3** (backpressure detection + throttling)

---

### Mitigation Strategy 3: Partition Splitting (Post-Phase 5)

**Goal**: Dynamically increase partition count for hot keys

```rust
// Detect hot partition and split it into sub-partitions
pub struct AdaptivePartitionManager {
    partition_metrics: Vec<PartitionMetrics>,
    split_threshold: f64,  // e.g., 2x average throughput
}

impl AdaptivePartitionManager {
    async fn detect_and_split_hot_partitions(&mut self) {
        let avg_throughput = self.calculate_average_throughput();

        for (partition_id, metrics) in self.partition_metrics.iter().enumerate() {
            if metrics.throughput() > avg_throughput * self.split_threshold {
                // Partition overloaded - trigger split
                self.split_partition(partition_id, 2).await;
            }
        }
    }

    async fn split_partition(&mut self, partition_id: usize, split_factor: usize) {
        // 1. Create split_factor new partitions
        // 2. Reroute future records to new partitions
        // 3. Migrate existing state (gradual, background)
        // 4. Remove old partition once migration complete
    }
}
```

**Complexity**: High (requires state migration, query plan rewriting)

**Phasing**: Post-Phase 5 (research project)

---

### Architectural Trade-offs: Join Type Comparison

| Join Type | Partitionability | Congestion Risk | Throughput | Memory | Phasing |
|-----------|-----------------|-----------------|-----------|---------|---------|
| **Broadcast JOIN** (small table) | ✅ Fully parallel | ❌ None (local lookups) | 1.5M rec/sec | High (replicated) | Phase 2 ✅ |
| **Co-partitioned JOIN** (same key) | ✅ Fully parallel | ⚠️ If skewed keys | 1.2M rec/sec | Medium | Phase 4 |
| **Co-partitioned + Salting** | ✅ Fully parallel | ✅ Mitigated | 1.2M rec/sec | Medium | Post-Phase 5 |
| **Repartition JOIN** (different keys) | ⚠️ Multi-stage | ⚠️ Shuffle overhead | 500-800K rec/sec | High (2x buffers) | Post-Phase 5 |
| **No correlation** | ❌ Requires broadcast | ❌ Memory limits | Variable | Very high | Not planned |

---

### Recommendations for Phase 1-2 Implementation

**✅ Include in Phase 1-2**:
1. **Pure GROUP BY aggregations** (28% of workload, fully partitionable)
2. **Broadcast JOINs with small lookup tables** (<10MB, no congestion risk)
3. **Basic backpressure detection** (queue depth monitoring)

**⚠️ Defer to Phase 3-4**:
1. **Co-partitioned stream-stream joins** (requires backpressure + monitoring)
2. **Temporal window joins** (buffer management complexity)
3. **Hot key detection** (runtime profiling infrastructure)

**❌ Defer to Post-Phase 5 (Research)**:
1. **Salted key distribution** (requires query planner changes)
2. **Repartition joins** (multi-stage pipeline complexity)
3. **Adaptive partition splitting** (state migration complexity)

---

### Open Questions for Future Phases

1. **Hot Key Detection**:
   - What threshold defines a "hot key"? (e.g., >2x average, >100K rec/sec)
   - How often to profile key distribution? (every 10 seconds, 1 minute?)

2. **Salting Strategy**:
   - Static salt factor or dynamic based on skew severity?
   - How to merge salted aggregations efficiently?

3. **State Migration**:
   - Gradual migration (process new records on new partition, drain old partition)?
   - Snapshot-based migration (pause, transfer, resume)?

4. **Performance Impact**:
   - What is acceptable throughput degradation for skewed workloads? (20%? 50%?)
   - Should system reject queries that cannot be efficiently partitioned?

---

## Conclusion

### Summary

**V2 Partitioned Pipeline Architecture** provides:

1. **Ultra-Low-Latency**: p95 <1ms (100x improvement over V1: 50-100ms)
2. **True Parallelism**: Linear scaling across N cores
3. **High Throughput**: 1.5M rec/sec on 8 cores (65x improvement over V1: 23K)
4. **Horizontal Scaling**: 300M rec/sec on 10 machines
5. **Fine-Grained Backpressure**: Per-partition monitoring and throttling
6. **CPU Efficiency**: Core pinning + cache locality
7. **Fault Isolation**: Partition failures don't affect others

**⚠️ Known Limitations** (to be addressed post-Phase 5):
- Skewed join keys can cause partition congestion
- Repartition joins require multi-stage pipeline (not yet implemented)
- Hot key detection and mitigation deferred to future enhancement

### Next Steps

1. ✅ **Complete Phase 0** (Phase 4B + 4C): SQL engine baseline 200K rec/sec
2. 🔧 **Implement Phase 1** (Week 3): Hash router + partition manager
3. 🔧 **Validate scaling** (Week 4): Measure 2, 4, 8, 16 partition performance
4. 🔧 **Add observability** (Week 5): Prometheus + Grafana dashboards
5. 🔧 **Production hardening** (Weeks 6-8): System fields, watermarks, recovery

**Total Timeline**: 8 weeks (2 weeks Phase 0 + 6 weeks V2 implementation)

**Expected Outcome**: 65x improvement over V1 (23K → 1.5M rec/sec on 8 cores)

---

**Document Status**: Architecture Blueprint - Ready for Implementation
**Last Updated**: November 6, 2025
**Next Review**: After Phase 0 completion (Phase 4B + 4C benchmarks)
