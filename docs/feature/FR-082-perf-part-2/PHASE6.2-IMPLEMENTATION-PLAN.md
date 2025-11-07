# Phase 6.2: Parallel Processing Architecture - Implementation Plan

**Target**: Fix 8x scaling bottleneck via parallel reading and lock-free optimization
**Timeline**: November 14-15, 2025 (1-2 days)
**Complexity**: Medium (refactor main loop, optimize locks)
**Success Criteria**: V2@8p achieves ≥300K rec/sec (4x+ improvement over current 68K)

---

## Overview

Phase 6.1a implemented functional SQL execution but revealed that the **single-threaded main loop is the bottleneck** preventing 8x scaling. Phase 6.2 will:

1. **Parallelize the reading/routing pipeline** (Approach A: Parallel Reader Tasks)
2. **Optimize Arc<Mutex<StreamExecutionEngine>>** to Arc<RwLock> (simpler than per-partition state)
3. **Validate 8x scaling target** with new performance tests

---

## Implementation Steps

### Step 1: Analyze Current Main Loop Bottleneck (Baseline)

**File**: `src/velostream/server/v2/coordinator.rs:723-784`

**Current Flow**:
```rust
loop {
    // SERIAL: Only one thread doing all this work
    match Self::read_batch_from_sources(&mut context).await {
        Ok(Some(batch)) => {
            let routed = self.route_batch(&batch, &group_by_columns).await?;
            for (partition_id, records) in routed.iter().enumerate() {
                partition_senders[partition_id].send(records.clone()).await?;
            }
            stats.records_processed += batch.len() as u64;
        }
        Ok(None) => break,
        Err(e) => { /* error handling */ }
    }
}
```

**Problem**: This single loop is the critical path. By the time partition tasks receive data, the loop has already processed most available batches.

**Metrics to Capture**:
- Main loop throughput (batches/sec)
- Partition queue depths (how much data waiting)
- Lock contention frequency

### Step 2: Implement Parallel Reader Tasks (Primary Fix)

**File**: `src/velostream/server/v2/coordinator.rs` - New method `process_multi_job_parallel()`

**New Architecture**:

```rust
pub async fn process_multi_job(
    &self,
    readers: HashMap<String, Box<dyn DataReader>>,
    writers: HashMap<String, Box<dyn DataWriter>>,
    engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,  // Note: RwLock instead of Mutex
    query: StreamingQuery,
    job_name: String,
    mut shutdown_rx: mpsc::Receiver<()>,
) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
    // ... initialization ...

    // CHANGE 1: Create one reader task per source (PARALLEL READING)
    let source_names: Vec<_> = context.list_sources();
    let mut reader_tasks = Vec::with_capacity(source_names.len());

    for source_name in source_names {
        let context_clone = context.clone();  // Note: ProcessorContext needs Clone impl
        let partition_senders_clone = partition_senders.clone();
        let strategy_clone = self.strategy.clone();
        let job_name_clone = job_name.clone();
        let group_by_columns_clone = group_by_columns.clone();

        let reader_task = tokio::spawn(async move {
            Self::reader_task(
                &source_name,
                context_clone,
                partition_senders_clone,
                strategy_clone,
                group_by_columns_clone,
                job_name_clone,
            )
            .await
        });

        reader_tasks.push(reader_task);
    }

    // ... existing partition task spawning ...

    // Wait for all reader tasks to complete
    for (source_idx, task) in reader_tasks.into_iter().enumerate() {
        match task.await {
            Ok(Ok(source_stats)) => {
                stats.records_processed += source_stats.records_read;
                stats.batches_processed += source_stats.batches_read;
            }
            Ok(Err(e)) => {
                error!("Reader task {} failed: {:?}", source_idx, e);
                stats.batches_failed += 1;
            }
            Err(e) => {
                error!("Reader task {} panicked: {:?}", source_idx, e);
                stats.batches_failed += 1;
            }
        }
    }

    // ... existing result collection and partition task waiting ...

    Ok(stats)
}
```

**New Helper Method - Reader Task**:

```rust
async fn reader_task(
    source_name: &str,
    mut context: ProcessorContext,
    partition_senders: Vec<mpsc::Sender<Vec<StreamRecord>>>,
    strategy: Arc<dyn PartitioningStrategy>,
    group_by_columns: Vec<String>,
    job_name: String,
) -> Result<SourceReadStats, Box<dyn std::error::Error + Send + Sync>> {
    let mut stats = SourceReadStats::default();

    info!("Reader task for source '{}' starting", source_name);

    loop {
        // Each reader task reads from its own source independently
        context.set_active_reader(source_name)?;

        match context.read().await {
            Ok(batch) => {
                if batch.is_empty() {
                    continue;
                }

                stats.batches_read += 1;
                stats.records_read += batch.len() as u64;

                // Route batch to partitions (still sequential per reader, but parallel across readers)
                let mut partitioned: Vec<Vec<StreamRecord>> =
                    vec![Vec::new(); partition_senders.len()];

                for record in batch {
                    let routing_context = RoutingContext {
                        source_partition: None,
                        source_partition_key: None,
                        group_by_columns: group_by_columns.clone(),
                        num_partitions: partition_senders.len(),
                        num_cpu_slots: num_cpus::get(),
                    };

                    match strategy.route_record(&record, &routing_context).await {
                        Ok(partition_id) => {
                            partitioned[partition_id].push(record);
                        }
                        Err(_) => {
                            partitioned[0].push(record);  // Fallback to partition 0
                        }
                    }
                }

                // Send to partition channels (non-blocking)
                for (partition_id, records) in partitioned.into_iter().enumerate() {
                    if !records.is_empty() {
                        if partition_senders[partition_id].send(records).await.is_err() {
                            // Partition task exited
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Reader task for source '{}' failed: {:?}", source_name, e);
                // Continue trying or break depending on error type
                if is_fatal_error(&e) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // Check if source is finished
        if !context.has_more_data(source_name).await.unwrap_or(false) {
            info!("Reader task for source '{}' finished", source_name);
            break;
        }
    }

    Ok(stats)
}
```

**Impact**:
- ✅ Multiple threads now read from sources in parallel
- ✅ Partition tasks receive data more consistently
- ✅ Main thread no longer the bottleneck
- ✅ Expected improvement: 3-4x (limited by lock contention)

### Step 3: Optimize StreamExecutionEngine Lock to RwLock

**File**: `src/velostream/sql/execution/engine.rs` - Update engine lock type

**Current State**:
```rust
// In coordinator.rs
engine: Arc<tokio::sync::Mutex<StreamExecutionEngine>>
```

**Change To**:
```rust
// In coordinator.rs
engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>
```

**Update execute_batch() Method**:

```rust
async fn execute_batch(
    batch: &[StreamRecord],
    engine: &Arc<tokio::sync::RwLock<StreamExecutionEngine>>,  // CHANGED: RwLock
    query: &StreamingQuery,
    job_name: &str,
) -> Result<Vec<Arc<StreamRecord>>, Box<dyn std::error::Error + Send + Sync>> {
    // ... setup ...

    // Get state from engine (READ lock - multiple readers can coexist)
    let (group_states, window_states) = {
        let engine_lock = engine.read().await;  // CHANGED: .read() instead of .lock()
        (
            engine_lock.get_group_states().clone(),
            engine_lock.get_window_states(),
        )
    };

    // Process records without holding lock
    let mut output_records = Vec::new();
    for record in batch {
        match QueryProcessor::process_query(query, record, &mut context) {
            Ok(result) => {
                if let Some(output) = result.record {
                    output_records.push(Arc::new(output));
                }
            }
            Err(e) => {
                warn!("Job '{}': Failed to process record: {:?}", job_name, e);
            }
        }
    }

    // Update state in engine (WRITE lock - serialized, but faster than Mutex)
    {
        let mut engine_lock = engine.write().await;  // CHANGED: .write() instead of .lock()
        engine_lock.set_group_states(context.group_by_states);
        engine_lock.set_window_states(context.persistent_window_states);
    }

    Ok(output_records)
}
```

**Impact**:
- ✅ Multiple partition tasks can READ state simultaneously
- ✅ Only WRITE operations block (state updates are fast)
- ✅ Expected improvement: 1.5-2x (from current Mutex)
- ✅ Combined with parallel readers: 4.5-8x overall

### Step 4: Update StreamJobServer to Use New process_multi_job()

**File**: `src/velostream/server/stream_job_server.rs:820-870`

**Current Code**:
```rust
let v2 = PartitionedJobCoordinator::new(config);
let result = v2.process_multi_job(readers, writers, engine, query, ...).await?;
```

**No changes needed** - the updated `process_multi_job()` is backward compatible. StreamJobServer will automatically benefit from the parallel processing.

### Step 5: Implement SourceReadStats Tracking

**File**: `src/velostream/server/v2/coordinator.rs` - Add new struct

```rust
#[derive(Debug, Default)]
struct SourceReadStats {
    records_read: u64,
    batches_read: u64,
}
```

### Step 6: Add ProcessorContext Clone Support

**File**: `src/velostream/sql/execution/processors/context.rs`

Add `Clone` implementation (if not already present):

```rust
#[derive(Clone)]
pub struct ProcessorContext {
    query_id: String,
    readers: Arc<RwLock<HashMap<String, Box<dyn DataReader>>>>,
    writers: Arc<RwLock<HashMap<String, Box<dyn DataWriter>>>>,
    // ... other fields that need Clone ...
}
```

**Note**: This may require changing `Box<dyn DataReader>` to `Arc<dyn DataReader>` for Clone to work safely.

### Step 7: Update Tests for Lock Type

**File**: `tests/unit/server/v2/phase6_v1_vs_v2_performance_test.rs`

Update test setup:
```rust
let engine = Arc::new(tokio::sync::RwLock::new(StreamExecutionEngine::new(tx)));
```

### Step 8: New Scaling Validation Tests

**File**: `tests/unit/server/v2/phase6_2_parallel_scaling_test.rs` (NEW)

Create comprehensive scaling tests:

```rust
#[tokio::test]
#[ignore]
async fn test_v2_parallel_scaling_1_to_16_cores() {
    // Run V2 with 1, 2, 4, 8, 16 partitions
    // Measure scaling efficiency
    // Validate ≥85% efficiency (vs theoretical 100% linear scaling)
}

#[tokio::test]
#[ignore]
async fn test_v2_parallel_reading_benefit() {
    // Test with realistic I/O (file-based mock reader)
    // Measure improvement from parallel readers
    // Expected: 2-3x vs single reader
}

#[tokio::test]
#[ignore]
async fn test_rwlock_optimization_benefit() {
    // Profile lock contention before/after RwLock
    // Validate reduced lock wait times
}
```

---

## Performance Predictions

### Before Phase 6.2 (Current):
```
V2@1p:  68,387 rec/sec  (1 partition)
V2@8p:  68,379 rec/sec  (8 partitions - NO SCALING)
Efficiency: 0.36% (should be 100%)
```

### After Phase 6.2a (Parallel Readers Only):
```
V2@1p:  80,000 rec/sec  (baseline improves ~10%)
V2@8p: 240,000 rec/sec  (3.5x improvement)
Efficiency: 44% (limited by RwLock contention)
```

### After Phase 6.2b (Parallel Readers + RwLock):
```
V2@1p:  90,000 rec/sec  (baseline improves ~15%)
V2@8p: 360,000 rec/sec  (5.3x improvement)
Efficiency: 79% (approaching target)
```

### Target After Phase 6.2 Complete:
```
V2@1p:  90,000 rec/sec
V2@8p: 450,000 rec/sec
V2@16p: 700,000 rec/sec
Efficiency: 85-90% linear scaling
```

---

## Risk Mitigation

### Risk 1: ProcessorContext Not Clone-able
- **Mitigation**: May need to refactor to use Arc<RwLock<...>> for complex fields
- **Fallback**: Create separate reader context per task instead of cloning

### Risk 2: RwLock Slower on Low Contention
- **Mitigation**: Profile first, RwLock should be faster on modern Tokio
- **Fallback**: Keep Mutex if RwLock doesn't improve performance

### Risk 3: Reader Tasks Desynchronize
- **Mitigation**: Each reader task handles its own source independently
- **Fallback**: Add synchronization if race conditions occur

### Risk 4: Partition Channel Overflow
- **Mitigation**: Current buffer_size=1000 should handle 8 reader tasks
- **Fallback**: Increase buffer size if needed (currently adequate)

---

## Validation Checklist

Before declaring Phase 6.2 complete:

- [ ] Code compiles without warnings
- [ ] All 520 unit tests pass
- [ ] Performance tests show 4x+ improvement (V2@8p > 300K rec/sec)
- [ ] Scaling efficiency ≥85% (8x partitions = 6.8x+ throughput)
- [ ] No lock contention spikes in profiling
- [ ] No data correctness regressions
- [ ] Partition queue depths normal (not overflowing)
- [ ] Reader tasks complete successfully
- [ ] No memory leaks or deadlocks

---

## Timeline

| Task | Time | Notes |
|---|---|---|
| Step 1-2: Implement parallel readers | 1 day | Main work |
| Step 3: Optimize to RwLock | 2 hours | Straightforward |
| Step 4-7: Update tests & context | 2 hours | Minor changes |
| Step 8: Validation & profiling | 1 day | Performance validation |
| **Total** | **1-2 days** | Ready to execute |

---

## Success Metrics

After Phase 6.2:

| Metric | Before | After | Target |
|---|---|---|---|
| **V2@8p Throughput** | 68K rec/sec | 350-450K rec/sec | 450K+ rec/sec |
| **Scaling Efficiency** | 0.36% | 79-85% | 85-90% |
| **Lock Contention** | High (Mutex) | Low (RwLock) | Minimal |
| **Reader Bottleneck** | 1 thread | N threads | Eliminated |
| **Overall Score** | ❌ Fail | ✅ Pass | ✅✅ Pass+ |

---

## Next Phase: Phase 7 (Optional)

After Phase 6.2 achieves 8x scaling, Phase 7 can add:
- **SIMD Vectorization**: Process multiple records in parallel (2-3x)
- **Advanced Partitioning**: Detect hot partitions and rebalance
- **Adaptive Batching**: Dynamically adjust batch sizes

Expected cumulative improvement: **16-24x** by end of Phase 7.

---

**Document**: Phase 6.2 Implementation Plan
**Status**: Ready to Execute
**Estimated Effort**: 1-2 days
**Blocked By**: Nothing
**Next Step**: Execute Phase 6.2a (Parallel Readers Implementation)
