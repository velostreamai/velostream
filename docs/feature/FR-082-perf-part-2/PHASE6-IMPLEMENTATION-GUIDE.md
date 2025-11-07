# Phase 6: Implementation Guide for process_multi_job()

**Target**: Implement PartitionedJobCoordinator.process_multi_job() for real SQL execution
**Estimated Effort**: 2-3 days
**Complexity**: High (coordination, state management, error handling)

---

## Overview

The process_multi_job() method is the core execution engine for V2 architecture. It transforms batches of input records into query results by:

1. **Routing**: Distributing records to partitions based on GROUP BY keys
2. **Processing**: Executing SQL independently in each partition
3. **Coordination**: Collecting results from all partitions
4. **Writing**: Sending results to output sinks

Current Status:
- ✅ JobProcessor trait implemented with pass-through
- ✅ StreamJobServer integration (logging processor config)
- ❌ Actual multi-partition SQL execution (to be implemented)

---

## Step-by-Step Implementation

### Step 1: Method Skeleton (File: `src/velostream/server/v2/coordinator.rs`)

Add this method to the `impl PartitionedJobCoordinator` block:

```rust
/// Execute multi-partition job processing with GROUP BY consistency
///
/// Distributes records to partitions based on configured routing strategy
/// (e.g., AlwaysHashStrategy, SmartRepartition, StickyPartition) to ensure
/// records with the same GROUP BY key route to the same partition.
pub async fn process_multi_job(
    &self,
    readers: HashMap<String, Box<dyn DataReader>>,
    writers: HashMap<String, Box<dyn DataWriter>>,
    engine: Arc<Mutex<StreamExecutionEngine>>,
    query: StreamingQuery,
    job_name: String,
    mut shutdown_rx: mpsc::Receiver<()>,
) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
    // TODO: Implementation steps 2-6 below
    todo!("Implement multi-partition job processing")
}
```

---

### Step 2: Initialize Context & Metrics

```rust
// Extract GROUP BY columns from query
let group_by_columns = Self::extract_group_by_columns(&query);
info!(
    "Job '{}': V2 processing with GROUP BY columns: {:?}",
    job_name, group_by_columns
);

// Create partition state managers (one per partition)
let mut partition_managers = Vec::with_capacity(self.num_partitions);
for partition_id in 0..self.num_partitions {
    let manager = PartitionStateManager::new(
        partition_id,
        self.num_partitions,
        job_name.clone(),
    );
    partition_managers.push(Arc::new(Mutex::new(manager)));
}

// Initialize statistics
let mut stats = JobExecutionStats::new();
let start_time = Instant::now();

// Create processor context (like SimpleJobProcessor does)
let context = ProcessorContext::new_with_sources(
    &job_name,
    readers,
    writers,
);

// Configure routing strategy with GROUP BY columns
let mut routing_context = RoutingContext::new(
    self.num_partitions,
    group_by_columns.clone(),
);
```

---

### Step 3: Create Partition Channels

```rust
// Create MPSC channels for each partition
let mut partition_senders = Vec::with_capacity(self.num_partitions);
let mut partition_receivers = Vec::with_capacity(self.num_partitions);

for _ in 0..self.num_partitions {
    let (tx, rx) = mpsc::channel(self.config.partition_buffer_size);
    partition_senders.push(tx);
    partition_receivers.push(rx);
}

// Create result collection channels
let (result_tx, mut result_rx) = mpsc::channel(100);
```

---

### Step 4: Main Processing Loop

```rust
loop {
    // Check shutdown signal
    if shutdown_rx.try_recv().is_ok() {
        info!("Job '{}' received shutdown signal", job_name);
        break;
    }

    // Check if all sources have data
    let sources_finished = Self::check_sources_finished(&context).await?;
    if sources_finished {
        info!("Job '{}': All sources finished", job_name);
        break;
    }

    // Read batch from sources
    match Self::read_batch_from_sources(&context).await {
        Ok(Some(batch)) => {
            if batch.is_empty() {
                continue;  // Skip empty batches
            }

            // Route batch to partitions using configured strategy
            let routed = self.route_batch(&batch, &routing_context)?;

            // Send routed records to partition channels
            for (partition_id, records) in routed.iter().enumerate() {
                if !records.is_empty() {
                    partition_senders[partition_id]
                        .send(records.clone())
                        .await?;
                }
            }

            stats.batches_processed += 1;
            stats.records_processed += batch.len() as u64;
        }
        Ok(None) => break,  // No more data
        Err(e) => {
            warn!("Job '{}': Failed to read batch: {:?}", job_name, e);
            stats.batches_failed += 1;
            continue;
        }
    }
}
```

---

### Step 5: Partition Processing Tasks

```rust
// Spawn partition processing tasks (parallel execution)
let partition_handles: Vec<_> = (0..self.num_partitions)
    .map(|partition_id| {
        let rx = partition_receivers.remove(0);  // Take receiver for this partition
        let engine = engine.clone();
        let query = query.clone();
        let job_name = job_name.clone();
        let result_tx = result_tx.clone();
        let manager = partition_managers[partition_id].clone();

        tokio::spawn(async move {
            Self::process_partition(
                partition_id,
                rx,
                engine,
                query,
                job_name,
                result_tx,
                manager,
            )
            .await
        })
    })
    .collect();

// Drop original sender so result collector knows when done
drop(result_tx);
```

---

### Step 6: Helper Methods

#### Extract GROUP BY Columns
```rust
fn extract_group_by_columns(query: &StreamingQuery) -> Vec<String> {
    // Query.group_by contains the GROUP BY clause
    // Extract column names from the Expr
    match &query.group_by {
        Some(exprs) => {
            exprs
                .iter()
                .filter_map(|expr| {
                    match expr {
                        Expr::Column(col) => Some(col.clone()),
                        Expr::QualifiedColumn { column, .. } => Some(column.clone()),
                        _ => None,
                    }
                })
                .collect()
        }
        None => Vec::new(),
    }
}
```

#### Route Batch to Partitions
```rust
fn route_batch(
    &self,
    batch: &[StreamRecord],
    routing_context: &RoutingContext,
) -> Result<Vec<Vec<StreamRecord>>, Box<dyn Error>> {
    // Create empty partitions
    let mut partitioned: Vec<Vec<StreamRecord>> =
        vec![Vec::new(); self.num_partitions];

    // Use configured strategy to determine partition for each record
    for record in batch {
        let partition_id = self.strategy.get_partition(
            record,
            self.num_partitions,
            routing_context,
        );

        partitioned[partition_id].push(record.clone());
    }

    Ok(partitioned)
}
```

#### Process Single Partition
```rust
async fn process_partition(
    partition_id: usize,
    mut rx: mpsc::Receiver<Vec<StreamRecord>>,
    engine: Arc<Mutex<StreamExecutionEngine>>,
    query: StreamingQuery,
    job_name: String,
    result_tx: mpsc::Sender<Vec<StreamRecord>>,
    state_manager: Arc<Mutex<PartitionStateManager>>,
) -> Result<(), Box<dyn Error>> {
    info!(
        "Job '{}': Partition {} starting",
        job_name, partition_id
    );

    while let Some(records) = rx.recv().await {
        // Execute SQL on this batch for this partition
        let engine_lock = engine.lock().await;

        let results = engine_lock.execute_batch(
            &records,
            &query,
        )?;

        // Update partition state
        {
            let mut manager = state_manager.lock().await;
            manager.record_processed(records.len());
        }

        // Send results upstream
        result_tx.send(results).await?;
    }

    info!(
        "Job '{}': Partition {} finished",
        job_name, partition_id
    );
    Ok(())
}
```

#### Collect Results and Write
```rust
// Collect results from all partitions
let mut writers_map = context.get_writers();

while let Some(results) = result_rx.recv().await {
    if !results.is_empty() {
        // Write to all configured sinks
        for (_sink_name, writer) in &mut writers_map {
            writer.write_batch(&results)?;
        }
        stats.records_written += results.len() as u64;
    }
}

// Wait for all partitions to finish
for handle in partition_handles {
    handle.await??;
}
```

---

## Key Design Patterns

### Pattern 1: Group By Consistency

The strategy MUST route records with the same GROUP BY key to the same partition:

```
GROUP BY user_id + user_name → Hash(user_id, user_name)
    ↓
    All records for (user_id=42, user_name="alice") → Partition 3
    All records for (user_id=43, user_name="bob") → Partition 1
    All records for (user_id=42, user_name="alice") → Partition 3  (again)
```

This ensures aggregation state stays consistent within each partition.

### Pattern 2: Partition Independence

Each partition operates independently:
- ✅ No locks between partitions (except result channel)
- ✅ No shared aggregation state
- ✅ Can run in parallel on different CPU cores
- ✅ Minimal coordination overhead

### Pattern 3: Result Merging

Results from all partitions must be merged:
- For non-aggregated queries: Just concatenate
- For aggregated queries: Merge partial results
- Order may differ between partitions: May need sort if ORDER BY used

---

## Error Handling

Add comprehensive error handling:

```rust
pub async fn process_multi_job(...) -> Result<JobExecutionStats, ...> {
    // Error scenarios to handle:

    // 1. Shutdown during processing
    if shutdown_rx.try_recv().is_ok() {
        info!("Graceful shutdown requested");
        // Cancel partition tasks
        for handle in partition_handles {
            handle.abort();
        }
    }

    // 2. Partition task failure
    for result in future::select_all(&partition_handles).await {
        if let Err(e) = result {
            error!("Partition task failed: {:?}", e);
            stats.batches_failed += 1;
        }
    }

    // 3. Channel closed (backpressure)
    if partition_senders[i].send(records).await.is_err() {
        warn!("Partition {} channel closed (backpressure)", i);
        // Either wait or drop records
    }

    Ok(stats)
}
```

---

## Testing Strategy

### Unit Tests
```rust
#[tokio::test]
async fn test_process_multi_job_v2_basic() {
    // Create coordinator with 2 partitions
    // Send 100 records with 10 groups
    // Verify: all records processed, routed to correct partitions
}

#[tokio::test]
async fn test_process_multi_job_groupby_consistency() {
    // Send records with GROUP BY keys in random order
    // Verify: all records with same key go to same partition
}
```

### Integration Tests
```rust
#[tokio::test]
async fn test_v2_vs_v1_same_results() {
    // Run same SQL query through V1 and V2
    // Verify: results are identical (order-independent)
}

#[tokio::test]
async fn test_v2_scaling_100k_records() {
    // Run V2 with 100K records, 1/2/4/8 partitions
    // Measure: throughput should scale linearly
    // Expected: 8 partitions = 8x V1 throughput
}
```

---

## Optimization Opportunities

### For Phase 6a (Lock-Free)
1. Replace Arc<Mutex<PartitionStateManager>> with Arc<AtomicU64> for metrics
2. Use RwLock instead of Mutex for state that's read-heavy
3. Minimize lock hold time in process_partition()

### For Phase 7 (SIMD)
1. Vectorize GROUP BY key hashing
2. Vectorize aggregation operations
3. Batch record copying in route_batch()

---

## Validation Checklist

Before declaring this complete:

- [ ] Code compiles without errors
- [ ] 6 existing integration tests still pass
- [ ] New V2 process_multi_job tests pass
- [ ] GROUP BY consistency validated
- [ ] Partition independence confirmed
- [ ] Results match V1 (functionally equivalent)
- [ ] Scaling efficiency ≥ 95% (linear)
- [ ] No memory leaks (tokio tasks properly awaited)
- [ ] Shutdown graceful (no panics)

---

## References

- SimpleJobProcessor.process_multi_job(): Lines 174-400 in `src/velostream/server/processors/simple.rs`
- PartitioningStrategy trait: `src/velostream/server/v2/partitioning_strategy.rs`
- HashRouter: `src/velostream/server/v2/hash_router.rs`
- StreamExecutionEngine.execute_batch(): `src/velostream/sql/mod.rs`

---

**Document**: Phase 6 Implementation Guide
**Status**: Ready for Development
**Estimated Time**: 2-3 days
**Next Step**: Clone this document and begin implementation
