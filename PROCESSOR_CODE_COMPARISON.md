# Code Comparison: SimpleJobProcessor vs AdaptiveJobProcessor

## Main Processing Loop

### SimpleJobProcessor (V1)
**File**: `src/velostream/server/processors/simple.rs` lines 217-395
**Method**: `process_multi_job()`

```rust
pub async fn process_multi_job(
    &self,
    readers: HashMap<String, Box<dyn DataReader>>,
    writers: HashMap<String, Box<dyn DataWriter>>,
    engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
    query: StreamingQuery,
    job_name: String,
    _shutdown_rx: mpsc::Receiver<()>,
) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
    let mut stats = JobExecutionStats::new();

    // Main processing loop
    loop {
        // Check for stop signal from processor
        if self.stop_flag.load(Ordering::Relaxed) {
            info!("Job '{}' received stop signal", job_name);
            break;
        }

        // Check if all sources have finished processing (polling)
        let sources_finished = {
            let source_names = context.list_sources();
            let mut all_finished = true;
            for source_name in source_names {
                match context.has_more_data(&source_name).await {
                    Ok(has_more) => {
                        if has_more {
                            all_finished = false;
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Failed to check has_more for source '{}'", source_name);
                        all_finished = false;
                        break;
                    }
                }
            }
            all_finished
        };

        if sources_finished {
            info!("All sources have finished - no more data to process");
            break;
        }

        // Process from all sources
        match self.process_data(&mut context, &engine, &query, &job_name, &mut stats).await {
            Ok(()) => {
                if self.config.log_progress {
                    log_job_progress(&job_name, &stats);
                }
            }
            Err(e) => {
                warn!("Batch processing failed: {:?}", e);
                stats.batches_failed += 1;
                tokio::time::sleep(self.config.retry_backoff).await;
            }
        }
    }

    // Final commit and flush
    for source_name in context.list_sources() {
        if let Err(e) = context.commit_source(&source_name).await {
            error!("Failed to commit source '{}'", source_name);
        }
    }

    if let Err(e) = context.flush_all().await {
        warn!("Failed to flush all sinks");
    }

    Ok(stats)
}
```

**Key Characteristics**:
- Polling loop to check if sources finished
- Sequential source processing via `process_data()`
- Error handling with retry backoff
- Explicit final commit/flush at end

---

### AdaptiveJobProcessor (V2)
**File**: `src/velostream/server/v2/job_processor_v2.rs` lines 74-215
**Method**: `process_job()`

```rust
async fn process_job(
    &self,
    mut reader: Box<dyn DataReader>,
    writer: Option<Box<dyn DataWriter>>,
    _engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
    query: StreamingQuery,
    job_name: String,
    _shutdown_rx: mpsc::Receiver<()>,
) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
    let start_time = std::time::Instant::now();
    let mut aggregated_stats = JobExecutionStats::new();

    // Step 1: Wrap query in Arc
    let query_arc = Arc::new(query);

    // Step 2: Initialize partitions with Phase 6.6 synchronous receivers
    let shared_writer = writer.map(|w| Arc::new(tokio::sync::Mutex::new(w)));
    let (batch_senders, _metrics) =
        self.initialize_partitions_v6_6(query_arc.clone(), shared_writer.clone());

    info!("Initialized {} Phase 6.6 partition receivers", batch_senders.len());

    // Yield to allow partition receiver tasks to start
    tokio::task::yield_now().await;

    // Step 3: Read batches and route to receivers
    let mut consecutive_empty = 0;
    let mut total_routed = 0u64;
    let max_consecutive_empty = self.config().empty_batch_count;
    let wait_on_empty = Duration::from_millis(self.config().wait_on_empty_batch_ms);

    loop {
        // Check if processor stop signal was raised
        if self.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
            info!("Stop signal received, exiting read loop");
            break;
        }

        match reader.read().await {
            Ok(batch) => {
                if batch.is_empty() {
                    consecutive_empty += 1;
                    if consecutive_empty >= max_consecutive_empty {
                        info!("Reached max consecutive empty batches ({}), exiting", max_consecutive_empty);
                        break;
                    }
                    // Wait before next read to avoid busy-waiting
                    tokio::time::sleep(wait_on_empty).await;
                    continue;
                }

                consecutive_empty = 0;
                let batch_size = batch.len();

                // Route batch to receivers (Phase 6.6 batch-based routing)
                match self.process_batch_for_receivers(batch, &batch_senders).await {
                    Ok(routed_count) => {
                        aggregated_stats.records_processed += routed_count as u64;
                        aggregated_stats.batches_processed += 1;
                        total_routed += routed_count as u64;
                    }
                    Err(e) => {
                        log::warn!("Error routing batch: {:?}", e);
                        aggregated_stats.records_failed += batch_size as u64;
                    }
                }
            }
            Err(e) => {
                log::warn!("Error reading batch: {:?}", e);
                consecutive_empty += 1;
            }
        }
    }

    // Step 4: Close batch senders to signal EOF
    drop(batch_senders);

    // Step 5: Wait for receiver tasks to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Step 6: Finalize writer
    if let Some(writer_arc) = shared_writer {
        let mut w = writer_arc.lock().await;
        let _ = w.flush().await;
        let _ = w.commit().await;
    }

    // Step 7: Finalize reader
    let _ = reader.commit().await;

    aggregated_stats.total_processing_time = start_time.elapsed();
    Ok(aggregated_stats)
}
```

**Key Characteristics**:
- Single reader (no polling loop)
- Consecutive empty batch detection
- Non-blocking batch routing via channel
- Partition tasks run independently
- Simple final flush/commit

---

## SQL Execution (Record Processing)

### SimpleJobProcessor (V1)
**File**: `src/velostream/server/processors/common.rs` lines 323-639
**Function**: `process_batch()`

```rust
pub async fn process_batch(
    batch: Vec<StreamRecord>,
    engine: &Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
    query: &StreamingQuery,
    job_name: &str,
) -> BatchProcessingResultWithOutput {
    let batch_start = Instant::now();
    let batch_size = batch.len();
    let mut records_processed = 0;
    let mut records_failed = 0;
    let mut output_records = Vec::new();

    // Get context with potential lock
    let processor_context_arc = {
        let engine_lock = engine.read().await;
        if let Some(context_arc) = engine_lock.get_query_execution_context(query) {
            context_arc
        } else {
            // Fallback: need write lock for lazy init
            drop(engine_lock);
            let mut engine_lock = engine.write().await;
            match engine_lock.ensure_query_execution(query) {
                Some(context_arc) => context_arc,
                None => {
                    error!("Failed to initialize query execution");
                    return BatchProcessingResultWithOutput {
                        records_processed: 0,
                        records_failed: batch.len(),
                        // ... other fields
                    };
                }
            }
        }
    };

    // Get streaming config (another lock)
    let streaming_config = {
        let engine_lock = engine.read().await;
        engine_lock.streaming_config().clone()
    };

    // Set config and process ALL records under SINGLE Mutex lock
    {
        let mut ctx = processor_context_arc.lock().unwrap();
        ctx.streaming_config = Some(streaming_config);

        // Process records while holding Mutex lock
        for (index, record) in batch.into_iter().enumerate() {
            match QueryProcessor::process_query(query, &record, &mut ctx) {
                Ok(result) => {
                    records_processed += 1;
                    // Collect output
                    if let Some(output) = result.record {
                        output_records.push(Arc::new(output));
                    }
                }
                Err(e) => {
                    records_failed += 1;
                    error_details.push(ProcessingError {
                        record_index: index,
                        error_message: extract_error_context(&e),
                        recoverable: is_recoverable_error(&e),
                    });
                }
            }
        }
    } // MutexGuard dropped here

    BatchProcessingResultWithOutput {
        records_processed,
        records_failed,
        processing_time: batch_start.elapsed(),
        batch_size,
        error_details,
        output_records,
    }
}
```

**Lock Acquisition Pattern**:
```
Timeline:
  [engine.read()]  ← Get context reference (LOCK 1)
         ↓
  [drop, engine.write()]  ← Fallback: lazy init (LOCK 2)
         ↓
  [engine.read()]  ← Get streaming config (LOCK 3)
         ↓
  [processor_context_arc.lock()]  ← Process all records (LOCK 4)
         ↓
  [Mutex dropped]  ← Release, then continue

TOTAL: 2-3 async engine locks + 1 sync context lock
```

---

### AdaptiveJobProcessor (V2)
**File**: `src/velostream/server/v2/partition_receiver.rs` lines 153-228 & 253-285
**Methods**: `run()` and `process_batch()`

```rust
pub async fn run(&mut self) -> Result<(), SqlError> {
    debug!("PartitionReceiver {}: Starting synchronous processing loop", self.partition_id);

    let mut total_records = 0u64;
    let mut batch_count = 0u64;

    loop {
        // Wait for next batch (or EOF if channel closes)
        match self.receiver.recv().await {
            Some(batch) => {
                let start = Instant::now();
                let batch_size = batch.len();

                // Process batch SYNCHRONOUSLY (no async overhead)
                match self.process_batch(&batch) {
                    Ok((processed, output_records)) => {
                        total_records += processed as u64;
                        batch_count += 1;

                        self.metrics.record_batch_processed(processed as u64);
                        self.metrics.record_latency(start.elapsed());

                        debug!("Processed batch of {} records ({} output)", processed, output_records.len());

                        // Write output records to sink if available
                        if !output_records.is_empty() {
                            if let Some(ref writer_arc) = self.writer {
                                let mut writer = writer_arc.lock().await;
                                if let Err(e) = writer.write_batch(output_records).await {
                                    warn!("Error writing {} output records to sink: {}", batch_size, e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Error processing batch: {}", e);
                    }
                }
            }
            None => {
                // Channel closed - EOF signal
                debug!("Received EOF, shutting down (processed {} batches, {} records)",
                    batch_count, total_records);
                break;
            }
        }
    }

    Ok(())
}

// SYNCHRONOUS method - NO ASYNC/AWAIT
fn process_batch(
    &mut self,
    batch: &[StreamRecord],
) -> Result<(usize, Vec<Arc<StreamRecord>>), SqlError> {
    let mut processed = 0;
    let mut output_records = Vec::new();

    // Direct method calls on owned engine (NO LOCKS)
    for record in batch {
        match self
            .execution_engine
            .execute_with_record_sync(&self.query, record)
        {
            Ok(Some(output)) => {
                processed += 1;
                output_records.push(Arc::new(output));
            }
            Ok(None) => {
                processed += 1;
                // Record was buffered (windowed query)
            }
            Err(e) => {
                warn!("Error processing record in partition {}: {}", self.partition_id, e);
            }
        }
    }

    Ok((processed, output_records))
}
```

**Lock Acquisition Pattern**:
```
Timeline:
  [channel.recv().await]  ← Wait for batch
         ↓
  [process_batch()]  ← SYNCHRONOUS - NO LOCKS AT ALL
         ↓
  [writer_arc.lock().await]  ← ONLY lock if writing to sink
         ↓
  [repeat]

TOTAL: 0 locks in hot path (only writer Mutex for I/O)
```

---

## Writing Strategy

### SimpleJobProcessor (V1)
**File**: `src/velostream/server/processors/simple.rs` lines 663-771
**Method**: `process_data()`

```rust
// Decision point: should we commit?
let should_commit = should_commit_batch(
    self.config.failure_strategy,
    total_records_failed,
    job_name,
);

if should_commit {
    // ... write output records ...

    if !output_owned.is_empty() && !sink_names.is_empty() {
        if sink_names.len() == 1 {
            // Single sink: use move semantics
            let ser_start = Instant::now();
            let record_count = output_owned.len();
            match context.write_batch_to(&sink_names[0], output_owned).await {
                Ok(()) => {
                    let ser_duration = ser_start.elapsed().as_millis() as u64;
                    ObservabilityHelper::record_serialization_success(
                        self.observability_wrapper.observability_ref(),
                        job_name,
                        &parent_batch_span_guard,
                        record_count,
                        ser_duration,
                        None,
                    );
                }
                Err(e) => {
                    warn!("Failed to write {} records to sink '{}': {:?}",
                        record_count, &sink_names[0], e);
                    if matches!(self.config.failure_strategy, FailureStrategy::FailBatch) {
                        return Err(format!("Failed to write to sink: {:?}", e).into());
                    }
                }
            }
        } else {
            // Multiple sinks: use shared slice to avoid clones
            for sink_name in &sink_names {
                match context.write_batch_to_shared(sink_name, &output_owned).await {
                    Ok(()) => {
                        ObservabilityHelper::record_serialization_success(/*...*/);
                    }
                    Err(e) => {
                        warn!("Failed to write to sink '{}': {:?}", sink_name, e);
                    }
                }
            }
        }
    }

    // Commit all sources
    for source_name in &source_names {
        if let Err(e) = context.commit_source(source_name).await {
            error!("Failed to commit source '{}': {:?}", source_name, e);
        }
    }

    // Flush all sinks
    if let Err(e) = context.flush_all().await {
        warn!("Failed to flush sinks: {:?}", e);
    }

    stats.batches_processed += 1;
    stats.records_processed += total_records_processed as u64;
} else {
    stats.batches_failed += 1;
    warn!("Skipping commit due to {} failures", total_records_failed);
}
```

**Characteristics**:
- Decision point after processing
- Atomic batch (all or nothing)
- Sequential sink writes (per-sink async operation)
- Explicit commit/flush at end

---

### AdaptiveJobProcessor (V2)
**File**: `src/velostream/server/v2/partition_receiver.rs` lines 186-197
**Method**: `run()`

```rust
// Write output records to sink if available
if !output_records.is_empty() {
    if let Some(ref writer_arc) = self.writer {
        let mut writer = writer_arc.lock().await;
        if let Err(e) = writer.write_batch(output_records).await {
            warn!("PartitionReceiver {}: Error writing {} output records to sink: {}",
                self.partition_id, batch_size, e);
        }
    }
}
```

**Characteristics**:
- No decision point (always write)
- Continuous writing (after each batch)
- Single shared writer
- Minimal error handling

---

## Synchronization & Commit

### SimpleJobProcessor (V1)
**File**: `src/velostream/server/processors/simple.rs` lines 805-835

```rust
// Determine if batch should be committed
let should_commit = should_commit_batch(
    self.config.failure_strategy,
    total_records_failed,
    job_name,
);

if should_commit {
    // ... write output records ...

    // Commit all sources (sequential)
    for source_name in &source_names {
        if let Err(e) = context.commit_source(source_name).await {
            error!("Failed to commit source '{}': {:?}", source_name, e);
            ErrorTracker::record_error(/*...*/);
            if matches!(self.config.failure_strategy, FailureStrategy::FailBatch) {
                return Err(format!("Failed to commit: {:?}", e).into());
            }
        }
    }

    // Flush all sinks (sequential)
    if let Err(e) = context.flush_all().await {
        warn!("Failed to flush sinks: {:?}", e);
        ErrorTracker::record_error(/*...*/);
        if matches!(self.config.failure_strategy, FailureStrategy::FailBatch) {
            return Err(format!("Failed to flush: {:?}", e).into());
        }
    }

    // Update stats
    stats.batches_processed += 1;
    stats.records_processed += total_records_processed as u64;
    stats.records_failed += total_records_failed as u64;

    // Complete batch span
    if let Some(mut batch_span) = parent_batch_span_guard {
        let batch_duration = batch_start.elapsed().as_millis() as u64;
        batch_span.set_total_records(total_records_processed as u64);
        batch_span.set_batch_duration(batch_duration);
        batch_span.set_success();
    }
} else {
    // Batch failed
    stats.batches_failed += 1;
    warn!("Skipping commit due to {} failures", total_records_failed);

    if let Some(mut batch_span) = parent_batch_span_guard {
        batch_span.set_error(&format!("{} records failed", total_records_failed));
    }
}
```

**Synchronization Pattern**:
1. Per-batch decision based on failure_strategy
2. Atomic batch semantics
3. Sequential per-source commits
4. Error handling per commit operation

---

### AdaptiveJobProcessor (V2)
**File**: `src/velostream/server/v2/job_processor_v2.rs` lines 184-202

```rust
// Step 4: Close batch senders to signal EOF
drop(batch_senders);

// Step 5: Wait for receiver tasks to complete
tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

// Step 6: Finalize writer
if let Some(writer_arc) = shared_writer {
    let mut w = writer_arc.lock().await;
    let _ = w.flush().await;
    let _ = w.commit().await;
}

// Step 7: Finalize reader
let _ = reader.commit().await;

aggregated_stats.total_processing_time = start_time.elapsed();

info!(
    "V2 AdaptiveJobProcessor::process_job: {} completed\n  Records: {} routed in {:?}\n  Phase 6.6 batch-based processing: {} partitions with synchronous receivers (owned engines)",
    job_name,
    total_routed,
    aggregated_stats.total_processing_time,
    self.num_partitions()
);

Ok(aggregated_stats)
```

**Synchronization Pattern**:
1. No per-batch decision
2. Continuous processing
3. Channel close signals EOF
4. 100ms sleep waits for partitions
5. Single final flush/commit
6. No per-source error handling

---

## Summary of Differences

| Aspect | V1 (Simple) | V2 (Adaptive) |
|--------|------------|--------------|
| **Main Loop** | Polling all sources | Single reader |
| **Data Flow** | Batch → Process → Commit → Write | Batch → Route → Process (parallel) |
| **Locks per Batch** | 2-4 | 0 (hot path) |
| **Process Function** | Async | Synchronous |
| **Write Decision** | Per-batch | Continuous |
| **Commit Strategy** | After batch decision | End of job |
| **Per-Source Handling** | Explicit per source | Implicit in routing |
| **Parallelism** | None (sequential) | Full (N partitions) |
