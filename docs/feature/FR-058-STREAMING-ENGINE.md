# FR-058: Streaming SQL Engine Architecture Redesign

## Feature Request Summary

**Title**: Redesign StreamExecutionEngine from Lock-Based to Message-Passing Architecture  
**Type**: Architecture Enhancement  
**Priority**: High  
**Status**: Specification  
**Epic**: Core Engine Performance  

## Problem Statement

The current `StreamExecutionEngine` implementation uses a lock-based architecture that creates deadlocks and limits scalability. When processing batches, the `SimpleProcessor` locks the engine for every record, preventing the engine's internal message processing loop from running. This causes the internal bounded channel to fill up (200 capacity), leading to pipeline deadlocks after exactly 200 records.

### Current Architecture Issues

```rust
// Current problematic pattern in SimpleProcessor
for record in batch {
    let mut engine_lock = engine.lock().await;  // 🔒 BLOCKS ENGINE
    engine_lock.execute_with_record(query, record).await;  // Direct call
}
// Problem: engine.start() message loop can never run while locked!
```

**Symptoms:**
- Benchmarks hang after exactly 200 records (2x channel capacity)
- Reader stops being called after initial batches
- No records reach the DataWriter
- Channel fills up but never drains (message loop blocked by locks)

## Industry Analysis

### How Leading Stream Engines Handle This

**Apache Flink:**
- **Message-passing** with mailbox model
- Each task runs in own thread with bounded mailbox
- Records flow through async queues between operators
- **Credit-based backpressure** - downstream grants credits to upstream
- Errors handled asynchronously, escalated to job/operator failure

**Kafka Streams:**
- **Message-driven** model (records from topic partitions)
- Each stream thread has task loop: pull → process → push
- Uses **batching** for efficiency, no per-record locking
- **Pull-based backpressure** - consumers poll at their own pace
- Errors fail stream thread → trigger restart

**ksqlDB & Materialize:**
- Built on message-passing foundations
- Async fault tolerance with checkpointing
- Strong ordering guarantees within partitions

**Industry Consensus:**
- Lock-based models don't scale beyond single-threaded processing
- Message-passing is industry standard for streaming engines
- Backpressure handled via bounded channels/queues
- Async error handling with correlation IDs

## Architecture Comparison

### Current Lock-Based Architecture

#### ✅ Pros:
- **Simple mental model** - direct method calls
- **Synchronous errors** - immediate error handling per record
- **No message serialization overhead**
- **Deterministic execution order**
- **Easy debugging** - stack traces show direct call paths
- **Transactional semantics** - each record processed atomically

#### ❌ Cons:
- **Deadlock prone** - engine can't process messages while locked
- **Poor concurrency** - only one batch processes at a time
- **Blocking backpressure** - entire pipeline stops when engine busy
- **Scalability limits** - can't distribute across threads
- **Resource contention** - all work under single lock
- **Industry anti-pattern** - no major streaming engine uses this approach

### Proposed Message-Passing Architecture

#### ✅ Pros:
- **True async processing** - engine runs independently
- **Natural backpressure** - bounded channels provide flow control
- **Concurrent processing** - multiple batches can be "in flight"
- **Scalability** - can distribute across multiple engine instances
- **No deadlocks** - no shared mutable state
- **Resource efficiency** - better CPU utilization
- **Industry alignment** - follows Flink/Kafka Streams patterns
- **Future-proof** - enables distributed execution

#### ❌ Cons:
- **Complex error handling** - errors are asynchronous
- **Message ordering** - harder to guarantee processing order
- **Latency overhead** - message queue adds latency
- **Debugging complexity** - async stack traces harder to follow
- **Result coordination** - need to correlate inputs with outputs
- **Memory overhead** - messages queued in channels

## Requirements

### Functional Requirements

1. **Message-Passing Core**
   - Replace lock-based `execute_with_record()` with async message passing
   - Engine runs background message processing loop (`start()` method)
   - Processors send `ExecutionMessage::ProcessRecord` to engine
   - Engine processes messages and emits results to output channel

2. **Backpressure Management**
   - Bounded channels between processor and engine (configurable size)
   - When channel fills, `send()` blocks providing natural backpressure
   - Backpressure flows: Reader ← Processor ← Engine Channel Full
   - Monitor queue fill percentage for observability

3. **Error Handling**
   - Async error propagation with correlation IDs
   - Configurable error strategies: Fail Fast, Dead Letter Queue, Skip & Continue
   - Error metrics and logging for debugging

4. **Ordering Guarantees**
   - Maintain record processing order within single stream partition
   - Support multiple concurrent partitions for parallelism

5. **Batch Optimization**
   - Process small batches through message system (not individual records)
   - Reduce message overhead while maintaining responsive backpressure

### Non-Functional Requirements

1. **Performance**
   - Throughput: Target >10k records/sec (vs current ~8 records/sec)
   - Latency: <1ms additional overhead from message passing
   - Memory: Bounded memory usage via channel capacity limits

2. **Scalability**
   - Support multiple concurrent processor instances
   - Enable future distributed execution across nodes

3. **Reliability**
   - Zero deadlocks under normal operation
   - Graceful degradation under backpressure
   - Proper shutdown and resource cleanup

4. **Observability**
   - Metrics: queue depth, processing rate, error rate
   - Structured logging for async error correlation
   - Health checks for engine background tasks

## Design Options

### Option 1: Pure Message-Passing (Recommended)

```rust
// Processor sends messages to background engine
let message = ExecutionMessage::ProcessBatch {
    batch_id: uuid::Uuid::new_v4(),
    records: batch,
    correlation_id: generate_correlation_id(),
};
engine_sender.send(message).await?;  // Blocks if channel full (backpressure)

// Background engine task processes messages
async fn engine_task(mut receiver, output_sender) {
    while let Some(message) = receiver.recv().await {
        match message {
            ProcessBatch { batch_id, records, correlation_id } => {
                let results = process_records(records).await;
                output_sender.send(BatchResult { batch_id, results, correlation_id }).await;
            }
        }
    }
}
```

### Option 2: Hybrid Architecture

- Default: Message-passing for production workloads
- Fallback: Direct processing mode for testing/debugging
- Configuration flag to choose execution mode

### Option 3: Batched Messages

- Send entire batches as single messages (reduce message overhead)
- Maintain backpressure at batch level rather than record level
- Better performance, slightly coarser backpressure control

## Implementation Plan

### Phase 1: Foundation (Week 1)
- [ ] Add `get_message_sender()` method to `StreamExecutionEngine`
- [ ] Modify `process_batch_with_output()` to use message-passing
- [ ] Ensure `engine.start()` runs in background task
- [ ] Add correlation IDs for async error handling

### Phase 2: Backpressure & Error Handling (Week 2)
- [ ] Implement proper backpressure flow through bounded channels
- [ ] Add async error propagation with correlation
- [ ] Create configurable error handling strategies
- [ ] Add comprehensive logging and metrics

### Phase 3: Optimization (Week 3)
- [ ] Implement batch-level message passing
- [ ] Optimize channel sizes based on benchmarking
- [ ] Add performance monitoring and health checks
- [ ] Comprehensive testing across all processor types

### Phase 4: Advanced Features (Week 4)
- [ ] Support multiple concurrent engine instances
- [ ] Add partition-based processing for parallelism
- [ ] Implement graceful shutdown and resource cleanup
- [ ] Documentation and migration guide

## Success Criteria

### Performance Targets
- [ ] Benchmark processes all 10,000 records without hanging
- [ ] Throughput >1000 records/sec (vs current 8 records/sec)  
- [ ] Memory usage remains bounded under load
- [ ] Zero deadlocks in stress testing

### Functional Validation
- [ ] All existing tests pass with new architecture
- [ ] Proper error handling and propagation
- [ ] Backpressure correctly slows upstream processing
- [ ] Resource cleanup on shutdown

### Operational Excellence
- [ ] Clear metrics for monitoring queue health
- [ ] Structured logging enables debugging async issues
- [ ] Performance characteristics well-documented
- [ ] Migration path from current architecture

## Risks & Mitigation

### High Risk: Async Error Complexity
- **Risk**: Harder to debug async error propagation
- **Mitigation**: Comprehensive correlation IDs, structured logging, detailed documentation

### Medium Risk: Performance Regression
- **Risk**: Message overhead might reduce performance
- **Mitigation**: Thorough benchmarking, batch optimization, performance monitoring

### Medium Risk: Migration Complexity  
- **Risk**: Breaking changes to existing processors
- **Mitigation**: Phased rollout, backward compatibility where possible, comprehensive testing

### Low Risk: Ordering Guarantees
- **Risk**: Message-passing might break record ordering
- **Mitigation**: Single-threaded processing per partition, well-defined ordering semantics

## Acceptance Criteria

- [ ] `benchmark_simple_select_baseline` processes all 10,000 records successfully
- [ ] No deadlocks under normal or stress conditions
- [ ] Throughput improvement of at least 100x over current implementation
- [ ] All existing functionality preserved
- [ ] Comprehensive error handling and observability
- [ ] Clean shutdown and resource management
- [ ] Industry-standard architecture alignment

## References

- [Apache Flink Architecture](https://nightlies.apache.org/flink/flink-docs-stable/concepts/flink-architecture/)
- [Kafka Streams Architecture](https://kafka.apache.org/documentation/streams/architecture)
- [Backpressure in Stream Processing](https://www.ververica.com/blog/how-flink-handles-backpressure)
- [Mailbox Model Implementation](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/flink-architecture/#mailbox-model)

---

**Next Steps**: Review this specification with stakeholders and get approval before implementation begins.