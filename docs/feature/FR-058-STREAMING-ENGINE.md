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

4. **Observability & Monitoring**
   - **Real-time Metrics**: Queue depth, processing rate, error rate, latency percentiles
   - **Structured Logging**: Async error correlation with trace IDs
   - **Health Checks**: Engine background task monitoring
   - **Distributed Tracing**: End-to-end request flow visibility
   - **Performance Profiling**: CPU, memory, and I/O bottleneck detection

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
- [ ] **CRITICAL**: Fix hanging tests disabled in commit 108d42b
  - `test_simple_processor_sink_failure_continues_processing` - hangs due to unbounded channel loops
  - `test_15_minute_moving_average` - timing/ordering issues with new channel system  
  - `test_1_hour_moving_average` - timing/ordering issues with new channel system
  - `test_4_hour_moving_average` - timing/ordering issues with new channel system

### Phase 2: Backpressure & Error Handling (Week 2)
- [ ] Implement proper backpressure flow through bounded channels
- [ ] Add async error propagation with correlation
- [ ] Create configurable error handling strategies
- [ ] **OBSERVABILITY**: Implement comprehensive metrics collection (StreamEngineMetrics, ProcessorMetrics)
- [ ] **OBSERVABILITY**: Add structured logging with trace context propagation
- [ ] **OBSERVABILITY**: Create health check endpoints with degradation detection

### Phase 3: Optimization (Week 3)
- [ ] Implement batch-level message passing
- [ ] Optimize channel sizes based on benchmarking
- [ ] **OBSERVABILITY**: Implement distributed tracing with OpenTelemetry/Jaeger
- [ ] **OBSERVABILITY**: Add business metrics and performance profiling integration  
- [ ] **OBSERVABILITY**: Build real-time operations dashboard (Grafana)
- [ ] Comprehensive testing across all processor types

### Phase 4: Advanced Features (Week 4)
- [ ] Support multiple concurrent engine instances
- [ ] Add partition-based processing for parallelism
- [ ] Implement graceful shutdown and resource cleanup
- [ ] **OBSERVABILITY**: Set up continuous profiling and anomaly detection in production
- [ ] **OBSERVABILITY**: Create runbook automation and capacity planning dashboards
- [ ] Documentation and migration guide

## Observability & Monitoring Architecture

### Core Observability Requirements

A message-passing streaming engine introduces async complexity that demands comprehensive observability for production operation, debugging, and performance optimization.

### 1. Metrics Collection & Monitoring

#### **Engine-Level Metrics**
```rust
pub struct StreamEngineMetrics {
    // Channel Health
    pub queue_depth: Gauge,           // Current messages in queue
    pub queue_capacity_utilization: Gauge,  // % of channel capacity used
    pub queue_high_water_mark: Counter,     // Times queue >90% full
    
    // Processing Performance  
    pub records_processed_total: Counter,
    pub records_failed_total: Counter,
    pub processing_duration_seconds: Histogram,  // P50, P95, P99 latencies
    pub batch_size_distribution: Histogram,
    
    // Backpressure & Flow Control
    pub backpressure_events: Counter,
    pub channel_send_duration: Histogram,
    pub channel_recv_duration: Histogram,
    
    // Error Tracking
    pub errors_by_type: CounterVec,    // Labels: error_type, correlation_id
    pub retry_attempts: Counter,
    pub dead_letter_messages: Counter,
}
```

#### **Processor-Level Metrics**
```rust
pub struct ProcessorMetrics {
    // Pipeline Health
    pub active_processors: Gauge,
    pub processor_restarts: Counter,
    pub processor_uptime_seconds: Gauge,
    
    // Resource Utilization
    pub cpu_usage_percent: Gauge,
    pub memory_usage_bytes: Gauge,
    pub gc_collections: Counter,      // For memory-managed workloads
    
    // Data Flow
    pub input_rate_records_per_sec: Gauge,
    pub output_rate_records_per_sec: Gauge,
    pub processing_lag_seconds: Gauge,
}
```

#### **Business Logic Metrics**
```rust
pub struct BusinessMetrics {
    // SQL Query Performance
    pub query_execution_duration: Histogram,  // Labels: query_type, table
    pub aggregation_window_size: Histogram,
    pub join_operation_duration: Histogram,
    
    // Financial Analytics (Domain-Specific)
    pub trades_processed: Counter,
    pub price_updates_applied: Counter,
    pub risk_calculations_completed: Counter,
    pub portfolio_valuations: Counter,
}
```

### 2. Distributed Tracing

#### **Trace Context Propagation**
```rust
pub struct TraceContext {
    pub trace_id: String,        // Unique across entire request flow
    pub span_id: String,         // Unique within trace
    pub parent_span_id: Option<String>,
    pub correlation_id: String,  // Business correlation (trade_id, etc.)
    pub baggage: HashMap<String, String>,  // Cross-service context
}

pub struct ExecutionMessage {
    pub trace_context: TraceContext,
    pub payload: MessagePayload,
    pub timestamp: Instant,
    pub retry_count: u32,
}
```

#### **Instrumentation Points**
- **Message Ingestion**: Kafka consumer → Engine queue
- **Engine Processing**: Message dequeue → SQL execution → Result emission  
- **Backpressure Events**: Channel full → Backpressure propagation
- **Error Handling**: Exception → Retry → Dead letter queue
- **Cross-Service Calls**: Engine → External services (schema registry, etc.)

#### **Jaeger/OpenTelemetry Integration**
```rust
use opentelemetry::{trace::Tracer, Context};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[tracing::instrument(
    skip(self, message),
    fields(
        trace_id = %message.trace_context.trace_id,
        correlation_id = %message.trace_context.correlation_id,
        queue_depth = self.get_queue_depth()
    )
)]
async fn process_message(&self, message: ExecutionMessage) -> Result<(), ProcessingError> {
    let span = tracing::Span::current();
    span.set_attribute("processing.batch_size", message.payload.records.len() as i64);
    
    // Processing logic with automatic span propagation
    self.execute_with_tracing(message).await
}
```

### 3. Structured Logging

#### **Log Levels & Categories**
- **ERROR**: Processing failures, system errors, resource exhaustion
- **WARN**: Backpressure events, retry attempts, performance degradation
- **INFO**: Processing milestones, configuration changes, health status
- **DEBUG**: Message flow details, correlation tracking
- **TRACE**: Fine-grained execution steps (development/troubleshooting)

#### **Log Structure**
```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO", 
  "message": "Batch processed successfully",
  "trace_id": "550e8400-e29b-41d4-a716-446655440000",
  "correlation_id": "trade_batch_20240115_001",
  "component": "stream_execution_engine",
  "processor_id": "simple_processor_001",
  "metrics": {
    "records_processed": 1250,
    "processing_duration_ms": 45,
    "queue_depth_before": 2340,
    "queue_depth_after": 1090,
    "memory_usage_mb": 256
  },
  "context": {
    "query_type": "aggregation",
    "table": "financial_trades",
    "batch_size": 1250
  }
}
```

### 4. Health Checks & Alerting

#### **Health Check Endpoints**
```rust
pub struct HealthCheckService {
    engines: Vec<Arc<StreamExecutionEngine>>,
    processors: Vec<Arc<dyn JobProcessor>>,
}

impl HealthCheckService {
    pub async fn check_engine_health(&self) -> HealthStatus {
        HealthStatus {
            status: if self.all_engines_healthy() { "healthy" } else { "degraded" },
            checks: vec![
                Check { name: "channel_capacity", status: self.check_channel_capacity() },
                Check { name: "processing_rate", status: self.check_processing_rate() },
                Check { name: "error_rate", status: self.check_error_rate() },
                Check { name: "memory_usage", status: self.check_memory_usage() },
            ],
            timestamp: Utc::now(),
        }
    }
}
```

#### **Critical Alerts**
- **Queue Depth**: Alert if >80% capacity for >5 minutes
- **Processing Rate**: Alert if <50% of baseline for >2 minutes  
- **Error Rate**: Alert if >5% error rate for >1 minute
- **Memory Growth**: Alert if memory usage growing >10%/hour
- **Engine Restarts**: Alert on any unexpected engine restart
- **Correlation Loss**: Alert if trace correlation drops <95%

### 5. Performance Profiling

#### **Runtime Performance Monitoring**
```rust
pub struct PerformanceProfiler {
    cpu_profiler: Arc<CpuProfiler>,
    memory_profiler: Arc<MemoryProfiler>, 
    io_profiler: Arc<IoProfiler>,
}

impl PerformanceProfiler {
    pub async fn profile_execution(&self, duration: Duration) -> ProfileReport {
        let cpu_profile = self.cpu_profiler.sample(duration).await;
        let memory_profile = self.memory_profiler.snapshot().await;
        let io_profile = self.io_profiler.measure(duration).await;
        
        ProfileReport {
            hotspots: cpu_profile.identify_bottlenecks(),
            memory_leaks: memory_profile.detect_leaks(),
            io_bottlenecks: io_profile.slow_operations(),
            recommendations: self.generate_optimization_recommendations(),
        }
    }
}
```

#### **Continuous Profiling Integration**
- **Pyroscope**: Continuous CPU profiling for production workloads
- **Memory Profiling**: Heap allocation tracking and leak detection
- **I/O Profiling**: Disk and network operation performance analysis
- **Lock Contention**: Mutex/channel contention detection and resolution

### 6. Dashboards & Visualization

#### **Real-Time Operations Dashboard**
- **Engine Health Overview**: Status, throughput, error rates
- **Message Flow Visualization**: Queue depths, processing rates
- **Performance Heatmaps**: Latency distribution over time
- **Error Analysis**: Error types, frequency, resolution status
- **Resource Utilization**: CPU, memory, network usage

#### **Business Intelligence Dashboard**  
- **Financial Analytics Performance**: Trade processing rates, risk calculation latency
- **Data Quality Metrics**: Record completeness, schema validation success
- **SLA Compliance**: Processing time SLAs, availability metrics
- **Capacity Planning**: Growth trends, scaling recommendations

### 7. Implementation Strategy

#### **Phase 1: Foundation (Week 1)**
- [ ] Implement basic metrics collection (queue depth, processing rate)
- [ ] Add structured logging with trace IDs
- [ ] Create health check endpoints
- [ ] Set up basic alerting for critical failures

#### **Phase 2: Advanced Observability (Week 2)**
- [ ] Implement distributed tracing with OpenTelemetry
- [ ] Add comprehensive business metrics
- [ ] Create performance profiling integration
- [ ] Build real-time operations dashboard

#### **Phase 3: Production Operations (Week 3)**
- [ ] Set up continuous profiling in production
- [ ] Implement automated anomaly detection
- [ ] Create runbook automation for common issues
- [ ] Add capacity planning and forecasting

### 8. Tools & Technologies

#### **Metrics & Monitoring Stack**
- **Prometheus**: Metrics collection and storage
- **Grafana**: Dashboards and visualization
- **AlertManager**: Alert routing and notification

#### **Tracing & Logging Stack**
- **Jaeger**: Distributed tracing storage and UI
- **OpenTelemetry**: Instrumentation and trace collection
- **ELK Stack**: Log aggregation, search, and analysis

#### **Performance & Profiling**
- **Pyroscope**: Continuous profiling for production
- **Tokio Console**: Rust async runtime debugging
- **Perf/FlameGraph**: Low-level CPU profiling

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
- [ ] **Observability**: Complete metrics coverage (engine, processor, business-level)
- [ ] **Monitoring**: Real-time dashboards with <1s latency for critical metrics
- [ ] **Tracing**: End-to-end trace correlation >95% for all requests
- [ ] **Alerting**: Sub-minute alert response for critical system degradation
- [ ] **Profiling**: Continuous production profiling with automated bottleneck detection
- [ ] **Health Checks**: Comprehensive health endpoints with dependency validation
- [ ] **Documentation**: Complete runbooks for all operational scenarios
- [ ] **Migration**: Clear migration path with rollback procedures documented

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