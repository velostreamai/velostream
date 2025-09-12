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

## Topology Explanation & Query Plan Analysis System

### Overview

A production streaming engine must provide comprehensive topology explanation and query plan analysis capabilities for operators to understand data flow, optimize SQL queries, debug issues, and maintain system reliability. This includes both traditional SQL query plan explanation and streaming topology introspection. The message-passing architecture makes this even more critical as data flow becomes async and distributed.

### 1. SQL Query Plan Explanation

#### **Traditional Query Plan Analysis**
FerrisStreams provides comprehensive SQL query plan explanation similar to traditional databases, but adapted for streaming workloads:

```rust
pub struct QueryPlanExplainer {
    query_analyzer: Arc<QueryAnalyzer>,
    logical_planner: Arc<LogicalPlanner>,
    physical_planner: Arc<PhysicalPlanner>,
    cost_model: Arc<StreamingCostModel>,
    statistics_provider: Arc<StatisticsProvider>,
}

impl QueryPlanExplainer {
    pub async fn explain_query_plan(&self, sql: &str, explain_options: ExplainOptions) -> QueryPlanExplanation {
        let parsed_query = self.query_analyzer.parse(sql)?;
        let logical_plan = self.logical_planner.create_logical_plan(&parsed_query)?;
        let physical_plan = self.physical_planner.create_physical_plan(&logical_plan)?;
        let cost_estimates = self.cost_model.estimate_costs(&physical_plan).await?;
        
        QueryPlanExplanation {
            // Traditional query plan components
            logical_plan: self.build_logical_plan_tree(&logical_plan),
            physical_plan: self.build_physical_plan_tree(&physical_plan),
            cost_estimates: cost_estimates,
            cardinality_estimates: self.estimate_cardinalities(&physical_plan).await?,
            
            // Streaming-specific components  
            streaming_topology: self.build_streaming_topology(&physical_plan),
            windowing_analysis: self.analyze_windowing(&physical_plan),
            state_management: self.analyze_state_requirements(&physical_plan),
            parallelism_strategy: self.determine_parallelism(&physical_plan),
            
            // Performance analysis
            bottleneck_analysis: self.identify_bottlenecks(&physical_plan, &cost_estimates),
            optimization_hints: self.suggest_optimizations(&logical_plan, &physical_plan),
        }
    }
}

pub struct LogicalPlanNode {
    pub id: String,
    pub operation: LogicalOperation,
    pub children: Vec<LogicalPlanNode>,
    pub schema: Schema,
    pub predicates: Vec<Predicate>,
    pub estimated_cardinality: Option<u64>,
    pub cost_estimate: Option<f64>,
}

pub struct PhysicalPlanNode {
    pub id: String,
    pub operator: PhysicalOperator,
    pub children: Vec<PhysicalPlanNode>,
    pub input_schema: Schema,
    pub output_schema: Schema,
    pub parallelism: u32,
    pub memory_requirement: usize,
    pub cpu_cost: f64,
    pub io_cost: f64,
    pub streaming_properties: StreamingProperties,
}

pub enum LogicalOperation {
    TableScan { table: String, predicates: Vec<Predicate> },
    StreamScan { stream: String, window_spec: Option<WindowSpec> },
    Filter { condition: Expr },
    Project { expressions: Vec<NamedExpr> },
    Aggregate { group_by: Vec<Expr>, aggregates: Vec<AggregateExpr> },
    Join { join_type: JoinType, condition: Expr, left_keys: Vec<Expr>, right_keys: Vec<Expr> },
    Window { window_spec: WindowSpec, functions: Vec<WindowFunc> },
    Sort { expressions: Vec<SortExpr> },
    Limit { count: u64, offset: Option<u64> },
}

pub enum PhysicalOperator {
    // Source operators
    KafkaStreamScan { topic: String, consumer_config: HashMap<String, String> },
    FileStreamScan { path: String, format: FileFormat },
    
    // Processing operators
    Filter { predicate: PhysicalExpr, selectivity: f64 },
    Project { expressions: Vec<PhysicalNamedExpr> },
    HashAggregate { 
        group_by: Vec<PhysicalExpr>, 
        aggregates: Vec<PhysicalAggregateExpr>,
        estimated_groups: u64,
    },
    SortMergeJoin { 
        join_type: JoinType, 
        left_keys: Vec<PhysicalExpr>, 
        right_keys: Vec<PhysicalExpr>,
        estimated_join_selectivity: f64,
    },
    HashJoin {
        join_type: JoinType,
        build_side: BuildSide,
        probe_keys: Vec<PhysicalExpr>,
        build_keys: Vec<PhysicalExpr>,
        estimated_build_size: usize,
    },
    WindowAggregate {
        window_spec: PhysicalWindowSpec,
        functions: Vec<PhysicalWindowFunc>,
        state_size_estimate: usize,
    },
    
    // Sink operators
    KafkaStreamSink { topic: String, producer_config: HashMap<String, String> },
    FileStreamSink { path: String, format: FileFormat },
    ConsoleSink,
}

pub struct StreamingProperties {
    pub requires_state: bool,
    pub state_size_estimate: Option<usize>,
    pub watermark_strategy: Option<WatermarkStrategy>,
    pub key_distribution: KeyDistribution,
    pub ordering_properties: OrderingProperties,
    pub partitioning_scheme: PartitioningScheme,
}
```

#### **Streaming Topology Analysis**
```rust
pub struct StreamingTopologyAnalyzer {
    topology_builder: Arc<TopologyBuilder>,
    metrics_collector: Arc<MetricsCollector>,
}

impl StreamingTopologyAnalyzer {
    pub async fn explain_streaming_topology(&self, physical_plan: &PhysicalPlan) -> StreamingTopologyExplanation {
        let topology = self.topology_builder.build_topology(physical_plan)?;
        let runtime_metrics = self.metrics_collector.get_topology_metrics(&topology).await?;
        
        StreamingTopologyExplanation {
            // Data flow topology
            data_flow_graph: self.create_data_flow_graph(&topology),
            processor_nodes: self.extract_processor_nodes(&topology),
            channel_connections: self.extract_channel_connections(&topology),
            
            // Runtime characteristics
            current_metrics: runtime_metrics,
            performance_characteristics: self.analyze_performance(&topology, &runtime_metrics),
            bottleneck_analysis: self.identify_topology_bottlenecks(&topology, &runtime_metrics),
            
            // Streaming-specific analysis
            backpressure_analysis: self.analyze_backpressure(&topology, &runtime_metrics),
            state_distribution: self.analyze_state_distribution(&topology),
            parallelism_utilization: self.analyze_parallelism_utilization(&topology, &runtime_metrics),
            
            // Optimization opportunities
            scaling_recommendations: self.suggest_scaling(&topology, &runtime_metrics),
            topology_optimizations: self.suggest_topology_optimizations(&topology),
        }
    }
}
```

#### **Execution Plan Visualization**
```rust
pub struct ExecutionPlan {
    pub operators: Vec<OperatorNode>,
    pub data_dependencies: Vec<DataDependency>,
    pub parallelism_strategy: ParallelismStrategy,
    pub resource_requirements: ResourceRequirements,
}

pub struct OperatorNode {
    pub id: String,
    pub operator_type: OperatorType,  // Source, Transform, Aggregate, Sink
    pub sql_fragment: String,         // Original SQL that created this operator
    pub input_schema: Schema,
    pub output_schema: Schema,
    pub estimated_selectivity: f64,   // % of records that pass through
    pub parallelism: u32,
    pub memory_requirement: usize,
    pub cpu_requirement: f64,
}

pub enum OperatorType {
    Source { connector_type: String, properties: HashMap<String, String> },
    Filter { condition: String, selectivity: f64 },
    Project { fields: Vec<String> },
    Aggregate { group_by: Vec<String>, functions: Vec<String> },
    Join { join_type: JoinType, condition: String },
    Window { window_spec: WindowSpec, functions: Vec<String> },
    Sink { connector_type: String, properties: HashMap<String, String> },
}
```

### 2. Data Flow Topology

#### **Stream Processing Pipeline Visualization**
```rust
pub struct DataFlowTopology {
    pub sources: Vec<DataSource>,
    pub processors: Vec<ProcessorNode>,
    pub sinks: Vec<DataSink>,
    pub channels: Vec<ChannelConnection>,
    pub backpressure_graph: BackpressureGraph,
}

pub struct ProcessorNode {
    pub id: String,
    pub processor_type: String,
    pub input_channels: Vec<ChannelId>,
    pub output_channels: Vec<ChannelId>,
    pub current_queue_depth: usize,
    pub processing_rate: f64,        // records/sec
    pub error_rate: f64,             // errors/sec  
    pub resource_usage: ResourceUsage,
    pub health_status: HealthStatus,
}

pub struct ChannelConnection {
    pub id: ChannelId,
    pub from_processor: String,
    pub to_processor: String,
    pub channel_type: ChannelType,   // Bounded, Unbounded
    pub capacity: Option<usize>,
    pub current_depth: usize,
    pub throughput: f64,             // messages/sec
    pub backpressure_events: u64,
}
```

#### **Interactive Topology Browser**
```rust
pub struct TopologyBrowser {
    topology: Arc<RwLock<DataFlowTopology>>,
    metrics_store: Arc<MetricsStore>,
}

impl TopologyBrowser {
    // Get real-time topology with live metrics
    pub async fn get_live_topology(&self) -> LiveTopology {
        let topology = self.topology.read().await;
        let live_metrics = self.metrics_store.get_current_metrics().await;
        
        LiveTopology {
            static_topology: topology.clone(),
            live_metrics,
            performance_summary: self.summarize_performance(&live_metrics),
            health_summary: self.summarize_health(&topology, &live_metrics),
            bottlenecks: self.identify_current_bottlenecks(&topology, &live_metrics),
        }
    }
    
    // Trace data lineage for specific record
    pub async fn trace_record_lineage(&self, record_id: &str) -> RecordLineage {
        let trace_events = self.metrics_store.get_trace_events(record_id).await;
        
        RecordLineage {
            record_id: record_id.to_string(),
            source_info: self.extract_source_info(&trace_events),
            processing_path: self.build_processing_path(&trace_events),
            transformations: self.extract_transformations(&trace_events),
            sink_destinations: self.extract_sink_info(&trace_events),
            total_processing_time: self.calculate_total_time(&trace_events),
            bottlenecks_encountered: self.identify_record_bottlenecks(&trace_events),
        }
    }
}
```

### 3. Performance Topology Analysis

#### **Bottleneck Detection & Analysis**
```rust
pub struct TopologyPerformanceAnalyzer {
    topology: Arc<DataFlowTopology>,
    metrics_history: Arc<MetricsHistory>,
}

impl TopologyPerformanceAnalyzer {
    pub async fn analyze_performance_topology(&self) -> PerformanceTopology {
        let current_metrics = self.metrics_history.get_latest().await;
        let historical_trends = self.metrics_history.get_trends(Duration::from_hours(24)).await;
        
        PerformanceTopology {
            throughput_analysis: self.analyze_throughput_by_operator(&current_metrics),
            latency_analysis: self.analyze_latency_by_path(&current_metrics),
            resource_utilization: self.analyze_resource_usage(&current_metrics),
            bottleneck_ranking: self.rank_bottlenecks(&current_metrics, &historical_trends),
            scaling_recommendations: self.generate_scaling_recommendations(&historical_trends),
            optimization_opportunities: self.identify_optimization_opportunities(),
        }
    }
    
    pub fn explain_bottleneck(&self, bottleneck: &Bottleneck) -> BottleneckExplanation {
        BottleneckExplanation {
            description: self.describe_bottleneck(bottleneck),
            root_cause_analysis: self.analyze_root_cause(bottleneck),
            impact_analysis: self.analyze_impact(bottleneck),
            resolution_steps: self.suggest_resolution_steps(bottleneck),
            estimated_improvement: self.estimate_improvement(bottleneck),
            risk_assessment: self.assess_resolution_risk(bottleneck),
        }
    }
}
```

### 4. Command-Line Interface (EXPLAIN Commands)

#### **SQL EXPLAIN Command Extensions**

**Traditional Query Plan Commands:**
```sql
-- Basic logical and physical query plan
EXPLAIN 
SELECT symbol, AVG(price) as avg_price 
FROM trades 
WHERE price > 100
WINDOW TUMBLING(5m) 
GROUP BY symbol;

-- Detailed execution plan with cost estimates and cardinality
EXPLAIN (ANALYZE true, COSTS true, BUFFERS true, TIMING true)
SELECT t1.symbol, t1.price, t2.volume
FROM trades t1 
JOIN volumes t2 ON t1.symbol = t2.symbol
WHERE t1.price > 100;

-- JSON format for programmatic analysis  
EXPLAIN (FORMAT JSON, ANALYZE true)
SELECT symbol, COUNT(*) as trade_count
FROM trades
GROUP BY symbol;

-- Verbose plan with detailed operator information
EXPLAIN (VERBOSE true, COSTS true)
SELECT symbol, 
       AVG(price) as avg_price,
       MAX(price) as max_price,
       MIN(price) as min_price,
       STDDEV(price) as price_volatility
FROM trades
WINDOW SLIDING(1h, 5m)
GROUP BY symbol;
```

**Streaming Topology Commands:**
```sql
-- Basic streaming topology explanation
EXPLAIN TOPOLOGY 
SELECT symbol, AVG(price) as avg_price 
FROM trades 
WINDOW TUMBLING(5m) 
GROUP BY symbol;

-- Live topology with current runtime metrics
EXPLAIN TOPOLOGY (LIVE true, METRICS true)
SELECT symbol, COUNT(*) as trade_count
FROM trades
GROUP BY symbol
EMIT CHANGES;

-- Detailed topology with performance analysis and bottlenecks
EXPLAIN TOPOLOGY (ANALYZE true, PERFORMANCE true, BOTTLENECKS true)
SELECT symbol, 
       AVG(price) as avg_price,
       MAX(price) as max_price,
       COUNT(*) as trade_count
FROM trades
WINDOW SLIDING(1h, 5m)
GROUP BY symbol;

-- Combined query plan + topology explanation
EXPLAIN (PLAN true, TOPOLOGY true, ANALYZE true)
SELECT t1.symbol, t1.price, t2.volume
FROM trades t1 
JOIN volumes t2 ON t1.symbol = t2.symbol
WHERE t1.price > 100;
```

**Example Output - Traditional Query Plan:**
```
QUERY PLAN
-----------
StreamingAggregate  (cost=1000.00..2000.00 rows=100 width=32) (actual time=0.123..0.145 rows=95 loops=1)
  Group Key: symbol
  Window: TUMBLING(5 minutes)
  Aggregate Functions: AVG(price)
  State Size Estimate: 1024 bytes per group
  ->  StreamingScan on trades  (cost=0.00..1000.00 rows=5000 width=16) (actual time=0.001..0.102 rows=4876 loops=1)
        Filter: (price > 100::numeric)
        Rows Removed by Filter: 124
        Kafka Topic: financial_trades
        Partition Assignment: 0,1,2,3
        Consumer Group: query_executor_001
        Watermark Strategy: Bounded(10s)

Planning Time: 2.34 ms  
Execution Time: 145.67 ms
Peak Memory Usage: 2.1 MB
```

**Example Output - Streaming Topology:**
```
STREAMING TOPOLOGY
-----------------
┌─[KafkaSource: trades]─────────────────────┐
│ Topic: financial_trades                   │
│ Partitions: 4 (0,1,2,3)                 │ 
│ Current Rate: 1,250 records/sec          │
│ Lag: 45ms                                │
└─────────────┬─────────────────────────────┘
              │ Channel: bounded(1000)
              │ Depth: 234/1000 (23%)
              │ Throughput: 1,250 msgs/sec
              v
┌─[FilterProcessor]─────────────────────────┐
│ Condition: price > 100                   │
│ Selectivity: 97.5%                       │
│ Processing Rate: 1,219 records/sec       │
│ CPU Usage: 15%                           │
└─────────────┬─────────────────────────────┘
              │ Channel: bounded(1000) 
              │ Depth: 12/1000 (1%)
              │ Throughput: 1,219 msgs/sec
              v
┌─[WindowAggregateProcessor]────────────────┐
│ Window: TUMBLING(5m)                     │
│ Group By: symbol                         │
│ Functions: AVG(price)                    │
│ Active Groups: 95                        │
│ State Size: 97KB                         │
│ Processing Rate: 1,219 records/sec       │
│ Output Rate: 95 records/5min             │
└─────────────┬─────────────────────────────┘
              │ Channel: unbounded
              │ Depth: 0 (no backpressure)
              │ Throughput: 0.32 msgs/sec
              v
┌─[ConsoleSink]─────────────────────────────┐
│ Format: JSON                             │
│ Output Rate: 0.32 records/sec            │
└───────────────────────────────────────────┘

Performance Analysis:
- Bottleneck: None detected
- Memory Usage: 2.1MB (within limits)
- Backpressure: None detected
- Scaling Recommendation: Current parallelism sufficient
```

#### **CLI Topology Commands**
```bash
# Show current topology overview
ferris-cli topology show

# Explain specific query topology  
ferris-cli topology explain --query "SELECT ..."

# Show live topology with real-time metrics
ferris-cli topology live --refresh 1s

# Analyze topology performance and bottlenecks
ferris-cli topology analyze --timerange 1h

# Trace specific record through topology
ferris-cli topology trace --record-id "trade_12345"

# Show topology health and degradation
ferris-cli topology health --detailed

# Export topology for external analysis
ferris-cli topology export --format graphviz --output topology.dot
ferris-cli topology export --format json --output topology.json
```

### 5. Visual Topology Representations

#### **Web-Based Topology Visualizer**
```rust
pub struct TopologyVisualizer {
    topology_service: Arc<TopologyService>,
    metrics_service: Arc<MetricsService>,
}

impl TopologyVisualizer {
    // Generate interactive topology visualization
    pub async fn generate_interactive_topology(&self) -> InteractiveTopology {
        let topology = self.topology_service.get_current_topology().await;
        let live_metrics = self.metrics_service.get_live_metrics().await;
        
        InteractiveTopology {
            nodes: self.create_visual_nodes(&topology, &live_metrics),
            edges: self.create_visual_edges(&topology, &live_metrics),
            layouts: self.generate_layout_options(&topology),
            interactions: self.define_interactions(),
            real_time_updates: self.setup_live_updates(),
        }
    }
}

pub struct VisualNode {
    pub id: String,
    pub label: String,
    pub node_type: NodeType,
    pub position: Position,
    pub size: Size,
    pub color: Color,           // Based on health status
    pub metrics_overlay: MetricsOverlay,
    pub drill_down_available: bool,
}

pub struct VisualEdge {
    pub from: String,
    pub to: String,
    pub label: String,
    pub thickness: f32,         // Based on throughput
    pub color: Color,           // Based on backpressure/health
    pub animation: EdgeAnimation, // Data flow animation
    pub metrics: EdgeMetrics,
}
```

#### **Export Formats**
- **GraphViz DOT**: For generating static topology diagrams
- **JSON**: For external analysis tools and custom visualizations  
- **SVG**: For documentation and presentations
- **Prometheus Metrics**: For integration with monitoring systems
- **OpenAPI Spec**: For topology REST API documentation

### 6. Topology Documentation Generation

#### **Automated Documentation**
```rust
pub struct TopologyDocumentationGenerator {
    topology_analyzer: Arc<TopologyAnalyzer>,
    template_engine: Arc<TemplateEngine>,
}

impl TopologyDocumentationGenerator {
    pub async fn generate_topology_documentation(&self, 
                                                  format: DocumentationFormat) -> TopologyDocumentation {
        let topology = self.topology_analyzer.analyze_current_topology().await;
        
        match format {
            DocumentationFormat::Markdown => self.generate_markdown_docs(&topology),
            DocumentationFormat::Html => self.generate_html_docs(&topology),
            DocumentationFormat::Confluence => self.generate_confluence_docs(&topology),
            DocumentationFormat::OpenApi => self.generate_api_docs(&topology),
        }
    }
}

pub struct TopologyDocumentation {
    pub overview: String,
    pub data_sources: Vec<DataSourceDocumentation>,
    pub processing_stages: Vec<ProcessingStageDocumentation>,
    pub data_sinks: Vec<DataSinkDocumentation>,
    pub performance_characteristics: PerformanceDocumentation,
    pub operational_runbooks: Vec<RunbookEntry>,
    pub troubleshooting_guide: TroubleshootingGuide,
}
```

### 7. Integration with Existing Systems

#### **Observability Integration**
- **Grafana Integration**: Topology dashboards with live metrics
- **Jaeger Integration**: Distributed tracing topology correlation
- **Prometheus Integration**: Topology-aware alerting rules
- **ELK Integration**: Topology context in log analysis

#### **Development Integration**
- **IDE Extensions**: Topology visualization in development environments
- **CI/CD Integration**: Topology validation in deployment pipelines  
- **Testing Integration**: Topology-aware integration testing
- **Documentation Integration**: Auto-generated topology documentation

### 8. Implementation Phases

#### **Phase 1: Basic Topology Explanation (Week 1)**
- [ ] Implement basic query plan explanation
- [ ] Create simple topology visualization
- [ ] Add CLI topology commands
- [ ] Basic performance bottleneck detection

#### **Phase 2: Advanced Analysis (Week 2)**  
- [ ] Implement live topology browser with real-time metrics
- [ ] Add record lineage tracing
- [ ] Create interactive web-based visualizer
- [ ] Implement bottleneck analysis and optimization suggestions

#### **Phase 3: Production Integration (Week 3)**
- [ ] Integrate with observability stack (Grafana/Jaeger/Prometheus)
- [ ] Implement automated documentation generation
- [ ] Add topology export capabilities
- [ ] Create operational runbooks and troubleshooting guides

#### **Phase 4: Advanced Features (Week 4)**
- [ ] Implement predictive topology analysis
- [ ] Add topology change detection and alerting
- [ ] Create topology testing and validation tools
- [ ] Build topology-aware capacity planning

### 9. Use Cases & Benefits

#### **Development & Debugging**
- **Query Optimization**: Understand execution plans and identify inefficiencies
- **Performance Tuning**: Visual identification of bottlenecks and resource constraints
- **Data Lineage**: Track data transformations and dependencies
- **Impact Analysis**: Understand downstream effects of changes

#### **Operations & Monitoring**
- **Real-time Health**: Visual topology health with live metrics
- **Troubleshooting**: Rapid identification of failing components
- **Capacity Planning**: Resource usage analysis and scaling recommendations
- **Change Management**: Impact assessment for topology modifications

#### **Business Understanding**
- **Data Flow Documentation**: Clear business process to technical implementation mapping
- **Compliance**: Data lineage for regulatory requirements
- **Optimization**: Business impact of performance improvements
- **Communication**: Visual topology for stakeholder discussions

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