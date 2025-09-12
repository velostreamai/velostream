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

### 3. Multi-Query Topology Analysis

#### **Cross-Query Dependency Analysis**
In production streaming systems, multiple related SQL queries often share data sources, intermediate results, and processing resources. Understanding these relationships is critical for optimization and operational management.

```rust
pub struct MultiQueryTopologyAnalyzer {
    query_registry: Arc<QueryRegistry>,
    dependency_analyzer: Arc<DependencyAnalyzer>,  
    resource_analyzer: Arc<ResourceAnalyzer>,
    sharing_optimizer: Arc<SharingOptimizer>,
}

impl MultiQueryTopologyAnalyzer {
    pub async fn analyze_multi_query_topology(&self, query_group: &QueryGroup) -> MultiQueryTopology {
        let individual_topologies = self.build_individual_topologies(query_group).await?;
        let shared_resources = self.identify_shared_resources(&individual_topologies)?;
        let data_dependencies = self.analyze_data_dependencies(query_group).await?;
        let resource_contention = self.analyze_resource_contention(&individual_topologies).await?;
        
        MultiQueryTopology {
            query_topologies: individual_topologies,
            shared_topology_graph: self.build_shared_topology_graph(&shared_resources),
            data_flow_dependencies: data_dependencies,
            resource_sharing_analysis: shared_resources,
            cross_query_optimization_opportunities: self.identify_sharing_opportunities(query_group),
            resource_contention_analysis: resource_contention,
            consolidated_performance_metrics: self.aggregate_performance_metrics(query_group).await?,
        }
    }
}

pub struct QueryGroup {
    pub group_id: String,
    pub group_metadata: QueryGroupMetadata,
    pub queries: Vec<RegisteredQuery>,
    pub shared_sources: Vec<SharedDataSource>,
    pub shared_sinks: Vec<SharedDataSink>,
    pub deployment_context: DeploymentContext,
}

pub struct QueryGroupMetadata {
    pub job_name: String,
    pub version: SemanticVersion,
    pub description: Option<String>,
    pub owner: String,
    pub team: String,
    pub environment: Environment,  // dev, staging, prod
    pub created_at: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
    pub configuration_history: Vec<ConfigurationChange>,
    pub tags: HashMap<String, String>,
    pub service_level_objectives: Vec<ServiceLevelObjective>,
}

pub struct RegisteredQuery {
    pub query_id: String,
    pub query_metadata: QueryMetadata,
    pub sql: String,
    pub logical_plan: LogicalPlan,
    pub physical_plan: PhysicalPlan,
    pub runtime_topology: StreamingTopology,
    pub resource_requirements: ResourceRequirements,
    pub current_metrics: QueryMetrics,
}

pub struct QueryMetadata {
    pub name: String,
    pub version: SemanticVersion,
    pub description: Option<String>,
    pub owner: String,
    pub created_at: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
    pub git_commit_hash: Option<String>,
    pub build_id: Option<String>,
    pub deployment_id: String,
    pub configuration_checksum: String,
    pub schema_version: String,
    pub feature_flags: HashMap<String, bool>,
    pub compliance_requirements: Vec<ComplianceRequirement>,
    pub business_context: BusinessContext,
}

pub struct ConfigurationChange {
    pub change_id: String,
    pub timestamp: DateTime<Utc>,
    pub change_type: ChangeType,
    pub author: String,
    pub description: String,
    pub configuration_diff: ConfigurationDiff,
    pub rollback_info: Option<RollbackInfo>,
    pub approval_metadata: Option<ApprovalMetadata>,
}

pub enum ChangeType {
    Initial,
    SqlUpdate,
    ConfigurationChange,
    SchemaEvolution,
    ResourceScaling,
    FeatureFlagToggle,
    EnvironmentPromotion,
    Rollback,
}

pub struct BusinessContext {
    pub business_domain: String,          // "financial_analytics", "risk_management"  
    pub data_classification: DataClassification, // Public, Internal, Confidential, Restricted
    pub retention_policy: RetentionPolicy,
    pub sla_requirements: SlaRequirements,
    pub cost_center: String,
    pub regulatory_requirements: Vec<String>,
}

pub struct SharedDataSource {
    pub source_id: String,
    pub source_type: DataSourceType,  // Kafka, File, etc.
    pub consuming_queries: Vec<String>,
    pub partitioning_strategy: PartitioningStrategy,
    pub current_load: f64,
    pub sharing_efficiency: f64,
}
```

#### **Data Lineage Across Multiple Queries**
```rust
pub struct CrossQueryDataLineage {
    lineage_analyzer: Arc<LineageAnalyzer>,
    impact_analyzer: Arc<ImpactAnalyzer>,
}

impl CrossQueryDataLineage {
    pub async fn trace_cross_query_lineage(&self, 
                                           source_record_id: &str) -> CrossQueryLineageTrace {
        let primary_trace = self.lineage_analyzer.trace_record(source_record_id).await?;
        let derived_traces = self.find_derived_records(&primary_trace).await?;
        let downstream_impact = self.analyze_downstream_impact(&derived_traces).await?;
        
        CrossQueryLineageTrace {
            source_record: primary_trace.source_record,
            primary_processing_path: primary_trace.processing_path,
            cross_query_derivations: derived_traces,
            downstream_queries_affected: downstream_impact.affected_queries,
            total_processing_latency: self.calculate_total_latency(&primary_trace, &derived_traces),
            data_quality_propagation: self.analyze_quality_propagation(&primary_trace, &derived_traces),
            compliance_chain: self.build_compliance_chain(&primary_trace, &derived_traces),
        }
    }
    
    pub async fn analyze_query_impact(&self, query_id: &str, 
                                      change_type: QueryChangeType) -> QueryImpactAnalysis {
        let affected_queries = self.find_downstream_queries(query_id).await?;
        let shared_resources = self.find_shared_resources(query_id).await?;
        let performance_impact = self.estimate_performance_impact(query_id, change_type).await?;
        
        QueryImpactAnalysis {
            target_query: query_id.to_string(),
            change_type,
            directly_affected_queries: affected_queries.direct,
            transitively_affected_queries: affected_queries.transitive,
            shared_resource_impact: shared_resources,
            estimated_performance_impact: performance_impact,
            risk_assessment: self.assess_change_risk(query_id, change_type, &affected_queries),
            rollback_plan: self.generate_rollback_plan(query_id, &affected_queries),
        }
    }
}

pub struct CrossQueryLineageTrace {
    pub source_record: RecordInfo,
    pub primary_processing_path: ProcessingPath,
    pub cross_query_derivations: Vec<DerivedRecord>,
    pub downstream_queries_affected: Vec<String>,
    pub total_processing_latency: Duration,
    pub data_quality_propagation: QualityPropagation,
    pub compliance_chain: ComplianceChain,
}

pub struct DerivedRecord {
    pub derived_record_id: String,
    pub source_query: String,
    pub target_query: String,
    pub transformation_type: TransformationType,
    pub processing_latency: Duration,
    pub data_quality_score: f64,
}
```

#### **Resource Sharing Analysis**
```rust
pub struct ResourceSharingAnalyzer {
    resource_monitor: Arc<ResourceMonitor>,
    contention_detector: Arc<ContentionDetector>,
}

impl ResourceSharingAnalyzer {
    pub async fn analyze_resource_sharing(&self, queries: &[RegisteredQuery]) -> ResourceSharingAnalysis {
        let shared_sources = self.identify_shared_data_sources(queries)?;
        let shared_processors = self.identify_shared_processors(queries)?;
        let shared_sinks = self.identify_shared_data_sinks(queries)?;
        let resource_contention = self.detect_resource_contention(queries).await?;
        
        ResourceSharingAnalysis {
            shared_data_sources: shared_sources,
            shared_processors: shared_processors,
            shared_data_sinks: shared_sinks,
            resource_contention_hotspots: resource_contention,
            sharing_efficiency_metrics: self.calculate_sharing_efficiency(queries),
            optimization_recommendations: self.recommend_sharing_optimizations(queries),
            cost_benefit_analysis: self.analyze_sharing_cost_benefits(queries),
        }
    }
    
    pub fn identify_sharing_opportunities(&self, queries: &[RegisteredQuery]) -> Vec<SharingOpportunity> {
        let mut opportunities = Vec::new();
        
        // Identify common subexpressions across queries
        let common_filters = self.find_common_filters(queries);
        let common_aggregations = self.find_common_aggregations(queries);
        let common_joins = self.find_common_joins(queries);
        
        // Identify source fanout opportunities
        let source_fanout = self.find_source_fanout_opportunities(queries);
        
        // Identify materialized view opportunities
        let materialization = self.find_materialization_opportunities(queries);
        
        opportunities.extend(common_filters.into_iter().map(SharingOpportunity::CommonFilter));
        opportunities.extend(common_aggregations.into_iter().map(SharingOpportunity::CommonAggregation));
        opportunities.extend(common_joins.into_iter().map(SharingOpportunity::CommonJoin));
        opportunities.extend(source_fanout.into_iter().map(SharingOpportunity::SourceFanout));
        opportunities.extend(materialization.into_iter().map(SharingOpportunity::Materialization));
        
        opportunities
    }
}

pub enum SharingOpportunity {
    CommonFilter { 
        filter_expression: String, 
        queries: Vec<String>, 
        estimated_savings: ResourceSavings 
    },
    CommonAggregation { 
        aggregation_spec: AggregationSpec, 
        queries: Vec<String>, 
        estimated_savings: ResourceSavings 
    },
    CommonJoin { 
        join_spec: JoinSpec, 
        queries: Vec<String>, 
        estimated_savings: ResourceSavings 
    },
    SourceFanout { 
        source: DataSource, 
        queries: Vec<String>, 
        current_efficiency: f64, 
        potential_efficiency: f64 
    },
    Materialization { 
        intermediate_result: MaterializationSpec, 
        consumer_queries: Vec<String>, 
        estimated_performance_gain: f64 
    },
}
```

### 4. Enhanced SQL EXPLAIN for Multi-Query Analysis

#### **Multi-Query EXPLAIN Commands**
```sql
-- Analyze relationships between multiple queries
EXPLAIN MULTI_QUERY 
WITH QUERIES (
  'query1' AS (SELECT symbol, AVG(price) FROM trades WINDOW TUMBLING(5m) GROUP BY symbol),
  'query2' AS (SELECT symbol, MAX(price) FROM trades WINDOW TUMBLING(5m) GROUP BY symbol),
  'query3' AS (SELECT symbol, COUNT(*) FROM trades WHERE price > 100 GROUP BY symbol)
);

-- Show shared resource analysis
EXPLAIN SHARED_RESOURCES
FOR QUERIES ('portfolio_risk_query', 'trading_analytics_query', 'compliance_report_query');

-- Analyze data lineage across queries  
EXPLAIN LINEAGE
FROM SOURCE 'kafka://trades/trade_12345'
THROUGH QUERIES ('risk_calculation', 'portfolio_update', 'audit_log');

-- Impact analysis for query changes
EXPLAIN IMPACT 
FOR QUERY 'portfolio_risk_query'
CHANGE TYPE 'schema_evolution';

-- Cross-query optimization opportunities
EXPLAIN OPTIMIZATION_OPPORTUNITIES
FOR QUERY_GROUP 'financial_analytics_suite';
```

#### **Example Multi-Query Analysis Output**
```
MULTI-QUERY TOPOLOGY ANALYSIS
=============================

┌─ JOB METADATA ─────────────────────────────────────────────────────┐
│ Job Name: financial_analytics_suite                                 │
│ Version: 2.1.3                                                      │
│ Owner: trading-platform-team                                        │
│ Environment: production                                              │
│ Deployment ID: deploy-20240115-143022-7f8a9b2                      │
│ Git Commit: 7f8a9b2c (feat: add real-time risk calculations)       │
│ Build ID: jenkins-2024-0115-build-4721                             │
│ Created: 2024-01-10 09:15:32 UTC                                   │
│ Modified: 2024-01-15 14:30:22 UTC                                  │
│ Configuration Checksum: sha256:a7f8c9d2e1b4...                      │
│                                                                     │
│ Business Context:                                                   │
│   Domain: financial_analytics                                       │
│   Classification: Confidential                                     │
│   Cost Center: TRADING-TECH-001                                    │
│   SLA: 99.9% uptime, <100ms P95 latency                           │
│   Compliance: SOX, MiFID II, GDPR                                  │
│                                                                     │
│ Feature Flags:                                                      │
│   ✓ enhanced_risk_calculation: enabled                             │
│   ✓ real_time_alerts: enabled                                      │
│   ✗ experimental_ml_predictions: disabled                          │
└─────────────────────────────────────────────────────────────────────┘

Query Group: financial_analytics_suite (3 queries)
───────────────────────────────────────────────────

┌─ QUERY REGISTRY ───────────────────────────────────────────────────┐
│                                                                     │
│ Query 1: portfolio_risk_calculator                                  │
│   Version: 2.1.2                                                   │
│   Owner: risk-management-team                                       │
│   Schema Version: trades_v3.1                                      │
│   Last Modified: 2024-01-15 14:25:18 UTC                          │
│   Git Commit: 6e7d8a1b (fix: improve risk calculation precision)   │
│                                                                     │
│ Query 2: real_time_price_alerts                                     │
│   Version: 2.0.1                                                   │
│   Owner: trading-platform-team                                     │
│   Schema Version: trades_v3.1                                      │
│   Last Modified: 2024-01-14 11:42:07 UTC                          │
│   Git Commit: 9c4f2a8d (feat: add volatility thresholds)          │
│                                                                     │
│ Query 3: compliance_audit_trail                                     │
│   Version: 1.8.3                                                   │
│   Owner: compliance-team                                            │
│   Schema Version: trades_v3.0 (migration pending)                  │
│   Last Modified: 2024-01-12 16:20:45 UTC                          │
│   Git Commit: 2b5e9f7c (fix: ensure GDPR compliance)              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─ CONFIGURATION CHANGE HISTORY ─────────────────────────────────────┐
│                                                                     │
│ Recent Changes (Last 7 days):                                      │
│                                                                     │
│ 2024-01-15 14:30:22 UTC - ResourceScaling                         │
│   Author: ops-team                                                 │
│   Change: Increased Kafka consumer parallelism 8 → 12             │
│   Approval: auto-approved (performance optimization)               │
│   Rollback: available (change-id: cfg-20240115-1430)              │
│                                                                     │
│ 2024-01-14 09:15:10 UTC - FeatureFlagToggle                       │
│   Author: risk-management-team                                     │
│   Change: Enabled enhanced_risk_calculation                        │
│   Approval: manual (risk-manager, platform-lead)                  │
│   Rollback: available (change-id: cfg-20240114-0915)              │
│                                                                     │
│ 2024-01-13 16:45:33 UTC - SqlUpdate                              │
│   Author: trading-platform-team                                   │
│   Change: Added volatility calculation to portfolio_risk_calculator│
│   Approval: manual (code-review, qa-testing)                      │
│   Rollback: available (change-id: cfg-20240113-1645)              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─[SharedKafkaSource: trades]──────────────────────┐
│ Topic: financial_trades                          │
│ Consumers: 3 (query1, query2, query3)          │
│ Total Rate: 3,750 records/sec                   │
│ Sharing Efficiency: 85% (good)                  │
│ Partitioning: Round-robin across consumers       │
└─┬────────────────────────┬───────────────────────┘
  │                        │
  │ ┌─[query1]─────────────│──[query3]──────────────┐
  │ │ Window: TUMBLING(5m) │  Filter: price > 100  │
  │ │ Function: AVG(price) │  Function: COUNT(*)    │
  │ │ Rate: 1,250 rec/sec  │  Rate: 1,250 rec/sec   │
  │ └──────────────────────┼─────────────────────────┘
  │                        │
  v                        v
┌─[query2]─────────────────────────────────────────┐
│ Window: TUMBLING(5m)                             │
│ Function: MAX(price)                             │ 
│ Rate: 1,250 rec/sec                             │
│ Shared Window State: 75KB (with query1)         │
└──────────────────────────────────────────────────┘

SHARED RESOURCE ANALYSIS:
━━━━━━━━━━━━━━━━━━━━━━━━━━
• Data Sources:
  └── trades (Kafka): Used by 3 queries, efficiency 85%

• Processing Resources:
  └── TUMBLING(5m) window: Shared by query1 & query2 
      ├── Current: 2 separate processors (150KB state)
      └── Optimized: 1 shared processor (75KB state) → 50% memory savings

• Optimization Opportunities:
  1. Merge TUMBLING(5m) windows for query1 & query2
     └── Savings: 50% memory, 30% CPU, identical results
  2. Pre-filter common condition (price > 0) 
     └── Savings: 15% processing overhead
  3. Materialize 5-minute symbol aggregates
     └── Benefits: query3 latency -60%, query1/query2 consistency

CROSS-QUERY DATA LINEAGE:
━━━━━━━━━━━━━━━━━━━━━━━━━━
trade_12345 (2024-01-15 10:30:00)
├── query1: AVG calculation (10:30:05) → portfolio_update
├── query2: MAX calculation (10:30:05) → risk_threshold_check  
└── query3: COUNT increment (10:30:01) → compliance_audit

Impact Radius: 3 queries, 6 downstream systems
Processing Latency: 1-5 seconds end-to-end
Compliance Chain: Complete (all regulations tracked)

PERFORMANCE METRICS (Last 1 Hour):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                  Query1    Query2    Query3    Total
Records/sec:      1,250     1,250     1,219     3,719
Memory Usage:     85KB      87KB      45KB      217KB
CPU Usage:        12%       13%       8%        33%
Error Rate:       0.01%     0.01%     0.02%     0.013%
Backpressure:     None      None      None      None

RECOMMENDATIONS:
━━━━━━━━━━━━━━━━
Priority 1: Implement shared TUMBLING(5m) processor
Priority 2: Add materialized view for symbol aggregates  
Priority 3: Optimize Kafka consumer group assignments
Priority 4: Consider query consolidation for query1/query2

Estimated Benefits: 
├── Memory: -50% (75KB savings)
├── CPU: -30% (10% absolute reduction)
├── Latency: -40% average across all queries
└── Operational Complexity: -25%
```

#### **CLI Multi-Query Commands with Metadata Support**
```bash
# Analyze query group topology with full metadata
ferris-cli multi-query analyze --group financial_analytics --include-metadata

# Show query registry with version and ownership information
ferris-cli multi-query registry --group financial_analytics --show-history

# Show shared resources across queries with metadata context
ferris-cli multi-query shared-resources --queries query1,query2,query3 --include-owners

# Cross-query lineage tracing with compliance tracking
ferris-cli multi-query lineage --record-id trade_12345 --trace-depth 3 --include-compliance-chain

# Impact analysis for query changes with approval workflows
ferris-cli multi-query impact --query portfolio_risk --change schema_update --include-approvals

# Show configuration change history
ferris-cli multi-query history --group financial_analytics --timerange 7d --show-approvals

# Version comparison between environments
ferris-cli multi-query diff --group financial_analytics --env1 staging --env2 production

# Find optimization opportunities with cost analysis and ownership
ferris-cli multi-query optimize --group trading_suite --include-cost-analysis --include-owners

# Real-time multi-query dashboard with metadata overlay
ferris-cli multi-query dashboard --refresh 2s --group financial_analytics --show-versions

# Export multi-query topology with full metadata context
ferris-cli multi-query export --format graphviz --include-lineage --include-metadata --output multi_query_topology.dot

# Compliance and audit reporting
ferris-cli multi-query compliance-report --group financial_analytics --regulations SOX,GDPR --format json

# Show feature flag status across query group
ferris-cli multi-query feature-flags --group financial_analytics --environment production

# Configuration validation and drift detection
ferris-cli multi-query validate-config --group financial_analytics --baseline production
```

#### **Enhanced EXPLAIN Commands with Metadata**
```sql
-- Show query metadata alongside topology
EXPLAIN TOPOLOGY (METADATA true, HISTORY true)
SELECT symbol, AVG(price) FROM trades WINDOW TUMBLING(5m) GROUP BY symbol;

-- Multi-query analysis with full metadata context
EXPLAIN MULTI_QUERY (METADATA true, OWNERSHIP true, COMPLIANCE true)
WITH QUERIES (
  'portfolio_risk_calculator' AS (...),
  'real_time_price_alerts' AS (...),  
  'compliance_audit_trail' AS (...)
);

-- Configuration change impact analysis
EXPLAIN IMPACT (METADATA true, APPROVALS true, ROLLBACK_PLAN true)
FOR QUERY 'portfolio_risk_calculator'
CHANGE TYPE 'sql_update'
CHANGE DESCRIPTION 'Add new volatility calculation';

-- Version and environment comparison
EXPLAIN DIFF (METADATA true, VERSIONS true)
QUERY 'portfolio_risk_calculator'
BETWEEN ENVIRONMENTS ('staging', 'production');
```

#### **Metadata-Enriched Output Examples**
```bash
# Query registry with ownership
$ ferris-cli multi-query registry --group financial_analytics --show-history

QUERY REGISTRY - financial_analytics_suite
═══════════════════════════════════════════

┌─ portfolio_risk_calculator ─────────────────────────────────────────┐
│ Version: 2.1.2 → 2.1.3 (pending deployment)                        │
│ Owner: risk-management-team (primary), trading-platform-team (collab)│
│ Business Impact: Critical (affects $2.1B portfolio)                 │
│ SLA: 99.95% uptime, <50ms P95 latency                              │
│ Dependencies: trades_v3.1, market_data_v2.3, risk_models_v1.8      │
│ Compliance: SOX (critical), MiFID II (required)                     │
│                                                                      │
│ Recent Changes:                                                      │
│   2024-01-15: Added enhanced volatility calculation                  │
│   2024-01-13: Improved precision for ScaledInteger calculations     │
│   2024-01-10: Schema migration trades_v3.0 → v3.1                  │
│                                                                      │
│ Deployment History:                                                  │
│   Production: 2.1.2 (deployed 2024-01-15 08:30 UTC)               │
│   Staging: 2.1.3 (deployed 2024-01-15 14:25 UTC)                  │
│   Development: 2.2.0-alpha (active development)                     │
└──────────────────────────────────────────────────────────────────────┘

# Configuration drift detection
$ ferris-cli multi-query validate-config --group financial_analytics --baseline production

CONFIGURATION VALIDATION - financial_analytics_suite
════════════════════════════════════════════════════

Environment: staging
Baseline: production  
Validation Time: 2024-01-15 15:45:22 UTC

DRIFT DETECTED:
━━━━━━━━━━━━━━━

⚠️  portfolio_risk_calculator:
   └── Feature Flag Difference:
       • enhanced_risk_calculation: enabled (staging) vs disabled (production)
       • Risk Level: Medium (affects calculation accuracy)
       • Recommendation: Coordinate deployment with risk-management-team

⚠️  real_time_price_alerts:  
   └── Resource Configuration Difference:
       • Kafka consumer parallelism: 12 (staging) vs 8 (production)
       • Risk Level: Low (performance optimization)
       • Recommendation: Deploy after monitoring staging performance

✅ compliance_audit_trail:
   └── Configuration matches baseline (no drift)

SUMMARY:
• Total Queries: 3
• Queries with Drift: 2  
• High Risk Changes: 0
• Medium Risk Changes: 1
• Low Risk Changes: 1
• Approval Required: 1 (portfolio_risk_calculator feature flag)
```

### 5. Performance Topology Analysis

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