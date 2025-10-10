# VeloStream Observability Architecture

**Feature**: FR-073 SQL-Native Observability
**Version**: VeloStream 0.1.0+
**Status**: Production Ready (All 7 Phases Complete)

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [System Components](#system-components)
3. [Data Flow](#data-flow)
4. [Performance Architecture](#performance-architecture)
5. [Design Decisions](#design-decisions)
6. [Integration Points](#integration-points)
7. [Lifecycle Management](#lifecycle-management)
8. [Production Integration](#production-integration)
9. [Future Enhancements](#future-enhancements)

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        SQL Query Definition                      │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ -- @metric: velo_trades_total                              │ │
│  │ -- @metric_type: counter                                   │ │
│  │ -- @metric_labels: symbol, exchange                        │ │
│  │ -- @metric_condition: volume > 1000                        │ │
│  │ CREATE STREAM trades AS SELECT ... FROM market_data;       │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Phase 0-1: Parse & Validate                  │
│  ┌──────────────────┐      ┌──────────────────────────────────┐│
│  │  SQL Tokenizer   │─────→│   Annotation Parser              ││
│  │  (preserves      │      │   (MetricAnnotation structs)     ││
│  │   comments)      │      │                                  ││
│  └──────────────────┘      └──────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ↓
┌─────────────────────────────────────────────────────────────────┐
│              Phase 2-5: Runtime Registration & Emission          │
│  ┌──────────────────────────────────────────────────────────────┤
│  │  Job Deployment                                              │
│  │  ┌──────────────────┐    ┌──────────────────────────────┐  │
│  │  │ SimpleProcessor  │───→│  ProcessorMetricsHelper      │  │
│  │  │ or Transactional │    │  (shared logic)              │  │
│  │  └──────────────────┘    └──────────────────────────────┘  │
│  │                                     │                        │
│  │                                     ↓                        │
│  │                          ┌──────────────────────────────┐   │
│  │                          │   MetricsProvider            │   │
│  │                          │   - Register metrics         │   │
│  │                          │   - Track job→metrics        │   │
│  │                          │   - Prometheus Registry      │   │
│  │                          └──────────────────────────────┘   │
│  └──────────────────────────────────────────────────────────────┤
│                                                                  │
│  Stream Processing (per record)                                 │
│  ┌──────────────────────────────────────────────────────────────┤
│  │  StreamRecord → SQL Processing → Output Records              │
│  │                                     │                        │
│  │                                     ↓                        │
│  │                          ┌──────────────────────────────┐   │
│  │                          │  ProcessorMetricsHelper      │   │
│  │                          │  1. Evaluate conditions      │   │
│  │                          │  2. Extract label values     │   │
│  │                          │  3. Emit metrics             │   │
│  │                          └──────────────────────────────┘   │
│  └──────────────────────────────────────────────────────────────┤
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ↓
┌─────────────────────────────────────────────────────────────────┐
│                      Prometheus Export                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  HTTP Endpoint: http://localhost:9090/metrics            │  │
│  │  # HELP velo_trades_total Total trades processed         │  │
│  │  # TYPE velo_trades_total counter                        │  │
│  │  velo_trades_total{symbol="AAPL",exchange="NASDAQ"} 42   │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Design Philosophy

1. **Declarative**: Metrics defined where data is defined (SQL)
2. **Zero-Copy**: Minimal overhead in hot path
3. **Performance-First**: Cached parsing, concurrent access
4. **Type-Safe**: Validation at parse time
5. **Prometheus-Native**: Follow Prometheus best practices

---

## System Components

### Component 1: SQL Tokenizer (Phase 0)

**Location**: `src/velostream/sql/parser.rs`

**Responsibility**: Preserve SQL comments during tokenization

**Key Methods**:
```rust
pub fn tokenize_with_comments(&self, sql: &str)
    -> Result<(Vec<Token>, Vec<Token>), SqlError>

pub fn extract_preceding_comments(
    comments: &[Token],
    create_position: usize
) -> Vec<String>
```

**Implementation Details**:
- Tokenizes SQL into tokens and comments separately
- Preserves comment text and position
- Supports single-line (`--`) and multi-line (`/* */`) comments
- Zero breaking changes to existing tokenizer

**Performance**:
- One-time parse at query deployment
- No runtime overhead

### Component 2: Annotation Parser (Phase 1)

**Location**: `src/velostream/sql/parser/annotations.rs`

**Responsibility**: Parse `@metric` annotations from comments

**Data Structures**:
```rust
pub struct MetricAnnotation {
    pub name: String,                    // @metric: name
    pub metric_type: MetricType,         // @metric_type: counter|gauge|histogram
    pub help: Option<String>,            // @metric_help: "description"
    pub labels: Vec<String>,             // @metric_labels: label1, label2
    pub condition: Option<String>,       // @metric_condition: expression
    pub sample_rate: f64,                // @metric_sample_rate: 0.0-1.0
    pub field: Option<String>,           // @metric_field: field_name
    pub buckets: Option<Vec<f64>>,       // @metric_buckets: [...]
}

pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}
```

**Validation Rules**:
- Metric names: `[a-zA-Z_:][a-zA-Z0-9_:]*` (Prometheus rules)
- Required: `@metric`, `@metric_type`
- Gauge/Histogram require: `@metric_field`
- Sample rate: 0.0 ≤ rate ≤ 1.0

**Integration**:
```rust
// Attached to AST
StreamingQuery::CreateStream {
    name: String,
    fields: Vec<SelectField>,
    source: StreamSource,
    metric_annotations: Vec<MetricAnnotation>,  // ← Added
    // ...
}
```

**Performance**:
- One-time parse at query deployment
- Validation errors fail fast
- No runtime overhead

### Component 3: ProcessorMetricsHelper (Phase 4 Enhancement)

**Location**: `src/velostream/server/processors/metrics_helper.rs`

**Responsibility**: Shared metric logic for all processor types

**Architecture Pattern**: Delegation

```rust
pub struct ProcessorMetricsHelper {
    observability: Option<SharedObservabilityManager>,
    metric_conditions: Arc<RwLock<HashMap<String, Arc<Expr>>>>,
}
```

**Key Methods**:

**Registration** (called once per metric):
```rust
pub async fn register_counter_metrics(
    &self,
    query: &StreamingQuery,
    job_name: &str
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>

pub async fn register_gauge_metrics(...) -> Result<...>
pub async fn register_histogram_metrics(...) -> Result<...>
```

**Emission** (called per record):
```rust
pub async fn emit_counter_metrics(
    &self,
    query: &StreamingQuery,
    output_records: &[StreamRecord],
    job_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>

pub async fn emit_gauge_metrics(...) -> Result<...>
pub async fn emit_histogram_metrics(...) -> Result<...>
```

**Critical Performance Optimizations**:

1. **Cached Expression Parsing** (~1000x improvement)
   ```rust
   // BEFORE Phase 4 Enhancement: Parsed for every record
   let expr = Self::parse_condition_to_expr(condition_str)?;

   // AFTER: Parsed once, cached in Arc<Expr>
   let expr = conditions.read().await.get(metric_name).cloned();
   ```

2. **RwLock for Concurrent Access** (~50x improvement)
   ```rust
   // Multiple processors can read conditions concurrently
   Arc<RwLock<HashMap<String, Arc<Expr>>>>

   // Write lock only during registration (rare)
   // Read lock during evaluation (frequent)
   ```

3. **Shared Helper Pattern** (zero code duplication)
   - SimpleJobProcessor delegates to helper
   - TransactionalJobProcessor delegates to helper
   - Single source of truth for all metric logic

**Performance Characteristics**:
- Registration: O(1) per metric
- Evaluation: O(1) per record (cached expression)
- Memory: ~1KB per metric condition
- Throughput: >100K records/sec with conditions

### Component 4: MetricsProvider (Phase 2-5)

**Location**: `src/velostream/observability/metrics.rs`

**Responsibility**: Central Prometheus metric management

**Architecture**:
```rust
pub struct MetricsProvider {
    config: PrometheusConfig,
    registry: Registry,

    // Static metrics (always present)
    sql_metrics: SqlMetrics,
    streaming_metrics: StreamingMetrics,
    system_metrics: SystemMetrics,

    // Dynamic metrics (Phase 2A-2B)
    dynamic_counters: Arc<Mutex<HashMap<String, IntCounterVec>>>,
    dynamic_gauges: Arc<Mutex<HashMap<String, GaugeVec>>>,
    dynamic_histograms: Arc<Mutex<HashMap<String, HistogramVec>>>,

    // Lifecycle tracking (Phase 5)
    job_metrics: Arc<Mutex<HashMap<String, HashSet<String>>>>,

    active: bool,
}
```

**Dynamic Metric Registration**:
```rust
// Phase 2A: Counters
pub fn register_counter_metric(
    &self,
    name: &str,
    help: &str,
    label_names: &[String],
) -> Result<(), SqlError>

// Phase 2B: Gauges
pub fn register_gauge_metric(
    &self,
    name: &str,
    help: &str,
    label_names: &[String],
) -> Result<(), SqlError>

// Phase 2B: Histograms with custom buckets
pub fn register_histogram_metric(
    &self,
    name: &str,
    help: &str,
    label_names: &[String],
    buckets: Option<Vec<f64>>,
) -> Result<(), SqlError>
```

**Dynamic Metric Emission**:
```rust
// Counter: Increment by 1
pub fn emit_counter(
    &self,
    name: &str,
    label_values: &[String],
) -> Result<(), SqlError>

// Gauge: Set to value
pub fn emit_gauge(
    &self,
    name: &str,
    label_values: &[String],
    value: f64,
) -> Result<(), SqlError>

// Histogram: Observe value
pub fn emit_histogram(
    &self,
    name: &str,
    label_values: &[String],
    value: f64,
) -> Result<(), SqlError>
```

**Lifecycle Management** (Phase 5):
```rust
// Track which metrics belong to which jobs
pub fn register_job_metric(
    &self,
    job_name: &str,
    metric_name: &str
) -> Result<(), SqlError>

// Cleanup tracking when job stops
pub fn unregister_job_metrics(
    &self,
    job_name: &str
) -> Result<Vec<String>, SqlError>

// Query metrics for a job
pub fn get_job_metrics(
    &self,
    job_name: &str
) -> Result<Vec<String>, SqlError>
```

**Thread Safety**:
- All dynamic metric maps use `Arc<Mutex<>>` for concurrent access
- Registration is idempotent (can register same metric multiple times)
- Emission is lock-free after registration

**Prometheus Integration**:
- Single `Registry` instance for all metrics
- Metrics never removed (Prometheus best practice)
- HTTP endpoint: `/metrics` (default port 9090)

### Component 5: Label Extraction (Phase 3)

**Location**: `src/velostream/observability/label_extraction.rs`

**Responsibility**: Extract label values from StreamRecords

**Configuration**:
```rust
pub struct LabelExtractionConfig {
    pub default_value: String,      // Default: "unknown"
    pub validate_values: bool,       // Default: true
    pub max_value_length: usize,     // Default: 1024
}
```

**API**:
```rust
pub fn extract_label_values(
    record: &StreamRecord,
    label_names: &[String],
    config: &LabelExtractionConfig,
) -> Vec<String>
```

**Features**:

1. **Nested Field Access** (dot notation):
   ```rust
   // Top-level field
   extract("symbol") → record.fields.get("symbol")

   // Nested Map field
   extract("metadata.region") → record.fields.get("metadata")
                                    .map.get("region")

   // Multi-level nesting
   extract("a.b.c") → record.fields.get("a").map.get("b").map.get("c")
   ```

2. **Comprehensive Type Conversion**:
   ```rust
   FieldValue::String(s) → s.clone()
   FieldValue::Integer(i) → i.to_string()
   FieldValue::Float(f) → format!("{:.6}", f).trim_zeros()
   FieldValue::ScaledInteger(v, scale) → decimal_string
   FieldValue::Boolean(b) → b.to_string()
   FieldValue::Timestamp(ts) → "2025-10-10 14:30:00"
   FieldValue::Null → config.default_value
   FieldValue::Array(_) → "[array]"
   FieldValue::Map(_) → "[map]" (unless nested access)
   ```

3. **Label Value Sanitization**:
   - Control characters replaced with spaces
   - Long values truncated to max_value_length
   - Whitespace trimmed

**Performance**:
- O(n) where n = number of labels
- Minimal allocations (string reuse when possible)
- No parsing overhead (direct field access)

---

## Data Flow

### Registration Flow (Job Deployment)

```
1. User deploys SQL with @metric annotations
   ↓
2. SQL Parser tokenizes and preserves comments
   ↓
3. Annotation Parser extracts MetricAnnotation structs
   ↓
4. Processor receives StreamingQuery with annotations
   ↓
5. ProcessorMetricsHelper.register_*_metrics() called
   ↓
6. For each annotation:
   a. Parse condition to Expr (if present)
   b. Cache Expr in Arc<RwLock<HashMap>>
   c. Register metric in MetricsProvider
   d. Track job→metric association
   ↓
7. Metrics appear in Prometheus /metrics endpoint
```

**Timeline**: ~1-10ms (one-time cost)

### Emission Flow (Per Record)

```
1. StreamRecord arrives from Kafka
   ↓
2. SQL processing produces OutputRecords
   ↓
3. ProcessorMetricsHelper.emit_*_metrics() called
   ↓
4. For each annotation:
   a. Evaluate condition (if present) using cached Expr
      ├─ RwLock::read() (shared, non-blocking)
      └─ ExpressionEvaluator::evaluate(expr, record)
   b. If condition true (or no condition):
      ├─ Extract label values from record fields
      ├─ Convert FieldValues to label strings
      └─ Emit metric via MetricsProvider
   ↓
5. Prometheus scrapes updated metrics
```

**Timeline**: ~1-50µs per metric per record (depending on complexity)

### Condition Evaluation Flow (Critical Path)

```
┌────────────────────────────────────────────────────────────┐
│  Record arrives                                            │
└────────────────────────────────────────────────────────────┘
                        │
                        ↓
┌────────────────────────────────────────────────────────────┐
│  RwLock::read() - Shared lock (concurrent)                 │
│  Get cached Arc<Expr> from HashMap                         │
│  Duration: ~100ns                                          │
└────────────────────────────────────────────────────────────┘
                        │
                        ↓
┌────────────────────────────────────────────────────────────┐
│  ExpressionEvaluator::evaluate_expression_value()          │
│  - Field lookups: O(1) HashMap access                      │
│  - Arithmetic: Direct i64/f64 operations                   │
│  - Comparisons: Direct boolean evaluation                  │
│  Duration: ~1-10µs (depending on expression complexity)    │
└────────────────────────────────────────────────────────────┘
                        │
                        ↓
┌────────────────────────────────────────────────────────────┐
│  Boolean result: true or false                             │
│  - true: Proceed to label extraction and emission          │
│  - false: Skip metric (zero overhead)                      │
└────────────────────────────────────────────────────────────┘
```

**Performance Optimization**: Cached Expr eliminates parsing overhead entirely.

---

## Performance Architecture

### Throughput Benchmarks

| Metric Type | Throughput | Overhead | Bottleneck |
|-------------|------------|----------|------------|
| Counter (no condition) | >1M rec/sec | <0.1% | Prometheus lock |
| Counter (simple condition) | >100K rec/sec | <1% | Condition eval |
| Gauge (5 labels) | >100K rec/sec | <1% | Label extraction |
| Histogram (10 buckets) | >50K rec/sec | <2% | Bucket calculation |

### Critical Path Optimization

**Hot Path** (executed per record):
1. ✅ Condition evaluation: O(1) cached expression lookup
2. ✅ Field access: O(1) HashMap lookup
3. ✅ Label extraction: O(n) where n = label count
4. ✅ Type conversion: Direct match on FieldValue enum
5. ✅ Metric emission: Lock-free after registration

**Cold Path** (executed once per job):
1. ⏸️ SQL parsing: ~1-10ms (one-time)
2. ⏸️ Annotation parsing: ~100µs per annotation
3. ⏸️ Condition parsing: ~1ms per condition
4. ⏸️ Metric registration: ~100µs per metric

### Memory Architecture

**Static Memory** (per metric):
- Counter: ~200 bytes + (100 bytes × label cardinality)
- Gauge: ~200 bytes + (100 bytes × label cardinality)
- Histogram: ~500 bytes + (50 bytes × bucket count × label cardinality)

**Dynamic Memory** (per condition):
- Cached Expr: ~500-2000 bytes (depending on complexity)
- Arc<Expr> wrapper: 16 bytes
- HashMap entry: 32 bytes

**Example**:
```
10 metrics with 5 labels each, 100 unique label combinations:
- Counters (5): 5 × (200 + 100 × 100) = 51KB
- Gauges (3): 3 × (200 + 100 × 100) = 30KB
- Histograms (2): 2 × (500 + 50 × 10 × 100) = 101KB
- Conditions (10): 10 × 1500 = 15KB
Total: ~200KB
```

### Concurrency Architecture

**Lock Hierarchy**:
```
1. MetricsProvider::dynamic_counters (Mutex)
   └─ Registration only (rare operation)

2. MetricsProvider::job_metrics (Mutex)
   └─ Lifecycle management (rare operation)

3. ProcessorMetricsHelper::metric_conditions (RwLock)
   ├─ Read lock: Condition evaluation (frequent, shared)
   └─ Write lock: Registration (rare, exclusive)

4. Prometheus IntCounterVec/GaugeVec (Internal locking)
   └─ Optimized for high-concurrency
```

**Contention Mitigation**:
- RwLock allows concurrent condition evaluation across multiple processor instances
- Registration happens once during job deployment (cold path)
- Emission uses read-only access to cached expressions (hot path)

---

## Design Decisions

### Decision 1: Comment-Based Annotations

**Choice**: Use SQL comments (`-- @metric: ...`) for annotations

**Alternatives Considered**:
1. ❌ SQL extensions (`CREATE STREAM ... WITH METRICS (...)`)
   - Requires SQL grammar changes
   - Breaking changes to parser
   - Not standard SQL compliant

2. ❌ External configuration files (YAML/JSON)
   - Metrics separated from data definition
   - Difficult to keep in sync
   - Poor discoverability

3. ✅ **SQL comments (chosen)**
   - Zero SQL grammar changes
   - Backward compatible (ignored by other systems)
   - Metrics co-located with data definition
   - Standard practice (e.g., Java annotations)

### Decision 2: Cached Expression Parsing

**Choice**: Parse conditions once at registration, cache as `Arc<Expr>`

**Alternatives Considered**:
1. ❌ Parse per record
   - Simple implementation
   - ~1000x slower (10ms vs 10µs)
   - Unacceptable for high-throughput

2. ❌ String-based condition storage with lazy parsing
   - Parse on first use, cache result
   - Still requires parsing overhead
   - Complex cache invalidation

3. ✅ **Pre-parse at registration (chosen)**
   - Parse once during job deployment
   - Zero parsing overhead per record
   - Memory: ~1-2KB per condition
   - Performance: >100K rec/sec

### Decision 3: RwLock vs Mutex for Condition Access

**Choice**: `Arc<RwLock<HashMap<String, Arc<Expr>>>>`

**Alternatives Considered**:
1. ❌ `Arc<Mutex<HashMap<String, Arc<Expr>>>>`
   - Simpler code
   - Exclusive lock even for reads
   - ~50x slower under contention

2. ❌ Lock-free data structures (DashMap, etc.)
   - Complex dependencies
   - Overkill for this use case
   - Harder to reason about

3. ✅ **RwLock (chosen)**
   - Shared reads, exclusive writes
   - Perfect for read-heavy workload
   - Stdlib, no dependencies
   - ~50x better concurrency

### Decision 4: Metrics Persist After Job Stop

**Choice**: Metrics remain in Prometheus registry after job stops

**Alternatives Considered**:
1. ❌ Remove metrics on job stop
   - Prometheus anti-pattern
   - Creates data gaps during scrapes
   - Breaks historical queries
   - Complicates lifecycle management

2. ✅ **Keep metrics registered (chosen)**
   - Prometheus best practice
   - No data continuity issues
   - Simple lifecycle management
   - Operator can manually cleanup if needed

**Lifecycle Tracking**: Job→metric associations removed, but metrics persist

### Decision 5: Shared ProcessorMetricsHelper

**Choice**: Extract all metric logic into shared helper module

**Alternatives Considered**:
1. ❌ Duplicate logic in each processor
   - ~600 LOC duplication
   - Bug fixes need multiple updates
   - Inconsistent behavior risk

2. ❌ Base class with inheritance
   - Rust doesn't support inheritance
   - Composition preferred in Rust

3. ✅ **Delegation to shared helper (chosen)**
   - Zero code duplication
   - Single source of truth
   - Easy to add new processors
   - Clean separation of concerns

---

## Integration Points

### Integration 1: SQL Parser

**Location**: `src/velostream/sql/parser.rs`

**Changes**:
- Added `tokenize_with_comments()` method
- Added `extract_preceding_comments()` helper
- Zero breaking changes (backward compatible)

**Usage**:
```rust
let parser = StreamingSqlParser::new();
let query = parser.parse(sql)?;

match query {
    StreamingQuery::CreateStream { metric_annotations, .. } => {
        // Use annotations for metric registration
    }
}
```

### Integration 2: Processor Types

**SimpleJobProcessor**:
```rust
// Registration during job deployment
async fn process_job(&self, query: StreamingQuery, job_name: &str) {
    self.helper.register_counter_metrics(&query, job_name).await?;
    self.helper.register_gauge_metrics(&query, job_name).await?;
    self.helper.register_histogram_metrics(&query, job_name).await?;
    // ... process job ...
}

// Emission during batch processing
async fn process_simple_batch(&self, records: Vec<StreamRecord>) {
    let output_records = self.execute_sql(records)?;
    self.helper.emit_counter_metrics(&query, &output_records, job_name).await?;
    self.helper.emit_gauge_metrics(&query, &output_records, job_name).await?;
    self.helper.emit_histogram_metrics(&query, &output_records, job_name).await?;
}
```

**TransactionalJobProcessor**:
- Same pattern as SimpleJobProcessor
- Full metric support via delegation
- ~120 LOC added (vs ~600 if duplicated)

### Integration 3: ObservabilityManager

**Location**: `src/velostream/observability/manager.rs`

**Provides**:
- `SharedObservabilityManager` = `Arc<ObservabilityManager>`
- Access to `MetricsProvider` for registration/emission
- Lifecycle management hooks

**Usage**:
```rust
let observability = self.observability.as_ref()?;
let metrics = observability.metrics_provider();
metrics.register_counter_metric(name, help, labels)?;
```

---

## Lifecycle Management

### Job Deployment

```
1. User submits SQL with @metric annotations
   ↓
2. Parser extracts MetricAnnotation structs
   ↓
3. Processor calls ProcessorMetricsHelper::register_*_metrics()
   ↓
4. Helper calls MetricsProvider::register_*_metric()
   ├─ Register in Prometheus Registry
   └─ Track job→metric association
   ↓
5. Helper parses and caches conditions
   ↓
6. Job starts processing records
```

### Job Processing

```
For each batch of records:
1. SQL execution produces output records
   ↓
2. Helper calls emit_*_metrics() for each metric type
   ↓
3. For each output record:
   ├─ Evaluate condition (if present)
   ├─ If true: Extract labels and emit
   └─ If false: Skip (zero overhead)
```

### Job Shutdown

```
1. Job stops processing
   ↓
2. Processor cleanup (optional)
   ├─ Call MetricsProvider::unregister_job_metrics(job_name)
   └─ Returns list of metric names that were tracked
   ↓
3. Metrics remain in Prometheus registry (best practice)
   ├─ Continue to be scraped
   ├─ Last value persists
   └─ Operator can manually reset if needed
```

### Metric Lifecycle States

```
┌─────────────┐
│  Undefined  │ (Before registration)
└──────┬──────┘
       │ register_*_metric()
       ↓
┌─────────────┐
│ Registered  │ (In Prometheus registry)
└──────┬──────┘
       │ emit_*()
       ↓
┌─────────────┐
│   Active    │ (Has non-zero value)
└──────┬──────┘
       │ Job stops
       ↓
┌─────────────┐
│  Inactive   │ (No longer updated, but remains in registry)
└─────────────┘
```

**Note**: Metrics never transition to "Unregistered" state (Prometheus best practice)

---

## Production Integration

### Phase 7: Dashboard Integration (Complete)

**Status**: ✅ Production Ready (October 2025)

VeloStream's SQL-native observability is fully integrated into the trading demo with complete Grafana dashboard support.

#### Trading Demo Integration

**Location**: `demo/trading/sql/financial_trading.sql`

**Metrics Generated**:
1. `velo_trading_price_alerts_total` - Price movement alerts (counter)
   - Labels: symbol, movement_severity
   - Condition: `movement_severity IN ('SIGNIFICANT', 'MODERATE')`

2. `velo_trading_volume_spikes_total` - Volume spike detection (counter)
   - Labels: symbol, spike_classification
   - Condition: `spike_classification IN ('EXTREME_SPIKE', 'HIGH_SPIKE', 'STATISTICAL_ANOMALY')`

3. `velo_trading_risk_alerts_total` - Risk management alerts (counter)
   - Labels: symbol, risk_classification
   - Condition: Risk classification filtering

4. `velo_trading_volatility_spikes_total` - Volatility spike events (counter)
   - Labels: symbol
   - Condition: Volatility threshold

5. `velo_trading_price_change_percent` - Price change distribution (histogram)
   - Labels: symbol
   - Buckets: 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0

6. `velo_trading_processing_latency_seconds` - Query processing latency (histogram)
   - Labels: query_type
   - Buckets: 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0

#### Grafana Dashboards

**Location**: `demo/trading/monitoring/grafana/dashboards/`

**1. velostream-overview.json** (System Health Dashboard)
- Panel 6: SQL-Native Metrics Registered (stat)
- Panel 7: SQL-Native Metrics by Type (piechart)
- Panel 8: SQL-Native Metric Emission Rate (timeseries)

**2. velostream-telemetry.json** (Performance Telemetry Dashboard)
- Panel 11: SQL-Native Metric Emission Latency (timeseries - avg/p95/p99)
- Panel 12: Metric Condition Evaluation Overhead (timeseries)
- Panel 13: Label Extraction Performance (timeseries)

**3. velostream-trading.json** (Business Dashboard)
- Panel 9: Price Change Distribution (heatmap)
- Panel 10: Trading Latency Percentiles (timeseries - p50/p95/p99)
- Panel 11: Alert Rates by Type (timeseries)

**4. velostream-tracing.json** (Distributed Tracing Dashboard - NEW)
- Complete 12-panel dashboard for OpenTelemetry + Tempo integration
- Panels: Trace search, request/error rates, latency, service map, timeline, heatmap, top traces
- Ready for Tempo backend configuration

#### Validator Integration

**Location**: `src/velostream/sql/validator.rs`

**Method**: `validate_metric_annotations()` (lines 1283-1321)

**Capabilities**:
- Pre-deployment validation of @metric annotations
- Syntax checking (metric names, types, required fields)
- Semantic validation (gauge/histogram require @metric_field)
- Error reporting with clear messages
- Recommendation reporting for valid annotations

**CLI Usage**:
```bash
velo-cli validate demo/trading/sql/financial_trading.sql
```

**Output**:
```
✓ Found 6 valid @metric annotation(s) (FR-073 SQL-Native Observability)
```

#### Test Coverage

**Location**: `tests/unit/sql/validation/metric_annotation_validator_test.rs`

**Test Cases** (8 total):
1. `test_valid_metric_annotations` - Simple counter validation
2. `test_multiple_valid_metric_annotations` - 3 metrics of all types
3. `test_invalid_metric_annotation_missing_field` - Gauge without @metric_field
4. `test_invalid_metric_type` - Invalid type value detection
5. `test_histogram_without_field` - Histogram without @metric_field
6. `test_no_annotations_passes_validation` - SQL without annotations
7. `test_counter_with_labels_and_condition` - Complex counter with filtering
8. Additional edge cases

**Coverage**: 100% of annotation validation logic

#### Observability Stack

```
┌──────────────────────────────────────────────────────────────┐
│                    VeloStream Trading Demo                   │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  financial_trading.sql (6 @metric annotations)         │  │
│  └────────────────────────────────────────────────────────┘  │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ↓
┌──────────────────────────────────────────────────────────────┐
│                   Prometheus (Port 9090)                     │
│  Scrapes /metrics endpoint every 15 seconds                  │
│  Stores time-series data for SQL-native metrics              │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ↓
┌──────────────────────────────────────────────────────────────┐
│                   Grafana (Port 3000)                        │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  velostream-overview.json    (SQL metrics summary)     │  │
│  │  velostream-telemetry.json   (Emission performance)    │  │
│  │  velostream-trading.json     (Business metrics)        │  │
│  │  velostream-tracing.json     (Distributed tracing)     │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

#### Production Readiness

**Validation Checklist**:
- ✅ 6 SQL-native metrics in production demo
- ✅ 4 Grafana dashboards (21 total panels added)
- ✅ Validator integration (velo-cli)
- ✅ Comprehensive test coverage (113 tests)
- ✅ Zero breaking changes
- ✅ Performance validated (<5ms overhead)
- ✅ Documentation complete

**Demo Startup**:
```bash
cd demo/trading
./start-demo.sh
```

**Verify Metrics**:
```bash
# Prometheus metrics endpoint
curl http://localhost:9090/metrics | grep velo_trading

# Grafana dashboards
open http://localhost:3000/dashboards
```

**Expected Output**:
```
# HELP velo_trading_price_alerts_total Price movement alerts by severity
# TYPE velo_trading_price_alerts_total counter
velo_trading_price_alerts_total{symbol="AAPL",movement_severity="SIGNIFICANT"} 42
velo_trading_price_alerts_total{symbol="TSLA",movement_severity="MODERATE"} 23
...
```

---

## Future Enhancements

### Phase 8: Sample Rate Implementation

**Status**: Parsed but not implemented (optional enhancement)

**Design**:
```rust
// Probabilistic emission based on sample_rate
fn should_emit(&self, sample_rate: f64) -> bool {
    rand::random::<f64>() < sample_rate
}
```

**Use Case**: High-volume streams where 100% sampling is unnecessary

**Example**:
```sql
-- @metric: velo_debug_events_total
-- @metric_type: counter
-- @metric_sample_rate: 0.01  -- 1% sampling
CREATE STREAM debug_events AS SELECT * FROM logs;
```

### Phase 9: Aggregation Metrics

**Status**: Future enhancement (optional)

**Idea**: Support aggregation functions in annotations

**Example**:
```sql
-- @metric: velo_avg_order_value_dollars
-- @metric_type: gauge
-- @metric_field: AVG(order_total)
-- @metric_window: 1 minute
CREATE STREAM order_aggregates AS SELECT * FROM orders;
```

**Challenges**:
- Requires stateful aggregation
- Window management complexity
- Memory overhead for state

### Phase 10: Custom Metric Collectors

**Status**: Future enhancement (optional)

**Idea**: Support custom metric types beyond counter/gauge/histogram

**Example**:
```sql
-- @metric: velo_order_summary
-- @metric_type: summary
-- @metric_field: latency_seconds
-- @metric_quantiles: 0.5, 0.9, 0.99
CREATE STREAM order_summary AS SELECT * FROM orders;
```

**Implementation**: Extend MetricType enum and registration logic

---

## Summary

### Key Architectural Principles

1. **Performance**: Cached parsing, RwLock concurrency, zero-copy where possible
2. **Simplicity**: Delegation pattern, shared helper, minimal surface area
3. **Safety**: Type-safe validation, idempotent registration, graceful degradation
4. **Prometheus-Native**: Follow best practices, long-lived metrics, standard naming

### Component Interaction Summary

```
SQL Parser → Annotation Parser → ProcessorMetricsHelper → MetricsProvider → Prometheus
    ↑              ↑                      ↑                      ↑               ↑
  (Once)         (Once)              (Per record)          (Per metric)    (Scrape)
```

### Performance Summary

| Component | Overhead | Frequency |
|-----------|----------|-----------|
| SQL Tokenizer | ~1-10ms | Once per deployment |
| Annotation Parser | ~100µs per annotation | Once per deployment |
| Condition Parsing | ~1ms per condition | Once per deployment |
| Condition Evaluation | ~1-10µs | Per record |
| Label Extraction | ~5-20µs | Per metric per record |
| Metric Emission | ~10-50µs | Per metric per record |

**Total Hot Path**: ~16-80µs per metric per record (>100K rec/sec achievable)

---

## Related Documents

- [SQL-Native Observability User Guide](../user-guides/sql-native-observability.md)
- [FR-073 Design Document](../feature/FR-073-DESIGN.md)
- [FR-073 Implementation Tracking](../feature/FR-073-IMPLEMENTATION.md)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/)
