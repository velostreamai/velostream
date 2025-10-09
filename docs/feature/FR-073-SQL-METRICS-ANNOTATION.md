# Feature Request: SQL-Based Metrics Annotation for Business Observability

## Overview

Enable SQL queries to annotate their outputs with Prometheus metrics declarations, allowing business-level observability to be defined declaratively alongside the SQL logic that generates the events. This creates a self-documenting, integrated approach to both stream processing and observability.

**Competitive Advantage**: This feature provides a significant edge over Apache Flink, which requires separate Java/Scala code for custom metrics. Velostream users can define metrics directly in SQL, making observability a first-class citizen of the streaming pipeline.

“This moves Velostream toward a unified SQL + Observability model, where every query is both a transformation and a self-monitoring entity.”

## Current State

### System Metrics (✅ Implemented)
Velostream currently exports system-level metrics:
- `velo_streaming_operations_total{operation="deserialization|serialization|sql_processing"}`
- `velo_sql_records_processed_total`
- `velo_sql_queries_total`
- `process_cpu_seconds_total`
- `process_resident_memory_bytes`

These metrics are exposed on port 9091 and scraped by Prometheus.

### Business Metrics (❌ Not Implemented)
Application-specific business metrics require:
- Separate external services consuming Kafka topics
- Custom Python/Rust code to count events
- Additional infrastructure (metrics exporters)
- No co-location with business logic

**Example**: The financial trading demo generates `volume_spikes`, `price_alerts`, and `risk_alerts` topics, but these events are not available as Prometheus metrics without building a separate exporter service.

## Proposed Feature

### SQL Metrics Annotation Syntax

Add support for `@metric` annotations in SQL comments to declare Prometheus metrics that should be exported when records are written to output topics:

```sql
-- Market data aggregation with moving averages
CREATE STREAM market_data_stream AS
SELECT
    symbol,
    price,
    volume,
    event_time,
    AVG(price) OVER (
        PARTITION BY symbol
        ORDER BY event_time
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as hourly_avg_price,
    AVG(volume) OVER (
        PARTITION BY symbol
        ORDER BY event_time
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as hourly_avg_volume
FROM raw_market_data;

-- Detect volume spikes and export as Prometheus counter
-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_help: "Total number of volume spikes detected across all symbols"
-- @metric_labels: symbol, spike_ratio
-- @metric_condition: volume > hourly_avg_volume * 2.0
CREATE STREAM volume_spikes AS
SELECT
    symbol,
    volume,
    hourly_avg_volume,
    (volume / hourly_avg_volume) as spike_ratio,
    event_time
FROM market_data_stream
WHERE volume > hourly_avg_volume * 2.0;

-- Price alerts with metric tracking
-- @metric: velo_trading_price_alerts_total
-- @metric_type: counter
-- @metric_help: "Total number of price alerts generated"
-- @metric_labels: symbol, alert_type
-- @metric_condition: true  -- Every record is an alert
CREATE STREAM price_alerts AS
SELECT
    symbol,
    price,
    hourly_avg_price,
    ABS(price - hourly_avg_price) / hourly_avg_price as price_deviation,
    CASE
        WHEN price > hourly_avg_price * 1.05 THEN 'SPIKE'
        WHEN price < hourly_avg_price * 0.95 THEN 'DROP'
    END as alert_type,
    event_time
FROM market_data_stream
WHERE price > hourly_avg_price * 1.05
   OR price < hourly_avg_price * 0.95;

-- Risk management alerts
-- @metric: velo_trading_risk_alerts_total
-- @metric_type: counter
-- @metric_help: "Total number of risk management alerts"
-- @metric_labels: trader_id, risk_level
CREATE STREAM risk_alerts AS
SELECT
    trader_id,
    total_exposure,
    position_count,
    CASE
        WHEN total_exposure > 1000000 THEN 'CRITICAL'
        WHEN total_exposure > 500000 THEN 'HIGH'
        ELSE 'MEDIUM'
    END as risk_level,
    event_time
FROM trader_positions
WHERE total_exposure > 100000;
```

### Gauge Metrics for State Tracking

Support gauge metrics that track current state rather than just counting events:

```sql
-- Active trading symbols gauge
-- @metric: velo_trading_active_symbols
-- @metric_type: gauge
-- @metric_help: "Number of actively traded symbols in the last 5 minutes"
-- @metric_aggregation: count_distinct
-- @metric_field: symbol
-- @metric_window: 5m
CREATE STREAM active_symbols_tracking AS
SELECT
    symbol,
    event_time
FROM market_data_stream;

-- Average position size per trader
-- @metric: velo_trading_avg_position_size
-- @metric_type: gauge
-- @metric_help: "Average position size across all active traders"
-- @metric_aggregation: avg
-- @metric_field: position_value
-- @metric_labels: trader_type
CREATE STREAM position_tracking AS
SELECT
    trader_id,
    trader_type,
    position_value,
    event_time
FROM trading_positions;
```

### Histogram Metrics for Distribution Analysis

Support histogram metrics for analyzing value distributions:

```sql
-- Order execution latency histogram
-- @metric: velo_trading_order_latency_seconds
-- @metric_type: histogram
-- @metric_help: "Distribution of order execution latencies"
-- @metric_buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
-- @metric_field: execution_latency_ms
-- @metric_labels: order_type
CREATE STREAM order_execution_tracking AS
SELECT
    order_id,
    order_type,
    EXTRACT(EPOCH FROM (execution_time - order_time)) * 1000 as execution_latency_ms,
    event_time
FROM order_executions;
```

### Metric Lifecycle
Metrics are automatically registered when a stream is deployed and unregistered when it is dropped, ensuring clean Prometheus state across job restarts.


## Benefits

### 1. **Competitive Edge Over Apache Flink**

#### Flink Approach (Complex)
```java
// Flink requires custom Java/Scala code
public class VolumeSpikesMetrics extends RichMapFunction<Trade, Trade> {
    private transient Counter volumeSpikesCounter;

    @Override
    public void open(Configuration config) {
        this.volumeSpikesCounter = getRuntimeContext()
            .getMetricGroup()
            .counter("velo_trading_volume_spikes_total");
    }

    @Override
    public Trade map(Trade trade) {
        if (trade.volume > trade.avgVolume * 2.0) {
            volumeSpikesCounter.inc();
        }
        return trade;
    }
}

// Then use in Flink SQL
stream
    .map(new VolumeSpikesMetrics())
    .addSink(kafkaSink);
```

#### Velostream Approach (Declarative)
```sql
-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_condition: volume > avg_volume * 2.0
CREATE STREAM volume_spikes AS
SELECT * FROM trades WHERE volume > avg_volume * 2.0;
```

**Key Advantages**:
- ✅ No separate Java/Scala code required
- ✅ Metrics defined with business logic, not separately
- ✅ Self-documenting - SQL shows both logic AND observability
- ✅ Easier to maintain and modify
- ✅ Lower barrier to entry for data engineers

### 2. **Self-Documenting Pipelines**

SQL files become comprehensive documentation of:
- What business events are detected
- What metrics are exported
- How metrics map to business logic
- What labels are available for analysis

Example:
```sql
-- This single SQL file documents:
-- 1. Business Logic: Detect volume spikes > 2x average
-- 2. Observability: Export velo_trading_volume_spikes_total counter
-- 3. Analysis Dimensions: Labels by symbol and spike_ratio
-- 4. Integration Point: Data flows to Grafana dashboard "Volume Spikes"

-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_labels: symbol, spike_ratio
CREATE STREAM volume_spikes AS ...
```

### 3. **Integrated Development Workflow**

Developers work in a single context:
1. Write SQL to detect business events
2. Add `@metric` annotations for observability
3. Deploy SQL file - metrics automatically available
4. Build Grafana dashboards using declared metrics

No need to:
- Write separate metrics exporter services
- Maintain mapping between topics and metrics
- Deploy additional infrastructure
- Context-switch between SQL and application code

### 4. **Version Control Integration**

Metrics declarations are versioned alongside SQL logic:
```bash
git diff financial_trading.sql
```
Shows both business logic changes AND observability impact:
```diff
 -- @metric: velo_trading_volume_spikes_total
 -- @metric_type: counter
+-- @metric_labels: symbol, spike_ratio, exchange
--- @metric_condition: volume > avg_volume * 2.0
+-- @metric_condition: volume > avg_volume * 3.0
```

### 5. **Production Operations Benefits**

**Alerting Setup**:
```yaml
# Prometheus alerting rules reference SQL-declared metrics
groups:
  - name: trading_alerts
    rules:
      - alert: HighVolumeSpikeRate
        expr: rate(velo_trading_volume_spikes_total[5m]) > 10
        annotations:
          summary: "Unusual volume spike activity"
          # Metric name matches SQL @metric annotation
```

**Dashboard Discovery**:
- Grafana can query Prometheus `/api/v1/label/__name__/values`
- Discover all `velo_trading_*` metrics
- Auto-generate dashboard templates
- Metrics include help text from `@metric_help` annotations

## Strategic Differentiation: Dual-Plane Observability

This feature positions Velostream to **leapfrog Flink, Arroyo, and Materialize** in terms of operational visibility and product differentiation.

### Multi-Backend Export Strategy

**Prometheus vs ClickHouse (and Beyond)**:

#### Prometheus
✅ **Strengths**:
- Great for ephemeral metrics (rates, counters, gauges, histograms)
- Best suited for "live" runtime health metrics: throughput, latency, error counts, watermark lag, backpressure signals

❌ **Limitations**:
- Poor for retention, ad-hoc analytics, or joins (especially for multi-month time spans)
- Limited to 5-30 minute retention windows for high-cardinality data

#### ClickHouse
✅ **Strengths**:
- Excellent for analytical retention: can store per-key or per-stream metric history for weeks/months
- Can be queried directly via SQL and Grafana
- Extremely fast for high-cardinality time series (symbols, users, stream IDs)

💡 **Best For**: "Historical introspection" — e.g., "show top 10 symbols with imbalance alerts in the last 7 days"

#### Ideal Setup: Dual Exporters

Use both backends simultaneously for comprehensive observability:

```yaml
# config.yaml
metrics:
  exporters:
    - type: prometheus
      port: 9091
      retention: 30m
    - type: clickhouse
      url: clickhouse://metrics-db:8123
      table: velostream_metrics
      batch_size: 5000
      retention: 90d
```

**SQL Annotation Support**:
```sql
-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @export: clickhouse, prometheus  -- NEW: Multi-backend export
CREATE STREAM volume_spikes AS ...
```

This makes Velostream a **"dual-plane observability" engine**:
- **Live metrics** → Prometheus (operational dashboards)
- **Rich analytical metrics** → ClickHouse (historical analysis, auditing)

### Distributed Tracing Integration (OpenTelemetry)

**Critical Differentiator**: This is a must-have if Velostream aims to be modern and deep on platform observability.

#### Why it Matters
- Allows a single transaction (tick update, imbalance alert) to be traced end-to-end across pipelines
- Perfect for debugging: "where did latency spike?", "which operator was slow?", "what input topic caused the anomaly?"
- Ties into industry-standard tools: Grafana Tempo, Jaeger, Honeycomb

#### Implementation Plan

Velostream runtime emits OpenTelemetry traces:

| Span Name | Description |
|-----------|-------------|
| `stream.plan.compile` | Query parsing, optimization |
| `stream.task.init` | Stream deployment & operator graph build |
| `stream.record.process` | Each record batch or operator execution |
| `stream.sink.write` | Writes to Kafka, ClickHouse, etc. |
| `stream.metric.export` | Exporter flush cycle |

Each span carries contextual tags:
```
stream=volume_spikes
operator=FilterOperator
symbol=AAPL
partition=3
latency_ms=12
```

**Configuration**:
```yaml
tracing:
  enabled: true
  exporter: otlp
  endpoint: http://tempo:4317
  service_name: velostream
  sampling_rate: 0.1
```

**SQL Annotation Support**:
```sql
-- @metric: trade_imbalance_total
-- @trace: enabled                  -- NEW: Enable tracing for this stream
-- @trace_sample_rate: 0.05         -- NEW: Sample 5% of records
-- @export: clickhouse, prometheus
CREATE STREAM imbalance_alerts AS ...
```

### Unified Observability Layer (The Real Edge)

**Product Differentiation**: Velostream can unify **metrics, logs, and traces** under a single declarative schema driven by SQL annotations.

**What Makes This Special**:

Each annotated stream automatically registers:
1. **Prometheus metrics endpoint** (real-time monitoring)
2. **ClickHouse long-term storage** (historical analytics)
3. **OpenTelemetry tracing context** (end-to-end debugging)

**No YAML config. No manual instrumentation. All declarative. All SQL-first.**

This would be a **huge differentiator over Flink or Arroyo**, where observability is bolt-on.

#### Future Optional Add-ons

| Feature | Benefit |
|---------|---------|
| **Metrics lineage map** | Trace which metrics came from which SQL streams |
| **ClickHouse as universal sink** | Store all metrics + DLQ + audit events |
| **Grafana dashboard autogen** | Generate dashboards per metric annotation |
| **Trace-to-metric correlation** | Jump from Grafana metric → Jaeger trace instantly |
| **Slow stream detector** | Use metrics to auto-alert when operator latency exceeds baseline |

### Phased Rollout Recommendation

#### ✅ Short Term (MVP)
- Prometheus export only
- Optional OpenTelemetry traces

#### ✅ Medium Term (Differentiator)
- Dual exporters (Prometheus + ClickHouse)
- SQL-level export annotations (`@export: prometheus, clickhouse`)
- Full tracing integration

#### ✅ Long Term (Market Edge)
- Unified declarative observability layer: metrics, traces, lineage — all SQL-driven
- Auto-generated dashboards from annotations
- Metrics discovery API and catalog UI

---

**Design**

**Annotation Grammar**

Each annotation must appear immediately before a CREATE STREAM statement.

| Directive             | Description                                                                 |
| --------------------- | --------------------------------------------------------------------------- |
| `@metric`             | Name of metric (snake_case). Automatically prefixed with `velo_` if absent. |
| `@metric_type`        | `counter` | `gauge` | `histogram`.                                          |
| `@metric_labels`      | Comma-separated list of fields to attach as labels.                         |
| `@metric_sample_rate` | Optional. 0 < x ≤ 1. Defaults = 1.0.                                        |
| `@metric_aggregation` | Optional for gauges. One of `sum`, `avg`, `max`, `min`, `count_distinct`.   |


Multiple annotations can be declared; all are merged for that stream. Inline comments are ignored for metric parsing.

Metric Lifecycle

Metrics are registered automatically when a stream is deployed. Metrics are unregistered when the stream is dropped or the job stops.

Names are namespaced as:
<prefix>_<stream_name>_<metric_name>


_Example: velo_volume_spikes_total{stream="volume_spikes"}._

**Default system labels**
_Label	Description_
stream	SQL stream name
job	Deployment/job identifier
instance	Host or pod name


_Metrics follow Prometheus naming rules ([a-zA-Z_:][a-zA-Z0-9_:]*)._


**Emission Semantics**
_Metric Type, 	Emission Rule_
Counter:	Incremented once per emitted record matching the stream’s WHERE clause.
Gauge:	Updated at each batch boundary or watermark advancement.
Histogram:	Observes a numeric field per matching record.

Counters and histograms respect @metric_sample_rate.

**Example Configuration**

# config.yaml
metrics:
annotations_enabled: true
prometheus_port: 9091
ignore_invalid_metrics: false


## Implementation Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                     SQL File with Annotations                    │
│                                                                   │
│  -- @metric: velo_trading_volume_spikes_total                   │
│  -- @metric_type: counter                                        │
│  -- @metric_labels: symbol                                       │
│  CREATE STREAM volume_spikes AS                                  │
│  SELECT * FROM market_data WHERE volume > avg_volume * 2.0;     │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│          Phase 1: SQL Parser (annotations.rs)                    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ parse_metric_annotations(sql_comments)                    │  │
│  │   → Vec<MetricAnnotation>                                 │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│        StreamDefinition (parser/mod.rs)                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ pub struct StreamDefinition {                             │  │
│  │     pub stream_name: String,                              │  │
│  │     pub query: SelectStatement,                           │  │
│  │     pub metric_annotations: Vec<MetricAnnotation>, // NEW │  │
│  │ }                                                          │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│   Phase 2: Metrics Registry (observability/metrics.rs)          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ register_annotated_metric(annotation)                     │  │
│  │   → Register Counter/Gauge/Histogram in Prometheus        │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│   Phase 3: Runtime Emission (processors/simple.rs)              │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ process_batch() → for each record:                        │  │
│  │   1. Extract labels from record fields                    │  │
│  │   2. Evaluate @metric_condition                           │  │
│  │   3. Emit metric (counter.inc() / gauge.set())            │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│         Prometheus Metrics Endpoint (:9091/metrics)             │
│                                                                   │
│  # HELP velo_trading_volume_spikes_total Volume spikes detected │
│  # TYPE velo_trading_volume_spikes_total counter                │
│  velo_trading_volume_spikes_total{symbol="AAPL"} 42             │
│  velo_trading_volume_spikes_total{symbol="TSLA"} 17             │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
              Grafana Dashboard
              (Auto-discovery via Prometheus API)
```

### Component Overview

| Component | Location | Responsibility |
|-----------|----------|----------------|
| **Annotation Parser** | `src/velostream/sql/parser/annotations.rs` | Parse `@metric` comments into structured data |
| **Stream Definition** | `src/velostream/sql/parser/mod.rs` | Store annotations with stream metadata |
| **Metrics Registry** | `src/velostream/observability/metrics.rs` | Register Prometheus metrics at stream startup |
| **Runtime Emitter** | `src/velostream/server/processors/simple.rs` | Emit metrics during record processing |
| **Label Extractor** | `src/velostream/server/processors/common.rs` | Extract label values from record fields |
| **Condition Evaluator** | `src/velostream/sql/execution/expression/mod.rs` | Evaluate `@metric_condition` expressions |

### Data Flow

1. **Parse Time**: SQL parser extracts `@metric` annotations → `Vec<MetricAnnotation>`
2. **Deploy Time**: Stream deployment registers metrics in Prometheus registry
3. **Runtime**: Each processed record triggers metric emission (if conditions match)
4. **Scrape Time**: Prometheus scrapes metrics endpoint on port 9091
5. **Query Time**: Grafana queries Prometheus for dashboard visualization

### Integration Points

**Existing Infrastructure** (Already Complete):
- ✅ `ObservabilityManager` - Core observability infrastructure
- ✅ `MetricsProvider` - Prometheus metrics export on port 9091
- ✅ `StreamJobServer` - Stream deployment and lifecycle management
- ✅ `SimpleStreamProcessor` - Batch processing with access to observability

**New Components** (To Be Implemented):
- ❌ `annotations.rs` - Annotation parser module
- ❌ `MetricAnnotation` struct - Annotation data structure
- ❌ Label extraction logic in processors
- ❌ Condition evaluation for filtered metrics

---

## Implementation Plan

### Level of Effort Summary

| Phase | Duration | Complexity | Files Modified | Tests Added | Total LOC |
|-------|----------|------------|----------------|-------------|-----------|
| **Phase 1: Parser** | 1.5 weeks | Medium | 3 | 8 tests | ~400 LOC |
| **Phase 2: Runtime** | 2 weeks | High | 4 | 12 tests | ~600 LOC |
| **Phase 3: Labels** | 0.5 weeks | Low | 2 | 6 tests | ~200 LOC |
| **Phase 4: Conditions** | 1 week | Medium | 3 | 8 tests | ~350 LOC |
| **Phase 5: Registry** | 1 week | Low | 2 | 5 tests | ~250 LOC |
| **Phase 6: Documentation** | 1 week | Low | - | - | ~500 LOC |
| **TOTAL** | **7 weeks** | - | **14 files** | **39 tests** | **~2,300 LOC** |

### MVP Scope (2 Weeks)

**Reduced Scope for Quick Validation**:
- Counter metrics only (no gauges or histograms)
- Simple label extraction (no nested fields)
- No condition evaluation (all records emit metrics)
- Basic error handling

**MVP Deliverables**:
```sql
-- @metric: my_events_total
-- @metric_type: counter
-- @metric_labels: status
CREATE STREAM events AS SELECT event_id, status FROM source;
```

**MVP Files**:
- `src/velostream/sql/parser/annotations.rs` (~200 LOC)
- `src/velostream/server/processors/simple.rs` (modifications, ~100 LOC)
- `src/velostream/observability/metrics.rs` (modifications, ~80 LOC)
- `tests/unit/sql/parser/metric_annotations_test.rs` (~150 LOC)
- `tests/integration/metrics/sql_annotations_integration_test.rs` (~100 LOC)

---

### Phase 1: SQL Parser Enhancement (Week 1-2)

**Duration**: 1.5 weeks
**Complexity**: Medium
**LOC**: ~400 lines

#### 1.1 Create Annotation Parser Module

**New File**: `src/velostream/sql/parser/annotations.rs` (~300 LOC)

```rust
// src/velostream/sql/parser/annotations.rs

use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub struct MetricAnnotation {
    /// Metric name (e.g., "velo_trading_volume_spikes_total")
    pub name: String,

    /// Type of Prometheus metric
    pub metric_type: MetricType,

    /// Human-readable help text for Prometheus
    pub help: Option<String>,

    /// Fields to extract as metric labels
    pub labels: Vec<String>,

    /// SQL expression to filter which records emit metrics
    /// Example: "volume > avg_volume * 2.0"
    pub condition: Option<String>,

    /// For gauges: how to aggregate values
    pub aggregation: Option<Aggregation>,

    /// For gauges/histograms: which field to measure
    pub field: Option<String>,

    /// For gauges: time window for aggregation
    pub window: Option<Duration>,

    /// For histograms: bucket boundaries
    pub buckets: Option<Vec<f64>>,

    /// Sample rate (0.0 to 1.0) for high-volume streams
    pub sample_rate: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregation {
    Sum,
    Avg,
    Max,
    Min,
    CountDistinct,
}

/// Parse metric annotations from SQL comment lines
///
/// Example input:
/// ```
/// -- @metric: velo_trading_volume_spikes_total
/// -- @metric_type: counter
/// -- @metric_help: "Volume spikes detected"
/// -- @metric_labels: symbol, spike_ratio
/// -- @metric_condition: volume > avg_volume * 2.0
/// ```
pub fn parse_metric_annotations(sql_comments: &[String]) -> Result<Vec<MetricAnnotation>, AnnotationError> {
    let mut current_annotation: Option<HashMap<String, String>> = None;
    let mut annotations = Vec::new();

    for line in sql_comments {
        let trimmed = line.trim();

        // Check if line starts with @metric annotation
        if trimmed.starts_with("@metric:") {
            // Save previous annotation if exists
            if let Some(anno) = current_annotation.take() {
                annotations.push(build_annotation(anno)?);
            }

            // Start new annotation
            let name = trimmed.trim_start_matches("@metric:").trim();
            let mut anno = HashMap::new();
            anno.insert("name".to_string(), name.to_string());
            current_annotation = Some(anno);
        } else if let Some(ref mut anno) = current_annotation {
            // Parse other annotation fields
            if let Some((key, value)) = parse_annotation_field(trimmed) {
                anno.insert(key, value);
            }
        }
    }

    // Save last annotation
    if let Some(anno) = current_annotation {
        annotations.push(build_annotation(anno)?);
    }

    Ok(annotations)
}

fn parse_annotation_field(line: &str) -> Option<(String, String)> {
    if line.starts_with("@metric_") {
        let rest = line.trim_start_matches('@');
        if let Some(colon_idx) = rest.find(':') {
            let key = rest[..colon_idx].trim();
            let value = rest[colon_idx + 1..].trim();
            return Some((key.to_string(), value.to_string()));
        }
    }
    None
}

fn build_annotation(fields: HashMap<String, String>) -> Result<MetricAnnotation, AnnotationError> {
    // Extract required fields
    let name = fields.get("name")
        .ok_or(AnnotationError::MissingField("name"))?
        .clone();

    let metric_type_str = fields.get("metric_type")
        .ok_or(AnnotationError::MissingField("metric_type"))?;

    let metric_type = match metric_type_str.as_str() {
        "counter" => MetricType::Counter,
        "gauge" => MetricType::Gauge,
        "histogram" => MetricType::Histogram,
        _ => return Err(AnnotationError::InvalidMetricType(metric_type_str.clone())),
    };

    // Parse optional fields
    let help = fields.get("metric_help").map(|s| s.trim_matches('"').to_string());

    let labels = fields.get("metric_labels")
        .map(|s| s.split(',').map(|l| l.trim().to_string()).collect())
        .unwrap_or_default();

    let condition = fields.get("metric_condition").cloned();

    let sample_rate = fields.get("metric_sample_rate")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0);

    // Validate sample rate
    if sample_rate <= 0.0 || sample_rate > 1.0 {
        return Err(AnnotationError::InvalidSampleRate(sample_rate));
    }

    Ok(MetricAnnotation {
        name,
        metric_type,
        help,
        labels,
        condition,
        aggregation: None, // TODO: Parse from fields
        field: None,
        window: None,
        buckets: None,
        sample_rate,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum AnnotationError {
    #[error("Missing required field: {0}")]
    MissingField(&'static str),

    #[error("Invalid metric type: {0}")]
    InvalidMetricType(String),

    #[error("Invalid sample rate: {0} (must be 0 < x <= 1)")]
    InvalidSampleRate(f64),
}
```

**Related Existing Code**:
- Parser infrastructure: `src/velostream/sql/parser/mod.rs:1-50`
- Comment handling: Reference implementation in `src/velostream/sql/parser/lexer.rs` (if exists)
- Error types: `src/velostream/sql/error.rs:1-100`

#### 1.2 Integrate with Stream Definition

**Modified File**: `src/velostream/sql/parser/mod.rs` (~50 LOC additions)

**Current Code** (around line 100):
```rust
pub struct StreamDefinition {
    pub stream_name: String,
    pub query: SelectStatement,
    pub sink_config: SinkConfig,
}
```

**New Code**:
```rust
pub struct StreamDefinition {
    pub stream_name: String,
    pub query: SelectStatement,
    pub sink_config: SinkConfig,
    pub metric_annotations: Vec<MetricAnnotation>, // NEW
}

impl StreamDefinition {
    pub fn new(
        stream_name: String,
        query: SelectStatement,
        sink_config: SinkConfig,
    ) -> Self {
        Self {
            stream_name,
            query,
            sink_config,
            metric_annotations: Vec::new(),
        }
    }

    pub fn with_metric_annotations(mut self, annotations: Vec<MetricAnnotation>) -> Self {
        self.metric_annotations = annotations;
        self
    }
}
```

**Parser Integration** (around line 200):
```rust
// In parse_create_stream() function
fn parse_create_stream(tokens: &[Token]) -> Result<StreamingQuery, SqlError> {
    // ... existing parsing logic ...

    // NEW: Extract comments before CREATE STREAM statement
    let comments = extract_comments_before_statement(tokens);
    let metric_annotations = parse_metric_annotations(&comments)
        .map_err(|e| SqlError::AnnotationError(e.to_string()))?;

    let stream_def = StreamDefinition::new(stream_name, query, sink_config)
        .with_metric_annotations(metric_annotations);

    Ok(StreamingQuery::CreateStream { definition: stream_def })
}
```

#### 1.3 Tests

**New File**: `tests/unit/sql/parser/metric_annotations_test.rs` (~150 LOC)

```rust
use velostream::velostream::sql::parser::annotations::*;

#[test]
fn test_parse_counter_annotation() {
    let comments = vec![
        "-- @metric: test_counter_total".to_string(),
        "-- @metric_type: counter".to_string(),
        "-- @metric_help: \"Test counter metric\"".to_string(),
        "-- @metric_labels: label1, label2".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    let anno = &annotations[0];
    assert_eq!(anno.name, "test_counter_total");
    assert_eq!(anno.metric_type, MetricType::Counter);
    assert_eq!(anno.help, Some("Test counter metric".to_string()));
    assert_eq!(anno.labels, vec!["label1", "label2"]);
}

#[test]
fn test_parse_multiple_annotations() {
    let comments = vec![
        "-- @metric: metric1_total".to_string(),
        "-- @metric_type: counter".to_string(),
        "-- @metric: metric2_gauge".to_string(),
        "-- @metric_type: gauge".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();
    assert_eq!(annotations.len(), 2);
}

#[test]
fn test_missing_required_field() {
    let comments = vec![
        "-- @metric: test_metric".to_string(),
        // Missing @metric_type
    ];

    let result = parse_metric_annotations(&comments);
    assert!(result.is_err());
}

#[test]
fn test_invalid_metric_type() {
    let comments = vec![
        "-- @metric: test_metric".to_string(),
        "-- @metric_type: invalid".to_string(),
    ];

    let result = parse_metric_annotations(&comments);
    assert!(matches!(result, Err(AnnotationError::InvalidMetricType(_))));
}

#[test]
fn test_sample_rate_validation() {
    let comments = vec![
        "-- @metric: test_metric".to_string(),
        "-- @metric_type: counter".to_string(),
        "-- @metric_sample_rate: 0.5".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();
    assert_eq!(annotations[0].sample_rate, 0.5);
}

#[test]
fn test_condition_parsing() {
    let comments = vec![
        "-- @metric: conditional_metric".to_string(),
        "-- @metric_type: counter".to_string(),
        "-- @metric_condition: volume > avg_volume * 2.0".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();
    assert_eq!(
        annotations[0].condition,
        Some("volume > avg_volume * 2.0".to_string())
    );
}
```

**Test Registration**: Add to `tests/unit/sql/parser/mod.rs`:
```rust
pub mod metric_annotations_test;
```

#### Phase 1 Deliverables

- [ ] Create `src/velostream/sql/parser/annotations.rs` (~300 LOC)
- [ ] Modify `src/velostream/sql/parser/mod.rs` (~50 LOC)
- [ ] Add `MetricAnnotation` to `StreamDefinition`
- [ ] Create `tests/unit/sql/parser/metric_annotations_test.rs` (~150 LOC)
- [ ] Register test in `tests/unit/sql/parser/mod.rs`
- [ ] 8 comprehensive unit tests passing
- [ ] Documentation comments for all public APIs

**Dependencies**:
- Existing SQL parser infrastructure
- Token/lexer for comment extraction
- Error handling framework

**Risk Mitigation**:
- Start with simple case (counter only)
- Extensive test coverage (8+ tests)
- Clear error messages for invalid annotations

### Phase 2: Metrics Runtime Integration (Week 3-4)

**Duration**: 2 weeks
**Complexity**: High
**LOC**: ~600 lines

**Modified Files**:
- `src/velostream/server/processors/simple.rs` (~200 LOC)
- `src/velostream/observability/metrics.rs` (~150 LOC)
- `src/velostream/server/stream_job_server.rs` (~50 LOC)

**Key Implementation**:

```rust
// src/velostream/server/processors/simple.rs (around line 150)

impl SimpleStreamProcessor {
    /// Process batch and emit metrics for annotated streams
    async fn process_batch(&mut self, batch: RecordBatch) -> Result<(), ProcessingError> {
        // Existing SQL processing...
        let output_records = self.process_sql(batch)?;

        // NEW: Emit metrics for annotated streams
        if !self.stream_def.metric_annotations.is_empty() {
            for annotation in &self.stream_def.metric_annotations {
                self.emit_metrics_for_annotation(annotation, &output_records).await?;
            }
        }

        Ok(())
    }

    fn emit_metrics_for_annotation(
        &self,
        annotation: &MetricAnnotation,
        records: &[StreamRecord]
    ) -> Result<(), MetricsError> {
        let Some(metrics) = self.observability.metrics() else {
            return Ok(()); // Metrics not enabled
        };

        match annotation.metric_type {
            MetricType::Counter => {
                let count = records.len() as u64;
                if count > 0 {
                    // Extract labels from first record (for batch-level metrics)
                    let labels = self.extract_labels(annotation, &records[0])?;
                    metrics.increment_counter_by(&annotation.name, count, labels)?;
                }
            }
            // Gauge and Histogram implementations follow similar pattern
            _ => {} // TODO: Implement in later phases
        }
        Ok(())
    }
}
```

**Existing Code References**:
- Current processor implementation: `src/velostream/server/processors/simple.rs:100-200`
- Observability access: `src/velostream/server/processors/simple.rs:50` (existing field)
- MetricsProvider interface: `src/velostream/observability/metrics.rs:30-100`

**Phase 2 Deliverables**:
- [ ] Integrate metrics emission in `process_batch()` (~200 LOC)
- [ ] Add `increment_counter_by()` method to MetricsProvider (~50 LOC)
- [ ] Register metrics at stream startup in StreamJobServer (~50 LOC)
- [ ] Create 12 unit tests for runtime emission
- [ ] Create 3 integration tests for end-to-end flow
- [ ] Performance benchmarks (< 5% overhead target)

### Phase 3: Label Extraction (Week 5)

**Duration**: 0.5 weeks
**Complexity**: Low
**LOC**: ~200 lines

**Modified Files**:
- `src/velostream/server/processors/common.rs` (~150 LOC)
- `tests/unit/server/processors/label_extraction_test.rs` (~150 LOC)

**Key Implementation** - Add to `common.rs`:

```rust
/// Extract label values from record fields
pub fn extract_metric_labels(
    annotation: &MetricAnnotation,
    record: &StreamRecord
) -> Result<HashMap<String, String>, MetricsError> {
    let mut labels = HashMap::new();

    for label_name in &annotation.labels {
        if let Some(field_value) = record.fields.get(label_name) {
            labels.insert(label_name.clone(), field_value.to_string());
        } else {
            // Missing label - use empty string or error based on config
            log::warn!("Missing label field: {}", label_name);
        }
    }

    Ok(labels)
}
```

**Related Code**:
- FieldValue::to_string(): `src/velostream/sql/execution/types.rs:200`
- StreamRecord structure: `src/velostream/sql/execution/mod.rs:50`

**Phase 3 Deliverables**:
- [ ] Label extraction utility function (~50 LOC)
- [ ] Handle missing labels gracefully
- [ ] 6 unit tests for label extraction
- [ ] Integration with Phase 2 runtime emitter

### Phase 4: Condition Evaluation (Week 6)

**Duration**: 1 week
**Complexity**: Medium
**LOC**: ~350 lines

**Goal**: Only emit metrics when conditions are met

**Modified Files**:
- `src/velostream/sql/execution/expression/mod.rs` (~150 LOC additions)
- `src/velostream/server/processors/simple.rs` (~100 LOC modifications)
- `tests/unit/sql/execution/expression/condition_eval_test.rs` (~100 LOC)

#### 4.1 Condition Evaluator Implementation

**Modified File**: `src/velostream/sql/execution/expression/mod.rs` (around line 400)

**Existing Code Reference**:
- Expression evaluation infrastructure: `src/velostream/sql/execution/expression/mod.rs:1-400`
- FieldValue comparisons: `src/velostream/sql/execution/types.rs:150-250`

**New Code**:

```rust
// src/velostream/sql/execution/expression/mod.rs

use std::collections::HashMap;
use crate::velostream::sql::execution::types::FieldValue;

/// Evaluate a simple condition expression against record fields
///
/// Supports expressions like:
/// - "volume > avg_volume * 2.0"
/// - "price > 100.0 AND status = 'active'"
/// - "event_type IN ('ALERT', 'WARNING')"
pub fn evaluate_condition_string(
    condition: &str,
    fields: &HashMap<String, FieldValue>
) -> Result<bool, ConditionError> {
    // Parse the condition string into an expression
    let expr = parse_condition_expression(condition)?;

    // Evaluate the expression
    let result = evaluate_expression(&expr, fields)?;

    // Convert result to boolean
    match result {
        FieldValue::Boolean(b) => Ok(b),
        _ => Err(ConditionError::NonBooleanResult(result)),
    }
}

#[derive(Debug, Clone)]
enum ConditionExpr {
    Field(String),
    Literal(FieldValue),
    BinaryOp {
        left: Box<ConditionExpr>,
        op: BinaryOperator,
        right: Box<ConditionExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum BinaryOperator {
    // Comparison
    Eq, Ne, Gt, Lt, Gte, Lte,
    // Arithmetic
    Add, Sub, Mul, Div,
    // Logical
    And, Or,
}

fn parse_condition_expression(condition: &str) -> Result<ConditionExpr, ConditionError> {
    // Simple recursive descent parser for conditions
    // Handles: field comparisons, literals, arithmetic, logical operators

    let tokens = tokenize_condition(condition)?;
    parse_tokens(&tokens, 0).map(|(expr, _)| expr)
}

fn tokenize_condition(s: &str) -> Result<Vec<Token>, ConditionError> {
    // Tokenize input: "volume > avg_volume * 2.0"
    // → [Ident("volume"), Op(">"), Ident("avg_volume"), Op("*"), Literal(2.0)]

    let mut tokens = Vec::new();
    let mut chars = s.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' => { chars.next(); } // Skip whitespace
            '>' | '<' | '=' | '!' => {
                // Handle operators: >, <, =, !=, >=, <=
                chars.next();
                let mut op = ch.to_string();
                if chars.peek() == Some(&'=') {
                    op.push(chars.next().unwrap());
                }
                tokens.push(Token::Operator(op));
            }
            '+' | '-' | '*' | '/' => {
                tokens.push(Token::Operator(chars.next().unwrap().to_string()));
            }
            '0'..='9' => {
                // Parse number literal
                let mut num = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_numeric() || ch == '.' {
                        num.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                let value: f64 = num.parse()
                    .map_err(|_| ConditionError::InvalidNumber(num))?;
                tokens.push(Token::Literal(FieldValue::Float(value)));
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                // Parse identifier (field name or keyword)
                let mut ident = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch.is_alphanumeric() || ch == '_' {
                        ident.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                // Check for keywords
                match ident.to_uppercase().as_str() {
                    "AND" => tokens.push(Token::Operator("AND".to_string())),
                    "OR" => tokens.push(Token::Operator("OR".to_string())),
                    "TRUE" => tokens.push(Token::Literal(FieldValue::Boolean(true))),
                    "FALSE" => tokens.push(Token::Literal(FieldValue::Boolean(false))),
                    _ => tokens.push(Token::Identifier(ident)),
                }
            }
            _ => return Err(ConditionError::UnexpectedChar(ch)),
        }
    }

    Ok(tokens)
}

fn evaluate_expression(
    expr: &ConditionExpr,
    fields: &HashMap<String, FieldValue>
) -> Result<FieldValue, ConditionError> {
    match expr {
        ConditionExpr::Field(name) => {
            fields.get(name)
                .cloned()
                .ok_or_else(|| ConditionError::MissingField(name.clone()))
        }
        ConditionExpr::Literal(value) => Ok(value.clone()),
        ConditionExpr::BinaryOp { left, op, right } => {
            let left_val = evaluate_expression(left, fields)?;
            let right_val = evaluate_expression(right, fields)?;

            apply_binary_op(&left_val, op, &right_val)
        }
    }
}

fn apply_binary_op(
    left: &FieldValue,
    op: &BinaryOperator,
    right: &FieldValue
) -> Result<FieldValue, ConditionError> {
    use BinaryOperator::*;

    match op {
        Gt => Ok(FieldValue::Boolean(compare_values(left, right)? > 0)),
        Lt => Ok(FieldValue::Boolean(compare_values(left, right)? < 0)),
        Gte => Ok(FieldValue::Boolean(compare_values(left, right)? >= 0)),
        Lte => Ok(FieldValue::Boolean(compare_values(left, right)? <= 0)),
        Eq => Ok(FieldValue::Boolean(compare_values(left, right)? == 0)),
        Ne => Ok(FieldValue::Boolean(compare_values(left, right)? != 0)),

        Add => numeric_op(left, right, |a, b| a + b),
        Sub => numeric_op(left, right, |a, b| a - b),
        Mul => numeric_op(left, right, |a, b| a * b),
        Div => numeric_op(left, right, |a, b| a / b),

        And => boolean_op(left, right, |a, b| a && b),
        Or => boolean_op(left, right, |a, b| a || b),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConditionError {
    #[error("Missing field in record: {0}")]
    MissingField(String),

    #[error("Condition evaluated to non-boolean: {0:?}")]
    NonBooleanResult(FieldValue),

    #[error("Invalid number: {0}")]
    InvalidNumber(String),

    #[error("Unexpected character: {0}")]
    UnexpectedChar(char),

    #[error("Type mismatch in comparison")]
    TypeMismatch,
}
```

#### 4.2 Integration with Metrics Emission

**Modified File**: `src/velostream/server/processors/simple.rs` (around line 250)

```rust
impl SimpleStreamProcessor {
    /// Emit metrics for records matching annotation conditions
    fn emit_metrics_for_annotation(
        &self,
        annotation: &MetricAnnotation,
        records: &[StreamRecord]
    ) -> Result<(), MetricsError> {
        let Some(metrics) = self.observability.metrics() else {
            return Ok(());
        };

        // Filter records by condition
        let matching_records: Vec<&StreamRecord> = if let Some(condition) = &annotation.condition {
            records.iter()
                .filter(|r| evaluate_condition_string(condition, &r.fields).unwrap_or(false))
                .collect()
        } else {
            records.iter().collect()
        };

        if matching_records.is_empty() {
            return Ok(()); // No records match condition
        }

        match annotation.metric_type {
            MetricType::Counter => {
                // Apply sampling if configured
                let count = if annotation.sample_rate < 1.0 {
                    self.apply_sampling(&matching_records, annotation.sample_rate).len() as u64
                } else {
                    matching_records.len() as u64
                };

                if count > 0 {
                    // Extract labels from first matching record
                    let labels = extract_metric_labels(annotation, matching_records[0])?;
                    metrics.increment_counter_by(&annotation.name, count, labels)?;
                }
            }
            MetricType::Gauge => {
                // Gauge updates with latest value
                if let Some(field_name) = &annotation.field {
                    if let Some(value) = matching_records.last()
                        .and_then(|r| r.fields.get(field_name))
                    {
                        let numeric_value = value.as_f64()
                            .ok_or(MetricsError::NonNumericGaugeValue)?;
                        let labels = extract_metric_labels(annotation, matching_records.last().unwrap())?;
                        metrics.set_gauge(&annotation.name, numeric_value, labels)?;
                    }
                }
            }
            MetricType::Histogram => {
                // Observe each matching record
                if let Some(field_name) = &annotation.field {
                    for record in matching_records {
                        if let Some(value) = record.fields.get(field_name) {
                            let numeric_value = value.as_f64()
                                .ok_or(MetricsError::NonNumericHistogramValue)?;
                            let labels = extract_metric_labels(annotation, record)?;
                            metrics.observe_histogram(&annotation.name, numeric_value, labels)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn apply_sampling(&self, records: &[&StreamRecord], sample_rate: f64) -> Vec<&StreamRecord> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        records.iter()
            .filter(|_| rng.gen::<f64>() < sample_rate)
            .copied()
            .collect()
    }
}
```

#### 4.3 Tests

**New File**: `tests/unit/sql/execution/expression/condition_eval_test.rs` (~100 LOC)

```rust
use velostream::velostream::sql::execution::expression::evaluate_condition_string;
use velostream::velostream::sql::execution::types::FieldValue;
use std::collections::HashMap;

#[test]
fn test_simple_comparison() {
    let mut fields = HashMap::new();
    fields.insert("volume".to_string(), FieldValue::Float(100.0));
    fields.insert("avg_volume".to_string(), FieldValue::Float(40.0));

    // Test: volume > avg_volume * 2.0 → 100 > 80 → true
    let result = evaluate_condition_string("volume > avg_volume * 2.0", &fields).unwrap();
    assert!(result);
}

#[test]
fn test_logical_operators() {
    let mut fields = HashMap::new();
    fields.insert("price".to_string(), FieldValue::Float(150.0));
    fields.insert("status".to_string(), FieldValue::String("active".to_string()));

    // Price > 100 AND status = 'active'
    let condition = "price > 100.0 AND status = active";
    let result = evaluate_condition_string(condition, &fields).unwrap();
    assert!(result);
}

#[test]
fn test_missing_field_error() {
    let fields = HashMap::new();

    let result = evaluate_condition_string("missing_field > 100", &fields);
    assert!(result.is_err());
}
```

#### Phase 4 Deliverables

- [ ] Condition expression parser (~150 LOC)
- [ ] Expression evaluator with operator support (~100 LOC)
- [ ] Integration with metrics emission (~100 LOC)
- [ ] 8 comprehensive unit tests
- [ ] Error handling for invalid expressions
- [ ] Support for comparison, arithmetic, and logical operators
```

### Phase 5: Metrics Registry Enhancement (Week 7)

**Duration**: 1 week
**Complexity**: Low
**LOC**: ~250 lines

**Goal**: Register metrics with proper metadata and lifecycle management

**Modified Files**:
- `src/velostream/observability/metrics.rs` (~150 LOC additions)
- `src/velostream/server/stream_job_server.rs` (~50 LOC modifications)
- `tests/unit/observability/metric_registry_test.rs` (~100 LOC)

#### 5.1 Enhanced Metrics Registry

**Modified File**: `src/velostream/observability/metrics.rs` (around line 200)

**Existing Code Reference**:
- Current MetricsProvider: `src/velostream/observability/metrics.rs:50-200`
- Counter/Gauge registration: `src/velostream/observability/metrics.rs:150-180`

**New Code**:

```rust
// src/velostream/observability/metrics.rs

use prometheus::{IntCounterVec, GaugeVec, HistogramVec, Registry, Opts};
use std::collections::HashMap;

/// Enhanced metrics provider with annotation support
pub struct MetricsProvider {
    registry: Registry,
    counters: HashMap<String, IntCounterVec>,
    gauges: HashMap<String, GaugeVec>,
    histograms: HashMap<String, HistogramVec>,

    /// Track which stream registered which metrics (for cleanup)
    stream_metrics: HashMap<String, Vec<String>>,
}

impl MetricsProvider {
    /// Register a metric from SQL annotation
    ///
    /// This method:
    /// 1. Validates metric name (Prometheus naming rules)
    /// 2. Creates metric with proper metadata (help text)
    /// 3. Registers metric in Prometheus registry
    /// 4. Tracks association between stream and metric
    pub fn register_annotated_metric(
        &mut self,
        stream_name: &str,
        annotation: &MetricAnnotation
    ) -> Result<(), MetricsError> {
        // Validate metric name
        self.validate_metric_name(&annotation.name)?;

        // Check if metric already registered (idempotent)
        if self.is_metric_registered(&annotation.name) {
            log::debug!("Metric {} already registered, skipping", annotation.name);
            return Ok(());
        }

        // Register based on type
        match annotation.metric_type {
            MetricType::Counter => {
                self.register_counter(annotation)?;
            }
            MetricType::Gauge => {
                self.register_gauge(annotation)?;
            }
            MetricType::Histogram => {
                self.register_histogram(annotation)?;
            }
        }

        // Track stream → metric association
        self.stream_metrics
            .entry(stream_name.to_string())
            .or_insert_with(Vec::new)
            .push(annotation.name.clone());

        log::info!(
            "Registered metric '{}' (type: {:?}) for stream '{}'",
            annotation.name,
            annotation.metric_type,
            stream_name
        );

        Ok(())
    }

    fn register_counter(&mut self, annotation: &MetricAnnotation) -> Result<(), MetricsError> {
        let opts = Opts::new(
            annotation.name.clone(),
            annotation.help.clone().unwrap_or_else(|| format!("Counter metric: {}", annotation.name))
        );

        let counter = if annotation.labels.is_empty() {
            // Simple counter without labels
            IntCounterVec::new(opts, &[])?
        } else {
            // Counter with labels
            let label_names: Vec<&str> = annotation.labels.iter().map(|s| s.as_str()).collect();
            IntCounterVec::new(opts, &label_names)?
        };

        self.registry.register(Box::new(counter.clone()))
            .map_err(|e| MetricsError::RegistrationFailed(e.to_string()))?;

        self.counters.insert(annotation.name.clone(), counter);
        Ok(())
    }

    fn register_gauge(&mut self, annotation: &MetricAnnotation) -> Result<(), MetricsError> {
        let opts = Opts::new(
            annotation.name.clone(),
            annotation.help.clone().unwrap_or_else(|| format!("Gauge metric: {}", annotation.name))
        );

        let label_names: Vec<&str> = annotation.labels.iter().map(|s| s.as_str()).collect();
        let gauge = GaugeVec::new(opts, &label_names)?;

        self.registry.register(Box::new(gauge.clone()))
            .map_err(|e| MetricsError::RegistrationFailed(e.to_string()))?;

        self.gauges.insert(annotation.name.clone(), gauge);
        Ok(())
    }

    fn register_histogram(&mut self, annotation: &MetricAnnotation) -> Result<(), MetricsError> {
        let buckets = annotation.buckets.clone().unwrap_or_else(|| {
            // Default buckets suitable for latency measurements (milliseconds)
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
        });

        let opts = prometheus::HistogramOpts::new(
            annotation.name.clone(),
            annotation.help.clone().unwrap_or_else(|| format!("Histogram metric: {}", annotation.name))
        ).buckets(buckets);

        let label_names: Vec<&str> = annotation.labels.iter().map(|s| s.as_str()).collect();
        let histogram = HistogramVec::new(opts, &label_names)?;

        self.registry.register(Box::new(histogram.clone()))
            .map_err(|e| MetricsError::RegistrationFailed(e.to_string()))?;

        self.histograms.insert(annotation.name.clone(), histogram);
        Ok(())
    }

    /// Unregister all metrics associated with a stream
    ///
    /// Called when stream is dropped or deployment stops
    pub fn unregister_stream_metrics(&mut self, stream_name: &str) -> Result<(), MetricsError> {
        if let Some(metric_names) = self.stream_metrics.remove(stream_name) {
            for metric_name in metric_names {
                // Remove from internal tracking
                self.counters.remove(&metric_name);
                self.gauges.remove(&metric_name);
                self.histograms.remove(&metric_name);

                // Note: Prometheus registry doesn't support unregistration easily
                // Metrics will remain but won't be updated
                log::info!("Unregistered metric '{}' for stream '{}'", metric_name, stream_name);
            }
        }
        Ok(())
    }

    fn validate_metric_name(&self, name: &str) -> Result<(), MetricsError> {
        // Prometheus naming rules: [a-zA-Z_:][a-zA-Z0-9_:]*
        if name.is_empty() {
            return Err(MetricsError::InvalidMetricName("Metric name cannot be empty".to_string()));
        }

        let first_char = name.chars().next().unwrap();
        if !first_char.is_alphabetic() && first_char != '_' && first_char != ':' {
            return Err(MetricsError::InvalidMetricName(
                format!("Metric name must start with letter, underscore, or colon: {}", name)
            ));
        }

        for ch in name.chars() {
            if !ch.is_alphanumeric() && ch != '_' && ch != ':' {
                return Err(MetricsError::InvalidMetricName(
                    format!("Invalid character in metric name '{}': {}", name, ch)
                ));
            }
        }

        Ok(())
    }

    fn is_metric_registered(&self, name: &str) -> bool {
        self.counters.contains_key(name)
            || self.gauges.contains_key(name)
            || self.histograms.contains_key(name)
    }

    /// Get counter by name (for emission)
    pub fn get_counter(&self, name: &str) -> Option<&IntCounterVec> {
        self.counters.get(name)
    }

    /// Get gauge by name (for emission)
    pub fn get_gauge(&self, name: &str) -> Option<&GaugeVec> {
        self.gauges.get(name)
    }

    /// Get histogram by name (for emission)
    pub fn get_histogram(&self, name: &str) -> Option<&HistogramVec> {
        self.histograms.get(name)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("Invalid metric name: {0}")]
    InvalidMetricName(String),

    #[error("Metric registration failed: {0}")]
    RegistrationFailed(String),

    #[error("Metric not found: {0}")]
    MetricNotFound(String),

    #[error("Non-numeric value for gauge metric")]
    NonNumericGaugeValue,

    #[error("Non-numeric value for histogram metric")]
    NonNumericHistogramValue,
}
```

#### 5.2 Stream Deployment Integration

**Modified File**: `src/velostream/server/stream_job_server.rs` (around line 180)

```rust
// In deploy_stream() function

async fn deploy_stream(&self, stream_def: StreamDefinition) -> Result<(), DeploymentError> {
    // ... existing deployment logic ...

    // NEW: Register metrics from annotations
    if let Some(metrics_provider) = self.observability.metrics() {
        for annotation in &stream_def.metric_annotations {
            metrics_provider.register_annotated_metric(&stream_def.stream_name, annotation)
                .map_err(|e| DeploymentError::MetricsRegistrationFailed(e.to_string()))?;
        }
    }

    // ... continue with deployment ...
}

async fn drop_stream(&self, stream_name: &str) -> Result<(), DeploymentError> {
    // ... existing drop logic ...

    // NEW: Unregister metrics
    if let Some(metrics_provider) = self.observability.metrics() {
        metrics_provider.unregister_stream_metrics(stream_name)
            .map_err(|e| DeploymentError::MetricsCleanupFailed(e.to_string()))?;
    }

    Ok(())
}
```

#### 5.3 Tests

**New File**: `tests/unit/observability/metric_registry_test.rs` (~100 LOC)

```rust
use velostream::velostream::observability::metrics::{MetricsProvider, MetricsError};
use velostream::velostream::sql::parser::annotations::{MetricAnnotation, MetricType};

#[test]
fn test_register_counter_metric() {
    let mut provider = MetricsProvider::new();

    let annotation = MetricAnnotation {
        name: "test_counter_total".to_string(),
        metric_type: MetricType::Counter,
        help: Some("Test counter".to_string()),
        labels: vec!["label1".to_string()],
        ..Default::default()
    };

    provider.register_annotated_metric("test_stream", &annotation).unwrap();
    assert!(provider.is_metric_registered("test_counter_total"));
}

#[test]
fn test_invalid_metric_name() {
    let mut provider = MetricsProvider::new();

    let annotation = MetricAnnotation {
        name: "123_invalid".to_string(), // Starts with number
        metric_type: MetricType::Counter,
        ..Default::default()
    };

    let result = provider.register_annotated_metric("test_stream", &annotation);
    assert!(matches!(result, Err(MetricsError::InvalidMetricName(_))));
}

#[test]
fn test_unregister_stream_metrics() {
    let mut provider = MetricsProvider::new();

    let annotation = MetricAnnotation {
        name: "stream_metric_total".to_string(),
        metric_type: MetricType::Counter,
        ..Default::default()
    };

    provider.register_annotated_metric("stream1", &annotation).unwrap();
    provider.unregister_stream_metrics("stream1").unwrap();

    // Metric should be removed from tracking
    assert!(!provider.is_metric_registered("stream_metric_total"));
}
```

#### Phase 5 Deliverables

- [ ] Enhanced MetricsProvider with annotation support (~150 LOC)
- [ ] Metric lifecycle management (register/unregister) (~50 LOC)
- [ ] Prometheus naming validation (~30 LOC)
- [ ] Stream deployment integration (~50 LOC)
- [ ] 5 unit tests for registry operations
- [ ] Error handling for invalid metrics

---

### Phase 6: Documentation and Examples (Week 8)

**Duration**: 1 week
**Complexity**: Low
**LOC**: ~500 lines (documentation)

**Goal**: Comprehensive documentation, user guides, and working examples

**Deliverables**:
- User guide for SQL metrics annotations
- API reference documentation
- Updated financial trading demo
- Tutorial and migration guide

#### 6.1 User Documentation

**New File**: `docs/user/sql-metrics-annotations.md` (~200 LOC)

```markdown
# SQL Metrics Annotations Guide

## Overview

Velostream allows you to declare Prometheus metrics directly in your SQL files using
comment annotations. This provides a declarative, self-documenting approach to business
observability.

## Quick Start

Add `@metric` annotations before CREATE STREAM statements:

```sql
-- @metric: order_events_total
-- @metric_type: counter
-- @metric_help: "Total number of order events processed"
-- @metric_labels: status, customer_type
CREATE STREAM orders AS
SELECT order_id, status, customer_type, amount
FROM order_stream
WHERE amount > 0;
```

When this stream is deployed, Velostream automatically:
1. Registers `order_events_total` counter in Prometheus
2. Increments counter for each processed order
3. Attaches `status` and `customer_type` as labels

## Metric Types

### Counter - Counting Events

Use counters for monotonically increasing values (events, requests, errors):

```sql
-- @metric: api_requests_total
-- @metric_type: counter
-- @metric_labels: endpoint, method
CREATE STREAM api_events AS ...
```

### Gauge - Current Values

Use gauges for values that can go up and down (temperature, queue length):

```sql
-- @metric: active_connections
-- @metric_type: gauge
-- @metric_field: connection_count
CREATE STREAM connection_tracking AS ...
```

### Histogram - Distributions

Use histograms for analyzing value distributions (latency, request size):

```sql
-- @metric: request_duration_seconds
-- @metric_type: histogram
-- @metric_field: duration_ms
-- @metric_buckets: [0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
-- @metric_labels: service
CREATE STREAM request_tracking AS ...
```

## Advanced Features

### Conditional Metrics

Only emit metrics when conditions are met:

```sql
-- @metric: high_value_orders_total
-- @metric_type: counter
-- @metric_condition: amount > 1000.0
CREATE STREAM high_value_orders AS ...
```

### Sampling

Reduce metrics overhead for high-throughput streams:

```sql
-- @metric: events_sample_total
-- @metric_type: counter
-- @metric_sample_rate: 0.1  -- Sample 10% of events
CREATE STREAM high_volume_events AS ...
```

## Best Practices

1. **Use Clear Names**: Follow Prometheus naming conventions
   - Counters should end with `_total`
   - Gauges should describe the value (e.g., `active_connections`)
   - Histograms should end with unit (e.g., `_seconds`, `_bytes`)

2. **Limit Label Cardinality**: Avoid high-cardinality labels
   - ✅ Good: `customer_type` (enterprise, individual)
   - ❌ Bad: `customer_id` (millions of unique values)

3. **Add Help Text**: Document what the metric measures

4. **Group Related Metrics**: Use consistent naming prefixes
   - `trading_volume_spikes_total`
   - `trading_price_alerts_total`
   - `trading_risk_alerts_total`
```

#### 6.2 API Reference Documentation

**New File**: `docs/api/metrics-annotations-reference.md` (~150 LOC)

```markdown
# Metrics Annotation API Reference

## Annotation Directives

### @metric: <name>
**Required**. The name of the Prometheus metric.

- Must follow Prometheus naming rules: `[a-zA-Z_:][a-zA-Z0-9_:]*`
- Automatically prefixed with `velo_` if not present
- Should end with `_total` for counters

**Example**: `@metric: trading_events_total`

### @metric_type: counter|gauge|histogram
**Required**. The type of Prometheus metric.

- `counter`: Monotonically increasing value
- `gauge`: Value that can increase or decrease
- `histogram`: Distribution of values across buckets

**Example**: `@metric_type: counter`

### @metric_help: "<description>"
**Optional**. Human-readable description shown in Prometheus.

**Example**: `@metric_help: "Total number of trading events processed"`

### @metric_labels: label1, label2, ...
**Optional**. Comma-separated list of record fields to use as labels.

Labels enable filtering and grouping in Prometheus queries.

**Example**: `@metric_labels: symbol, alert_type`

### @metric_condition: <expression>
**Optional**. SQL expression to filter which records emit metrics.

Only records matching the condition will increment the metric.

**Example**: `@metric_condition: volume > avg_volume * 2.0`

Supported operators:
- Comparison: `>`, `<`, `>=`, `<=`, `=`, `!=`
- Arithmetic: `+`, `-`, `*`, `/`
- Logical: `AND`, `OR`

### @metric_field: <field_name>
**Required for gauges and histograms**. Which field value to measure.

**Example**: `@metric_field: latency_ms`

### @metric_buckets: [value1, value2, ...]
**Optional for histograms**. Histogram bucket boundaries.

Default: `[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]`

**Example**: `@metric_buckets: [0.1, 1.0, 10.0, 100.0, 1000.0]`

### @metric_sample_rate: <0.0-1.0>
**Optional**. Fraction of records to sample (reduce metrics overhead).

Default: `1.0` (100% sampling)

**Example**: `@metric_sample_rate: 0.1` (10% sampling)

## Error Handling

### Parser Errors

**Missing Required Field**:
```
Error: Missing required annotation field 'metric_type'
Location: financial_trading.sql:42
```

**Invalid Metric Type**:
```
Error: Invalid metric type 'average' (must be counter, gauge, or histogram)
Location: financial_trading.sql:43
```

### Runtime Errors

**Missing Label Field**:
```
Warning: Label field 'symbol' not found in record, using empty string
Stream: volume_spikes
Metric: velo_trading_volume_spikes_total
```

**Non-Numeric Gauge Value**:
```
Error: Gauge metric requires numeric field, got String
Stream: price_tracking
Metric: velo_trading_avg_price
Field: price
```
```

#### 6.3 Updated Financial Trading Demo

**Modified File**: `demo/trading/sql/financial_trading.sql` (add annotations)

Add comprehensive metrics annotations to demonstrate the feature:

```sql
-- Add to existing volume_spikes stream
-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_help: "Total number of volume spikes detected"
-- @metric_labels: symbol
-- @metric_condition: volume > hourly_avg_volume * 2.0
CREATE STREAM volume_spikes AS ...

-- Add to existing price_alerts stream
-- @metric: velo_trading_price_alerts_total
-- @metric_type: counter
-- @metric_help: "Total number of price alerts generated"
-- @metric_labels: symbol, alert_type
CREATE STREAM price_alerts AS ...

-- Add to existing risk_alerts stream
-- @metric: velo_trading_risk_alerts_total
-- @metric_type: counter
-- @metric_help: "Total number of risk management alerts"
-- @metric_labels: trader_id, risk_level
CREATE STREAM risk_alerts AS ...
```

**New File**: `demo/trading/README.md` (update documentation)

```markdown
# Financial Trading Demo

## Metrics

This demo showcases SQL-based metrics annotation. The following business metrics
are exported to Prometheus:

- `velo_trading_volume_spikes_total{symbol}` - Volume spike detections
- `velo_trading_price_alerts_total{symbol,alert_type}` - Price alerts
- `velo_trading_risk_alerts_total{trader_id,risk_level}` - Risk alerts

These metrics are defined directly in `sql/financial_trading.sql` using `@metric`
annotations, demonstrating Velostream's declarative observability approach.

View metrics in Grafana: http://localhost:3000/d/velostream-trading
```

#### 6.4 Tutorial and Migration Guide

**New File**: `docs/tutorials/metrics-annotation-tutorial.md` (~100 LOC)

```markdown
# Tutorial: Adding Business Metrics to Your Streams

## Step 1: Identify Business Events

Which business events should be tracked as metrics?

Example: E-commerce order processing
- Order submissions
- Payment failures
- High-value orders
- Cancellations

## Step 2: Choose Metric Types

- **Counter**: Order submissions, payment failures, cancellations
- **Gauge**: Active orders, average order value
- **Histogram**: Order processing latency

## Step 3: Add Annotations

```sql
-- Track order submissions
-- @metric: ecommerce_orders_total
-- @metric_type: counter
-- @metric_labels: status
CREATE STREAM orders AS
SELECT order_id, status, amount FROM order_stream;

-- Track payment failures
-- @metric: ecommerce_payment_failures_total
-- @metric_type: counter
-- @metric_labels: error_code
-- @metric_condition: status = 'FAILED'
CREATE STREAM payment_failures AS
SELECT * FROM orders WHERE status = 'FAILED';
```

## Step 4: Deploy and Verify

```bash
# Deploy SQL with metrics annotations
velo-sql-multi deploy-app --file ecommerce.sql

# Check Prometheus metrics
curl http://localhost:9091/metrics | grep ecommerce

# Create Grafana dashboard
```

## Migration from External Exporters

If you currently use separate metrics exporter services:

**Before** (external exporter):
```python
# separate_exporter.py
consumer = KafkaConsumer('orders')
counter = Counter('orders_total', ['status'])

for msg in consumer:
    counter.labels(status=msg['status']).inc()
```

**After** (SQL annotations):
```sql
-- @metric: orders_total
-- @metric_type: counter
-- @metric_labels: status
CREATE STREAM orders AS SELECT * FROM order_stream;
```

Benefits:
- No separate service to deploy
- Metrics co-located with business logic
- Self-documenting SQL files
```

#### Phase 6 Deliverables

- [ ] User guide (sql-metrics-annotations.md) (~200 LOC)
- [ ] API reference (metrics-annotations-reference.md) (~150 LOC)
- [ ] Updated financial trading demo with annotations
- [ ] Tutorial and migration guide (~100 LOC)
- [ ] Example queries for Grafana dashboards
- [ ] Troubleshooting guide

---

## Complete Documentation Requirements

### User-Facing Documentation

1. **Getting Started Guide** (`docs/user/sql-metrics-annotations.md`)
   - Overview and benefits
   - Quick start example
   - Metric type reference
   - Best practices

2. **API Reference** (`docs/api/metrics-annotations-reference.md`)
   - Complete annotation directive reference
   - Syntax and validation rules
   - Error messages and troubleshooting
   - Advanced usage patterns

3. **Tutorial** (`docs/tutorials/metrics-annotation-tutorial.md`)
   - Step-by-step walkthrough
   - Real-world examples
   - Migration from external exporters
   - Integration with Grafana

### Developer Documentation

4. **Architecture Guide** (`docs/developer/metrics-annotation-architecture.md`)
   - High-level design diagram
   - Component interactions
   - Data flow from SQL → Prometheus
   - Extension points for custom exporters

5. **Implementation Notes** (`docs/developer/metrics-annotation-implementation.md`)
   - Parser implementation details
   - Runtime emission strategy
   - Performance considerations
   - Testing strategy

### Examples and Demos

6. **Financial Trading Demo** (`demo/trading/`)
   - Updated SQL with comprehensive annotations
   - README explaining metrics strategy
   - Grafana dashboard showcasing metrics
   - Health check script validation

7. **Additional Examples** (`examples/metrics-annotations/`)
   - E-commerce order processing
   - IoT sensor data monitoring
   - Web analytics tracking
   - System health monitoring

### API and SDK Documentation

8. **Metrics Provider API** (`docs/api/metrics-provider.md`)
   - `register_annotated_metric()` method
   - `unregister_stream_metrics()` method
   - Error handling patterns
   - Thread safety guarantees

9. **Annotation Parser API** (`docs/api/annotation-parser.md`)
   - `parse_metric_annotations()` function
   - `MetricAnnotation` struct reference
   - Custom annotation extensions
   - Validation rules

### Operations Documentation

10. **Deployment Guide** (`docs/operations/metrics-deployment.md`)
    - Configuration reference
    - Prometheus integration
    - Grafana dashboard setup
    - Troubleshooting common issues

11. **Monitoring Guide** (`docs/operations/metrics-monitoring.md`)
    - Alert rule examples
    - Dashboard templates
    - Performance tuning
    - Cardinality management

### Testing Documentation

12. **Testing Guide** (`tests/README-metrics-annotations.md`)
    - Unit test patterns
    - Integration test setup
    - Performance test benchmarks
    - Example test cases

---

## Example: Financial Trading Demo Integration

### Current State (Without Feature)
```
SQL (financial_trading.sql)
  ↓ produces topics
volume_spikes, price_alerts, risk_alerts
  ↓ requires separate service
metrics_exporter.py (NEW service needed)
  ↓ exposes
Prometheus metrics
  ↓ scraped by
Grafana Dashboard
```

### With Feature (Integrated)
```
SQL (financial_trading.sql with @metric annotations)
  ↓ directly exports
Prometheus metrics (no extra service)
  ↓ scraped by
Grafana Dashboard
```

**Updated SQL**:
```sql
-- demo/trading/sql/financial_trading.sql

-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_help: "Volume spikes detected across all symbols"
-- @metric_labels: symbol
CREATE STREAM volume_spikes AS
SELECT symbol, volume, avg_volume, event_time
FROM market_data_stream
WHERE volume > avg_volume * 2.0
INTO volume_spikes;

-- @metric: velo_trading_price_alerts_total
-- @metric_type: counter
-- @metric_help: "Price alerts generated for unusual movements"
-- @metric_labels: symbol, alert_type
CREATE STREAM price_alerts AS
SELECT symbol, price, alert_type, event_time
FROM market_data_stream
WHERE ...
INTO price_alerts;

-- @metric: velo_trading_active_symbols
-- @metric_type: gauge
-- @metric_help: "Number of actively traded symbols (5min window)"
-- @metric_aggregation: count_distinct
-- @metric_field: symbol
-- @metric_window: 5m
CREATE STREAM symbol_tracking AS
SELECT symbol, event_time
FROM market_data_stream;
```

**Result**: Grafana "Velostream Trading Demo" dashboard immediately works with no additional infrastructure.

## Validation and Testing

### Unit Tests
```rust
// tests/unit/sql/parser/metric_annotations_test.rs

#[test]
fn test_parse_counter_annotation() {
    let sql = r#"
        -- @metric: test_counter_total
        -- @metric_type: counter
        -- @metric_help: "Test counter metric"
        -- @metric_labels: label1, label2
        CREATE STREAM test AS SELECT * FROM source;
    "#;

    let annotations = parse_metric_annotations(sql);
    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations[0].name, "test_counter_total");
    assert_eq!(annotations[0].metric_type, MetricType::Counter);
}

#[test]
fn test_metric_condition_evaluation() {
    let condition = "volume > avg_volume * 2.0";
    let record = create_test_record(vec![
        ("volume", FieldValue::Float(100.0)),
        ("avg_volume", FieldValue::Float(40.0)),
    ]);

    assert!(evaluate_condition(condition, &record).unwrap());
}
```

### Integration Tests
```rust
// tests/integration/metrics/sql_annotations_integration_test.rs

#[tokio::test]
async fn test_counter_metrics_from_sql_annotation() {
    let sql = r#"
        -- @metric: test_events_total
        -- @metric_type: counter
        CREATE STREAM test AS SELECT * FROM source INTO sink;
    "#;

    let job = deploy_job(sql).await.unwrap();

    // Send test records
    send_records(vec![record1, record2, record3]).await;

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify metric exported
    let metrics = fetch_prometheus_metrics("http://localhost:9091/metrics").await;
    assert!(metrics.contains("test_events_total 3"));
}
```

## Migration Path

### Phase 1: Opt-In (v1.0)
- Feature available but optional
- Existing deployments unaffected
- Users can gradually add annotations

### Phase 2: Recommended Practice (v1.1)
- Documentation highlights feature
- Examples updated to use annotations
- Tools provided to migrate existing metrics

### Phase 3: Best Practice (v2.0)
- Default demos use annotations
- IDE plugins provide annotation autocomplete
- Metrics discovery built into CLI tools

## Documentation Requirements

### User Guide
```markdown
# SQL Metrics Annotation Guide

## Overview
Velostream allows you to declare Prometheus metrics directly in your SQL files...

## Quick Start
Add @metric annotations before CREATE STREAM statements...

## Metric Types
- Counter: For counting events
- Gauge: For tracking current values
- Histogram: For analyzing distributions

## Examples
See demos/trading/sql/financial_trading.sql for complete examples.
```

### API Reference
```markdown
# Metric Annotation Syntax Reference

## @metric: <name>
The name of the Prometheus metric (required)

## @metric_type: counter|gauge|histogram
The type of metric (required)

## @metric_help: "<description>"
Human-readable description (optional)

## @metric_labels: label1, label2, ...
Fields to use as metric labels (optional)

## @metric_condition: <expression>
SQL expression to filter which records emit metrics (optional)
```

## Success Metrics

### Adoption
- 80% of demo applications use metric annotations within 6 months
- Community examples showcase SQL-based observability

### Developer Experience
- Reduced time to implement business metrics: 10 minutes → 2 minutes
- Reduced lines of code: 50+ lines (Python exporter) → 5 lines (SQL annotations)
- Faster iteration: No separate service deployment needed

### Competitive Position
- Highlighted as key differentiator vs Apache Flink in marketing materials
- Featured in conference talks and blog posts
- Adopted by enterprise users as "killer feature"

## Risks and Mitigations

### Risk 1: Performance Impact
**Concern**: Evaluating conditions and extracting labels adds overhead

**Mitigation**:
- Compile conditions to optimized expressions during SQL parsing
- Cache label extraction results per batch
- Make metrics emission async to not block stream processing
- Provide `@metric_sample_rate` annotation for high-throughput streams

### Risk 2: Complex Condition Expressions
**Concern**: Users may write invalid or complex conditions

**Mitigation**:
- Validate conditions at SQL parse time
- Provide clear error messages for invalid expressions
- Limit expression complexity (e.g., no nested functions initially)
- Comprehensive documentation with examples

### Risk 3: Cardinality Explosion
**Concern**: Too many label combinations can overwhelm Prometheus

**Mitigation**:
- Warn if label count > 10
- Document cardinality best practices
- Provide `@metric_max_cardinality` annotation
- Built-in label validation (reject high-cardinality fields like UUIDs)

## Future Enhancements

### Phase 2 Features
1. **Composite Metrics**: Derive metrics from multiple streams
2. **Alert Annotations**: Define Prometheus alerts in SQL comments
3. **Dashboard Templates**: Auto-generate Grafana dashboards from annotations
4. **Metrics Discovery API**: REST endpoint listing all SQL-declared metrics

### Phase 3 Features
1. **IDE Integration**: VSCode plugin with metric annotation autocomplete
2. **Metrics Catalog**: Web UI showing all declared metrics across SQL files
3. **Impact Analysis**: Show which dashboards/alerts use each metric
4. **A/B Testing**: Compare metrics before/after SQL changes

---

## FUTURE DIRECTION: AI-Native Streaming Engine

Building on SQL-based metrics annotation, Velostream can evolve into an **AI-native streaming engine** that provides autonomous monitoring, intelligent optimization, and predictive analytics capabilities that fundamentally differentiate it from Apache Flink, Arroyo, and Materialize.

### 1. Intelligent Anomaly Detection on Metrics

**Vision**: Velostream continuously runs its own metrics through built-in streaming ML models for real-time anomaly detection.

**Implementation**:
```sql
-- @metric: velo_trading_price_volatility
-- @metric_type: gauge
-- @metric_field: price_volatility
-- @ai: anomaly_detector                    -- NEW: AI annotation
-- @ai.model: "EWMAOutlier"                  -- Exponential Weighted Moving Average
-- @ai.sensitivity: 3.0                      -- Z-score threshold
-- @ai.emit_stream: "volatility_anomalies"  -- Output anomaly events
CREATE STREAM volatility_tracking AS
SELECT
    symbol,
    STDDEV(price) OVER (
        PARTITION BY symbol
        ORDER BY event_time
        RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
    ) as price_volatility
FROM market_data;

-- Anomalies automatically emitted to this stream
CREATE STREAM volatility_anomalies AS
SELECT stream_name, operator_id, metric_name, metric_value, anomaly_score
FROM system_metrics
WHERE anomaly_score > 3.0;
```

**Detection Capabilities**:
- **Latency Spikes**: Detect sudden operator or partition lag
- **CPU/Memory Saturation**: Identify resource saturation patterns
- **Throughput Degradation**: Flag unexplained throughput drops
- **Watermark Skew**: Detect time alignment issues

**Built-in Models**:
- **Online Z-Score**: Simple statistical outlier detection
- **Robust MAD** (Median Absolute Deviation): Resistant to outliers
- **EWMA** (Exponential Weighted Moving Average): Adaptive thresholding
- **Bayesian Change-Point Detection**: Structural shift identification
- **Isolation Forest**: Multivariate anomaly detection

**Autonomous Capabilities**:
```sql
-- Self-monitoring and self-correcting
-- @ai: adaptive_threshold
-- @ai.action: "adjust_watermark"
-- @ai.target: "watermark_interval"
CREATE STREAM watermark_optimizer AS
SELECT
    stream_name,
    AVG(lag_ms) as avg_lag,
    CASE
        WHEN avg_lag > 1000 THEN 'increase_interval'
        WHEN avg_lag < 100 THEN 'decrease_interval'
    END as recommended_action
FROM stream_lag_metrics
GROUP BY stream_name;
```

**Integration Points**:
- Grafana dashboards with anomaly overlays
- Slack/PagerDuty alerting
- Self-tuning parameter adjustment (watermarks, batch sizes)
- Automatic capacity scaling triggers

**Competitive Advantage**: **Autonomic capabilities** - Velostream becomes self-monitoring and self-correcting, reducing operational overhead and preventing incidents before they impact users.

---

### 2. AI for Query Optimization

**Vision**: LLM-powered "SQL Copilot" that analyzes running queries and provides intelligent optimization recommendations.

**Capabilities**:

#### 2.1 Pipeline Fusion Analysis
```
Copilot Analysis:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔍 Optimization Opportunity Detected

Streams: volume_analysis, price_analysis
Common Pattern: Both partition by symbol and window by 1 minute

💡 Recommendation: Fuse pipelines to reduce Kafka reads by 50%

Suggested SQL:
CREATE STREAM fused_analysis AS
SELECT
    symbol,
    AVG(volume) as avg_volume,
    AVG(price) as avg_price
FROM market_data
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1' MINUTE);

Impact:
  ✅ -50% Kafka consumer overhead
  ✅ -30% memory footprint
  ✅ +40% throughput
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

#### 2.2 Materialized View Reuse
```
Copilot Analysis:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔍 Redundant Computation Detected

Query: risk_alerts (current)
Reusable Table: trader_positions (existing)

💡 Recommendation: Reuse existing materialized view

Performance Impact:
  ✅ Eliminate duplicate aggregation (saves 200ms/batch)
  ✅ Reduce memory by 500MB
  ✅ Use existing state instead of rebuilding
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

#### 2.3 Index and Partitioning Suggestions
```
Copilot Analysis:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔍 Join Performance Issue Detected

Query: SELECT * FROM trades JOIN positions ON trader_id
Join Type: Hash Join
Throughput: 12K records/sec (below 40K target)

💡 Recommendation: Add partitioning by trader_id

Impact:
  ✅ +230% throughput (12K → 40K records/sec)
  ✅ Reduce join latency from 50ms → 15ms
  ✅ Enable partition-wise parallel join

Suggested Kafka Topic Config:
  partitions: 16
  partition.key: trader_id
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

**Implementation Architecture**:
```
┌─────────────────────────────────────────────────────┐
│          Velostream SQL Copilot Engine              │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌──────────────┐    ┌────────────────┐            │
│  │   Query AST  │───▶│  Execution     │            │
│  │   Analyzer   │    │  Plan Metrics  │            │
│  └──────────────┘    └────────────────┘            │
│         │                     │                      │
│         ▼                     ▼                      │
│  ┌─────────────────────────────────────┐            │
│  │  LLM Fine-Tuned on Velostream       │            │
│  │  - Execution plans                   │            │
│  │  - Operator metrics                  │            │
│  │  - Historical optimizations          │            │
│  └─────────────────────────────────────┘            │
│         │                                             │
│         ▼                                             │
│  ┌──────────────────────────────────┐               │
│  │  Recommendations:                 │               │
│  │  • Pipeline fusion                │               │
│  │  • Materialized view reuse        │               │
│  │  • Partitioning optimization      │               │
│  │  • Index suggestions              │               │
│  └──────────────────────────────────┘               │
│         │                                             │
│         ▼                                             │
│  ┌──────────────────────────────────┐               │
│  │  Output:                          │               │
│  │  • REPL interactive suggestions   │               │
│  │  • Grafana dashboard integration  │               │
│  │  • CLI optimization reports       │               │
│  └──────────────────────────────────┘               │
└─────────────────────────────────────────────────────┘
```

**User Experience**:
```bash
$ velo-cli optimize --analyze financial_trading.sql

🤖 Velostream SQL Copilot - Optimization Analysis
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Analyzing 8 streams across 450 lines of SQL...

✅ 3 optimization opportunities found

1. Pipeline Fusion (HIGH IMPACT)
   Streams: volume_analysis + price_analysis
   Savings: 50% Kafka reads, 30% memory

2. Materialized View Reuse (MEDIUM IMPACT)
   Query: risk_alerts
   Reuse: trader_positions table
   Savings: 200ms/batch, 500MB memory

3. Partitioning Optimization (HIGH IMPACT)
   Join: trades ⋈ positions
   Throughput: +230% (12K → 40K records/sec)

Apply optimizations? [Y/n]
```

**Competitive Advantage**: **"Copilot for Stream SQL"** - Provides high-level productivity and efficiency that even Flink and Materialize lack, appealing to both data engineers and infrastructure teams.

---

### 3. AI-Enhanced Trace Analysis

**Vision**: AI-powered analysis of OpenTelemetry traces to produce human-friendly explanations and root cause identification.

**Capabilities**:

#### 3.1 Automatic Trace Summarization
```
AI Trace Analysis for Job: financial_trading
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔍 Latency Breakdown:
  • 90% of latency originates in JOIN operator (market_data ⋈ positions)
  • Root cause: Partition skew on trader_id='TRADER_123' (800ms vs 50ms avg)
  • Recommendation: Rebalance partitions or add secondary partition key

🔍 Bottleneck Detection:
  • Operator: price_alerts_agg
  • Issue: Intermittent stalls due to high watermark skew
  • Event time lag: 2.3s (normal: 200ms)
  • Recommendation: Increase watermark interval or reduce batch size

🔍 Resource Utilization:
  • CPU: 75% avg, 95% peak (operator: volume_aggregation)
  • Memory: 4.2GB / 8GB (53% utilized)
  • Recommendation: Increase CPU allocation for volume_aggregation operator
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

#### 3.2 Grafana Integration
```yaml
# Grafana dashboard panel configuration
panels:
  - title: "AI Trace Insights"
    type: "text"
    datasource: "Velostream-AI"
    query: |
      SELECT ai_trace_summary(trace_id)
      FROM traces
      WHERE job_name = 'financial_trading'
      ORDER BY timestamp DESC
      LIMIT 1
```

**Output**:
```
Latest AI Insights (updated 30s ago):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️  Performance Degradation Detected

JOIN operator experiencing 3x slowdown
Affected: market_data ⋈ trader_positions
Duration: Last 5 minutes

Root Cause Analysis:
  1. Partition imbalance on trader_id
  2. Hot partition: TRADER_123 (15K msgs/sec)
  3. Cold partitions: avg 2K msgs/sec

Recommended Actions:
  ✓ Rebalance Kafka topic partitions
  ✓ Add composite partition key (trader_id, symbol)
  ✓ Increase parallelism for JOIN operator
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

#### 3.3 CLI Trace Explorer
```bash
$ velo-cli trace analyze --job financial_trading --last 10m

🤖 AI-Powered Trace Analysis
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Analyzed 1,247 traces (10 minutes)

🔍 Top Performance Issues:
  1. JOIN operator latency spike (90th %ile: 800ms → 50ms normal)
     └─ Partition skew detected on trader_id

  2. Watermark propagation delay (2.3s lag)
     └─ price_alerts_agg operator stalling

  3. CPU saturation (95% peak on volume_aggregation)
     └─ Recommend: Increase CPU allocation

🎯 Recommended Fixes (priority order):
  1. HIGH: Rebalance Kafka partitions (impact: -75% latency)
  2. MED:  Adjust watermark interval +500ms (impact: -50% stalls)
  3. LOW:  CPU allocation +2 cores (impact: -20% peak satency)

Apply fixes automatically? [y/N]
```

**Competitive Advantage**: **Enterprise-grade trace intelligence** - Reduces mean-time-to-resolution (MTTR) for production issues from hours to minutes.

---

### 4. Predictive Capacity Planning

**Vision**: Train lightweight regression models on historical metrics (from ClickHouse) to forecast resource demand and prevent capacity issues.

**Forecasting Capabilities**:

#### 4.1 Throughput Demand Prediction
```sql
-- Train forecasting model on historical throughput data
CREATE FORECAST MODEL throughput_forecast AS
SELECT
    timestamp,
    stream_name,
    throughput_records_per_sec
FROM metrics_history
WHERE timestamp > NOW() - INTERVAL '30' DAY
MODEL 'ARIMA(7,1,1)';  -- 7-day seasonality

-- Predict next 24 hours
SELECT
    timestamp,
    stream_name,
    predicted_throughput,
    confidence_interval_95
FROM FORECAST(throughput_forecast, INTERVAL '24' HOUR);
```

**Output**:
```
Throughput Forecast (Next 24 Hours):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Stream: market_data_stream

Current:    42K records/sec
Peak (15h): 85K records/sec (⚠️ +102% from current)
Confidence: 95% CI [78K - 92K]

⚠️  Capacity Alert:
    Current capacity: 60K records/sec
    Predicted peak:   85K records/sec
    Shortfall:        25K records/sec (-42%)

🎯 Recommended Actions:
    1. Scale up 2 additional partitions (+30K capacity)
    2. Schedule scale-up for 14:30 (30min before peak)
    3. Estimated cost: $12/hour during peak
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

#### 4.2 Stream Lag Accumulation Prediction
```sql
-- Detect lag accumulation trends
CREATE FORECAST MODEL lag_forecast AS
SELECT
    timestamp,
    stream_name,
    consumer_lag_seconds
FROM metrics_history
WHERE stream_name = 'market_data_stream'
MODEL 'LinearRegression';

-- Alert if lag will exceed threshold in next hour
SELECT
    stream_name,
    predicted_lag_1h,
    CASE
        WHEN predicted_lag_1h > 300 THEN 'CRITICAL'
        WHEN predicted_lag_1h > 120 THEN 'WARNING'
        ELSE 'OK'
    END as alert_level
FROM FORECAST(lag_forecast, INTERVAL '1' HOUR);
```

#### 4.3 Resource Usage Forecasting
```sql
-- Forecast CPU and memory usage
CREATE FORECAST MODEL resource_forecast AS
SELECT
    timestamp,
    operator_name,
    cpu_percent,
    memory_mb
FROM metrics_history
WHERE timestamp > NOW() - INTERVAL '7' DAY
MODEL 'Prophet';  -- Facebook Prophet for multiple seasonality

-- Predict resource saturation
SELECT
    operator_name,
    predicted_cpu_percent,
    predicted_memory_mb,
    CASE
        WHEN predicted_cpu_percent > 90 THEN 'Scale CPU'
        WHEN predicted_memory_mb > 7500 THEN 'Scale Memory'
        ELSE 'No Action'
    END as recommendation
FROM FORECAST(resource_forecast, INTERVAL '2' HOUR);
```

**DevOps Integration**:
```yaml
# Kubernetes HPA integration
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: velostream-predictive-scaler
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: velostream-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: External
    external:
      metric:
        name: velostream_predicted_throughput
        selector:
          matchLabels:
            stream: market_data
      target:
        type: Value
        value: "60000"  # Scale when predicted > 60K records/sec
```

**Competitive Advantage**: **Proactive capacity management** - Scale infrastructure ahead of demand, preventing incidents and optimizing cloud costs.

---

### 5. Declarative AI Hooks in SQL

**Vision**: Just like `@metric` annotations, introduce `@ai` annotations for declarative ML integration directly in SQL.

**Annotation Syntax**:
```sql
-- @ai: anomaly_detector
-- @ai.model: "EWMAOutlier"
-- @ai.target: "price_volatility"
-- @ai.threshold: 3.0
-- @ai.emit_stream: "volatility_anomalies"
CREATE STREAM volatility_monitoring AS
SELECT
    symbol,
    STDDEV(price) OVER (
        PARTITION BY symbol
        ORDER BY event_time
        RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
    ) as price_volatility
FROM market_data;

-- Anomalies automatically emitted
CREATE STREAM volatility_anomalies AS
SELECT
    symbol,
    price_volatility,
    anomaly_score,
    anomaly_type,
    timestamp
FROM AI_ANOMALIES('volatility_monitoring');
```

**Built-in AI Models**:

| Model | Use Case | Annotation |
|-------|----------|------------|
| **EWMAOutlier** | Simple statistical anomalies | `@ai.model: "EWMAOutlier"` |
| **IsolationForest** | Multivariate anomalies | `@ai.model: "IsolationForest"` |
| **ARIMA** | Time series forecasting | `@ai.model: "ARIMA(7,1,1)"` |
| **Prophet** | Multiple seasonality forecasting | `@ai.model: "Prophet"` |
| **Kalman Filter** | Adaptive signal filtering | `@ai.model: "KalmanFilter"` |
| **Change Point Detection** | Structural shifts | `@ai.model: "Bayesian ChangePoint"` |

**Runtime Behavior**:
```rust
// Velostream runtime automatically:
// 1. Trains lightweight online model
// 2. Continuously evaluates model on new data
// 3. Emits anomaly events to side stream
// 4. Updates model parameters adaptively

impl AIAnnotationProcessor {
    fn process_batch(&mut self, batch: RecordBatch) -> Result<(), ProcessingError> {
        for annotation in &self.ai_annotations {
            match annotation.model {
                "EWMAOutlier" => {
                    let anomalies = self.ewma_detector.detect(batch, annotation)?;
                    self.emit_to_stream(&annotation.emit_stream, anomalies)?;
                }
                "IsolationForest" => {
                    let anomalies = self.isolation_forest.detect(batch, annotation)?;
                    self.emit_to_stream(&annotation.emit_stream, anomalies)?;
                }
                // ... other models
            }
        }
        Ok(())
    }
}
```

**Example Use Cases**:

#### Fraud Detection
```sql
-- @ai: anomaly_detector
-- @ai.model: "IsolationForest"
-- @ai.features: "transaction_amount, merchant_id, user_id, time_of_day"
-- @ai.contamination: 0.01
-- @ai.emit_stream: "fraud_alerts"
CREATE STREAM transaction_monitoring AS
SELECT
    transaction_id,
    transaction_amount,
    merchant_id,
    user_id,
    EXTRACT(HOUR FROM timestamp) as time_of_day
FROM transactions;
```

#### Predictive Maintenance
```sql
-- @ai: forecasting
-- @ai.model: "ARIMA(1,1,1)"
-- @ai.target: "sensor_temperature"
-- @ai.horizon: "2 hours"
-- @ai.emit_stream: "maintenance_alerts"
CREATE STREAM equipment_monitoring AS
SELECT
    equipment_id,
    sensor_temperature,
    sensor_vibration,
    timestamp
FROM iot_sensors;
```

#### Traffic Spike Prediction
```sql
-- @ai: forecasting
-- @ai.model: "Prophet"
-- @ai.target: "request_rate"
-- @ai.horizon: "30 minutes"
-- @ai.emit_stream: "scaling_triggers"
CREATE STREAM traffic_forecasting AS
SELECT
    endpoint,
    COUNT(*) OVER (
        PARTITION BY endpoint
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
    ) as request_rate
FROM api_requests;
```

**Competitive Advantage**: **Stream-native AI operators** - Developers can use AI declaratively in SQL without writing separate Python/Java ML code. No other streaming engine offers this.

---

## Strategic Positioning: "Velostream — The AI-Native Streaming Engine"

### Market Differentiation

| Feature | Velostream | Apache Flink | Arroyo | Materialize |
|---------|------------|--------------|--------|-------------|
| **SQL-Driven Observability** | ✅ Declarative `@metric` | ❌ Requires Java code | ❌ Limited | ❌ Basic metrics |
| **Built-in Anomaly Detection** | ✅ `@ai` annotations | ❌ External tools | ❌ Not supported | ❌ Not supported |
| **Predictive Capacity Planning** | ✅ Time series forecasting | ❌ Manual analysis | ❌ Not supported | ❌ Not supported |
| **AI-Powered Query Optimization** | ✅ SQL Copilot | ❌ Manual tuning | ❌ Manual tuning | ❌ Basic optimizer |
| **Autonomous Self-Healing** | ✅ Adaptive thresholds | ❌ Not supported | ❌ Not supported | ❌ Not supported |
| **OpenTelemetry AI Analysis** | ✅ Trace summarization | ❌ External tools | ❌ Not supported | ❌ Not supported |

### Value Proposition

**For Data Engineers**:
- **10x Productivity**: Declarative AI in SQL vs writing separate ML pipelines
- **Self-Documenting**: Metrics, AI models, and business logic co-located
- **Faster Iteration**: No deployment of separate services

**For DevOps/SRE Teams**:
- **Proactive Monitoring**: Predict issues before they impact users
- **Autonomous Operation**: Self-tuning parameters reduce manual intervention
- **Cost Optimization**: Predictive scaling reduces over-provisioning

**For Enterprise Users**:
- **Reduced MTTR**: AI-powered root cause analysis
- **Compliance**: Audit trail of model decisions in SQL
- **Vendor Lock-in Avoidance**: Standard SQL syntax, not proprietary APIs

### Marketing Messaging

> **"Velostream — The AI-Native Streaming Engine for Real-Time Analytics, Observability, and Automation"**

**Key Messages**:
1. **Declarative AI**: Machine learning as first-class SQL feature
2. **Autonomous Operation**: Self-monitoring, self-correcting, self-optimizing
3. **Enterprise Intelligence**: Predictive alerting and capacity planning
4. **Developer Productivity**: 10x faster implementation vs traditional approaches

### Roadmap Integration

**Phase 1** (Current): SQL Metrics Annotation (FR-073)
- Foundation for declarative observability

**Phase 2** (6 months): AI-Enhanced Observability
- Anomaly detection on metrics
- AI-powered trace analysis
- SQL Copilot prototype

**Phase 3** (12 months): Predictive Intelligence
- Time series forecasting
- Capacity planning automation
- Auto-scaling integration

**Phase 4** (18 months): Autonomous Streaming
- Self-tuning parameters
- Automatic query optimization
- Full autonomic computing capabilities

---

## Conclusion

SQL-based metrics annotation provides Velostream with a significant competitive advantage over Apache Flink by making observability a first-class, declarative part of stream processing. This feature aligns with modern DevOps practices of co-locating code with observability declarations and dramatically simplifies the path from SQL query to production dashboard.

**Recommended Priority**: HIGH - This is a market differentiator that can drive adoption.

**Estimated Effort**: 7 weeks for full implementation, 2 weeks for MVP (counter metrics only)

**Dependencies**:
- Existing ObservabilityManager infrastructure (✅ completed)
- Prometheus metrics export (✅ working)
- SQL parser (✅ stable)

**Next Steps**:
1. Review and approve RFC
2. Create detailed implementation plan
3. Prototype counter metrics annotation (Week 1-2)
4. Demo to stakeholders for feedback
5. Full implementation (Week 3-7)
