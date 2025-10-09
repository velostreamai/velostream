# Feature Request: SQL-Based Metrics Annotation for Business Observability

## Overview

Enable SQL queries to annotate their outputs with Prometheus metrics declarations, allowing business-level observability to be defined declaratively alongside the SQL logic that generates the events. This creates a self-documenting, integrated approach to both stream processing and observability.

**Competitive Advantage**: This feature provides a significant edge over Apache Flink, which requires separate Java/Scala code for custom metrics. Velostream users can define metrics directly in SQL, making observability a first-class citizen of the streaming pipeline.

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

## Implementation Architecture

### Phase 1: SQL Parser Enhancement (Week 1-2)

**Goal**: Parse `@metric` annotations from SQL comments

```rust
// src/velostream/sql/parser/annotations.rs

#[derive(Debug, Clone)]
pub struct MetricAnnotation {
    pub name: String,                    // velo_trading_volume_spikes_total
    pub metric_type: MetricType,         // counter, gauge, histogram
    pub help: Option<String>,            // Human-readable description
    pub labels: Vec<String>,             // [symbol, spike_ratio]
    pub condition: Option<String>,       // volume > avg_volume * 2.0
    pub aggregation: Option<Aggregation>, // For gauges: count_distinct, avg, sum
    pub field: Option<String>,           // Field to aggregate/measure
    pub window: Option<Duration>,        // Time window for gauges
    pub buckets: Option<Vec<f64>>,      // Bucket boundaries for histograms
}

#[derive(Debug, Clone)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

pub fn parse_metric_annotations(sql_comments: &[String]) -> Vec<MetricAnnotation> {
    // Parse -- @metric: lines and build annotations
}
```

**Integration Point**:
```rust
// src/velostream/sql/parser/mod.rs
pub struct StreamDefinition {
    pub stream_name: String,
    pub query: SelectStatement,
    pub sink_config: SinkConfig,
    pub metric_annotations: Vec<MetricAnnotation>, // NEW
}
```

### Phase 2: Metrics Runtime Integration (Week 3-4)

**Goal**: Emit metrics when records match conditions

```rust
// src/velostream/server/processors/simple.rs

impl SimpleStreamProcessor {
    async fn process_batch(&mut self, batch: RecordBatch) -> Result<(), ProcessingError> {
        // Existing processing...

        // NEW: Check metric annotations for this stream
        for annotation in &self.stream_def.metric_annotations {
            self.emit_metrics_for_annotation(annotation, &batch).await?;
        }

        Ok(())
    }

    fn emit_metrics_for_annotation(
        &self,
        annotation: &MetricAnnotation,
        batch: &RecordBatch
    ) -> Result<(), MetricsError> {
        match annotation.metric_type {
            MetricType::Counter => {
                let count = self.count_matching_records(annotation, batch)?;
                if count > 0 {
                    self.metrics.increment_counter_by(
                        &annotation.name,
                        count,
                        &self.extract_labels(annotation, batch)?
                    );
                }
            }
            MetricType::Gauge => {
                let value = self.compute_gauge_value(annotation, batch)?;
                self.metrics.set_gauge(
                    &annotation.name,
                    value,
                    &self.extract_labels(annotation, batch)?
                );
            }
            MetricType::Histogram => {
                for record in batch.records() {
                    if let Some(value) = self.extract_histogram_value(annotation, record)? {
                        self.metrics.observe_histogram(
                            &annotation.name,
                            value,
                            &self.extract_labels(annotation, record)?
                        );
                    }
                }
            }
        }
        Ok(())
    }
}
```

### Phase 3: Label Extraction (Week 5)

**Goal**: Extract metric labels from record fields

```rust
impl SimpleStreamProcessor {
    fn extract_labels(
        &self,
        annotation: &MetricAnnotation,
        record: &StreamRecord
    ) -> Result<HashMap<String, String>, MetricsError> {
        let mut labels = HashMap::new();

        for label_name in &annotation.labels {
            if let Some(field_value) = record.fields.get(label_name) {
                labels.insert(
                    label_name.clone(),
                    field_value.to_string()
                );
            }
        }

        Ok(labels)
    }
}
```

### Phase 4: Condition Evaluation (Week 6)

**Goal**: Only emit metrics when conditions are met

```rust
impl SimpleStreamProcessor {
    fn evaluate_condition(
        &self,
        condition: &str,
        record: &StreamRecord
    ) -> Result<bool, EvaluationError> {
        // Parse condition expression: "volume > avg_volume * 2.0"
        // Evaluate against record fields
        // Return true if condition matches

        let expr = parse_expression(condition)?;
        expr.evaluate(&record.fields)
    }

    fn count_matching_records(
        &self,
        annotation: &MetricAnnotation,
        batch: &RecordBatch
    ) -> Result<u64, MetricsError> {
        if let Some(condition) = &annotation.condition {
            let count = batch.records()
                .filter(|r| self.evaluate_condition(condition, r).unwrap_or(false))
                .count();
            Ok(count as u64)
        } else {
            // No condition = count all records
            Ok(batch.len() as u64)
        }
    }
}
```

### Phase 5: Metrics Registry Enhancement (Week 7)

**Goal**: Register metrics with proper metadata

```rust
// src/velostream/observability/metrics.rs

impl MetricsProvider {
    pub fn register_annotated_metric(
        &mut self,
        annotation: &MetricAnnotation
    ) -> Result<(), MetricsError> {
        match annotation.metric_type {
            MetricType::Counter => {
                let counter = register_int_counter_vec!(
                    annotation.name.clone(),
                    annotation.help.clone().unwrap_or_default(),
                    &annotation.labels
                )?;
                self.counters.insert(annotation.name.clone(), counter);
            }
            MetricType::Gauge => {
                let gauge = register_gauge_vec!(
                    annotation.name.clone(),
                    annotation.help.clone().unwrap_or_default(),
                    &annotation.labels
                )?;
                self.gauges.insert(annotation.name.clone(), gauge);
            }
            MetricType::Histogram => {
                let buckets = annotation.buckets.clone()
                    .unwrap_or_else(|| vec![0.001, 0.01, 0.1, 1.0, 10.0]);
                let histogram = register_histogram_vec!(
                    annotation.name.clone(),
                    annotation.help.clone().unwrap_or_default(),
                    &annotation.labels,
                    buckets
                )?;
                self.histograms.insert(annotation.name.clone(), histogram);
            }
        }
        Ok(())
    }
}
```

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
