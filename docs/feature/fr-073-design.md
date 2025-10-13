# FR-073: SQL-Native Observability Design

## Overview

Enable comprehensive observability in VeloStream through SQL-native metric annotations.

**Core Concept**: Declarative Prometheus metrics defined directly in SQL comments using `@metric` annotations, eliminating the need for separate metrics exporter services.

**Competitive Advantage**:
- **vs Apache Flink**: No separate Java/Scala code required for metrics
- **vs Arroyo/Materialize**: SQL-native observability with built-in validation
- **Unique Value**: Self-documenting pipelines where metrics live alongside business logic

---

## Current State

### System Metrics (✅ Implemented)
VeloStream exports system-level metrics:
- `velo_streaming_operations_total{operation="deserialization|serialization|sql_processing"}`
- `velo_sql_records_processed_total`
- `velo_sql_queries_total`
- `process_cpu_seconds_total`
- `process_resident_memory_bytes`

These metrics are exposed on port 9091 and scraped by Prometheus.

### Business Metrics (❌ Gap)
Application-specific business metrics currently require:
- Separate external services consuming Kafka topics
- Custom Python/Rust code to count events
- Additional infrastructure (metrics exporters)
- No co-location with business logic

**Example**: The financial trading demo generates `volume_spikes`, `price_alerts`, and `risk_alerts` topics, but these events are not available as Prometheus metrics without building a separate exporter service.

---

## Proposed Solution: SQL Metric Annotations

### Annotation Syntax

Metric annotations are declared in SQL comments before `CREATE STREAM` statements:

```sql
-- @metric: <metric_name>
-- @metric_type: counter|gauge|histogram
-- @metric_help: "description"
-- @metric_labels: label1, label2
-- @metric_condition: <sql_expression>
-- @metric_sample_rate: <0.0-1.0>
-- @metric_field: <field_name>
-- @metric_buckets: [v1, v2, ...]
```

### Counter Metrics (Event Counting)

Track the total number of events matching a condition:

```sql
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
```

**Prometheus Output**:
```
velo_trading_volume_spikes_total{symbol="AAPL", spike_ratio="2.5"} 142
velo_trading_volume_spikes_total{symbol="GOOGL", spike_ratio="3.2"} 87
```

### Gauge Metrics (State Tracking)

Track current state values over time:

```sql
-- @metric: velo_trading_active_symbols
-- @metric_type: gauge
-- @metric_help: "Number of actively traded symbols"
-- @metric_field: symbol
CREATE STREAM active_symbols AS
SELECT DISTINCT symbol, event_time
FROM market_data_stream;

-- @metric: velo_trading_avg_position_size
-- @metric_type: gauge
-- @metric_help: "Average position size across all active traders"
-- @metric_field: position_value
-- @metric_labels: trader_type
CREATE STREAM position_tracking AS
SELECT trader_id, trader_type, position_value, event_time
FROM trading_positions;
```

### Histogram Metrics (Distribution Analysis)

Track value distributions across buckets:

```sql
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

---

## Annotation Specifications

### Required Fields

| Annotation | Counter | Gauge | Histogram |
|------------|---------|-------|-----------|
| `@metric` | ✅ Required | ✅ Required | ✅ Required |
| `@metric_type` | ✅ Required | ✅ Required | ✅ Required |
| `@metric_field` | ❌ N/A | ✅ Required | ✅ Required |

### Optional Fields

| Annotation | Description | Default | Valid For |
|------------|-------------|---------|-----------|
| `@metric_help` | Human-readable description | Empty | All types |
| `@metric_labels` | Comma-separated field names to use as labels | None | All types |
| `@metric_condition` | SQL expression to filter which records emit metrics | `true` | All types |
| `@metric_sample_rate` | Sampling rate (0.0 to 1.0) for high-volume streams | `1.0` | All types |
| `@metric_buckets` | Histogram bucket boundaries | Standard buckets | Histogram only |

### Validation Rules

**Metric Naming** (Prometheus standard):
- Must match pattern: `[a-zA-Z_:][a-zA-Z0-9_:]*`
- Must not start with a number
- Only alphanumeric, underscore, and colon characters allowed

**Sample Rate**:
- Must be between 0.0 and 1.0 (exclusive of 0.0, inclusive of 1.0)
- `0.1` = 10% sampling, `1.0` = 100% sampling

**Field Names**:
- Must reference actual fields in the SELECT clause
- Required for gauge and histogram types

---

## Benefits

### 1. Competitive Edge Over Apache Flink

**Flink Approach** (Requires separate Java/Scala code):
```java
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
```

**VeloStream Approach** (Pure SQL):
```sql
-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_condition: volume > avg_volume * 2.0
CREATE STREAM volume_spikes AS
SELECT * FROM trades WHERE volume > avg_volume * 2.0;
```

**Advantages**:
- ✅ No separate code required
- ✅ Metrics defined with business logic
- ✅ Self-documenting
- ✅ Easier to maintain and modify
- ✅ Lower barrier to entry

### 2. Self-Documenting Pipelines

SQL files become comprehensive documentation:
- What business events are detected
- What metrics are exported
- How metrics map to business logic
- What labels are available for analysis

### 3. Integrated Development Workflow

Single-context development:
1. Write SQL to detect business events
2. Add `@metric` annotations for observability
3. Deploy SQL file - metrics automatically available
4. Build Grafana dashboards using declared metrics

No need to:
- Write separate metrics exporter services
- Maintain mapping between topics and metrics
- Deploy additional infrastructure
- Context-switch between SQL and application code

### 4. Version Control Integration

Metrics declarations are versioned alongside SQL logic:

```diff
 -- @metric: velo_trading_volume_spikes_total
 -- @metric_type: counter
+-- @metric_labels: symbol, spike_ratio, exchange
--- @metric_condition: volume > avg_volume * 2.0
+-- @metric_condition: volume > avg_volume * 3.0
```

Git diff shows both business logic AND observability impact.

### 5. Production Operations Benefits

**Alerting**:
```yaml
groups:
  - name: trading_alerts
    rules:
      - alert: HighVolumeSpikeRate
        expr: rate(velo_trading_volume_spikes_total[5m]) > 10
        annotations:
          summary: "Unusual volume spike activity"
```

**Dashboard Discovery**:
- Grafana queries Prometheus for all `velo_trading_*` metrics
- Auto-generate dashboard templates
- Metrics include help text from annotations

---

## Architecture

### Metric Lifecycle

1. **Parse Time**: Annotations extracted from SQL comments during parsing
2. **Registration Time**: Metrics registered with Prometheus registry when stream is deployed
3. **Runtime**: Metrics incremented/updated as records flow through the stream
4. **Cleanup**: Metrics unregistered when stream is dropped

### Integration Points

```
┌─────────────────────────────────────────────────────────────┐
│                    SQL Parser                                │
│  - Extract @metric annotations from comments                 │
│  - Validate annotation syntax                                │
│  - Attach to StreamingQuery AST                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                 Stream Job Server                            │
│  - Register metrics with Prometheus on stream deployment     │
│  - Pass annotations to processor                             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Stream Processor (Runtime)                      │
│  - Evaluate conditions on each record                        │
│  - Extract label values from record fields                   │
│  - Increment counters / observe values                       │
│  - Apply sampling if configured                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│               Prometheus Exporter                            │
│  - Expose metrics on /metrics endpoint (port 9091)          │
│  - Scraped by Prometheus                                     │
└─────────────────────────────────────────────────────────────┘
```

---

## Success Criteria

### Functional Requirements

- ✅ Parse and validate metric annotations from SQL comments
- ✅ Support counter, gauge, and histogram metric types
- ✅ Support metric labels extracted from record fields
- ✅ Support conditional metric emission
- ✅ Support sampling for high-volume streams
- ✅ Automatic metric registration/deregistration
- ✅ Prometheus-compatible metric naming

### Non-Functional Requirements

- **Performance**: Metric emission must add < 5% overhead to record processing
- **Reliability**: Invalid annotations must produce clear error messages
- **Compatibility**: Zero breaking changes to existing SQL syntax
- **Maintainability**: Metrics defined in SQL, not scattered across codebase

---

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

---

## Future Enhancements

### Multi-Backend Export
```sql
-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @export: clickhouse, prometheus  -- Export to multiple backends
```

### Distributed Tracing
```sql
-- @metric: trade_imbalance_total
-- @trace: enabled                  -- Enable OpenTelemetry tracing
-- @trace_sample_rate: 0.05
```

### Advanced Aggregations
```sql
-- @metric: velo_trading_avg_position_size
-- @metric_type: gauge
-- @metric_aggregation: avg
-- @metric_window: 5m
```

---

## References

- Prometheus Naming Best Practices: https://prometheus.io/docs/practices/naming/
- Prometheus Metric Types: https://prometheus.io/docs/concepts/metric_types/
- OpenTelemetry Specification: https://opentelemetry.io/docs/specs/otel/
