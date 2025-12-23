# SQL Annotations Reference

Velostream SQL files support annotations in comments that configure job processing, observability, deployment, and metrics. Annotations use the format `-- @annotation_name: value`.

## Table of Contents

- [Job Processing Annotations](#job-processing-annotations)
- [Application Metadata Annotations](#application-metadata-annotations)
- [Deployment Annotations](#deployment-annotations)
- [Observability Annotations](#observability-annotations)
- [Metric Annotations](#metric-annotations)
- [SLA and Governance Annotations](#sla-and-governance-annotations)

---

## Job Processing Annotations

Control how queries are executed by the stream processing engine.

### @job_mode

Selects the job processor architecture.

```sql
-- @job_mode: simple
CREATE STREAM output AS SELECT * FROM input;
```

| Value | Description | Use Case |
|-------|-------------|----------|
| `simple` | Single-threaded, best-effort delivery. Lowest latency, no transaction overhead. | Development, low-volume streams, latency-sensitive workloads |
| `transactional` | Exactly-once semantics with Kafka transactions. Higher latency but guarantees. | Financial data, audit logs, compliance-critical pipelines |
| `adaptive` | Multi-partition parallel processing. Auto-scales based on CPU cores. | High-throughput production workloads |

**Default**: `adaptive` (if not specified)

### @batch_size

Sets the maximum number of records processed per batch.

```sql
-- @job_mode: simple
-- @batch_size: 500
CREATE STREAM output AS SELECT * FROM input;
```

**Default**: 100

### @num_partitions

Sets the number of parallel partitions for adaptive mode.

```sql
-- @job_mode: adaptive
-- @num_partitions: 8
CREATE STREAM output AS SELECT * FROM input;
```

**Default**: Number of CPU cores

### @partitioning_strategy

Controls how records are routed to partitions in adaptive mode.

```sql
-- @job_mode: adaptive
-- @num_partitions: 8
-- @partitioning_strategy: hash
CREATE STREAM output AS SELECT * FROM input;
```

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `sticky` | Routes by source partition (zero-cost affinity) | Default, preserves ordering per partition |
| `hash` | Hash-based routing on key fields | Even distribution, GROUP BY operations |
| `smart` | Adaptive routing based on load | Variable workloads |
| `roundrobin` | Round-robin distribution | Maximum parallelism, no ordering needs |
| `fanin` | All records to single partition | Aggregation, final reduction |

**Default**: `sticky`

---

## Application Metadata Annotations

Document and organize SQL applications.

### @app

Application identifier for grouping related queries.

```sql
-- @app: trading_analytics
-- @version: 1.0.0
-- @description: Real-time trading signal generation
CREATE STREAM signals AS SELECT * FROM market_data;
```

### @version

Semantic version of the query/application.

```sql
-- @version: 2.1.0
```

### @description

Human-readable description.

```sql
-- @description: Aggregates order flow for imbalance detection
```

### @phase

Deployment phase indicator.

```sql
-- @phase: production
```

### @depends_on

Declares dependencies on other applications or streams.

```sql
-- @app: price_analytics
-- @depends_on: app_market_data (market_data_ts)
CREATE STREAM alerts AS SELECT * FROM market_data_ts;
```

---

## Deployment Annotations

Configure deployment context for observability and operations.

### @deployment.node_id

Unique node identifier. Supports environment variable substitution.

```sql
-- @deployment.node_id: prod-trading-cluster-${TRADING_POD_ID:1}
```

### @deployment.node_name

Human-readable node name.

```sql
-- @deployment.node_name: Production Trading Analytics Platform
```

### @deployment.region

Deployment region (e.g., AWS region).

```sql
-- @deployment.region: ${AWS_REGION:us-east-1}
```

**Environment Variable Substitution**:
- Format: `${VAR_NAME:default_value}`
- If `VAR_NAME` exists in environment, uses its value
- Otherwise uses `default_value`

---

## Observability Annotations

Enable metrics, tracing, and profiling.

### @observability.metrics.enabled

Enable Prometheus metrics collection.

```sql
-- @observability.metrics.enabled: true
```

### @observability.tracing.enabled

Enable distributed tracing (OpenTelemetry).

```sql
-- @observability.tracing.enabled: true
```

### @observability.profiling.enabled

Enable CPU profiling with configurable overhead.

```sql
-- @observability.profiling.enabled: dev
```

| Value | Sampling | Overhead | Use Case |
|-------|----------|----------|----------|
| `off` | Disabled | 0% | Production default |
| `dev` | 1000 Hz | 8-10% | Development, debugging |
| `prod` | 50 Hz | 2-3% | Production profiling |

### @observability.error_reporting.enabled

Enable structured error reporting.

```sql
-- @observability.error_reporting.enabled: true
```

---

## Metric Annotations

Define custom Prometheus metrics inline with SQL.

### Basic Counter

```sql
-- @metric: orders_processed_total
-- @metric_type: counter
-- @metric_help: Total number of orders processed
-- @metric_labels: symbol, exchange
CREATE STREAM enriched_orders AS
SELECT symbol, exchange, amount FROM orders;
```

### Gauge with Field

```sql
-- @metric: current_price
-- @metric_type: gauge
-- @metric_help: Current price per symbol
-- @metric_field: price
-- @metric_labels: symbol
CREATE STREAM prices AS SELECT symbol, price FROM market_data;
```

### Histogram with Buckets

```sql
-- @metric: order_latency_seconds
-- @metric_type: histogram
-- @metric_help: Order processing latency
-- @metric_field: latency_ms
-- @metric_buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
CREATE STREAM order_metrics AS SELECT order_id, latency_ms FROM orders;
```

### Metric Annotation Reference

| Annotation | Required | Description |
|------------|----------|-------------|
| `@metric` | Yes | Metric name (Prometheus naming: `snake_case_total`) |
| `@metric_type` | Yes | `counter`, `gauge`, or `histogram` |
| `@metric_help` | No | Description shown in Prometheus |
| `@metric_labels` | No | Comma-separated label names |
| `@metric_field` | For gauge/histogram | Field to extract value from |
| `@metric_buckets` | For histogram | Bucket boundaries as JSON array |
| `@metric_condition` | No | SQL expression to filter when metric fires |
| `@metric_sample_rate` | No | Sampling rate 0.0-1.0 (default: 1.0) |

---

## SLA and Governance Annotations

Document SLAs and compliance requirements.

### @sla.latency.p99

Expected P99 latency target.

```sql
-- @sla.latency.p99: 50ms
```

### @sla.availability

Availability target.

```sql
-- @sla.availability: 99.99%
```

### @data_retention

Data retention policy.

```sql
-- @data_retention: 30d
```

### @compliance

Compliance requirements.

```sql
-- @compliance: SOX, GDPR
```

---

## Complete Example

```sql
-- =============================================================================
-- APP: Market Data Pipeline
-- =============================================================================
-- @app: market_data_pipeline
-- @version: 1.0.0
-- @description: Core market data processing with OHLCV aggregation
-- @phase: production
--
-- @deployment.node_id: trading-${POD_ID:1}
-- @deployment.region: ${AWS_REGION:us-east-1}
--
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: prod
--
-- @sla.latency.p99: 10ms
-- @sla.availability: 99.99%
-- @compliance: SOX
-- =============================================================================

-- @job_mode: simple
-- @batch_size: 1000
--
-- @metric: market_data_records_total
-- @metric_type: counter
-- @metric_help: Total market data records processed
-- @metric_labels: symbol, exchange

CREATE STREAM market_data_ts AS
SELECT
    symbol PRIMARY KEY,
    exchange,
    price,
    volume,
    timestamp as _event_time
FROM in_market_data
EMIT CHANGES
WITH (
    'event.time.field' = 'timestamp',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '5s'
);
```

---

## See Also

- [SQL Grammar Rules](../sql/PARSER_GRAMMAR.md) - SQL syntax reference
- [SQL Copy-Paste Examples](../sql/COPY_PASTE_EXAMPLES.md) - Working query examples
- [SQL Native Observability](sql-native-observability.md) - Detailed observability guide
