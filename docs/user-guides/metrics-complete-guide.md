# Velostream Metrics: Complete Guide

## Table of Contents

- [Overview](#overview)
- [Metric Types](#metric-types)
- [Architecture](#architecture)
- [Validation and Safeguards](#validation-and-safeguards)
- [Annotations Reference](#annotations-reference)
- [Complete Examples](#complete-examples)
- [Limitations and Best Practices](#limitations-and-best-practices)
- [Prometheus Configuration](#prometheus-configuration)
- [Troubleshooting](#troubleshooting)

---

## Overview

Velostream provides **SQL-native metrics** that emit Prometheus-compatible metrics directly from streaming queries. Metrics are defined inline with SQL using `@metric` annotations and automatically exported to Prometheus.

### Key Features

- ✅ **Three metric types**: Counter, Gauge, Histogram
- ✅ **Event-time timestamps**: Metrics use `_event_time` for accurate time-series data
- ✅ **Watermark-based emission**: Guarantees monotonic timestamps (no out-of-order metrics)
- ✅ **Cardinality protection**: Configurable limits prevent OOM from high-cardinality labels
- ✅ **Automatic validation**: Age limits, negative value detection, missing field warnings
- ✅ **Zero-copy labels**: Extracts labels directly from stream records

---

## Metric Types

### Counter

**Purpose**: Track cumulative totals that only increase (e.g., total orders, total errors).

**Behavior**:
- Starts at 0
- Increments on each matching record
- **Never decreases** (Prometheus requirement)
- Resets to 0 on job restart

**Prometheus Representation**: Single value per label combination

**SQL Example**:
```sql
-- Count all processed records
-- @metric: orders_processed_total
-- @metric_type: counter
-- @metric_help: Total number of orders processed
-- @metric_labels: symbol, exchange
CREATE STREAM orders AS
SELECT symbol, exchange, price FROM in_orders;
```

**How it works**:
- Each output record increments the counter by 1
- If `@metric_field` specified, increments by field value
- Cumulative across all records with same labels

**Use Cases**:
- Total records processed
- Error counts
- Event counts by type

---

### Counter with Field Value

**Purpose**: Increment counter by a numeric field value (e.g., total volume, total revenue).

**SQL Example**:
```sql
-- Sum trading volume
-- @metric: trading_volume_total
-- @metric_type: counter
-- @metric_field: volume
-- @metric_help: Total trading volume in shares
-- @metric_labels: symbol
CREATE STREAM trades AS
SELECT symbol, volume, price FROM in_trades;
```

**How it works**:
- Extracts `volume` from each record
- Adds `volume` to cumulative counter
- Emits cumulative total with event-time timestamp

**Example Data Flow**:
```
Record 1: {symbol: "AAPL", volume: 100} → counter = 100
Record 2: {symbol: "AAPL", volume: 50}  → counter = 150
Record 3: {symbol: "AAPL", volume: 25}  → counter = 175
```

**Validation**:
- ⚠️ **Negative values rejected** with throttled warning
- ⚠️ **Missing field** logs warning and increments by 1 instead

---

### Gauge

**Purpose**: Track current state that can increase or decrease (e.g., current price, queue depth).

**Behavior**:
- Represents **current value** (not cumulative)
- Can increase or decrease
- Replaces previous value for same labels

**Prometheus Representation**: Current value per label combination

**SQL Example**:
```sql
-- Track current price per symbol
-- @metric: current_price
-- @metric_type: gauge
-- @metric_field: price
-- @metric_help: Current trading price
-- @metric_labels: symbol
CREATE STREAM market_data AS
SELECT symbol, price, volume FROM in_market_data;
```

**How it works**:
- Extracts `price` from each record
- Emits current value (no accumulation)
- Overwrites previous value in Prometheus

**Example Data Flow**:
```
Record 1: {symbol: "AAPL", price: 150.0} → gauge = 150.0
Record 2: {symbol: "AAPL", price: 151.5} → gauge = 151.5 (replaces 150.0)
Record 3: {symbol: "AAPL", price: 149.0} → gauge = 149.0 (replaces 151.5)
```

**Use Cases**:
- Current price/rate
- Queue depth
- Active connections
- Temperature/sensor readings

**⚠️ Requirement**: Must specify `@metric_field`

---

### Histogram

**Purpose**: Track distribution of values across buckets (e.g., latency distribution, order sizes).

**Behavior**:
- Tracks count of observations in each bucket
- Maintains cumulative `_sum` and `_count`
- Buckets are **cumulative** (le="1.0" includes all values ≤ 1.0)

**Prometheus Representation**:
- `{name}_bucket{le="X"}` - Count of observations ≤ X
- `{name}_bucket{le="+Inf"}` - Total count (equals `_count`)
- `{name}_sum` - Sum of all observed values
- `{name}_count` - Total number of observations

**SQL Example**:
```sql
-- Track order latency distribution
-- @metric: order_latency_seconds
-- @metric_type: histogram
-- @metric_field: latency_seconds
-- @metric_buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
-- @metric_help: Order processing latency distribution
-- @metric_labels: exchange
CREATE STREAM order_metrics AS
SELECT order_id, exchange, latency_seconds FROM in_orders;
```

**How it works**:
1. Observes value from `@metric_field`
2. Increments bucket counts for all buckets where `value ≤ bucket`
3. Updates `_sum` (total of all values)
4. Updates `_count` (number of observations)

**Example Data Flow**:
```
Buckets: [0.01, 0.1, 1.0]

Observation 1: 0.005 seconds
  _bucket{le="0.01"}  = 1   (0.005 ≤ 0.01)
  _bucket{le="0.1"}   = 1   (0.005 ≤ 0.1)
  _bucket{le="1.0"}   = 1   (0.005 ≤ 1.0)
  _bucket{le="+Inf"}  = 1
  _sum                = 0.005
  _count              = 1

Observation 2: 0.5 seconds
  _bucket{le="0.01"}  = 1   (0.5 > 0.01, not incremented)
  _bucket{le="0.1"}   = 1   (0.5 > 0.1, not incremented)
  _bucket{le="1.0"}   = 2   (0.5 ≤ 1.0, incremented)
  _bucket{le="+Inf"}  = 2
  _sum                = 0.505 (0.005 + 0.5)
  _count              = 2
```

**Bucket Configuration**:
- **Default buckets** (if not specified):
  ```
  [0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0]
  ```
- **Custom buckets**: Must be in **strictly ascending order** and **positive values**
- **Validation**: Checked at parse time (query fails if invalid)

**Use Cases**:
- Latency distributions
- Request/response sizes
- Processing times
- Value distributions

**⚠️ Requirements**:
- Must specify `@metric_field`
- Buckets must be ascending order
- Buckets must be positive values
- Bucket configuration **cannot be changed** after first initialization

**📊 Prometheus Queries**:
```promql
# P95 latency
histogram_quantile(0.95, rate(order_latency_seconds_bucket[5m]))

# Average latency
rate(order_latency_seconds_sum[5m]) / rate(order_latency_seconds_count[5m])
```

---

## Architecture

### Emission Flow

```
┌─────────────────┐
│  Stream Record  │
│  {symbol: AAPL} │
│  {price: 150.0} │
│  {_event_time}  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────┐
│  Metric Annotation Parser   │
│  - Extract labels           │
│  - Extract field value      │
│  - Check condition          │
└────────┬────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  Validation Layer           │
│  ✓ Timestamp age (≤48h)     │
│  ✓ Negative counter check   │
│  ✓ Missing field check      │
│  ✓ Label sanitization       │
└────────┬────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  Watermark Check            │
│  - Discard late arrivals    │
│  - Ensure monotonic times   │
└────────┬────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  Cardinality Check          │
│  - Enforce max combinations │
│  - Prevent OOM              │
└────────┬────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  Metric State Update        │
│  - Counter: accumulate      │
│  - Gauge: replace           │
│  - Histogram: bucket update │
└────────┬────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  Prometheus Remote-Write    │
│  - Emit with event_time     │
│  - Send to Prometheus       │
└─────────────────────────────┘
```

### Watermark-Based Emission

**Problem**: Event-time data can arrive out of order, but Prometheus requires monotonic timestamps.

**Solution**: Watermark tracking per metric + label combination.

**How it works**:
1. Track last emitted timestamp for each `(metric_name, labels)` combination
2. Discard records with `event_time ≤ last_timestamp` (late arrivals)
3. Only emit metrics with strictly increasing timestamps

**Example**:
```
Symbol: AAPL, Metric: current_price

Event 1: _event_time=1000, price=150.0 → ✅ Emit (first)
Event 2: _event_time=2000, price=151.0 → ✅ Emit (2000 > 1000)
Event 3: _event_time=1500, price=149.0 → ❌ Discard (1500 ≤ 2000, late arrival)
Event 4: _event_time=3000, price=152.0 → ✅ Emit (3000 > 2000)
```

**Benefits**:
- ✅ Guarantees monotonic timestamps
- ✅ No duplicate timestamps
- ✅ Prometheus-compatible time series

**Trade-off**:
- ⚠️ Late arrivals are discarded (logged with throttled warnings)

---

### Label Independence

Each unique label combination has its **own independent watermark**.

**Example**:
```sql
-- @metric: current_price
-- @metric_labels: symbol
```

**Label Combinations**:
- `{symbol="AAPL"}` - Independent watermark
- `{symbol="MSFT"}` - Independent watermark
- `{symbol="GOOGL"}` - Independent watermark

**Timeline**:
```
AAPL: event_time=1000 → ✅ Emit
MSFT: event_time=500  → ✅ Emit (independent watermark)
AAPL: event_time=900  → ❌ Discard (late for AAPL)
MSFT: event_time=600  → ✅ Emit (valid for MSFT)
```

---

## Validation and Safeguards

Velostream implements **5 critical validations** to ensure metric correctness and prevent operational issues.

### 1. Timestamp Age Validation

**Purpose**: Prevent Prometheus remote-write rejection for samples that are too old.

**Configuration**:
```rust
LabelHandlingConfig {
    max_timestamp_age_ms: 48 * 60 * 60 * 1000, // 48 hours (default)
}
```

**Behavior**:
- Metrics with `event_time` older than threshold are discarded
- Throttled warnings logged (1st + every 1000th)

**Why**: Prometheus remote-write typically rejects samples older than ~2 hours. The 48h default is configurable to match your Prometheus setup.

**Warning Example**:
```
WARN Metric 'current_price' timestamp too old: age=172800000ms (max=172800000ms), discarded. Total discarded: 1
```

---

### 2. Metric Name Validation

**Purpose**: Ensure metric names follow Prometheus naming rules.

**Rules**:
- Must start with `[a-zA-Z_:]`
- Must contain only `[a-zA-Z0-9_:]`
- Cannot be empty

**Validation**: Enforced at **parse time** (query fails if invalid)

**Examples**:
```
✅ Valid:   orders_processed_total
✅ Valid:   http_requests_total
✅ Valid:   api:latency:seconds
❌ Invalid: 123_orders (starts with number)
❌ Invalid: orders-processed (contains hyphen)
❌ Invalid: orders.total (contains dot)
```

---

### 3. Label Value Sanitization

**Purpose**: Escape special characters that break Prometheus exposition format.

**Escaped Characters**:
- `\` → `\\` (backslash)
- `\n` → `\\n` (newline)
- `\r` → `\\r` (carriage return)
- `\t` → `\\t` (tab)
- `"` → `\\"` (double quote)

**Example**:
```
Input:  {description: "Order \"ABC\"\nProcessed"}
Output: {description: "Order \\\"ABC\\\"\\nProcessed"}
```

**Why**: Newlines and quotes in label values break Prometheus text format, causing scrape failures.

---

### 4. Negative Counter Validation

**Purpose**: Counters must be non-negative per Prometheus specification.

**Behavior**:
- Negative values are **rejected**
- Throttled warnings logged
- Processing continues (graceful degradation)

**Warning Example**:
```
WARN Counter metric 'trading_volume_total' has negative value: -100.5. Counters must be non-negative. Labels: ["AAPL"]. Total rejected: 1
```

**Use Case**: Catches data quality issues (e.g., incorrect field mapping).

---

### 5. Missing Field Handling

**Purpose**: Detect typos or missing fields in `@metric_field` annotations.

**Behavior**:
- Missing field logs **warning** (upgraded from debug)
- Counter increments by 1 instead of field value
- Gauge/histogram skipped for that record

**Warning Example**:
```
WARN Job 'market_data': Metric 'current_price' field 'pricee' not found in record. Check that @metric_field references a valid field name. Total missing: 1
```

---

### 6. Cardinality Protection

**Purpose**: Prevent OOM from unbounded label combinations (e.g., `user_id`, `session_id`).

**Configuration**:
```rust
LabelHandlingConfig {
    max_cardinality: 100, // Default: 100 unique label combinations per metric
}
```

**Behavior**:
- Tracks unique `(metric_name, label_values)` combinations
- Rejects new combinations beyond limit
- Throttled warnings logged

**Warning Example**:
```
WARN Counter metric 'requests_total' cardinality limit reached (100 unique label combinations). Discarding new label combination: ["user_9999"]. Total discarded: 1
```

**Recommended Limits**:
| Label Type | Max Cardinality | Example |
|------------|-----------------|---------|
| Low (symbol, exchange) | 100-1,000 | Trading symbols (~5K total) |
| Medium (user_id) | 10,000-100,000 | Active users |
| High (session_id) | ❌ **Avoid** | Unbounded growth → OOM |

**⚠️ Set to 0 to disable** (use with caution - can cause OOM).

---

## Annotations Reference

### Complete Annotation Set

| Annotation | Required | Type | Description | Default |
|------------|----------|------|-------------|---------|
| `@metric` | ✅ Yes | String | Metric name (Prometheus format) | - |
| `@metric_type` | ✅ Yes | Enum | `counter`, `gauge`, or `histogram` | - |
| `@metric_help` | ❌ No | String | Description for Prometheus UI | (empty) |
| `@metric_labels` | ❌ No | List | Comma-separated label names | `[]` |
| `@metric_field` | ⚠️ Conditional | String | Field to extract value from | - |
| `@metric_buckets` | ❌ No | Array | Histogram bucket boundaries | Default buckets |
| `@metric_condition` | ❌ No | Expression | SQL filter condition | Always true |
| `@metric_sample_rate` | ❌ No | Float | Sampling rate (0.0-1.0) | 1.0 (100%) |

### @metric_field Requirements

| Metric Type | @metric_field | Behavior |
|-------------|---------------|----------|
| Counter (without field) | ❌ Not specified | Counts records (increments by 1) |
| Counter (with field) | ✅ Specified | Sums field value (increments by value) |
| Gauge | ✅ **Required** | Emits field value |
| Histogram | ✅ **Required** | Observes field value distribution |

---

## Complete Examples

### Example 1: Basic Counter (Record Count)

```sql
-- Count all processed trades
-- @metric: trades_processed_total
-- @metric_type: counter
-- @metric_help: Total number of trades processed
-- @metric_labels: symbol, exchange
CREATE STREAM trades AS
SELECT
    symbol,
    exchange,
    price,
    volume,
    timestamp as _event_time
FROM in_trades
EMIT CHANGES;
```

**Prometheus Query**:
```promql
# Trades per second by symbol
rate(trades_processed_total{symbol="AAPL"}[1m])

# Total trades across all symbols
sum(trades_processed_total)
```

---

### Example 2: Counter with Field (Volume Tracking)

```sql
-- Track cumulative trading volume
-- @metric: trading_volume_shares_total
-- @metric_type: counter
-- @metric_field: volume
-- @metric_help: Total trading volume in shares
-- @metric_labels: symbol, exchange
CREATE STREAM volume_metrics AS
SELECT
    symbol,
    exchange,
    volume,
    price,
    timestamp as _event_time
FROM in_trades
WHERE volume > 0  -- Filter zero-volume trades
EMIT CHANGES;
```

**Prometheus Query**:
```promql
# Volume per second
rate(trading_volume_shares_total{symbol="AAPL"}[5m])

# Top 10 symbols by volume
topk(10, trading_volume_shares_total)
```

---

### Example 3: Gauge (Current Price)

```sql
-- Track current price per symbol
-- @metric: current_price
-- @metric_type: gauge
-- @metric_field: price
-- @metric_help: Current trading price in USD
-- @metric_labels: symbol, exchange
CREATE STREAM market_data_ts AS
SELECT
    symbol,
    exchange,
    price,
    volume,
    timestamp as _event_time
FROM in_market_data
EMIT CHANGES;
```

**Prometheus Query**:
```promql
# Current price for AAPL
current_price{symbol="AAPL"}

# Price change over 5 minutes
delta(current_price{symbol="AAPL"}[5m])
```

---

### Example 4: Histogram (Latency Distribution)

```sql
-- Track order processing latency
-- @metric: order_processing_latency_seconds
-- @metric_type: histogram
-- @metric_field: latency_seconds
-- @metric_buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
-- @metric_help: Order processing latency distribution
-- @metric_labels: order_type, exchange
CREATE STREAM order_latency AS
SELECT
    order_id,
    order_type,
    exchange,
    (processed_at - received_at) / 1000.0 as latency_seconds,
    processed_at as _event_time
FROM in_orders
EMIT CHANGES;
```

**Prometheus Queries**:
```promql
# P95 latency
histogram_quantile(0.95, rate(order_processing_latency_seconds_bucket[5m]))

# P50 latency (median)
histogram_quantile(0.50, rate(order_processing_latency_seconds_bucket[5m]))

# P99 latency
histogram_quantile(0.99, rate(order_processing_latency_seconds_bucket[5m]))

# Average latency
rate(order_processing_latency_seconds_sum[5m]) / rate(order_processing_latency_seconds_count[5m])

# Total observations
rate(order_processing_latency_seconds_count[5m])
```

---

### Example 5: Conditional Metrics

```sql
-- Count only large orders (>$10,000)
-- @metric: large_orders_total
-- @metric_type: counter
-- @metric_help: Count of orders exceeding $10,000
-- @metric_labels: symbol
-- @metric_condition: total_value > 10000
CREATE STREAM large_orders AS
SELECT
    symbol,
    price * volume as total_value,
    timestamp as _event_time
FROM in_orders
EMIT CHANGES;
```

**How it works**:
- `@metric_condition` evaluates SQL expression per record
- Only records where condition is `true` emit the metric
- Condition is evaluated **after** SELECT (can use computed fields)

---

### Example 6: Sampled Metrics (High Volume)

```sql
-- Sample 10% of tick data for monitoring
-- @metric: tick_sample_total
-- @metric_type: counter
-- @metric_help: Sampled tick count (10% sampling)
-- @metric_labels: symbol
-- @metric_sample_rate: 0.1
CREATE STREAM tick_sample AS
SELECT
    symbol,
    price,
    timestamp as _event_time
FROM in_ticks
EMIT CHANGES;
```

**Why**: For extremely high-volume streams (1M+ records/sec), sampling reduces metric cardinality while maintaining statistical accuracy.

**Formula**: `actual_count ≈ metric_count / sample_rate`

---

### Example 7: Multiple Metrics on Same Stream

```sql
-- Track multiple metrics from single stream
-- @metric: orders_received_total
-- @metric_type: counter
-- @metric_help: Total orders received
-- @metric_labels: symbol, order_type
--
-- @metric: order_value_usd_total
-- @metric_type: counter
-- @metric_field: total_value
-- @metric_help: Total order value in USD
-- @metric_labels: symbol, order_type
--
-- @metric: current_order_size
-- @metric_type: gauge
-- @metric_field: volume
-- @metric_help: Most recent order size
-- @metric_labels: symbol
CREATE STREAM order_metrics AS
SELECT
    symbol,
    order_type,
    price,
    volume,
    price * volume as total_value,
    timestamp as _event_time
FROM in_orders
EMIT CHANGES;
```

**Result**: 3 metrics emitted per record (1 counter, 1 counter with field, 1 gauge).

---

## Limitations and Best Practices

### Limitations

| Limitation | Impact | Workaround |
|------------|--------|------------|
| **Late arrivals discarded** | Out-of-order events dropped | Ensure sources emit near-real-time data |
| **Cardinality limit (100)** | High-cardinality labels rejected | Use low-cardinality labels (symbol, not user_id) |
| **Timestamp age (48h)** | Old events rejected | Configure Prometheus out-of-order window |
| **Histogram buckets fixed** | Cannot change buckets after init | Restart job to reconfigure |
| **Counters reset on restart** | Total lost on job restart | Use Prometheus queries to handle resets |
| **No summary metric type** | Only counter/gauge/histogram | Use histogram with appropriate buckets |

---

### Best Practices

#### 1. Choose Low-Cardinality Labels

**✅ Good** (Low cardinality):
```sql
-- @metric_labels: symbol, exchange, order_type
-- Cardinality: ~5,000 symbols × 10 exchanges × 5 types = 250K combinations
```

**❌ Bad** (High cardinality):
```sql
-- @metric_labels: user_id, session_id
-- Cardinality: Millions of users × billions of sessions = OOM
```

**Rule**: Label cardinality should be < 10,000 combinations per metric.

---

#### 2. Use Appropriate Metric Types

| Use Case | Metric Type | Reason |
|----------|-------------|--------|
| Total events | Counter | Monotonic, cumulative |
| Current state | Gauge | Can increase/decrease |
| Latency/duration | Histogram | Need percentiles (P50, P95, P99) |
| Size distribution | Histogram | Track distribution, not just average |
| Sampled data | Counter with sample_rate | Reduce overhead |

---

#### 3. Name Metrics Following Prometheus Conventions

**Conventions**:
- Use `snake_case`
- Suffix counters with `_total`
- Include units in name (e.g., `_seconds`, `_bytes`)
- No dots or hyphens

**Examples**:
```
✅ orders_processed_total
✅ http_requests_total
✅ request_duration_seconds
✅ response_size_bytes
❌ OrdersProcessed (camelCase)
❌ orders.total (dot)
❌ orders-total (hyphen)
```

---

#### 4. Set Histogram Buckets for Your Use Case

**Latency** (milliseconds → seconds):
```
[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
```

**Order Sizes** (small to large):
```
[10, 50, 100, 500, 1000, 5000, 10000, 50000]
```

**Request Sizes** (bytes → megabytes):
```
[100, 1000, 10000, 100000, 1000000]
```

**Rule**: Cover 90-99% of expected values with ~8-12 buckets.

---

#### 5. Use @metric_condition for Filtering

**Instead of WHERE clause**:
```sql
-- ❌ Bad: Filters entire stream
CREATE STREAM errors AS
SELECT * FROM events
WHERE error_code IS NOT NULL;

-- @metric: errors_total
```

**Use condition**:
```sql
-- ✅ Good: Filters only metric emission
-- @metric: errors_total
-- @metric_condition: error_code IS NOT NULL
CREATE STREAM all_events AS
SELECT * FROM events;
```

**Why**: Processes all events, emits metric only when condition matches.

---

#### 6. Event-Time is Critical

**Always set `_event_time`**:
```sql
SELECT
    symbol,
    price,
    timestamp as _event_time  -- ← Required for accurate metrics
FROM in_market_data;
```

**Why**: Metrics use event-time for timestamps. Without it, processing-time is used (less accurate).

---

## Prometheus Configuration

### Accepting Old Samples (Out-of-Order)

Velostream's default `max_timestamp_age_ms` is 48 hours. Configure Prometheus to accept samples within this window:

**Prometheus 2.39+**:
```bash
prometheus \
  --enable-feature=out-of-order-time-window \
  --storage.tsdb.out-of-order.time-window=48h
```

**Grafana Mimir**:
```yaml
limits:
  out_of_order_time_window: 48h
```

**VictoriaMetrics**: Accepts old samples by default (up to 14 days).

---

### Remote-Write Configuration

**prometheus.yml**:
```yaml
remote_write:
  - url: "http://prometheus:9009/api/v1/write"
    queue_config:
      max_samples_per_send: 1000
      max_shards: 10
    retry_on_http_429: true
```

---

## Troubleshooting

### Issue: Metrics not appearing in Prometheus

**Check**:
1. **Is observability enabled?**
   ```sql
   -- @observability.metrics.enabled: true
   ```

2. **Is remote-write configured?**
   ```bash
   # Check Prometheus config
   curl http://prometheus:9090/api/v1/targets
   ```

3. **Are metrics being emitted?**
   ```bash
   # Check Velostream logs
   grep "push_counter\|push_gauge\|push_histogram" velostream.log
   ```

---

### Issue: "timestamp too old" warnings

**Symptom**:
```
WARN Metric 'current_price' timestamp too old: age=172800000ms
```

**Solutions**:
1. **Check event-time** - Is `_event_time` set correctly?
2. **Increase Velostream threshold**:
   ```rust
   max_timestamp_age_ms: 7 * 24 * 60 * 60 * 1000  // 7 days
   ```
3. **Configure Prometheus** to accept older samples (see above)

---

### Issue: "Late arrival discarded" warnings

**Symptom**:
```
WARN Late arrival discarded for metric 'current_price' (labels: ["AAPL"]): timestamp=1000, last_timestamp=2000, lag=1000ms
```

**Cause**: Event-time is out of order.

**Solutions**:
1. **Fix source ordering** - Ensure source emits events in event-time order
2. **Add watermark** to source:
   ```sql
   WITH (
       'watermark.strategy' = 'bounded_out_of_orderness',
       'watermark.max_out_of_orderness' = '5s'
   )
   ```

---

### Issue: High cardinality warnings

**Symptom**:
```
WARN Counter metric 'requests_total' cardinality limit reached (100 unique label combinations)
```

**Solutions**:
1. **Reduce label cardinality** - Remove high-cardinality labels (user_id, session_id)
2. **Increase limit** (cautiously):
   ```rust
   max_cardinality: 1000  // Be careful - can cause OOM
   ```
3. **Use aggregation** - Pre-aggregate before emitting metrics

---

### Issue: Histogram buckets not matching data

**Symptom**: P95/P99 always show as max bucket value.

**Cause**: Buckets too small for actual data distribution.

**Solution**: Adjust `@metric_buckets` to cover expected range:
```sql
-- Before: [0.001, 0.01, 0.1, 1.0]
-- Data mostly in 5-50 second range

-- After: [0.1, 1.0, 5.0, 10.0, 50.0, 100.0]
-- @metric_buckets: [0.1, 1.0, 5.0, 10.0, 50.0, 100.0]
```

---

### Issue: Negative counter values rejected

**Symptom**:
```
WARN Counter metric 'trading_volume_total' has negative value: -100.5
```

**Cause**: Data quality issue or incorrect field mapping.

**Solutions**:
1. **Check field mapping** - Is `@metric_field` correct?
2. **Filter negative values**:
   ```sql
   -- @metric_condition: volume > 0
   ```
3. **Fix data source** - Investigate why source emits negative values

---

## Summary

### Quick Reference

| Metric Type | Required Annotations | Prometheus Representation |
|-------------|---------------------|---------------------------|
| **Counter** | `@metric`, `@metric_type: counter` | Single value (cumulative) |
| **Counter (field)** | + `@metric_field` | Single value (sum of field) |
| **Gauge** | `@metric`, `@metric_type: gauge`, `@metric_field` | Current value |
| **Histogram** | `@metric`, `@metric_type: histogram`, `@metric_field` | `_bucket`, `_sum`, `_count` |

### Validation Summary

1. ✅ **Timestamp age**: ≤ 48 hours (configurable)
2. ✅ **Metric names**: Prometheus format `[a-zA-Z_:][a-zA-Z0-9_:]*`
3. ✅ **Label sanitization**: Escapes newlines, quotes, etc.
4. ✅ **Negative counters**: Rejected with warnings
5. ✅ **Missing fields**: Warnings with counts
6. ✅ **Cardinality**: Limited to 100 combinations (configurable)

### Architecture Summary

- **Watermarks**: Per (metric, labels) combination
- **Emission**: Event-time based, monotonic timestamps
- **State**: In-memory (reset on restart)
- **Performance**: Lock-free atomics, O(1) lookups

---

## See Also

- [SQL Annotations Reference](sql-annotations.md) - Complete annotation list
- [Prometheus Documentation](https://prometheus.io/docs/concepts/metric_types/) - Official metric types
- [Prometheus Best Practices](https://prometheus.io/docs/practices/naming/) - Naming conventions
