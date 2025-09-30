# RFC: Built-In Outlier Detection Functions

**Tags:** analytics, window-functions, anomaly-detection

## Summary

This RFC proposes adding built-in outlier detection capabilities to Velostream, enabling developers to identify anomalous data points in real-time streaming pipelines with minimal user-defined logic. The goal is to offer a first-class analytical primitive that can be used within SQL queries or the API — similar to AVG, STDDEV, and window functions — but designed for statistical or robust anomaly detection in high-frequency or aggregated streams.

## Motivation

Outlier detection is a foundational capability in time-series and event-stream analytics, especially for:

- **Financial tick data** — detecting price spikes, abnormal spreads, or liquidity shocks
- **IoT sensor streams** — spotting faulty readings or hardware issues in real-time
- **Operational telemetry** — identifying latency or volume anomalies
- **User metrics / monitoring** — alerting on unusual user behaviour patterns

While users can manually implement outlier detection using windowed aggregates (AVG, STDDEV, PERCENTILE_CONT, etc.), this approach is cumbersome, error-prone, and lacks built-in support for stateful detection and thresholding. A declarative syntax or function-level primitive would significantly simplify these use cases.

## Goals

- **Declarative simplicity** — outlier detection should be expressible in SQL or a simple API function.
- **Real-time performance** — detection should operate incrementally over windows without materializing large state.
- **Flexibility** — support different methods (z-score, MAD, quantile).
- **Integration with windowing** — work seamlessly with TUMBLE, HOP, and SESSION windows.
- **Streaming-first** — no need for batch lookbacks or full dataset materialization.

## Non-Goals

- Full ML model integration (e.g. Isolation Forest, DBSCAN) — these may come later via external function hooks.
- Historical retraining — focus is on incremental, stateful detection in live streams.

## Proposed Design

### 1. Function-level Primitive

Introduce a scalar or aggregate function for identifying anomalies:

```sql
SELECT
  ts,
  value,
  IS_OUTLIER(value, method => 'zscore', threshold => 3.0, window => INTERVAL '5' MINUTE) AS is_anomaly
FROM prices
GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE);
```

Alternative inline form:

```sql
WHERE IS_OUTLIER(value, 'zscore', 3.0)
```

Supported methods:

- **zscore** — |x - μ| / σ > threshold
- **mad** — median absolute deviation
- **quantile** — percentile-based
- **ewma** — exponentially weighted moving average deviation

### 2. Integration with Windows

Each call is evaluated within the current aggregation window, using incremental stats (e.g. Welford's algorithm) for performance.

```sql
SELECT
  symbol,
  window_start,
  window_end,
  COUNT(*) AS samples,
  SUM(is_anomaly) AS anomalies
FROM (
  SELECT *,
    IS_OUTLIER(price, 'mad', 3.5) AS is_anomaly
  FROM tick_stream
)
GROUP BY TUMBLE(ts, INTERVAL '1' SECOND), symbol;
```

### 3. Sink Emission (CDC)

Detected anomalies should be emitted downstream as CDC updates or event stream outputs, allowing integration into alerting or dashboard systems:

```sql
CREATE TABLE price_anomalies AS
SELECT *
FROM tick_stream
WHERE IS_OUTLIER(price, 'zscore', 3.0)
EMIT CHANGES;
```

## Implementation Details

- Built on top of existing aggregate state machinery.
- Reuses AVG, STDDEV, and COUNT under the hood.
- Maintains per-key incremental stats for streaming consistency.
- Optionally exposes confidence intervals or z-scores as secondary outputs.

## Performance Considerations

### 1. Complexity per Update

Each record update incurs O(1) time complexity per key:

- **Z-Score:** updates mean and variance incrementally (Welford algorithm).
- **MAD / Quantile:** approximate or sketch-based implementations (t-digest or GK algorithm).
- **EWMA:** O(1) using exponential decay factor α.

This ensures linear scalability with respect to input throughput.

### 2. Memory Usage

Per-key state consists of:

- For Z-score: {count, mean, m2} (constant)
- For MAD: rolling sample window or sketch (~O(log n))
- For EWMA: constant state {avg, variance}

Memory grows linearly with key cardinality, not with event count.
Typical overhead is tens of bytes per key, well below windowed aggregate footprints.

### 3. Window Retention

- Sliding and hopping windows reuse existing retention logic.
- The outlier function should piggyback on the window store to avoid double state.
- Older state should be evicted after watermark advancement or TTL expiry.

### 4. Throughput Targets

Given its constant-time update path, IS_OUTLIER() can operate at:

- 100k–1M events/sec per core (depending on method and key count)
- Minimal GC overhead for constant-memory variants
- Inline vectorization possible if stream batches values (SIMD-friendly ops)

### 5. Backpressure and Lag

If outlier computation triggers downstream filtering or CDC emission, backpressure can arise only from sink latency, not detection cost itself.
Design should keep outlier marking inline with aggregation to minimize reorder delays.

### 6. Accuracy Trade-offs

Approximate methods (MAD, quantile sketches) may introduce small deviations (<1%) for very high-velocity streams but maintain statistical stability for real-world financial and IoT workloads.

### 7. Key Cardinality Scaling

For streams with very high key diversity (e.g. millions of stock symbols or device IDs), the implementation should support:

- LRU eviction of inactive keys
- Optional state compaction or key partitioning via sharding

### 8. Optimization Opportunities

Future optimizations:

- SIMD acceleration of z-score updates
- Vectorized window aggregation
- Mergeable sketches for parallel scaling
- Native Rust implementation leveraging no_std for WASM integration

# Outlier Detection Use Cases

## 🏦 1. Financial Market Tick Data

**Goal:** Detect abnormal price or volume spikes in real time.

**Example Query:**

```sql
CREATE STREAM price_anomalies AS
SELECT
    symbol,
    event_time,
    price,
    IS_OUTLIER(price, 'zscore', 4.0) AS price_outlier,
    IS_OUTLIER(volume, 'mad', 3.5) AS volume_outlier
FROM ticks_stream
WINDOW SLIDING (SIZE 10 SECONDS, ADVANCE BY 1 SECOND)
WHERE price_outlier OR volume_outlier
INTO alerts_sink;
```

**Use Case:**

- Spot flash crashes or pump-and-dump patterns.
- Flag suspicious liquidity spikes in illiquid markets.
- Feed anomaly signals to risk engines or alerting dashboards.

**Business Value:** Early warning system for trading desks, risk managers, and quant teams.

## ⚙️ 2. IoT Sensor Monitoring

**Goal:** Detect out-of-range or failing sensor signals (temperature, vibration, humidity).

**Example Query:**

```sql
CREATE STREAM iot_outliers AS
SELECT
    device_id,
    event_time,
    IS_OUTLIER(temp, 'ewma', 0.2) AS temp_anomaly,
    IS_OUTLIER(vibration, 'zscore', 3.0) AS vibration_anomaly
FROM sensor_readings
WINDOW SLIDING (SIZE 30 SECONDS, ADVANCE BY 5 SECONDS)
WHERE temp_anomaly OR vibration_anomaly
INTO iot_anomalies_sink;
```

**Use Case:**

- Identify early hardware degradation.
- Detect sudden spikes in environmental readings.
- Auto-trigger maintenance alerts or control system interventions.

**Business Value:** Reduce downtime, predict failures before impact.

## 💾 3. Data Pipeline Quality Assurance

**Goal:** Catch malformed or unexpected data spikes during ingestion.

**Example Query:**

```sql
CREATE STREAM data_integrity_monitor AS
SELECT
    dataset,
    source,
    IS_OUTLIER(record_size_bytes, 'mad', 4.0) AS size_anomaly,
    IS_OUTLIER(event_delay_ms, 'zscore', 3.0) AS latency_anomaly
FROM ingestion_metrics
WINDOW TUMBLING (SIZE 5 MINUTES)
WHERE size_anomaly OR latency_anomaly
INTO quality_alerts_sink;
```

**Use Case:**

- Detect sudden schema shifts, payload bloat, or delay spikes.
- Guard against upstream bugs or malformed payloads.

**Business Value:** Improve data reliability before downstream ETL or AI ingestion.

## 🧠 4. AI / ML Model Drift Detection

**Goal:** Identify when live data deviates significantly from training distribution.

**Example Query:**

```sql
CREATE STREAM model_input_monitor AS
SELECT
    feature_name,
    IS_OUTLIER(feature_value, 'zscore', 3.5) AS drift_flag
FROM live_features
WINDOW SLIDING (SIZE 1 HOUR, ADVANCE BY 10 MINUTES)
WHERE drift_flag
INTO model_drift_sink;
```

**Use Case:**

- Detect concept drift in deployed models.
- Trigger model retraining or shadow evaluation.

**Business Value:** Prevent accuracy decay and ensure fair, stable predictions.

## 📈 5. Real-Time KPI Monitoring

**Goal:** Automatically alert when performance or usage metrics deviate unexpectedly.

**Example Query:**

```sql
CREATE STREAM kpi_anomalies AS
SELECT
    service_name,
    IS_OUTLIER(request_latency_ms, 'zscore', 3.0) AS latency_outlier,
    IS_OUTLIER(error_rate, 'mad', 3.5) AS error_outlier
FROM service_metrics
WINDOW SLIDING (SIZE 5 MINUTES, ADVANCE 1 MINUTE)
WHERE latency_outlier OR error_outlier
INTO alerting_sink;
```

**Use Case:**

- Catch degraded service response times.
- Detect error bursts early.
- Feed alerts to PagerDuty, Slack, or Grafana dashboards.

**Business Value:** Enables proactive observability and incident prevention.

## 📡 6. Streaming Fraud Detection

**Goal:** Detect suspicious or extreme events in financial transactions.

**Example Query:**

```sql
CREATE STREAM fraud_alerts AS
SELECT
    user_id,
    region,
    IS_OUTLIER(transaction_amount, 'quantile', 0.99) AS high_value_txn,
    IS_OUTLIER(txn_frequency, 'zscore', 3.0) AS burst_activity
FROM payments_stream
WINDOW SLIDING (SIZE 10 MINUTES, ADVANCE 1 MINUTE)
WHERE high_value_txn OR burst_activity
INTO fraud_alerts_sink;
```

**Use Case:**

- Identify spending bursts, account takeovers, or bot-driven abuse.

**Business Value:** Strengthens AML/KYC pipelines with real-time anomaly logic.

## 🛰️ 7. Edge / Sensor Fusion Scenarios

**Goal:** Combine multiple signal sources and detect joint anomalies.

**Example Query:**

```sql
CREATE STREAM fusion_anomalies AS
SELECT
    device_id,
    IS_OUTLIER(temp, 'mad', 3.5) OR IS_OUTLIER(pressure, 'zscore', 3.0) AS is_abnormal
FROM fused_signals
WINDOW TUMBLING (SIZE 10 SECONDS)
WHERE is_abnormal
INTO fused_sink;
```

**Use Case:**

- Multi-sensor validation (temp + pressure + vibration).
- Ideal for aerospace, energy, or manufacturing telemetry.

**Business Value:** Enables lightweight real-time health checks at the edge.

## 🔍 8. Outlier-Aware Aggregations

**Goal:** Combine outlier detection with dynamic aggregations (e.g., exclude spikes).

**Example Query:**

```sql
CREATE STREAM cleaned_metrics AS
SELECT
    metric,
    AVG(value) FILTER (WHERE NOT IS_OUTLIER(value, 'zscore', 3.0)) AS smoothed_avg
FROM metrics_stream
WINDOW SLIDING (SIZE 1 MINUTE, ADVANCE 10 SECONDS)
INTO cleaned_metrics_sink;
```

**Use Case:**

- Smooth volatile data streams.
- Improve rolling averages by excluding extreme noise.

**Business Value:** Higher signal quality for dashboards and ML pipelines.




# Testing and Validation Plan

This section outlines the testing strategy for verifying correctness, performance, and stability of the IS_OUTLIER() implementation.

## 1. Unit Tests

**Goal:** Ensure correctness of statistical calculations and thresholds.

| Test | Description | Expected Outcome |
|------|-------------|------------------|
| test_zscore_simple | Single-key stream with known mean/variance | Flags values > 3σ as anomalies |
| test_mad_consistency | Sequence with symmetric deviations | Detects 3.5× median deviations correctly |
| test_quantile_bounds | Percentile-based thresholding (95%) | Detects top/bottom 5% values |
| test_ewma_response | Exponentially smoothed average deviation | Adapts smoothly to trending data |
| test_zero_variance | All identical values | No false positives |
| test_null_and_missing_values | NULL or missing input | Skips gracefully without breaking state |
| test_mixed_methods | Combine zscore + MAD in nested queries | Correct flagging per method |
| test_threshold_edge_cases | Threshold = 0 or < 1 | Handles gracefully, no crash |

All tests should run under the `cargo test` or equivalent harness and assert boolean flags, numeric z-scores, or MAD deviations where applicable.

## 2. Integration Tests

**Goal:** Validate streaming behaviour, state retention, and correctness across windows.

| Test | Scenario | Expectation |
|------|----------|-------------|
| integration_sliding_window | Continuous stream of values across sliding windows | Only flag anomalies within each window scope |
| integration_tumble_keyed | Multi-symbol keyed stream | Each key maintains independent detection state |
| integration_late_events | Events arriving after watermark | Late arrivals ignored or appended based on policy |
| integration_restart_recovery | Restart node mid-stream | State restored correctly without false positives |
| integration_high_cardinality | 100k keys in parallel | Memory scales linearly, throughput stable |

Each integration test should emit metrics for:

- Event throughput (eps)
- State size (bytes)
- Anomaly detection accuracy (%)

## 3. Golden Dataset Validation

Use static datasets with known statistical properties for cross-validation:

| Dataset | Description | Expected Behaviour |
|---------|-------------|-------------------|
| normal_dist.csv | Gaussian(μ=100, σ=10) | 0.3% flagged at 3σ |
| bimodal.csv | Two clusters (μ=100, μ=200) | Bimodal anomalies not falsely flagged |
| spike_series.csv | 1% injected spikes | Spikes correctly identified |
| flat_series.csv | Constant signal | No anomalies |
| trend_series.csv | Linear upward drift | EWMA tracks trend, low false positives |

All golden datasets should be versioned in `/tests/data/` and validated against expected outlier ratios.

## 4. Performance and Load Testing

**Goal:** Validate scalability and latency under real workloads.

| Test | Scenario | Metric | Target |
|------|----------|--------|--------|
| perf_single_core | 1M events/sec single key | Throughput | ≥ 800k eps |
| perf_multi_key | 100k keys evenly distributed | Memory overhead | < 10 MB per million keys |
| perf_latency | 10k/sec with hop window 1s | End-to-end latency | < 10ms |
| perf_eviction | Sliding window expiry | State eviction delay | < 1 window size |
| perf_backpressure | Slow sink consumer | No data loss | ✅ |

Tests can use synthetic event generators (velostream-bench or custom Rust harness).

## 5. Correctness under Faults

Simulate failure and recovery scenarios:

- **Node restart mid-window** — Verify checkpoint restoration of aggregates.
- **Partial state loss** — Rebuild from upstream replay (Kafka offsets).
- **Out-of-order events** — Ensure consistent window assignment and anomaly marking.

Metrics to verify:

- Zero false positives after recovery.
- State version matches last committed checkpoint.

## 6. Statistical Accuracy Benchmarks

Compare against Python/Numpy baseline:

```python
import numpy as np

values = np.random.normal(100, 10, 10000)
mean, std = values.mean(), values.std()
z_scores = (values - mean) / std
anomalies = (np.abs(z_scores) > 3.0).sum()
```

Then stream identical data through Velostream and compare anomaly counts.
Target >99.5% accuracy parity.

## 7. End-to-End Pipeline Demo

Create a reference pipeline for financial tick detection:

1. Ingest random tick data (1M/s)
2. Apply `IS_OUTLIER(price, 'zscore', 4.0)`
3. Emit results to `alerts_sink`
4. Verify counts and flag ratio in Grafana

Expected results:

- ~0.3–0.5% of ticks flagged as anomalies
- CPU usage < 50% on 4-core instance
- State remains bounded

## 8. Regression Suite

Each new release should re-run:

- Unit + Integration + Golden + Perf suites
- Compare metrics vs previous release
- Enforce no >2% regression in throughput or accuracy

## 9. Observability Hooks

During test runs:

- Expose metrics: `outliers_detected_total`, `window_state_bytes`, `mean_latency_ms`
- Validate observability integration (`/metrics` endpoint or tracing span)

## 10. Test Environment

| Component | Version |
|-----------|---------|
| Velostream | ≥ 0.5.x |
| Rust | 1.80+ |
| Kafka (optional) | 3.7+ |
| OS | Linux/macOS |
| CI | GitHub Actions / Cargo Test Matrix |

## Future Extensions

- Support for adaptive thresholds (rolling quantile bounds).
- Integration with machine learning models (e.g. Isolation Forest, ONNX).
- Support for multi-variate outlier detection.

## Alternatives Considered

- **User-defined functions (UDFs)** — flexible but lack native state management.
- **External ML pipelines** — complex to maintain and deploy.
- **Static thresholds** — too rigid for dynamic streams.

## Open Questions

- Should detection emit anomalies as boolean flags or filtered event streams?
- Do we want per-key dynamic thresholds (e.g. symbol-by-symbol z-score)?
- Should users be able to tune window size independently of the main aggregation window?

## References

- NIST: Outlier Detection Techniques
- Arroyo aggregate API
- Materialize SQL Functions
- Streaming MAD and Welford's Algorithm
