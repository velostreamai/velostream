# Velostream Time-Series Roadmap (RFC)

## 1. Overview

This RFC proposes a roadmap for evolving Velostream into a next-generation time-series stream processing engine, capable of handling financial, IoT, and observability workloads with low latency, high expressiveness, and built-in analytical intelligence.

Velostream aims to combine the simplicity of SQL, power of streaming, and time-series awareness found in systems like KDB/Q, RisingWave, and Materialize, while maintaining a single-binary deployment model and Rust-level performance.

## 2. Motivation

Traditional stream processors (e.g. Flink, Arroyo, Materialize) focus primarily on event-driven analytics or CDC pipelines, but they often lack:

- Native time-series semantics (temporal joins, quantization, lookbacks)
- Statistical and AI-based functions (outlier detection, anomaly scoring)
- Built-in signal processing (rolling averages, volatility measures)
- Developer simplicity (no cluster setup, fast local iteration)

Meanwhile, real-time databases like KDB and QuasarDB deliver exceptional time-series performance — but with proprietary languages, complex clustering, and limited streaming support.

Velostream can bridge this gap with:

- A single binary engine
- SQL-native stream processing
- Time-aware operators and statistical functions
- Outlier detection, trend recognition, and quantization built-in

## 3. Goals

- 🕒 Provide native time-series support within Velostream SQL
- 📈 Enable realtime outlier detection, moving averages, volatility tracking, and sliding window stats
- ⚡ Deliver sub-second latency across quantized tick and sensor streams
- 🔍 Support wildcard queries over nested structured values (e.g. `value.stock.*.price > 100`)
- 🧠 Build intelligent statistical and anomaly functions natively (no external UDFs required)
- 🧩 Maintain single binary simplicity and declarative SQL interface

## 4. Problem Statement

Current Velostream prototypes focus on row-by-row event streams and CTAS pipelines, but lack dedicated time-series functions and temporal window semantics.

Without temporal awareness, it's difficult to:

- Compute rolling or moving aggregates
- Detect outliers and volatility in real-time
- Efficiently bucket tick-level data into time frames
- Integrate statistical insight directly into SQL queries

## 5. Design Principles

- **Declarative First:** All features accessible via SQL (no imperative logic).
- **Deterministic Windows:** Time and event windows should yield consistent results under replay.
- **Composable Functions:** Statistical primitives (e.g., `Z_SCORE`, `STDDEV`, `EMA`) must compose in SQL expressions.
- **Dynamic Schema Support:** Handle nested and semi-structured JSON-like values with wildcard paths.
- **Minimal Footprint:** Single binary, in-memory operation, embeddable in local pipelines.

## 6. Core Features

### 6.1 Temporal Windows

- `TUMBLE(interval)` — Fixed-size windows (e.g. 1s, 5s, 1m)
- `HOP(interval, slide)` — Overlapping moving windows
- `SESSION(timeout)` — Gaps trigger session closures

### 6.2 Quantization Functions

Aggregate fine-grained events into buckets:

```sql
SELECT
  symbol,
  TUMBLE(ts, INTERVAL '1' SECOND) AS bucket,
  AVG(price) AS avg_price,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM ticks
GROUP BY symbol, bucket;
```

### 6.3 Rolling and Moving Aggregates

- `MOVING_AVG(value, window_size)`
- `MOVING_STDDEV(value, window_size)`
- `MOVING_MIN`, `MOVING_MAX`

### 6.4 Outlier and Anomaly Detection

Built-in functions for real-time statistical anomaly detection:

- `Z_SCORE(value, window_size)`
- `IS_OUTLIER(value, sensitivity)`
- `DEVIATION_FROM_BASELINE(metric, lookback)`
- `KURTOSIS(value, window)`
- `SKEWNESS(value, window)`

### 6.5 Wildcard Accessors

Enable pattern queries over nested data:

```sql
SELECT * FROM stream
WHERE value.stock.*.price > 100;
```

### 6.6 Statistical Primitives

Core aggregates for subqueries:

- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`
- `PERCENTILE_CONT`, `MEDIAN`
- `COVAR_POP`, `CORR` (cross-metric correlation)

## 7. Extended Functions (Future Phases)

| Function | Description | Use Case |
|----------|-------------|----------|
| `EMA(value, alpha)` | Exponential moving average | Smoothing noisy tick data |
| `VOLATILITY(price, window)` | Rolling stddev of returns | Financial analytics |
| `RESAMPLE(interval, agg_fn)` | Downsample series | IoT data compaction |
| `CUSUM(value)` | Cumulative sum detection | Change-point detection |
| `DERIVATIVE(value)` | Rate of change | Sensor velocity tracking |
| `ROLLUP(levels)` | Multi-level aggregations | Hierarchical metrics |
| `TREND(value, window)` | Linear trend detection | Predictive anomaly warnings |

## 8. Example Use Cases

### 8.1 Financial Market Ticks

Detect extreme volatility or liquidity events:

```sql
SELECT *
FROM ticks
WHERE IS_OUTLIER(price, 3)
  OR VOLATILITY(price, INTERVAL '5' SECOND) > 0.02;
```

### 8.2 IoT Sensor Streams

Monitor drift or deviation across nested metrics:

```sql
SELECT *
FROM sensors
WHERE value.machine.*.temperature > 90
  OR Z_SCORE(value.machine.*.vibration, 30) > 2.5;
```

### 8.3 Observability Metrics

Flag anomaly spikes in error rates or latencies:

```sql
SELECT service, IS_OUTLIER(error_rate, 5)
FROM metrics
WHERE MOVING_AVG(error_rate, 10) > 0.1;
```

## 9. Differentiation

Velostream aims to differentiate by combining:

- Materialize's SQL ergonomics
- Flink's stateful stream processing
- Arroyo's Rust-native speed
- KDB/Q's time-series intelligence

**In a single binary, embeddable package**

No existing engine currently offers all of:
- ✅ SQL-native
- ✅ Time-series aware
- ✅ Real-time outlier detection
- ✅ Structured wildcard access
- ✅ Single binary deploy

## 10. Testing and Validation Plan

| Test Type | Description |
|-----------|-------------|
| Unit Tests | Validate correctness of all statistical and time functions |
| Replay Tests | Re-run recorded tick/IoT datasets to confirm deterministic output |
| Stress Tests | Measure latency across 10K–100K msg/sec input |
| Anomaly Benchmarks | Compare outlier accuracy vs. scikit-learn baselines |
| CDC Consistency | Validate CTAS pipelines maintain sink parity |
| Integration | Kafka, S3, HTTP source/sink validation |

## 11. Phased Delivery Roadmap

| Phase | Focus | Deliverables |
|-------|-------|--------------|
| Phase 1 (Q4 2025) | Temporal primitives | TUMBLE/HOP/SESSION windows, quantization |
| Phase 2 (Q1 2026) | Statistical core | MOVING_AVG, STDDEV, Z_SCORE |
| Phase 3 (Q2 2026) | Outlier & anomaly layer | IS_OUTLIER, VOLATILITY, TREND |
| Phase 4 (Q3 2026) | Wildcard & structured access | JSONPath support, nested metric filters |
| Phase 5 (Q4 2026) | Predictive extensions | EMA, TREND, derivative, baseline deviation |

## 12. Future Opportunities

- AI-assisted query generation ("What's anomalous in the last 10 minutes?")
- Adaptive window sizing (dynamic based on variance)
- Integrations with vector DBs for contextual embeddings
- Graph-style data lineage for anomaly causation tracing

## 13. Conclusion

This roadmap establishes Velostream as a stream-native time-series intelligence engine — combining statistical reasoning, SQL simplicity, and sub-second processing into a single deployable binary.

By focusing on temporal expressiveness, native anomaly detection, and declarative analytics, Velostream can stand alongside — and even surpass — specialized engines like Materialize, RisingWave, and KDB, with a lighter operational footprint and faster iteration cycle.