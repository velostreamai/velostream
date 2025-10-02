# 🧠 Velostream Analytics Roadmap

## Overview

Velostream aims to go beyond traditional streaming SQL engines by providing intelligent analytics primitives that detect trends, outliers, and complex behaviors directly within the data stream.

While most competitors (e.g. Materialize, RisingWave, Arroyo) focus on aggregation, Velostream will differentiate through real-time statistical and pattern functions designed for financial, IoT, and telemetry workloads.

This document outlines the phased roadmap for introducing advanced analytic functions and how they enhance Velostream's value proposition.

## 🎯 Goals

- Simplify common streaming analytics — make complex rolling or anomaly detection easy.
- Bridge the gap between traditional SQL and real-time intelligence.
- Support financial tick, IoT, and telemetry workloads out of the box.
- Offer a single-binary experience — lightweight yet powerful.

## 🚀 Tier 1 (v0.3) — High ROI, Low Complexity

Focus on core analytic foundations that work seamlessly with window functions and are easy to adopt.

| Function | Description | Use Case | Notes |
|----------|-------------|----------|-------|
| `DERIVATIVE(value)` / `RATE(value)` | Compute change over time | Detect momentum, drift | Useful for finance & IoT |
| `PERCENT_CHANGE(value)` | % change from last sample | Price change alerts | Ties into anomaly detection |
| `LAG(value, 1)` / `LEAD(value, 1)` | Compare to previous/next record | Trend comparisons | Foundation for DELTA |
| `PERCENTILE_CONT(value, 0.9)` | Continuous percentile | Threshold-based alerts | For latency and risk |
| `ROLLING_AVG(value, N)` | Rolling average over last N | Moving average | Simplifies MA logic |
| `ROLLING_STDDEV(value, N)` | Rolling volatility | Detect instability | Needed for outliers |

**✅ Why it matters:**
- Provides the statistical foundation for detecting spikes, outliers, or changes.
- 💡 These functions make Velostream immediately useful for streaming analytics.

## ⚙️ Tier 2 (v0.4) — Analytical Power-Ups

Introduce deeper statistical operators for anomaly, correlation, and trend detection.

| Function | Description | Use Case | Notes |
|----------|-------------|----------|-------|
| `Z_SCORE(value)` | Standardized score | Outlier scoring | Core of anomaly detection |
| `NORMALIZE(value)` | Scale to [0,1] | Feature scaling | Prepares ML inputs |
| `CORR(a,b)` | Pearson correlation | Co-movement, cross-stream validation | Detects relationship breakdowns |
| `COVAR_POP(a,b)` | Covariance | Risk modelling | Finance workloads |
| `APPROX_COUNT_DISTINCT()` | HyperLogLog | Cardinality tracking | User/session counts |
| `TREND_DIRECTION(value)` | Linear regression trend | "UP", "DOWN", "FLAT" | Simple momentum signal |

**✅ Why it matters:**
- Turns Velostream into a real-time statistical engine, reducing dependence on offline analysis.

## 🧠 Tier 3 (v0.5) — Intelligence & Pattern Recognition

Focus on detecting structured changes or sequences in live data.

| Function | Description | Use Case | Notes |
|----------|-------------|----------|-------|
| `PATTERN_MATCH(expr, pattern)` | Detect symbolic sequences | Tick patterns, sensor anomalies | "↑↑↓" detection |
| `DETECT_SPIKE(value, threshold)` | Identify sudden change | Price spikes, signal bursts | Wrapper on RATE/Z-score |
| `DETECT_DRIFT(value, window)` | Detect gradual change | Sensor drift, trading bias | Monitors long-term shifts |
| `EMA(value, alpha)` | Exponential moving average | Trend smoothing | Popular in finance |
| `TOP_K(field, k)` | Approximate top-k items | Leaderboards, heavy hitters | Great for dashboards |

**✅ Why it matters:**
- Enables pattern-based detection directly in SQL — a differentiator over all current engines.

## 🔮 Tier 4 (v0.6+) — Differentiators & Advanced Analytics

These functions position Velostream as a real-time intelligence layer, beyond SQL aggregation.

| Function | Description | Use Case | Notes |
|----------|-------------|----------|-------|
| `RESAMPLE(stream, INTERVAL, AGG)` | Quantize irregular data | Tick → 1s bars | Key for fast-moving data |
| `FFT(value)` / `POWER_SPECTRUM(value)` | Frequency analysis | Detect periodicity | Advanced signal insight |
| `SCORE_LINEAR(value, weight)` | Lightweight ML scoring | On-stream inference | Bridges ML + SQL |
| `OUTLIER_AUTO(value)` | Automatic outlier detection | Self-tuning thresholds | Adaptive analytics |

**✅ Why it matters:**
- Moves Velostream from "fast SQL" → "real-time intelligence engine".

## 🧩 Comparison Matrix

| Category | Velostream | Materialize | Arroyo | RisingWave | Flink |
|----------|------------|-------------|--------|------------|-------|
| Built-in outlier functions | ✅ Planned | ❌ | ❌ | ⚠️ (manual) | ⚠️ (UDF) |
| Rolling analytics | ✅ | ⚠️ | ⚠️ | ⚠️ | ✅ |
| Pattern matching | ✅ Planned | ❌ | ❌ | ❌ | ⚠️ CEP only |
| Correlation / Trend | ✅ | ❌ | ❌ | ❌ | ⚠️ |
| Single binary runtime | ✅ | ❌ | ✅ | ❌ | ❌ |
| Developer UX | 🟢 SQL + YAML | 🟠 SQL | 🟢 | 🟠 | 🔴 Complex |

## 🧪 Testing and Validation Plan

- **Unit Tests** — For each analytic function (statistical correctness + null handling).
- **Streaming Integration Tests** — Feed simulated tick/IoT data; validate rolling computations.
- **Regression Benchmarks** — Compare performance with and without analytics enabled.
- **Cross-engine Comparison** — Validate results against pandas, Flink SQL, or DuckDB.
- **Demo Scenarios** — Include real-world pipelines:
    - Outlier detection in tick data
    - IoT temperature drift detection
    - Quantized price bar generation

## 💡 Example Use Cases

| Domain | Example |
|--------|---------|
| Finance | Detecting >5% price spikes, volume surges, moving volatility |
| IoT / Sensors | Identifying gradual sensor drift or noisy readings |
| Telemetry | Flagging latency outliers or CPU spikes |
| Risk / Ops | Monitoring correlation breakdown between related metrics |
| Observability | Real-time SLO breach prediction using percentiles |

## 🧭 Summary

Velostream's analytics roadmap introduces statistical, temporal, and pattern intelligence natively into SQL. This empowers users to move from raw data → actionable signals without Python or Spark jobs — keeping pipelines lightweight and maintainable.

By phasing in Tier 1–3 features, Velostream can leapfrog Materialize and Arroyo in intelligence while staying simpler than Flink.