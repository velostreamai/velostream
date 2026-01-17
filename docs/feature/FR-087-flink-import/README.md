# FR-087: Flink SQL Import & Auto-Observability

> **Status**: Planning | **Priority**: Critical (Adoption Wedge) | **Tier**: Open Source

## Overview

A zero-friction adoption path that imports existing Flink SQL jobs and auto-generates everything needed for testing, observability, and migration — without changing the existing Flink deployment.

```bash
velo import flink-job.sql

# Output:
# ✓ Converted 3 queries to Velostream SQL
# ✓ Generated 2 schemas with synthetic data config
# ✓ Generated test_spec.yaml with 15 AI-powered assertions
# ✓ Added 8 @metric annotations for observability
# ✓ Created Grafana dashboard: dashboards/flink-job.json
# ✓ Created 4 alert rules
# ✓ Shadow mode ready
#
# Run: velo test ./converted/
# Dashboard: velo run ./converted/ --dashboard
```

**They import Flink SQL. They get dashboards, testing, and alerts. Instantly.**

---

## Strategic Value

### The Adoption Problem

| Challenge | Traditional Approach | This Feature |
|-----------|---------------------|--------------|
| Migration friction | Rewrite all SQL | Zero rewrite |
| Risk of switching | Big bang migration | Shadow mode, gradual |
| Time to value | Weeks/months | Minutes |
| Test coverage | Build from scratch | Auto-generated |
| Observability | Manual setup | Auto-generated |

### The Wedge Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│  USER HAS FLINK                                                  │
│  "I have existing jobs, can't justify rewrite"                  │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  IMPORT (Zero Risk)                                              │
│  velo import flink-job.sql                                       │
│  - No changes to Flink                                           │
│  - Immediate value: tests, dashboards, alerts                   │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  TEST (Build Confidence)                                         │
│  velo test ./converted/                                          │
│  - AI-powered test harness                                       │
│  - Synthetic data generation                                     │
│  - Failure analysis                                              │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  SHADOW (Compare)                                                │
│  velo run ./converted/ --shadow                                  │
│  - Run alongside Flink                                           │
│  - Compare outputs                                               │
│  - Measure latency, correctness                                  │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  MIGRATE (When Ready)                                            │
│  - Confidence built through testing                              │
│  - Dashboards already working                                    │
│  - Switch when ready, or don't                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Core Capabilities

### 1. SQL Conversion

Automatic conversion of Flink SQL syntax to Velostream SQL.

**Flink SQL Input:**
```sql
CREATE TABLE trades (
    symbol STRING,
    price DECIMAL(19,4),
    quantity BIGINT,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'trades',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

SELECT
    symbol,
    COUNT(*) as trade_count,
    SUM(quantity) as total_volume,
    AVG(price) as avg_price
FROM trades
GROUP BY symbol, TUMBLE(event_time, INTERVAL '1' MINUTE);
```

**Velostream SQL Output:**
```sql
-- Auto-converted from: flink-job.sql
-- Original Flink connector: kafka

-- @metric: trade_count
-- @metric_type: counter
-- @metric_labels: symbol
-- @metric_help: Number of trades per symbol per window

-- @metric: total_volume
-- @metric_type: counter
-- @metric_labels: symbol
-- @metric_help: Total quantity traded per symbol

-- @metric: avg_price
-- @metric_type: gauge
-- @metric_labels: symbol
-- @alert: avg_price > 1000
-- @alert_severity: warning

SELECT
    symbol,
    COUNT(*) as trade_count,
    SUM(quantity) as total_volume,
    AVG(price) as avg_price
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE)
EMIT CHANGES
WITH (
    'trades.type' = 'kafka_source',
    'trades.bootstrap.servers' = '${KAFKA_BROKERS:localhost:9092}',
    'trades.topic' = 'trades',
    'trades.format' = 'json'
);
```

### 2. AI-Driven Metric Generation

Automatic inference of meaningful metrics from query semantics.

| Query Pattern | Generated Annotation | Metric Type |
|---------------|---------------------|-------------|
| `COUNT(*)` | `@metric: record_count` | counter |
| `SUM(quantity)` | `@metric: total_quantity` | counter |
| `AVG(price)` | `@metric: avg_price` | gauge |
| `MAX(latency)` | `@metric: max_latency` | gauge |
| `MIN(value)` | `@metric: min_value` | gauge |
| `GROUP BY symbol` | `@metric_labels: symbol` | labels |
| `GROUP BY region, product` | `@metric_labels: region, product` | labels |
| `WHERE status = 'error'` | `@metric: error_count` | counter |
| `WHERE fraud_score > 0.8` | `@alert: high_fraud_score` | alert |
| Window aggregation | `@metric_window` | interval hint |

**AI Enhancement:**
- Infers business meaning from column names
- Suggests appropriate alert thresholds
- Generates metric help text
- Identifies anomaly detection candidates

### 3. Schema Generation

Auto-generate schemas for synthetic data generation.

**Generated Schema:**
```yaml
# schemas/trades.schema.yaml
# Auto-generated from Flink DDL + Kafka sampling

schema:
  name: trades
  source: flink_ddl
  kafka_topic: trades

fields:
  - name: symbol
    type: string
    constraints:
      # Sampled from Kafka topic
      enum: [AAPL, GOOGL, MSFT, TSLA, NVDA, META, AMZN]

  - name: price
    type: decimal
    precision: 19
    scale: 4
    constraints:
      # Inferred from Kafka sample
      min: 45.23
      max: 892.50
      distribution: normal

  - name: quantity
    type: integer
    constraints:
      min: 1
      max: 100000
      distribution: log_normal

  - name: event_time
    type: timestamp
    constraints:
      range: relative
      start: "-1h"
      end: "now"
```

### 4. Test Spec Generation

AI-powered test assertions based on query understanding.

**Generated Test Spec:**
```yaml
# test_spec.yaml
# Auto-generated with AI analysis

test_suite:
  name: flink-job (imported)
  source_system: flink
  original_file: flink-job.sql

  defaults:
    records_per_source: 1000
    timeout_ms: 30000

queries:
  - name: trade_aggregation
    description: "Aggregates trades by symbol in 1-minute windows"

    assertions:
      # Basic correctness
      - type: record_count
        operator: greater_than
        expected: 0
        reason: "Window should produce at least one output"

      - type: schema_contains
        fields: [symbol, trade_count, total_volume, avg_price]

      - type: no_nulls
        fields: [symbol, trade_count]
        reason: "GROUP BY key and COUNT should never be null"

      # Semantic assertions (AI-generated)
      - type: aggregate_check
        expression: "SUM(total_volume)"
        operator: equals
        expected: "{{inputs.trades.sum_quantity}}"
        reason: "AI: Window aggregation should preserve total quantity"

      - type: field_values
        field: trade_count
        operator: greater_than
        value: 0
        reason: "AI: COUNT(*) should always be positive"

      - type: field_values
        field: avg_price
        operator: between
        min: 0
        max: 10000
        reason: "AI: Average price should be within reasonable bounds"

      # Performance assertions
      - type: execution_time
        max_ms: 5000
        reason: "1-minute window should complete quickly"
```

### 5. Dashboard Generation

Auto-generate Grafana dashboards from @metric annotations.

**Generated Dashboard Layout:**
```
+---------------------------------------------------------------------+
|  TRADE ANALYTICS                                                     |
|  Auto-generated from: flink-job.sql                                 |
+---------------------------------------------------------------------+
|                                                                      |
|  +---------------------------+  +---------------------------+        |
|  | trade_count by symbol     |  | avg_price by symbol       |        |
|  | [Bar Chart]               |  | [Time Series]             |        |
|  |                           |  |                           |        |
|  | AAPL  ████████ 1,234     |  | ──── AAPL                 |        |
|  | TSLA  ██████ 892         |  | ──── TSLA                 |        |
|  | NVDA  ████ 567           |  | ──── NVDA                 |        |
|  +---------------------------+  +---------------------------+        |
|                                                                      |
|  +-------------------------------------------------------------+    |
|  | total_volume (all symbols)                                   |    |
|  | [Area Chart - Stacked by Symbol]                             |    |
|  |                                                               |    |
|  | ▁▂▃▅▆▇█▇▆▅▄▃▂▁▂▃▄▅▆▇█▇▆▅▄▃▂▁                                |    |
|  +-------------------------------------------------------------+    |
|                                                                      |
|  ALERTS                                                              |
|  +-------------------------------------------------------------+    |
|  | [!] avg_price > $1000 for NVDA (triggered 3x today)         |    |
|  | [!] trade_count spike detected for AAPL                     |    |
|  +-------------------------------------------------------------+    |
|                                                                      |
+---------------------------------------------------------------------+
```

**Generated Grafana JSON:**
```json
{
  "dashboard": {
    "title": "Trade Analytics (from flink-job.sql)",
    "tags": ["velostream", "imported", "flink"],
    "panels": [
      {
        "title": "trade_count by symbol",
        "type": "barchart",
        "targets": [{
          "expr": "trade_count{job=\"flink-job\"}"
        }]
      }
    ]
  }
}
```

### 6. Shadow Mode

Run alongside Flink and compare outputs.

```bash
velo run ./converted/ --shadow \
  --compare-topic kafka://flink-output-topic \
  --report shadow_report.json
```

**Shadow Mode Report:**
```
+---------------------------------------------------------------------+
|  SHADOW MODE COMPARISON                                              |
|  Velostream vs Flink (24 hour run)                                  |
+---------------------------------------------------------------------+
|                                                                      |
|  CORRECTNESS                                                         |
|  +----------------------------+--------------------+                 |
|  | Metric                     | Result             |                 |
|  +----------------------------+--------------------+                 |
|  | Records compared           | 1,234,567          |                 |
|  | Exact matches              | 1,234,401 (99.98%) |                 |
|  | Mismatches                 | 166 (0.02%)        |                 |
|  | Mismatch cause             | Floating point     |                 |
|  +----------------------------+--------------------+                 |
|                                                                      |
|  PERFORMANCE                                                         |
|  +----------------------------+--------------------+                 |
|  | Metric                     | Velostream | Flink |                 |
|  +----------------------------+--------------------+                 |
|  | Avg latency                | 12ms       | 45ms  |                 |
|  | P99 latency                | 28ms       | 120ms |                 |
|  | Memory usage               | 256MB      | 2.1GB |                 |
|  | CPU usage                  | 0.8 cores  | 2.4   |                 |
|  +----------------------------+--------------------+                 |
|                                                                      |
|  RECOMMENDATION: Safe to migrate. 3.75x faster, 8x less memory.     |
|                                                                      |
+---------------------------------------------------------------------+
```

---

## CLI Interface

```bash
# Basic import
velo import flink-job.sql

# Import with Kafka sampling for better schemas
velo import flink-job.sql --sample-kafka

# Import with AI enhancement
velo import flink-job.sql --ai

# Full import with all features
velo import flink-job.sql \
  --ai \
  --sample-kafka \
  --dashboard \
  --output ./converted/

# Import and immediately test
velo import flink-job.sql --test

# Import and run in shadow mode
velo import flink-job.sql --shadow --compare-topic kafka://flink-output

# Import from directory of Flink SQL files
velo import ./flink-jobs/ --output ./converted/

# Import from Flink REST API (if available)
velo import --flink-api http://flink-jobmanager:8081 --job-id abc123
```

---

## Output Structure

```
./converted/
├── app.sql                      # Velostream SQL with @metrics
├── original/
│   └── flink-job.sql            # Original Flink SQL (preserved)
├── schemas/
│   ├── trades.schema.yaml       # Auto-generated schema
│   └── customers.schema.yaml
├── test_spec.yaml               # AI-generated test assertions
├── dashboards/
│   └── app.grafana.json         # Ready-to-import Grafana dashboard
├── alerts/
│   └── app.alertmanager.yaml    # Prometheus alerting rules
├── shadow_config.yaml           # Shadow mode configuration
└── conversion_report.md         # What was converted, any warnings
```

---

## Conversion Rules

### SQL Syntax Mapping

| Flink SQL | Velostream SQL |
|-----------|----------------|
| `TUMBLE(event_time, INTERVAL '5' MINUTE)` | `WINDOW TUMBLING(INTERVAL '5' MINUTE)` |
| `HOP(event_time, INTERVAL '1' MIN, INTERVAL '5' MIN)` | `WINDOW SLIDING(INTERVAL '5' MINUTE, INTERVAL '1' MINUTE)` |
| `SESSION(event_time, INTERVAL '30' SECOND)` | `WINDOW SESSION(INTERVAL '30' SECOND)` |
| `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND` | Watermark config in WITH clause |
| `'connector' = 'kafka'` | `'source.type' = 'kafka_source'` |
| `PROCTIME()` | `_processing_time` |
| `ROWTIME` | `_event_time` |

### Connector Mapping

| Flink Connector | Velostream Equivalent |
|-----------------|----------------------|
| `kafka` | `kafka_source` / `kafka_sink` |
| `filesystem` | `file_source` / `file_sink` |
| `jdbc` | `postgres_source` (for CDC) |
| `elasticsearch` | Future: `elasticsearch_sink` |
| `upsert-kafka` | `kafka_sink` with upsert mode |

### Unsupported Features

Features that can't be directly converted (with suggested alternatives):

| Flink Feature | Status | Alternative |
|---------------|--------|-------------|
| User-Defined Functions (UDF) | Not converted | Rewrite in SQL or Rust |
| Complex Event Processing (CEP) | Not converted | Use window functions |
| State backends | N/A | Velostream manages state |
| Savepoints | N/A | Different recovery model |

---

## AI Features

### Metric Inference

The AI analyzes query semantics to generate meaningful metrics:

```sql
-- Input: Flink SQL
SELECT customer_id, SUM(amount) as total_spent
FROM orders
WHERE status = 'completed'
GROUP BY customer_id;

-- AI adds:
-- @metric: total_spent
-- @metric_type: gauge
-- @metric_labels: customer_id
-- @metric_help: Total amount spent by customer (completed orders only)
-- @alert: total_spent > 10000
-- @alert_name: high_spender_detected
-- @alert_severity: info
```

### Test Assertion Generation

The AI understands query semantics to generate meaningful tests:

```yaml
# AI-generated assertions for the above query
assertions:
  - type: field_values
    field: total_spent
    operator: greater_than_or_equal
    value: 0
    reason: "AI: SUM of positive amounts should be non-negative"

  - type: no_nulls
    fields: [customer_id]
    reason: "AI: GROUP BY key should never be null"

  - type: aggregate_check
    expression: "COUNT(DISTINCT customer_id)"
    operator: less_than_or_equal
    expected: "{{inputs.orders.distinct_customer_id}}"
    reason: "AI: Output customers cannot exceed input customers"
```

### Schema Enhancement

AI enhances schemas based on column names and sampled data:

```yaml
# AI-enhanced schema
fields:
  - name: customer_id
    type: string
    constraints:
      pattern: "CUST-[0-9]{6}"  # AI: Inferred from samples

  - name: amount
    type: decimal
    constraints:
      min: 0.01        # AI: Monetary amounts are positive
      max: 100000      # AI: Inferred from sample distribution
      precision: 19
      scale: 2         # AI: Currency typically has 2 decimal places
```

---

## Implementation Phases

### Phase 1: Core Conversion (LoE: 2-3 weeks)

| Task | Description | LoE |
|------|-------------|-----|
| 1.1 | Flink SQL parser (using sqlparser-rs with Flink dialect) | 16 hours |
| 1.2 | DDL conversion (CREATE TABLE) | 8 hours |
| 1.3 | DML conversion (SELECT, INSERT) | 16 hours |
| 1.4 | Window function mapping | 8 hours |
| 1.5 | Connector configuration mapping | 8 hours |
| 1.6 | CLI skeleton (`velo import`) | 4 hours |
| 1.7 | Conversion report generation | 4 hours |
| 1.8 | Unit tests for conversion | 16 hours |

### Phase 2: Auto-Observability (LoE: 2-3 weeks)

| Task | Description | LoE |
|------|-------------|-----|
| 2.1 | Metric inference from aggregations | 16 hours |
| 2.2 | Label inference from GROUP BY | 8 hours |
| 2.3 | Alert inference from WHERE clauses | 8 hours |
| 2.4 | @metric annotation generation | 8 hours |
| 2.5 | Grafana dashboard JSON generation | 16 hours |
| 2.6 | Prometheus alerting rules generation | 8 hours |
| 2.7 | AI enhancement integration | 16 hours |

### Phase 3: Schema & Test Generation (LoE: 2-3 weeks)

| Task | Description | LoE |
|------|-------------|-----|
| 3.1 | Schema extraction from DDL | 8 hours |
| 3.2 | Kafka sampling for constraints | 16 hours |
| 3.3 | AI-powered constraint inference | 16 hours |
| 3.4 | Test spec generation | 16 hours |
| 3.5 | AI assertion generation | 16 hours |
| 3.6 | Integration with FR-084 test harness | 8 hours |

### Phase 4: Shadow Mode (LoE: 2-3 weeks)

| Task | Description | LoE |
|------|-------------|-----|
| 4.1 | Shadow mode execution | 16 hours |
| 4.2 | Output comparison engine | 16 hours |
| 4.3 | Latency comparison | 8 hours |
| 4.4 | Resource usage comparison | 8 hours |
| 4.5 | Comparison report generation | 8 hours |
| 4.6 | Migration recommendation engine | 8 hours |

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Time from Flink SQL to running test | < 5 minutes |
| Time from import to dashboard | < 10 minutes |
| Conversion success rate | > 90% of common patterns |
| Shadow mode accuracy | > 99.9% match with Flink |
| User effort required | Zero code changes |

---

## Defensive Moat

| Aspect | Why It's Hard to Copy |
|--------|----------------------|
| Flink SQL dialect knowledge | Edge cases, version differences |
| AI metric inference | Requires understanding query semantics |
| AI test generation | Requires understanding correctness criteria |
| Dashboard generation | Layout, visualization type selection |
| Shadow mode comparison | Semantic matching, not just byte comparison |
| Integrated value | All pieces work together seamlessly |

---

## Competitive Positioning

### Architecture: No Mandatory Dependencies

Velostream runs in three modes with zero mandatory infrastructure:

```
+---------------------------------------------------------------------+
|                       VELOSTREAM                                     |
|              SQL Streaming Runtime for the Age of AI                |
+---------------------------------------------------------------------+
|                                                                      |
|  MODE 1: FILE-BASED (Zero Dependencies)                             |
|  +-------------+     +-------------+     +-------------+            |
|  | CSV / JSON  |---->| Velostream  |---->| CSV / JSON  |            |
|  | Parquet     |     |             |     | Parquet     |            |
|  +-------------+     +-------------+     +-------------+            |
|                                                                      |
|  MODE 2: KAFKA-BASED (Streaming)                                    |
|  +-------------+     +-------------+     +-------------+            |
|  | Kafka       |---->| Velostream  |---->| Kafka       |            |
|  | + Connect   |     |             |     | + Connect   |            |
|  +-------------+     +-------------+     +-------------+            |
|                                                                      |
|  MODE 3: LAKEHOUSE (Future)                                         |
|  +-------------+     +-------------+     +-------------+            |
|  | Iceberg     |---->| Velostream  |---->| Iceberg     |            |
|  |             |     |             |     |             |            |
|  +-------------+     +-------------+     +-------------+            |
|                                                                      |
+---------------------------------------------------------------------+
```

| Mode | Dependencies | Use Case |
|------|--------------|----------|
| Files | None | Local dev, CI/CD, learning |
| Kafka | Kafka (optional Connect) | Real-time streaming |
| Iceberg | Iceberg catalog | Lakehouse, batch replay |

### Connector Strategy

| Layer | Responsibility | Examples |
|-------|----------------|----------|
| **Native** | First-class, optimized | Files, Kafka, Iceberg (future) |
| **Kafka Connect** | 200+ external systems | Postgres CDC, S3, Snowflake, Elasticsearch |
| **Direct** (future) | High-performance | ClickHouse, Postgres CDC |

**Philosophy:** Native for core patterns, Kafka Connect for the long tail.

---

### Competitor Attack Playbook

Anticipated attacks from Flink/Confluent/Materialize and how to counter:

#### Attack 1: "Conversion is Incomplete"

**What they'll say:**
> "Sure, it converts simple queries. But UDFs? CEP patterns? State backends? They can't convert real production jobs."

**Counter:**

| Defense | Response |
|---------|----------|
| Transparency | Conversion report shows exactly what converted and what didn't |
| 80/20 rule | Most production jobs ARE simple aggregations and joins |
| Escape hatch | Unsupported features flagged with rewrite suggestions |
| Prove it | "Import YOUR job right now, see what happens" |

---

#### Attack 2: "No Production Maturity"

**What they'll say:**
> "Flink has 10 years of production hardening. Alibaba runs exabytes through it. This is unproven."

**Counter:**

| Defense | Response |
|---------|----------|
| Shadow mode | "Run both in production, compare results for a week" |
| Gradual migration | "Start with one non-critical job" |
| Rust stability | "Memory-safe by design, not by 10 years of patches" |
| Transparency | "Here's our test suite, benchmarks, run them yourself" |

---

#### Attack 3: "Just Trading Lock-in"

**What they'll say:**
> "You're just moving from Flink lock-in to Velostream lock-in."

**Counter:**

| Defense | Response |
|---------|----------|
| Open source | Apache 2.0 - fork it if you want |
| Standard SQL | More SQL-standard than Flink's dialect |
| Import/export | "We imported from Flink, export works too" |
| No cloud lock-in | Unlike Confluent Cloud's proprietary features |

---

#### Attack 4: "@metric is Just Comments"

**What they'll say:**
> "Real observability requires deep instrumentation. SQL comments aren't real observability."

**Counter:**

| Defense | Response |
|---------|----------|
| Engine integration | Comments are parsed, metrics are real Prometheus endpoints |
| Zero config | "Show me your Flink to Prometheus setup time" |
| Prove it | "Dashboard working in 5 minutes vs your days of setup" |

**Note:** This attack validates the feature - they're scared of it.

---

#### Attack 5: "AI Testing is a Gimmick"

**What they'll say:**
> "AI can't understand your business logic. Real testing requires domain experts."

**Counter:**

| Defense | Response |
|---------|----------|
| Additive | "AI generates baseline, you add business logic on top" |
| Something > nothing | "You have zero tests today" |
| Failure analysis | "AI explains WHY tests failed, humans decide what to do" |

---

#### Attack 6: "Performance Claims are Synthetic"

**What they'll say:**
> "Micro-benchmarks don't reflect production."

**Counter:**

| Defense | Response |
|---------|----------|
| Shadow mode | "Run YOUR workload, measure YOUR numbers" |
| Resource comparison | "Same inputs, you measure CPU/memory" |
| Reproducible | "Here's exactly how we benchmark, run it yourself" |

---

#### Attack 7: "Where Are Your Connectors?"

**What they'll say:**
> "Flink has 50+ connectors. Where are yours?"

**Counter:**

| Defense | Response |
|---------|----------|
| Kafka Connect | "200+ connectors already exist, we don't reinvent them" |
| File-based | "Or skip Kafka entirely, run on CSV/Parquet files" |
| Focus | "We do streaming SQL, connectivity is pluggable" |
| Iceberg | "Lakehouse integration coming for unified batch/stream" |

---

#### Attack 8: "You Require Kafka"

**What they'll say:**
> "So it's just another Kafka-dependent thing?"

**Counter:**

| Defense | Response |
|---------|----------|
| File mode | "Run entirely on CSV files. No Kafka, no Docker" |
| Zero deps | "Download, point at a file, run SQL. 30 seconds." |
| Your choice | "Add Kafka when you need streaming. It's optional." |

---

#### Attack 9: "Small Company Risk"

**What they'll say:**
> "Who supports this in 5 years? Flink has Alibaba, Apple behind it."

**Counter:**

| Defense | Response |
|---------|----------|
| Open source | "Apache 2.0 - community can fork and continue" |
| Simplicity | "Single binary, easier to maintain than Flink cluster" |
| No dependency | "No vendor lock-in, no managed service required" |

**Note:** This is a real concern. Address with traction, funding, or community growth.

---

### Threat Assessment

| Attack | Severity | Mitigation |
|--------|----------|------------|
| Conversion incomplete | HIGH | Transparency, prove on their job |
| No production maturity | HIGH | Shadow mode, gradual migration |
| Small company risk | HIGH | Open source, community, traction |
| Ecosystem gap | MEDIUM | Kafka Connect, file mode, Iceberg |
| AI is a gimmick | LOW | Demonstrate value, it's additive |
| @metric is a hack | LOW | Show it working in 5 minutes |

---

### Pre-emptive Positioning

Address these in docs and pitch before competitors attack:

1. **"Conversion works for 80%+ of jobs, and tells you exactly what it can't convert."**

2. **"Run shadow mode for a week before committing. Zero risk."**

3. **"Apache 2.0. Single binary. No lock-in. Fork it if we disappear."**

4. **"You have zero test coverage today. AI gives you a baseline. Add your own on top."**

5. **"Don't believe our benchmarks. Run your workload. Compare yourself."**

6. **"No Kafka required. Run on files locally. Add Kafka when you need streaming."**

7. **"200+ Kafka Connect connectors work out of the box. We focus on SQL."**

---

### Judo Moves

When they attack, flip it:

| Their Attack | Your Flip |
|--------------|-----------|
| "Conversion is incomplete" | "So migrating FROM Flink requires rewriting? That's the lock-in we're solving." |
| "No production maturity" | "That's why we have shadow mode. Run both. Zero risk." |
| "Small company" | "That's why it's open source. You're not dependent on us." |
| "Where are connectors?" | "Kafka Connect has 200+. Why would we rebuild them?" |
| "You require Kafka" | "We don't. Run on files. Show me Flink running on CSV." |

---

### What They CAN'T Attack

These features have no competitor equivalent:

| Strength | Why It's Unassailable |
|----------|----------------------|
| Instant dashboards from SQL | They don't have @metric |
| AI test generation | They have nothing |
| Zero-config observability | They require manual setup |
| Import existing jobs | They require rewrite |
| Single binary | They require clusters |
| File-based mode | They require infrastructure |

---

## The Pitch

> *"Point Velostream at your Flink SQL. Get dashboards, testing, and alerts you never had. Zero effort, zero risk. Your Flink jobs keep running — you've just added observability and testing that would have taken weeks to build. Migrate when you're confident, or don't. Either way, you win."*

---

## Files to Create

```
src/velostream/import/
├── mod.rs                    # Module exports
├── flink/
│   ├── mod.rs               # Flink import module
│   ├── parser.rs            # Flink SQL parsing
│   ├── converter.rs         # Flink to Velostream conversion
│   ├── connector_map.rs     # Connector configuration mapping
│   └── dialect.rs           # Flink SQL dialect specifics
├── observability/
│   ├── mod.rs               # Observability generation
│   ├── metric_inference.rs  # Infer metrics from queries
│   ├── alert_inference.rs   # Infer alerts from queries
│   └── dashboard_gen.rs     # Generate Grafana dashboards
├── schema/
│   ├── mod.rs               # Schema generation
│   ├── ddl_extractor.rs     # Extract from DDL
│   ├── kafka_sampler.rs     # Sample from Kafka for constraints
│   └── ai_enhancer.rs       # AI-powered enhancement
├── testing/
│   ├── mod.rs               # Test generation
│   ├── spec_generator.rs    # Generate test_spec.yaml
│   └── ai_assertions.rs     # AI-powered assertion generation
├── shadow/
│   ├── mod.rs               # Shadow mode
│   ├── executor.rs          # Run in shadow mode
│   ├── comparator.rs        # Compare outputs
│   └── reporter.rs          # Generate comparison reports
└── cli.rs                    # CLI for velo import

src/bin/
└── velo-import.rs            # (or integrate into main velo binary)
```

---

## References

- [Flink SQL Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/)
- [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs) - SQL parsing in Rust
- [FR-084: Test Harness](../FR-084-app-test-harness/README.md) - Integration target
- [FR-085: Velostream Studio](../FR-085-velo-sql-studio/README.md) - Dashboard generation patterns
