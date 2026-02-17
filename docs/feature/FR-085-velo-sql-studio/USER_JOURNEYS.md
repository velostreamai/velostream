# FR-085: Velostream SQL Studio - User Journeys

> **Document Purpose**: User-centric workflows showing how people actually use the Studio
> **Last Updated**: February 2026

---

## Overview

This document maps out the primary user journeys through Velostream Studio. Each journey
describes a persona, their goal, the entry point, and a step-by-step flow with concrete
chat messages, tool calls, and artifacts produced.

The Studio supports three fundamental starting points:
1. **Data-first** ("I have data, help me query it") — connect to any source (Kafka, files, S3, databases), explore data, build SQL from real schemas
2. **Intent-first** ("I know what I want, generate it") — describe a pipeline in English, generate SQL and test data
3. **Template-first** ("Give me a starting point") — browse pre-built application templates, customize for your data

The Studio works with **all Velostream data sources**:
- **Kafka** — streaming topics with partition-level exploration
- **File / FileMmap** — CSV, JSON, JSON Lines, Parquet, Avro, ORC (FileMmap for high-throughput batch)
- **S3** — Object storage with format and compression support
- **ClickHouse** — Analytical database integration
- **Database (CDC)** — Change data capture from relational databases
- **URI-based** — Extensible via `kafka://`, `file://`, `s3://`, `clickhouse://` schemes

This document covers **10 user journeys**:

| # | Journey | Starting Point |
|---|---------|---------------|
| 1 | Explore First | Data-first (Kafka) |
| 2 | Greenfield | Intent-first |
| 3 | Import Existing SQL | Intent-first |
| 4 | Debug & Iterate | Data-first |
| 5 | Observe & Monitor | Data-first |
| 6 | Team Onboarding | Intent-first |
| 7 | Build Me an App | Intent-first (NL → full app) |
| 8 | Start from a Template | Template-first |
| 9 | AI Proactive Intelligence | Cross-cutting |
| 10 | Files, S3, & Databases | Data-first (non-Kafka) |

Across all journeys, the AI acts as a **proactive collaborator** — not just responding to
requests but actively suggesting optimizations, detecting issues, recommending metrics,
and identifying opportunities the user may not have considered.

---

## Journey 1: Explore First

> **Persona**: Data engineer with a running Kafka cluster
> **Goal**: Discover what data is available and build a query from real schemas
> **Entry point**: Studio chat thread

### Flow

```
User: "Connect to kafka at broker1:9092"
  → Tool: connect_source(uri: "kafka://broker1:9092")
  → Stores connection in ThreadContext.connection
  → Response: "Connected to broker1:9092. What would you like to explore?"

User: "What topics do I have?"
  → Tool: list_topics()
  → API: GET /api/topics
  → Infra: TestHarnessInfra::fetch_topic_info(None)
  → Artifact: TopicListArtifact
    ┌──────────────────────────────────────────────────────────┐
    │ trades           │ 6 partitions │ 1.2M messages         │
    │ orders           │ 3 partitions │ 450K messages         │
    │ customer-events  │ 1 partition  │ 89K messages          │
    └──────────────────────────────────────────────────────────┘

User: "What does the trades topic look like?"
  → Tool: inspect_topic(topic: "trades")
  → API: GET /api/topics/trades/schema
  → Infra: TestHarnessInfra::fetch_topic_schema("trades", 10)
  → Artifact: SchemaViewerArtifact
    │ Field     │ Type     │ Example          │
    │ symbol    │ String   │ "AAPL"           │
    │ price     │ Float    │ 152.34           │
    │ quantity  │ Integer  │ 5000             │
    │ timestamp │ DateTime │ 2026-02-16T...   │

User: "Show me the last 10 messages"
  → Tool: peek_messages(topic: "trades", limit: 10, from_end: true)
  → API: GET /api/topics/trades/messages?limit=10&from_end=true
  → Infra: TestHarnessInfra::peek_topic_messages("trades", 10, true, None, None)
  → Artifact: DataPreviewArtifact
    [Formatted JSON messages with partition, offset, timestamp metadata]

User: "Write a query to get average price per symbol in 5-minute windows"
  → Tool: generate_sql(prompt, context: { schemas: { trades: inferred_schema } })
  → AI uses the real discovered schema as context
  → Artifact: SqlEditorArtifact

User: [clicks Test]
  → Tool: test_query(sql, schema: inferred_schema)
  → Artifact: TestResultsArtifact

User: [clicks Deploy]
  → Tool: deploy_pipeline(notebook_id, config)
  → Artifact: DeploySummaryArtifact
```

### Backend Requirements

| API Endpoint | Infra Method | Description |
|-------------|--------------|-------------|
| `POST /api/connect` | — | Store Kafka connection in session |
| `GET /api/topics` | `fetch_topic_info(None)` | List all topics |
| `GET /api/topics/{name}/schema` | `fetch_topic_schema(name, 10)` | Infer schema from samples |
| `GET /api/topics/{name}/messages` | `peek_topic_messages(...)` | Peek at messages |

### Key Insight

The real schema discovered from Kafka becomes the context for SQL generation. Instead of
the AI guessing field names, it uses the actual fields from `TopicSchema.fields`.

---

## Journey 2: Greenfield

> **Persona**: Developer starting a new project
> **Goal**: Design and deploy a streaming pipeline from scratch
> **Entry point**: Studio chat thread (no existing data)

### Flow

```
User: "I want to monitor trading activity in real-time"
  → AI suggests schema fields based on domain knowledge
  → Tool: generate_sql(prompt: "monitor trading activity in real-time")
  → Artifact: SqlEditorArtifact (with @metric annotations)
    -- @metric: trade_volume
    -- @metric_type: counter
    -- @metric_labels: symbol
    SELECT symbol, SUM(quantity) as volume, AVG(price) as avg_price
    FROM trades
    GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '5' MINUTE)
    EMIT CHANGES

User: "Generate some test data for this"
  → Tool: generate_data(schema: inferred_from_sql, records: 1000)
  → API: POST /api/generate-data
  → Artifact: DataPreviewArtifact (synthetic data sample)

User: [clicks Test]
  → Tool: test_query(sql, schema: inferred, records: 1000)
  → Artifact: TestResultsArtifact
    ✅ Passed (3/3 assertions)
    • record_count: 5 (expected: > 0)
    • schema_contains: [symbol, volume, avg_price]
    • no_nulls: [symbol, volume, avg_price]

User: "Add alerting when volume exceeds 1M"
  → AI modifies SQL with @alert annotation
  → Tool: generate_sql(prompt: "add alert when volume > 1M", context: { previousSql })
  → Artifact: SqlEditorArtifact (updated with @alert)

User: [clicks Deploy]
  → Tool: deploy_pipeline(notebook_id, config: { deploy_dashboard: true })
  → Artifact: DeploySummaryArtifact (jobs + metrics + alerts + Grafana link)
```

### Backend Requirements

Existing tools only — no new endpoints needed:
- `POST /api/nl-to-sql` — SQL generation
- `POST /api/generate-data` — Synthetic data via `SchemaDataGenerator`
- `POST /api/test` — Test harness execution
- `POST /api/deploy` — Pipeline deployment

---

## Journey 3: Import Existing SQL

> **Persona**: Developer with existing streaming SQL files
> **Goal**: Validate, test, and deploy existing SQL through the Studio
> **Entry point**: Paste or upload SQL into chat

### Flow

```
User: [pastes SQL into chat]
    SELECT symbol, AVG(price) as avg_price, COUNT(*) as trade_count
    FROM trades
    GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '5' MINUTE)
    EMIT CHANGES

  → Tool: validate_sql(sql)
  → API: POST /api/validate
  → Artifact: SqlEditorArtifact (validated, with syntax highlighting)
  → Response: "Valid streaming SQL. Groups by symbol with 5-minute tumbling windows."

User: "Test this query"
  → AI infers schema from SQL (fields: symbol STRING, price DECIMAL, quantity INTEGER)
  → Tool: test_query(sql, schema: inferred, records: 1000)
  → Generates synthetic data matching inferred schema
  → Artifact: TestResultsArtifact

User: "Add monitoring to this query"
  → AI analyzes query and suggests appropriate @metric annotations
  → Tool: generate_sql(prompt: "add monitoring", context: { previousSql })
  → Artifact: SqlEditorArtifact (annotated version)
    -- @metric: avg_price_per_symbol
    -- @metric_type: gauge
    -- @metric_labels: symbol
    SELECT symbol, AVG(price) as avg_price, COUNT(*) as trade_count
    FROM trades
    GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '5' MINUTE)
    EMIT CHANGES

User: [clicks Deploy]
  → Tool: deploy_pipeline(notebook_id, config: { deploy_dashboard: true })
  → Artifact: DeploySummaryArtifact
```

### Backend Requirements

Uses existing endpoints:
- `POST /api/validate` — SQL validation via `SqlValidator`
- `POST /api/test` — Schema inference via `SchemaInferencer::infer_from_sql()`
- `POST /api/nl-to-sql` — Annotation via `Annotator::analyze()`
- `POST /api/deploy` — Pipeline deployment

---

## Journey 4: Debug & Iterate

> **Persona**: Developer debugging a running pipeline
> **Goal**: Compare input and output topics to find data issues
> **Entry point**: Studio chat with Kafka connection

### Flow

```
User: "Connect to broker1:9092 and show me the output topic 'trade-alerts'"
  → Tool: connect_source(uri: "kafka://broker1:9092")
  → Tool: inspect_topic(topic: "trade-alerts")
  → Artifact: SchemaViewerArtifact (inferred schema of output)

User: "Show me the last 5 messages from trade-alerts"
  → Tool: peek_messages(topic: "trade-alerts", limit: 5, from_end: true)
  → Artifact: DataPreviewArtifact
    { "symbol": "AAPL", "alert_type": "volume_spike", "price": 0.00, ... }
    { "symbol": "TSLA", "alert_type": "volume_spike", "price": 0.00, ... }

User: "These prices look wrong — they're all zero. The source data is on 'raw-trades'"
  → Tool: peek_messages(topic: "raw-trades", limit: 5, from_end: true)
  → Artifact: DataPreviewArtifact (side-by-side comparison)
    { "symbol": "AAPL", "price": 185.42, "quantity": 5000, ... }
    { "symbol": "TSLA", "price": 242.10, "quantity": 3200, ... }
  → AI: "The source has valid prices. The issue is likely in the SQL query.
         Can you share the query that populates trade-alerts?"

User: [pastes SQL — reveals a missing field in SELECT]
  → Tool: validate_sql(sql)
  → AI: "Found it — the query selects `amount` but the source field is `price`.
         Here's the fix:"
  → Artifact: SqlEditorArtifact (corrected SQL)

User: [clicks Test] → verifies fix → [clicks Deploy] → re-deploys
```

### Backend Requirements

| API Endpoint | Infra Method | Description |
|-------------|--------------|-------------|
| `POST /api/connect` | — | Store connection |
| `GET /api/topics/{name}/schema` | `fetch_topic_schema(...)` | Inspect output topic |
| `GET /api/topics/{name}/messages` | `peek_topic_messages(...)` | Peek at both topics |
| `POST /api/validate` | — | Validate corrected SQL |

---

## Journey 5: Observe & Monitor

> **Persona**: DevOps/SRE adding observability to existing pipelines
> **Goal**: Add metrics, alerts, and dashboards to deployed SQL
> **Entry point**: Paste existing SQL into Studio

### Flow

```
User: [pastes existing production SQL]
    SELECT region, COUNT(*) as order_count, SUM(total) as revenue
    FROM orders
    GROUP BY region
    WINDOW TUMBLING(INTERVAL '1' MINUTE)
    EMIT CHANGES

User: "Add Prometheus metrics and alerting to this"
  → Tool: generate_sql(prompt: "add metrics and alerting", context: { previousSql })
  → Artifact: SqlEditorArtifact (annotated version)
    -- @metric: order_count_by_region
    -- @metric_type: counter
    -- @metric_labels: region
    -- @metric: revenue_by_region
    -- @metric_type: gauge
    -- @metric_labels: region
    -- @alert: revenue > 100000
    -- @alert_severity: warning
    SELECT region, COUNT(*) as order_count, SUM(total) as revenue
    FROM orders
    GROUP BY region
    WINDOW TUMBLING(INTERVAL '1' MINUTE)
    EMIT CHANGES

User: "Generate a Grafana dashboard for this"
  → Tool: generate_dashboard(notebook_id)
  → API: POST /api/dashboards/generate
  → Artifact: GrafanaEmbed (dashboard preview)

User: [clicks Deploy]
  → Tool: deploy_pipeline(notebook_id, config: { deploy_dashboard: true })
  → Artifact: DeploySummaryArtifact
    • 1 streaming SQL job
    • 2 Prometheus metrics
    • 1 alert rule (revenue > 100K)
    • Grafana dashboard: http://grafana:3000/d/xyz/orders
```

### Backend Requirements

Existing endpoints only:
- `POST /api/nl-to-sql` — Annotation generation
- `POST /api/dashboards/generate` — Grafana dashboard JSON
- `POST /api/deploy` — Pipeline + dashboard deployment

---

## Journey 6: Team Onboarding

> **Persona**: New team member learning Velostream
> **Goal**: Learn streaming SQL concepts through guided lessons
> **Entry point**: "Start the tutorial" in Studio chat

### Flow

```
User: "Start the tutorial"
  → AI begins interactive lesson sequence matching quickstart guides

  Lesson 1: Passthrough
  → "Let's start with the simplest query — passing data through unchanged."
  → Artifact: SqlEditorArtifact (pre-filled example)
    SELECT * FROM trades EMIT CHANGES
  → "Click [Test] to see it work with synthetic data."
  → User clicks [Test] → TestResultsArtifact (all passing)
  → "All records pass through. Now try adding a WHERE clause..."

  Lesson 2: Filtering
  → "Filter trades to only high-value ones."
  → User types: "Show me trades where price > 1000"
  → AI generates SQL → User tests it → Learns filtering

  Lesson 3: Aggregation
  → "Now let's count trades per symbol using GROUP BY."
  → Guided walk-through of GROUP BY + EMIT CHANGES

  Lesson 4: Windowed Aggregation
  → "Time-based windows bucket data into intervals."
  → Introduces WINDOW TUMBLING(INTERVAL '5' MINUTE)

  Lesson 5: Row Windows
  → "Row windows compute running statistics over the last N records."
  → Introduces ROWS WINDOW BUFFER 100 ROWS

  Lesson 6: Joins
  → "Enrich streaming data with reference data."
  → Introduces JOIN syntax with two input topics

  Lesson 7: Metrics & Alerts
  → "Add @metric annotations to make SQL self-monitoring."
  → Introduces @metric, @alert annotations

  Lesson 8: Deploy
  → "Deploy your query as a production pipeline."
  → Walks through the deploy flow with dashboard
```

### Backend Requirements

No new endpoints — uses existing tools with curated prompts:
- AI maintains lesson state in thread context
- Each lesson uses `generate_sql`, `test_query`, and `deploy_pipeline`

---

## Journey 7: Build Me an App

> **Persona**: Product manager or data engineer who wants a complete application, not individual queries
> **Goal**: Go from a single English sentence to a fully deployed, multi-query application with dashboards
> **Entry point**: Studio chat thread — a single high-level request
> **Key difference from other journeys**: The AI generates **multiple coordinated queries**, not one at a time

### What Makes This Different

In Journeys 1-6, the user builds one query at a time. Journey 7 is the **"build the whole thing"**
experience. The user describes a business outcome and the AI designs an entire application:
multiple queries, input/output topic wiring, metrics, alerts, and a unified Grafana dashboard.

### Flow

```
User: "Build me a real-time trading analytics dashboard"

  → AI analyzes the request and produces an application plan
  → Tool: generate_app(prompt: "real-time trading analytics dashboard")
  → API: POST /api/generate-app
  → AI determines: this needs 4 coordinated queries + dashboard

  → Artifact: AppPreviewArtifact
    ┌──────────────────────────────────────────────────────────────────┐
    │ 📋 Application: Trading Analytics                                │
    │                                                                  │
    │ Queries (4):                                                     │
    │                                                                  │
    │ 1. VWAP Calculator                                               │
    │    trades → trade-vwap                                           │
    │    SELECT symbol,                                                │
    │           SUM(price * quantity) / SUM(quantity) as vwap,         │
    │           SUM(quantity) as volume                                │
    │    FROM trades                                                   │
    │    GROUP BY symbol                                               │
    │    WINDOW TUMBLING(INTERVAL '5' MINUTE)                          │
    │    EMIT CHANGES                                                  │
    │                                                                  │
    │ 2. Volume Spike Detector                                         │
    │    trades → trade-alerts                                         │
    │    -- @alert: volume > 1000000                                   │
    │    -- @alert_severity: warning                                   │
    │    SELECT symbol, SUM(quantity) as volume                        │
    │    FROM trades                                                   │
    │    GROUP BY symbol                                               │
    │    WINDOW TUMBLING(INTERVAL '1' MINUTE)                          │
    │    HAVING SUM(quantity) > 1000000                                │
    │    EMIT CHANGES                                                  │
    │                                                                  │
    │ 3. Price Movement Tracker                                        │
    │    trades → price-movements                                      │
    │    SELECT symbol,                                                │
    │           AVG(price) as avg_price,                               │
    │           STDDEV(price) / AVG(price) as volatility               │
    │    FROM trades                                                   │
    │    GROUP BY symbol                                               │
    │    WINDOW TUMBLING(INTERVAL '5' MINUTE)                          │
    │    EMIT CHANGES                                                  │
    │                                                                  │
    │ 4. Top Movers (Cross-Query)                                      │
    │    price-movements → top-movers                                  │
    │    SELECT symbol, volatility                                     │
    │    FROM price-movements                                          │
    │    WHERE volatility > 0.02                                       │
    │    ORDER BY volatility DESC                                      │
    │    LIMIT 10                                                      │
    │    EMIT CHANGES                                                  │
    │                                                                  │
    │ Metrics (6):                                                     │
    │  • vwap_per_symbol (gauge, labels: symbol)                       │
    │  • trade_volume (counter, labels: symbol)                        │
    │  • volume_spike_count (counter)                                  │
    │  • avg_price (gauge, labels: symbol)                             │
    │  • volatility (gauge, labels: symbol)                            │
    │  • top_movers_count (gauge)                                      │
    │                                                                  │
    │ Alerts (2):                                                      │
    │  • volume_spike: volume > 1M per symbol per minute               │
    │  • high_volatility: volatility > 5% per symbol                   │
    │                                                                  │
    │ Dashboard: 6-panel Grafana layout                                │
    │  ┌────────────┬────────────┐                                     │
    │  │ VWAP/Symbol│ Volume/Sym │                                     │
    │  ├────────────┼────────────┤                                     │
    │  │ Volatility │ Top Movers │                                     │
    │  ├────────────┼────────────┤                                     │
    │  │ Alerts     │ Throughput │                                     │
    │  └────────────┴────────────┘                                     │
    │                                                                  │
    │ Pipeline Topology:                                               │
    │  trades ──┬── VWAP Calculator ──── trade-vwap                    │
    │           ├── Volume Spike ──────── trade-alerts                 │
    │           └── Price Movement ──┬── price-movements               │
    │                                └── Top Movers ── top-movers      │
    │                                                                  │
    │              [Edit Queries] [Test All] [Deploy App →]            │
    └──────────────────────────────────────────────────────────────────┘

User: "Looks good, but I also want to track order flow — can you add a query
       that joins trades with the orders topic?"

  → AI inspects the orders topic schema (if connected) or infers it
  → Adds a 5th query: Trade-Order Enrichment (JOIN)
  → Updates the AppPreviewArtifact with the new query + wiring
  → Updates the dashboard layout to include the new panel

User: [clicks Test All]
  → Tool: test_app(app_id) — tests ALL queries with coordinated synthetic data
  → Generates data for `trades` and `orders` with matching foreign keys
  → Runs all 5 queries, validates each independently
  → Artifact: TestResultsArtifact (consolidated)
    ✅ 5/5 queries passed
    • VWAP Calculator: 3 output records, 45ms
    • Volume Spike Detector: 1 alert, 52ms
    • Price Movement Tracker: 3 records, 38ms
    • Top Movers: 2 records, 22ms
    • Trade-Order Enrichment: 4 records, 89ms (JOIN)

User: [clicks Deploy App]
  → Tool: deploy_app(app_id, config)
  → Deploys all 5 queries as coordinated Velostream jobs
  → Registers all 6 metrics with Prometheus
  → Configures both alert rules
  → Generates and deploys unified 6-panel Grafana dashboard
  → Artifact: DeploySummaryArtifact
    • Pipeline: trading-analytics (5 jobs)
    • Dashboard: http://grafana:3000/d/trading-analytics
    • Alerts: 2 rules active
    • Status: All jobs running
```

### How the AI Designs an App

When the user says "build me an app", the AI doesn't just generate one query. It:

1. **Decomposes the request** into logical processing stages
2. **Designs the data flow** — which topics feed which queries, where outputs go
3. **Adds observability by default** — every query gets @metric annotations
4. **Creates alert rules** for business-relevant thresholds
5. **Designs the dashboard layout** — panels arranged by logical grouping
6. **Wires the topology** — multi-stage pipelines where one query's output feeds the next

If a Kafka connection is active, the AI uses real schemas to ensure field names match.
If not, it infers schemas from domain knowledge and generates synthetic data for testing.

### Backend Requirements

| API Endpoint | Description |
|-------------|-------------|
| `POST /api/generate-app` | AI generates multi-query application from NL description |
| `POST /api/apps/{id}/test` | Test all queries in an app with coordinated data |
| `POST /api/apps/{id}/deploy` | Deploy all queries + metrics + alerts + dashboard |
| `GET /api/apps/{id}` | Get app definition (queries, metrics, topology) |
| `PUT /api/apps/{id}` | Update app (add/remove/modify queries) |

---

## Journey 8: Start from a Template

> **Persona**: Developer who wants a proven starting point, not a blank canvas
> **Goal**: Select a pre-built application template, customize it for their data, and deploy
> **Entry point**: Template browser or "show me templates" in chat

### Template Library

Templates are **complete, tested application blueprints** for common streaming use cases.
Each template includes multiple SQL queries, metrics, alerts, a Grafana dashboard layout,
and a test spec. Templates are maintained as part of the Velostream distribution.

```
Available Templates:

┌────────────────────────────────────────────────────────────────────┐
│ 📦 Trading Analytics          │ 4 queries │ Finance              │
│ VWAP, volume monitoring, price alerts, top movers                 │
├────────────────────────────────────────────────────────────────────┤
│ 📦 Fraud Detection            │ 5 queries │ FinTech              │
│ Velocity checks, amount anomalies, geo-fencing, pattern matching  │
├────────────────────────────────────────────────────────────────────┤
│ 📦 IoT Device Monitoring      │ 3 queries │ IoT / Manufacturing  │
│ Sensor anomaly detection, fleet health, predictive maintenance    │
├────────────────────────────────────────────────────────────────────┤
│ 📦 API Observability          │ 4 queries │ Platform / SRE       │
│ Request rate, error rate, latency percentiles, SLO tracking       │
├────────────────────────────────────────────────────────────────────┤
│ 📦 E-Commerce Analytics       │ 5 queries │ Retail               │
│ Cart tracking, conversion funnel, revenue by region, inventory    │
├────────────────────────────────────────────────────────────────────┤
│ 📦 Clickstream Analytics      │ 3 queries │ AdTech / Product     │
│ Session tracking, page flow, engagement scoring                   │
├────────────────────────────────────────────────────────────────────┤
│ 📦 Log Analytics              │ 3 queries │ DevOps               │
│ Error rate tracking, pattern detection, alert aggregation         │
├────────────────────────────────────────────────────────────────────┤
│ 📦 AI Agent Monitoring        │ 4 queries │ AI / MLOps           │
│ Decision audit, latency tracking, confidence scoring, drift       │
└────────────────────────────────────────────────────────────────────┘
```

### Flow

```
User: "Show me templates"
  → Tool: list_templates()
  → API: GET /api/templates
  → Artifact: TemplateBrowserArtifact
    [Grid of template cards with descriptions, query counts, categories]

User: "I want the fraud detection template"
  → Tool: get_template(template_id: "fraud-detection")
  → API: GET /api/templates/fraud-detection
  → Artifact: AppPreviewArtifact (read-only template preview)
    Shows all 5 queries, metrics, alerts, dashboard layout, expected input schemas

User: "Customize this for my data — I have a 'transactions' topic on broker1:9092"
  → Tool: connect_source(uri: "kafka://broker1:9092")
  → Tool: inspect_topic(topic: "transactions")
  → Tool: customize_template(template_id: "fraud-detection", mappings: {
      source_topic: "transactions",
      field_mappings: {
        "amount" → "transaction_amount",
        "user_id" → "customer_id",
        "timestamp" → "event_time"
      }
    })
  → API: POST /api/templates/fraud-detection/customize
  → AI maps template's expected fields to actual topic fields
  → AI detects extra fields in real schema and suggests additional queries
  → Artifact: AppPreviewArtifact (customized version)

  → AI proactive suggestion:
    "I notice your transactions topic also has a `merchant_category` field
     and a `device_fingerprint` field that aren't in the standard template.
     Would you like me to add:
     • A merchant category anomaly detector?
     • A device fingerprint velocity check?
     These are common fraud signals."

User: "Yes, add both"
  → AI adds 2 more queries to the app, updates dashboard
  → Artifact: AppPreviewArtifact (7 queries now)

User: [clicks Test All] → Tests with real schema + synthetic data
User: [clicks Deploy App] → Full deployment
```

### Template Customization Process

When a user selects a template and connects it to real data, the AI performs:

1. **Schema mapping** — maps template's expected fields to actual topic fields
   - Exact name matches are auto-mapped
   - Similar names are suggested (e.g., `user_id` → `customer_id`)
   - Missing fields are flagged with alternatives
2. **Field type adaptation** — adjusts SQL types if the real data differs
   (e.g., template expects Integer but data has Float)
3. **Threshold calibration** — AI samples real data to suggest appropriate alert
   thresholds instead of using template defaults
   (e.g., "Your average transaction is $47, template default alert at $10K seems right,
    but I'd suggest a velocity alert at 5 transactions/minute based on your data patterns")
4. **Extra field discovery** — identifies fields in the real schema that aren't in
   the template and suggests additional queries that could use them
5. **Test data alignment** — generates synthetic data that matches the real schema
   for testing before deployment

### Template Definition Format

```yaml
# templates/fraud-detection.yaml
id: fraud-detection
name: Fraud Detection
description: Real-time fraud pattern detection for financial transactions
category: FinTech
version: "1.0"
tags: [fraud, finance, security, alerting]

# Expected input schema (mapped to actual fields during customization)
input:
  topic: transactions          # User maps this to their actual topic
  fields:
    - name: transaction_id
      type: String
      required: true
    - name: user_id
      type: String
      required: true
    - name: amount
      type: Float
      required: true
    - name: merchant_id
      type: String
      required: false
    - name: timestamp
      type: DateTime
      required: true
    - name: location
      type: String
      required: false

# Queries in execution order
queries:
  - name: velocity-check
    description: Flag users with too many transactions in a short window
    sql: |
      -- @metric: transaction_velocity
      -- @metric_type: gauge
      -- @metric_labels: user_id
      -- @alert: tx_count > 10
      -- @alert_severity: warning
      SELECT user_id, COUNT(*) as tx_count, SUM(amount) as total_amount
      FROM {input_topic}
      GROUP BY user_id
      WINDOW TUMBLING(INTERVAL '1' MINUTE)
      HAVING COUNT(*) > 5
      EMIT CHANGES
    output_topic: fraud-velocity-alerts

  - name: amount-anomaly
    description: Detect unusually large transactions
    sql: |
      -- @metric: large_transaction_count
      -- @metric_type: counter
      -- @alert: amount > {threshold_large_amount}
      SELECT transaction_id, user_id, amount, timestamp
      FROM {input_topic}
      WHERE amount > {threshold_large_amount}
      EMIT CHANGES
    output_topic: fraud-amount-alerts
    parameters:
      threshold_large_amount:
        default: 10000
        description: Transactions above this amount trigger alerts
        auto_calibrate: true   # AI adjusts based on real data sampling

  - name: geo-velocity
    description: Flag impossible travel (transactions from distant locations in short time)
    sql: |
      -- @metric: geo_anomaly_count
      -- @metric_type: counter
      SELECT t1.user_id, t1.location as loc1, t2.location as loc2,
             t1.timestamp as time1, t2.timestamp as time2
      FROM {input_topic} t1
      JOIN {input_topic} t2
        ON t1.user_id = t2.user_id
      WHERE t1.location != t2.location
      EMIT CHANGES
    output_topic: fraud-geo-alerts
    requires_fields: [location]  # Only included if location field exists

  # ... more queries

# Dashboard layout
dashboard:
  title: "Fraud Detection"
  panels:
    - title: Transaction Velocity
      query: transaction_velocity
      type: line
      position: { x: 0, y: 0, w: 12, h: 8 }
    - title: Large Transactions
      query: large_transaction_count
      type: bar
      position: { x: 12, y: 0, w: 12, h: 8 }
    - title: Geo Anomalies
      query: geo_anomaly_count
      type: gauge
      position: { x: 0, y: 8, w: 8, h: 6 }
    - title: Alert Feed
      type: table
      source: fraud-velocity-alerts
      position: { x: 8, y: 8, w: 16, h: 6 }

# Test specification
test:
  records: 5000
  seed: 42
  assertions:
    - type: record_count
      query: velocity-check
      greater_than: 0
    - type: record_count
      query: amount-anomaly
      greater_than: 0
```

### Backend Requirements

| API Endpoint | Description |
|-------------|-------------|
| `GET /api/templates` | List all available templates with metadata |
| `GET /api/templates/{id}` | Get full template definition |
| `POST /api/templates/{id}/customize` | Customize template for real data (field mapping, threshold calibration) |
| `POST /api/templates/{id}/preview` | Preview customized template as AppPreviewArtifact |

---

## Journey 9: AI Proactive Intelligence

> **Persona**: Any user, at any point in any journey
> **Goal**: The AI doesn't wait to be asked — it actively identifies opportunities, risks, and improvements
> **Entry point**: Happens automatically throughout all other journeys

This is not a standalone journey but a **cross-cutting capability** that enriches every
other journey. The AI monitors the conversation context and proactively intervenes when
it identifies something valuable.

### 9.1: Proactive Suggestions After Data Exploration

When the user explores a Kafka cluster (Journey 1), the AI analyzes what it finds and
suggests queries, applications, or templates before being asked.

```
User: "Connect to broker1:9092 and show me all topics"
  → [Normal exploration flow — TopicListArtifact shows 12 topics]

🤖 AI Proactive Analysis:
  "Looking at your topics, I notice some patterns:

   📊 You have a classic trading data pipeline:
   • trades (1.2M messages) — raw trade events
   • orders (450K messages) — order submissions
   • positions (89K messages) — portfolio positions

   I could build you a complete trading analytics app that:
   • Calculates real-time VWAP from trades
   • Joins orders with trades for fill rates
   • Monitors position risk from positions

   💡 Would you like me to build this? Or would you prefer to start
      with the 'Trading Analytics' template?

   I also see these topics that might need attention:
   ⚠️ dead-letter-queue has 12,400 messages — want me to inspect them?
   ⚠️ trades-v1 hasn't received messages in 3 days — possibly deprecated?"
```

### 9.2: Schema-Aware Query Optimization

After the user writes or generates SQL, the AI analyzes it against the actual data and
suggests improvements.

```
User: "Write a query to count orders per customer"
  → [Normal flow — generates SQL]

🤖 AI Proactive Analysis:
  "Query looks correct. A few observations from your data:

   ⚡ Performance: Your orders topic has 450K messages across 3 partitions.
     The GROUP BY customer_id will create ~15,000 groups. Consider adding
     HAVING COUNT(*) > 1 to reduce output volume by ~60%.

   📊 Suggestion: I notice orders has a `region` field. Adding it to
     GROUP BY would give you geographic breakdown at no extra cost:
     GROUP BY customer_id, region

   🔔 Monitoring: This is a natural place for an alert. Want me to add:
     -- @alert: order_count > 100
     -- @alert_severity: warning
     to flag unusually active customers?

   [Apply All] [Apply Performance Only] [Ignore]"
```

### 9.3: Auto-Annotation Recommendations

When the user writes SQL without @metric annotations, the AI suggests which metrics
would be valuable and why.

```
User: [pastes bare SQL]
    SELECT symbol, AVG(price) as avg_price, SUM(quantity) as volume
    FROM trades
    GROUP BY symbol
    WINDOW TUMBLING(INTERVAL '5' MINUTE)
    EMIT CHANGES

🤖 AI Proactive Analysis:
  "This query has no observability annotations. Based on the query structure,
   I recommend adding:

   Metrics:
   • avg_price (gauge, labels: symbol) — track price movements per symbol
   • volume (counter, labels: symbol) — total volume over time
   • window_output_count (counter) — records emitted per window
   • processing_latency_ms (histogram) — window processing time

   Alerts:
   • avg_price change > 5% in 1 minute — sudden price movement
   • volume spike > 2x rolling average — unusual activity

   Dashboard:
   • 4-panel Grafana layout: price chart, volume bars, alerts feed, latency

   Would you like me to add these annotations? I can:
   1. Add just the metrics (lightweight monitoring)
   2. Add metrics + alerts (active monitoring)
   3. Add metrics + alerts + dashboard (full observability)

   [Option 1] [Option 2] [Option 3 (Recommended)]"
```

### 9.4: Pipeline Health Monitoring

When the user deploys a pipeline, the AI continues to monitor its health and
proactively surfaces issues.

```
[After deployment, user returns to the Studio thread]

🤖 AI Proactive Update:
  "Your trading-analytics pipeline has been running for 2 hours. Status update:

   ✅ VWAP Calculator: 12,500 records/sec, p99 latency 8ms
   ✅ Volume Spike Detector: 3 alerts triggered (normal range)
   ⚠️ Price Movement Tracker: p99 latency increased from 5ms to 45ms
      └─ Root cause: symbol group count grew from 50 to 800.
         The WINDOW is accumulating too many groups.
      └─ Suggestion: Add WHERE symbol IN (top 100 by volume)
         or increase window interval to 15 minutes.
   ❌ Top Movers: Consumer lag growing (1,200 messages behind)
      └─ Root cause: Output topic partition count (1) is bottlenecking.
         The upstream produces to 6 partitions.
      └─ Suggestion: Increase top-movers topic to 6 partitions.

   [Apply Fix: Price Tracker] [Apply Fix: Top Movers] [Ignore]"
```

### 9.5: Schema Evolution Detection

When the AI detects that a source topic's schema has changed since the query was written,
it proactively alerts the user.

```
[User opens an existing notebook/thread]

🤖 AI Proactive Alert:
  "I checked the schemas of your source topics and found a change:

   📋 Topic 'orders' schema has evolved since your query was deployed:

   Added fields:
   + discount_code (String) — new since Feb 14
   + shipping_priority (String) — new since Feb 14

   Removed fields:
   - legacy_status (String) — no longer present

   Impact on your queries:
   • order-analytics: No impact (doesn't use removed field)
   • order-enrichment: ⚠️ Uses 'legacy_status' which no longer exists
     └─ This query will produce null for the legacy_status field
     └─ Suggested fix: Replace with new 'shipping_priority' field

   [View Diff] [Apply Suggested Fix] [Ignore]"
```

### 9.6: NL-to-Dashboard (Skip the SQL)

For non-technical users who don't even want to see SQL — go directly from English to
a live dashboard.

```
User: "I just want a dashboard showing trade volume and price by symbol,
       updating every minute. I don't need to see the SQL."

  → AI generates the complete app internally (queries + metrics + dashboard)
  → Skips showing SqlEditorArtifact — goes straight to dashboard preview
  → Tool: generate_app(prompt, options: { skip_sql_preview: true })

  → Artifact: GrafanaEmbed (live dashboard preview)
    ┌──────────────────────────────────────────────────────────────┐
    │ Trading Dashboard (Preview)                                   │
    │                                                              │
    │ ┌──────────────────────┬────────────────────────┐            │
    │ │ Volume by Symbol     │ Avg Price by Symbol    │            │
    │ │ AAPL ████████ 125K   │ AAPL  $185.42          │            │
    │ │ TSLA ██████ 89K      │ TSLA  $242.10          │            │
    │ │ MSFT ████ 67K        │ MSFT  $378.91          │            │
    │ └──────────────────────┴────────────────────────┘            │
    │                                                              │
    │ Updating every 1 minute                                      │
    │                [View SQL] [Edit] [Deploy →]                  │
    └──────────────────────────────────────────────────────────────┘

  → "Here's your dashboard. I created 2 queries behind the scenes
     with @metric annotations. Click [View SQL] if you want to see
     or edit the underlying queries. Otherwise, click [Deploy] to
     go live."

User: [clicks Deploy]
  → Full deployment without ever seeing SQL
```

### How Proactive Intelligence Works

The AI proactive features are powered by **context analysis at every step**:

| Trigger | What the AI Analyzes | What It Surfaces |
|---------|---------------------|-----------------|
| After `connect_source` | Source data (topics, files, tables), sizes, patterns | Suggested apps, templates, stale topics |
| After `inspect_topic` | Schema fields and types | Related queries, joins with other topics, potential issues |
| After `generate_sql` | Query structure vs data | Performance tips, missing metrics, optimization opportunities |
| After `test_query` | Test results and data patterns | Threshold suggestions, edge cases, additional assertions |
| After `deploy_pipeline` | Job metrics over time | Latency issues, consumer lag, scaling recommendations |
| On thread open | Source schemas vs deployed queries | Schema evolution alerts, deprecated field warnings |
| After `list_topics` | Topic naming patterns | Pipeline topology suggestions, DLQ inspection |

### Backend Requirements

| API Endpoint | Description |
|-------------|-------------|
| `POST /api/analyze` | Analyze thread context and return proactive suggestions |
| `POST /api/analyze/schema-diff` | Compare current topic schemas against deployed queries |
| `POST /api/analyze/performance` | Analyze running job metrics and suggest optimizations |
| `POST /api/analyze/annotations` | Suggest @metric and @alert annotations for bare SQL |

---

## Journey 10: Work with Files, S3, and Databases

> **Persona**: Data engineer or analyst with data in files, object storage, or databases
> **Goal**: Use Velostream Studio with non-Kafka data sources
> **Entry point**: Studio chat thread

Velostream supports URI-based data sources beyond Kafka. The Studio generalizes the
exploration model to work with any supported source type.

### Flow: File-Based Streaming

```
User: "I have CSV trade data at /data/trades/*.csv, show me the schema"
  → Tool: connect_source(uri: "file:///data/trades/*.csv", format: "csv")
  → Studio infers schema from first file
  → Artifact: SchemaViewerArtifact
    │ Field     │ Type     │ Example          │
    │ symbol    │ String   │ "AAPL"           │
    │ price     │ Float    │ 152.34           │
    │ qty       │ Integer  │ 5000             │
    │ ts        │ String   │ "2026-02-16..."  │

User: "Preview the first 10 rows"
  → Tool: peek_source(uri: "file:///data/trades/*.csv", limit: 10)
  → Artifact: DataPreviewArtifact (tabular rows instead of JSON messages)

User: "Calculate VWAP per symbol"
  → Tool: generate_sql(prompt, context: { schemas: { source: inferred_schema } })
  → AI generates:
    SELECT symbol, SUM(price * qty) / SUM(qty) as vwap
    FROM file:///data/trades/*.csv
    GROUP BY symbol
    EMIT CHANGES
  → Artifact: SqlEditorArtifact

User: [clicks Run]
  → Executes against local file data
  → Artifact: QueryResultsArtifact with live bar chart
```

### Flow: High-Throughput Batch with FileMmap

```
User: "I need to process a 50GB JSON file at /data/large_events.json as fast as possible"
  → AI recognizes large file use case
  → Response: "For large files, I recommend memory-mapped I/O (FileMmap) for
    maximum throughput. This uses OS virtual memory to stream through the file
    without loading it all into RAM."

User: "Yes, use mmap"
  → Tool: connect_source(uri: "file:///data/large_events.json", options: { use_mmap: true, format: "json" })
  → Schema inferred from first records
  → Artifact: SchemaViewerArtifact

User: "Aggregate events by type and count per hour"
  → Tool: generate_sql(prompt, context: { source_type: "file_mmap" })
  → AI generates SQL optimized for batch processing:
    SELECT event_type, COUNT(*) as event_count
    FROM file:///data/large_events.json
    GROUP BY event_type
    WINDOW TUMBLING(INTERVAL '1' HOUR)
    EMIT CHANGES
  → Artifact: SqlEditorArtifact
```

### Flow: S3 Object Storage

```
User: "Connect to my S3 bucket s3://analytics-data/events/ — it has Parquet files"
  → Tool: connect_source(uri: "s3://analytics-data/events/", format: "parquet", options: { region: "us-west-2" })
  → Lists objects, infers schema from Parquet metadata
  → Artifact: SchemaViewerArtifact (Parquet schema is exact — no inference needed)

User: "Show me the data"
  → Tool: peek_source(uri: "s3://analytics-data/events/", limit: 10)
  → Artifact: DataPreviewArtifact

User: "Build me an anomaly detection app"
  → Tool: generate_app(prompt, context: { source_uri: "s3://analytics-data/events/", schema: ... })
  → Artifact: AppPreviewArtifact (multi-query app reading from S3)
```

### Flow: Database CDC

```
User: "Connect to my Postgres at postgres://db:5432/orders_db"
  → Tool: connect_source(uri: "postgres://db:5432/orders_db")
  → Lists tables, shows schemas
  → Artifact: SchemaViewerArtifact (multiple tables)

User: "Stream changes from the orders table"
  → AI generates CDC-aware SQL:
    SELECT * FROM postgres://db:5432/orders_db?table=orders
    EMIT CHANGES
  → Artifact: SqlEditorArtifact

User: "Replicate to Kafka"
  → AI generates a replication pipeline:
    CREATE STREAM order_replication AS
    SELECT * FROM postgres://db:5432/orders_db?table=orders
    INTO kafka://broker:9092/orders-replicated
    EMIT CHANGES
```

### Supported Source URIs

| URI Scheme | Source Type | Streaming | Batch (Mmap) | Formats |
|-----------|-------------|:---------:|:------------:|---------|
| `kafka://host:port/topic` | Kafka | Yes | — | JSON, Avro, Protobuf |
| `file:///path/to/data` | File | Yes (watch) | Yes | CSV, JSON, JSONL, Parquet, Avro, ORC |
| `s3://bucket/prefix` | S3 | — | Yes | CSV, JSON, JSONL, Parquet, Avro, ORC |
| `clickhouse://host:port/db` | ClickHouse | — | Yes | Native |
| `postgres://host:port/db` | Database (CDC) | Yes | — | Row-level changes |

### Backend Requirements

| API Endpoint | Description |
|-------------|-------------|
| `POST /api/connect` | Generalized source connection (any URI scheme) |
| `GET /api/sources/{id}/schema` | Infer or read schema from connected source |
| `GET /api/sources/{id}/preview` | Preview data from source (rows or messages) |
| `GET /api/sources/{id}/stats` | Source statistics (record count, size, partitions) |

### Key Insight

The Studio's exploration model is **URI-driven**. Whether data is in Kafka topics,
local CSV files, S3 Parquet datasets, or Postgres tables, the user experience is
consistent: connect → explore schema → preview data → generate SQL → test → deploy.
The AI adapts its SQL generation based on the source type (e.g., using FileMmap for
large local files, suggesting batch strategies for S3, or CDC semantics for databases).

---

## Cross-Cutting Concerns

### Session & Connection Model

The Studio maintains a **session** that persists across messages in a thread. Data source
connections are stored in the `ThreadContext` and passed to backend API calls. The connection
model is **URI-driven** — any supported data source (Kafka, File, FileMmap, S3, ClickHouse,
Database) uses the same connect → explore → query pattern.

```typescript
interface ThreadContext {
  // Core fields
  sources: Map<string, SourceConfig>;
  sinks: Map<string, SinkConfig>;
  metrics: MetricAnnotation[];
  alerts: AlertAnnotation[];
  schemas: Map<string, DataSchema>;

  // Data source connections (one or more, keyed by alias or URI)
  // Supports: kafka://, file://, s3://, clickhouse://, postgres://
  connections: Map<string, DataSourceConnection>;

  // App generation state (set by generate_app / customize_template tools)
  app?: {
    id: string;
    name: string;
    queries: AppQuery[];
    dashboard?: DashboardLayout;
    templateId?: string;         // if created from a template
  };
}

interface DataSourceConnection {
  uri: string;                   // e.g. "kafka://broker1:9092", "file:///data/trades.csv"
  type: 'kafka' | 'file' | 'file_mmap' | 's3' | 'clickhouse' | 'database';
  format?: 'json' | 'csv' | 'jsonl' | 'parquet' | 'avro' | 'orc' | 'protobuf';
  options?: Record<string, string>;  // source-specific options (region, delimiter, etc.)
  schemaRegistryUrl?: string;    // Kafka-specific
  connected_at: string;
}
```

When connections are active, exploration tools automatically use them. Multiple connections
can coexist (e.g., Kafka for streaming input + S3 for historical lookups). The AI uses the
source type to tailor SQL generation — for example, suggesting FileMmap for large local
files, batch strategies for S3, or CDC semantics for database sources.

When an app is being built (Journey 7 or 8), the `app` field tracks the multi-query
application state, including all generated queries, dashboard layout, and template origin.

### Complete Tool Registry

| Tool | Description | API Endpoint | Phase | Artifact |
|------|-------------|--------------|-------|----------|
| **Exploration Tools** | | | | |
| `connect_source` | Connect to any data source (Kafka, File, S3, DB) | `POST /api/connect` | 1.6 | — (confirmation text) |
| `list_sources` | List topics/files/tables on connected source | `GET /api/sources` | 1.6 | `TopicListArtifact` / `DataPreviewArtifact` |
| `inspect_source` | Infer schema from source samples | `GET /api/sources/{id}/schema` | 1.6 | `SchemaViewerArtifact` |
| `peek_source` | Preview data from source (messages/rows) | `GET /api/sources/{id}/preview` | 1.6 | `DataPreviewArtifact` |
| **SQL Tools** | | | | |
| `generate_sql` | Generate SQL from natural language | `POST /api/nl-to-sql` | 2 | `SqlEditorArtifact` |
| `validate_sql` | Validate SQL syntax | `POST /api/validate` | 2 | `SqlEditorArtifact` |
| `execute_query` | Execute SQL and return results | `POST /api/execute` | 2 | `QueryResultsArtifact` |
| **Test Tools** | | | | |
| `test_query` | Test SQL with synthetic data | `POST /api/test` | 3 | `TestResultsArtifact` |
| `generate_data` | Generate synthetic test data | `POST /api/generate-data` | 3 | `DataPreviewArtifact` |
| **Observability Tools** | | | | |
| `generate_dashboard` | Generate Grafana dashboard | `POST /api/dashboards/generate` | 4 | `GrafanaEmbed` |
| **Deployment Tools** | | | | |
| `deploy_pipeline` | Deploy notebook as pipeline | `POST /api/deploy` | 5 | `DeploySummaryArtifact` |
| **App Generation Tools** | | | | |
| `generate_app` | Generate multi-query app from NL | `POST /api/generate-app` | 1.7 | `AppPreviewArtifact` |
| `test_app` | Test all queries in an app | `POST /api/apps/{id}/test` | 1.7 | `TestResultsArtifact` |
| `deploy_app` | Deploy all app queries + dashboard | `POST /api/apps/{id}/deploy` | 1.7 | `DeploySummaryArtifact` |
| **Template Tools** | | | | |
| `list_templates` | Browse available templates | `GET /api/templates` | 1.7 | `TemplateBrowserArtifact` |
| `get_template` | Get template detail and preview | `GET /api/templates/{id}` | 1.7 | `AppPreviewArtifact` |
| `customize_template` | Customize template for user's data | `POST /api/templates/{id}/customize` | 1.7 | `AppPreviewArtifact` |
| **AI Analysis Tools** | | | | |
| `analyze` | Proactive AI suggestions for thread | `POST /api/analyze` | 6 | — (inline suggestions) |
| `analyze_schema_diff` | Detect schema changes vs deployed | `POST /api/analyze/schema-diff` | 6 | `SchemaViewerArtifact` |
| `analyze_performance` | Suggest performance optimizations | `POST /api/analyze/performance` | 6 | — (inline suggestions) |
| `analyze_annotations` | Suggest @metric/@alert annotations | `POST /api/analyze/annotations` | 6 | `SqlEditorArtifact` |

### Artifact Types

| Artifact Type | Description | Rendered By | Introduced |
|---------------|-------------|-------------|------------|
| `sql-editor` | Editable Monaco SQL with Run/Test/Deploy buttons | `SqlEditorArtifact.tsx` | Phase 2 |
| `query-results` | Auto-selected chart or table with live data | `QueryResultsArtifact.tsx` | Phase 2 |
| `schema-viewer` | Field names, types, and sample values | `SchemaViewerArtifact.tsx` | Phase 2 |
| `test-results` | Pass/fail assertions with AI failure analysis | `TestResultsArtifact.tsx` | Phase 3 |
| `deploy-summary` | Jobs, metrics, alerts, Grafana dashboard link | `DeploySummaryArtifact.tsx` | Phase 5 |
| `topology` | React Flow pipeline DAG with live metrics | `TopologyArtifact.tsx` | Phase 4 |
| `grafana-embed` | Embedded Grafana dashboard iframe | `GrafanaEmbed.tsx` | Phase 4 |
| `topic-list` | Topic grid with partition counts and message counts | `TopicListArtifact.tsx` | Phase 2.8 |
| `data-preview` | Formatted JSON messages with offset/partition metadata | `DataPreviewArtifact.tsx` | Phase 2.8 |
| `app-preview` | Multi-query app with queries, metrics, alerts, dashboard layout, topology | `AppPreviewArtifact.tsx` | Phase 2.9 |
| `template-browser` | Template library grid with categories, descriptions, preview | `TemplateBrowserArtifact.tsx` | Phase 2.9 |

### Journey-to-Phase Mapping

| Journey | Phase 1 (Backend) | Phase 1.6 (Explore API) | Phase 1.7 (App/Template API) | Phase 2 (Frontend) | Phase 2.8 (Explore UI) | Phase 2.9 (App/Template UI) | Phase 3 (Test) | Phase 4 (Observe) | Phase 5 (Deploy) | Phase 6 (AI Intelligence) |
|---------|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| 1. Explore First | | x | | x | x | | x | | x | x |
| 2. Greenfield | x | | | x | | | x | | x | x |
| 3. Import SQL | x | | | x | | | x | | x | x |
| 4. Debug & Iterate | | x | | x | x | | x | | x | x |
| 5. Observe & Monitor | x | | | x | | | | x | x | x |
| 6. Team Onboarding | x | | | x | | | x | | x | |
| 7. Build Me an App | | x | x | x | x | x | x | x | x | x |
| 8. Start from Template | | | x | x | | x | x | | x | x |
| 9. AI Proactive Intelligence | | x | | x | | | | x | | x |
| 10. Files, S3, & Databases | x | x | x | x | x | x | x | | x | x |

---

## Infra Method Reference

The exploration endpoints delegate to existing `TestHarnessInfra` methods
in `src/velostream/test_harness/infra.rs`:

| Method | Signature | Returns |
|--------|-----------|---------|
| `fetch_topic_info` | `(&self, topic_filter: Option<&str>) -> Result<Vec<TopicInfo>>` | Topics with partition counts, message counts, watermarks |
| `fetch_topic_schema` | `(&self, topic: &str, max_records: usize) -> Result<TopicSchema>` | Inferred field names/types, sample JSON, has_keys flag |
| `peek_topic_messages` | `(&self, topic: &str, limit: usize, from_end: bool, start_offset: Option<i64>, partition_filter: Option<i32>) -> Result<Vec<TopicMessage>>` | Messages with partition, offset, key, value, timestamp, headers |
| `get_consumer_info` | `(&self) -> Result<Vec<ConsumerInfo>>` | Consumer groups with subscriptions, positions, state |

### Return Types (from `statement_executor.rs`)

```rust
pub struct TopicInfo {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub total_messages: i64,
    pub is_test_topic: bool,
}

pub struct TopicSchema {
    pub topic: String,
    pub fields: Vec<(String, String)>,        // (name, type)
    pub sample_value: Option<String>,          // first record as JSON
    pub has_keys: bool,
    pub records_sampled: usize,
}

pub struct TopicMessage {
    pub partition: i32,
    pub offset: i64,
    pub key: Option<String>,
    pub value: String,                         // JSON string
    pub timestamp_ms: Option<i64>,
    pub headers: Vec<(String, String)>,
}

pub struct ConsumerInfo {
    pub group_id: String,
    pub subscribed_topics: Vec<String>,
    pub positions: Vec<ConsumerPosition>,
    pub state: ConsumerState,
}
```
