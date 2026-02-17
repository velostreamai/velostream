# FR-085: Velostream SQL Studio

## Vision

**Velostream** is the real-time data layer for AI — a streaming SQL platform with turnkey applications for AI observability, data replication, and governance.

**Velostream Studio** is the AI-powered notebook interface that makes streaming SQL development as easy as typing English.

```
┌─────────────────────────────────────────────────────────────┐
│                      VELOSTREAM                              │
│           The Real-Time Data Layer for AI                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  PLATFORM (Open Source)                                      │
│  • Streaming SQL engine                                      │
│  • 42x faster financial precision                            │
│  • PyFlink replacement (<10µs Python)                        │
│  • Test harness with synthetic data                          │
│                                                              │
│  TURNKEY APPS (Commercial)                                   │
│  • AI Black Box Recorder    — Audit every AI decision       │
│  • Cluster Linker           — 80% cheaper than Confluent    │
│  • AI Semantic Lineage      — Explain what data drove AI    │
│                                                              │
│  ENTERPRISE (License)                                        │
│  • RBAC, SSO, Audit Logs                                    │
│  • Multi-node clustering                                     │
│  • Chaos + regression testing                                │
│                                                              │
│  STUDIO (This Feature)                                       │
│  • AI-powered notebook                                       │
│  • NL→SQL + live charts                                      │
│  • One-click deploy                                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Quick Demo

### 30-Second Dashboard

```bash
# Install
curl -sSL https://velostream.dev/install | bash

# Run a query with instant Grafana dashboard
echo "SELECT symbol, AVG(price) FROM kafka://trades GROUP BY symbol" | \
  velo run --dashboard

# Browser opens: Live dashboard updating in real-time
```

### SQL That Tests Itself

```bash
# Test your SQL with synthetic data
velo test query.sql --records 10000

# Output:
# ✅ Passed (3/3 assertions)
# • record_count: 7 (expected: > 0)
# • no_nulls: [symbol, volume]
# • execution_time: 45ms (expected: < 1000ms)
```

### Talk to Your Streams

```
💬 "Show me fraud patterns by region in real-time"

🤖 AI Generated:
┌──────────────────────────────────────────────────────────┐
│ SELECT region, COUNT(*) as fraud_count,                  │
│        AVG(amount) as avg_amount                         │
│ FROM transactions                                        │
│ WHERE fraud_score > 0.8                                  │
│ GROUP BY region                                          │
│ WINDOW TUMBLING('1 minute')                              │
└──────────────────────────────────────────────────────────┘

📊 Auto-selected: Geo Heatmap (updating live)
```

### Explore Your Data

```
💬 "Connect to my Kafka at broker1:9092 and show me what's there"

🤖 Connected. Found 12 topics:
┌──────────────────────────────────────────────────────────────┐
│ trades           │ 6 partitions │ 1.2M messages             │
│ orders           │ 3 partitions │ 450K messages             │
│ customer-events  │ 1 partition  │ 89K messages              │
│ ...              │              │                            │
└──────────────────────────────────────────────────────────────┘

💬 "What does the trades topic look like?"

🤖 Inferred schema from 10 samples:
┌──────────────────────────────────────────────────────────────┐
│ Field     │ Type     │ Example                               │
├───────────┼──────────┼───────────────────────────────────────┤
│ symbol    │ String   │ "AAPL"                                │
│ price     │ Float    │ 152.34                                │
│ quantity  │ Integer  │ 5000                                  │
│ timestamp │ DateTime │ 2026-02-16T10:30:00Z                  │
└──────────────────────────────────────────────────────────────┘

💬 "Write me a real-time VWAP query"

🤖 Generated (using your actual schema):
┌──────────────────────────────────────────────────────────────┐
│ SELECT symbol,                                                │
│        SUM(price * quantity) / SUM(quantity) as vwap,         │
│        SUM(quantity) as volume                                │
│ FROM trades                                                   │
│ GROUP BY symbol                                               │
│ WINDOW TUMBLING(INTERVAL '5' MINUTE)                          │
│ EMIT CHANGES                                                  │
└──────────────────────────────────────────────────────────────┘
```

No more guessing field names — the Studio discovers your real data and uses it as context.

### Build Me an App

```
💬 "Build me a real-time trading analytics app that tracks VWAP,
    detects volume spikes, and shows top movers"

🤖 Generated "Trading Analytics" — 4 queries, 6 metrics, 2 alerts:
┌──────────────────────────────────────────────────────────────┐
│ 📋 Trading Analytics                                         │
│                                                              │
│ Queries:                                                     │
│ 1. VWAP Calculator          trades → trade-vwap              │
│ 2. Volume Spike Detector    trades → trade-alerts            │
│ 3. Price Movement Tracker   trades → price-movements         │
│ 4. Top Movers               price-movements → top-movers     │
│                                                              │
│ Dashboard: 6-panel Grafana layout                            │
│  ┌────────────┬────────────┐                                 │
│  │ VWAP/Symbol│ Volume/Sym │                                 │
│  ├────────────┼────────────┤                                 │
│  │ Volatility │ Top Movers │                                 │
│  └────────────┴────────────┘                                 │
│                                                              │
│              [Edit Queries] [Test All] [Deploy App →]        │
└──────────────────────────────────────────────────────────────┘

💬 [clicks Test All] → ✅ 4/4 queries passed
💬 [clicks Deploy App] → 4 jobs running, dashboard live
```

One sentence → complete streaming application with monitoring.

### Start from a Template

```
💬 "Show me templates for IoT"

🤖 Found 2 templates:
┌─────────────────────────────────────────────┐
│ 🏭 IoT Sensor Monitoring                    │
│ Sensor health, anomaly detection, fleet     │
│ aggregation. 5 queries, 8 metrics.          │
│                          [Preview] [Use →]  │
├─────────────────────────────────────────────┤
│ 📡 IoT Fleet Tracker                        │
│ Device location, connectivity, uptime.      │
│ 3 queries, 4 metrics.                       │
│                          [Preview] [Use →]  │
└─────────────────────────────────────────────┘

💬 [clicks Use on "IoT Sensor Monitoring"]
💬 "My sensor data is on topic 'factory-sensors' with fields
    device_id, temperature, humidity, pressure, ts"

🤖 Mapped template to your schema:
   sensor_id → device_id ✅
   reading   → temperature ✅ (primary), humidity, pressure (added)
   timestamp → ts ✅

   Customized 5 queries for your data.
   [Test All] [Deploy →]
```

Zero SQL required — pick a template, point it at your data, deploy.

### Any Data Source

```
# Kafka streaming
💬 "Connect to kafka://broker1:9092 and show me topics"

# Local files (CSV, JSON, Parquet)
💬 "Show me the schema of /data/trades.csv"

# High-throughput batch (memory-mapped I/O for large files)
💬 "Process /data/50gb-events.json using mmap for max performance"

# S3 object storage
💬 "Connect to s3://analytics-data/events/ — it's Parquet files"

# Database CDC
💬 "Stream changes from postgres://db:5432/orders_db"

# The Studio adapts SQL generation to each source type:
# • Kafka: streaming with partitions, consumer groups, schema registry
# • File/FileMmap: batch or file-watching, format detection
# • S3: object listing, Parquet metadata, compression
# • Database: CDC semantics, table listing, schema introspection
```

One interface for all your data — streaming, batch, and hybrid.

---

## Why Velostream?

### vs. Apache Flink

| Pain Point | Flink | Velostream |
|------------|-------|------------|
| Learning curve | Java/Scala required | SQL-first |
| Python performance | PyFlink: 1-10ms bridge overhead | <10µs Python IPC |
| Testing | Manual fixtures, no synthetic data | Built-in test harness |
| Observability | External setup | @metrics in SQL |

### vs. Lenses.io

| Feature | Lenses.io | Velostream |
|---------|-----------|------------|
| NL→SQL | ✅ via MCP | ✅ Native |
| Live Charts | ❌ Tables only | ✅ Auto-selected |
| Test Harness | ❌ None | ✅ Synthetic data + assertions |
| Notebook UI | ❌ Explorer | ✅ Cell-based |
| Open Source | ❌ Enterprise only | ✅ Apache 2.0 |

### vs. Databricks

| Feature | Databricks | Velostream |
|---------|------------|------------|
| Streaming | ⚠️ Structured Streaming | ✅ Native streaming SQL |
| Real-time latency | Seconds-minutes | Milliseconds |
| Financial precision | ❌ Float64 | ✅ ScaledInteger (42x faster) |
| Deployment | Complex notebooks | One-click deploy |
| Pricing | $$$$ | Open source core |

---

## Product Portfolio

### Open Source (Apache 2.0)

Everything you need to build streaming SQL applications:

- **Streaming SQL Engine** — Parse, plan, execute streaming queries
- **Connectors** — Kafka, Postgres, Redis, File
- **Test Harness (FR-084)** — Synthetic data generation, assertions, AI failure analysis
- **@metric Annotations** — SQL comments → Prometheus metrics
- **CLI Tools** — `velo run`, `velo test`, `velo validate`
- **Studio (Basic)** — Notebook UI, NL→SQL, visualization

### Turnkey Apps (Commercial)

Pre-built solutions for specific use cases:

| App | What It Does | Pricing |
|-----|--------------|---------|
| **AI Black Box Recorder** | Capture, query, replay all AI agent decisions | $0.001/decision |
| **Cluster Linker** | Cross-cluster Kafka replication (any vendor) | $0.05/GB |
| **AI Semantic Lineage** | Trace what data influenced AI decisions | $2K-20K/month |

### Enterprise License (Commercial)

Features for scale, security, and compliance:

| Feature | Description |
|---------|-------------|
| SSO/SAML/OIDC | Enterprise identity integration |
| RBAC | Role-based access control |
| Audit Logging | SOC2/HIPAA compliance |
| Multi-node Clustering | Horizontal scaling |
| Chaos Testing | Inject failures before deploy |
| Pipeline Lineage | Impact analysis for changes |

---

## Studio Features

### 1. AI-Powered Notebook

```
┌─ Cell 1 ─────────────────────────────────────────────────────────┐
│ 💬 "Show me trading volume by symbol for the last hour"          │
├──────────────────────────────────────────────────────────────────┤
│ SELECT symbol, SUM(quantity) as volume                           │
│ FROM trades                                                      │
│ GROUP BY symbol                                                  │
│ WINDOW TUMBLING(INTERVAL '5' MINUTE)                             │
│ EMIT CHANGES                               [Edit] [Run] [Test]   │
├──────────────────────────────────────────────────────────────────┤
│ 📊 [Bar Chart: Volume by Symbol - LIVE]                          │
│     AAPL ████████████ 125,000                                    │
│     TSLA ████████ 89,000                                         │
└──────────────────────────────────────────────────────────────────┘
```

### 2. Integrated Testing

Click `[Test]` on any cell:

```
┌─ Test Results ───────────────────────────────────────────────────┐
│ ✅ Passed (3/3 assertions)                                       │
│                                                                  │
│ ✓ record_count: 7 (expected: > 0)                                │
│ ✓ schema_contains: [symbol, volume]                              │
│ ✓ no_nulls: [symbol, volume]                                     │
│                                                                  │
│ Performance:                                                     │
│   • Execution time: 45ms                                         │
│   • Throughput: 222,222 records/sec                              │
│                                                                  │
│ [View Full Report] [Add More Assertions]                         │
└──────────────────────────────────────────────────────────────────┘
```

### 3. AI Failure Analysis

When tests fail, Claude explains why:

```
┌─ Test Results ───────────────────────────────────────────────────┐
│ ❌ Failed (1/3 assertions)                                       │
│                                                                  │
│ ✗ join_coverage: 0% match (expected: > 80%)                      │
│                                                                  │
│ 🤖 AI Analysis:                                                  │
│ The JOIN on 'customer_id' produced no matches because:           │
│ • trades contains customer_ids: [CUST001, CUST002, CUST003]      │
│ • customers table contains: [C-100, C-200, C-300]                │
│                                                                  │
│ Suggested fix: Add a foreign key relationship in your schema.   │
│                                                                  │
│ [Apply Fix] [Regenerate Data] [Ignore]                           │
└──────────────────────────────────────────────────────────────────┘
```

### 4. SQL-Native Observability

```sql
-- @metric: trade_volume
-- @metric_type: counter
-- @metric_labels: symbol
-- @alert: volume > 1000000

SELECT symbol, SUM(quantity) as volume
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
```

Click `[Deploy]` → Auto-generated Grafana dashboard.

### 5. One-Click Deploy

```
┌─ Deploy Summary ─────────────────────────────────────────────────┐
│ 📋 Notebook: Trading Analytics                                   │
│                                                                  │
│ Will deploy:                                                     │
│ ☑️ 2 streaming SQL jobs                                          │
│ ☑️ 3 @metrics → Prometheus                                       │
│ ☑️ 1 @alert → AlertManager                                       │
│ ☑️ Auto-generated Grafana dashboard                              │
│                                                                  │
│ [Preview Dashboard]                    [Cancel] [Deploy →]       │
└──────────────────────────────────────────────────────────────────┘
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      USER INTERFACES                             │
│  Studio (Web)  │  CLI  │  REST API  │  MCP Server (AI Agents)   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      VELOSTREAM CORE                             │
│  SQL Engine  │  Connectors  │  Test Harness  │  Observability   │
└────────────────────────────┬────────────────────────────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Black Box      │ │  Cluster        │ │  Semantic       │
│  Recorder       │ │  Linker         │ │  Lineage        │
└─────────────────┘ └─────────────────┘ └─────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ENTERPRISE FEATURES                           │
│  Auth (SSO)  │  Audit  │  Clustering  │  Chaos  │  Lineage      │
└─────────────────────────────────────────────────────────────────┘
```

See [ARCHITECTURE.md](./ARCHITECTURE.md) for detailed technical architecture.

---

## Tech Stack

| Component | Technology | Role |
|-----------|------------|------|
| **Core Engine** | Rust | Streaming SQL execution |
| **Studio Backend** | Rust (Axum) | REST API, WebSocket streaming |
| **Studio Frontend** | Next.js 14, React, TypeScript | App shell, routing |
| **Chat Framework** | assistant-ui | Thread management, streaming, tool results, artifact panels |
| **Chat UI** | shadcn.io/ai + shadcn/ui | Message bubbles, tool cards, code blocks, theming |
| **SQL Editor** | Monaco Editor | Velostream syntax highlighting, schema-aware autocomplete |
| **Visualization** | Recharts | Time series, bar charts, gauges (rendered as artifacts) |
| **Data Tables** | TanStack Table | Virtual scrolling query results |
| **Topology** | React Flow | Pipeline DAG visualization |
| **AI** | Claude API (Anthropic) | NL→SQL, completions, failure analysis |
| **Styling** | Tailwind CSS | Utility-first CSS |

---

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | High-level product architecture |
| [NOTEBOOK_DESIGN.md](./NOTEBOOK_DESIGN.md) | Detailed Studio/notebook implementation |
| [API.md](./API.md) | REST API specification |
| [USER_JOURNEYS.md](./USER_JOURNEYS.md) | User workflows and exploration flows |
| [TODO.md](./TODO.md) | Implementation tasks and progress |
| [COMPETITIVE_ANALYSIS.md](./COMPETITIVE_ANALYSIS.md) | Market analysis and positioning |

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Time to first "wow" | < 30 seconds |
| NL→SQL success rate | > 90% valid SQL |
| NL→App generation success | > 85% deployable apps |
| Template customization time | < 60 seconds |
| Test feedback loop | < 5 seconds |
| Notebook → Deploy | < 2 minutes |
| AI analysis helpfulness | > 70% resolve failures |
| Proactive suggestion acceptance | > 40% applied |

---

## Roadmap

The chat-first architecture with assistant-ui dramatically reduces frontend effort,
collapsing the original Phases 2-5 into a single phase.

| Phase | Focus | Status |
|-------|-------|--------|
| **Phase 1** | Studio Backend (Axum REST API + WebSocket + Exploration) | 📋 Planned |
| **Phase 1.7** | App Generation & Templates API | 📋 Planned |
| **Phase 2** | Chat-First Frontend (assistant-ui + shadcn.io/ai + Monaco + Recharts) | 📋 Planned |
| **Phase 2.9** | App & Template Artifacts (AppPreview, TemplateBrowser) | 📋 Planned |
| **Phase 3** | Test Harness Integration (FR-084 via tool results) | 📋 Planned |
| **Phase 4** | Observability + Topology (React Flow, Grafana embed) | 📋 Planned |
| **Phase 5** | Notebook Lifecycle + Deployment | 📋 Planned |
| **Phase 6** | AI Proactive Intelligence (suggestions, schema monitoring) | 📋 Planned |

> **Note**: MCP Server, AI Black Box Recorder, Cluster Linker, and Enterprise Features
> are separate product initiatives — see [ARCHITECTURE.md](./ARCHITECTURE.md) for the
> full product portfolio roadmap.

See [TODO.md](./TODO.md) for detailed task breakdown.
