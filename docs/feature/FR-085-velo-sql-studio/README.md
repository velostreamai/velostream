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

| Component | Technology |
|-----------|------------|
| **Core Engine** | Rust |
| **Studio Backend** | Rust (Axum) |
| **Studio Frontend** | Next.js 14, React, TypeScript |
| **SQL Editor** | Monaco Editor |
| **Visualization** | Recharts, TanStack Table |
| **AI** | Claude API (Anthropic) |
| **Styling** | Tailwind CSS, shadcn/ui |

---

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](./ARCHITECTURE.md) | High-level product architecture |
| [NOTEBOOK_DESIGN.md](./NOTEBOOK_DESIGN.md) | Detailed Studio/notebook implementation |
| [API.md](./API.md) | REST API specification |
| [TODO.md](./TODO.md) | Implementation tasks and progress |
| [COMPETITIVE_ANALYSIS.md](./COMPETITIVE_ANALYSIS.md) | Market analysis and positioning |

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Time to first "wow" | < 30 seconds |
| NL→SQL success rate | > 90% valid SQL |
| Test feedback loop | < 5 seconds |
| Notebook → Deploy | < 2 minutes |
| AI analysis helpfulness | > 70% resolve failures |

---

## Roadmap

| Phase | Focus | Status |
|-------|-------|--------|
| **Phase 1-4** | Studio Backend + Frontend + Editor | 🔧 In Progress |
| **Phase 5** | AI Features (NL→SQL, Completions) | 📋 Planned |
| **Phase 6** | Test Harness Integration | 📋 Planned |
| **Phase 7-8** | Visualization + Observability | 📋 Planned |
| **Phase 9-10** | Notebook Lifecycle + Deployment | 📋 Planned |
| **Phase 11** | MCP Server | 📋 Planned |
| **Phase 12** | AI Black Box Recorder | 📋 Planned |
| **Phase 13** | Cluster Linker | 📋 Planned |
| **Phase 14** | Enterprise Features | 📋 Planned |

See [TODO.md](./TODO.md) for detailed task breakdown.
