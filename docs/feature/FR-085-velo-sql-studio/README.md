# FR-085: Velostream SQL Studio

## Overview

**Velostream SQL Studio** is an AI-powered **notebook interface** for building, testing, and deploying streaming SQL applications. Think Jupyter meets streaming SQL with Claude as your copilot.

## Vision

Transform streaming SQL development from "write SQL → deploy → pray" into an **iterative, visual, AI-guided experience**:

```
Natural Language → SQL Cell → Live Visualization → Accumulated App → Deploy Pipeline
```

## Value Propositions

### 1. AI-Powered Notebook Experience
- **Natural Language → SQL Cells**: Describe what you want, get validated SQL
- **Conversation-Driven Development**: Each cell builds on previous context
- **Smart Visualization**: AI auto-selects chart types based on query structure
- **Copilot Completions**: Ghost text suggestions in Monaco (Tab to accept)

### 2. SQL-Native Observability
- **@metric annotations**: Define Prometheus metrics directly in SQL
- **Auto-generated Dashboards**: Grafana dashboards from accumulated @metrics
- **BYOD Support**: Customers use their own observability stack
- **Embedded Option**: Grafana panels embedded in Studio UI

### 3. Integrated Test Harness (FR-084)
- **Synthetic Data Generation**: Schema-driven test data with realistic distributions
- **SQL Validation**: Real-time syntax and semantic validation
- **Assertion Testing**: Validate outputs before deployment
- **AI Failure Analysis**: Claude explains why tests fail and suggests fixes

### 4. Exploration → Production Pipeline
- **Notebook Development**: Interactive cells with live preview
- **Accumulated Context**: Cells chain together into complete pipeline
- **One-Click Deploy**: Notebook → Deployed streaming jobs + Grafana dashboard
- **Managed Visualization**: Dashboards auto-configured from deployed jobs

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      VELOSTREAM SQL STUDIO                              │
├─────────────────────────────────────────────────────────────────────────┤
│  Notebook Interface (Next.js + React)                                   │
│  ├── NotebookView - Scrollable cell list with accumulated context       │
│  ├── Cell - NL prompt + Monaco SQL editor + visualization               │
│  │   ├── NlPrompt - The user's natural language request                 │
│  │   ├── SqlEditor - Monaco with AI completions                         │
│  │   ├── VizRenderer - Recharts (auto-selected chart type)              │
│  │   └── CellControls - [Edit] [Run] [Test] [Delete]                    │
│  ├── ChatInput - Streaming AI responses for new cells                   │
│  ├── NotebookSummary - Aggregated @metrics, @alerts, sources, sinks     │
│  └── DeployDialog - Notebook → Production wizard                        │
├─────────────────────────────────────────────────────────────────────────┤
│  AI Layer                                                               │
│  ├── NL → SQL Generation (with full notebook context)                   │
│  ├── Visualization Recommendation (chart type inference)                │
│  ├── Data Pattern Discovery ("what patterns do you see?")               │
│  ├── Query Optimization Suggestions                                     │
│  ├── Annotation Auto-generation (@metric suggestions)                   │
│  └── Test Failure Analysis (via velo-test AI integration)               │
├─────────────────────────────────────────────────────────────────────────┤
│  Backend API (Rust - extend Velostream)                                 │
│  ├── POST /api/validate - SQL validation                                │
│  ├── POST /api/execute - Query execution with streaming results         │
│  ├── POST /api/generate-data - Synthetic data via test harness          │
│  ├── POST /api/test - Run assertions via velo-test                      │
│  ├── GET  /api/schema - Tables/columns for autocomplete                 │
│  ├── POST /api/completions - AI completion suggestions                  │
│  ├── POST /api/nl-to-sql - Natural language to SQL                      │
│  ├── CRUD /api/notebooks - Notebook persistence                         │
│  ├── CRUD /api/jobs - Job management                                    │
│  ├── POST /api/deploy - Notebook → Pipeline deployment                  │
│  └── GET  /metrics - Prometheus (existing)                              │
├─────────────────────────────────────────────────────────────────────────┤
│  Test Harness Integration (FR-084)                                      │
│  ├── SchemaDataGenerator - Realistic test data                          │
│  ├── QueryExecutor - Execute SQL with captured outputs                  │
│  ├── AssertionEngine - Validate results against expectations            │
│  ├── AiAssistant - Schema inference, failure analysis                   │
│  └── InMemorySchemaRegistry - Avro/Protobuf schema support              │
├─────────────────────────────────────────────────────────────────────────┤
│  External Integrations                                                  │
│  ├── Kafka (data source/sink)                                           │
│  ├── Prometheus (metrics scraping)                                      │
│  ├── Grafana (BYOD or embedded dashboards)                              │
│  └── Claude API (NL→SQL, completions, analysis)                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## User Journey

```
┌─────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: EXPLORATION (Notebook)                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  User: "Show me trading volume by symbol for the last hour"             │
│                           ↓                                             │
│  ┌─ Cell 1 ─────────────────────────────────────────────────────────┐  │
│  │ 💬 "Show me trading volume by symbol for the last hour"          │  │
│  ├──────────────────────────────────────────────────────────────────┤  │
│  │ ```sql                                                           │  │
│  │ SELECT symbol, SUM(quantity) as volume                           │  │
│  │ FROM trades                                                      │  │
│  │ GROUP BY symbol                                                  │  │
│  │ WINDOW TUMBLING(INTERVAL '5' MINUTE)                             │  │
│  │ EMIT CHANGES                                                     │  │
│  │ ```                                      [Edit] [Run] [Test] [▼] │  │
│  ├──────────────────────────────────────────────────────────────────┤  │
│  │ 📊 [Bar Chart: Volume by Symbol]                                 │  │
│  │     AAPL ████████████ 125,000                                    │  │
│  │     TSLA ████████ 89,000                                         │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│ STAGE 2: ITERATE & EVOLVE                                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  User: "Now show price volatility, flag when > 5%"                      │
│                           ↓                                             │
│  ┌─ Cell 2 ─────────────────────────────────────────────────────────┐  │
│  │ 💬 "Now show price volatility, flag when > 5%"                   │  │
│  ├──────────────────────────────────────────────────────────────────┤  │
│  │ ```sql                                                           │  │
│  │ -- @metric: price_volatility                                     │  │
│  │ -- @metric_type: gauge                                           │  │
│  │ -- @alert: volatility > 0.05                                     │  │
│  │ SELECT symbol,                                                   │  │
│  │        STDDEV(price) / AVG(price) as volatility                  │  │
│  │ FROM trades                                                      │  │
│  │ GROUP BY symbol                                                  │  │
│  │ WINDOW TUMBLING(INTERVAL '1' MINUTE)                             │  │
│  │ EMIT CHANGES                                                     │  │
│  │ ```                                      [Edit] [Run] [Test] [▼] │  │
│  ├──────────────────────────────────────────────────────────────────┤  │
│  │ 📈 [Line Chart: Volatility Over Time]                            │  │
│  │     🔴 TSLA: 7.2% (ALERT!)                                       │  │
│  │     🟢 AAPL: 2.1%                                                │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│ STAGE 3: TEST WITH SYNTHETIC DATA                                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  User: "Test this with 10,000 synthetic trades"                         │
│                           ↓                                             │
│  [Test Harness generates schema-driven test data]                       │
│  [Runs SQL against generated data]                                      │
│  [Validates assertions]                                                 │
│                                                                         │
│  ┌─ Test Results ───────────────────────────────────────────────────┐  │
│  │ ✅ Cell 1: volume_by_symbol                                      │  │
│  │    • 10,000 records processed → 7 unique symbols                 │  │
│  │    • Execution time: 45ms                                        │  │
│  │                                                                  │  │
│  │ ✅ Cell 2: price_volatility                                      │  │
│  │    • Volatility range: 0.8% - 12.3%                              │  │
│  │    • Alerts triggered: 3 symbols                                 │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│ STAGE 4: DEPLOY AS PIPELINE                                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  User clicks: [Deploy Notebook]                                         │
│                           ↓                                             │
│  ┌─ Deploy Summary ─────────────────────────────────────────────────┐  │
│  │ 📋 Notebook: Trading Analytics                                   │  │
│  │                                                                  │  │
│  │ Will deploy:                                                     │  │
│  │ ☑️ 2 streaming SQL jobs                                          │  │
│  │ ☑️ 1 @metric → Prometheus endpoint                               │  │
│  │ ☑️ 1 @alert → AlertManager rule                                  │  │
│  │ ☑️ Auto-generated Grafana dashboard                              │  │
│  │                                                                  │  │
│  │ [Preview Dashboard]                    [Cancel] [Deploy →]       │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│ STAGE 5: PRODUCTION MONITORING                                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─ Pipeline: trading-analytics ────────────────────────────────────┐  │
│  │ Status: ✅ Running                                               │  │
│  │                                                                  │  │
│  │ Jobs:                                                            │  │
│  │   • volume_by_symbol      ✅ 12.5K/sec | Latency: 2.3ms         │  │
│  │   • price_volatility      ✅ 12.5K/sec | Latency: 3.1ms         │  │
│  │                                                                  │  │
│  │ Alerts: 🔴 2 active (TSLA, NVDA volatility > 5%)                 │  │
│  │                                                                  │  │
│  │ [Open in Grafana ↗] [View Embedded] [Edit Notebook]              │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Test Harness Integration (FR-084)

The notebook integrates deeply with the velo-test harness for data generation and validation.

### Synthetic Data Generation

```
┌─ Generate Test Data ────────────────────────────────────────────────────┐
│                                                                         │
│ Source: trades                                                          │
│                                                                         │
│ Schema: [Auto-inferred from SQL] [Upload YAML] [AI Generate]            │
│                                                                         │
│ Fields:                                                                 │
│ ┌─────────────────────────────────────────────────────────────────────┐│
│ │ symbol    STRING   enum: [AAPL, GOOGL, MSFT, TSLA, NVDA]           ││
│ │ price     DECIMAL  min: 50.0, max: 5000.0, distribution: log_normal ││
│ │ quantity  INTEGER  min: 100, max: 100000                            ││
│ │ timestamp TIMESTAMP range: relative, start: -1h, end: now           ││
│ └─────────────────────────────────────────────────────────────────────┘│
│                                                                         │
│ Records: [10000    ]  Seed: [42      ] (for reproducibility)           │
│                                                                         │
│                              [Cancel] [Generate & Run →]                │
└─────────────────────────────────────────────────────────────────────────┘
```

### Inline Testing

Each cell has a `[Test]` button that:
1. Generates synthetic data based on schema
2. Executes the SQL
3. Runs assertions
4. Shows results inline

```
┌─ Cell Test Results ─────────────────────────────────────────────────────┐
│ ✅ Passed (3/3 assertions)                                              │
│                                                                         │
│ ✓ record_count: 7 (expected: > 0)                                       │
│ ✓ schema_contains: [symbol, volume]                                     │
│ ✓ no_nulls: [symbol, volume]                                            │
│                                                                         │
│ Performance:                                                            │
│   • Execution time: 45ms                                                │
│   • Memory peak: 12 MB                                                  │
│   • Throughput: 222,222 records/sec                                     │
│                                                                         │
│ [View Full Report] [Add More Assertions]                                │
└─────────────────────────────────────────────────────────────────────────┘
```

### AI Failure Analysis

When tests fail, Claude analyzes the failure:

```
┌─ Cell Test Results ─────────────────────────────────────────────────────┐
│ ❌ Failed (1/3 assertions)                                              │
│                                                                         │
│ ✓ schema_contains: [symbol, volume]                                     │
│ ✓ no_nulls: [symbol, volume]                                            │
│ ✗ join_coverage: 0% match (expected: > 80%)                             │
│                                                                         │
│ 🤖 AI Analysis:                                                         │
│ The JOIN on 'customer_id' produced no matches because:                  │
│ • trades contains customer_ids: [CUST001, CUST002, CUST003]             │
│ • customers table contains: [C-100, C-200, C-300]                       │
│                                                                         │
│ Suggested fix:                                                          │
│ Add a foreign key relationship in your schema:                          │
│ ```yaml                                                                 │
│ relationships:                                                          │
│   - field: customer_id                                                  │
│     references: customers.id                                            │
│     strategy: sample                                                    │
│ ```                                                                     │
│                                                                         │
│ [Apply Fix] [Regenerate Data] [Ignore]                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Component | Technology | Rationale |
|-----------|------------|-----------|
| Frontend | Next.js 14 (App Router) | Production-ready, SSR, API routes |
| Notebook UI | Custom React components | Flexible cell-based layout |
| SQL Editor | Monaco Editor | VS Code quality, AI completions support |
| Inline Charts | Recharts | Lightweight, React-native, streaming |
| Data Tables | TanStack Table | Virtual scrolling, large datasets |
| Styling | Tailwind + shadcn/ui | Fast, consistent, dark mode |
| Real-Time | WebSocket / SSE | Streaming query results |
| Backend | Rust (extend Velostream) | Leverage existing parser/runtime |
| Test Harness | velo-test (FR-084) | Existing data gen, assertions, AI |
| LLM | Claude API (Anthropic) | Best for code, supports FIM |

## Competitive Differentiation

| Feature | Databricks | Jupyter | Flink SQL | Lenses.io | **Velostream Studio** |
|---------|------------|---------|-----------|-----------|----------------------|
| Streaming SQL | ❌ Batch | ❌ Batch | ✅ | ✅ | ✅ |
| AI NL→SQL | ✅ | ❌ | ❌ | ❌ | ✅ |
| AI Completions | ✅ | ❌ | ❌ | ❌ | ✅ |
| Notebook Interface | ✅ | ✅ | ❌ | ❌ | ✅ |
| @metrics in SQL | ❌ | ❌ | ❌ | ❌ | ✅ |
| Auto Dashboards | ❌ | ❌ | ❌ | ❌ | ✅ |
| Synthetic Test Data | ❌ | ❌ | ❌ | ❌ | ✅ |
| AI Test Analysis | ❌ | ❌ | ❌ | ❌ | ✅ |
| Deploy as Pipeline | ✅ | ❌ | ✅ | ✅ | ✅ |
| Financial Precision | ❌ | ❌ | ❌ | ❌ | ✅ |

**Unique Combination**: First AI-native streaming SQL notebook with integrated test harness.

## Related Documents

- [DESIGN.md](./DESIGN.md) - Detailed technical design
- [TODO.md](./TODO.md) - Implementation tasks and progress
- [API.md](./API.md) - REST API specification
- [FR-084 Test Harness](../FR-084-app-test-harness/README.md) - Test harness documentation

## Success Metrics

- **NL→SQL Success Rate**: >90% valid SQL on first try
- **AI Completion Acceptance**: >80% of suggestions accepted
- **Time to First Visualization**: <30 seconds for new users
- **Test Feedback Loop**: <5 seconds from [Test] click to results
- **Notebook→Deploy Time**: <2 minutes for simple pipelines
- **AI Analysis Helpfulness**: >70% of suggestions resolve failures
