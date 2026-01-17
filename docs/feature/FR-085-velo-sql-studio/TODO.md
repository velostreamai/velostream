# FR-085: Velostream - Implementation Tasks

> **Scope**: Complete product implementation including Studio, Turnkey Apps, and Enterprise features
> **Last Updated**: November 27, 2025

---

## Progress Summary

| Phase | Focus | Status | Completion |
|-------|-------|--------|------------|
| **Phase 1** | Studio Backend Foundation | Not Started | 0% |
| **Phase 2** | Studio Frontend Foundation | Not Started | 0% |
| **Phase 3** | Notebook Interface | Not Started | 0% |
| **Phase 4** | Monaco SQL Editor | Not Started | 0% |
| **Phase 5** | AI Features | Not Started | 0% |
| **Phase 6** | Test Harness Integration | Not Started | 0% |
| **Phase 7** | Results Visualization | Not Started | 0% |
| **Phase 8** | Observability Integration | Not Started | 0% |
| **Phase 9** | Notebook Lifecycle | Not Started | 0% |
| **Phase 10** | Deployment | Not Started | 0% |
| **Phase 11** | MCP Server | Not Started | 0% |
| **Phase 12** | AI Black Box Recorder | Not Started | 0% |
| **Phase 13** | Cluster Linker | Not Started | 0% |
| **Phase 14** | AI Semantic Lineage | Not Started | 0% |
| **Phase 15** | Enterprise Features | Not Started | 0% |
| **Phase 16** | Polish & Documentation | Not Started | 0% |

**Overall Progress: 0%**

---

## Phase 1: Studio Backend Foundation

### 1.1 Rust REST API Module
- [ ] Create `src/api/mod.rs` - API module structure
- [ ] Create `src/api/server.rs` - Axum HTTP server setup
- [ ] Create `src/api/routes/validate.rs` - SQL validation endpoint
- [ ] Create `src/api/routes/execute.rs` - Query execution endpoint
- [ ] Create `src/api/routes/schema.rs` - Schema introspection endpoint
- [ ] Create `src/api/routes/completions.rs` - AI completion proxy endpoint
- [ ] Add tower-http middleware (CORS, logging, compression)

### 1.2 Test Harness API Integration
- [ ] Create `src/api/routes/test.rs` - Test execution endpoint (FR-084 integration)
- [ ] Create `src/api/routes/generate_data.rs` - Synthetic data generation endpoint
- [ ] Wire up `SchemaDataGenerator` from FR-084
- [ ] Wire up `AssertionEngine` from FR-084
- [ ] Wire up `AiAssistant` from FR-084 for failure analysis

### 1.3 WebSocket Streaming
- [ ] Create `src/api/routes/stream.rs` - WebSocket handler
- [ ] Implement query result streaming
- [ ] Add client connection management (pause/resume/cancel)
- [ ] Implement backpressure handling

### 1.4 Binary Updates
- [ ] Create `src/bin/velo_studio.rs` - Studio server binary
- [ ] Add CLI options for port, CORS origins
- [ ] Integrate with existing Velostream runtime

---

## Phase 2: Studio Frontend Foundation

### 2.1 Next.js Project Setup
- [ ] Initialize Next.js 14 with App Router in `studio/` directory
- [ ] Configure TypeScript
- [ ] Install and configure Tailwind CSS
- [ ] Install shadcn/ui components
- [ ] Configure ESLint and Prettier
- [ ] Set up environment variables

### 2.2 App Layout
- [ ] Create app layout with header
- [ ] Implement dark/light mode toggle
- [ ] Create sidebar for notebooks list
- [ ] Add responsive design

### 2.3 API Client
- [ ] Create `lib/api/client.ts` - Base API client
- [ ] Create `lib/api/notebooks.ts` - Notebook CRUD
- [ ] Create `lib/api/execute.ts` - Query execution
- [ ] Create `lib/api/test.ts` - Test harness API
- [ ] Add WebSocket connection management
- [ ] Create React hooks (`useNotebook`, `useCell`, etc.)

---

## Phase 3: Notebook Interface

### 3.1 Notebook Components
- [ ] Create `components/notebook/NotebookView.tsx` - Main container
- [ ] Create `components/notebook/Cell.tsx` - Single cell component
- [ ] Create `components/notebook/CellHeader.tsx` - NL prompt display
- [ ] Create `components/notebook/CellControls.tsx` - Run/Test/Delete buttons
- [ ] Create `components/notebook/NotebookSummary.tsx` - Accumulated metrics/alerts

### 3.2 Notebook State Management
- [ ] Create `hooks/useNotebook.ts` - Notebook state hook
- [ ] Create `hooks/useCell.ts` - Cell state hook
- [ ] Implement cell CRUD operations
- [ ] Implement notebook context accumulation (sources, sinks, metrics)
- [ ] Add auto-save to local storage

### 3.3 Chat Input
- [ ] Create `components/chat/ChatInput.tsx` - NL input with send button
- [ ] Create `components/chat/ChatSuggestions.tsx` - Quick action suggestions
- [ ] Create `components/chat/StreamingResponse.tsx` - Typewriter effect for AI

---

## Phase 4: Monaco SQL Editor

### 4.1 Basic Editor Integration
- [ ] Install `@monaco-editor/react`
- [ ] Create `components/editor/SqlEditor.tsx` - Monaco wrapper
- [ ] Configure SQL language mode
- [ ] Add Velostream syntax highlighting (WINDOW, EMIT, ROWS BUFFER)
- [ ] Implement error markers from validation

### 4.2 Velostream Language Extension
- [ ] Register custom language tokens
- [ ] Add WINDOW, EMIT, ROWS BUFFER keywords
- [ ] Add @metric annotation highlighting
- [ ] Configure bracket matching

### 4.3 Schema-Aware Autocomplete
- [ ] Fetch schema from `/api/schema`
- [ ] Register CompletionItemProvider
- [ ] Add table name completions
- [ ] Add column name completions (context-aware)
- [ ] Add function completions with signatures

---

## Phase 5: AI Features

### 5.1 Backend LLM Integration
- [ ] Create `src/api/handlers/ai_handler.rs` - Claude API integration
- [ ] Create completion request handler
- [ ] Implement FIM (Fill-in-Middle) prompting
- [ ] Add schema context to prompts
- [ ] Configure rate limiting

### 5.2 Inline Completions Provider
- [ ] Create `lib/ai/completions.ts`
- [ ] Register Monaco InlineCompletionsProvider
- [ ] Implement debounced completion requests
- [ ] Add ghost text styling
- [ ] Handle Tab to accept

### 5.3 Natural Language → SQL
- [ ] Create `src/api/routes/nl_to_sql.rs` - NL→SQL endpoint
- [ ] Create `lib/ai/nl-to-sql.ts` - Frontend integration
- [ ] Add notebook context to prompts (previous cells)
- [ ] Implement validation loop (retry on error)
- [ ] Show explanation alongside generated SQL

---

## Phase 6: Test Harness Integration

### 6.1 Test Dialog
- [ ] Create `components/test/TestDialog.tsx` - Configure test run
- [ ] Create `components/test/SchemaEditor.tsx` - Edit data schema
- [ ] Create `components/test/AssertionBuilder.tsx` - Add assertions

### 6.2 Inline Testing
- [ ] Add [Test] button to cell controls
- [ ] Generate synthetic data on test click
- [ ] Execute SQL and capture output
- [ ] Run assertions and display results inline

### 6.3 Test Results Display
- [ ] Create `components/notebook/CellTestResults.tsx` - Inline results
- [ ] Create `components/test/TestReport.tsx` - Full test report
- [ ] Show pass/fail for each assertion
- [ ] Display performance metrics (execution time, throughput)

### 6.4 AI Failure Analysis
- [ ] Integrate FR-084's AiAssistant for failure analysis
- [ ] Display AI analysis when tests fail
- [ ] Show suggested fixes
- [ ] Add "Apply Fix" button

---

## Phase 7: Results Visualization

### 7.1 Data Table
- [ ] Install TanStack Table
- [ ] Create `components/viz/DataTable.tsx` - Data table component
- [ ] Implement virtual scrolling
- [ ] Add column sorting
- [ ] Add column filtering
- [ ] Handle large datasets efficiently

### 7.2 Inline Charts
- [ ] Install Recharts
- [ ] Create `components/viz/VizRenderer.tsx` - Auto-select chart type
- [ ] Create `components/viz/LineChart.tsx` - Time series
- [ ] Create `components/viz/BarChart.tsx` - Categorical data
- [ ] Create `components/viz/GaugeChart.tsx` - Single metrics

### 7.3 Visualization Inference
- [ ] Create `lib/viz/infer-chart.ts` - Chart type inference
- [ ] Detect time series queries → line chart
- [ ] Detect categorical groupings → bar chart
- [ ] Detect single metrics → gauge

### 7.4 Streaming Results
- [ ] Implement WebSocket result handler
- [ ] Add progressive rendering
- [ ] Show record count and throughput
- [ ] Add pause/resume controls

---

## Phase 8: Observability Integration

### 8.1 @metric Annotation Parsing
- [ ] Create annotation parser in Rust
- [ ] Extract metric name, type, labels
- [ ] Validate annotation syntax
- [ ] Return parsed annotations in API

### 8.2 Grafana Dashboard Generation
- [ ] Create `lib/grafana/dashboard.ts` - Dashboard generator
- [ ] Generate dashboard JSON from @metrics
- [ ] Add panel type mapping (counter→graph, gauge→gauge)
- [ ] Support label dimensions
- [ ] Export dashboard as JSON

### 8.3 Grafana Embed
- [ ] Create `components/viz/GrafanaEmbed.tsx` - Iframe embed
- [ ] Configure iframe embedding
- [ ] Handle authentication (anonymous or token)
- [ ] Add panel refresh controls

### 8.4 BYOD Documentation
- [ ] Document Prometheus scraping setup
- [ ] Provide Grafana dashboard templates
- [ ] Add Datadog integration guide
- [ ] Include example alert configurations

---

## Phase 9: Notebook Lifecycle

### 9.1 Notebook CRUD
- [ ] Create `src/api/routes/notebooks.rs` - Notebook persistence API
- [ ] Create notebooks list page
- [ ] Implement notebook creation
- [ ] Implement notebook deletion
- [ ] Add notebook renaming

### 9.2 Persistence
- [ ] Set up SQLite for notebook storage
- [ ] Implement notebook save
- [ ] Implement notebook load
- [ ] Add auto-save functionality

### 9.3 Notebook Summary
- [ ] Accumulate sources from all cells
- [ ] Accumulate sinks from all cells
- [ ] Accumulate @metrics from all cells
- [ ] Accumulate @alerts from all cells
- [ ] Display summary panel

---

## Phase 10: Deployment

### 10.1 Deploy Dialog
- [ ] Create `components/deploy/DeployDialog.tsx` - Deployment wizard
- [ ] Create `components/deploy/DeployPreview.tsx` - What will be deployed
- [ ] Show list of jobs to deploy
- [ ] Show metrics to register
- [ ] Show alerts to configure

### 10.2 Pipeline Deployment
- [ ] Create `src/api/routes/deploy.rs` - Deployment endpoint
- [ ] Deploy each cell as a Velostream job
- [ ] Register metrics with Prometheus
- [ ] Configure alerts

### 10.3 Dashboard Deployment
- [ ] Create `components/deploy/DashboardPreview.tsx` - Grafana preview
- [ ] Generate Grafana dashboard from accumulated @metrics
- [ ] Deploy dashboard to Grafana (if configured)
- [ ] Return dashboard URL

### 10.4 Pipeline Management
- [ ] Create pipelines list page
- [ ] Create `components/deploy/PipelineStatus.tsx` - Running jobs status
- [ ] Show job status (running/stopped/failed)
- [ ] Add start/stop/restart controls
- [ ] Display job metrics

---

## Phase 11: MCP Server (Compete with Lenses.io)

### 11.1 MCP Protocol Implementation
- [ ] Create `src/mcp/mod.rs` - MCP module structure
- [ ] Implement MCP JSON-RPC protocol
- [ ] Create tool registry

### 11.2 MCP Tools
- [ ] Implement `list_streams()` - List available streams
- [ ] Implement `describe_stream()` - Get schema, sample data
- [ ] Implement `query_stream()` - Execute streaming SQL
- [ ] Implement `validate_sql()` - Check SQL syntax
- [ ] Implement `generate_test_data()` - Create synthetic data
- [ ] Implement `run_test()` - Execute test with assertions
- [ ] Implement `get_metrics()` - Fetch current metric values
- [ ] Implement `deploy_query()` - Deploy SQL as production job

### 11.3 MCP Resources
- [ ] Implement `streams://{name}` - Stream schema and metadata
- [ ] Implement `metrics://{name}` - Metric definition and current value
- [ ] Implement `jobs://{id}` - Running job status

### 11.4 MCP Server Binary
- [ ] Create `src/bin/velo_mcp.rs` - MCP server binary
- [ ] Add stdio transport (for Claude Desktop, Cursor, etc.)
- [ ] Add HTTP transport option
- [ ] Document integration with Claude, VS Code, Cursor

---

## Phase 12: AI Black Box Recorder (Turnkey App)

### 12.1 Decision Ingest
- [ ] Create `velostream-apps/blackbox-recorder/` crate
- [ ] Define Decision schema (agent_id, context, input, output, latency)
- [ ] Create Kafka consumer for decision events
- [ ] Create HTTP endpoint for direct ingest
- [ ] Implement batching and buffering

### 12.2 Python SDK
- [ ] Create `velostream-blackbox` Python package
- [ ] Implement `@record_decision` decorator
- [ ] Implement `BlackBoxClient` class
- [ ] Add async support
- [ ] Add OpenTelemetry integration
- [ ] Publish to PyPI

### 12.3 Query & Replay
- [ ] Implement decision storage (Kafka + indexing)
- [ ] Create `GET /api/decisions/{id}` endpoint
- [ ] Create `POST /api/decisions/search` endpoint
- [ ] Create `POST /api/decisions/replay` endpoint
- [ ] Add decision comparison (diff two decisions)

### 12.4 Dashboard
- [ ] Create Black Box dashboard in Studio
- [ ] Show decisions/sec, latency distribution
- [ ] Add agent-level views
- [ ] Add anomaly detection alerts
- [ ] Create replay console

### 12.5 Pricing & Licensing
- [ ] Implement usage metering (decisions/month)
- [ ] Create license check for commercial features
- [ ] Add billing integration (Stripe)

---

## Phase 13: Cluster Linker (Turnkey App)

### 13.1 Core Replication
- [ ] Create `velostream-apps/cluster-linker/` crate
- [ ] Implement Kafka-to-Kafka replication
- [ ] Support multiple vendor combinations (Confluent, MSK, Redpanda)
- [ ] Add schema translation (Avro ↔ JSON ↔ Protobuf)
- [ ] Implement exactly-once delivery

### 13.2 SQL Transforms
- [ ] Allow SQL transforms during replication
- [ ] Support filtering (WHERE clause)
- [ ] Support projection (SELECT columns)
- [ ] Support simple aggregations

### 13.3 CLI
- [ ] Create `velo link create` command
- [ ] Create `velo link list` command
- [ ] Create `velo link status` command
- [ ] Create `velo link delete` command
- [ ] Add YAML config file support

### 13.4 Monitoring
- [ ] Track bytes replicated
- [ ] Track lag (source vs target offset)
- [ ] Add alerts for replication failures
- [ ] Create Grafana dashboard for links

### 13.5 Pricing
- [ ] Implement usage metering (GB/month)
- [ ] Compare pricing to Confluent ($0.05/GB vs $0.15/GB)
- [ ] Add billing integration

---

## Phase 14: AI Semantic Lineage (Turnkey App)

### 14.1 Lineage Capture
- [ ] Create `velostream-apps/semantic-lineage/` crate
- [ ] Extend Black Box Recorder with source tracing
- [ ] Capture data source for each input field
- [ ] Track freshness (how old is the data?)
- [ ] Detect failures (API errors, timeouts, fallbacks)

### 14.2 Lineage Storage
- [ ] Design lineage graph schema
- [ ] Implement storage (Kafka + graph DB or indexed storage)
- [ ] Create query API for lineage

### 14.3 AI Analysis
- [ ] Integrate Claude for lineage explanation
- [ ] Generate human-readable explanations
- [ ] Detect anomalies (stale data, failed sources)
- [ ] Suggest fixes

### 14.4 UI
- [ ] Create lineage visualization in Studio
- [ ] Show data flow diagram
- [ ] Highlight issues (stale, failed, missing)
- [ ] Add "Replay Decision" button

### 14.5 Compliance
- [ ] Export lineage for audit
- [ ] Support regulatory formats (EU AI Act)
- [ ] Add retention policies

---

## Phase 15: Enterprise Features

### 15.1 Authentication
- [ ] Create `velostream-enterprise/auth/` module
- [ ] Implement SSO/SAML integration
- [ ] Implement OIDC integration
- [ ] Add API key management

### 15.2 Authorization (RBAC)
- [ ] Define role model (admin, developer, analyst)
- [ ] Define permission model (streams, jobs, notebooks, deploy)
- [ ] Implement permission checks in API
- [ ] Create role management UI

### 15.3 Audit Logging
- [ ] Create `velostream-enterprise/audit/` module
- [ ] Log all API calls with user, action, resource
- [ ] Log all data access
- [ ] Create audit log query API
- [ ] Export to SIEM (Splunk, etc.)

### 15.4 Multi-node Clustering
- [ ] Create `velostream-enterprise/clustering/` module
- [ ] Implement leader election (Raft or similar)
- [ ] Implement job distribution across nodes
- [ ] Add node health monitoring
- [ ] Implement automatic failover

### 15.5 Chaos Testing
- [ ] Create `velostream-enterprise/chaos/` module
- [ ] Implement Kafka failure injection
- [ ] Implement network partition simulation
- [ ] Implement latency injection
- [ ] Create chaos test runner

### 15.6 Regression Testing
- [ ] Implement output comparison across versions
- [ ] Create regression test runner
- [ ] Generate diff reports
- [ ] Integrate with CI/CD

### 15.7 Pipeline Lineage
- [ ] Track dependencies between jobs
- [ ] Implement impact analysis ("what breaks if I change this?")
- [ ] Create lineage visualization
- [ ] Add change management workflow

### 15.8 License Management
- [ ] Define feature flags for enterprise
- [ ] Implement license file format
- [ ] Add license check at startup
- [ ] Create license management API

---

## Phase 16: Polish & Documentation

### 16.1 Error Handling
- [ ] Implement global error boundary
- [ ] Add toast notifications
- [ ] Improve SQL error messages
- [ ] Add retry mechanisms

### 16.2 Performance Optimization
- [ ] Implement completion caching
- [ ] Add request deduplication
- [ ] Optimize WebSocket reconnection
- [ ] Profile and fix bottlenecks

### 16.3 Documentation
- [ ] Write user guide (Studio)
- [ ] Write API documentation
- [ ] Write MCP integration guide
- [ ] Write Black Box Recorder guide
- [ ] Write Cluster Linker guide
- [ ] Write Enterprise admin guide
- [ ] Add inline help tooltips
- [ ] Record demo videos

### 16.4 Testing
- [ ] Add unit tests for API client
- [ ] Add component tests
- [ ] Add E2E tests with Playwright
- [ ] Performance benchmarks
- [ ] Security testing

---

## Milestones

| Milestone | Phases | Description | Target |
|-----------|--------|-------------|--------|
| **M1: Studio MVP** | 1-4 | Backend API + Notebook UI + Editor | 8 weeks |
| **M2: AI Features** | 5 | NL→SQL + Copilot completions | +4 weeks |
| **M3: Testing** | 6 | Synthetic data + assertions + AI analysis | +3 weeks |
| **M4: Visualization** | 7-8 | Charts + Grafana integration | +4 weeks |
| **M5: Production** | 9-10 | Notebook lifecycle + deployment | +4 weeks |
| **M6: MCP Parity** | 11 | Match Lenses.io MCP features | +3 weeks |
| **M7: Black Box** | 12 | AI decision recording + replay | +6 weeks |
| **M8: Linker** | 13 | Kafka cluster replication | +4 weeks |
| **M9: Lineage** | 14 | AI semantic lineage | +6 weeks |
| **M10: Enterprise** | 15 | Auth, RBAC, audit, clustering | +8 weeks |
| **M11: GA** | 16 | Polish, docs, testing | +4 weeks |

**Total Estimated: 54 weeks (13.5 months)**

---

## Dependencies

### NPM Packages (Studio Frontend)

```json
{
  "dependencies": {
    "next": "^14.0",
    "react": "^18.0",
    "@monaco-editor/react": "^4.6",
    "recharts": "^2.10",
    "@tanstack/react-table": "^8.10",
    "tailwindcss": "^3.4",
    "@radix-ui/react-*": "latest",
    "lucide-react": "^0.300",
    "zustand": "^4.4",
    "swr": "^2.2"
  }
}
```

### Rust Crates

```toml
# Studio Backend
axum = { version = "0.7", features = ["ws"] }
tower-http = { version = "0.5", features = ["cors", "compression"] }
tokio-tungstenite = "0.21"
sqlx = { version = "0.7", features = ["sqlite", "runtime-tokio"] }

# AI Integration
reqwest = { version = "0.11", features = ["json"] }

# MCP Server
jsonrpc-core = "18.0"

# Enterprise (separate crate)
# openssl, saml2, raft, etc.
```

### Python Packages (Black Box SDK)

```toml
[project]
dependencies = [
    "httpx>=0.25",
    "pydantic>=2.0",
    "opentelemetry-api>=1.20",
]
```

---

## FR-084 Components Reused

| FR-084 Component | Usage | Phases |
|------------------|-------|--------|
| `SchemaDataGenerator` | Synthetic data for cell testing, MCP | 6, 11 |
| `QueryExecutor` | Execute SQL cells with captured output | 6 |
| `AssertionEngine` | Validate cell outputs against assertions | 6 |
| `AiAssistant` | Schema inference, failure analysis | 5, 6 |
| `InMemorySchemaRegistry` | Avro/Protobuf schema support | 1 |
| `TestSpec` | Define notebook-wide test configurations | 6 |
| `CapturedOutput` | Capture results for visualization | 7 |
