# FR-085: Velostream SQL Studio - Implementation Tasks

> **Scope**: Studio web interface (chat-first with artifacts)
> **Architecture**: assistant-ui + shadcn.io/ai + Monaco + Recharts + React Flow
> **Last Updated**: February 2026

---

## Progress Summary

| Phase | Focus | Status | Completion |
|-------|-------|--------|------------|
| **Phase 1** | Studio Backend (Axum REST API + WebSocket) | Not Started | 0% |
| **Phase 1.6** | Data Source Exploration API (Kafka, File, FileMmap, S3, DB) | Not Started | 0% |
| **Phase 1.7** | App Generation & Templates API | Not Started | 0% |
| **Phase 2** | Chat-First Frontend (assistant-ui + artifacts) | Not Started | 0% |
| **Phase 2.8** | Exploration Artifacts (topic list, data preview) | Not Started | 0% |
| **Phase 2.9** | App & Template Artifacts | Not Started | 0% |
| **Phase 3** | Test Harness Integration (FR-084 via tool results) | Not Started | 0% |
| **Phase 4** | Observability + Topology (React Flow, Grafana) | Not Started | 0% |
| **Phase 5** | Notebook Lifecycle + Deployment | Not Started | 0% |
| **Phase 6** | AI Proactive Intelligence | Not Started | 0% |

**Overall Progress: 0%**

> **Note**: MCP Server, Black Box Recorder, Cluster Linker, Semantic Lineage, and
> Enterprise Features are separate product initiatives tracked outside FR-085.
> See [ARCHITECTURE.md](./ARCHITECTURE.md) for the full product portfolio.

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

### 1.4 AI Chat Endpoint (for assistant-ui)
- [ ] Create `src/api/routes/chat.rs` - Streaming chat endpoint
- [ ] Integrate with Claude API for NL→SQL generation
- [ ] Define tool schemas (generate_sql, execute_query, test_query, deploy)
- [ ] Implement tool execution handlers
- [ ] Add thread context injection (schemas, previous SQL, metrics)

### 1.5 Binary Updates
- [ ] Create `src/bin/velo_studio.rs` - Studio server binary
- [ ] Add CLI options for port, CORS origins, Claude API key
- [ ] Integrate with existing Velostream runtime

### 1.6 Data Source Exploration API
- [ ] Create `src/api/routes/connect.rs` - URI-based data source connection (`POST /api/connect`) — supports Kafka, File, FileMmap, S3, ClickHouse, Database
- [ ] Create `src/api/routes/sources.rs` - Generic source listing (`GET /api/sources`) and detail (`GET /api/sources/{id}`)
- [ ] Create `src/api/routes/source_schema.rs` - Schema inference from any source (`GET /api/sources/{id}/schema`)
- [ ] Create `src/api/routes/source_preview.rs` - Data preview from any source (`GET /api/sources/{id}/preview`)
- [ ] Create `src/api/routes/topics.rs` - Kafka-specific topic listing (`GET /api/topics`) and detail (`GET /api/topics/{name}`)
- [ ] Create `src/api/routes/topic_schema.rs` - Kafka-specific schema inference (`GET /api/topics/{name}/schema`)
- [ ] Create `src/api/routes/messages.rs` - Kafka-specific message peek (`GET /api/topics/{name}/messages`)
- [ ] Create `src/api/routes/consumers.rs` - Consumer group listing (`GET /api/consumers`)
- [ ] Wire `TestHarnessInfra` exploration methods into Kafka API routes
- [ ] Wire `FileDataSource` and `FileMmapDataSource` into file exploration routes
- [ ] Wire `DataSourceRegistry` for URI-based source resolution
- [ ] Add session-scoped multi-source connection state to `AppState` (supports concurrent connections)

### 1.7 App Generation & Templates API
- [ ] Create `src/api/routes/generate_app.rs` - Multi-query app generation endpoint (`POST /api/generate-app`)
- [ ] Create `src/api/routes/apps.rs` - App CRUD (`GET /api/apps/{id}`, `PUT /api/apps/{id}`)
- [ ] Create `src/api/routes/app_test.rs` - Coordinated app testing (`POST /api/apps/{id}/test`)
- [ ] Create `src/api/routes/app_deploy.rs` - Full app deployment (`POST /api/apps/{id}/deploy`)
- [ ] Create `src/api/routes/templates.rs` - Template listing and detail (`GET /api/templates`, `GET /api/templates/{id}`)
- [ ] Create `src/api/routes/template_customize.rs` - Template customization (`POST /api/templates/{id}/customize`)
- [ ] Create `src/api/routes/template_preview.rs` - Template preview (`POST /api/templates/{id}/preview`)
- [ ] Create `src/api/models/app.rs` - App model (queries, metrics, alerts, dashboard, topology)
- [ ] Create `src/api/models/template.rs` - Template model (schema requirements, configurable params, YAML definition)
- [ ] Implement multi-query coordinated test data generation (foreign key alignment across topics)
- [ ] Implement multi-job coordinated deployment (deploy all queries + metrics + alerts + dashboard)
- [ ] Create built-in template library (trading-analytics, iot-sensor-monitoring, ecommerce-funnel, fraud-detection, log-analytics, api-monitoring, clickstream, change-data-capture)
- [ ] Implement auto-discover schema mapping (inspect connected topics and suggest field mappings)
- [ ] Add app state persistence to session/SQLite

---

## Phase 2: Chat-First Frontend

### 2.1 Project Setup
- [ ] Initialize Next.js 14 with App Router in `studio/` directory
- [ ] Configure TypeScript, Tailwind CSS, ESLint
- [ ] Install assistant-ui, @assistant-ui/react-ai-sdk, ai SDK
- [ ] Install shadcn/ui + shadcn.io/ai chat components
- [ ] Install Monaco Editor, Recharts, TanStack Table, React Flow
- [ ] Set up environment variables (API URL, etc.)

### 2.2 assistant-ui Runtime
- [ ] Create `ThreadProvider.tsx` - assistant-ui runtime configuration
- [ ] Configure Vercel AI SDK adapter for Axum backend
- [ ] Register Velostream tool definitions
- [ ] Configure custom artifact renderers for tool results

### 2.3 Thread View (Chat + Artifacts)
- [ ] Create `ThreadView.tsx` - Split-pane layout (chat left, artifact right)
- [ ] Create `AssistantMessage.tsx` - Custom AI message with tool result rendering
- [ ] Create `Suggestions.tsx` - Quick action chips ("Show me...", "Test this...")
- [ ] Implement thread list page (saved notebooks)
- [ ] Implement dark/light mode toggle

### 2.4 Artifact Renderers
- [ ] Create `SqlEditorArtifact.tsx` - Monaco editor with Edit/Run/Test buttons
- [ ] Create `QueryResultsArtifact.tsx` - Auto-selected chart or table
- [ ] Create `SchemaViewerArtifact.tsx` - Schema display with field types
- [ ] Implement artifact selection (clicking artifact focuses it in right panel)

### 2.5 Monaco SQL Editor Integration
- [ ] Create `SqlEditor.tsx` - Monaco wrapper component
- [ ] Register Velostream SQL language tokens (WINDOW, EMIT, ROWS BUFFER)
- [ ] Add @metric annotation highlighting
- [ ] Implement schema-aware autocomplete (CompletionItemProvider)
- [ ] Register AI inline completions provider (via /api/completions)

### 2.6 Visualization
- [ ] Create `VizRenderer.tsx` - Auto-select chart type from query metadata
- [ ] Create `LineChart.tsx` - Time series (Recharts)
- [ ] Create `BarChart.tsx` - Categorical data (Recharts)
- [ ] Create `GaugeChart.tsx` - Single metrics (Recharts)
- [ ] Create `DataTable.tsx` - TanStack Table with virtual scrolling
- [ ] Create `infer-chart.ts` - Chart type inference from columns/aggregations

### 2.7 WebSocket Streaming Results
- [ ] Create `useStreamingResults.ts` hook - WebSocket connection management
- [ ] Implement progressive rendering in QueryResultsArtifact
- [ ] Add pause/resume controls
- [ ] Show live record count and throughput

### 2.8 Exploration Artifacts
- [ ] Create `TopicListArtifact.tsx` - Topic grid with partition counts and message counts
- [ ] Create `DataPreviewArtifact.tsx` - Formatted message display with offset/partition metadata
- [ ] Create exploration tool definitions: `connect-kafka.ts`, `list-topics.ts`, `inspect-topic.ts`, `peek-messages.ts`

### 2.9 App & Template Artifacts
- [ ] Create `AppPreviewArtifact.tsx` - Multi-query app preview (queries, metrics, alerts, dashboard layout, topology)
- [ ] Create `TemplateBrowserArtifact.tsx` - Template library grid with categories, descriptions, tags, popularity
- [ ] Create `TemplateDetailArtifact.tsx` - Full template preview with schema requirements and sample SQL
- [ ] Create `AppTestResultsArtifact.tsx` - Consolidated test results for all queries in an app
- [ ] Create app generation tool definitions: `generate-app.ts`, `test-app.ts`, `deploy-app.ts`
- [ ] Create template tool definitions: `list-templates.ts`, `get-template.ts`, `customize-template.ts`
- [ ] Implement "Edit Queries" interaction in AppPreviewArtifact (inline Monaco editor per query)
- [ ] Implement "Test All" button in AppPreviewArtifact (calls `POST /api/apps/{id}/test`)
- [ ] Implement "Deploy App" button in AppPreviewArtifact (calls `POST /api/apps/{id}/deploy`)
- [ ] Implement schema mapping UI in TemplateBrowserArtifact (field-to-field drag mapping)
- [ ] Implement template preview with real data sampling (calls `POST /api/templates/{id}/preview`)

---

## Phase 3: Test Harness Integration

### 3.1 Test Tool
- [ ] Create `test-query.ts` tool definition (calls POST /api/test)
- [ ] Create `TestResultsArtifact.tsx` - Pass/fail display with assertion details
- [ ] Show AI failure analysis when tests fail
- [ ] Add "Apply Fix" action (sends fix to chat thread for AI to apply)

### 3.2 Test Configuration Dialog
- [ ] Create `TestDialog.tsx` - Configure schema, record count, seed
- [ ] Create assertion builder (record_count, schema_contains, field_values, etc.)
- [ ] Support FR-084 assertion types (see CLAUDE.md for full list)

### 3.3 Data Generation Tool
- [ ] Create `generate-data.ts` tool definition (calls POST /api/generate-data)
- [ ] Display generated data sample in chat
- [ ] Support foreign key relationships

---

## Phase 4: Observability + Topology

### 4.1 @metric Annotation Display
- [ ] Parse @metric, @alert annotations from SQL in artifacts
- [ ] Show annotation badges on SQL editor artifacts
- [ ] Accumulate metrics/alerts across thread into ThreadContext

### 4.2 Grafana Dashboard Generation
- [ ] Create dashboard generation tool (calls POST /api/dashboards/generate)
- [ ] Create `GrafanaEmbed.tsx` - Iframe embed for deployed dashboards
- [ ] Show dashboard preview before deploy

### 4.3 Pipeline Topology (React Flow)
- [ ] Create `TopologyArtifact.tsx` - React Flow DAG of streaming pipeline
- [ ] Auto-generate topology from thread SQL (sources → transforms → sinks)
- [ ] Show live metrics on topology nodes (records/sec, latency)

---

## Phase 5: Notebook Lifecycle + Deployment

### 5.1 Thread Persistence
- [ ] Create `src/api/routes/notebooks.rs` - Thread/notebook CRUD API
- [ ] Set up SQLite for thread storage (notebook metadata + thread ID)
- [ ] Implement save/load/rename/delete
- [ ] Add auto-save functionality

### 5.2 Deployment
- [ ] Create `deploy-pipeline.ts` tool definition (calls POST /api/deploy)
- [ ] Create `DeploySummaryArtifact.tsx` - Jobs, metrics, alerts, dashboard link
- [ ] Create `DeployDialog.tsx` - Deployment wizard with preview
- [ ] Deploy each SQL artifact as a Velostream job via StreamJobServer

### 5.3 Pipeline Management
- [ ] Create pipelines list page
- [ ] Create `PipelineStatus.tsx` - Running jobs with start/stop/restart
- [ ] Show job metrics (records processed, throughput, uptime)

### 5.4 Polish
- [ ] Global error boundary and toast notifications
- [ ] Completion caching and request deduplication
- [ ] WebSocket reconnection handling
- [ ] Keyboard shortcuts (Cmd+Enter to run, etc.)
- [ ] Responsive design for smaller screens

---

## Phase 6: AI Proactive Intelligence

### 6.1 Analysis Backend
- [ ] Create `src/api/routes/analyze.rs` - Proactive AI analysis endpoint (`POST /api/analyze`)
- [ ] Create `src/api/routes/analyze_schema.rs` - Schema diff detection (`POST /api/analyze/schema-diff`)
- [ ] Create `src/api/routes/analyze_performance.rs` - Performance optimization suggestions (`POST /api/analyze/performance`)
- [ ] Create `src/api/routes/analyze_annotations.rs` - Auto-annotation suggestions (`POST /api/analyze/annotations`)
- [ ] Create `src/api/models/suggestion.rs` - Suggestion model (type, priority, title, description, action)
- [ ] Implement context analysis engine (analyze thread context and generate prioritized suggestions)
- [ ] Implement schema diff engine (compare current topic schemas against deployed query expectations)
- [ ] Implement performance analysis engine (analyze running job metrics from Prometheus)
- [ ] Implement annotation recommender (infer appropriate @metric/@alert from query structure)

### 6.2 Proactive Triggers (Frontend)
- [ ] Create `useProactiveSuggestions.ts` hook - Triggers analysis after key events
- [ ] Trigger analysis after `connect_source` (suggest data to explore, templates to use)
- [ ] Trigger analysis after `generate_sql` (suggest metrics, windows, ScaledInteger)
- [ ] Trigger analysis after `test_query` (suggest fixes, additional assertions)
- [ ] Trigger analysis after `deploy_pipeline` (suggest monitoring, alerting, schema watching)
- [ ] Create `SuggestionBanner.tsx` - Non-intrusive suggestion display above chat input
- [ ] Create `SuggestionCard.tsx` - Expandable suggestion with "Apply" action button
- [ ] Implement suggestion dismissal and "don't show again" preferences

### 6.3 Continuous Monitoring Intelligence
- [ ] Implement periodic schema evolution detection (poll topic schemas every N minutes)
- [ ] Implement pipeline health monitoring (poll job metrics, detect anomalies)
- [ ] Create `HealthDashboard.tsx` - Overview of all deployed pipelines with AI health assessment
- [ ] Implement NL-to-dashboard shortcut ("Show me a dashboard for X" → generates dashboard spec)
- [ ] Implement proactive alert suggestions based on deployed metric history

---

## Milestones

| Milestone | Phases | Description | Estimate |
|-----------|--------|-------------|----------|
| **M1: Backend API** | 1, 1.6 | REST API + WebSocket + Chat + Exploration endpoints | 5 weeks |
| **M2: App & Template Backend** | 1.7 | App generation, templates, coordinated deployment | 3 weeks |
| **M3: Chat MVP** | 2, 2.8 | Chat thread + SQL artifact + Exploration artifacts | 5 weeks |
| **M4: App & Template Frontend** | 2.9 | App preview, template browser, schema mapping UI | 3 weeks |
| **M5: Testing** | 3 | Test harness integration + AI failure analysis | 2 weeks |
| **M6: Observability** | 4 | Metrics, topology, Grafana | 3 weeks |
| **M7: Production** | 5 | Persistence + deployment + management | 3 weeks |
| **M8: AI Intelligence** | 6 | Proactive suggestions, schema monitoring, performance analysis | 3 weeks |

**Total Estimated: 27 weeks (6.75 months)**

> The chat-first architecture with assistant-ui still provides massive savings over
> custom UI work. The additional 11 weeks cover app generation (Journey 7), templates
> (Journey 8), and AI proactive intelligence (Journey 9) — features that fundamentally
> differentiate Velostream Studio from every competitor.

---

## Dependencies

### NPM Packages (Studio Frontend)

```json
{
  "dependencies": {
    "next": "^14.0",
    "react": "^18.0",
    "@assistant-ui/react": "latest",
    "@assistant-ui/react-ai-sdk": "latest",
    "ai": "^3.0",
    "@ai-sdk/anthropic": "latest",
    "@monaco-editor/react": "^4.6",
    "recharts": "^2.10",
    "@tanstack/react-table": "^8.10",
    "@xyflow/react": "^12.0",
    "tailwindcss": "^3.4",
    "@radix-ui/react-*": "latest",
    "lucide-react": "^0.300",
    "zod": "^3.22"
  }
}
```

### Rust Crates (Studio Backend)

```toml
# Web server
axum = { version = "0.7", features = ["ws"] }
tower-http = { version = "0.5", features = ["cors", "compression"] }
tokio-tungstenite = "0.21"

# Database (notebook persistence)
sqlx = { version = "0.7", features = ["sqlite", "runtime-tokio"] }

# AI Integration
reqwest = { version = "0.11", features = ["json"] }
```

---

## FR-084 Components Reused

| FR-084 Component | Studio Usage | Phase |
|------------------|--------------|-------|
| `SchemaDataGenerator` | Synthetic data for test tool | 3 |
| `QueryExecutor` | Execute SQL via test/execute endpoints | 1, 3 |
| `AssertionEngine` | Validate query outputs in test tool | 3 |
| `AiAssistant` | Schema inference, failure analysis | 1, 3 |
| `InMemorySchemaRegistry` | Avro/Protobuf schema support | 1 |
| `TestSpec` | Define test configurations | 3 |
| `CapturedOutput` | Capture results for visualization artifacts | 2 |
| `TestHarnessInfra::fetch_topic_info` | List topics with partition/message counts | 1.6 |
| `TestHarnessInfra::fetch_topic_schema` | Infer schema from topic message samples | 1.6, 1.7 |
| `TestHarnessInfra::peek_topic_messages` | Preview messages without consuming | 1.6 |
| `TestHarnessInfra::get_consumer_info` | List consumer groups and positions | 1.6 |
| `SchemaInferencer::infer_from_sql()` | Infer input schema from SQL for annotation analysis | 6 |
| `StreamJobServer` | Deploy coordinated multi-job apps | 1.7 |
| `ProcessorMetricsHelper` | Register and emit metrics for deployed app queries | 1.7, 6 |
