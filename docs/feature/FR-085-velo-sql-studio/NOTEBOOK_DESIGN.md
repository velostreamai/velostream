# FR-085: Velostream SQL Studio - Technical Design

## Overview

This document details the technical architecture for Velostream SQL Studio, an AI-powered notebook interface for streaming SQL development with integrated test harness support.

---

## Core Concepts

### Notebook Model

```typescript
interface Notebook {
  id: string;
  name: string;
  cells: Cell[];
  context: NotebookContext;
  metadata: NotebookMetadata;
}

interface Cell {
  id: string;
  type: 'sql' | 'markdown' | 'insight';

  // The natural language prompt that created this cell
  nlPrompt?: string;

  // SQL content (editable)
  sql?: string;

  // Parsed annotations
  annotations: SqlAnnotations;

  // Last execution results
  results?: CellResults;

  // Test configuration
  testConfig?: CellTestConfig;

  // Visualization config (auto-inferred or manual)
  vizConfig?: VizConfig;
}

interface NotebookContext {
  // Accumulated from all cells
  sources: Map<string, SourceConfig>;
  sinks: Map<string, SinkConfig>;
  metrics: MetricAnnotation[];
  alerts: AlertAnnotation[];

  // Schemas (auto-inferred or user-defined)
  schemas: Map<string, DataSchema>;

  // Cell dependencies (for execution order)
  dependencies: CellDependency[];
}
```

### Cell Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CELL LIFECYCLE                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. CREATE                                                              │
│     ├─ User types NL prompt                                             │
│     ├─ AI generates SQL (with notebook context)                         │
│     ├─ SQL validated                                                    │
│     ├─ Annotations parsed (@metric, @alert)                             │
│     └─ Visualization type inferred                                      │
│                                                                         │
│  2. EDIT                                                                │
│     ├─ User modifies SQL in Monaco                                      │
│     ├─ AI completions suggest as typing                                 │
│     ├─ Re-validate on change                                            │
│     └─ Re-infer visualization if columns change                         │
│                                                                         │
│  3. RUN                                                                 │
│     ├─ Generate synthetic data (if no live source)                      │
│     ├─ Execute SQL via backend                                          │
│     ├─ Stream results via WebSocket                                     │
│     └─ Render visualization                                             │
│                                                                         │
│  4. TEST                                                                │
│     ├─ Generate test data via velo-test harness                         │
│     ├─ Execute SQL                                                      │
│     ├─ Run assertions                                                   │
│     ├─ Show pass/fail with AI analysis                                  │
│     └─ Suggest fixes if failed                                          │
│                                                                         │
│  5. DELETE                                                              │
│     ├─ Remove from notebook                                             │
│     └─ Update context (remove sources/sinks/metrics)                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Frontend Architecture

### Project Structure

```
studio/
├── src/
│   ├── app/
│   │   ├── page.tsx                    # Main notebook view
│   │   ├── notebooks/
│   │   │   ├── page.tsx                # Notebook list
│   │   │   └── [id]/page.tsx           # Single notebook
│   │   ├── pipelines/
│   │   │   ├── page.tsx                # Deployed pipelines list
│   │   │   └── [id]/page.tsx           # Pipeline detail
│   │   └── api/                        # Next.js API routes (proxy)
│   │
│   ├── components/
│   │   ├── notebook/
│   │   │   ├── NotebookView.tsx        # Main notebook container
│   │   │   ├── Cell.tsx                # Single cell component
│   │   │   ├── CellHeader.tsx          # NL prompt display
│   │   │   ├── CellEditor.tsx          # Monaco SQL editor
│   │   │   ├── CellResults.tsx         # Results table/chart
│   │   │   ├── CellControls.tsx        # Run/Test/Delete buttons
│   │   │   ├── CellTestResults.tsx     # Inline test results
│   │   │   └── NotebookSummary.tsx     # Accumulated metrics/alerts
│   │   │
│   │   ├── chat/
│   │   │   ├── ChatInput.tsx           # NL input with send button
│   │   │   ├── ChatSuggestions.tsx     # Quick action suggestions
│   │   │   └── StreamingResponse.tsx   # Typewriter effect for AI
│   │   │
│   │   ├── editor/
│   │   │   ├── SqlEditor.tsx           # Monaco wrapper
│   │   │   ├── CompletionProvider.tsx  # AI completions
│   │   │   └── AnnotationHighlight.tsx # @metric highlighting
│   │   │
│   │   ├── viz/
│   │   │   ├── VizRenderer.tsx         # Auto-select chart type
│   │   │   ├── BarChart.tsx            # Categorical data
│   │   │   ├── LineChart.tsx           # Time series
│   │   │   ├── GaugeChart.tsx          # Single metrics
│   │   │   ├── DataTable.tsx           # TanStack table
│   │   │   └── GrafanaEmbed.tsx        # Iframe embed
│   │   │
│   │   ├── test/
│   │   │   ├── TestDialog.tsx          # Configure test run
│   │   │   ├── SchemaEditor.tsx        # Edit data schema
│   │   │   ├── AssertionBuilder.tsx    # Add assertions
│   │   │   └── TestReport.tsx          # Full test report
│   │   │
│   │   └── deploy/
│   │       ├── DeployDialog.tsx        # Deployment wizard
│   │       ├── DeployPreview.tsx       # What will be deployed
│   │       ├── DashboardPreview.tsx    # Grafana preview
│   │       └── PipelineStatus.tsx      # Running jobs status
│   │
│   ├── lib/
│   │   ├── api/
│   │   │   ├── client.ts               # API client
│   │   │   ├── notebooks.ts            # Notebook CRUD
│   │   │   ├── execute.ts              # Query execution
│   │   │   ├── test.ts                 # Test harness API
│   │   │   └── deploy.ts               # Deployment API
│   │   │
│   │   ├── ai/
│   │   │   ├── completions.ts          # Monaco completion provider
│   │   │   ├── nl-to-sql.ts            # NL→SQL generation
│   │   │   └── prompts.ts              # System prompts
│   │   │
│   │   ├── viz/
│   │   │   ├── infer-chart.ts          # Chart type inference
│   │   │   └── transform-data.ts       # Data transformation
│   │   │
│   │   └── notebook/
│   │       ├── context.ts              # Notebook context management
│   │       ├── annotations.ts          # Parse @metric, @alert
│   │       └── dependencies.ts         # Cell dependency graph
│   │
│   └── hooks/
│       ├── useNotebook.ts              # Notebook state management
│       ├── useCell.ts                  # Cell state management
│       ├── useWebSocket.ts             # Streaming results
│       └── useAiCompletion.ts          # AI completion hook
│
├── package.json
├── tailwind.config.js
└── tsconfig.json
```

### Key Components

#### NotebookView.tsx

```typescript
'use client';

import { useNotebook } from '@/hooks/useNotebook';
import { Cell } from './Cell';
import { ChatInput } from '../chat/ChatInput';
import { NotebookSummary } from './NotebookSummary';
import { DeployButton } from '../deploy/DeployButton';

export function NotebookView({ notebookId }: { notebookId: string }) {
  const {
    notebook,
    cells,
    context,
    addCell,
    updateCell,
    deleteCell,
    runCell,
    testCell,
  } = useNotebook(notebookId);

  const handleNlSubmit = async (prompt: string) => {
    // AI generates SQL from prompt with full notebook context
    const response = await fetch('/api/nl-to-sql', {
      method: 'POST',
      body: JSON.stringify({
        prompt,
        context: {
          previousCells: cells.map(c => ({ sql: c.sql, results: c.results })),
          schemas: context.schemas,
          sources: context.sources,
        },
      }),
    });

    const { sql, annotations, vizType } = await response.json();

    addCell({
      type: 'sql',
      nlPrompt: prompt,
      sql,
      annotations,
      vizConfig: { type: vizType },
    });
  };

  return (
    <div className="flex flex-col h-screen">
      {/* Header */}
      <header className="border-b p-4 flex justify-between items-center">
        <h1 className="text-xl font-semibold">{notebook.name}</h1>
        <DeployButton notebook={notebook} context={context} />
      </header>

      {/* Cells */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {cells.map((cell) => (
          <Cell
            key={cell.id}
            cell={cell}
            onUpdate={(updates) => updateCell(cell.id, updates)}
            onDelete={() => deleteCell(cell.id)}
            onRun={() => runCell(cell.id)}
            onTest={() => testCell(cell.id)}
          />
        ))}
      </div>

      {/* Notebook Summary */}
      <NotebookSummary context={context} />

      {/* Chat Input */}
      <div className="border-t p-4">
        <ChatInput onSubmit={handleNlSubmit} />
      </div>
    </div>
  );
}
```

#### Cell.tsx

```typescript
'use client';

import { useState } from 'react';
import { CellHeader } from './CellHeader';
import { CellEditor } from './CellEditor';
import { CellResults } from './CellResults';
import { CellControls } from './CellControls';
import { CellTestResults } from './CellTestResults';
import type { Cell as CellType } from '@/types';

interface CellProps {
  cell: CellType;
  onUpdate: (updates: Partial<CellType>) => void;
  onDelete: () => void;
  onRun: () => Promise<void>;
  onTest: () => Promise<void>;
}

export function Cell({ cell, onUpdate, onDelete, onRun, onTest }: CellProps) {
  const [isEditing, setIsEditing] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [isTesting, setIsTesting] = useState(false);

  const handleRun = async () => {
    setIsRunning(true);
    try {
      await onRun();
    } finally {
      setIsRunning(false);
    }
  };

  const handleTest = async () => {
    setIsTesting(true);
    try {
      await onTest();
    } finally {
      setIsTesting(false);
    }
  };

  return (
    <div className="border rounded-lg bg-card">
      {/* NL Prompt Header */}
      {cell.nlPrompt && (
        <CellHeader prompt={cell.nlPrompt} />
      )}

      {/* SQL Editor */}
      <CellEditor
        sql={cell.sql || ''}
        isEditing={isEditing}
        annotations={cell.annotations}
        onSqlChange={(sql) => onUpdate({ sql })}
        onToggleEdit={() => setIsEditing(!isEditing)}
      />

      {/* Controls */}
      <CellControls
        isRunning={isRunning}
        isTesting={isTesting}
        onEdit={() => setIsEditing(true)}
        onRun={handleRun}
        onTest={handleTest}
        onDelete={onDelete}
      />

      {/* Results (Chart or Table) */}
      {cell.results && (
        <CellResults
          results={cell.results}
          vizConfig={cell.vizConfig}
        />
      )}

      {/* Test Results (inline) */}
      {cell.testResults && (
        <CellTestResults results={cell.testResults} />
      )}
    </div>
  );
}
```

#### Monaco AI Completions

```typescript
// lib/ai/completions.ts
import * as monaco from 'monaco-editor';

const COMPLETION_SYSTEM_PROMPT = `You are Velostream SQL completion engine.
Return ONLY the completion text, no explanation.

Velostream streaming SQL syntax:
- WINDOW TUMBLING(INTERVAL 'n' unit)
- WINDOW SLIDING(INTERVAL 'n' unit, INTERVAL 'm' unit)
- WINDOW SESSION(INTERVAL 'n' unit)
- ROWS WINDOW BUFFER N ROWS PARTITION BY col ORDER BY col
- EMIT CHANGES | EMIT FINAL
- @metric, @metric_type, @metric_labels annotations

Available tables and columns:
{schema}

Recent cell context:
{previousCells}`;

export function registerCompletionProvider(
  schema: SchemaInfo,
  previousCells: Cell[]
) {
  return monaco.languages.registerInlineCompletionsProvider('sql', {
    provideInlineCompletions: async (model, position, context) => {
      const textUntilPosition = model.getValueInRange({
        startLineNumber: 1,
        startColumn: 1,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      });

      const textAfterPosition = model.getValueInRange({
        startLineNumber: position.lineNumber,
        startColumn: position.column,
        endLineNumber: model.getLineCount(),
        endColumn: model.getLineMaxColumn(model.getLineCount()),
      });

      // Debounce - don't call API too frequently
      const completion = await fetchCompletion({
        prefix: textUntilPosition,
        suffix: textAfterPosition,
        schema,
        previousCells,
      });

      if (!completion) return { items: [] };

      return {
        items: [{
          insertText: completion.text,
          range: new monaco.Range(
            position.lineNumber,
            position.column,
            position.lineNumber,
            position.column
          ),
        }],
      };
    },
    freeInlineCompletions: () => {},
  });
}
```

#### Visualization Inference

```typescript
// lib/viz/infer-chart.ts

interface QueryMetadata {
  columns: ColumnInfo[];
  hasGroupBy: boolean;
  hasWindow: boolean;
  windowType?: 'tumbling' | 'sliding' | 'session';
  aggregations: string[];
}

export function inferChartType(metadata: QueryMetadata): VizType {
  const { columns, hasGroupBy, hasWindow, aggregations } = metadata;

  // Time series: has timestamp column + aggregations
  const hasTimestamp = columns.some(c => c.type === 'TIMESTAMP');
  if (hasTimestamp && hasWindow) {
    return 'line';
  }

  // Single metric: one numeric column, no group by
  if (aggregations.length === 1 && !hasGroupBy) {
    return 'gauge';
  }

  // Categorical grouping: GROUP BY string column
  const groupByColumn = columns.find(c => c.isGroupKey);
  if (groupByColumn?.type === 'STRING' && aggregations.length > 0) {
    // Pie chart for proportions (COUNT, SUM with few groups)
    if (aggregations.includes('COUNT') || aggregations.includes('SUM')) {
      return 'bar'; // Default to bar, user can switch to pie
    }
  }

  // Distribution: numeric columns without specific grouping
  if (columns.some(c => c.type === 'DECIMAL' || c.type === 'INTEGER')) {
    return 'bar';
  }

  // Default: show as table
  return 'table';
}

export function transformForChart(
  data: Record<string, any>[],
  vizType: VizType,
  columns: ColumnInfo[]
): ChartData {
  switch (vizType) {
    case 'line':
      return transformTimeSeries(data, columns);
    case 'bar':
      return transformCategorical(data, columns);
    case 'gauge':
      return transformGauge(data, columns);
    default:
      return { rows: data };
  }
}
```

---

## Backend Architecture

### API Module Structure

```
src/
├── api/
│   ├── mod.rs                    # API module exports
│   ├── server.rs                 # Axum HTTP server
│   ├── routes/
│   │   ├── mod.rs
│   │   ├── validate.rs           # POST /api/validate
│   │   ├── execute.rs            # POST /api/execute
│   │   ├── stream.rs             # WebSocket /api/stream/{id}
│   │   ├── schema.rs             # GET /api/schema
│   │   ├── completions.rs        # POST /api/completions
│   │   ├── nl_to_sql.rs          # POST /api/nl-to-sql
│   │   ├── notebooks.rs          # CRUD /api/notebooks
│   │   ├── test.rs               # POST /api/test (velo-test integration)
│   │   ├── generate_data.rs      # POST /api/generate-data
│   │   ├── deploy.rs             # POST /api/deploy
│   │   └── jobs.rs               # CRUD /api/jobs
│   │
│   ├── handlers/
│   │   ├── mod.rs
│   │   ├── ai_handler.rs         # Claude API integration
│   │   ├── notebook_handler.rs   # Notebook operations
│   │   └── deploy_handler.rs     # Deployment logic
│   │
│   └── models/
│       ├── mod.rs
│       ├── notebook.rs           # Notebook, Cell types
│       ├── execution.rs          # ExecutionResult, StreamingResult
│       └── test.rs               # TestConfig, TestResult
│
├── bin/
│   └── velo_studio.rs            # Studio server binary
```

### Test Harness Integration

The Studio integrates with FR-084's test harness components:

```rust
// src/api/routes/test.rs
use crate::velostream::test_harness::{
    SchemaDataGenerator,
    QueryExecutor,
    AssertionEngine,
    AiAssistant,
    TestSpec,
};

#[derive(Deserialize)]
pub struct TestCellRequest {
    pub sql: String,
    pub schema: Option<DataSchema>,
    pub records: Option<usize>,
    pub assertions: Vec<Assertion>,
    pub seed: Option<u64>,
}

#[derive(Serialize)]
pub struct TestCellResponse {
    pub passed: bool,
    pub assertions: Vec<AssertionResult>,
    pub output: CapturedOutput,
    pub performance: PerformanceMetrics,
    pub ai_analysis: Option<String>,
}

pub async fn test_cell(
    State(state): State<AppState>,
    Json(request): Json<TestCellRequest>,
) -> Result<Json<TestCellResponse>, ApiError> {
    // 1. Get or infer schema
    let schema = match request.schema {
        Some(s) => s,
        None => {
            // AI-infer schema from SQL
            let ai = AiAssistant::new(&state.claude_api_key);
            ai.infer_schema(&request.sql, &[]).await?
        }
    };

    // 2. Generate test data using FR-084's SchemaDataGenerator
    let mut generator = SchemaDataGenerator::new(request.seed);
    let records = generator.generate(&schema, request.records.unwrap_or(1000))?;

    // 3. Execute SQL with generated data
    let executor = QueryExecutor::new_in_memory();
    let output = executor.execute(&request.sql, &records).await?;

    // 4. Run assertions using FR-084's AssertionEngine
    let assertion_engine = AssertionEngine::new();
    let assertion_results: Vec<AssertionResult> = request.assertions
        .iter()
        .map(|a| assertion_engine.evaluate(a, &output))
        .collect();

    let passed = assertion_results.iter().all(|r| r.passed);

    // 5. AI analysis if any failures (using FR-084's AiAssistant)
    let ai_analysis = if !passed {
        let ai = AiAssistant::new(&state.claude_api_key);
        let failures: Vec<_> = assertion_results.iter()
            .filter(|r| !r.passed)
            .collect();
        Some(ai.analyze_failures(&failures, &request.sql, &schema).await?)
    } else {
        None
    };

    Ok(Json(TestCellResponse {
        passed,
        assertions: assertion_results,
        output,
        performance: PerformanceMetrics {
            execution_time_ms: output.execution_time_ms,
            memory_peak_bytes: output.memory_peak_bytes,
            records_per_second: output.records.len() as f64 /
                (output.execution_time_ms as f64 / 1000.0),
        },
        ai_analysis,
    }))
}
```

### Generate Data Endpoint

```rust
// src/api/routes/generate_data.rs
use crate::velostream::test_harness::{
    SchemaDataGenerator,
    DataSchema,
    FieldConstraint,
};

#[derive(Deserialize)]
pub struct GenerateDataRequest {
    pub schema: DataSchema,
    pub records: usize,
    pub seed: Option<u64>,
    pub foreign_keys: Option<Vec<ForeignKeyConfig>>,
}

#[derive(Serialize)]
pub struct GenerateDataResponse {
    pub records: Vec<serde_json::Value>,
    pub schema_used: DataSchema,
}

pub async fn generate_data(
    Json(request): Json<GenerateDataRequest>,
) -> Result<Json<GenerateDataResponse>, ApiError> {
    let mut generator = SchemaDataGenerator::new(request.seed);

    // Load foreign key reference data (FR-084 Phase 9 feature)
    if let Some(fks) = request.foreign_keys {
        for fk in fks {
            generator.load_reference_data(
                &fk.table,
                &fk.field,
                fk.values.clone(),
            );
        }
    }

    // Generate records
    let records = generator.generate(&request.schema, request.records)?;

    // Convert to JSON
    let json_records: Vec<serde_json::Value> = records
        .iter()
        .map(|r| r.to_json())
        .collect();

    Ok(Json(GenerateDataResponse {
        records: json_records,
        schema_used: request.schema,
    }))
}
```

### NL to SQL with Notebook Context

```rust
// src/api/routes/nl_to_sql.rs

const NL_TO_SQL_SYSTEM_PROMPT: &str = r#"
You are a Velostream streaming SQL expert. Convert natural language to valid Velostream SQL.

IMPORTANT Velostream syntax rules:
1. Time windows: WINDOW TUMBLING(INTERVAL 'n' MINUTE) - goes AFTER GROUP BY
2. Row windows: ROWS WINDOW BUFFER N ROWS PARTITION BY col ORDER BY col - inside OVER()
3. Emit modes: EMIT CHANGES (incremental) or EMIT FINAL (on window close)
4. Annotations: -- @metric: name, -- @metric_type: counter|gauge|histogram

Available sources and schemas:
{schemas}

Previous cells in this notebook (for context):
{previous_cells}

User request: {prompt}

Return JSON:
{
  "sql": "the SQL query",
  "explanation": "brief explanation",
  "suggestedVizType": "line|bar|gauge|table",
  "annotations": [{"type": "metric", "name": "...", ...}]
}
"#;

#[derive(Deserialize)]
pub struct NlToSqlRequest {
    pub prompt: String,
    pub context: NotebookContext,
}

#[derive(Serialize)]
pub struct NlToSqlResponse {
    pub sql: String,
    pub explanation: String,
    pub suggested_viz_type: String,
    pub annotations: Vec<SqlAnnotation>,
    pub validation: ValidationResult,
}

pub async fn nl_to_sql(
    State(state): State<AppState>,
    Json(request): Json<NlToSqlRequest>,
) -> Result<Json<NlToSqlResponse>, ApiError> {
    // Build prompt with context
    let prompt = NL_TO_SQL_SYSTEM_PROMPT
        .replace("{schemas}", &format_schemas(&request.context.schemas))
        .replace("{previous_cells}", &format_cells(&request.context.previous_cells))
        .replace("{prompt}", &request.prompt);

    // Call Claude API
    let client = anthropic::Client::new(&state.claude_api_key);
    let response = client
        .messages()
        .create(MessageCreateParams {
            model: "claude-sonnet-4-20250514",
            max_tokens: 2048,
            messages: vec![Message {
                role: Role::User,
                content: prompt,
            }],
        })
        .await?;

    // Parse response
    let generated: GeneratedSql = serde_json::from_str(&response.content)?;

    // Validate the generated SQL
    let validator = SqlValidator::new();
    let validation = validator.validate(&generated.sql)?;

    // If validation fails, ask Claude to fix it
    let final_sql = if !validation.valid {
        let fix_prompt = format!(
            "The SQL you generated has an error: {}\n\nPlease fix it:\n{}",
            validation.error.unwrap_or_default(),
            generated.sql
        );
        let fixed = call_claude_fix(&client, &fix_prompt).await?;
        fixed.sql
    } else {
        generated.sql
    };

    Ok(Json(NlToSqlResponse {
        sql: final_sql,
        explanation: generated.explanation,
        suggested_viz_type: generated.suggested_viz_type,
        annotations: generated.annotations,
        validation,
    }))
}
```

### Deploy Notebook as Pipeline

```rust
// src/api/routes/deploy.rs

#[derive(Deserialize)]
pub struct DeployNotebookRequest {
    pub notebook_id: String,
    pub name: String,
    pub config: DeployConfig,
}

#[derive(Serialize)]
pub struct DeployNotebookResponse {
    pub pipeline_id: String,
    pub jobs: Vec<DeployedJob>,
    pub metrics: Vec<DeployedMetric>,
    pub alerts: Vec<DeployedAlert>,
    pub dashboard_url: Option<String>,
}

pub async fn deploy_notebook(
    State(state): State<AppState>,
    Json(request): Json<DeployNotebookRequest>,
) -> Result<Json<DeployNotebookResponse>, ApiError> {
    // 1. Load notebook
    let notebook = state.notebook_store.get(&request.notebook_id).await?;

    // 2. Build deployment plan
    let cells: Vec<_> = notebook.cells.iter()
        .filter(|c| c.cell_type == "sql")
        .collect();

    // 3. Deploy each cell as a job using existing StreamJobServer
    let mut deployed_jobs = Vec::new();
    for cell in &cells {
        let job = state.job_server.deploy_job(
            &format!("{}_{}", request.name, cell.id),
            "1.0.0",
            &cell.sql.clone().unwrap_or_default(),
            None, // Let the SQL define the sink
            None,
            None,
        ).await?;
        deployed_jobs.push(job);
    }

    // 4. Register metrics with Prometheus
    let metrics: Vec<_> = cells.iter()
        .flat_map(|c| &c.annotations.metrics)
        .collect();

    // 5. Configure alerts
    let alerts: Vec<_> = cells.iter()
        .flat_map(|c| &c.annotations.alerts)
        .collect();

    // 6. Generate and deploy Grafana dashboard
    let dashboard = generate_grafana_dashboard(&request.name, &metrics);
    let dashboard_url = if request.config.deploy_dashboard {
        Some(deploy_to_grafana(&state.grafana_client, &dashboard).await?)
    } else {
        None
    };

    Ok(Json(DeployNotebookResponse {
        pipeline_id: uuid::Uuid::new_v4().to_string(),
        jobs: deployed_jobs,
        metrics: metrics.into_iter().cloned().collect(),
        alerts: alerts.into_iter().cloned().collect(),
        dashboard_url,
    }))
}
```

---

## Annotation System

### SQL Annotations

```rust
// src/api/models/annotations.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlAnnotations {
    pub metrics: Vec<MetricAnnotation>,
    pub alerts: Vec<AlertAnnotation>,
    pub viz_hints: Vec<VizHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricAnnotation {
    pub name: String,
    pub metric_type: MetricType, // counter, gauge, histogram
    pub labels: Vec<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertAnnotation {
    pub condition: String,       // e.g., "volatility > 0.05"
    pub for_duration: Option<String>, // e.g., "5m"
    pub severity: Option<String>,     // e.g., "critical", "warning"
    pub channel: Option<String>,      // e.g., "slack:#alerts"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VizHint {
    pub chart_type: Option<String>,  // line, bar, gauge
    pub title: Option<String>,
    pub x_axis: Option<String>,
    pub y_axis: Option<String>,
}

pub fn parse_annotations(sql: &str) -> SqlAnnotations {
    let mut annotations = SqlAnnotations::default();

    for line in sql.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("--") {
            continue;
        }

        let content = trimmed.trim_start_matches("--").trim();

        if content.starts_with("@metric:") {
            annotations.metrics.push(parse_metric(content));
        } else if content.starts_with("@alert:") {
            annotations.alerts.push(parse_alert(content));
        } else if content.starts_with("@viz:") {
            annotations.viz_hints.push(parse_viz_hint(content));
        }
    }

    annotations
}
```

### Example Annotated SQL

```sql
-- @metric: trade_volume
-- @metric_type: counter
-- @metric_labels: symbol, exchange
-- @viz: bar
-- @viz_title: Trading Volume by Symbol

SELECT
    symbol,
    exchange,
    COUNT(*) as trade_count,
    SUM(quantity) as volume
FROM trades
GROUP BY symbol, exchange
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
```

---

## Dashboard Generation

```rust
// src/api/handlers/dashboard_handler.rs

pub fn generate_grafana_dashboard(
    name: &str,
    metrics: &[MetricAnnotation],
) -> GrafanaDashboard {
    let panels: Vec<Panel> = metrics.iter().enumerate().map(|(i, metric)| {
        let panel_type = match metric.metric_type {
            MetricType::Counter => "graph",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "heatmap",
        };

        let query = if metric.labels.is_empty() {
            format!("velostream_{}", metric.name)
        } else {
            format!(
                "sum(rate(velostream_{}[5m])) by ({})",
                metric.name,
                metric.labels.join(", ")
            )
        };

        Panel {
            id: i as u32,
            panel_type: panel_type.to_string(),
            title: metric.name.clone(),
            grid_pos: GridPos {
                x: (i % 2) as u32 * 12,
                y: (i / 2) as u32 * 8,
                w: 12,
                h: 8,
            },
            targets: vec![Target {
                expr: query,
                legend_format: if metric.labels.is_empty() {
                    metric.name.clone()
                } else {
                    format!("{{{}}}", metric.labels.join(", "))
                },
            }],
        }
    }).collect();

    GrafanaDashboard {
        title: format!("Velostream: {}", name),
        panels,
        time: TimeRange {
            from: "now-1h".to_string(),
            to: "now".to_string(),
        },
        refresh: "5s".to_string(),
    }
}
```

---

## WebSocket Streaming

```rust
// src/api/routes/stream.rs
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use futures::{StreamExt, SinkExt};

pub async fn stream_handler(
    ws: WebSocketUpgrade,
    Path(execution_id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_stream(socket, execution_id, state))
}

async fn handle_stream(
    socket: WebSocket,
    execution_id: String,
    state: AppState,
) {
    let (mut sender, mut receiver) = socket.split();

    // Get the execution stream
    let execution = state.executions.get(&execution_id).await;

    // Send initial connection message
    sender.send(Message::Text(json!({
        "type": "connected",
        "execution_id": execution_id,
    }).to_string())).await.ok();

    // Stream results
    let mut result_stream = execution.results_stream();
    let mut batch_number = 0;

    loop {
        tokio::select! {
            // Receive from execution
            Some(rows) = result_stream.next() => {
                batch_number += 1;
                sender.send(Message::Text(json!({
                    "type": "rows",
                    "data": rows,
                    "batch_number": batch_number,
                }).to_string())).await.ok();
            }

            // Receive client commands
            Some(Ok(msg)) = receiver.next() => {
                if let Message::Text(text) = msg {
                    let cmd: ClientCommand = serde_json::from_str(&text).unwrap();
                    match cmd.action.as_str() {
                        "pause" => execution.pause(),
                        "resume" => execution.resume(),
                        "cancel" => {
                            execution.cancel();
                            break;
                        }
                        _ => {}
                    }
                }
            }

            // Execution complete
            _ = execution.completion() => {
                sender.send(Message::Text(json!({
                    "type": "complete",
                    "total_rows": execution.total_rows(),
                    "execution_time_ms": execution.elapsed_ms(),
                }).to_string())).await.ok();
                break;
            }
        }
    }
}
```

---

## Test Harness Component Reuse

The Studio leverages existing FR-084 test harness components:

| FR-084 Component | Studio Usage |
|------------------|--------------|
| `SchemaDataGenerator` | Generate synthetic data for cell testing |
| `QueryExecutor` | Execute SQL cells with captured output |
| `AssertionEngine` | Validate cell outputs against assertions |
| `AiAssistant` | Schema inference, failure analysis |
| `InMemorySchemaRegistry` | Avro/Protobuf schema support |
| `TestSpec` | Define notebook-wide test configurations |
| `CapturedOutput` | Capture results for visualization |

### Test Configuration per Cell

```typescript
interface CellTestConfig {
  // Data generation
  schema?: DataSchema;           // Auto-inferred or user-defined
  records?: number;              // Default: 1000
  seed?: number;                 // For reproducibility

  // Assertions (auto-generated or user-defined)
  assertions: Assertion[];

  // From FR-084 assertion types
  // - record_count: { operator, expected }
  // - schema_contains: { fields }
  // - no_nulls: { fields }
  // - field_values: { field, operator, value }
  // - aggregate_check: { expression, operator, expected }
  // - execution_time: { max_ms, min_ms }
  // - memory_usage: { max_mb }
}
```

---

## File Structure Summary

```
velostream/
├── src/
│   ├── api/                          # NEW: Studio API module
│   │   ├── mod.rs
│   │   ├── server.rs
│   │   ├── routes/
│   │   │   ├── validate.rs
│   │   │   ├── execute.rs
│   │   │   ├── stream.rs
│   │   │   ├── test.rs               # Integrates with FR-084
│   │   │   ├── generate_data.rs      # Uses FR-084 generator
│   │   │   ├── nl_to_sql.rs
│   │   │   ├── notebooks.rs
│   │   │   └── deploy.rs
│   │   ├── handlers/
│   │   └── models/
│   ├── velostream/
│   │   ├── test_harness/             # EXISTING: FR-084
│   │   │   ├── generator.rs          # SchemaDataGenerator
│   │   │   ├── executor.rs           # QueryExecutor
│   │   │   ├── assertions.rs         # AssertionEngine
│   │   │   ├── ai.rs                 # AiAssistant
│   │   │   └── schema.rs             # In-memory schema registry
│   │   └── ...
│   └── bin/
│       └── velo_studio.rs            # NEW: Studio server binary

studio/                                # NEW: Frontend project
├── src/
│   ├── app/
│   ├── components/
│   │   ├── notebook/
│   │   ├── chat/
│   │   ├── editor/
│   │   ├── viz/
│   │   ├── test/
│   │   └── deploy/
│   ├── lib/
│   │   ├── api/
│   │   ├── ai/
│   │   ├── viz/
│   │   └── notebook/
│   └── hooks/
├── package.json
└── tailwind.config.js
```

---

## Dependencies

### Rust Crates

```toml
[dependencies]
# Web server
axum = { version = "0.7", features = ["ws"] }
tower-http = { version = "0.5", features = ["cors", "compression"] }
tokio = { version = "1.0", features = ["full"] }

# Serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# Database (notebook persistence)
sqlx = { version = "0.7", features = ["sqlite", "runtime-tokio"] }

# AI
reqwest = { version = "0.11", features = ["json"] }

# WebSocket
tokio-tungstenite = "0.21"
futures = "0.3"
```

### NPM Packages

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

---

## Security Considerations

1. **API Authentication**: JWT tokens for API access
2. **SQL Injection**: All SQL goes through Velostream parser (not direct DB)
3. **Rate Limiting**: AI endpoints rate-limited per user
4. **CORS**: Strict origin checking in production
5. **Secrets**: Claude API key stored securely (env vars, secrets manager)

---

## Performance Targets

| Metric | Target |
|--------|--------|
| NL→SQL latency | <2s |
| AI completion latency | <500ms |
| SQL validation | <50ms |
| Test execution (1K records) | <1s |
| Test execution (100K records) | <10s |
| WebSocket message latency | <50ms |
| Dashboard generation | <100ms |
