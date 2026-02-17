# FR-085: Velostream SQL Studio - Technical Design

## Overview

This document details the technical architecture for Velostream SQL Studio, an AI-powered
**chat-first** interface for streaming SQL development with integrated test harness support.

The key insight: instead of building a traditional cell-based notebook with a secondary
chat input, the Studio is a **conversation thread** where AI responses produce **artifacts**
— editable SQL queries, live charts, test results, and deployment summaries. This is
powered by [assistant-ui](https://github.com/Yonom/assistant-ui) (open-source chat
framework) and [shadcn.io/ai](https://sdk.vercel.ai/docs) (chat-specific UI components).

---

## Core Concepts

### Thread Model (replaces Notebook Model)

The conversation thread IS the notebook. Each user message is a prompt, and each AI
response contains tool calls that produce artifacts.

```typescript
// assistant-ui provides Thread, Message, and ToolResult types.
// We extend them with Velostream-specific artifact types.

// A Velostream "notebook" is persisted as a thread with metadata
interface VelostreamThread {
  id: string;
  name: string;
  threadId: string;           // assistant-ui thread ID
  context: ThreadContext;     // accumulated sources, metrics, alerts
  metadata: ThreadMetadata;
}

// Accumulated context from all tool results in the thread
interface ThreadContext {
  sources: Map<string, SourceConfig>;
  sinks: Map<string, SinkConfig>;
  metrics: MetricAnnotation[];
  alerts: AlertAnnotation[];
  schemas: Map<string, DataSchema>;

  // Data source connections — supports Kafka, File, FileMmap, S3, ClickHouse, Database
  // Multiple connections can coexist (e.g., Kafka + S3 for hybrid pipelines)
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

// Tool result artifacts rendered in the chat thread
type ArtifactType =
  | 'sql-editor'         // Monaco editor with editable SQL
  | 'query-results'      // Table or chart with live data
  | 'test-results'       // Pass/fail assertions with AI analysis
  | 'deploy-summary'     // Jobs, metrics, alerts, dashboard link
  | 'topology'           // React Flow pipeline DAG
  | 'schema-viewer'      // Data schema display
  | 'topic-list'         // Topic grid with partition/message counts
  | 'data-preview'       // Formatted JSON messages with metadata
  | 'app-preview'        // Multi-query app with queries, metrics, alerts, dashboard, topology
  | 'template-browser';  // Template library grid with categories, descriptions, preview

interface Artifact {
  type: ArtifactType;
  data: unknown;        // type-specific payload
  editable: boolean;    // can the user modify this artifact?
}
```

### Interaction Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CHAT-FIRST INTERACTION FLOW                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. USER MESSAGE                                                        │
│     └─ "Show me trading volume by symbol for the last hour"             │
│                                                                         │
│  2. AI RESPONSE (streamed via assistant-ui)                             │
│     ├─ Text: "I'll create a windowed aggregation query..."              │
│     └─ Tool call: generate_sql(prompt, context)                         │
│        └─ Tool result → SQL Editor artifact (editable)                  │
│                                                                         │
│  3. USER INTERACTION WITH ARTIFACT                                      │
│     ├─ [Edit] → Opens Monaco editor inline                              │
│     ├─ [Run]  → Tool call: execute_query(sql)                           │
│     │   └─ Tool result → Chart/Table artifact (live streaming)          │
│     ├─ [Test] → Tool call: test_query(sql, schema, assertions)          │
│     │   └─ Tool result → Test Results artifact (pass/fail)              │
│     └─ [Deploy] → Tool call: deploy_pipeline(notebook_id)               │
│         └─ Tool result → Deploy Summary artifact                        │
│                                                                         │
│  4. FOLLOW-UP MESSAGE                                                   │
│     └─ "Add a filter for volume > 1000"                                 │
│     └─ AI sees previous artifacts as context, generates updated SQL     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Frontend Architecture

### Design Principle: Build on assistant-ui, Don't Reinvent

assistant-ui provides the chat thread, streaming, message rendering, and tool-result
framework. We focus on building **artifact renderers** (SQL editor, charts, test results)
and **tool definitions** (the bridge between chat and backend API).

What assistant-ui handles for us (skip building):
- Chat message rendering with markdown
- Streaming text display (typewriter effect)
- Tool call / tool result rendering pipeline
- Thread state management (messages, branches, edits)
- Input composition with attachments
- Message editing and regeneration

What we build:
- Artifact renderers (Monaco, Recharts, TanStack Table, React Flow)
- Tool definitions (validate, execute, test, deploy → backend API calls)
- Velostream-specific UI (pipeline topology, deploy wizard, schema viewer)
- WebSocket integration for live streaming query results

### Project Structure

```
studio/
├── src/
│   ├── app/
│   │   ├── layout.tsx                   # Root layout with assistant-ui provider
│   │   ├── page.tsx                     # Thread list (saved notebooks)
│   │   ├── thread/
│   │   │   └── [id]/page.tsx            # Single thread = notebook
│   │   ├── pipelines/
│   │   │   ├── page.tsx                 # Deployed pipelines list
│   │   │   └── [id]/page.tsx            # Pipeline detail
│   │   └── api/                         # Next.js API routes (proxy to Axum)
│   │
│   ├── components/
│   │   ├── chat/
│   │   │   ├── ThreadProvider.tsx       # assistant-ui runtime config + tools
│   │   │   ├── ThreadView.tsx           # Main chat + artifact split view
│   │   │   ├── AssistantMessage.tsx     # Custom AI message with tool results
│   │   │   └── Suggestions.tsx          # Quick action chips
│   │   │
│   │   ├── artifacts/                   # Tool result renderers
│   │   │   ├── SqlEditorArtifact.tsx    # Monaco editor (editable, run/test buttons)
│   │   │   ├── QueryResultsArtifact.tsx # Chart or table (auto-selected)
│   │   │   ├── TestResultsArtifact.tsx  # Pass/fail with AI analysis
│   │   │   ├── DeploySummaryArtifact.tsx # Jobs, metrics, dashboard link
│   │   │   ├── TopologyArtifact.tsx     # React Flow pipeline DAG
│   │   │   ├── SchemaViewerArtifact.tsx # Schema display with field types
│   │   │   ├── TopicListArtifact.tsx    # Topic grid (partition/message counts)
│   │   │   ├── DataPreviewArtifact.tsx  # JSON message viewer with metadata
│   │   │   ├── AppPreviewArtifact.tsx   # Multi-query app preview (queries/dashboard/topology)
│   │   │   └── TemplateBrowserArtifact.tsx # Template library with categories and search
│   │   │
│   │   ├── editor/
│   │   │   ├── SqlEditor.tsx            # Monaco wrapper with Velostream syntax
│   │   │   └── CompletionProvider.tsx   # AI-powered inline completions
│   │   │
│   │   ├── viz/
│   │   │   ├── VizRenderer.tsx          # Auto-select chart type from query metadata
│   │   │   ├── BarChart.tsx             # Categorical data
│   │   │   ├── LineChart.tsx            # Time series
│   │   │   ├── GaugeChart.tsx           # Single metrics
│   │   │   ├── DataTable.tsx            # TanStack virtual table
│   │   │   └── GrafanaEmbed.tsx         # Iframe embed for deployed dashboards
│   │   │
│   │   ├── test/
│   │   │   ├── TestDialog.tsx           # Configure schema, records, assertions
│   │   │   └── TestReport.tsx           # Detailed test report view
│   │   │
│   │   └── deploy/
│   │       ├── DeployDialog.tsx         # Deployment wizard
│   │       └── PipelineStatus.tsx       # Running jobs status
│   │
│   ├── lib/
│   │   ├── tools/                       # assistant-ui tool definitions
│   │   │   ├── validate-sql.ts          # POST /api/validate
│   │   │   ├── generate-sql.ts          # POST /api/nl-to-sql
│   │   │   ├── execute-query.ts         # POST /api/execute
│   │   │   ├── test-query.ts            # POST /api/test
│   │   │   ├── deploy-pipeline.ts       # POST /api/deploy
│   │   │   ├── generate-data.ts         # POST /api/generate-data
│   │   │   ├── connect-source.ts         # POST /api/connect (Kafka, File, S3, DB)
│   │   │   ├── list-sources.ts          # GET /api/sources (topics/files/tables)
│   │   │   ├── inspect-source.ts        # GET /api/sources/{id}/schema
│   │   │   ├── peek-source.ts           # GET /api/sources/{id}/preview
│   │   │   ├── generate-app.ts          # POST /api/generate-app
│   │   │   ├── test-app.ts              # POST /api/apps/{id}/test
│   │   │   ├── deploy-app.ts            # POST /api/apps/{id}/deploy
│   │   │   ├── list-templates.ts        # GET /api/templates
│   │   │   ├── get-template.ts          # GET /api/templates/{id}
│   │   │   ├── customize-template.ts    # POST /api/templates/{id}/customize
│   │   │   └── analyze.ts              # POST /api/analyze, /api/analyze/annotations
│   │   │
│   │   ├── api/
│   │   │   ├── client.ts                # Base API client
│   │   │   └── websocket.ts             # WebSocket for streaming results
│   │   │
│   │   ├── viz/
│   │   │   ├── infer-chart.ts           # Chart type inference from query metadata
│   │   │   └── transform-data.ts        # Data transformation for charts
│   │   │
│   │   └── context/
│   │       ├── thread-context.ts        # Accumulate sources/metrics from thread
│   │       └── annotations.ts           # Parse @metric, @alert from SQL
│   │
│   └── hooks/
│       ├── useStreamingResults.ts       # WebSocket hook for live query data
│       ├── useArtifactActions.ts        # Edit/Run/Test/Deploy actions on artifacts
│       └── useProactiveSuggestions.ts   # Trigger AI analysis after key events
│
├── package.json
├── tailwind.config.js
└── tsconfig.json
```

### Key Components

#### ThreadProvider.tsx — assistant-ui Runtime Configuration

```typescript
'use client';

import { AssistantRuntimeProvider } from '@assistant-ui/react';
import { useVercelUseChat } from '@assistant-ui/react-ai-sdk';
import { useChat } from 'ai/react';
import { velostreamTools } from '@/lib/tools';

export function ThreadProvider({ children, threadId }: {
  children: React.ReactNode;
  threadId?: string;
}) {
  // Connect to Velostream backend via Vercel AI SDK adapter
  const chat = useChat({
    api: '/api/chat',       // Next.js proxy → Axum backend
    id: threadId,
    maxSteps: 5,            // Allow multi-step tool chains
  });

  const runtime = useVercelUseChat(chat);

  return (
    <AssistantRuntimeProvider runtime={runtime}>
      {children}
    </AssistantRuntimeProvider>
  );
}
```

#### ThreadView.tsx — Split Chat + Artifact Layout

```typescript
'use client';

import { Thread, ThreadMessages, Composer } from '@assistant-ui/react';
import { AssistantMessage } from './AssistantMessage';
import { Suggestions } from './Suggestions';

export function ThreadView() {
  return (
    <div className="flex h-screen">
      {/* Chat Panel (left) */}
      <div className="w-1/2 flex flex-col border-r">
        <Thread>
          <ThreadMessages
            components={{
              AssistantMessage: AssistantMessage,
            }}
          />
          <Suggestions />
          <Composer placeholder="Describe your streaming query..." />
        </Thread>
      </div>

      {/* Artifact Panel (right) — shows the active/selected artifact */}
      <div className="w-1/2 flex flex-col">
        <ArtifactPanel />
      </div>
    </div>
  );
}
```

#### SqlEditorArtifact.tsx — Monaco as a Tool Result

```typescript
'use client';

import { useState } from 'react';
import { SqlEditor } from '../editor/SqlEditor';
import { Button } from '@/components/ui/button';

interface SqlEditorArtifactProps {
  sql: string;
  annotations: SqlAnnotations;
  explanation: string;
  onRun: (sql: string) => Promise<void>;
  onTest: (sql: string) => Promise<void>;
}

// Rendered inside the chat thread as a tool result
export function SqlEditorArtifact({
  sql: initialSql,
  annotations,
  explanation,
  onRun,
  onTest,
}: SqlEditorArtifactProps) {
  const [sql, setSql] = useState(initialSql);
  const [isEditing, setIsEditing] = useState(false);

  return (
    <div className="border rounded-lg bg-card">
      {/* Explanation from AI */}
      <div className="p-3 text-sm text-muted-foreground border-b">
        {explanation}
      </div>

      {/* Monaco SQL Editor */}
      <SqlEditor
        sql={sql}
        readOnly={!isEditing}
        annotations={annotations}
        onChange={setSql}
      />

      {/* Action buttons */}
      <div className="flex gap-2 p-2 border-t">
        <Button variant="outline" size="sm"
          onClick={() => setIsEditing(!isEditing)}>
          {isEditing ? 'Done' : 'Edit'}
        </Button>
        <Button size="sm" onClick={() => onRun(sql)}>
          Run
        </Button>
        <Button variant="secondary" size="sm"
          onClick={() => onTest(sql)}>
          Test
        </Button>
      </div>
    </div>
  );
}
```

#### Tool Definitions (assistant-ui ↔ Backend API Bridge)

```typescript
// lib/tools/generate-sql.ts
// Registered as an assistant-ui tool — the AI calls this to generate SQL

import { tool } from 'ai';
import { z } from 'zod';

export const generateSqlTool = tool({
  description: 'Generate Velostream streaming SQL from a natural language description',
  parameters: z.object({
    prompt: z.string().describe('Natural language description of the query'),
    context: z.object({
      schemas: z.record(z.any()).optional(),
      previousSql: z.array(z.string()).optional(),
    }).optional(),
  }),
  execute: async ({ prompt, context }) => {
    const response = await fetch('/api/nl-to-sql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt, context }),
    });
    return response.json();
    // Returns: { sql, explanation, suggestedVizType, annotations, validation }
    // Rendered by SqlEditorArtifact
  },
});

// lib/tools/execute-query.ts
export const executeQueryTool = tool({
  description: 'Execute a Velostream SQL query and return results',
  parameters: z.object({
    sql: z.string().describe('The SQL query to execute'),
    options: z.object({
      timeout_ms: z.number().optional(),
      max_rows: z.number().optional(),
    }).optional(),
  }),
  execute: async ({ sql, options }) => {
    const response = await fetch('/api/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql, options }),
    });
    return response.json();
    // Returns: { columns, rows, metadata }
    // Rendered by QueryResultsArtifact
  },
});

// lib/tools/test-query.ts
export const testQueryTool = tool({
  description: 'Test a SQL query with synthetic data and assertions',
  parameters: z.object({
    sql: z.string(),
    schema: z.any().optional(),
    records: z.number().optional(),
    assertions: z.array(z.any()).optional(),
  }),
  execute: async (params) => {
    const response = await fetch('/api/test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params),
    });
    return response.json();
    // Returns: { passed, assertions, output, performance, ai_analysis }
    // Rendered by TestResultsArtifact
  },
});
```

#### Data Source Exploration Tool Definitions

```typescript
// lib/tools/connect-source.ts
// Connects to any supported data source — Kafka, File, FileMmap, S3, ClickHouse, Database

export const connectSourceTool = tool({
  description: 'Connect to a data source (Kafka, File, FileMmap, S3, ClickHouse, or Database)',
  parameters: z.object({
    uri: z.string().describe('Source URI (e.g., kafka://broker:9092, file:///data/trades.csv, s3://bucket/prefix)'),
    type: z.enum(['kafka', 'file', 'file_mmap', 's3', 'clickhouse', 'database']).optional()
      .describe('Source type (auto-detected from URI scheme if omitted)'),
    format: z.enum(['json', 'csv', 'jsonl', 'parquet', 'avro', 'orc', 'protobuf']).optional()
      .describe('Data format (for file-based sources)'),
    options: z.record(z.string()).optional()
      .describe('Source-specific options (region, delimiter, header, schema_registry_url, etc.)'),
  }),
  execute: async ({ uri, type, format, options }) => {
    const response = await fetch('/api/connect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ uri, type, options: { ...options, format } }),
    });
    return response.json();
    // Returns: { connected, source_id, uri, type, details }
    // Updates ThreadContext.connections
  },
});

// lib/tools/list-sources.ts
// Lists available data items (topics for Kafka, files for File/S3, tables for DB)

export const listSourcesTool = tool({
  description: 'List available data (Kafka topics, files, database tables) on connected source',
  parameters: z.object({
    source_id: z.string().optional().describe('Source connection ID (uses default if omitted)'),
    filter: z.string().optional().describe('Filter by name substring'),
  }),
  execute: async ({ source_id, filter }) => {
    const params = new URLSearchParams();
    if (source_id) params.set('source_id', source_id);
    if (filter) params.set('filter', filter);
    const response = await fetch(`/api/sources?${params}`);
    return response.json();
    // Returns: { items: [{ name, type, details }], total }
    // Rendered by TopicListArtifact (generic — works for topics, files, tables)
  },
});

// lib/tools/inspect-source.ts
// Infers schema from any source by sampling data

export const inspectSourceTool = tool({
  description: 'Inspect a data source to discover its schema and sample data',
  parameters: z.object({
    source_id: z.string().describe('Source connection ID'),
    name: z.string().optional().describe('Specific item name (topic, file, table)'),
    maxRecords: z.number().optional().describe('Records to sample (default: 10)'),
  }),
  execute: async ({ source_id, name, maxRecords }) => {
    const params = new URLSearchParams();
    if (maxRecords) params.set('max_records', String(maxRecords));
    const path = name ? `/api/sources/${source_id}/schema?name=${encodeURIComponent(name)}` : `/api/sources/${source_id}/schema`;
    const response = await fetch(`${path}${params.toString() ? '&' + params.toString() : ''}`);
    return response.json();
    // Returns: { source, fields, sample_value, records_sampled }
    // Rendered by SchemaViewerArtifact
  },
});

// lib/tools/peek-source.ts
// Previews data from any source (messages for Kafka, rows for files/DB)

export const peekSourceTool = tool({
  description: 'Preview data from a source (messages, rows, or records)',
  parameters: z.object({
    source_id: z.string().describe('Source connection ID'),
    name: z.string().optional().describe('Specific item name (topic, file, table)'),
    limit: z.number().optional().describe('Number of records (default: 10)'),
    fromEnd: z.boolean().optional().describe('Read from latest/end (default: true, Kafka only)'),
  }),
  execute: async ({ source_id, name, limit, fromEnd }) => {
    const params = new URLSearchParams();
    if (name) params.set('name', name);
    if (limit) params.set('limit', String(limit));
    if (fromEnd !== undefined) params.set('from_end', String(fromEnd));
    const response = await fetch(`/api/sources/${source_id}/preview?${params}`);
    return response.json();
    // Returns: { source, records: [...], format }
    // Rendered by DataPreviewArtifact
  },
});
```

> **Note**: The Kafka-specific tools (`list_topics`, `inspect_topic`, `peek_messages`)
> are retained as convenience aliases that delegate to the generic source tools when
> a Kafka connection is the default. This ensures backward compatibility with Journey 1.

#### TopicListArtifact.tsx — Topic Discovery Grid

```typescript
'use client';

interface TopicListArtifactProps {
  topics: Array<{
    name: string;
    partitions: Array<{ partition: number; message_count: number }>;
    total_messages: number;
    is_test_topic: boolean;
  }>;
  onInspect: (topicName: string) => void;
  onPeek: (topicName: string) => void;
}

export function TopicListArtifact({ topics, onInspect, onPeek }: TopicListArtifactProps) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3 p-4">
      {topics.map((topic) => (
        <div key={topic.name} className="border rounded-lg p-3 hover:bg-accent/50">
          <div className="flex justify-between items-start">
            <div>
              <h4 className="font-mono font-medium">{topic.name}</h4>
              <p className="text-sm text-muted-foreground mt-1">
                {topic.partitions.length} partitions ·{' '}
                {topic.total_messages.toLocaleString()} messages
              </p>
            </div>
            {topic.is_test_topic && (
              <span className="text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded">
                test
              </span>
            )}
          </div>
          <div className="flex gap-2 mt-2">
            <Button variant="outline" size="xs" onClick={() => onInspect(topic.name)}>
              Schema
            </Button>
            <Button variant="outline" size="xs" onClick={() => onPeek(topic.name)}>
              Peek
            </Button>
          </div>
        </div>
      ))}
    </div>
  );
}
```

#### DataPreviewArtifact.tsx — Message Viewer

```typescript
'use client';

interface DataPreviewArtifactProps {
  topic: string;
  messages: Array<{
    partition: number;
    offset: number;
    key: string | null;
    value: string;       // JSON string
    timestamp_ms: number | null;
    headers: Array<[string, string]>;
  }>;
}

export function DataPreviewArtifact({ topic, messages }: DataPreviewArtifactProps) {
  return (
    <div className="border rounded-lg">
      <div className="p-3 border-b bg-muted/50">
        <h4 className="font-mono text-sm">{topic}</h4>
        <p className="text-xs text-muted-foreground">
          {messages.length} messages
        </p>
      </div>
      <div className="divide-y max-h-96 overflow-y-auto">
        {messages.map((msg, i) => (
          <div key={i} className="p-3">
            <div className="flex gap-3 text-xs text-muted-foreground mb-1">
              <span>P{msg.partition}</span>
              <span>offset {msg.offset}</span>
              {msg.key && <span>key: {msg.key}</span>}
              {msg.timestamp_ms && (
                <span>{new Date(msg.timestamp_ms).toISOString()}</span>
              )}
            </div>
            <pre className="text-sm font-mono bg-muted/30 rounded p-2 overflow-x-auto">
              {JSON.stringify(JSON.parse(msg.value), null, 2)}
            </pre>
            {msg.headers.length > 0 && (
              <div className="text-xs text-muted-foreground mt-1">
                headers: {msg.headers.map(([k, v]) => `${k}=${v}`).join(', ')}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
```

#### App Generation Tool Definitions

```typescript
// lib/tools/generate-app.ts
import { tool } from 'ai';
import { z } from 'zod';

// Generates a multi-query application from a natural language description
// The AI analyzes the prompt, inspects connected data sources, and produces
// a coordinated set of SQL queries with metrics, alerts, dashboard, and topology
export const generateAppTool = tool({
  description: 'Generate a complete multi-query streaming application from a natural language description',
  parameters: z.object({
    prompt: z.string().describe('Natural language description of the desired application'),
    include_dashboard: z.boolean().default(true),
    include_alerts: z.boolean().default(true),
    max_queries: z.number().default(10),
  }),
  execute: async ({ prompt, include_dashboard, include_alerts, max_queries }) => {
    const response = await fetch('/api/generate-app', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        prompt,
        options: { include_dashboard, include_alerts, include_topology: true, max_queries },
      }),
    });
    const { app } = await response.json();
    return {
      artifact: { type: 'app-preview' as const, data: app, editable: true },
      text: `Generated "${app.name}" with ${app.queries.length} queries, ${app.queries.flatMap(q => q.metrics).length} metrics, and ${app.queries.flatMap(q => q.alerts).length} alerts.`,
    };
  },
});

// Tests all queries in an app with coordinated synthetic data
export const testAppTool = tool({
  description: 'Test all queries in an application with coordinated synthetic data',
  parameters: z.object({
    app_id: z.string().describe('The app ID to test'),
    records_per_source: z.number().default(1000),
    seed: z.number().optional(),
  }),
  execute: async ({ app_id, records_per_source, seed }) => {
    const response = await fetch(`/api/apps/${app_id}/test`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ records_per_source, seed, timeout_ms: 60000 }),
    });
    const result = await response.json();
    return {
      artifact: { type: 'test-results' as const, data: result, editable: false },
      text: result.passed
        ? `All ${result.total_queries} queries passed.`
        : `${result.passed_queries}/${result.total_queries} queries passed. See details below.`,
    };
  },
});

// Deploys all queries in an app as coordinated Velostream jobs
export const deployAppTool = tool({
  description: 'Deploy all queries in an application as coordinated streaming jobs with metrics, alerts, and dashboard',
  parameters: z.object({
    app_id: z.string().describe('The app ID to deploy'),
    name: z.string().describe('Deployment name'),
    deploy_dashboard: z.boolean().default(true),
    deploy_alerts: z.boolean().default(true),
  }),
  execute: async ({ app_id, name, deploy_dashboard, deploy_alerts }) => {
    const response = await fetch(`/api/apps/${app_id}/deploy`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, deploy_dashboard, deploy_alerts }),
    });
    const { deployment } = await response.json();
    return {
      artifact: { type: 'deploy-summary' as const, data: deployment, editable: false },
      text: `Deployed "${name}" — ${deployment.jobs.length} jobs running. Dashboard: ${deployment.dashboard_url}`,
    };
  },
});
```

#### Template Tool Definitions

```typescript
// lib/tools/templates.ts
import { tool } from 'ai';
import { z } from 'zod';

// Browse available application templates
export const listTemplatesTool = tool({
  description: 'List available application templates, optionally filtered by category',
  parameters: z.object({
    category: z.string().optional().describe('Filter by category: financial, iot, ecommerce, observability, security, general'),
    search: z.string().optional().describe('Search templates by name or description'),
  }),
  execute: async ({ category, search }) => {
    const params = new URLSearchParams();
    if (category) params.set('category', category);
    if (search) params.set('search', search);
    const response = await fetch(`/api/templates?${params}`);
    const { templates, categories } = await response.json();
    return {
      artifact: { type: 'template-browser' as const, data: { templates, categories }, editable: false },
      text: `Found ${templates.length} templates${category ? ` in "${category}"` : ''}.`,
    };
  },
});

// Get full template detail and preview
export const getTemplateTool = tool({
  description: 'Get full detail for a specific template including queries, metrics, and schema requirements',
  parameters: z.object({
    template_id: z.string().describe('Template ID to retrieve'),
  }),
  execute: async ({ template_id }) => {
    const response = await fetch(`/api/templates/${template_id}`);
    const { template } = await response.json();
    return {
      artifact: { type: 'app-preview' as const, data: template, editable: false },
      text: `Template "${template.name}": ${template.queries.length} queries, requires topics: ${Object.keys(template.schema_requirements).join(', ')}.`,
    };
  },
});

// Customize a template for the user's actual data
export const customizeTemplateTool = tool({
  description: 'Customize a template by mapping its schema to your actual topic fields and calibrating thresholds',
  parameters: z.object({
    template_id: z.string().describe('Template to customize'),
    schema_mapping: z.record(z.object({
      topic: z.string(),
      field_mapping: z.record(z.string()),
    })),
    params: z.record(z.string()).optional().describe('Template parameters (name, prefix, window sizes, thresholds)'),
    auto_discover: z.boolean().default(true).describe('Auto-inspect connected topics to verify schema'),
  }),
  execute: async ({ template_id, schema_mapping, params, auto_discover }) => {
    const response = await fetch(`/api/templates/${template_id}/customize`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ schema_mapping, params, auto_discover }),
    });
    const { app, schema_validation } = await response.json();
    const warnings = Object.values(schema_validation || {})
      .filter((v: any) => v.suggestions?.length > 0);
    return {
      artifact: { type: 'app-preview' as const, data: app, editable: true },
      text: `Customized "${app.name}" from template. ${warnings.length > 0 ? `${warnings.length} suggestion(s) — see details.` : 'All fields mapped successfully.'}`,
    };
  },
});
```

#### AI Analysis Tool Definitions

```typescript
// lib/tools/analyze.ts
import { tool } from 'ai';
import { z } from 'zod';

// Proactive AI analysis — called automatically after key events or on-demand
export const analyzeTool = tool({
  description: 'Analyze the current thread context and return proactive suggestions for improvements',
  parameters: z.object({
    trigger: z.enum(['post_connect', 'post_generate', 'post_test', 'post_deploy', 'on_demand'])
      .describe('What event triggered this analysis'),
  }),
  execute: async ({ trigger }, { threadContext }) => {
    const response = await fetch('/api/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ thread_context: threadContext, trigger }),
    });
    const { suggestions, context_summary } = await response.json();
    if (suggestions.length === 0) return { text: 'Everything looks good — no suggestions at this time.' };
    const highPriority = suggestions.filter((s: any) => s.priority === 'high');
    return {
      text: `${suggestions.length} suggestion(s)${highPriority.length > 0 ? ` (${highPriority.length} high priority)` : ''}:\n` +
        suggestions.map((s: any) => `• **${s.title}**: ${s.description}`).join('\n'),
      suggestions,
    };
  },
});

// Suggest @metric and @alert annotations for bare SQL
export const analyzeAnnotationsTool = tool({
  description: 'Analyze SQL queries and suggest appropriate @metric and @alert annotations',
  parameters: z.object({
    queries: z.array(z.object({
      sql: z.string(),
    })),
  }),
  execute: async ({ queries }) => {
    const response = await fetch('/api/analyze/annotations', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ queries }),
    });
    const { annotated_queries } = await response.json();
    const totalMetrics = annotated_queries.reduce((sum: number, q: any) => sum + q.added_metrics.length, 0);
    const totalAlerts = annotated_queries.reduce((sum: number, q: any) => sum + q.added_alerts.length, 0);
    return {
      artifact: { type: 'sql-editor' as const, data: { sql: annotated_queries[0].annotated_sql }, editable: true },
      text: `Added ${totalMetrics} metric(s) and ${totalAlerts} alert(s). Review the annotations and click Deploy when ready.`,
    };
  },
});
```

#### AppPreviewArtifact.tsx — Multi-Query Application Preview

```typescript
'use client';

import { useState } from 'react';
import { SqlEditor } from '../editor/SqlEditor';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';

interface AppQuery {
  id: string;
  name: string;
  sql: string;
  source_topic: string;
  sink_topic: string;
  metrics: Array<{ name: string; type: string; labels?: string[] }>;
  alerts: Array<{ name: string; condition: string; severity: string }>;
}

interface AppPreviewArtifactProps {
  app: {
    id: string;
    name: string;
    description: string;
    queries: AppQuery[];
    dashboard?: {
      title: string;
      panels: Array<{ title: string; type: string; position: { row: number; col: number } }>;
    };
    topology?: {
      nodes: Array<{ id: string; type: string; label: string }>;
      edges: Array<{ from: string; to: string }>;
    };
  };
  onTestAll: (appId: string) => Promise<void>;
  onDeployApp: (appId: string) => Promise<void>;
  onEditQuery: (queryId: string, newSql: string) => void;
}

export function AppPreviewArtifact({ app, onTestAll, onDeployApp, onEditQuery }: AppPreviewArtifactProps) {
  const [activeTab, setActiveTab] = useState('queries');

  const totalMetrics = app.queries.reduce((sum, q) => sum + q.metrics.length, 0);
  const totalAlerts = app.queries.reduce((sum, q) => sum + q.alerts.length, 0);

  return (
    <div className="border rounded-lg bg-card">
      {/* Header */}
      <div className="p-4 border-b">
        <h3 className="text-lg font-semibold">{app.name}</h3>
        <p className="text-sm text-muted-foreground mt-1">{app.description}</p>
        <div className="flex gap-4 mt-2 text-xs text-muted-foreground">
          <span>{app.queries.length} queries</span>
          <span>{totalMetrics} metrics</span>
          <span>{totalAlerts} alerts</span>
        </div>
      </div>

      {/* Tabs: Queries | Dashboard | Topology */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="w-full border-b rounded-none">
          <TabsTrigger value="queries">Queries</TabsTrigger>
          <TabsTrigger value="dashboard">Dashboard</TabsTrigger>
          <TabsTrigger value="topology">Topology</TabsTrigger>
        </TabsList>

        <TabsContent value="queries" className="divide-y">
          {app.queries.map((query) => (
            <div key={query.id} className="p-3">
              <div className="flex justify-between items-center mb-2">
                <h4 className="font-medium text-sm">{query.name}</h4>
                <span className="text-xs text-muted-foreground">
                  {query.source_topic} → {query.sink_topic}
                </span>
              </div>
              <SqlEditor
                sql={query.sql}
                readOnly={false}
                onChange={(newSql) => onEditQuery(query.id, newSql)}
                height="120px"
              />
              {query.metrics.length > 0 && (
                <div className="flex gap-1 mt-1 flex-wrap">
                  {query.metrics.map((m) => (
                    <span key={m.name} className="text-xs bg-blue-100 text-blue-800 px-1.5 py-0.5 rounded">
                      @{m.name} ({m.type})
                    </span>
                  ))}
                </div>
              )}
            </div>
          ))}
        </TabsContent>

        <TabsContent value="dashboard">
          {app.dashboard && (
            <div className="p-4 grid grid-cols-2 gap-2">
              {app.dashboard.panels.map((panel, i) => (
                <div key={i} className="border rounded p-2 text-center text-sm">
                  <div className="text-xs text-muted-foreground">{panel.type}</div>
                  <div className="font-medium">{panel.title}</div>
                </div>
              ))}
            </div>
          )}
        </TabsContent>

        <TabsContent value="topology">
          {/* React Flow topology visualization — see TopologyArtifact.tsx */}
          <div className="p-4 text-sm text-muted-foreground">
            Pipeline topology with {app.topology?.nodes.length} nodes
          </div>
        </TabsContent>
      </Tabs>

      {/* Actions */}
      <div className="p-3 border-t flex justify-end gap-2">
        <Button variant="outline" onClick={() => onTestAll(app.id)}>Test All</Button>
        <Button onClick={() => onDeployApp(app.id)}>Deploy App</Button>
      </div>
    </div>
  );
}
```

#### TemplateBrowserArtifact.tsx — Template Library

```typescript
'use client';

import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface Template {
  id: string;
  name: string;
  category: string;
  description: string;
  query_count: number;
  metrics_count: number;
  alerts_count: number;
  required_topics: string[];
  tags: string[];
  popularity: number;
}

interface TemplateBrowserArtifactProps {
  templates: Template[];
  categories: Array<{ name: string; count: number }>;
  onSelectTemplate: (templateId: string) => void;
  onPreviewTemplate: (templateId: string) => void;
}

export function TemplateBrowserArtifact({
  templates,
  categories,
  onSelectTemplate,
  onPreviewTemplate,
}: TemplateBrowserArtifactProps) {
  const [filter, setFilter] = useState<string | null>(null);
  const [search, setSearch] = useState('');

  const filtered = templates
    .filter((t) => !filter || t.category === filter)
    .filter((t) => !search || t.name.toLowerCase().includes(search.toLowerCase())
      || t.description.toLowerCase().includes(search.toLowerCase()));

  return (
    <div className="border rounded-lg bg-card">
      {/* Category chips */}
      <div className="p-3 border-b flex gap-2 flex-wrap items-center">
        <button
          className={`text-xs px-2 py-1 rounded ${!filter ? 'bg-primary text-primary-foreground' : 'bg-muted'}`}
          onClick={() => setFilter(null)}
        >
          All ({templates.length})
        </button>
        {categories.map((cat) => (
          <button
            key={cat.name}
            className={`text-xs px-2 py-1 rounded capitalize ${filter === cat.name ? 'bg-primary text-primary-foreground' : 'bg-muted'}`}
            onClick={() => setFilter(cat.name)}
          >
            {cat.name} ({cat.count})
          </button>
        ))}
        <Input
          placeholder="Search templates..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="ml-auto w-48 h-7 text-xs"
        />
      </div>

      {/* Template grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 p-4">
        {filtered.map((template) => (
          <div key={template.id} className="border rounded-lg p-3 hover:bg-accent/50 cursor-pointer"
               onClick={() => onPreviewTemplate(template.id)}>
            <div className="flex justify-between items-start">
              <h4 className="font-medium text-sm">{template.name}</h4>
              <span className="text-xs bg-muted px-1.5 py-0.5 rounded capitalize">{template.category}</span>
            </div>
            <p className="text-xs text-muted-foreground mt-1">{template.description}</p>
            <div className="flex gap-3 mt-2 text-xs text-muted-foreground">
              <span>{template.query_count} queries</span>
              <span>{template.metrics_count} metrics</span>
              <span>{template.alerts_count} alerts</span>
            </div>
            <div className="flex gap-1 mt-2 flex-wrap">
              {template.tags.slice(0, 4).map((tag) => (
                <span key={tag} className="text-xs bg-muted px-1 py-0.5 rounded">{tag}</span>
              ))}
            </div>
            <div className="mt-2 flex gap-2">
              <Button variant="outline" size="xs" onClick={(e) => { e.stopPropagation(); onPreviewTemplate(template.id); }}>
                Preview
              </Button>
              <Button size="xs" onClick={(e) => { e.stopPropagation(); onSelectTemplate(template.id); }}>
                Use Template
              </Button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
```

#### Monaco AI Completions (in-editor, separate from chat)

```typescript
// lib/ai/completions.ts — Monaco inline completions (not assistant-ui)
import * as monaco from 'monaco-editor';

export function registerCompletionProvider(
  schema: SchemaInfo,
  threadContext: ThreadContext
) {
  return monaco.languages.registerInlineCompletionsProvider('sql', {
    provideInlineCompletions: async (model, position, context) => {
      const prefix = model.getValueInRange({
        startLineNumber: 1, startColumn: 1,
        endLineNumber: position.lineNumber, endColumn: position.column,
      });
      const suffix = model.getValueInRange({
        startLineNumber: position.lineNumber, startColumn: position.column,
        endLineNumber: model.getLineCount(),
        endColumn: model.getLineMaxColumn(model.getLineCount()),
      });

      const response = await fetch('/api/completions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prefix, suffix, context: { schema, threadContext } }),
      });
      const { completions } = await response.json();
      if (!completions?.length) return { items: [] };

      return {
        items: [{
          insertText: completions[0].text,
          range: new monaco.Range(
            position.lineNumber, position.column,
            position.lineNumber, position.column,
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
│   │   ├── connect.rs            # POST /api/connect (Kafka connection)
│   │   ├── topics.rs             # GET /api/topics, /api/topics/{name}
│   │   ├── topic_schema.rs       # GET /api/topics/{name}/schema
│   │   ├── messages.rs           # GET /api/topics/{name}/messages
│   │   ├── consumers.rs          # GET /api/consumers
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

studio/                                # NEW: Frontend (chat-first)
├── src/
│   ├── app/                          # Next.js App Router
│   │   ├── layout.tsx                # Root with assistant-ui provider
│   │   ├── page.tsx                  # Thread list (saved notebooks)
│   │   └── thread/[id]/page.tsx      # Single thread = notebook
│   ├── components/
│   │   ├── chat/                     # assistant-ui thread + messages
│   │   ├── artifacts/                # Tool result renderers
│   │   ├── editor/                   # Monaco SQL editor
│   │   ├── viz/                      # Recharts, TanStack Table
│   │   ├── test/                     # Test dialog, report
│   │   └── deploy/                   # Deploy wizard, pipeline status
│   ├── lib/
│   │   ├── tools/                    # assistant-ui tool definitions
│   │   ├── api/                      # Base API client + WebSocket
│   │   ├── viz/                      # Chart inference + transform
│   │   └── context/                  # Thread context accumulation
│   └── hooks/
│       ├── useStreamingResults.ts    # WebSocket for live query data
│       └── useArtifactActions.ts     # Edit/Run/Test/Deploy actions
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

**Removed** (provided by assistant-ui):
- `zustand` — thread state managed by assistant-ui runtime
- `swr` — tool calls handle data fetching
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
