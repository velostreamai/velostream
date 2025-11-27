# FR-085: Velostream SQL Studio - REST API Specification

## Base URL

```
http://localhost:8080/api
```

---

## Authentication

Initial implementation uses no authentication (development mode). Production will support:
- API Key header: `X-API-Key: <key>`
- JWT Bearer token: `Authorization: Bearer <token>`

---

## Endpoints Overview

| Category | Endpoint | Method | Description |
|----------|----------|--------|-------------|
| SQL | `/api/validate` | POST | Validate SQL syntax |
| SQL | `/api/execute` | POST | Execute SQL query |
| SQL | `/api/stream/{id}` | WS | Stream query results |
| Schema | `/api/schema` | GET | List all schemas |
| Schema | `/api/schema/{name}` | GET | Get specific schema |
| AI | `/api/completions` | POST | Get AI completions |
| AI | `/api/nl-to-sql` | POST | Convert NL to SQL |
| Test | `/api/test` | POST | Run cell tests |
| Test | `/api/generate-data` | POST | Generate synthetic data |
| Notebooks | `/api/notebooks` | CRUD | Notebook management |
| Jobs | `/api/jobs` | CRUD | Job management |
| Deploy | `/api/deploy` | POST | Deploy notebook as pipeline |
| Observability | `/api/dashboards/generate` | POST | Generate Grafana dashboard |
| Metrics | `/metrics` | GET | Prometheus metrics |

---

## SQL Endpoints

### `POST /api/validate`

Validates SQL syntax and returns parse tree information.

**Request:**
```json
{
  "sql": "SELECT symbol, AVG(price) FROM trades GROUP BY symbol EMIT CHANGES",
  "context": {
    "tables": ["trades", "orders"]
  }
}
```

**Response (Success):**
```json
{
  "valid": true,
  "ast": {
    "type": "StreamingQuery",
    "source": "trades",
    "fields": ["symbol", "AVG(price)"],
    "group_by": ["symbol"],
    "emit_mode": "Changes"
  },
  "annotations": [
    {
      "type": "metric",
      "name": "trade_volume",
      "metric_type": "counter",
      "labels": ["symbol"]
    }
  ],
  "warnings": []
}
```

**Response (Error):**
```json
{
  "valid": false,
  "error": {
    "message": "Expected FROM clause",
    "line": 1,
    "column": 15,
    "context": "SELECT symbol AVG(price)..."
  },
  "suggestions": [
    "Add FROM clause: SELECT symbol, AVG(price) FROM <table>"
  ]
}
```

---

### `POST /api/execute`

Executes a SQL query and returns results.

**Request:**
```json
{
  "sql": "SELECT symbol, price FROM trades WHERE price > 100 LIMIT 10",
  "options": {
    "timeout_ms": 5000,
    "max_rows": 1000,
    "format": "json"
  }
}
```

**Response:**
```json
{
  "success": true,
  "execution_id": "exec_abc123",
  "columns": [
    { "name": "symbol", "type": "STRING" },
    { "name": "price", "type": "DECIMAL(18,4)" }
  ],
  "rows": [
    { "symbol": "AAPL", "price": "152.3400" },
    { "symbol": "GOOGL", "price": "2845.1200" }
  ],
  "metadata": {
    "row_count": 2,
    "execution_time_ms": 45,
    "is_streaming": false
  }
}
```

---

### `WS /api/stream/{execution_id}`

WebSocket endpoint for streaming query results.

**Connection:**
```
ws://localhost:8080/api/stream/exec_abc123
```

**Server Messages:**
```json
// Connection established
{ "type": "connected", "execution_id": "exec_abc123" }

// Row batch
{
  "type": "rows",
  "data": [
    { "symbol": "AAPL", "price": "152.34" },
    { "symbol": "MSFT", "price": "378.91" }
  ],
  "batch_number": 1
}

// Metrics update
{
  "type": "metrics",
  "records_per_second": 12500,
  "total_records": 50000,
  "latency_ms": 2.3
}

// Completion
{
  "type": "complete",
  "total_rows": 150000,
  "execution_time_ms": 12000
}

// Error
{
  "type": "error",
  "message": "Connection to Kafka lost",
  "recoverable": true
}
```

**Client Messages:**
```json
{ "action": "pause" }
{ "action": "resume" }
{ "action": "cancel" }
```

---

## Test Harness Endpoints (FR-084 Integration)

### `POST /api/test`

Runs tests on a SQL cell with synthetic data.

**Request:**
```json
{
  "sql": "SELECT symbol, SUM(quantity) as volume FROM trades GROUP BY symbol EMIT CHANGES",
  "schema": {
    "name": "trades",
    "fields": [
      { "name": "symbol", "type": "string", "constraints": { "enum": ["AAPL", "GOOGL", "MSFT"] } },
      { "name": "price", "type": "decimal", "constraints": { "min": 50.0, "max": 5000.0 } },
      { "name": "quantity", "type": "integer", "constraints": { "min": 100, "max": 100000 } }
    ]
  },
  "records": 1000,
  "seed": 42,
  "assertions": [
    { "type": "record_count", "operator": "greater_than", "expected": 0 },
    { "type": "schema_contains", "fields": ["symbol", "volume"] },
    { "type": "no_nulls", "fields": ["symbol", "volume"] }
  ]
}
```

**Response (Success):**
```json
{
  "passed": true,
  "assertions": [
    { "type": "record_count", "passed": true, "actual": 3, "expected": "> 0" },
    { "type": "schema_contains", "passed": true, "found": ["symbol", "volume"] },
    { "type": "no_nulls", "passed": true, "fields": ["symbol", "volume"] }
  ],
  "output": {
    "columns": [
      { "name": "symbol", "type": "STRING" },
      { "name": "volume", "type": "INTEGER" }
    ],
    "rows": [
      { "symbol": "AAPL", "volume": 125000 },
      { "symbol": "GOOGL", "volume": 89000 },
      { "symbol": "MSFT", "volume": 67000 }
    ],
    "row_count": 3
  },
  "performance": {
    "execution_time_ms": 45,
    "memory_peak_bytes": 12582912,
    "records_per_second": 22222
  },
  "ai_analysis": null
}
```

**Response (Failure with AI Analysis):**
```json
{
  "passed": false,
  "assertions": [
    { "type": "join_coverage", "passed": false, "actual": 0.0, "expected": "> 0.8" }
  ],
  "output": { "columns": [], "rows": [], "row_count": 0 },
  "performance": { "execution_time_ms": 89, "memory_peak_bytes": 8388608 },
  "ai_analysis": "The JOIN on 'customer_id' produced no matches because:\n• trades contains customer_ids: [CUST001, CUST002, CUST003]\n• customers table contains: [C-100, C-200, C-300]\n\nSuggested fix:\nAdd a foreign key relationship in your schema:\n```yaml\nrelationships:\n  - field: customer_id\n    references: customers.id\n    strategy: sample\n```"
}
```

---

### `POST /api/generate-data`

Generates synthetic data using FR-084's SchemaDataGenerator.

**Request:**
```json
{
  "schema": {
    "name": "trades",
    "fields": [
      { "name": "symbol", "type": "string", "constraints": { "enum": ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA"] } },
      { "name": "price", "type": "decimal", "constraints": { "min": 50.0, "max": 5000.0, "distribution": "log_normal" } },
      { "name": "quantity", "type": "integer", "constraints": { "min": 100, "max": 100000 } },
      { "name": "timestamp", "type": "timestamp", "constraints": { "range": "relative", "start": "-1h", "end": "now" } }
    ]
  },
  "records": 100,
  "seed": 42,
  "foreign_keys": [
    { "table": "customers", "field": "id", "values": ["CUST001", "CUST002", "CUST003"] }
  ]
}
```

**Response:**
```json
{
  "records": [
    { "symbol": "AAPL", "price": "152.3400", "quantity": 5000, "timestamp": "2024-01-15T10:30:00Z" },
    { "symbol": "GOOGL", "price": "2845.1200", "quantity": 1200, "timestamp": "2024-01-15T10:30:01Z" }
  ],
  "schema_used": { ... },
  "metadata": {
    "record_count": 100,
    "generation_time_ms": 12
  }
}
```

---

## AI Endpoints

### `POST /api/completions`

Returns AI-powered SQL completions for Monaco editor.

**Request:**
```json
{
  "prefix": "SELECT symbol, AVG(price) FROM trades GROUP BY symbol WINDOW ",
  "suffix": " EMIT CHANGES",
  "cursor_position": 58,
  "context": {
    "schema": { "tables": ["trades"], "columns": [...] },
    "previous_cells": [
      { "sql": "SELECT * FROM trades WHERE symbol = 'AAPL'" }
    ]
  }
}
```

**Response:**
```json
{
  "completions": [
    {
      "text": "TUMBLING(INTERVAL '5' MINUTE)",
      "confidence": 0.92,
      "explanation": "5-minute tumbling window for time-based aggregation"
    },
    {
      "text": "SLIDING(INTERVAL '1' HOUR, INTERVAL '5' MINUTE)",
      "confidence": 0.78,
      "explanation": "1-hour sliding window with 5-minute slide"
    }
  ],
  "usage": {
    "prompt_tokens": 245,
    "completion_tokens": 32
  }
}
```

---

### `POST /api/nl-to-sql`

Converts natural language to SQL with notebook context.

**Request:**
```json
{
  "prompt": "Show me the average price per symbol for the last hour with 5 minute windows",
  "context": {
    "schemas": { "trades": { "columns": [...] } },
    "previous_cells": [
      { "sql": "SELECT symbol, price FROM trades", "results": { "row_count": 100 } }
    ],
    "sources": ["trades"],
    "metrics": []
  }
}
```

**Response:**
```json
{
  "sql": "-- @metric: avg_price\n-- @metric_type: gauge\n-- @metric_labels: symbol\n\nSELECT\n    symbol,\n    AVG(price) as avg_price,\n    COUNT(*) as trade_count\nFROM trades\nGROUP BY symbol\nWINDOW TUMBLING(INTERVAL '5' MINUTE)\nEMIT CHANGES",
  "explanation": "This query calculates the average price and trade count for each symbol using 5-minute tumbling windows. I've added @metric annotations for Prometheus export.",
  "suggested_viz_type": "bar",
  "annotations": [
    { "type": "metric", "name": "avg_price", "metric_type": "gauge", "labels": ["symbol"] }
  ],
  "validation": { "valid": true, "warnings": [] }
}
```

---

## Notebook Endpoints

### `GET /api/notebooks`

Lists all notebooks.

**Response:**
```json
{
  "notebooks": [
    {
      "id": "nb_abc123",
      "name": "Trading Analytics",
      "cell_count": 5,
      "metrics_count": 3,
      "created_at": "2024-01-15T10:00:00Z",
      "updated_at": "2024-01-15T14:30:00Z"
    }
  ],
  "total": 1
}
```

---

### `POST /api/notebooks`

Creates a new notebook.

**Request:**
```json
{
  "name": "Trading Analytics"
}
```

**Response:**
```json
{
  "id": "nb_def456",
  "name": "Trading Analytics",
  "cells": [],
  "context": { "sources": [], "sinks": [], "metrics": [], "alerts": [] },
  "created_at": "2024-01-15T14:00:00Z"
}
```

---

### `GET /api/notebooks/{id}`

Gets a notebook with all cells.

**Response:**
```json
{
  "id": "nb_abc123",
  "name": "Trading Analytics",
  "cells": [
    {
      "id": "cell_001",
      "type": "sql",
      "nl_prompt": "Show me trading volume by symbol",
      "sql": "SELECT symbol, SUM(quantity) as volume FROM trades GROUP BY symbol EMIT CHANGES",
      "annotations": { "metrics": [], "alerts": [], "viz_hints": [] },
      "viz_config": { "type": "bar" },
      "test_config": {
        "schema": { ... },
        "records": 1000,
        "assertions": [...]
      }
    }
  ],
  "context": {
    "sources": ["trades"],
    "sinks": [],
    "metrics": [{ "name": "trade_volume", "type": "counter" }],
    "alerts": []
  }
}
```

---

### `PUT /api/notebooks/{id}`

Updates a notebook.

**Request:**
```json
{
  "name": "Trading Analytics v2",
  "cells": [...]
}
```

---

### `DELETE /api/notebooks/{id}`

Deletes a notebook.

**Response:**
```json
{ "id": "nb_abc123", "deleted": true }
```

---

### `POST /api/notebooks/{id}/cells`

Adds a cell to a notebook.

**Request:**
```json
{
  "type": "sql",
  "nl_prompt": "Add price volatility monitoring",
  "sql": "SELECT symbol, STDDEV(price) / AVG(price) as volatility FROM trades GROUP BY symbol EMIT CHANGES",
  "position": 1
}
```

**Response:**
```json
{
  "id": "cell_002",
  "type": "sql",
  "nl_prompt": "Add price volatility monitoring",
  "sql": "...",
  "annotations": { ... }
}
```

---

### `PUT /api/notebooks/{id}/cells/{cell_id}`

Updates a cell.

---

### `DELETE /api/notebooks/{id}/cells/{cell_id}`

Deletes a cell.

---

## Deployment Endpoints

### `POST /api/deploy`

Deploys a notebook as a pipeline.

**Request:**
```json
{
  "notebook_id": "nb_abc123",
  "name": "trading-analytics",
  "config": {
    "parallelism": 4,
    "checkpoint_interval_ms": 60000,
    "deploy_dashboard": true
  }
}
```

**Response:**
```json
{
  "pipeline_id": "pipe_xyz789",
  "jobs": [
    { "id": "job_001", "name": "trading-analytics_cell_001", "status": "starting" },
    { "id": "job_002", "name": "trading-analytics_cell_002", "status": "starting" }
  ],
  "metrics": [
    { "name": "trade_volume", "prometheus_name": "velostream_trade_volume_total" }
  ],
  "alerts": [
    { "name": "high_volatility", "condition": "volatility > 0.05" }
  ],
  "dashboard_url": "http://grafana:3000/d/xyz789/trading-analytics"
}
```

---

### `POST /api/dashboards/generate`

Generates a Grafana dashboard from SQL annotations.

**Request:**
```json
{
  "notebook_id": "nb_abc123",
  "dashboard_name": "Trading Analytics"
}
```

**Response:**
```json
{
  "dashboard": {
    "title": "Trading Analytics",
    "uid": "trading-analytics",
    "panels": [
      {
        "id": 1,
        "type": "graph",
        "title": "trade_volume",
        "gridPos": { "x": 0, "y": 0, "w": 12, "h": 8 },
        "targets": [
          {
            "expr": "sum(rate(velostream_trade_volume_total[5m])) by (symbol)",
            "legendFormat": "{{symbol}}"
          }
        ]
      }
    ]
  },
  "grafana_json": "{...full dashboard JSON...}",
  "import_url": "http://grafana:3000/dashboard/import"
}
```

---

## Job Endpoints

### `GET /api/jobs`

Lists all jobs.

**Response:**
```json
{
  "jobs": [
    {
      "id": "job_abc123",
      "name": "trading-analytics_cell_001",
      "status": "running",
      "pipeline_id": "pipe_xyz789",
      "sql": "SELECT symbol, AVG(price) FROM trades...",
      "metrics": {
        "records_processed": 1250000,
        "records_per_second": 12500,
        "uptime_seconds": 3600
      }
    }
  ],
  "total": 1
}
```

---

### `POST /api/jobs/{id}/start`

Starts a stopped job.

---

### `POST /api/jobs/{id}/stop`

Stops a running job.

---

### `DELETE /api/jobs/{id}`

Deletes a job.

---

## Schema Endpoints

### `GET /api/schema`

Returns available tables and their schemas.

**Response:**
```json
{
  "tables": [
    {
      "name": "trades",
      "type": "stream",
      "columns": [
        { "name": "symbol", "type": "STRING", "nullable": false },
        { "name": "price", "type": "DECIMAL(18,4)", "nullable": false },
        { "name": "quantity", "type": "INTEGER", "nullable": false },
        { "name": "timestamp", "type": "TIMESTAMP", "nullable": false }
      ],
      "source": { "type": "kafka", "topic": "market-trades", "format": "avro" }
    }
  ],
  "functions": [
    { "name": "AVG", "type": "aggregate", "signature": "AVG(numeric) -> DECIMAL" },
    { "name": "TUMBLING", "type": "window", "signature": "WINDOW TUMBLING(INTERVAL 'n' unit)" }
  ]
}
```

---

## Error Responses

All endpoints return errors in a consistent format:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "SQL syntax error at line 1, column 15",
    "details": { "line": 1, "column": 15, "context": "SELECT symbol price FROM..." }
  }
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `VALIDATION_ERROR` | 400 | SQL syntax or semantic error |
| `NOT_FOUND` | 404 | Resource not found |
| `EXECUTION_ERROR` | 500 | Query execution failed |
| `TIMEOUT` | 504 | Query execution timed out |
| `RATE_LIMITED` | 429 | Too many requests |
| `UNAUTHORIZED` | 401 | Authentication required |
| `TEST_FAILED` | 400 | Test assertions failed |
| `GENERATION_ERROR` | 500 | Data generation failed |

---

## Rate Limits

| Endpoint | Limit |
|----------|-------|
| `/api/validate` | 100/min |
| `/api/execute` | 30/min |
| `/api/completions` | 60/min |
| `/api/nl-to-sql` | 30/min |
| `/api/test` | 30/min |
| `/api/generate-data` | 30/min |
| `/api/notebooks` | 60/min |
| `/api/jobs` | 60/min |

---

## SDK Usage Examples

### TypeScript/JavaScript

```typescript
import { VelostreamStudio } from '@velostream/studio-sdk';

const studio = new VelostreamStudio('http://localhost:8080');

// Create a notebook
const notebook = await studio.createNotebook('Trading Analytics');

// Add a cell via natural language
const cell = await studio.addCellFromNL(notebook.id,
  'Show average price per symbol with 5-minute windows'
);

// Test the cell
const testResult = await studio.testCell(notebook.id, cell.id, {
  records: 1000,
  seed: 42
});

if (testResult.passed) {
  // Deploy as pipeline
  const pipeline = await studio.deploy(notebook.id, {
    name: 'trading-analytics',
    deployDashboard: true
  });

  console.log(`Dashboard: ${pipeline.dashboard_url}`);
}
```

### Python

```python
from velostream_studio import StudioClient

studio = StudioClient("http://localhost:8080")

# Create notebook
notebook = studio.create_notebook("Trading Analytics")

# Add cell via natural language
cell = studio.add_cell_from_nl(
    notebook.id,
    "Show average price per symbol with 5-minute windows"
)

# Test with synthetic data
result = studio.test_cell(notebook.id, cell.id, records=1000, seed=42)

if result.passed:
    # Deploy
    pipeline = studio.deploy(notebook.id, name="trading-analytics")
    print(f"Dashboard: {pipeline.dashboard_url}")
else:
    print(f"Test failed: {result.ai_analysis}")
```
