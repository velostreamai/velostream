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
| Exploration | `/api/connect` | POST | Connect to data source (Kafka, File, S3, DB) |
| Exploration | `/api/sources` | GET | List data items (topics/files/tables) |
| Exploration | `/api/sources/{id}` | GET | Get source detail |
| Exploration | `/api/sources/{id}/schema` | GET | Infer schema from source samples |
| Exploration | `/api/sources/{id}/preview` | GET | Preview data (messages/rows) |
| Exploration | `/api/topics` | GET | List Kafka topics (Kafka-specific) |
| Exploration | `/api/topics/{name}` | GET | Get Kafka topic detail |
| Exploration | `/api/topics/{name}/schema` | GET | Infer schema from topic samples |
| Exploration | `/api/topics/{name}/messages` | GET | Peek at topic messages |
| Exploration | `/api/consumers` | GET | List consumer groups |
| AI | `/api/completions` | POST | Get AI completions |
| AI | `/api/nl-to-sql` | POST | Convert NL to SQL |
| Test | `/api/test` | POST | Run cell tests |
| Test | `/api/generate-data` | POST | Generate synthetic data |
| App Generation | `/api/generate-app` | POST | Generate multi-query app from NL |
| App Generation | `/api/apps/{id}` | GET | Get app detail |
| App Generation | `/api/apps/{id}` | PUT | Update app (add/modify queries) |
| App Generation | `/api/apps/{id}/test` | POST | Test all queries in an app |
| App Generation | `/api/apps/{id}/deploy` | POST | Deploy entire app |
| Templates | `/api/templates` | GET | List available templates |
| Templates | `/api/templates/{id}` | GET | Get template detail |
| Templates | `/api/templates/{id}/customize` | POST | Customize template for user data |
| Templates | `/api/templates/{id}/preview` | POST | Preview template with user schema |
| AI Analysis | `/api/analyze` | POST | Get proactive AI suggestions |
| AI Analysis | `/api/analyze/schema-diff` | POST | Detect schema changes |
| AI Analysis | `/api/analyze/performance` | POST | Performance optimization suggestions |
| AI Analysis | `/api/analyze/annotations` | POST | Suggest @metric/@alert annotations |
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

## Data Source Exploration Endpoints

These endpoints enable the Studio to connect to data sources, discover available
data (topics, files, tables), infer schemas, and preview data. For Kafka, they delegate
to `TestHarnessInfra` exploration primitives in `src/velostream/test_harness/infra.rs`.
For file, S3, and database sources, they use the corresponding `DataSource` implementations.

**Supported source types:** Kafka, File, FileMmap, S3, ClickHouse, Database (CDC)

### `POST /api/connect`

Connects to a data source for the current session. Accepts URI-based connection strings
or explicit configuration. Subsequent exploration requests use this connection.

**Request (Kafka):**
```json
{
  "uri": "kafka://broker1:9092",
  "type": "kafka",
  "options": {
    "schema_registry_url": "http://schema-registry:8081",
    "security_protocol": "PLAINTEXT"
  }
}
```

**Request (File):**
```json
{
  "uri": "file:///data/trades/*.csv",
  "type": "file",
  "options": {
    "format": "csv",
    "header": "true",
    "delimiter": ","
  }
}
```

**Request (FileMmap — high-throughput batch):**
```json
{
  "uri": "file:///data/large_events.json",
  "type": "file_mmap",
  "options": {
    "format": "json"
  }
}
```

**Request (S3):**
```json
{
  "uri": "s3://analytics-data/events/",
  "type": "s3",
  "options": {
    "format": "parquet",
    "region": "us-west-2",
    "compression": "snappy"
  }
}
```

**Request (Database):**
```json
{
  "uri": "postgres://host:5432/mydb",
  "type": "database",
  "options": {
    "table": "orders",
    "mode": "cdc"
  }
}
```

**Response:**
```json
{
  "connected": true,
  "source_id": "src_abc123",
  "uri": "kafka://broker1:9092",
  "type": "kafka",
  "details": {
    "cluster_id": "abc123",
    "broker_count": 2,
    "schema_registry_url": "http://schema-registry:8081"
  }
}
```

**Response (File):**
```json
{
  "connected": true,
  "source_id": "src_def456",
  "uri": "file:///data/trades/*.csv",
  "type": "file",
  "details": {
    "files_matched": 24,
    "total_size_bytes": 1048576000,
    "format": "csv"
  }
}
```

**Error (connection failed):**
```json
{
  "error": {
    "code": "CONNECTION_ERROR",
    "message": "Failed to connect to broker1:9092: Connection refused",
    "details": { "uri": "kafka://broker1:9092" }
  }
}
```

---

### `GET /api/topics`

Lists all topics on the connected Kafka cluster. Maps to `TestHarnessInfra::fetch_topic_info(None)`.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `filter` | string | — | Optional topic name filter (substring match) |

**Response:**
```json
{
  "topics": [
    {
      "name": "trades",
      "partitions": [
        {
          "partition": 0,
          "low_offset": 0,
          "high_offset": 450000,
          "message_count": 450000,
          "latest_timestamp_ms": 1708099200000,
          "latest_key": "AAPL"
        },
        {
          "partition": 1,
          "low_offset": 0,
          "high_offset": 380000,
          "message_count": 380000,
          "latest_timestamp_ms": 1708099199500,
          "latest_key": "TSLA"
        }
      ],
      "total_messages": 1200000,
      "is_test_topic": false
    },
    {
      "name": "orders",
      "partitions": [...],
      "total_messages": 450000,
      "is_test_topic": false
    }
  ],
  "total": 12
}
```

---

### `GET /api/topics/{name}`

Gets detailed information for a single topic. Maps to `TestHarnessInfra::fetch_topic_info(Some(name))`.

**Response:**
```json
{
  "name": "trades",
  "partitions": [
    {
      "partition": 0,
      "low_offset": 0,
      "high_offset": 450000,
      "message_count": 450000,
      "latest_timestamp_ms": 1708099200000,
      "latest_key": "AAPL"
    }
  ],
  "total_messages": 1200000,
  "is_test_topic": false
}
```

---

### `GET /api/topics/{name}/schema`

Infers the schema of a topic by sampling recent messages. Maps to
`TestHarnessInfra::fetch_topic_schema(name, max_records)`.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_records` | integer | 10 | Number of messages to sample for inference |

**Response:**
```json
{
  "topic": "trades",
  "fields": [
    ["symbol", "String"],
    ["price", "Float"],
    ["quantity", "Integer"],
    ["timestamp", "DateTime"]
  ],
  "sample_value": "{\"symbol\":\"AAPL\",\"price\":152.34,\"quantity\":5000,\"timestamp\":\"2026-02-16T10:30:00Z\"}",
  "has_keys": true,
  "records_sampled": 10
}
```

---

### `GET /api/topics/{name}/messages`

Peeks at messages from a topic without consuming them. Maps to
`TestHarnessInfra::peek_topic_messages(topic, limit, from_end, start_offset, partition_filter)`.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 10 | Number of messages to return |
| `from_end` | boolean | true | Read from latest (true) or earliest (false) |
| `start_offset` | integer | — | Start from specific offset (overrides from_end) |
| `partition` | integer | — | Filter to a specific partition |

**Response:**
```json
{
  "topic": "trades",
  "messages": [
    {
      "partition": 0,
      "offset": 449995,
      "key": "AAPL",
      "value": "{\"symbol\":\"AAPL\",\"price\":185.42,\"quantity\":5000,\"timestamp\":\"2026-02-16T14:30:01Z\"}",
      "timestamp_ms": 1708099201000,
      "headers": [
        ["traceparent", "00-abc123-def456-01"]
      ]
    },
    {
      "partition": 1,
      "offset": 379998,
      "key": "TSLA",
      "value": "{\"symbol\":\"TSLA\",\"price\":242.10,\"quantity\":3200,\"timestamp\":\"2026-02-16T14:30:02Z\"}",
      "timestamp_ms": 1708099202000,
      "headers": []
    }
  ],
  "total_returned": 2
}
```

---

### `GET /api/consumers`

Lists consumer groups on the connected cluster. Maps to `TestHarnessInfra::get_consumer_info()`.

**Response:**
```json
{
  "consumers": [
    {
      "group_id": "velo-sql-trade-alerts",
      "subscribed_topics": ["trades"],
      "positions": [
        {
          "topic": "trades",
          "partition": 0,
          "current_offset": 449000,
          "high_watermark": 450000,
          "lag": 1000
        }
      ],
      "state": "Stable"
    }
  ],
  "total": 1
}
```

---

## App Generation Endpoints

These endpoints support Journey 7 (Build Me an App) — generating, testing, and deploying
multi-query streaming applications from natural language descriptions.

### `POST /api/generate-app`

Generates a complete multi-query streaming application from a natural language description.
The AI analyzes the request, connects to available data sources (if connected), and produces
a coordinated set of SQL queries with metrics, alerts, dashboard layout, and pipeline topology.

**Request:**
```json
{
  "prompt": "Build me a real-time trading analytics app that tracks VWAP, detects volume spikes, and shows top movers",
  "connection": {
    "brokers": "broker1:9092"
  },
  "options": {
    "include_dashboard": true,
    "include_alerts": true,
    "include_topology": true,
    "max_queries": 10
  }
}
```

**Response:**
```json
{
  "app": {
    "id": "app_a1b2c3d4",
    "name": "Trading Analytics",
    "description": "Real-time trading analytics with VWAP, volume spike detection, and top movers",
    "queries": [
      {
        "id": "q1",
        "name": "VWAP Calculator",
        "sql": "SELECT symbol, SUM(price * quantity) / SUM(quantity) as vwap, SUM(quantity) as volume FROM trades GROUP BY symbol WINDOW TUMBLING(INTERVAL '5' MINUTE) EMIT CHANGES",
        "source_topic": "trades",
        "sink_topic": "trade-vwap",
        "metrics": [
          { "name": "vwap_per_symbol", "type": "gauge", "labels": ["symbol"] },
          { "name": "trade_volume", "type": "counter", "labels": ["symbol"] }
        ],
        "alerts": []
      },
      {
        "id": "q2",
        "name": "Volume Spike Detector",
        "sql": "SELECT symbol, SUM(quantity) as volume FROM trades GROUP BY symbol WINDOW TUMBLING(INTERVAL '1' MINUTE) HAVING SUM(quantity) > 1000000 EMIT CHANGES",
        "source_topic": "trades",
        "sink_topic": "trade-alerts",
        "metrics": [
          { "name": "volume_spike_count", "type": "counter" }
        ],
        "alerts": [
          { "name": "volume_spike", "condition": "volume > 1000000", "severity": "warning" }
        ]
      },
      {
        "id": "q3",
        "name": "Price Movement Tracker",
        "sql": "SELECT symbol, AVG(price) as avg_price, STDDEV(price) / AVG(price) as volatility FROM trades GROUP BY symbol WINDOW TUMBLING(INTERVAL '5' MINUTE) EMIT CHANGES",
        "source_topic": "trades",
        "sink_topic": "price-movements",
        "metrics": [
          { "name": "avg_price", "type": "gauge", "labels": ["symbol"] },
          { "name": "volatility", "type": "gauge", "labels": ["symbol"] }
        ],
        "alerts": [
          { "name": "high_volatility", "condition": "volatility > 0.05", "severity": "warning" }
        ]
      },
      {
        "id": "q4",
        "name": "Top Movers",
        "sql": "SELECT symbol, volatility FROM price-movements WHERE volatility > 0.02 ORDER BY volatility DESC LIMIT 10 EMIT CHANGES",
        "source_topic": "price-movements",
        "sink_topic": "top-movers",
        "metrics": [
          { "name": "top_movers_count", "type": "gauge" }
        ],
        "alerts": []
      }
    ],
    "dashboard": {
      "title": "Trading Analytics",
      "panels": [
        { "title": "VWAP per Symbol", "type": "timeseries", "query_ref": "q1", "metric": "vwap_per_symbol", "position": { "row": 0, "col": 0 } },
        { "title": "Volume per Symbol", "type": "barchart", "query_ref": "q1", "metric": "trade_volume", "position": { "row": 0, "col": 1 } },
        { "title": "Volatility", "type": "timeseries", "query_ref": "q3", "metric": "volatility", "position": { "row": 1, "col": 0 } },
        { "title": "Top Movers", "type": "table", "query_ref": "q4", "position": { "row": 1, "col": 1 } },
        { "title": "Active Alerts", "type": "alertlist", "position": { "row": 2, "col": 0 } },
        { "title": "Throughput", "type": "stat", "metric": "records_per_second", "position": { "row": 2, "col": 1 } }
      ]
    },
    "topology": {
      "nodes": [
        { "id": "trades", "type": "source", "label": "trades" },
        { "id": "q1", "type": "transform", "label": "VWAP Calculator" },
        { "id": "q2", "type": "transform", "label": "Volume Spike Detector" },
        { "id": "q3", "type": "transform", "label": "Price Movement Tracker" },
        { "id": "q4", "type": "transform", "label": "Top Movers" },
        { "id": "trade-vwap", "type": "sink", "label": "trade-vwap" },
        { "id": "trade-alerts", "type": "sink", "label": "trade-alerts" },
        { "id": "price-movements", "type": "sink", "label": "price-movements" },
        { "id": "top-movers", "type": "sink", "label": "top-movers" }
      ],
      "edges": [
        { "from": "trades", "to": "q1" },
        { "from": "trades", "to": "q2" },
        { "from": "trades", "to": "q3" },
        { "from": "q1", "to": "trade-vwap" },
        { "from": "q2", "to": "trade-alerts" },
        { "from": "q3", "to": "price-movements" },
        { "from": "price-movements", "to": "q4" },
        { "from": "q4", "to": "top-movers" }
      ]
    },
    "created_at": "2026-02-16T10:30:00Z"
  }
}
```

### `GET /api/apps/{id}`

Retrieves the current state of a generated application.

**Response:** Same structure as `POST /api/generate-app` response.

### `PUT /api/apps/{id}`

Updates an existing app — add, modify, or remove queries. Used when the user iterates
on the generated app (e.g., "add an order flow query").

**Request:**
```json
{
  "action": "add_query",
  "prompt": "Add a query that joins trades with the orders topic to enrich trade data",
  "context": {
    "existing_queries": ["q1", "q2", "q3", "q4"],
    "available_topics": ["trades", "orders", "trade-vwap", "trade-alerts", "price-movements", "top-movers"]
  }
}
```

**Response:**
```json
{
  "app": { "...updated app with new query q5..." },
  "changes": {
    "added": ["q5"],
    "modified": [],
    "removed": [],
    "dashboard_panels_added": 1,
    "topology_edges_added": 3
  }
}
```

### `POST /api/apps/{id}/test`

Tests all queries in an application with coordinated synthetic data. Generates data
for all source topics with matching foreign keys to ensure JOINs produce results.

**Request:**
```json
{
  "records_per_source": 1000,
  "seed": 42,
  "timeout_ms": 60000,
  "assertions": {
    "per_query": {
      "record_count": { "greater_than": 0 },
      "no_nulls": true
    }
  }
}
```

**Response:**
```json
{
  "passed": true,
  "total_queries": 5,
  "passed_queries": 5,
  "results": [
    {
      "query_id": "q1",
      "name": "VWAP Calculator",
      "passed": true,
      "output_records": 3,
      "execution_time_ms": 45,
      "assertions": [
        { "type": "record_count", "passed": true, "actual": 3, "expected": "> 0" },
        { "type": "no_nulls", "passed": true }
      ]
    },
    {
      "query_id": "q2",
      "name": "Volume Spike Detector",
      "passed": true,
      "output_records": 1,
      "execution_time_ms": 52,
      "assertions": [
        { "type": "record_count", "passed": true, "actual": 1, "expected": "> 0" }
      ]
    }
  ],
  "data_generation": {
    "topics_populated": ["trades", "orders"],
    "records_generated": { "trades": 1000, "orders": 500 },
    "foreign_key_matches": { "trades.customer_id": "orders.customer_id" }
  }
}
```

### `POST /api/apps/{id}/deploy`

Deploys all queries in an application as coordinated Velostream jobs, registers
metrics with Prometheus, configures alert rules, and generates the Grafana dashboard.

**Request:**
```json
{
  "name": "trading-analytics",
  "deploy_dashboard": true,
  "deploy_alerts": true,
  "grafana_url": "http://grafana:3000",
  "prometheus_url": "http://prometheus:9090"
}
```

**Response:**
```json
{
  "deployment": {
    "id": "deploy_x1y2z3",
    "name": "trading-analytics",
    "jobs": [
      { "query_id": "q1", "job_id": "job_001", "status": "running" },
      { "query_id": "q2", "job_id": "job_002", "status": "running" },
      { "query_id": "q3", "job_id": "job_003", "status": "running" },
      { "query_id": "q4", "job_id": "job_004", "status": "running" },
      { "query_id": "q5", "job_id": "job_005", "status": "running" }
    ],
    "metrics_registered": 6,
    "alerts_configured": 2,
    "dashboard_url": "http://grafana:3000/d/trading-analytics",
    "status": "active",
    "deployed_at": "2026-02-16T10:35:00Z"
  }
}
```

---

## Template Endpoints

These endpoints support Journey 8 (Start from a Template) — browsing pre-built
application templates, previewing them, and customizing them for the user's data.

### `GET /api/templates`

Lists all available application templates with category filtering.

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `category` | string | Filter by category: `financial`, `iot`, `ecommerce`, `observability`, `security`, `general` |
| `search` | string | Search templates by name or description |

**Response:**
```json
{
  "templates": [
    {
      "id": "trading-analytics",
      "name": "Trading Analytics",
      "category": "financial",
      "description": "Real-time VWAP, volume tracking, and price movement analysis",
      "query_count": 4,
      "metrics_count": 6,
      "alerts_count": 2,
      "required_topics": ["trades"],
      "tags": ["vwap", "volume", "alerts", "financial"],
      "popularity": 1250,
      "preview_image": "/api/templates/trading-analytics/preview.png"
    },
    {
      "id": "iot-sensor-monitoring",
      "name": "IoT Sensor Monitoring",
      "category": "iot",
      "description": "Sensor health, anomaly detection, and fleet-wide aggregation",
      "query_count": 5,
      "metrics_count": 8,
      "alerts_count": 4,
      "required_topics": ["sensor-readings"],
      "tags": ["iot", "sensors", "anomaly", "monitoring"],
      "popularity": 890
    },
    {
      "id": "ecommerce-funnel",
      "name": "E-Commerce Funnel",
      "category": "ecommerce",
      "description": "Cart tracking, conversion analytics, and revenue metrics",
      "query_count": 6,
      "metrics_count": 10,
      "alerts_count": 3,
      "required_topics": ["page-views", "cart-events", "orders"],
      "tags": ["ecommerce", "funnel", "conversion", "revenue"]
    },
    {
      "id": "fraud-detection",
      "name": "Fraud Detection",
      "category": "security",
      "description": "Transaction scoring, velocity checks, and pattern matching",
      "query_count": 4,
      "metrics_count": 5,
      "alerts_count": 3,
      "required_topics": ["transactions"],
      "tags": ["fraud", "security", "scoring", "alerts"]
    }
  ],
  "total": 8,
  "categories": [
    { "name": "financial", "count": 2 },
    { "name": "iot", "count": 2 },
    { "name": "ecommerce", "count": 1 },
    { "name": "security", "count": 1 },
    { "name": "observability", "count": 1 },
    { "name": "general", "count": 1 }
  ]
}
```

### `GET /api/templates/{id}`

Gets full template detail including all query SQL, metrics, alerts, and dashboard layout.

**Response:**
```json
{
  "template": {
    "id": "trading-analytics",
    "name": "Trading Analytics",
    "category": "financial",
    "description": "Real-time VWAP, volume tracking, and price movement analysis",
    "version": "1.0.0",
    "schema_requirements": {
      "trades": {
        "required_fields": [
          { "name": "symbol", "type": "String", "description": "Trading symbol identifier" },
          { "name": "price", "type": "Float", "description": "Trade execution price" },
          { "name": "quantity", "type": "Integer", "description": "Number of shares/units" }
        ],
        "optional_fields": [
          { "name": "timestamp", "type": "DateTime", "description": "Trade timestamp (uses processing time if absent)" },
          { "name": "exchange", "type": "String", "description": "Exchange identifier for multi-exchange analysis" }
        ]
      }
    },
    "queries": [
      {
        "id": "q1",
        "name": "VWAP Calculator",
        "description": "Volume-weighted average price per symbol in 5-minute windows",
        "sql": "-- @metric: vwap_per_symbol\n-- @metric_type: gauge\n-- @metric_labels: symbol\nSELECT symbol, SUM(price * quantity) / SUM(quantity) as vwap, SUM(quantity) as volume\nFROM {{source_topic}}\nGROUP BY symbol\nWINDOW TUMBLING(INTERVAL '5' MINUTE)\nEMIT CHANGES",
        "source_topic": "{{source_topic}}",
        "sink_topic": "{{prefix}}-vwap",
        "configurable_params": {
          "window_size": { "default": "5 MINUTE", "description": "Aggregation window size" }
        }
      }
    ],
    "dashboard": {
      "title": "{{name}} Dashboard",
      "panels": []
    },
    "topology": {
      "nodes": [],
      "edges": []
    }
  }
}
```

### `POST /api/templates/{id}/customize`

Customizes a template for the user's actual data. Maps template schema requirements
to real topic fields, calibrates thresholds, and resolves template variables.

**Request:**
```json
{
  "schema_mapping": {
    "trades": {
      "topic": "my-trades-topic",
      "field_mapping": {
        "symbol": "ticker_symbol",
        "price": "execution_price",
        "quantity": "shares"
      }
    }
  },
  "params": {
    "prefix": "my-trading",
    "name": "My Trading Analytics",
    "window_size": "1 MINUTE"
  },
  "auto_discover": true
}
```

When `auto_discover` is true and a Kafka connection is active, the endpoint inspects
the mapped topics to verify the schema and suggest additional field mappings.

**Response:**
```json
{
  "app": {
    "id": "app_customized_1",
    "name": "My Trading Analytics",
    "template_id": "trading-analytics",
    "queries": [
      {
        "id": "q1",
        "name": "VWAP Calculator",
        "sql": "-- @metric: vwap_per_symbol\n-- @metric_type: gauge\n-- @metric_labels: ticker_symbol\nSELECT ticker_symbol, SUM(execution_price * shares) / SUM(shares) as vwap, SUM(shares) as volume\nFROM my-trades-topic\nGROUP BY ticker_symbol\nWINDOW TUMBLING(INTERVAL '1' MINUTE)\nEMIT CHANGES",
        "source_topic": "my-trades-topic",
        "sink_topic": "my-trading-vwap"
      }
    ],
    "schema_validation": {
      "my-trades-topic": {
        "valid": true,
        "mapped_fields": {
          "ticker_symbol": "String",
          "execution_price": "Float",
          "shares": "Integer"
        },
        "unmapped_fields": ["exchange_id", "trade_id", "timestamp"],
        "suggestions": [
          "Field 'exchange_id' could be added as a GROUP BY dimension for multi-exchange analysis"
        ]
      }
    }
  }
}
```

### `POST /api/templates/{id}/preview`

Previews what a template would look like with the user's schema without creating an app.
Useful for browsing templates before committing.

**Request:**
```json
{
  "topic": "my-trades-topic",
  "auto_map": true
}
```

**Response:**
```json
{
  "preview": {
    "template_id": "trading-analytics",
    "auto_mapping": {
      "confidence": 0.92,
      "mapped_fields": {
        "symbol": { "mapped_to": "ticker_symbol", "confidence": 0.95 },
        "price": { "mapped_to": "execution_price", "confidence": 0.90 },
        "quantity": { "mapped_to": "shares", "confidence": 0.88 }
      }
    },
    "sample_query_preview": "SELECT ticker_symbol, SUM(execution_price * shares) / SUM(shares) as vwap ...",
    "compatible": true,
    "warnings": []
  }
}
```

---

## AI Analysis Endpoints

These endpoints support Journey 9 (AI Proactive Intelligence) — the AI acts as a
proactive collaborator, analyzing thread context to suggest optimizations, detect
issues, and recommend improvements without being asked.

### `POST /api/analyze`

Analyzes the current thread context (connection, schemas, queries, metrics) and returns
proactive suggestions. Called automatically by the frontend after key events (connect,
generate, test, deploy) or triggered by the user asking "What should I do next?"

**Request:**
```json
{
  "thread_context": {
    "connection": { "brokers": "broker1:9092" },
    "schemas": {
      "trades": {
        "fields": [
          { "name": "symbol", "type": "String" },
          { "name": "price", "type": "Float" },
          { "name": "quantity", "type": "Integer" }
        ]
      }
    },
    "queries": [
      {
        "sql": "SELECT symbol, AVG(price) FROM trades GROUP BY symbol EMIT CHANGES",
        "has_metrics": false,
        "has_alerts": false
      }
    ],
    "deployed_jobs": [],
    "trigger": "post_generate"
  }
}
```

**Response:**
```json
{
  "suggestions": [
    {
      "type": "add_metrics",
      "priority": "high",
      "title": "Add observability annotations",
      "description": "Your query has no @metric annotations. Adding metrics enables auto-generated Grafana dashboards and production monitoring.",
      "suggested_annotations": [
        "-- @metric: avg_price_per_symbol",
        "-- @metric_type: gauge",
        "-- @metric_labels: symbol"
      ],
      "action": {
        "tool": "analyze_annotations",
        "label": "Add Metrics"
      }
    },
    {
      "type": "add_window",
      "priority": "medium",
      "title": "Consider windowed aggregation",
      "description": "Your GROUP BY has no WINDOW clause. Without windowing, results accumulate unbounded state. A tumbling window bounds memory and produces periodic output.",
      "suggested_sql": "... WINDOW TUMBLING(INTERVAL '5' MINUTE)",
      "action": {
        "tool": "generate_sql",
        "label": "Add Window"
      }
    },
    {
      "type": "use_scaled_integer",
      "priority": "medium",
      "title": "Use ScaledInteger for price fields",
      "description": "The 'price' field is typed as Float. For financial calculations, ScaledInteger provides exact arithmetic (42x faster than f64) and avoids floating-point rounding errors.",
      "action": {
        "tool": "generate_sql",
        "label": "Switch to ScaledInteger"
      }
    }
  ],
  "context_summary": {
    "connection": "broker1:9092",
    "topics_discovered": 12,
    "queries_generated": 1,
    "metrics_defined": 0,
    "alerts_defined": 0,
    "coverage_score": 0.3
  }
}
```

### `POST /api/analyze/schema-diff`

Compares current topic schemas against schemas used by deployed queries. Detects
field additions, removals, type changes, and suggests query updates.

**Request:**
```json
{
  "deployed_job_ids": ["job_001", "job_002"],
  "rescan_topics": true
}
```

**Response:**
```json
{
  "diffs": [
    {
      "topic": "trades",
      "changes": [
        {
          "type": "field_added",
          "field": "exchange_id",
          "field_type": "String",
          "impact": "No impact on existing queries. Could be used as additional GROUP BY dimension.",
          "suggestion": "Consider adding 'exchange_id' to GROUP BY for per-exchange analysis"
        },
        {
          "type": "type_changed",
          "field": "price",
          "old_type": "Float",
          "new_type": "String",
          "impact": "BREAKING: Query q1 uses AVG(price) which requires numeric type. Query will fail.",
          "severity": "critical",
          "suggestion": "Add CAST(price AS DOUBLE) or update upstream producer"
        }
      ],
      "last_checked": "2026-02-16T10:00:00Z",
      "current_check": "2026-02-16T10:35:00Z"
    }
  ],
  "healthy_topics": ["orders", "customer-events"],
  "summary": {
    "topics_checked": 3,
    "breaking_changes": 1,
    "new_fields": 1,
    "recommendations": 2
  }
}
```

### `POST /api/analyze/performance`

Analyzes running job metrics and suggests performance optimizations.

**Request:**
```json
{
  "job_ids": ["job_001", "job_002", "job_003"],
  "analysis_window_minutes": 30
}
```

**Response:**
```json
{
  "analysis": {
    "jobs": [
      {
        "job_id": "job_001",
        "name": "VWAP Calculator",
        "status": "healthy",
        "metrics": {
          "records_per_second": 15000,
          "avg_latency_ms": 2.3,
          "memory_mb": 128
        },
        "suggestions": []
      },
      {
        "job_id": "job_003",
        "name": "Price Movement Tracker",
        "status": "warning",
        "metrics": {
          "records_per_second": 15000,
          "avg_latency_ms": 45.2,
          "memory_mb": 512
        },
        "suggestions": [
          {
            "type": "high_latency",
            "title": "Latency above threshold",
            "description": "Average latency is 45ms, which may indicate state accumulation. The STDDEV function maintains per-group state that grows with cardinality.",
            "recommendation": "Consider adding a HAVING clause to prune low-volume symbols, or reduce window size"
          },
          {
            "type": "high_memory",
            "title": "Elevated memory usage",
            "description": "512MB memory suggests high group cardinality. Check if all symbols are needed or if filtering would help.",
            "recommendation": "Add WHERE clause to filter to actively-traded symbols"
          }
        ]
      }
    ],
    "overall_health": "warning",
    "total_throughput": 45000,
    "total_memory_mb": 768
  }
}
```

### `POST /api/analyze/annotations`

Analyzes SQL queries and suggests appropriate @metric and @alert annotations.

**Request:**
```json
{
  "queries": [
    {
      "sql": "SELECT symbol, AVG(price) as avg_price, SUM(quantity) as total_volume FROM trades GROUP BY symbol WINDOW TUMBLING(INTERVAL '5' MINUTE) EMIT CHANGES"
    }
  ]
}
```

**Response:**
```json
{
  "annotated_queries": [
    {
      "original_sql": "SELECT symbol, AVG(price) as avg_price, SUM(quantity) as total_volume FROM trades GROUP BY symbol WINDOW TUMBLING(INTERVAL '5' MINUTE) EMIT CHANGES",
      "annotated_sql": "-- @metric: avg_price\n-- @metric_type: gauge\n-- @metric_labels: symbol\n-- @metric: total_volume\n-- @metric_type: counter\n-- @metric_labels: symbol\n-- @alert: total_volume > 1000000\n-- @alert_severity: warning\nSELECT symbol, AVG(price) as avg_price, SUM(quantity) as total_volume FROM trades GROUP BY symbol WINDOW TUMBLING(INTERVAL '5' MINUTE) EMIT CHANGES",
      "added_metrics": [
        {
          "name": "avg_price",
          "type": "gauge",
          "labels": ["symbol"],
          "rationale": "AVG aggregation is a natural gauge — it represents a point-in-time value per group"
        },
        {
          "name": "total_volume",
          "type": "counter",
          "labels": ["symbol"],
          "rationale": "SUM(quantity) is a monotonically increasing count — natural counter metric"
        }
      ],
      "added_alerts": [
        {
          "name": "high_volume",
          "condition": "total_volume > 1000000",
          "severity": "warning",
          "rationale": "Volume spikes above 1M may indicate unusual trading activity"
        }
      ]
    }
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
| `CONNECTION_ERROR` | 502 | Kafka connection failed |
| `NOT_CONNECTED` | 400 | No active Kafka connection (call POST /api/connect first) |
| `TIMEOUT` | 504 | Query execution timed out |
| `RATE_LIMITED` | 429 | Too many requests |
| `UNAUTHORIZED` | 401 | Authentication required |
| `TEST_FAILED` | 400 | Test assertions failed |
| `GENERATION_ERROR` | 500 | Data generation failed |
| `APP_NOT_FOUND` | 404 | App ID does not exist |
| `TEMPLATE_NOT_FOUND` | 404 | Template ID does not exist |
| `SCHEMA_MISMATCH` | 400 | Template schema mapping does not match topic schema |
| `ANALYSIS_ERROR` | 500 | AI analysis failed |
| `DEPLOY_FAILED` | 500 | App deployment failed (partial deploy possible — check job statuses) |

---

## Rate Limits

| Endpoint | Limit |
|----------|-------|
| `/api/validate` | 100/min |
| `/api/execute` | 30/min |
| `/api/connect` | 10/min |
| `/api/topics` | 30/min |
| `/api/topics/{name}/schema` | 30/min |
| `/api/topics/{name}/messages` | 60/min |
| `/api/consumers` | 30/min |
| `/api/completions` | 60/min |
| `/api/nl-to-sql` | 30/min |
| `/api/test` | 30/min |
| `/api/generate-data` | 30/min |
| `/api/generate-app` | 10/min |
| `/api/apps/{id}` | 60/min |
| `/api/apps/{id}/test` | 10/min |
| `/api/apps/{id}/deploy` | 5/min |
| `/api/templates` | 60/min |
| `/api/templates/{id}` | 60/min |
| `/api/templates/{id}/customize` | 10/min |
| `/api/templates/{id}/preview` | 30/min |
| `/api/analyze` | 30/min |
| `/api/analyze/schema-diff` | 10/min |
| `/api/analyze/performance` | 10/min |
| `/api/analyze/annotations` | 30/min |
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
