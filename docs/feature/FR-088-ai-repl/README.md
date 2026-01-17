# FR-088: AI REPL for Streaming SQL Development

## Overview

An interactive AI-powered REPL (Read-Eval-Print Loop) for building, testing, and iterating on streaming SQL applications. Think "Claude Code for Velostream SQL" - conversational development with live execution.

## Command

```bash
velo-test ai [options]
```

## Vision

```
$ velo-test ai

🤖 Velostream AI Assistant
═══════════════════════════════════════════════════════════
Starting fresh session. I can help you:
  • Build SQL queries from natural language
  • Explore Kafka topics and schemas
  • Run queries and preview output
  • Save your work to a SQL file

🐳 Starting Kafka... ✅ localhost:59123 (testcontainers)
═══════════════════════════════════════════════════════════

> load my schemas from schemas/

📂 Loaded 3 schemas:
   • orders (7 fields) - schemas/orders.schema.yaml
   • customers (5 fields) - schemas/customers.schema.yaml
   • products (6 fields) - schemas/products.schema.yaml

> generate 5000 orders

▶️ Generating from orders.schema.yaml...
✅ Published 5,000 records to 'orders' topic (1.2s)

> show me high value orders joined with customer info

🤖 Here's a stream joining orders with customers:

```sql
-- @name: high_value_with_customer
-- @description: High-value orders enriched with customer data

CREATE STREAM high_value_with_customer AS
SELECT o.order_id,
       o.amount,
       c.customer_name,
       c.segment
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 500.0;
```

  [r] Run it    [e] Edit    [n] Try again

> r

📊 Output (312 records):

| order_id  | amount  | customer_name | segment    |
|-----------|---------|---------------|------------|
| ORD-a1b2  | 892.50  | Acme Corp     | Enterprise |
| ORD-c3d4  | 1250.00 | Jane Smith    | Premium    |

> save

🤖 Creating application structure:

  App name: order_analytics

✅ Saved:
   • order_analytics/sql/order_analytics.sql
   • order_analytics/test_spec.yaml
   • order_analytics/schemas/ (copied)
```

---

## Key Features

### 1. Schema Support (BYO + Generated + Inferred)

| Mode | Description | Command |
|------|-------------|---------|
| **BYO Schema** | Load existing schema files | `load schemas from <dir>` |
| **Generated** | AI creates schema from domain description | `create schema for <domain>` |
| **Inferred** | Sample Kafka topic to detect schema | `infer schema from <topic>` |

```
> load schemas from ./my_schemas/

📂 Loaded schemas:
   • orders.schema.yaml (8 fields)
   • users.schema.yaml (5 fields)

> create schema for IoT sensor data

🤖 What sensors are you tracking?
  [1] Temperature/Humidity
  [2] Motion/Occupancy
  [3] Energy/Power
  [4] Custom (describe)

> 1

📝 Generated sensor_data.schema.yaml:

```yaml
name: sensor_data
fields:
  - name: device_id
    type: string
    constraints:
      pattern: "SENSOR-{seq:6}"
  - name: temperature_c
    type: float
    constraints:
      range: { min: -40, max: 85 }
      distribution: { type: normal, mean: 22, stddev: 5 }
  - name: humidity_pct
    type: float
    constraints:
      range: { min: 0, max: 100 }
  - name: timestamp
    type: timestamp
```

Save to schemas/? [y/n]: y
```

### 2. Data Generation

```
> generate 10000 orders

▶️ Using schemas/orders.schema.yaml
✅ Published 10,000 records to 'orders' (2.1s)

> generate 1000 sensor readings with temperature between 30-40

▶️ Overriding constraints: temperature_c.range = {30, 40}
✅ Published 1,000 records to 'sensor_data' (0.4s)

> generate orders with 20% cancelled status

▶️ Overriding constraints: status.weights = [20, 20, 20, 20, 20]
   (Normalized: CANCELLED = 20%)
✅ Published 1,000 records to 'orders' (0.3s)
```

### 3. Query Building & Execution

```
> aggregate sensor readings by device, 5 minute windows

🤖 Creating time-windowed aggregation:

```sql
-- @name: sensor_5min_avg
-- @metrics: enabled

CREATE TABLE sensor_5min_avg AS
SELECT device_id,
       AVG(temperature_c) as avg_temp,
       AVG(humidity_pct) as avg_humidity,
       COUNT(*) as reading_count
FROM sensor_data
GROUP BY device_id
WINDOW TUMBLING(INTERVAL '5' MINUTE);
```

> r

📊 Output (24 devices × 12 windows = 288 rows):

| device_id   | avg_temp | avg_humidity | reading_count |
|-------------|----------|--------------|---------------|
| SENSOR-001  | 22.4     | 45.2         | 42            |

📈 Metrics:
   Throughput: 8,234 records/sec
   Latency: avg=8ms, p99=23ms
   Memory: 12 MB
```

### 4. Annotations & Configuration

```
> add profiling to the last query

🤖 Updated annotations:

```sql
-- @name: sensor_5min_avg
-- @metrics: enabled
-- @profiling: detailed
-- @profiling_sample_rate: 0.1
```

> run with profiling

📊 Profiling Report:

| Stage           | Time  | Records | % Total |
|-----------------|-------|---------|---------|
| Kafka consume   | 45ms  | 10,000  | 15%     |
| Deserialize     | 62ms  | 10,000  | 20%     |
| Window process  | 134ms | 10,000  | 44%     |
| Aggregate       | 45ms  | 288     | 15%     |
| Kafka produce   | 18ms  | 288     | 6%      |

> set timeout to 60s for all queries

✅ Default annotation updated: @timeout: 60s
```

### 5. File Operations

```
> save

🤖 Creating application:

  App name: sensor_analytics
  Description: IoT sensor monitoring and alerting

  📁 Structure:
  sensor_analytics/
  ├── sql/
  │   └── sensor_analytics.sql    (3 jobs)
  ├── schemas/
  │   └── sensor_data.schema.yaml (from session)
  ├── test_spec.yaml              (auto-generated)
  └── configs/
      └── kafka.yaml

  Generate test assertions? [y/n]: y

✅ Saved to ./sensor_analytics/

> load existing_app/sql/app.sql

📂 Loaded existing_app/sql/app.sql:
   [1] raw_events (CREATE STREAM)
   [2] hourly_stats (CREATE TABLE)
   [3] alerts (CREATE STREAM)

> show 2

```sql
-- @name: hourly_stats
-- @description: Hourly aggregation of events

CREATE TABLE hourly_stats AS
SELECT event_type,
       COUNT(*) as event_count,
       AVG(duration_ms) as avg_duration
FROM raw_events
GROUP BY event_type
WINDOW TUMBLING(INTERVAL '1' HOUR);
```

> change window to 15 minutes

🤖 Updated query:

```sql
-- @name: hourly_stats
-- @description: 15-minute aggregation of events

CREATE TABLE quarter_hourly_stats AS  -- Renamed for clarity
SELECT event_type,
       COUNT(*) as event_count,
       AVG(duration_ms) as avg_duration
FROM raw_events
GROUP BY event_type
WINDOW TUMBLING(INTERVAL '15' MINUTE);
```

Replace job #2? [y/n]: y
✅ Updated existing_app/sql/app.sql
```

---

## Commands Reference

### Session Management

| Command | Description |
|---------|-------------|
| `help` | Show all commands |
| `quit` / `exit` | Exit (prompts to save if unsaved changes) |
| `clear` | Clear conversation history |
| `status` | Show current session state |

### Schema Management

| Command | Description |
|---------|-------------|
| `load schemas from <dir>` | Load BYO schema files |
| `create schema for <domain>` | AI-guided schema creation |
| `infer schema from <topic>` | Sample topic and detect schema |
| `show schemas` | List loaded schemas |
| `show schema <name>` | Display schema details |
| `edit schema <name>` | Modify a schema |

### Data Generation

| Command | Description |
|---------|-------------|
| `generate <N> <source>` | Generate N records using schema |
| `generate <N> <source> with <constraints>` | Override constraints |
| `topics` | List Kafka topics with message counts |
| `messages <topic> [--last N]` | Peek at topic messages |

### Query Development

| Command | Description |
|---------|-------------|
| `<natural language>` | Describe what you want, AI generates SQL |
| `run` / `r` | Execute last suggested query |
| `run <N>` | Execute job N from current file |
| `edit` / `e` | Modify last suggestion before running |
| `show jobs` | List all jobs in session/file |
| `show <N>` | Display SQL for job N |
| `delete <N>` | Remove job N |

### File Operations

| Command | Description |
|---------|-------------|
| `save` | Save session (prompts for details) |
| `save <file>` | Save to specific file |
| `load <file>` | Load existing SQL file |
| `append` | Add last query to current file |
| `replace <N>` | Replace job N with last query |

### Configuration

| Command | Description |
|---------|-------------|
| `set timeout <duration>` | Set default timeout |
| `set metrics on/off` | Enable/disable metrics by default |
| `set profiling on/off` | Enable/disable profiling |
| `config` | Show current configuration |

---

## CLI Options

```bash
velo-test ai [OPTIONS]

Options:
  --kafka <bootstrap_servers>   Use external Kafka (default: testcontainers)
  --schemas <dir>               Pre-load schemas from directory
  --load <file>                 Start with existing SQL file
  --keep-containers             Keep testcontainers after exit
  --model <model>               Claude model to use (default: claude-sonnet-4-20250514)
  --no-color                    Disable colored output
  --history <file>              Load/save command history
  -v, --verbose                 Verbose logging
```

---

## Architecture

### System Prompt Structure

```
┌─────────────────────────────────────────────────────────────┐
│                    System Prompt                             │
├─────────────────────────────────────────────────────────────┤
│  STATIC CONTEXT                                              │
│  ├── Velostream SQL Grammar                                  │
│  ├── Annotation Syntax                                       │
│  ├── Schema Definition Format                                │
│  ├── Common Patterns & Examples                              │
│  └── Tool Definitions                                        │
├─────────────────────────────────────────────────────────────┤
│  DYNAMIC STATE (injected each turn)                          │
│  ├── Available Topics + Message Counts                       │
│  ├── Loaded Schemas (names + fields)                         │
│  ├── Current Jobs (from file/session)                        │
│  ├── Last Suggested Query                                    │
│  └── Last Execution Metrics                                  │
└─────────────────────────────────────────────────────────────┘
```

### Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        velo-test ai                          │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │    REPL     │  │   Session   │  │  System Prompt      │  │
│  │   (rustyline)│  │   Manager   │  │  Builder            │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│         │                │                   │               │
│         ▼                ▼                   ▼               │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                  AI Chat Handler                         ││
│  │  ┌───────────┐  ┌───────────┐  ┌───────────────────┐   ││
│  │  │ Claude    │  │ Response  │  │ Tool Executor     │   ││
│  │  │ Client    │  │ Parser    │  │                   │   ││
│  │  └───────────┘  └───────────┘  └───────────────────┘   ││
│  └─────────────────────────────────────────────────────────┘│
│         │                                    │               │
│         ▼                                    ▼               │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                Infrastructure Layer                      ││
│  │  ┌───────────┐  ┌───────────┐  ┌───────────────────┐   ││
│  │  │ Schema    │  │ Data      │  │ Query Executor    │   ││
│  │  │ Manager   │  │ Generator │  │ (test harness)    │   ││
│  │  └───────────┘  └───────────┘  └───────────────────┘   ││
│  │  ┌───────────┐  ┌───────────┐  ┌───────────────────┐   ││
│  │  │ File      │  │ Topic     │  │ Metrics           │   ││
│  │  │ Manager   │  │ Explorer  │  │ Collector         │   ││
│  │  └───────────┘  └───────────┘  └───────────────────┘   ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

### Tool Definitions

```rust
pub enum AiTool {
    // Schema operations
    LoadSchemas { directory: PathBuf },
    CreateSchema { domain: String },
    InferSchema { topic: String },

    // Data operations
    GenerateData { schema: String, count: usize, overrides: Option<String> },
    ListTopics,
    PeekMessages { topic: String, count: usize },

    // Query operations
    RunQuery { sql: String },
    ValidateQuery { sql: String },

    // File operations
    SaveFile { path: PathBuf, content: String },
    LoadFile { path: PathBuf },
    AppendToFile { path: PathBuf, content: String },

    // Session operations
    GetSessionState,
    SetConfig { key: String, value: String },
}
```

---

## Implementation Plan

### Phase 1: Foundation
- [ ] REPL infrastructure (rustyline)
- [ ] Session state management
- [ ] Basic command parsing
- [ ] System prompt structure

### Phase 2: Schema & Data
- [ ] BYO schema loading
- [ ] Schema inference from topics
- [ ] AI-guided schema creation
- [ ] Data generation with overrides

### Phase 3: Query Development
- [ ] AI query generation
- [ ] Query execution via test harness
- [ ] Metrics display
- [ ] Profiling support

### Phase 4: File Operations
- [ ] Save session to project structure
- [ ] Load existing SQL files
- [ ] Append/replace jobs
- [ ] Test spec generation

### Phase 5: Polish
- [ ] Command history (persistent)
- [ ] Tab completion
- [ ] Colored output
- [ ] Error recovery

---

## File Structure

```
src/velostream/test_harness/
├── ai_chat/
│   ├── mod.rs                 # Module exports
│   ├── repl.rs                # REPL loop (rustyline)
│   ├── session.rs             # Session state management
│   ├── system_prompt.rs       # System prompt builder
│   ├── chat_handler.rs        # Claude API interaction
│   ├── response_parser.rs     # Parse AI responses
│   ├── tool_executor.rs       # Execute tool calls
│   ├── commands.rs            # Command parsing
│   └── display.rs             # Output formatting
└── ai.rs                      # Existing AI module (reused)
```

---

## Dependencies

```toml
[dependencies]
# Existing
tokio = { version = "1", features = ["full"] }
reqwest = { version = "0.11", features = ["json"] }
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"

# New for AI REPL
rustyline = "14"              # Readline for REPL
colored = "2"                 # Terminal colors
indicatif = "0.17"            # Progress bars
```

---

## Success Criteria

1. **Schema flexibility**: BYO, generated, and inferred schemas all work seamlessly
2. **Conversational flow**: Natural language queries produce valid Velostream SQL
3. **Live execution**: Queries run against real Kafka with immediate feedback
4. **Metrics visibility**: Performance data helps users optimize queries
5. **Project generation**: Save creates complete, runnable project structure
6. **Edit existing**: Load, modify, and update existing SQL applications
7. **Graceful errors**: Invalid SQL or failed executions provide helpful feedback

---

## Related Features

- **FR-084**: SQL Application Test Harness (execution infrastructure)
- **FR-085**: Velo SQL Studio (web-based IDE - deferred)
- **FR-088**: AI REPL (this feature)

The AI REPL (FR-088) serves as a CLI-native stepping stone toward FR-085, validating the conversational SQL development paradigm before building the full web UI.
