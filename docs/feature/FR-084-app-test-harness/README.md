# FR-084: SQL Application Test Harness

> **📖 Documentation**
>
> - **User Documentation**: [docs/test-harness/](../../test-harness/README.md) - Getting started, user guide, quick reference
> - **Developer Documentation**: [docs/developer/test-harness/](../../developer/test-harness/README.md) - Architecture, extending, internals

## Overview

A comprehensive test framework for validating Velostream SQL applications. The harness enables developers to test their SQL pipelines with generated or captured data, validate outputs against assertions, and produce detailed reports.

## Goals

1. **Streamline Development**: Enable rapid iteration on SQL applications with automated testing
2. **Catch Errors Early**: Validate SQL syntax, schema compatibility, and business logic before deployment
3. **Pipeline Testing**: Execute queries in sequence, capturing sink outputs for downstream queries
4. **Flexible Data Generation**: Schema-driven test data generation with relationship support
5. **CI/CD Integration**: JUnit XML output, exit codes, and JSON reports for automation

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      SQL Test Harness                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
│  │  SQL Parser  │───▶│ Data Generator│───▶│  Executor    │          │
│  │  & Analyzer  │    │              │    │              │          │
│  └──────────────┘    └──────────────┘    └──────────────┘          │
│         │                   │                   │                   │
│         ▼                   ▼                   ▼                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
│  │ Schema       │    │ Test Data    │    │ Results      │          │
│  │ Inference    │    │ Files/Streams│    │ Collector    │          │
│  └──────────────┘    └──────────────┘    └──────────────┘          │
│                                                 │                   │
│                                                 ▼                   │
│                                          ┌──────────────┐          │
│                                          │ Report       │          │
│                                          │ Generator    │          │
│                                          └──────────────┘          │
└─────────────────────────────────────────────────────────────────────┘
```

## Implementation Status

> **Note**: This section shows the actual implementation status as of the completion of FR-084.

| Module | File | Status | Description |
|--------|------|--------|-------------|
| **Module Exports** | [`mod.rs`](../../src/velostream/test_harness/mod.rs) | ✅ Complete | Public API exports |
| **Infrastructure** | [`infra.rs`](../../src/velostream/test_harness/infra.rs) | ✅ Complete | Testcontainers Kafka, topic management, message peeking |
| **Schema** | [`schema.rs`](../../src/velostream/test_harness/schema.rs) | ✅ Complete | YAML schema parsing, validation, SchemaRegistry |
| **Data Generator** | [`generator.rs`](../../src/velostream/test_harness/generator.rs) | ✅ Complete | Enum, range, timestamp, distribution, derived fields |
| **Query Executor** | [`executor.rs`](../../src/velostream/test_harness/executor.rs) | ✅ Complete | Publishes data, deploys jobs via StreamJobServer, captures output |
| **Statement Executor** | [`statement_executor.rs`](../../src/velostream/test_harness/statement_executor.rs) | ✅ Complete | Interactive debugging, breakpoints, step execution, data visibility |
| **Sink Capture** | [`capture.rs`](../../src/velostream/test_harness/capture.rs) | ✅ Complete | Kafka consumer, file reader, timeout handling |
| **Assertions** | [`assertions.rs`](../../src/velostream/test_harness/assertions.rs) | ✅ Complete | 10+ assertion types, template assertions |
| **Reports** | [`report.rs`](../../src/velostream/test_harness/report.rs) | ✅ Complete | Text, JSON, JUnit XML output formats |
| **CLI** | [`cli.rs`](../../src/velostream/test_harness/cli.rs) | ✅ Complete | clap-based argument parsing |
| **AI Features** | [`ai.rs`](../../src/velostream/test_harness/ai.rs) | ✅ Complete | Claude API integration, schema inference, failure analysis |
| **Test Spec** | [`spec.rs`](../../src/velostream/test_harness/spec.rs) | ✅ Complete | YAML test spec parsing, validation |
| **Errors** | [`error.rs`](../../src/velostream/test_harness/error.rs) | ✅ Complete | Structured error types with thiserror |
| **Config Override** | [`config_override.rs`](../../src/velostream/test_harness/config_override.rs) | ✅ Complete | Bootstrap servers, topic prefix, temp paths |
| **Schema Inference** | [`inference.rs`](../../src/velostream/test_harness/inference.rs) | ✅ Complete | SQL analysis, CSV sampling |
| **Spec Generator** | [`spec_generator.rs`](../../src/velostream/test_harness/spec_generator.rs) | ✅ Complete | Auto-generate test specs from SQL |
| **Stress Test** | [`stress.rs`](../../src/velostream/test_harness/stress.rs) | ✅ Complete | Throughput metrics, memory tracking (macOS/Linux) |
| **DLQ Capture** | [`dlq.rs`](../../src/velostream/test_harness/dlq.rs) | ✅ Complete | Dead Letter Queue capture, error classification |
| **Fault Injection** | [`fault_injection.rs`](../../src/velostream/test_harness/fault_injection.rs) | ✅ Complete | Chaos testing: malformed records, duplicates, out-of-order |
| **Table State** | [`table_state.rs`](../../src/velostream/test_harness/table_state.rs) | ✅ Complete | CTAS table tracking, snapshots, changelog |
| **File I/O** | [`file_io.rs`](../../src/velostream/test_harness/file_io.rs) | ✅ Complete | CSV/JSON file loading and writing for test data |

### Known Limitations

All major features are now implemented. The test harness is fully functional.

### Recently Completed (Phases 7-10)

#### Phase 10: In-Memory Schema Registry ✅

The test harness now includes an in-memory schema registry that is **API-compatible with Confluent Schema Registry**. This means:
- No Docker container required for schema registry
- Same API as production Confluent Schema Registry
- Supports Avro and Protobuf schema registration
- Supports schema references for nested types

```rust
// Register a schema
let schema_id = infra.register_schema(
    "market_data-value",
    r#"{"type":"record","name":"MarketData","fields":[{"name":"symbol","type":"string"}]}"#
).await?;

// Retrieve a schema
let schema = infra.get_schema(schema_id).await?;

// Get latest schema for a subject
let latest = infra.get_latest_schema("market_data-value").await?;

// List all subjects
let subjects = infra.get_subjects().await?;
```

| Method | Description |
|--------|-------------|
| `register_schema(subject, schema)` | Register a new schema version |
| `register_schema_with_refs(subject, schema, refs)` | Register with references |
| `get_schema(id)` | Get schema by ID |
| `get_latest_schema(subject)` | Get latest version for subject |
| `get_subjects()` | List all registered subjects |
| `has_schema_registry()` | Check if registry is available |
| `schema_registry()` | Get the underlying backend |

**API Compatibility with Confluent:**

| Method | In-Memory | Confluent | Notes |
|--------|-----------|-----------|-------|
| `get_schema` | ✅ | ✅ | By ID |
| `get_latest_schema` | ✅ | ✅ | By subject |
| `get_schema_version` | ✅ | ✅ | Specific version |
| `register_schema` | ✅ | ✅ | Returns schema ID |
| `check_compatibility` | ✅ (always true) | ✅ | Real check in prod |
| `get_versions` | ✅ | ✅ | All versions for subject |
| `get_subjects` | ✅ | ✅ | All subjects |
| `delete_schema_version` | ✅ | ✅ | Delete specific version |

### Recently Completed (Phases 7-9)

#### Phase 7: StreamJobServer Integration ✅

The `QueryExecutor` now fully integrates with `StreamJobServer` to execute SQL queries:

```rust
// Initialize executor with server
let executor = QueryExecutor::new(infra, overrides)
    .with_server()  // Creates StreamJobServer instance
    .await?;

// Execute queries - internally calls:
// 1. server.deploy_job(name, version, query, topic, None, None)
// 2. wait_for_job_completion(job_name, timeout)
// 3. server.stop_job(&name) after capture
```

| Method | Description |
|--------|-------------|
| `with_server()` | Initializes `StreamJobServer` with bootstrap servers from infra |
| `deploy_job()` | Deploys SQL query as a streaming job |
| `wait_for_job_completion()` | Polls job status until done or timeout |
| `stop_job()` | Stops running job after sink capture |

#### Phase 8: Performance Assertions ✅

New assertion types for performance constraints:

```yaml
assertions:
  - type: execution_time
    max_ms: 5000      # Maximum execution time
    min_ms: 100       # Minimum execution time (optional)

  - type: memory_usage
    max_bytes: 104857600    # 100 MB max
    max_mb: 100             # Alternative: specify in MB
    max_growth_bytes: 52428800  # Max memory growth during execution

  - type: throughput
    min_records_per_second: 100       # Minimum throughput
    max_records_per_second: 10000     # Maximum throughput (for rate limiting)
    expected_records_per_second: 500  # Expected rate with tolerance
    tolerance_percent: 20             # Tolerance for expected rate (default: 20%)
```

`CapturedOutput` now includes memory metrics:
- `memory_peak_bytes: Option<u64>` - Peak memory during execution
- `memory_growth_bytes: Option<i64>` - Memory growth (can be negative)

#### Phase 9: Foreign Key Reference Data ✅

Full support for foreign key relationships in data generation:

```rust
let mut generator = SchemaDataGenerator::new(Some(42));

// Method 1: Load explicit values
generator.load_reference_data("customers", "id", vec![
    FieldValue::String("CUST001".to_string()),
    FieldValue::String("CUST002".to_string()),
]);

// Method 2: Load from generated records
let customer_records = generator.generate(&customer_schema, 100)?;
generator.load_reference_data_from_records("customers", "id", &customer_records);

// Now orders.customer_id will sample from loaded reference data
let order_records = generator.generate(&order_schema, 1000)?;
```

| Method | Description |
|--------|-------------|
| `load_reference_data(table, field, values)` | Load explicit value list |
| `load_reference_data_from_records(table, field, records)` | Extract from generated records |
| `has_reference_data(table, field)` | Check if data is loaded |
| `reference_data_count(table, field)` | Get count of loaded values |

## Components

### 1. Test Infrastructure (Testcontainers)

Uses testcontainers-rs to spin up real Kafka instances for integration testing.

```rust
pub struct TestHarnessInfra {
    kafka: ContainerAsync<KafkaContainer>,
    schema_registry: Option<ContainerAsync<SchemaRegistryContainer>>,
    temp_dir: TempDir,  // For file sinks
}

impl TestHarnessInfra {
    pub async fn start() -> Self;
    pub fn bootstrap_servers(&self) -> String;
    pub fn file_sink_path(&self, sink_name: &str) -> PathBuf;
}
```

**Benefits:**
- Real Kafka behavior (not mocked)
- Isolated per-test environment
- Automatic cleanup on test completion

### 2. Schema-Driven Data Generation

Define schemas in YAML for consistent, realistic test data generation.

**Schema Definition Format:**

```yaml
# schemas/market_data.schema.yaml
schema:
  name: market_data
  namespace: trading

fields:
  - name: symbol
    type: string
    constraints:
      enum: [AAPL, GOOGL, MSFT, AMZN, META, NVDA, TSLA]

  - name: price
    type: decimal
    precision: 19
    scale: 4
    constraints:
      min: 1.0
      max: 5000.0

  - name: bid_price
    type: decimal
    precision: 19
    scale: 4
    constraints:
      derived: "price * random(0.995, 0.999)"

  - name: ask_price
    type: decimal
    precision: 19
    scale: 4
    constraints:
      derived: "price * random(1.001, 1.005)"

  - name: volume
    type: integer
    constraints:
      min: 100
      max: 1000000
      distribution: log_normal

  - name: event_time
    type: timestamp
    constraints:
      range: relative
      start: "-1h"
      end: "now"
      distribution: uniform

  - name: exchange
    type: string
    constraints:
      enum: [NYSE, NASDAQ, CBOE]
      weights: [0.4, 0.5, 0.1]

# Relationships for JOIN consistency
relationships:
  - field: symbol
    references: instrument_reference.symbol
    strategy: sample  # Generate symbols that exist in reference table
```

**Supported Field Types:**
| Type | Constraints |
|------|-------------|
| `string` | `enum`, `pattern`, `min_length`, `max_length` |
| `integer` | `min`, `max`, `distribution` |
| `decimal` | `min`, `max`, `precision`, `scale` |
| `timestamp` | `range`, `start`, `end`, `distribution` |
| `boolean` | `probability` |

**Constraint Types:**
| Constraint | Description |
|------------|-------------|
| `enum` | Pick from list of values |
| `weights` | Weighted distribution for enums |
| `min/max` | Range bounds |
| `distribution` | `uniform`, `normal`, `log_normal` |
| `derived` | Expression based on other fields |
| `references` | Foreign key to another table |

### 3. Test Specification Format

Define test cases with inputs, assertions, and expected behaviors.

```yaml
# test_spec.yaml
test_suite:
  name: Financial Trading Demo Tests
  sql_file: financial_trading.sql

  defaults:
    records_per_source: 1000
    timeout_ms: 30000

  schemas:
    market_data_ts: schemas/market_data.schema.yaml
    trading_positions: schemas/positions.schema.yaml

  reference_tables:
    instrument_reference: data/instrument_reference.csv
    trader_limits: data/trader_limits.csv

queries:
  - name: market_data_ts
    line: 65
    inputs:
      market_data:
        records: 1000
    assertions:
      - type: record_count
        operator: equals
        expected: 1000

      - type: schema_contains
        fields: [symbol, price, volume, event_time, _timestamp]

      - type: no_nulls
        fields: [symbol, price, event_time]

  - name: tick_buckets
    line: 95
    inputs:
      market_data_ts:
        from_previous: true  # Chain from previous query output
    assertions:
      - type: record_count
        operator: between
        min: 10
        max: 100

      - type: aggregate_check
        expression: "SUM(trade_count)"
        operator: equals
        expected: "{{inputs.market_data_ts.count}}"

  - name: enriched_market_data
    line: 139
    inputs:
      market_data_ts:
        records: 500
      instrument_reference:
        from_file: data/instrument_reference.csv
    assertions:
      - type: join_coverage
        left: market_data_ts
        right: instrument_reference
        key: symbol
        min_match_rate: 0.8

      - type: field_not_null
        fields: [instrument_name, tick_size]
        on_failure: "Enrichment fields should be populated"
```

### 4. Assertion Types

| Assertion Type | Parameters | Description |
|----------------|------------|-------------|
| `record_count` | `operator`, `expected/min/max` | Validate output record count |
| `schema_contains` | `fields`, `key_field` | Required fields present in output (see below) |
| `no_nulls` | `fields` | Specified fields have no null values |
| `field_not_null` | `fields` | Alias for `no_nulls` |
| `field_values` | `field`, `operator`, `value` | Field values meet condition |
| `field_in_set` | `field`, `values` | Field values within allowed set |
| `aggregate_check` | `expression`, `operator`, `expected` | Aggregate expression validation |
| `join_coverage` | `left`, `right`, `key`, `min_match_rate` | JOIN produces expected matches |
| `execution_time` | `operator`, `value_ms` | Performance constraint |
| `memory_usage` | `operator`, `value_mb` | Memory constraint |
| `throughput` | `min/max/expected_records_per_second`, `tolerance_percent` | Throughput rate constraint |
| `template` | `name`, `template` | Custom Jinja-style template assertion |

**Operators:** `equals`, `greater_than`, `less_than`, `between`, `not_equals`

#### Key Field Support in `schema_contains`

When using GROUP BY with a key field configured in the SQL (e.g., `'sink.key.field' = 'symbol'`),
the key is stored in the Kafka message key, not in the value payload. Use `key_field` to validate this:

```yaml
assertions:
  - type: schema_contains
    key_field: symbol        # Field stored in Kafka message key (GROUP BY field)
    fields:                  # Fields in the value payload
      - trade_count
      - total_volume
      - avg_price
      - window_start
      - window_end
```

**Behavior:**
- `key_field` (optional): Validates that Kafka message keys are present and non-empty
- `fields`: Validates fields in the message value payload
- If `key_field` is specified but no keys are found, the assertion fails
- Empty string keys are considered invalid (keys must have content)

### 5. Sequential Pipeline Execution

Queries execute in dependency order, with sink outputs captured for downstream queries.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Sequential Query Execution                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Query 1                    Query 2                    Query 3       │
│  ┌─────────┐               ┌─────────┐               ┌─────────┐    │
│  │ market_ │               │ tick_   │               │enriched_│    │
│  │ data_ts │               │ buckets │               │market_  │    │
│  └────┬────┘               └────┬────┘               └────┬────┘    │
│       │                         │                         │         │
│       ▼                         ▼                         ▼         │
│  ┌─────────┐               ┌─────────┐               ┌─────────┐    │
│  │  Kafka  │──────────────▶│  Kafka  │──────────────▶│  Kafka  │    │
│  │  Topic  │  (captured)   │  Topic  │  (captured)   │  Topic  │    │
│  └─────────┘               └─────────┘               └─────────┘    │
│       │                         │                         │         │
│       ▼                         ▼                         ▼         │
│  ┌─────────┐               ┌─────────┐               ┌─────────┐    │
│  │ Sink    │               │ Sink    │               │ Sink    │    │
│  │ Capture │               │ Capture │               │ Capture │    │
│  └─────────┘               └─────────┘               └─────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

**Sink Capture Methods:**
- **Kafka**: Consume all messages from topic with timeout
- **File**: Read JSONL output file from temp directory

### 6. Integration with Existing Engine

The test harness reuses existing Velostream components for realistic testing:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Velostream Components Reused                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  src/velostream/sql/validator.rs      → SQL parsing & validation    │
│  src/velostream/sql/query_analyzer.rs → Source/sink extraction      │
│  src/velostream/server/mod.rs         → StreamJobServer execution   │
│  src/velostream/kafka/                → Producer/Consumer adapters  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Execution Flow:**
1. `SqlValidator` parses the SQL file and extracts queries
2. `QueryAnalyzer` identifies sources, sinks, and their configurations
3. Test harness injects testcontainers config overrides
4. `StreamJobServer` executes each query with real Kafka
5. Sink capture reads output for assertions

### 7. Configuration Override

The test harness automatically overrides production configurations to use test infrastructure:

```yaml
# Original config (production)
bootstrap.servers: kafka.prod.example.com:9092
topic: market_data

# Test harness overrides to:
bootstrap.servers: localhost:32789  # Testcontainers dynamic port
topic: test_a1b2c3_market_data      # Prefixed with run ID
```

**Override Rules:**
| Original Key | Test Override |
|--------------|---------------|
| `bootstrap.servers` | Testcontainers Kafka address |
| `topic` / `topic.name` | `test_{run_id}_{original_topic}` |
| File sink `path` | `{temp_dir}/{sink_name}.jsonl` |
| `schema.registry.url` | Testcontainers Schema Registry (if enabled) |

### 8. Test Isolation & Cleanup

Each test run is fully isolated:

**Topic Naming:**
```
test_{run_id}_{sink_name}
     │         │
     │         └── Original sink name from SQL
     └── UUID generated per test run (e.g., a1b2c3d4)
```

**Cleanup Strategy:**
- Topics are automatically deleted after test run completes
- Temp directories are removed on harness shutdown
- Consumer groups are cleaned up to prevent offset conflicts

**Isolation Benefits:**
- Parallel test runs don't interfere
- No leftover state between runs
- CI/CD safe with multiple concurrent jobs

### 9. Error Handling

**Testcontainers Failures:**
```
⚠️ Could not start Kafka container: Docker not available
   Skipping integration tests. Run with --mock for unit tests only.
```

**Query Timeouts:**
```yaml
# test_spec.yaml
queries:
  - name: slow_query
    timeout_ms: 60000  # Override default 30s timeout
```

**Retry Logic:**
- Transient Kafka errors: 3 retries with exponential backoff
- Container startup: Wait up to 60s for health check
- Topic creation: Retry up to 5 times

### 10. Report Output

```
═══════════════════════════════════════════════════════════════════
  VELOSTREAM SQL TEST HARNESS REPORT
  File: demo/trading/sql/financial_trading.sql
  Run:  2025-11-25 17:15:32 UTC
═══════════════════════════════════════════════════════════════════

SUMMARY
───────
Total Queries:    14
Passed:           12 ✅
Failed:           1  ❌
Skipped:          1  ⏭️
Duration:         3.2s

RESULTS BY QUERY
────────────────────────────────────────────────────────────────────
✅ Query #1: market_data_ts (Line 65)
   Input:  1000 records from market_data
   Output: 1000 records
   Time:   45ms

✅ Query #2: tick_buckets (Line 95)
   Input:  1000 records
   Output: 48 aggregated buckets (5-min windows)
   Time:   120ms
   Validations:
     ✓ Output schema matches
     ✓ COUNT(*) > 0 for all buckets

❌ Query #5: enriched_market_data (Line 139)
   Input:  1000 market records, 50 instruments
   Output: 0 records
   Time:   89ms
   FAILURE: join_coverage
   Details: JOIN produced no matches - check symbol matching
   Suggestion: Verify instrument_reference contains symbols from market_data

⏭️ Query #14: arbitrage_detection (Line 890)
   Skipped: Requires multi-exchange data (not in test spec)

DATA GENERATION SUMMARY
───────────────────────
  market_data_ts:        1000 records generated
  instrument_reference:  50 records (from CSV)
  trading_positions:     200 records generated

PERFORMANCE METRICS
───────────────────
  Total execution time:  3.2s
  Peak memory:           45 MB
  Slowest query:         Query #10 (890ms)

RECOMMENDATIONS
───────────────
  ⚠️ Query #5 JOIN failure - ensure test data has matching keys
  💡 Query #10 slow - consider adding index on symbol field

═══════════════════════════════════════════════════════════════════
```

**Output Formats:**
- `text` - Human-readable console output (default)
- `json` - Machine-readable for tooling integration
- `junit` - JUnit XML for CI/CD systems

## CLI Interface

```bash
# Run full pipeline with test spec
velo-test run demo/trading/sql/financial_trading.sql \
  --spec demo/trading/test_spec.yaml \
  --schemas demo/trading/schemas/

# Quick validation (parse only, no execution)
velo-test validate demo/trading/sql/financial_trading.sql

# Run single query by name
velo-test run demo/trading/sql/financial_trading.sql \
  --query enriched_market_data

# Keep containers running for debugging
velo-test run demo/trading/sql/financial_trading.sql \
  --spec demo/trading/test_spec.yaml \
  --keep-containers

# Generate test spec template from SQL file
velo-test init demo/trading/sql/financial_trading.sql \
  --output demo/trading/test_spec.yaml

# Infer schemas from SQL and existing CSV data
velo-test infer-schema demo/trading/sql/financial_trading.sql \
  --data-dir demo/trading/data/ \
  --output demo/trading/schemas/

# Output formats
velo-test run app.sql --output json > results.json
velo-test run app.sql --output junit > results.xml

# Stress test mode
velo-test stress demo/trading/sql/financial_trading.sql \
  --records 100000 \
  --duration 60s

# Interactive debug mode
velo-test debug demo/trading/sql/financial_trading.sql \
  --spec demo/trading/test_spec.yaml \
  --schemas demo/trading/schemas/
```

## Interactive Debug Mode

The `velo-test debug` command provides a full-featured interactive debugger for streaming SQL applications. It allows step-by-step execution, breakpoints, and deep inspection of topics, messages, and job state.

### Starting a Debug Session

```bash
# Basic debug session
velo-test debug app.sql

# With test spec and schemas
velo-test debug app.sql --spec test_spec.yaml --schemas ./schemas/

# With breakpoints on specific statements
velo-test debug app.sql --breakpoint create_enriched_stream --breakpoint final_aggregation

# Connect to existing Kafka (no testcontainers)
velo-test debug app.sql --kafka localhost:9092

# Keep containers after exit for inspection
velo-test debug app.sql --keep-containers
```

### Debug Commands Reference

#### Execution Control

| Command | Shortcut | Description |
|---------|----------|-------------|
| `step` | `s` | Execute the next SQL statement |
| `continue` | `c` | Run until the next breakpoint |
| `run` | `r` | Run all remaining statements |
| `break <N>` | `b <N>` | Set breakpoint on statement N |
| `unbreak <N>` | `u <N>` | Remove breakpoint from statement N |
| `clear` | `cb` | Clear all breakpoints |
| `quit` | `q` | Exit debugger and cleanup |

#### State Inspection

| Command | Shortcut | Description |
|---------|----------|-------------|
| `list` | `l` | List all SQL statements with breakpoints |
| `status` | `st` | Show current execution state |
| `inspect <N>` | `i <N>` | Inspect captured output from statement N |
| `inspect-all` | `ia` | Inspect all captured outputs |
| `history` | `hi` | Show command history |

#### Infrastructure Inspection

| Command | Shortcut | Description |
|---------|----------|-------------|
| `topics` | `lt` | List all Kafka topics with message counts and offsets |
| `consumers` | `lc` | List all consumer groups with lag info |
| `jobs` | `lj` | List all jobs with source/sink details |
| `schema <topic>` | `sc <topic>` | Show inferred schema for a topic |

#### Data Visibility Commands

| Command | Description |
|---------|-------------|
| `messages <topic\|N> [options]` | Peek at topic messages (see below) |
| `head <stmt> [-n N]` | Show first N records from statement output (default: 10) |
| `tail <stmt> [-n N]` | Show last N records from statement output (default: 10) |
| `filter <stmt> <expr>` | Filter records by expression (e.g., `status=FAILED`) |
| `export <stmt> <file>` | Export records to JSON or CSV file |

### Numbered Topic References

After running `topics`, you can reference topics by number instead of typing full names:

```
(debug) topics
   📋 Topics (3):
   (Use number with 'messages N' for quick access)
   [1] test_abc123_market_data [test] (150 messages)
   [2] test_abc123_enriched_output [test] (75 messages)
   [3] test_abc123_aggregated [test] (12 messages)

(debug) messages 1 --last 5    # Instead of typing full topic name
```

### Messages Command Options

```bash
# Show last 5 messages (default)
messages <topic|N>

# Show last N messages
messages <topic|N> --last 10

# Show first N messages
messages <topic|N> --first 10

# Show from specific offset
messages <topic|N> --offset 100

# Filter by partition
messages <topic|N> --partition 0 --last 5
```

**Message Display Format:**

Each message shows:
- Partition and offset
- Timestamp (formatted)
- Key (or `<null>` if not set)
- Headers (if present)
- Value (pretty-printed JSON)

```
📨 5 messages:
  ─────────────────────────────────────────
  [1] P0:offset 145
      timestamp: 2025-01-15 14:30:22.456
      key: AAPL
      headers:
        content-type: application/json
        source: market-feed-1
      value:
        {
          "symbol": "AAPL",
          "price": 178.50,
          "quantity": 100,
          "event_time": "2025-01-15T14:30:22Z"
        }
```

### Filter Expressions

Filter records using field comparisons:

```bash
# Equality
filter 1 status=COMPLETED

# Inequality
filter 1 error_count!=0

# Numeric comparisons
filter 1 price>100
filter 1 quantity>=1000

# Contains (substring match)
filter 1 message~error
```

**Operators:** `=`, `!=`, `>`, `<`, `>=`, `<=`, `~` (contains)

### Export Command

Export captured records to files:

```bash
# Export to JSON
export 1 results.json

# Export to CSV (inferred from extension)
export 1 results.csv
```

### Example Debug Session

```
🐛 Velostream SQL Debugger
════════════════════════════════════════
SQL File: demo/trading/sql/financial_trading.sql
Test Spec: demo/trading/test_spec.yaml
Timeout: 60000ms

📁 Loading schemas from demo/trading/schemas
   ✓ market_data (5 fields, key_field: symbol)
   ✓ positions (4 fields)

🔧 Initializing debug infrastructure...
🐳 Starting Kafka via testcontainers...
   Kafka: localhost:32789 (testcontainers)

📝 SQL Statements:
    [1] create_market_stream (CREATE STREAM)
    [2] create_positions_table (CREATE TABLE)
  * [3] enriched_trades (SELECT)           <- breakpoint
    [4] aggregated_output (SELECT)

▶️  Ready to execute [1/4] create_market_stream
(debug) step
   ✅ create_market_stream completed in 234ms
      Output: 0 records to market_data

   📋 Topic State:
      • test_abc123_market_data [test]: 100 msgs

(debug) topics
   📋 Topics (2):
   [1] test_abc123_market_data [test] (100 messages)
     └─ P0: 100 msgs (offsets: 0..100) [last: key="MSFT", @14:30:45]

(debug) messages 1 --last 2
📨 2 messages:
  ─────────────────────────────────────────
  [1] P0:offset 98
      timestamp: 2025-01-15 14:30:44.123
      key: AAPL
      value:
        {"symbol": "AAPL", "price": 178.50, "quantity": 100}
  ─────────────────────────────────────────
  [2] P0:offset 99
      timestamp: 2025-01-15 14:30:45.234
      key: MSFT
      value:
        {"symbol": "MSFT", "price": 425.20, "quantity": 50}

(debug) continue
⏸️  Paused at [3/4] enriched_trades (SELECT)

(debug) step
   ✅ enriched_trades completed in 456ms
      Output: 100 records to enriched_output

(debug) head 3 -n 3
📊 First (3 of 100 records) from 'enriched_trades':
  [1] {"symbol":"AAPL","price":178.50,"enriched_field":"value1"}
  [2] {"symbol":"GOOGL","price":142.30,"enriched_field":"value2"}
  [3] {"symbol":"MSFT","price":425.20,"enriched_field":"value3"}

(debug) filter 3 symbol=AAPL
🔍 Filtered 'enriched_trades' where symbol=AAPL:
   Found 15 of 100 records matching
  [1] {"symbol":"AAPL","price":178.50,"enriched_field":"value1"}
  [12] {"symbol":"AAPL","price":179.20,"enriched_field":"value1"}
  ...

(debug) export 3 results.json
✅ Exported 100 records to results.json (JSON)

(debug) jobs
   🔧 Jobs (3):
   ▶️ 🌊 create_market_stream [Stream] (Running)
     📝 SQL: CREATE STREAM market_data ...
     📈 Stats: 100 written, 0 errors, 234ms
     📥 Sources:
       • market_data_source [Kafka]
         bootstrap.servers: localhost:32789
         group.id: test-abc123-market-data
     📤 Sinks:
       • market_data_sink [Kafka]
         stats: 100 records

(debug) run
   ✅ aggregated_output completed in 123ms
✅ All statements completed

🏁 (inspect) quit
👋 Exiting debugger
🧹 Cleaning up...

📊 Debug Session Summary
   Executed: 4/4 statements
   Passed: 4/4
```

## Project Structure

> All file paths are relative to project root. Click to view source.

```
src/
├── bin/
│   └── velo-test.rs                  # CLI entry point (run, validate, debug, stress)
└── velostream/test_harness/
    ├── mod.rs                        # Module exports
    ├── infra.rs                      # Testcontainers setup, topic management, message peek
    ├── schema.rs                     # Schema parsing & types (YAML deserialization)
    ├── generator.rs                  # Data generation engine (distributions, derived)
    ├── executor.rs                   # Query execution (StreamJobServer integration)
    ├── statement_executor.rs         # Interactive debugging, breakpoints, data visibility
    ├── capture.rs                    # Sink capture (Kafka consumer, file reader)
    ├── assertions.rs                 # Assertion engine (10+ types, templates)
    ├── report.rs                     # Report generation (Text, JSON, JUnit)
    ├── cli.rs                        # CLI argument parsing (clap)
    ├── ai.rs                         # AI-powered features (Claude API)
    ├── spec.rs                       # Test specification parsing
    ├── error.rs                      # Error types (thiserror)
    ├── config_override.rs            # Config override mechanism
    ├── inference.rs                  # Schema inference from SQL/CSV
    ├── spec_generator.rs             # Auto-generate test specs
    ├── stress.rs                     # Stress test mode (throughput, memory)
    ├── dlq.rs                        # Dead Letter Queue capture, error classification
    ├── fault_injection.rs            # Chaos testing (malformed, duplicates, out-of-order)
    ├── table_state.rs                # CTAS table tracking, snapshots
    └── file_io.rs                    # CSV/JSON file loading and writing

tests/integration/
    └── test_harness_integration_test.rs  # Integration tests with testcontainers
```

**Source Links:**
- [`src/bin/velo-test.rs`](../../src/bin/velo-test.rs) - CLI entry point
- [`src/velostream/test_harness/`](../../src/velostream/test_harness/) - All test harness modules

## Implementation Phases

> **All phases completed.** See [TODO.md](./TODO.md) for detailed task tracking.

### Phase 1: Foundation ✅
- [x] CLI skeleton with clap - [`cli.rs`](../../src/velostream/test_harness/cli.rs)
- [x] Testcontainers infrastructure (Kafka) - [`infra.rs`](../../src/velostream/test_harness/infra.rs)
- [x] Basic schema parsing - [`schema.rs`](../../src/velostream/test_harness/schema.rs)
- [x] Simple data generation (enum, range) - [`generator.rs`](../../src/velostream/test_harness/generator.rs)

### Phase 2: Execution Engine ✅
- [x] Sequential query execution - [`executor.rs`](../../src/velostream/test_harness/executor.rs)
- [x] StreamJobServer integration - `with_server()`, `deploy_job()`, `wait_for_job_completion()`
- [x] Kafka sink capture - [`capture.rs`](../../src/velostream/test_harness/capture.rs)
- [x] File sink capture - [`capture.rs`](../../src/velostream/test_harness/capture.rs)
- [x] Input chaining (`from_previous`) - [`executor.rs`](../../src/velostream/test_harness/executor.rs)

### Phase 3: Assertions ✅
- [x] Basic assertions (record_count, schema_contains, no_nulls) - [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
- [x] Field value assertions - [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
- [x] Aggregate checks - [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
- [x] JOIN coverage validation - [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)

### Phase 4: Reporting ✅
- [x] Text report output - [`report.rs`](../../src/velostream/test_harness/report.rs)
- [x] JSON output - [`report.rs`](../../src/velostream/test_harness/report.rs)
- [x] JUnit XML output - [`report.rs`](../../src/velostream/test_harness/report.rs)

### Phase 5: Advanced Features ✅
- [x] Schema inference from SQL/CSV - [`inference.rs`](../../src/velostream/test_harness/inference.rs)
- [x] Test spec generation (`velo-test init`) - [`spec_generator.rs`](../../src/velostream/test_harness/spec_generator.rs)
- [x] Derived field expressions - [`generator.rs`](../../src/velostream/test_harness/generator.rs)
- [x] Foreign key relationships in data generation - [`generator.rs`](../../src/velostream/test_harness/generator.rs)
- [x] `execution_time`, `memory_usage`, and `throughput` assertions - [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
- [x] Template-based custom assertions - [`assertions.rs`](../../src/velostream/test_harness/assertions.rs)
- [x] Stress test mode - [`stress.rs`](../../src/velostream/test_harness/stress.rs)

### Phase 6: AI-Powered Features ✅
- [x] Claude API integration - [`ai.rs`](../../src/velostream/test_harness/ai.rs)
- [x] AI schema inference (`--ai` flag) - [`ai.rs`](../../src/velostream/test_harness/ai.rs)
- [x] AI failure analysis - [`ai.rs`](../../src/velostream/test_harness/ai.rs)
- [x] AI test generation - [`ai.rs`](../../src/velostream/test_harness/ai.rs)

## AI-Powered Features

Built-in Claude API integration for intelligent test assistance.

### AI Schema Inference

```bash
velo-test infer-schema financial_trading.sql \
  --data-dir data/ \
  --ai \
  --output schemas/
```

**Capabilities:**
- Analyzes SQL queries to understand field types and relationships
- Samples CSV data to infer realistic value ranges
- Generates schema.yaml with intelligent constraints
- Detects foreign key relationships from JOINs

**Example Output:**
```yaml
# AI-generated schema for market_data
schema:
  name: market_data
  # AI detected: price field appears to be currency (USD)
  # Range inferred from CSV sample: $45.23 - $4,892.50
fields:
  - name: price
    type: decimal
    precision: 19
    scale: 4
    constraints:
      min: 45.0
      max: 5000.0
      # AI note: Realistic stock price range for US equities
```

### AI Failure Analysis

When assertions fail, AI provides actionable explanations:

```
❌ Query #5: enriched_market_data
   FAILURE: join_coverage (0% match rate, expected 80%)

   🤖 AI Analysis:
   The JOIN on 'symbol' produced no matches because:
   - market_data contains symbols: [AAPL, GOOGL, MSFT]
   - instrument_reference contains symbols: [IBM, ORCL, SAP]

   Suggested fix:
   1. Update schema relationships to sample symbols from instrument_reference.csv
   2. Or add [AAPL, GOOGL, MSFT] to instrument_reference.csv

   Related schema change:
   ```yaml
   relationships:
     - field: symbol
       references: instrument_reference.symbol
       strategy: sample  # Ensures generated symbols exist in reference
   ```
```

### AI Test Generation

```bash
velo-test init financial_trading.sql --ai --output test_spec.yaml
```

**AI analyzes query patterns to generate intelligent assertions:**

| Query Pattern | Generated Assertion |
|---------------|---------------------|
| `GROUP BY` + aggregates | `aggregate_check` with expected totals |
| `JOIN` operations | `join_coverage` with realistic match rates |
| Window functions | Time-based validations |
| `WHERE` filters | `record_count` with estimated reduction |

**Example AI-Generated Test Spec:**
```yaml
# AI-generated test spec for financial_trading.sql
queries:
  - name: tick_buckets
    # AI detected: 5-minute tumbling window aggregation
    # Expected: ~12 buckets per hour of input data
    assertions:
      - type: record_count
        operator: between
        min: 10
        max: 15
        # AI note: Based on 1 hour of data with 5-min windows

      - type: aggregate_check
        expression: "SUM(trade_count)"
        operator: equals
        expected: "{{inputs.market_data_ts.count}}"
        # AI note: Window aggregation should preserve total count
```

### AI Implementation

```rust
// src/test_harness/ai.rs
pub struct AiAssistant {
    client: anthropic::Client,
    model: String,  // "claude-sonnet-4-20250514"
}

impl AiAssistant {
    /// Analyze SQL and CSV samples to generate schema definitions
    pub async fn infer_schema(
        &self,
        sql: &str,
        csv_samples: &[CsvSample],
    ) -> Result<Schema, AiError>;

    /// Explain why an assertion failed and suggest fixes
    pub async fn analyze_failure(
        &self,
        failure: &AssertionFailure,
        context: &TestContext,
    ) -> Result<String, AiError>;

    /// Generate test_spec.yaml from SQL analysis
    pub async fn generate_test_spec(
        &self,
        sql: &str,
        queries: &[ParsedQuery],
    ) -> Result<TestSpec, AiError>;
}
```

## Dependencies

```toml
[dependencies]
testcontainers = "0.15"
testcontainers-modules = { version = "0.3", features = ["kafka"] }
serde = { version = "1.0", features = ["derive"] }
serde_yaml = "0.9"
clap = { version = "4.0", features = ["derive"] }
tokio = { version = "1.0", features = ["full"] }
rand = "0.8"
chrono = "0.4"

# AI Features (Phase 6)
anthropic = "0.1"  # Claude API client
```

## Example: Complete Test Setup

```
demo/trading/
├── sql/
│   └── financial_trading.sql       # SQL application
├── schemas/
│   ├── market_data.schema.yaml     # Data generation schema
│   ├── positions.schema.yaml
│   └── order_book.schema.yaml
├── data/
│   ├── instrument_reference.csv    # Reference table data
│   ├── trader_limits.csv
│   └── firm_limits.csv
├── test_spec.yaml                  # Test specification
└── configs/
    └── *.yaml                      # Source/sink configs
```

**Run tests:**
```bash
cd demo/trading
velo-test run sql/financial_trading.sql --spec test_spec.yaml
```

## Claude Code Integration (CLAUDE.md)

For interactive SQL development assistance, include a `CLAUDE.md` in your SQL application directory:

```
demo/trading/
├── CLAUDE.md                       # Claude Code context for SQL assistance
├── sql/
│   └── financial_trading.sql
├── schemas/
├── data/
└── test_spec.yaml
```

**Example `demo/trading/CLAUDE.md`:**

```markdown
# Trading Demo - Claude Code Context

## Velostream SQL Syntax

This application uses Velostream streaming SQL. Key syntax differences from standard SQL:

### Window Functions
Use `ROWS WINDOW BUFFER N ROWS` syntax (NOT standard `OVER (ROWS BETWEEN ...)`):
```sql
-- Correct Velostream syntax
AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol ORDER BY event_time)

-- NOT standard SQL (will fail)
AVG(price) OVER (PARTITION BY symbol ORDER BY event_time ROWS BETWEEN 99 PRECEDING AND CURRENT ROW)
```

### Time Windows
```sql
GROUP BY symbol WINDOW TUMBLING(INTERVAL '5' MINUTE)
GROUP BY symbol WINDOW SLIDING(INTERVAL '1' HOUR, INTERVAL '5' MINUTE)
GROUP BY symbol WINDOW SESSION(INTERVAL '30' SECOND)
```

### Source/Sink Configuration
```sql
WITH (
    'source_name.type' = 'kafka_source',
    'source_name.config_file' = 'configs/source.yaml',
    'sink_name.type' = 'kafka_sink',
    'sink_name.config_file' = 'configs/sink.yaml'
)
```

## Test Specification Format

When writing test_spec.yaml:
- Use `from_previous: true` to chain query outputs
- Use `from_file: path.csv` for reference tables
- Available assertions: record_count, schema_contains, no_nulls, join_coverage, aggregate_check

## Schema Definition Format

When writing schemas/*.schema.yaml:
- Supported types: string, integer, decimal, timestamp, boolean
- Use `relationships` for JOIN consistency
- Use `derived` for calculated fields (e.g., bid_price from price)

## Common Patterns

### Stream-Table JOIN
```sql
SELECT s.*, t.enrichment_field
FROM stream_source s
LEFT JOIN table_source t ON s.key = t.key
```

### Windowed Aggregation
```sql
SELECT symbol, COUNT(*) as trade_count, AVG(price) as avg_price
FROM market_data
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
```
```

### Using Claude for SQL Construction

With the CLAUDE.md in place, Claude Code can help developers:

1. **Write new queries** with correct Velostream syntax
2. **Debug SQL errors** by understanding the parser grammar
3. **Generate test specs** for new queries
4. **Create schemas** for data generation

**Example interaction:**
```
User: Help me write a query that detects price spikes > 5% in 1-minute windows

Claude: Here's a Velostream query for detecting price spikes:

SELECT
    symbol,
    price,
    LAG(price, 1) OVER (ROWS WINDOW BUFFER 10 ROWS PARTITION BY symbol ORDER BY event_time) as prev_price,
    ((price - LAG(price, 1) OVER (ROWS WINDOW BUFFER 10 ROWS PARTITION BY symbol ORDER BY event_time))
     / LAG(price, 1) OVER (ROWS WINDOW BUFFER 10 ROWS PARTITION BY symbol ORDER BY event_time)) * 100 as pct_change
FROM market_data
WHERE ABS(((price - LAG(price, 1) OVER (...)) / LAG(price, 1) OVER (...))) * 100 > 5.0
EMIT CHANGES
WITH (
    'market_data.type' = 'kafka_source',
    'market_data.config_file' = 'configs/market_data_source.yaml'
);
```

## Success Criteria

1. **Usability**: Developer can create and run tests for a new SQL app in < 30 minutes
2. **Reliability**: Tests produce consistent results across runs
3. **Performance**: Full test suite for trading demo completes in < 60 seconds
4. **CI Integration**: Works seamlessly in GitHub Actions with JUnit output
5. **Error Clarity**: Failed assertions provide actionable debugging information
6. **AI Assistance**: Claude Code can help write correct Velostream SQL with CLAUDE.md context
