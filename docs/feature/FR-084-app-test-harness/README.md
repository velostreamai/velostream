# FR-084: SQL Application Test Harness

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
| `schema_contains` | `fields` | Required fields present in output |
| `no_nulls` | `fields` | Specified fields have no null values |
| `field_not_null` | `fields` | Alias for `no_nulls` |
| `field_values` | `field`, `operator`, `value` | Field values meet condition |
| `field_in_set` | `field`, `values` | Field values within allowed set |
| `aggregate_check` | `expression`, `operator`, `expected` | Aggregate expression validation |
| `join_coverage` | `left`, `right`, `key`, `min_match_rate` | JOIN produces expected matches |
| `execution_time` | `operator`, `value_ms` | Performance constraint |
| `memory_usage` | `operator`, `value_mb` | Memory constraint |
| `template` | `name`, `template` | Custom Jinja-style template assertion |

**Operators:** `equals`, `greater_than`, `less_than`, `between`, `not_equals`

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
```

## Project Structure

```
src/
├── bin/
│   └── velo_test.rs                  # CLI entry point
└── test_harness/
    ├── mod.rs                        # Module exports
    ├── infra.rs                      # Testcontainers setup
    ├── schema.rs                     # Schema parsing & types
    ├── generator.rs                  # Data generation engine
    ├── executor.rs                   # Pipeline executor
    ├── capture.rs                    # Sink capture (Kafka/File)
    ├── assertions.rs                 # Assertion engine
    ├── report.rs                     # Report generation
    ├── cli.rs                        # CLI argument parsing
    └── ai.rs                         # AI-powered features (Phase 6)
```

## Implementation Phases

### Phase 1: Foundation
- [ ] CLI skeleton with clap
- [ ] Testcontainers infrastructure (Kafka)
- [ ] Basic schema parsing
- [ ] Simple data generation (enum, range)

### Phase 2: Execution Engine
- [ ] Sequential query execution
- [ ] Kafka sink capture
- [ ] File sink capture
- [ ] Input chaining (`from_previous`)

### Phase 3: Assertions
- [ ] Basic assertions (record_count, schema_contains, no_nulls)
- [ ] Field value assertions
- [ ] Aggregate checks
- [ ] JOIN coverage validation

### Phase 4: Reporting
- [ ] Text report output
- [ ] JSON output
- [ ] JUnit XML output

### Phase 5: Advanced Features
- [ ] Schema inference from SQL/CSV
- [ ] Test spec generation (`velo-test init`)
- [ ] Derived field expressions
- [ ] Foreign key relationships in data generation
- [ ] Template-based custom assertions
- [ ] Stress test mode

### Phase 6: AI-Powered Features
- [ ] Claude API integration
- [ ] AI schema inference (`--ai` flag)
- [ ] AI failure analysis
- [ ] AI test generation

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
