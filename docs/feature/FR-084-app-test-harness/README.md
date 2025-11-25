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

### 6. Report Output

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
    └── cli.rs                        # CLI argument parsing
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

## Success Criteria

1. **Usability**: Developer can create and run tests for a new SQL app in < 30 minutes
2. **Reliability**: Tests produce consistent results across runs
3. **Performance**: Full test suite for trading demo completes in < 60 seconds
4. **CI Integration**: Works seamlessly in GitHub Actions with JUnit output
5. **Error Clarity**: Failed assertions provide actionable debugging information
