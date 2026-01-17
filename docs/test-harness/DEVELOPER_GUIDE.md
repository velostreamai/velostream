# FR-084: SQL Application Test Harness - Developer Guide

This guide walks you through using the Velostream SQL Application Test Harness to test your streaming SQL applications.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Project Structure](#project-structure)
3. [Schema Definitions](#schema-definitions)
4. [Test Specifications](#test-specifications)
5. [Assertions Reference](#assertions-reference)
6. [CLI Reference](#cli-reference)
7. [Data Generation](#data-generation)
8. [Foreign Key Relationships](#foreign-key-relationships)
9. [Performance Testing](#performance-testing)
10. [Schema Registry](#schema-registry)
11. [File-Based Testing](#file-based-testing)
12. [AI-Powered Features](#ai-powered-features)
13. [Troubleshooting](#troubleshooting)

---

## Quick Start

### 1. Create Your SQL Application

```sql
-- my_app.sql
-- Use CREATE TABLE for aggregations with GROUP BY (materialized result)
CREATE TABLE market_stats AS
SELECT
    symbol,
    AVG(price) AS avg_price,
    COUNT(*) AS trade_count
FROM market_data
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE)
EMIT CHANGES
WITH (
    -- Source configuration (uses source name as prefix)
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data_input',
    'market_data.format' = 'json',

    -- Sink configuration (uses output table name as prefix)
    'market_stats.type' = 'kafka_sink',
    'market_stats.topic' = 'market_stats_output',
    'market_stats.format' = 'json'
);
```

> **CSAS vs CTAS**: Use `CREATE STREAM` for continuous transformations (filters, joins). Use `CREATE TABLE` for aggregations with `GROUP BY` that produce materialized results.

### 2. Create a Schema for Test Data

```yaml
# schemas/market_data.schema.yaml
schema:
  name: market_data
  namespace: trading

fields:
  - name: symbol
    type: string
    constraints:
      enum: [AAPL, GOOGL, MSFT, AMZN]

  - name: price
    type: decimal
    precision: 19
    scale: 4
    constraints:
      min: 100.0
      max: 500.0

  - name: volume
    type: integer
    constraints:
      min: 100
      max: 10000

  - name: event_time
    type: timestamp
    constraints:
      range: relative
      start: "-1h"
      end: "now"
```

### 3. Create a Test Specification

```yaml
# test_spec.yaml
test_suite:
  name: Market Data Aggregation Tests
  sql_file: my_app.sql

  defaults:
    records_per_source: 1000
    timeout_ms: 30000

  schemas:
    market_data: schemas/market_data.schema.yaml

queries:
  - name: aggregated_output
    inputs:
      market_data:
        records: 1000
    assertions:
      - type: record_count
        operator: greater_than
        expected: 0

      - type: schema_contains
        fields: [symbol, avg_price, trade_count]

      - type: no_nulls
        fields: [symbol, avg_price]

      - type: execution_time
        max_ms: 10000
```

### 4. Run the Tests

```bash
# Run all tests
velo-test run my_app.sql --spec test_spec.yaml

# Run with verbose output
velo-test run my_app.sql --spec test_spec.yaml --verbose

# Output as JUnit XML for CI/CD
velo-test run my_app.sql --spec test_spec.yaml --output junit > results.xml
```

---

## Project Structure

Recommended directory structure for a SQL application with tests:

```
my_sql_app/
├── sql/
│   └── my_app.sql              # Your SQL application
├── schemas/
│   ├── market_data.schema.yaml # Input data schema
│   ├── orders.schema.yaml      # Another input schema
│   └── reference.schema.yaml   # Reference table schema
├── data/
│   └── reference_data.csv      # Static reference data
├── configs/
│   ├── source.yaml             # Kafka source config
│   └── sink.yaml               # Kafka sink config
├── test_spec.yaml              # Test specification
└── CLAUDE.md                   # (Optional) Claude Code context
```

---

## Schema Definitions

Schemas define how test data is generated. They support multiple field types and constraints.

### Field Types

| Type | Description | Example Value |
|------|-------------|---------------|
| `string` | Text data | `"AAPL"` |
| `integer` | Whole numbers | `42` |
| `decimal` | Precise decimals | `123.4567` |
| `float` | Floating point | `3.14159` |
| `boolean` | True/false | `true` |
| `timestamp` | Date/time | `2024-01-15T10:30:00` |

### Constraint Types

#### Enum Constraint
Pick from a list of values:

```yaml
- name: status
  type: string
  constraints:
    enum: [PENDING, ACTIVE, COMPLETED, CANCELLED]
```

#### Weighted Enum
Control distribution of enum values:

```yaml
- name: exchange
  type: string
  constraints:
    enum: [NYSE, NASDAQ, CBOE]
    weights: [0.4, 0.5, 0.1]  # 40% NYSE, 50% NASDAQ, 10% CBOE
```

#### Range Constraint
Generate values within a range:

```yaml
- name: price
  type: decimal
  constraints:
    min: 10.0
    max: 1000.0
```

#### Distribution
Control how values are distributed:

```yaml
- name: volume
  type: integer
  constraints:
    min: 100
    max: 1000000
    distribution: log_normal  # uniform, normal, log_normal
```

#### Pattern Constraint
Generate strings matching a pattern:

```yaml
- name: order_id
  type: string
  constraints:
    pattern: "ORD-[A-Z]{3}-[0-9]{6}"  # e.g., ORD-ABC-123456
```

#### Timestamp Range
Generate timestamps in a range:

```yaml
- name: event_time
  type: timestamp
  constraints:
    range: relative
    start: "-1h"      # 1 hour ago
    end: "now"        # Current time
    distribution: uniform
```

Absolute timestamps:

```yaml
- name: created_at
  type: timestamp
  constraints:
    range: absolute
    start: "2024-01-01T00:00:00"
    end: "2024-12-31T23:59:59"
```

#### Derived Fields
Calculate values based on other fields:

```yaml
- name: price
  type: decimal
  constraints:
    min: 100.0
    max: 500.0

- name: bid_price
  type: decimal
  constraints:
    derived: "price * random(0.995, 0.999)"

- name: ask_price
  type: decimal
  constraints:
    derived: "price * random(1.001, 1.005)"

- name: spread
  type: decimal
  constraints:
    derived: "ask_price - bid_price"
```

#### Foreign Key References
Reference values from another schema:

```yaml
- name: customer_id
  type: string
  constraints:
    references:
      schema: customers
      field: id
```

---

## Test Specifications

Test specs define what to test and how to validate results.

### Basic Structure

```yaml
test_suite:
  name: My Test Suite
  sql_file: path/to/app.sql

  defaults:
    records_per_source: 1000
    timeout_ms: 30000

  schemas:
    source_name: path/to/schema.yaml

queries:
  - name: query_name
    inputs:
      source_name:
        records: 500
    assertions:
      - type: record_count
        expected: 500
```

### Input Sources

#### Generated Data
```yaml
inputs:
  market_data:
    records: 1000  # Generate 1000 records using schema
```

#### From Previous Query
```yaml
inputs:
  enriched_data:
    from_previous: true  # Use output from previous query
```

#### From CSV File
```yaml
inputs:
  reference_table:
    from_file: data/reference.csv
```

### Query Options

```yaml
queries:
  - name: slow_query
    timeout_ms: 60000        # Override default timeout
    skip: false              # Set to true to skip this query
    inputs:
      # ...
    assertions:
      # ...
```

---

## Assertions Reference

### record_count
Validate the number of output records.

```yaml
# Exact count
- type: record_count
  operator: equals
  expected: 100

# Range
- type: record_count
  operator: between
  min: 10
  max: 100

# Comparison
- type: record_count
  operator: greater_than
  expected: 0
```

**Operators:** `equals`, `not_equals`, `greater_than`, `less_than`, `greater_than_or_equal`, `less_than_or_equal`, `between`

### schema_contains
Verify required fields exist in output.

```yaml
- type: schema_contains
  fields: [symbol, price, volume, timestamp]
```

### no_nulls
Ensure specified fields have no null values.

```yaml
- type: no_nulls
  fields: [symbol, price]
```

### field_values
Validate field values meet conditions.

```yaml
# All prices > 0
- type: field_values
  field: price
  operator: greater_than
  value: 0

# Status in allowed set
- type: field_values
  field: status
  operator: in
  values: [ACTIVE, COMPLETED]
```

### field_in_set
Validate field values are within allowed set.

```yaml
- type: field_in_set
  field: exchange
  values: [NYSE, NASDAQ, CBOE, ARCA]
```

### aggregate_check
Validate aggregate expressions.

```yaml
# Sum of quantities
- type: aggregate_check
  expression: "SUM(quantity)"
  operator: equals
  expected: 10000

# Average price in range
- type: aggregate_check
  expression: "AVG(price)"
  operator: between
  min: 100.0
  max: 500.0
```

### join_coverage
Validate JOIN match rates.

```yaml
- type: join_coverage
  left: orders
  right: customers
  key: customer_id
  min_match_rate: 0.95  # At least 95% of orders have matching customer
```

### execution_time
Validate query execution time.

```yaml
- type: execution_time
  max_ms: 5000   # Must complete within 5 seconds
  min_ms: 100    # Optional: must take at least 100ms
```

### memory_usage
Validate memory consumption.

```yaml
- type: memory_usage
  max_mb: 100              # Max 100 MB peak memory
  max_growth_bytes: 52428800  # Max 50 MB growth during execution
```

### throughput
Validate processing throughput (records per second).

```yaml
- type: throughput
  min_records_per_second: 100       # Minimum required throughput
  max_records_per_second: 10000     # Maximum allowed (for rate limiting)
  expected_records_per_second: 500  # Expected rate with tolerance
  tolerance_percent: 20             # Default: 20%
```

### template
Custom assertion using template expressions.

```yaml
- type: template
  name: price_sanity_check
  template: "{{ records | selectattr('price', 'gt', 0) | list | length }} == {{ records | length }}"
  message: "All prices must be positive"
```

---

## CLI Reference

### Commands

#### run
Execute tests against a SQL application.

```bash
velo-test run <sql_file> [options]

Options:
  --spec <file>       Test specification file (required)
  --schemas <dir>     Directory containing schema files
  --query <name>      Run only specific query
  --output <format>   Output format: text, json, junit (default: text)
  --verbose           Enable verbose output
  --timeout <ms>      Global timeout in milliseconds
```

Examples:
```bash
# Basic run
velo-test run app.sql --spec test_spec.yaml

# Single query
velo-test run app.sql --spec test_spec.yaml --query aggregated_output

# JUnit output for CI
velo-test run app.sql --spec test_spec.yaml --output junit > results.xml

# JSON output
velo-test run app.sql --spec test_spec.yaml --output json > results.json
```

#### validate
Validate SQL syntax without executing.

```bash
velo-test validate <sql_file>
```

#### init
Generate a test specification template from SQL.

```bash
velo-test init <sql_file> [options]

Options:
  --output <file>     Output file (default: test_spec.yaml)
  --ai                Use AI to generate intelligent assertions
```

Examples:
```bash
# Generate basic spec
velo-test init app.sql --output test_spec.yaml

# AI-assisted generation
velo-test init app.sql --ai --output test_spec.yaml
```

#### infer-schema
Infer schemas from SQL and sample data.

```bash
velo-test infer-schema <sql_file> [options]

Options:
  --data-dir <dir>    Directory with sample CSV files
  --output <dir>      Output directory for schemas
  --ai                Use AI for intelligent inference
```

#### stress
Run stress tests with high volume.

```bash
velo-test stress <sql_file> [options]

Options:
  --records <n>       Number of records to generate
  --duration <sec>    Test duration in seconds
  --spec <file>       Test specification file
```

---

## Data Generation

### Programmatic Usage

```rust
use velostream::velostream::test_harness::{
    SchemaDataGenerator,
    TestDataSchema,
};

// Create generator with seed for reproducibility
let mut generator = SchemaDataGenerator::new(Some(42));

// Load schema from file
let schema = TestDataSchema::from_file("schemas/market_data.schema.yaml")?;

// Generate records
let records = generator.generate(&schema, 1000)?;

// Access generated data
for record in &records {
    let symbol = record.get("symbol");
    let price = record.get("price");
    println!("{:?}: {:?}", symbol, price);
}
```

### Deterministic Generation

Use a seed for reproducible test data:

```rust
// Same seed = same data
let mut gen1 = SchemaDataGenerator::new(Some(42));
let mut gen2 = SchemaDataGenerator::new(Some(42));

let records1 = gen1.generate(&schema, 100)?;
let records2 = gen2.generate(&schema, 100)?;

assert_eq!(records1, records2);  // Identical!
```

---

## Foreign Key Relationships

Ensure referential integrity between tables.

### In Schema Definition

```yaml
# orders.schema.yaml
fields:
  - name: order_id
    type: string
    constraints:
      pattern: "ORD-[0-9]{8}"

  - name: customer_id
    type: string
    constraints:
      references:
        schema: customers
        field: id
```

### Programmatic Loading

```rust
let mut generator = SchemaDataGenerator::new(Some(42));

// Method 1: Load explicit values
generator.load_reference_data("customers", "id", vec![
    FieldValue::String("CUST001".to_string()),
    FieldValue::String("CUST002".to_string()),
    FieldValue::String("CUST003".to_string()),
]);

// Method 2: Load from generated records
let customer_records = generator.generate(&customer_schema, 100)?;
generator.load_reference_data_from_records("customers", "id", &customer_records);

// Now generate orders - customer_id will reference valid customers
let order_records = generator.generate(&order_schema, 1000)?;
```

---

## Performance Testing

### Execution Time Assertions

```yaml
assertions:
  - type: execution_time
    max_ms: 5000      # Must complete within 5 seconds
    min_ms: 100       # Must take at least 100ms (detect suspiciously fast)
```

### Memory Usage Assertions

```yaml
assertions:
  - type: memory_usage
    max_bytes: 104857600     # 100 MB max peak
    max_mb: 100              # Alternative syntax
    max_growth_bytes: 52428800  # Max 50 MB growth
```

### Stress Testing

```bash
# Run with 100K records for 60 seconds
velo-test stress app.sql --records 100000 --duration 60

# With specific spec
velo-test stress app.sql --spec test_spec.yaml --records 50000
```

### Programmatic Stress Test

```rust
use velostream::velostream::test_harness::stress::{
    StressTestConfig, StressTestRunner, MemoryTracker,
};

let config = StressTestConfig {
    records_per_batch: 1000,
    total_records: 100000,
    timeout: Duration::from_secs(120),
    track_memory: true,
};

let mut runner = StressTestRunner::new(config);
let results = runner.run(&executor).await?;

println!("Throughput: {} records/sec", results.throughput);
println!("Peak memory: {} MB", results.peak_memory_mb);
```

---

## Schema Registry

The test harness includes an in-memory schema registry that is API-compatible with Confluent Schema Registry.

### Usage

```rust
let mut infra = TestHarnessInfra::with_kafka("localhost:9092");
infra.start().await?;

// Register an Avro schema
let schema_id = infra.register_schema(
    "market_data-value",
    r#"{
        "type": "record",
        "name": "MarketData",
        "fields": [
            {"name": "symbol", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "volume", "type": "long"}
        ]
    }"#
).await?;

// Retrieve schema
let schema = infra.get_schema(schema_id).await?;

// Get latest version
let latest = infra.get_latest_schema("market_data-value").await?;

// List all subjects
let subjects = infra.get_subjects().await?;
```

### API Methods

| Method | Description |
|--------|-------------|
| `register_schema(subject, schema)` | Register new schema version |
| `register_schema_with_refs(subject, schema, refs)` | Register with references |
| `get_schema(id)` | Get schema by ID |
| `get_latest_schema(subject)` | Get latest version |
| `get_subjects()` | List all subjects |
| `has_schema_registry()` | Check availability |

---

## File-Based Testing

The test harness supports file-based input sources and output sinks, enabling testing without Kafka infrastructure.

### File Source Types

Configure inputs to read from files instead of Kafka:

```yaml
queries:
  - name: file_based_test
    inputs:
      - source: trades
        source_type:
          type: file
          path: ./input_trades.csv
          format: csv       # csv, csv_no_header, json_lines, json
          watch: false      # Set to true for continuous file watching
```

### File Sink Types

Configure outputs to write to files:

```yaml
queries:
  - name: file_output_test
    inputs:
      - source: trades
        source_type:
          type: file
          path: ./input.csv
          format: csv
    output:
      sink_type:
        type: file
        path: ./output.csv
        format: csv
```

### File Formats

| Format | Description | Extension |
|--------|-------------|-----------|
| `csv` | CSV with header row | `.csv` |
| `csv_no_header` | CSV without header | `.csv` |
| `json_lines` | One JSON object per line | `.jsonl` |
| `json` | JSON array of objects | `.json` |

### File Assertions

#### file_exists

Verify the output file exists with optional size constraints:

```yaml
assertions:
  - type: file_exists
    path: ./output.csv
    min_size_bytes: 100    # Optional: minimum file size
    max_size_bytes: 10000  # Optional: maximum file size
```

#### file_row_count

Verify the number of data rows in the output file:

```yaml
assertions:
  # Exact count
  - type: file_row_count
    path: ./output.csv
    format: csv
    equals: 100

  # Range check
  - type: file_row_count
    path: ./output.csv
    format: csv
    greater_than: 50
    less_than: 200
```

#### file_contains

Verify specific values exist in a field:

```yaml
assertions:
  # All values must be present
  - type: file_contains
    path: ./output.csv
    format: csv
    field: symbol
    expected_values:
      - AAPL
      - GOOGL
      - MSFT
    mode: all  # All values must exist

  # Any value must be present
  - type: file_contains
    path: ./output.csv
    format: csv
    field: exchange
    expected_values:
      - NYSE
      - NASDAQ
    mode: any  # At least one value must exist
```

#### file_matches

Compare output to an expected file:

```yaml
assertions:
  - type: file_matches
    actual_path: ./output.csv
    expected_path: ./expected_output.csv
    format: csv
    ignore_order: true          # Ignore row order
    ignore_fields:              # Fields to exclude from comparison
      - timestamp
      - event_id
    numeric_tolerance: 0.01     # Tolerance for floating-point comparison
```

### Complete File-Based Test Example

```yaml
# test_spec.yaml
application: file_io_demo
description: File-based input/output test

default_timeout_ms: 30000

queries:
  - name: trade_passthrough
    description: Read trades from CSV and output to CSV
    inputs:
      - source: trades
        source_type:
          type: file
          path: ./input_trades.csv
          format: csv
          watch: false
    output:
      sink_type:
        type: file
        path: ./output_trades.csv
        format: csv
    assertions:
      # Verify output file was created
      - type: file_exists
        path: ./output_trades.csv
        min_size_bytes: 100

      # Verify row count matches input
      - type: file_row_count
        path: ./output_trades.csv
        format: csv
        equals: 6

      # Verify expected symbols present
      - type: file_contains
        path: ./output_trades.csv
        format: csv
        field: symbol
        expected_values:
          - AAPL
          - GOOGL
          - MSFT
        mode: all

      # Verify output matches expected
      - type: file_matches
        actual_path: ./output_trades.csv
        expected_path: ./expected_output.csv
        format: csv
        ignore_order: true
        numeric_tolerance: 0.01
```

### Running File-Based Tests

```bash
# From the test directory
velo-test run passthrough.sql --spec test_spec.yaml

# Using full paths
velo-test run demo/test_harness_examples/tier1_basic/file_io/passthrough.sql \
  --spec demo/test_harness_examples/tier1_basic/file_io/test_spec.yaml
```

### Demo Example

See `demo/test_harness_examples/tier1_basic/file_io/` for a complete working example:

```
file_io/
├── input_trades.csv      # Sample trade data input
├── expected_output.csv   # Expected output for comparison
├── passthrough.sql       # Simple SQL passthrough query
├── test_spec.yaml        # Test specification with file assertions
└── README.md             # Documentation
```

---

## AI-Powered Features

### AI Schema Inference

```bash
velo-test infer-schema app.sql --data-dir data/ --ai --output schemas/
```

The AI will:
- Analyze SQL to understand field types
- Sample CSV data for realistic ranges
- Detect foreign key relationships from JOINs
- Generate appropriate constraints

### AI Test Generation

```bash
velo-test init app.sql --ai --output test_spec.yaml
```

The AI will:
- Analyze query patterns (GROUP BY, JOINs, windows)
- Generate appropriate assertions
- Estimate expected record counts
- Add performance constraints

### AI Failure Analysis

When tests fail, AI can explain why:

```
❌ Query #5: enriched_market_data
   FAILURE: join_coverage (0% match rate, expected 80%)

   🤖 AI Analysis:
   The JOIN on 'symbol' produced no matches because:
   - market_data contains symbols: [AAPL, GOOGL, MSFT]
   - instrument_reference contains symbols: [IBM, ORCL, SAP]

   Suggested fix:
   Update schema to sample symbols from instrument_reference
```

---

## Troubleshooting

### Common Issues

#### "No Kafka bootstrap servers configured"

**Cause:** Infrastructure not started or no Kafka connection.

**Solution:**
```rust
let mut infra = TestHarnessInfra::with_kafka("localhost:9092");
infra.start().await?;  // Don't forget this!
```

#### "Schema registry not initialized"

**Cause:** Calling schema methods before `start()`.

**Solution:** Call `infra.start()` first.

#### "Topic not found"

**Cause:** Topic doesn't exist or wrong name.

**Solution:**
```rust
// Create topic before use
let topic = infra.create_topic("my_topic", 1).await?;
```

#### Tests timing out

**Cause:** Query takes longer than timeout.

**Solution:**
```yaml
queries:
  - name: slow_query
    timeout_ms: 120000  # Increase timeout
```

#### Non-deterministic test failures

**Cause:** Random data generation without seed.

**Solution:**
```rust
// Use seed for reproducibility
let generator = SchemaDataGenerator::new(Some(42));
```

#### Foreign key values not matching

**Cause:** Reference data not loaded before generation.

**Solution:**
```rust
// Load reference data BEFORE generating dependent records
generator.load_reference_data("customers", "id", customer_ids);
let orders = generator.generate(&order_schema, 1000)?;
```

### Debug Mode

Enable verbose logging:

```bash
RUST_LOG=debug velo-test run app.sql --spec test_spec.yaml --verbose
```

### Getting Help

- Check the [README](./README.md) for feature overview
- See [TODO.md](./TODO.md) for implementation details
- File issues at the project repository

---

## Examples

### Complete Example: Financial Trading App

See the `demo/trading/` directory for a complete example including:
- SQL application with multiple queries
- Schema definitions for market data
- Test specification with comprehensive assertions
- Reference data files

```bash
cd demo/trading
velo-test run sql/financial_trading.sql --spec test_spec.yaml
```

### Example Test Output

```
═══════════════════════════════════════════════════════════════════
  VELOSTREAM SQL TEST HARNESS REPORT
  File: demo/trading/sql/financial_trading.sql
  Run:  2025-01-15 10:30:00 UTC
═══════════════════════════════════════════════════════════════════

SUMMARY
───────
Total Queries:    5
Passed:           5 ✅
Failed:           0 ❌
Skipped:          0 ⏭️
Duration:         2.3s

RESULTS BY QUERY
────────────────────────────────────────────────────────────────────
✅ Query #1: market_data_ts (Line 65)
   Input:  1000 records from market_data
   Output: 1000 records
   Time:   45ms
   Memory: 12.5 MB peak

✅ Query #2: tick_buckets (Line 95)
   Input:  1000 records
   Output: 48 aggregated buckets
   Time:   120ms

✅ Query #3: enriched_market_data (Line 139)
   Input:  1000 market + 50 instruments
   Output: 950 enriched records
   Time:   89ms
   Join Coverage: 95%

═══════════════════════════════════════════════════════════════════
```
