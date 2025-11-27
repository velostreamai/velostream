# Getting Started with the SQL Test Harness

This tutorial walks you through creating and running your first test for a Velostream SQL application.

## Prerequisites

### Option A: From Source (Current)

- Rust toolchain installed (rustc 1.70+)
- Docker running (for Kafka via testcontainers)
- Velostream source code cloned
- Build the test harness CLI:

```bash
# Clone the repository
git clone https://github.com/your-org/velostream.git
cd velostream

# Build in release mode
cargo build --release

# The velo-test binary will be at:
# ./target/release/velo-test

# Add to PATH (optional)
export PATH="$PATH:$(pwd)/target/release"
```

### Option B: Docker Image (Future)

> **Note**: Docker-based distribution is planned for a future release. This will allow running `velo-test` without installing Rust or building from source:
>
> ```bash
> # Future usage (not yet available)
> docker run -v $(pwd):/workspace velostream/velo-test validate /workspace/app.sql
> docker run -v $(pwd):/workspace velostream/velo-test run /workspace/app.sql --spec /workspace/test_spec.yaml
> ```
>
> Track progress: [GitHub Issue #XXX - Docker Distribution]

### Verify Installation

```bash
# Check velo-test is available
velo-test --help

# Expected output:
# velo-test - Velostream SQL Application Test Harness
#
# USAGE:
#     velo-test <COMMAND>
#
# COMMANDS:
#     validate    Validate SQL syntax without running tests
#     run         Run tests against a SQL application
#     init        Generate test specification from SQL file
#     ...
```

## Tutorial: Testing a Market Data Aggregation App

We'll create a simple SQL application that aggregates market data by symbol, then write tests to validate it.

### Step 1: Create the Project Structure

```bash
mkdir -p my_first_test/{sql,schemas,data}
cd my_first_test
```

### Step 2: Write the SQL Application

Create `sql/market_aggregation.sql`:

```sql
-- Market Data Aggregation
-- Aggregates trading data into 5-minute buckets per symbol

-- Use CREATE TABLE for queries with GROUP BY (materialized aggregation)
CREATE TABLE market_aggregates AS
SELECT
    symbol,
    COUNT(*) AS trade_count,
    SUM(volume) AS total_volume,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    _window_start AS window_start,
    _window_end AS window_end
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
    'market_aggregates.type' = 'kafka_sink',
    'market_aggregates.topic' = 'market_aggregates_output',
    'market_aggregates.format' = 'json'
);
```

> **Note**: Use `CREATE TABLE` (CTAS) for queries with `GROUP BY` that produce materialized aggregations. Use `CREATE STREAM` (CSAS) for continuous transformations without aggregation. The sink configuration uses the output table/stream name as the prefix (e.g., `'market_aggregates.type'`).

### Step 3: Define the Input Schema

Create `schemas/market_data.schema.yaml`:

```yaml
schema:
  name: market_data
  namespace: trading
  description: Real-time market data events

fields:
  - name: symbol
    type: string
    description: Stock ticker symbol
    constraints:
      enum: [AAPL, GOOGL, MSFT, AMZN, META]
      weights: [0.25, 0.20, 0.20, 0.20, 0.15]

  - name: price
    type: decimal
    precision: 19
    scale: 4
    description: Trade price
    constraints:
      min: 50.0
      max: 500.0
      distribution: normal

  - name: volume
    type: integer
    description: Number of shares traded
    constraints:
      min: 100
      max: 10000
      distribution: log_normal

  - name: event_time
    type: timestamp
    description: Time of the trade
    constraints:
      range: relative
      start: "-1h"
      end: "now"
      distribution: uniform
```

### Step 4: Create the Test Specification

Create `test_spec.yaml`:

```yaml
test_suite:
  name: Market Data Aggregation Tests
  description: Validates the market data aggregation query
  sql_file: sql/market_aggregation.sql

  defaults:
    records_per_source: 1000
    timeout_ms: 30000

  schemas:
    market_data: schemas/market_data.schema.yaml

queries:
  - name: market_aggregates
    description: Test aggregation produces expected results
    inputs:
      market_data:
        records: 1000
    assertions:
      # Should produce output records
      - type: record_count
        operator: greater_than
        expected: 0
        message: "Aggregation should produce at least one output"

      # Should have all required fields
      - type: schema_contains
        fields:
          - symbol
          - trade_count
          - total_volume
          - avg_price
          - min_price
          - max_price
        message: "Output must contain all aggregation fields"

      # No nulls in key fields
      - type: no_nulls
        fields: [symbol, trade_count, avg_price]
        message: "Key fields should never be null"

      # Validate trade counts are positive
      - type: field_values
        field: trade_count
        operator: greater_than
        value: 0
        message: "Trade count should be positive"

      # Validate price sanity
      - type: field_values
        field: avg_price
        operator: greater_than
        value: 0
        message: "Average price should be positive"

      # Performance constraint
      - type: execution_time
        max_ms: 10000
        message: "Query should complete within 10 seconds"

  - name: market_aggregates_high_volume
    description: Test with higher volume
    inputs:
      market_data:
        records: 5000
    assertions:
      - type: record_count
        operator: greater_than
        expected: 0

      # Verify total volume is preserved
      - type: aggregate_check
        expression: "SUM(total_volume)"
        operator: greater_than
        expected: 0
        message: "Total aggregated volume should be positive"

      # Memory shouldn't explode with more data
      - type: memory_usage
        max_mb: 200
        message: "Memory usage should stay reasonable"
```

### Step 5: Run the Tests

#### Option A: Validate SQL First (No Kafka Required)

```bash
# Validate SQL syntax
velo-test validate sql/market_aggregation.sql
```

Expected output:
```
✅ SQL validation passed: sql/market_aggregation.sql
   Queries found: 1
   Sources: market_data
   Sinks: market_aggregates
```

#### Option B: Run Full Tests (Requires Docker/Kafka)

```bash
# Run all tests
velo-test run sql/market_aggregation.sql --spec test_spec.yaml

# Run with verbose output
velo-test run sql/market_aggregation.sql --spec test_spec.yaml --verbose

# Run specific query test
velo-test run sql/market_aggregation.sql --spec test_spec.yaml --query market_aggregates
```

Expected output:
```
═══════════════════════════════════════════════════════════════════
  VELOSTREAM SQL TEST HARNESS REPORT
  File: sql/market_aggregation.sql
  Run:  2025-01-15 10:30:00 UTC
═══════════════════════════════════════════════════════════════════

SUMMARY
───────
Total Queries:    2
Passed:           2 ✅
Failed:           0 ❌
Duration:         3.2s

RESULTS BY QUERY
────────────────────────────────────────────────────────────────────
✅ Query #1: market_aggregates
   Input:  1000 records from market_data
   Output: 5 aggregated records (one per symbol)
   Time:   1.2s
   Assertions: 6/6 passed

✅ Query #2: market_aggregates_high_volume
   Input:  5000 records from market_data
   Output: 5 aggregated records
   Time:   1.8s
   Memory: 45 MB peak
   Assertions: 3/3 passed

═══════════════════════════════════════════════════════════════════
```

### Step 6: Generate JUnit Output for CI/CD

```bash
# Generate JUnit XML for CI integration
velo-test run sql/market_aggregation.sql --spec test_spec.yaml --output junit > test-results.xml
```

The XML file can be consumed by CI systems like Jenkins, GitHub Actions, etc.

---

## Next Steps

### Add More Test Cases

Expand your test spec with edge cases:

```yaml
queries:
  # ... existing queries ...

  - name: market_aggregates_empty_check
    description: Verify behavior with minimal input
    inputs:
      market_data:
        records: 10
    assertions:
      - type: record_count
        operator: between
        min: 1
        max: 5
        message: "Few records should still produce some output"

  - name: market_aggregates_single_symbol
    description: Test with single symbol filter
    inputs:
      market_data:
        records: 500
        # Future: filter support
    assertions:
      - type: record_count
        operator: greater_than
        expected: 0
```

### Add Foreign Key Relationships

If you have multiple related tables:

```yaml
# In orders.schema.yaml
fields:
  - name: customer_id
    type: string
    constraints:
      references:
        schema: customers
        field: id
```

### Use AI-Assisted Generation

```bash
# Let AI generate test spec
velo-test init sql/market_aggregation.sql --ai --output test_spec_ai.yaml

# Let AI infer schemas from sample data
velo-test infer-schema sql/market_aggregation.sql --data-dir data/ --ai --output schemas/
```

---

## Complete Example Files

All files from this tutorial are available in the demo directory:

```
demo/test_harness_examples/getting_started/
├── sql/
│   └── market_aggregation.sql
├── schemas/
│   └── market_data.schema.yaml
├── test_spec.yaml
└── README.md
```

Run the complete example:

```bash
cd demo/test_harness_examples/getting_started
velo-test run sql/market_aggregation.sql --spec test_spec.yaml
```

---

## Troubleshooting

### "Docker not available"

The test harness uses testcontainers to spin up Kafka. Make sure Docker is running:

```bash
docker info
```

### "Timeout waiting for Kafka"

Increase the timeout or check Docker resources:

```yaml
defaults:
  timeout_ms: 60000  # Increase to 60 seconds
```

### "No records produced"

Check that:
1. SQL syntax is valid (`velo-test validate`)
2. Schema generates valid data
3. Topic names match between source and test spec

### "Assertion failed: record_count"

Debug by checking the actual output:

```bash
velo-test run app.sql --spec test_spec.yaml --verbose
```

---

## Further Reading

- [Developer Guide](./DEVELOPER_GUIDE.md) - Complete reference
- [Quick Reference](./QUICK_REFERENCE.md) - Cheat sheet
- [README](./README.md) - Feature overview
- [Demo Apps](../../../demo/test_harness_examples/) - More examples
