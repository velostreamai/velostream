# Getting Started with the SQL Test Harness

This tutorial walks you through creating and running your first test for a Velostream SQL application.

## Choose Your Path

| Path | Time | Infrastructure | Best For |
|------|------|----------------|----------|
| **[Quickstart Demo](#path-1-quickstart-no-infrastructure)** | 5 min | None | First-time users, quick validation |
| **[File I/O Testing](#path-2-file-io-no-kafka)** | 15 min | None | CI/CD, offline testing |
| **[Full Kafka Testing](#path-3-full-kafka-testing)** | 30 min | Docker | Production-like testing |

---

## Path 1: Quickstart (No Infrastructure)

The fastest way to see Velostream in action:

```bash
# Build velo-test (one-time)
cargo build --release --bin velo-test

# Run the hello world example
cd demo/quickstart
../../target/release/velo-test run hello_world.sql

# See the output
cat hello_world_output.csv
```

**Next steps:** Work through the progressive examples in `demo/quickstart/` (filter → transform → aggregate → window).

See [LEARNING_PATH.md](./LEARNING_PATH.md) for the full progression.

---

## Path 2: File I/O (No Kafka)

For testing without Kafka infrastructure:

```bash
cd demo/test_harness_examples/file_io
../../target/release/velo-test run passthrough.sql
```

**Best for:** CI/CD pipelines, offline development, quick iteration.

---

## Path 3: Full Kafka Testing

For production-like testing with real Kafka:

```bash
# Requires Docker running
cd demo/test_harness_examples/getting_started
../velo-test.sh .
```

Continue below for the full tutorial.

---

## Quick Start (TL;DR)

If you already have a SQL file and just want to get testing:

```bash
# Interactive wizard - guides you through everything
velo-test quickstart my_app.sql
```

The quickstart wizard will:
1. ✅ Validate your SQL syntax
2. 📁 Find or generate a test spec
3. 📊 Set up schemas (if available)
4. 🧪 Run your first tests

For more control, continue with the full tutorial below.

---

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

> **Note**: Docker-based distribution is planned for a future release. This will allow running `velo-test` without installing Rust or building from source.

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
    'market_data.type' = 'kafka_source',
    'market_data.topic' = 'market_data_input',
    'market_data.format' = 'json',

    'market_aggregates.type' = 'kafka_sink',
    'market_aggregates.topic' = 'market_aggregates_output',
    'market_aggregates.format' = 'json'
);
```

> **Note**: Use `CREATE TABLE` (CTAS) for queries with `GROUP BY` that produce materialized aggregations. Use `CREATE STREAM` (CSAS) for continuous transformations without aggregation.

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
      - type: record_count
        operator: greater_than
        expected: 0
        message: "Aggregation should produce at least one output"

      - type: schema_contains
        fields:
          - symbol
          - trade_count
          - total_volume
          - avg_price
        message: "Output must contain all aggregation fields"

      - type: no_nulls
        fields: [symbol, trade_count, avg_price]
        message: "Key fields should never be null"

      - type: field_values
        field: trade_count
        operator: greater_than
        value: 0
        message: "Trade count should be positive"

      - type: execution_time
        max_ms: 10000
        message: "Query should complete within 10 seconds"
```

### Step 5: Run the Tests

#### Option A: Validate SQL First (No Kafka Required)

```bash
velo-test validate sql/market_aggregation.sql
```

Expected output:
```
SQL validation passed: sql/market_aggregation.sql
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

### Step 6: Generate JUnit Output for CI/CD

```bash
velo-test run sql/market_aggregation.sql --spec test_spec.yaml --output junit > test-results.xml
```

## Next Steps

- [User Guide](./USER_GUIDE.md) - Complete reference for all features
- [CLI Reference](./CLI_REFERENCE.md) - All CLI commands including `scaffold`
- [Assertions Reference](./ASSERTIONS.md) - All assertion types
- [Schema Reference](./SCHEMAS.md) - Schema definition format
- [Quick Reference](./QUICK_REFERENCE.md) - Cheat sheet

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

### Inspect Kafka State

Keep containers running after tests to inspect Kafka state:

```bash
velo-test run app.sql --spec test_spec.yaml --keep-containers
```

Then use Docker to inspect:
```bash
docker ps                                  # Find container ID
docker exec <id> kafka-topics --list ...   # List topics
docker stop <id>                           # Cleanup when done
```

## Demo Examples

Complete working examples are in `demo/test_harness_examples/`:

```bash
cd demo/test_harness_examples/getting_started
../../../target/release/velo-test run ./sql/market_aggregation.sql --spec ./test_spec.yaml
```

## Generate a Runner Script

Once your project is set up, generate a `velo-test.sh` runner script to simplify running tests:

```bash
# Generate runner script for your project
velo-test scaffold my_first_test/

# The generated script provides convenient commands:
cd my_first_test
./velo-test.sh                    # Run tests
./velo-test.sh validate           # Validate SQL only
./velo-test.sh --help             # Show all options
```

The scaffold command automatically detects your project structure and generates an appropriate script:

| Project Structure | Generated Script Style |
|-------------------|------------------------|
| `apps/*.sql` + `tests/*.yaml` | App-based runner (select by app name) |
| `tier*_*/` directories | Tiered runner (run by tier) |
| Single SQL file | Minimal runner |

See the [CLI Reference](./CLI_REFERENCE.md#velo-test-scaffold) for all scaffold options.
