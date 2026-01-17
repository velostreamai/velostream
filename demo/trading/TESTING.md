# Testing the Financial Trading Demo

This document describes how to test the `sql/financial_trading.sql` application using the FR-084 SQL Application Test Harness.

## Overview

The test harness provides:
- **Schema-driven data generation** - Realistic market data, positions, and order book events
- **Automated assertions** - Validate output schemas, record counts, and field values
- **Multiple test modes** - Validate, smoke test, full test, stress test
- **CI/CD integration** - JUnit XML and JSON output formats

## Prerequisites

1. **Build velo-test** (from project root):
   ```bash
   cargo build --release
   ```

2. **Docker** - Required for Kafka testcontainers (run mode only)

3. **Test artifacts** (included in this directory):
   - `test_spec.yaml` - Test specification
   - `schemas/*.schema.yaml` - Data generation schemas
   - `data/*.csv` - Reference data files

## Quick Reference

| Command | Description | Docker Required? |
|---------|-------------|------------------|
| `./velo-test.sh validate` | Validate SQL syntax | No |
| `./velo-test.sh` | Run full test suite | Yes |
| `./velo-test.sh smoke` | Quick test (100 records) | Yes |
| `./velo-test.sh stress` | High-volume test (10k records) | Yes |
| `velo-test run ... --keep-containers` | Keep containers for debugging | Yes |

## Test Specification

The `test_spec.yaml` defines tests for all 10 streaming jobs:

| Job # | Name | Description | Key Assertions |
|-------|------|-------------|----------------|
| 1 | `market_data_ts` | Event-time watermark processing | record_count, no_nulls, schema |
| 2 | `tick_buckets` | 1-second tumbling window | aggregation fields present |
| 3 | `enriched_market_data` | Stream-table JOIN | enrichment fields added |
| 4 | `advanced_price_movement_alerts` | Window functions (LAG/LEAD) | price_change_pct calculated |
| 5 | `compliant_market_data` | EXISTS subquery | compliance_status field |
| 6 | `active_hours_market_data` | IN/NOT IN subquery | market_session filtering |
| 7 | `volume_spike_analysis` | Sliding window + circuit breaker | spike_classification |
| 8 | `comprehensive_risk_monitor` | Complex JOIN with risk calc | risk_classification |
| 9 | `order_flow_imbalance_detection` | Order book analysis | buy_ratio, sell_ratio |
| 10 | `arbitrage_opportunities_detection` | Cross-exchange analysis | spread, spread_bps |

## Schema Files

### market_data.schema.yaml

Generates realistic market data events:

```yaml
fields:
  - name: symbol
    constraints:
      enum_values:
        values: [AAPL, GOOGL, MSFT, AMZN, META, NVDA, TSLA]
        weights: [0.20, 0.15, 0.15, 0.15, 0.10, 0.15, 0.10]

  - name: price
    type: decimal
    precision: 4
    constraints:
      range: {min: 50.0, max: 500.0}

  - name: bid_price
    constraints:
      derived: "price * random(0.995, 0.999)"
```

### trading_positions.schema.yaml

Generates position data for risk monitoring:

```yaml
fields:
  - name: trader_id
    constraints:
      enum_values:
        values: [TRADER001, TRADER002, TRADER003, TRADER004, TRADER005]

  - name: position_size
    constraints:
      range: {min: -10000, max: 10000}  # Supports short positions
```

### order_book.schema.yaml

Generates order book update events:

```yaml
fields:
  - name: side
    constraints:
      enum_values:
        values: [BUY, SELL]
        weights: [0.50, 0.50]

  - name: update_type
    constraints:
      enum_values:
        values: [ADD, MODIFY, DELETE, TRADE]
```

## Running Tests

### 1. Validate SQL Syntax

No Docker required - validates SQL parsing and syntax:

```bash
./velo-test.sh validate
```

Output:
```
═══════════════════════════════════════════════════════════════
  FINANCIAL TRADING DEMO - TEST HARNESS
═══════════════════════════════════════════════════════════════

Validating SQL syntax...

✅ SQL validation complete
```

### 2. Run Full Test Suite

Requires Docker for Kafka testcontainers:

```bash
./velo-test.sh
```

Expected output:
```
═══════════════════════════════════════════════════════════════
  VELOSTREAM SQL TEST HARNESS REPORT
═══════════════════════════════════════════════════════════════

SUMMARY
───────
Total Queries:    10
Passed:           10 ✅
Failed:           0  ❌
Duration:         45.2s

RESULTS BY QUERY
────────────────────────────────────────────────────────────────
✅ Query #1: market_data_ts
   Input:  1000 records
   Output: 1000 records
   Assertions: 4/4 passed

✅ Query #2: tick_buckets
   Input:  1000 records (from market_data_ts)
   Output: 48 aggregated buckets
   Assertions: 3/3 passed
...
```

### 3. Run Specific Query Test

Test a single query for faster iteration:

```bash
./velo-test.sh run --query market_data_ts
```

### 4. Output Formats

```bash
# Human-readable text (default)
./velo-test.sh

# JUnit XML for CI/CD
./velo-test.sh run --output junit > results.xml

# JSON for tooling integration
./velo-test.sh run --output json > results.json
```

## Assertion Types Used

| Assertion Type | Example | Description |
|---------------|---------|-------------|
| `record_count` | `equals: 1000` | Validate output record count |
| `schema_contains` | `fields: [symbol, price]` | Required fields present |
| `no_nulls` | `fields: [symbol, timestamp]` | Specified fields have no nulls |
| `execution_time` | `max_ms: 5000` | Performance constraint |

## Test Scenarios

The `test_spec.yaml` includes multiple scenarios:

### Smoke Test
```yaml
scenarios:
  - name: smoke_test
    default_records: 100
    queries: [market_data_ts, tick_buckets]
```

Run with: `./velo-test.sh smoke`

### Standard Test
```yaml
scenarios:
  - name: standard
    default_records: 1000
    queries: all
```

Run with: `./velo-test.sh`

### Stress Test
```yaml
scenarios:
  - name: stress
    default_records: 10000
    timeout_ms: 120000
    queries: [market_data_ts, tick_buckets, volume_spike_analysis]
```

Run with: `./velo-test.sh stress`

## CI/CD Integration

### GitHub Actions Example

```yaml
jobs:
  test-trading-demo:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable

      - name: Build velo-test
        run: cargo build --release

      - name: Run tests
        working-directory: demo/trading
        run: ./velo-test.sh run --output junit > results.xml

      - name: Publish Test Results
        uses: dorny/test-reporter@v1
        if: always()
        with:
          name: Trading Demo Tests
          path: demo/trading/results.xml
          reporter: java-junit
```

## Troubleshooting

### Debugging with Containers

When tests fail and you need to inspect the Kafka state, use `--keep-containers`:

```bash
velo-test run sql/financial_trading.sql --spec test_spec.yaml --keep-containers
```

This keeps the Docker containers running after test completion. You can then:
```bash
# Find the container
docker ps

# Inspect topics
docker exec <container_id> kafka-topics --list --bootstrap-server localhost:9092

# Consume messages from a topic
docker exec <container_id> kafka-console-consumer --bootstrap-server localhost:9092 --topic <topic> --from-beginning

# Cleanup when done
docker stop <container_id>
```

### Docker Not Running

```
Error: Cannot connect to Docker daemon
```

Solution: Start Docker Desktop or Docker daemon:
```bash
# macOS/Windows: Start Docker Desktop
# Linux:
sudo systemctl start docker
```

### velo-test Not Found

```
Error: velo-test not found
```

Solution: Build from project root:
```bash
cd ../..
cargo build --release
cd demo/trading
```

Or set the path explicitly:
```bash
VELO_TEST=../../target/release/velo-test ./velo-test.sh
```

### Timeout Errors

For slow systems, increase timeout:
```bash
./velo-test.sh run --timeout 120000
```

## Customizing Tests

### Add New Assertions

Edit `test_spec.yaml`:

```yaml
queries:
  - name: market_data_ts
    outputs:
      - sink: market_data_ts
        assertions:
          # Add custom assertions
          - type: field_values
            field: price
            operator: greater_than
            value: 0
```

### Modify Schema Generation

Edit `schemas/market_data.schema.yaml`:

```yaml
fields:
  - name: symbol
    constraints:
      enum_values:
        values: [AAPL, GOOGL, MSFT, AMZN]  # Custom stock universe
```

## Related Documentation

- [FR-084 Test Harness README](../../docs/feature/FR-084-app-test-harness/README.md)
- [Test Harness Quick Reference](../../docs/feature/FR-084-app-test-harness/QUICK_REFERENCE.md)
- [Getting Started Guide](../../docs/feature/FR-084-app-test-harness/GETTING_STARTED.md)
