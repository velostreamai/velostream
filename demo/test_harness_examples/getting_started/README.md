# Getting Started Demo

This is a complete, runnable example for the FR-084 SQL Application Test Harness.

## Prerequisites

**Currently requires building from source:**

```bash
# From the velostream root directory
cargo build --release

# Add to PATH (or use full path: ./target/release/velo-test)
export PATH="$PATH:$(pwd)/target/release"
```

> **Docker support planned**: A `velostream/velo-test` Docker image will be available in a future release.

## Quick Start

### Using velo-test.sh (Recommended)

```bash
# From test_harness_examples directory
./velo-test.sh getting_started

# Or from this directory (getting_started/)
../velo-test.sh .

# Validate only (no Docker required)
../velo-test.sh validate .
```

### Using velo-test.sh Directly

```bash
# From this directory (getting_started/)

# 1. Validate SQL syntax (no Kafka required)
../velo-test.sh validate .

# 2. Run full tests (requires Docker for Kafka)
../velo-test.sh .

# 3. Run with custom timeout
../velo-test.sh . --timeout 60000

# 4. Output in JSON format
../velo-test.sh . --output json
```

## Files

| File | Description |
|------|-------------|
| `sql/market_aggregation.sql` | Main demo - aggregates market data into OHLCV bars |
| `sql/simple_passthrough.sql` | Simple passthrough for basic pipeline testing |
| `schemas/market_data.schema.yaml` | Schema for generating test data |
| `test_spec.yaml` | Test specification for market_aggregation.sql |

> **Note**: The `test_spec.yaml` is configured for `market_aggregation.sql`. When using `velo-test.sh`, it will use the first SQL file found in the directory.

## What This Demo Tests

1. **Basic Aggregation** - 1000 records, validates output schema and values
2. **High Volume** - 5000 records, checks memory usage stays reasonable
3. **Minimal Input** - 10 records, verifies behavior with small datasets

## Tutorial

For a detailed walkthrough of creating this demo from scratch, see:
[Getting Started Guide](../../../docs/feature/FR-084-app-test-harness/GETTING_STARTED.md)

## Expected Output

```
═══════════════════════════════════════════════════════════════════
  VELOSTREAM SQL TEST HARNESS REPORT
═══════════════════════════════════════════════════════════════════

SUMMARY
───────
Total Queries:    3
Passed:           3 ✅
Failed:           0 ❌
Duration:         4.5s

RESULTS BY QUERY
────────────────────────────────────────────────────────────────────
✅ Query #1: market_aggregates
   Input:  1000 records
   Output: 5 aggregated records
   Assertions: 6/6 passed

✅ Query #2: market_aggregates_high_volume
   Input:  5000 records
   Output: 5 aggregated records
   Memory: 45 MB peak
   Assertions: 3/3 passed

✅ Query #3: market_aggregates_minimal
   Input:  10 records
   Output: 3 aggregated records
   Assertions: 2/2 passed
═══════════════════════════════════════════════════════════════════
```
