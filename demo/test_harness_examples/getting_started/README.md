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

| File | Purpose | How to Run |
|------|---------|------------|
| `sql/market_aggregation.sql` | Automated testing demo | `test_spec.yaml` |
| `sql/debug_demo.sql` | Interactive debugging demo | `velo-test debug` |
| `sql/simple_passthrough.sql` | Simple passthrough example | Run without spec |
| `schemas/market_data.schema.yaml` | Schema for generating test data | |

### Using velo-test.sh Interactive Mode

```bash
# Interactive mode - select SQL file, then action
../velo-test.sh cases .

# Example flow for debug_demo.sql:
# Step 1: Select SQL file to run:
#   1) sql/debug_demo.sql
#   2) sql/market_aggregation.sql
# Enter SQL file [1-3]: 1
#
# No matching spec file for debug_demo.sql
#   1) Run without assertions (just execute SQL)
#   2) Debug interactively (step-by-step)    ← Select this!
# Choose option [1-2, 0]: 2
# Starting interactive debugger...
```

### Direct velo-test Commands

```bash
# Market aggregation - automated testing with spec
velo-test run sql/market_aggregation.sql --spec test_spec.yaml --schemas schemas/

# Debug demo - interactive step-by-step debugging
velo-test debug sql/debug_demo.sql

# Simple passthrough - just run without assertions
velo-test run sql/simple_passthrough.sql --schemas schemas/
```

## Step-by-Step Debugging Demo

The `sql/debug_demo.sql` is a 3-stage pipeline perfect for learning the debugger:

```bash
# Validate the demo
velo-test validate sql/debug_demo.sql

# Debug interactively (requires Docker for Kafka)
velo-test debug sql/debug_demo.sql

# Set a breakpoint on the aggregation stage
velo-test debug sql/debug_demo.sql -b symbol_aggregates
```

**Debug Commands:**

*Execution Control:*
- `s` / `step` - Execute next statement
- `c` / `continue` - Run until next breakpoint
- `r` / `run` - Run all remaining statements
- `b <N>` / `break` - Set breakpoint on statement N
- `u <N>` / `unbreak` - Remove breakpoint
- `cb` / `clear` - Clear all breakpoints
- `q` / `quit` - Exit debugger

*State Inspection:*
- `l` / `list` - List all statements
- `i <N>` / `inspect` - Inspect output from statement N
- `ia` / `inspect-all` - Inspect all captured outputs
- `hi` / `history` - Show command history
- `st` / `status` - Show current state

*Infrastructure:*
- `topics` - List all Kafka topics (numbered for quick access)
- `consumers` - List consumer groups with lag
- `jobs` - List jobs with source/sink details
- `schema <topic>` - Show inferred schema for topic

*Data Visibility:*
- `messages <topic|N>` - Peek topic messages (use number from `topics`)
- `messages <N> --last 5` - Show last 5 messages
- `head <stmt> [-n N]` - Show first N records (default: 10)
- `tail <stmt> [-n N]` - Show last N records (default: 10)
- `filter <stmt> <expr>` - Filter records (e.g., `filter 1 status=FAILED`)
- `export <stmt> <file>` - Export records to JSON/CSV

**Example session:**
```
🐛 Velostream SQL Debugger
════════════════════════════════════════
SQL File: sql/debug_demo.sql

📝 SQL Statements:
   [1] high_value_trades (CREATE STREAM)
   [2] symbol_aggregates (CREATE TABLE)
   [3] flagged_symbols (CREATE STREAM)

▶️  Ready to execute [1/3] high_value_trades
(debug) s
   ✅ high_value_trades completed in 156ms
      Output: 8 records to high_value_trades

(debug) topics
   📋 Topics (1):
   [1] test_abc_market_data [test] (50 messages)
     └─ P0: 50 msgs (offsets: 0..50) [last: key="AAPL", @14:30:45]

(debug) messages 1 --last 2
📨 2 messages:
  [1] P0:offset 48
      timestamp: 2025-01-15 14:30:44.123
      key: AAPL
      value: {"symbol": "AAPL", "price": 178.50, "quantity": 100}

(debug) s
   ✅ symbol_aggregates completed in 203ms
      Output: 3 records to symbol_aggregates

(debug) head 2 -n 3
📊 First (3 of 3 records) from 'symbol_aggregates':
  [1] {"symbol":"AAPL","total_value":17850.00,"count":10}
  [2] {"symbol":"GOOGL","total_value":14230.00,"count":8}
  [3] {"symbol":"MSFT","total_value":21260.00,"count":5}

(debug) filter 2 symbol=AAPL
🔍 Filtered 'symbol_aggregates' where symbol=AAPL:
   Found 1 of 3 records matching
  [1] {"symbol":"AAPL","total_value":17850.00,"count":10}
```

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
