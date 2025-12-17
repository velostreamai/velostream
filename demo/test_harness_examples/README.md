# Test Harness Demo Applications

This directory contains demo SQL applications for testing the Velostream SQL Application Test Harness (FR-084).

## Directory Structure

```
test_harness_examples/
├── file_io/              # File-based I/O demo (no Kafka required!)
├── getting_started/      # Simple examples to get started
├── tier1_basic/          # Basic SQL operations (SELECT, WHERE, CAST)
├── tier2_aggregations/   # Aggregation functions and windows
├── tier3_joins/          # Stream-table and stream-stream joins
├── tier4_window_functions/ # LAG, LEAD, ROW_NUMBER, running aggregates
├── tier5_complex/        # Multi-stage pipelines, subqueries, CASE
├── tier6_edge_cases/     # Null handling, empty input, large volume
├── schemas/              # Shared schema definitions
├── configs/              # Shared Kafka configuration templates
└── data/                 # Sample CSV data files for table sources
```

## Quick Start: File I/O Demo (No Kafka Required!)

The fastest way to try the test harness - pure file-based testing:

```bash
cd demo/test_harness_examples/file_io

# Run the demo
velo-test run passthrough.sql --spec test_spec.yaml
```

This reads from `input_trades.csv`, processes through SQL, and writes to `output_trades.csv`.

## Quick Start with velo-test.sh

The `velo-test.sh` script provides an easy way to run tests:

```bash
# Run all tiers (auto-starts Kafka via Docker)
./velo-test.sh

# Validate SQL syntax only (no Docker needed)
./velo-test.sh validate

# Run specific tier
./velo-test.sh tier1
./velo-test.sh getting_started
./velo-test.sh file_io

# From a subdirectory (e.g., getting_started/)
../velo-test.sh .
```

## Direct velo-test Usage

### Validate SQL syntax
```bash
velo-test validate tier1_basic/01_passthrough.sql
```

### Generate test specification
```bash
velo-test init tier1_basic/01_passthrough.sql --output tier1_basic/01_passthrough.test.yaml
```

### Infer schemas from SQL
```bash
velo-test infer-schema tier1_basic/01_passthrough.sql --output schemas/
```

### Run tests (requires Kafka)
```bash
velo-test run tier1_basic/01_passthrough.sql --spec tier1_basic/01_passthrough.test.yaml --schemas schemas/
```

## Tier Overview

| Tier | Focus | Apps | Complexity |
|------|-------|------|------------|
| 1 | Basic Operations | 4 | Simple SELECT, WHERE, CAST |
| 2 | Aggregations | 5 | COUNT, SUM, AVG, Windows |
| 3 | Joins | 3 | Stream-table, stream-stream |
| 4 | Window Functions | 4 | LAG, LEAD, ROW_NUMBER |
| 5 | Complex Patterns | 4 | Pipelines, CASE, subqueries |
| 6 | Edge Cases | 4 | Nulls, empty, large volume |

## Common Schemas

All demos use schemas from the `schemas/` directory:
- `simple_record.schema.yaml` - Basic record with id, value, timestamp
- `market_data.schema.yaml` - Financial market data (symbol, price, volume)
- `order_event.schema.yaml` - E-commerce order events
- `user_activity.schema.yaml` - User activity events

## Step-by-Step Debugging

The test harness supports step-by-step execution for debugging complex SQL pipelines.

### Step Mode (--step flag)

Execute statements one at a time with the `--step` flag:

```bash
# Step through each statement interactively
velo-test run tier5_complex/40_pipeline.sql --step

# Commands:
#   [Enter] - Execute next statement
#   r       - Run all remaining statements
#   q       - Quit
```

### Interactive Debug Mode

For more control, use the `debug` subcommand:

```bash
# Interactive debugger with breakpoints
velo-test debug tier5_complex/40_pipeline.sql --breakpoint regional_summary

# Commands:
#   s, step        - Execute next statement
#   c, continue    - Run until next breakpoint
#   r, run         - Run all remaining statements
#   b <name>       - Set breakpoint on statement
#   u <name>       - Remove breakpoint
#   cb, clear      - Clear all breakpoints
#   l, list        - List all statements
#   i <name>       - Inspect output from statement
#   ia, inspect-all - Inspect all captured outputs
#   hi, history    - Show command history
#   st, status     - Show current state
#   q, quit        - Exit debugger
```

### Example: Debugging the Pipeline Demo

The `tier5_complex/40_pipeline.sql` has 3 stages - ideal for step debugging:

```bash
cd demo/test_harness_examples

# 1. First validate the SQL
../velo-test.sh validate tier5_complex/40_pipeline.sql

# 2. Debug with step mode
velo-test debug tier5_complex/40_pipeline.sql

# Output:
# 🐛 Velostream SQL Debugger
# ════════════════════════════════════════
# SQL File: tier5_complex/40_pipeline.sql
#
# 📝 SQL Statements:
#    [1] cleaned_transactions (CREATE STREAM)
#    [2] regional_summary (CREATE TABLE)
#    [3] flagged_regions (CREATE STREAM)
#
# ▶️  Ready to execute [1/3] cleaned_transactions
# (debug) s
#    ✅ cleaned_transactions completed in 245ms
#       Output: 10 records to cleaned_transactions
```

### Setting Breakpoints

Set breakpoints on specific queries:

```bash
# Break at the aggregation stage
velo-test debug tier5_complex/40_pipeline.sql -b regional_summary

# The debugger will run until it hits 'regional_summary'
```

## Writing New Demo Apps

1. Create SQL file in appropriate tier directory
2. Create matching `.test.yaml` file with test specification
3. Add any required schemas to `schemas/`
4. Add sample data files to `data/` if needed
