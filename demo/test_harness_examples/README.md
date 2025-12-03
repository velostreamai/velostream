# Test Harness Demo Applications

This directory contains demo SQL applications for testing the Velostream SQL Application Test Harness (FR-084).

## Directory Structure

```
test_harness_examples/
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

## Writing New Demo Apps

1. Create SQL file in appropriate tier directory
2. Create matching `.test.yaml` file with test specification
3. Add any required schemas to `schemas/`
4. Add sample data files to `data/` if needed
