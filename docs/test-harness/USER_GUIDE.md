# User Guide

Complete guide to using the Velostream SQL Application Test Harness.

## Quick Start

```bash
# New users: Use the interactive quickstart wizard
velo-test quickstart my_app.sql
```

The quickstart walks you through validation, spec generation, and running tests.

## Overview

The test harness provides a comprehensive framework for testing streaming SQL applications:

1. **Schema-driven data generation** - Generate realistic test data from schema definitions
2. **Declarative test specifications** - Define tests in YAML without writing code
3. **Rich assertions** - Validate output records, performance, and data quality
4. **Multiple output formats** - Text, JSON, and JUnit XML for CI/CD integration
5. **Infrastructure automation** - Automatic Kafka setup via testcontainers
6. **Interactive & non-interactive modes** - All commands support `-y` for CI/CD

## Project Setup

### Directory Structure

```
my_sql_app/
├── sql/
│   └── app.sql              # Your SQL application
├── schemas/
│   ├── orders.schema.yaml   # Input schema definitions
│   └── customers.schema.yaml
├── data/
│   └── sample_orders.csv    # Optional static test data
├── expected/
│   └── expected_output.csv  # Expected results for comparison
└── test_spec.yaml           # Test specification
```

### SQL Application

Your SQL application should use the Velostream SQL syntax:

```sql
-- Aggregation query (use CREATE TABLE)
CREATE TABLE order_stats AS
SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id
WINDOW TUMBLING(INTERVAL '1' HOUR)
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic' = 'orders_input',
    'orders.format' = 'json',

    'order_stats.type' = 'kafka_sink',
    'order_stats.topic' = 'order_stats_output',
    'order_stats.format' = 'json'
);
```

## Schema Definitions

Schemas define the structure and constraints for generating test data.

### Basic Schema

```yaml
schema:
  name: orders
  namespace: ecommerce

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

  - name: amount
    type: decimal
    precision: 19
    scale: 2
    constraints:
      min: 10.00
      max: 10000.00
      distribution: log_normal

  - name: status
    type: string
    constraints:
      enum: [PENDING, CONFIRMED, SHIPPED, DELIVERED]
      weights: [0.3, 0.3, 0.25, 0.15]

  - name: order_time
    type: timestamp
    constraints:
      range: relative
      start: "-24h"
      end: "now"
```

See [Schema Reference](./SCHEMAS.md) for complete documentation.

## Test Specifications

Test specs define what to test and how to validate results.

### Basic Test Spec

```yaml
application: order_processor
description: Tests for order processing pipeline

default_timeout_ms: 30000
default_records: 1000

schemas:
  orders: schemas/orders.schema.yaml
  customers: schemas/customers.schema.yaml

queries:
  - name: order_stats
    description: Test order aggregation
    inputs:
      orders:
        records: 1000
    assertions:
      - type: record_count
        operator: greater_than
        expected: 0
      - type: schema_contains
        fields: [customer_id, order_count, total_amount]
      - type: no_nulls
        fields: [customer_id, order_count]
```

### Multi-Query Tests

Test multiple queries with dependencies:

```yaml
queries:
  # First query generates base data
  - name: enriched_orders
    inputs:
      orders:
        records: 500
      customers:
        records: 100
    assertions:
      - type: record_count
        greater_than: 0

  # Second query uses output from first
  - name: customer_summary
    inputs:
      enriched_orders:
        from_previous: true  # Use output from enriched_orders
    assertions:
      - type: record_count
        greater_than: 0
```

### Explicit Dependencies

When a query requires reference tables or other SQL statements to be deployed first, use the `dependencies` field:

```yaml
queries:
  - name: vip_orders
    description: Filter orders by VIP customers
    dependencies:
      - vip_customers  # This CREATE TABLE must be deployed first
    inputs:
      - source: all_orders
        schema: order_event
        records: 200
    assertions:
      - type: record_count
        greater_than: 0
        less_than: 200  # Should filter out non-VIP orders
```

**How dependencies work:**

1. Dependencies are deployed via `StreamJobServer` before the main query
2. Dependencies are listed by name (matching the SQL statement name)
3. Each dependency is only deployed once (duplicates are skipped)
4. Dependencies execute in the order declared

**When to use dependencies:**

| Use Case | Example |
|----------|---------|
| Reference tables for JOINs | `dependencies: [products, customers]` |
| Subquery reference tables | `dependencies: [vip_customers]` |
| Prerequisite streams | `dependencies: [enriched_stream]` |

### File-Based Reference Tables

Load reference tables from CSV/JSON files for use in SQL JOINs and subqueries.

**Step 1: Create a config file for the file source**

```yaml
# configs/customers_table.yaml
source_type: file
file:
  path: ../data/vip_customers.csv
  format: csv
```

**Step 2: Define the CREATE TABLE in SQL**

```sql
-- Load VIP customers from CSV into a reference table
CREATE TABLE vip_customers AS
SELECT customer_id, name, tier, credit_limit
FROM vip_source
WITH (
    'vip_source.config_file' = '../configs/customers_table.yaml'
);
```

**Step 3: Use the table in your main query**

```sql
-- Filter orders using IN subquery against reference table
CREATE STREAM vip_orders AS
SELECT o.order_id, o.customer_id, o.product_id,
       o.quantity * o.unit_price AS order_total,
       o.status
FROM all_orders o
WHERE o.customer_id IN (
    SELECT customer_id FROM vip_customers WHERE tier IN ('gold', 'platinum')
)
WITH (
    'all_orders.type' = 'kafka_source',
    'all_orders.topic' = 'orders',
    'vip_orders.type' = 'kafka_sink',
    'vip_orders.topic' = 'vip_orders'
);
```

**Step 4: Declare the dependency in test spec**

```yaml
queries:
  - name: vip_orders
    dependencies:
      - vip_customers  # Reference table must be loaded first
    inputs:
      - source: all_orders
        schema: order_event
        records: 200
    assertions:
      - type: record_count
        greater_than: 0
```

**Supported file formats:**

| Format | Config Value | Description |
|--------|--------------|-------------|
| CSV | `format: csv` | Comma-separated with header row |
| JSON Lines | `format: json_lines` | One JSON object per line |
| JSON | `format: json` | JSON array of objects |

### Using Static Data

Use pre-existing data files instead of generated data:

```yaml
queries:
  - name: regression_test
    inputs:
      orders:
        from_file: data/regression_orders.csv
        format: csv
    assertions:
      - type: file_matches
        actual_path: ./output/results.csv
        expected_path: expected/expected_results.csv
        format: csv
        ignore_order: true
```

### Performance Tests

Include performance constraints:

```yaml
queries:
  - name: high_volume_test
    inputs:
      orders:
        records: 10000
    assertions:
      - type: record_count
        greater_than: 0

      - type: execution_time
        max_ms: 30000
        message: "Should complete within 30 seconds"

      - type: memory_usage
        max_mb: 200
        message: "Memory should stay under 200MB"

      - type: throughput
        min_records_per_second: 100
        message: "Should process at least 100 records/sec"
```

## Running Tests

### Basic Execution

```bash
# Validate SQL first
velo-test validate sql/app.sql

# Run all tests
velo-test run sql/app.sql --spec test_spec.yaml

# Run specific query
velo-test run sql/app.sql --spec test_spec.yaml --query order_stats
```

### Output Formats

```bash
# Human-readable text (default)
velo-test run sql/app.sql --spec test_spec.yaml

# JSON for programmatic processing
velo-test run sql/app.sql --spec test_spec.yaml --output json > results.json

# JUnit XML for CI/CD
velo-test run sql/app.sql --spec test_spec.yaml --output junit > results.xml
```

### Debugging

```bash
# Verbose output
velo-test run sql/app.sql --spec test_spec.yaml --verbose

# Debug logging
RUST_LOG=debug velo-test run sql/app.sql --spec test_spec.yaml

# Specific module debugging
RUST_LOG=velostream::test_harness=debug velo-test run sql/app.sql --spec test_spec.yaml
```

## Assertion Types

### Record Assertions

```yaml
# Count validation
- type: record_count
  operator: equals
  expected: 100

# Schema validation
- type: schema_contains
  fields: [id, name, value]

# Null checking
- type: no_nulls
  fields: [id, name]
```

### Value Assertions

```yaml
# Field value range
- type: field_values
  field: price
  operator: between
  min: 0
  max: 1000

# Allowed values
- type: field_in_set
  field: status
  values: [ACTIVE, INACTIVE, PENDING]

# Unique values
- type: unique_values
  field: order_id
```

### Aggregate Assertions

```yaml
# Sum validation
- type: aggregate_check
  expression: "SUM(amount)"
  operator: greater_than
  expected: 0

# Average validation
- type: aggregate_check
  expression: "AVG(quantity)"
  operator: between
  min: 1
  max: 100
```

### Performance Assertions

```yaml
# Execution time
- type: execution_time
  max_ms: 5000

# Memory usage
- type: memory_usage
  max_mb: 100

# Throughput rate
- type: throughput
  min_records_per_second: 100
```

### File Assertions

```yaml
# File existence
- type: file_exists
  path: ./output/results.csv
  min_size_bytes: 100

# Row count
- type: file_row_count
  path: ./output/results.csv
  format: csv
  equals: 100

# Content comparison
- type: file_matches
  actual_path: ./output/results.csv
  expected_path: ./expected/results.csv
  format: csv
  ignore_order: true
```

See [Assertions Reference](./ASSERTIONS.md) for complete documentation.

## File-Based Testing

Test without Kafka infrastructure using file-based sources/sinks.

### File Source Configuration

```yaml
queries:
  - name: file_based_test
    inputs:
      trades:
        source_type:
          type: file
          path: ./data/trades.csv
          format: csv
    output:
      sink_type:
        type: file
        path: ./output/results.csv
        format: csv
    assertions:
      - type: file_exists
        path: ./output/results.csv
```

### Supported Formats

| Format | Description |
|--------|-------------|
| `csv` | CSV with header row |
| `csv_no_header` | CSV without header |
| `json_lines` | JSON Lines (one object per line) |
| `json` | JSON array of objects |

## Advanced Features

### Foreign Key Relationships

Define relationships between schemas:

```yaml
# orders.schema.yaml
fields:
  - name: customer_id
    type: string
    constraints:
      references:
        schema: customers
        field: id
```

The generator automatically ensures referential integrity.

### AI-Assisted Features

Requires `ANTHROPIC_API_KEY` environment variable.

```bash
# Set your API key
export ANTHROPIC_API_KEY="your-key-here"

# Generate test spec with AI suggestions
velo-test init sql/app.sql --ai --output test_spec.yaml

# Infer schemas with AI
velo-test infer-schema sql/app.sql --data-dir ./data --ai --output ./schemas
```

### Stress Testing

```bash
# High-volume test
velo-test stress sql/app.sql --records 100000 --duration 60

# Rate-limited test (WIP - not yet implemented)
# velo-test stress sql/app.sql --rate 10000 --duration 300
```

### Configuration Overrides

Override settings at runtime:

```yaml
# In test_spec.yaml
config:
  bootstrap.servers: "${KAFKA_BOOTSTRAP:localhost:9092}"
  topic.prefix: "test_"
```

```bash
# Override via CLI
velo-test run sql/app.sql --spec test_spec.yaml --bootstrap-servers kafka:9092
```

## CI/CD Integration

### GitHub Actions

```yaml
name: SQL Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      kafka:
        image: confluentinc/cp-kafka:latest
        ports:
          - 9092:9092

    steps:
      - uses: actions/checkout@v3

      - name: Build test harness
        run: cargo build --release

      - name: Run SQL tests
        run: |
          ./target/release/velo-test run sql/app.sql \
            --spec test_spec.yaml \
            --output junit > test-results.xml

      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: test-results
          path: test-results.xml
```

### Jenkins

```groovy
pipeline {
    agent any

    stages {
        stage('Test') {
            steps {
                sh 'cargo build --release'
                sh './target/release/velo-test run sql/app.sql --spec test_spec.yaml --output junit > test-results.xml'
            }
        }
    }

    post {
        always {
            junit 'test-results.xml'
        }
    }
}
```

## Best Practices

### Schema Design

1. **Use realistic ranges** - Match production data distributions
2. **Add weights to enums** - Reflect real-world frequencies
3. **Use foreign keys** - Ensure referential integrity
4. **Document fields** - Add descriptions for clarity

### Test Design

1. **Start simple** - Begin with basic record count and schema assertions
2. **Add edge cases** - Test empty inputs, single records, high volumes
3. **Include performance** - Set time and memory constraints
4. **Use meaningful messages** - Make failures easy to diagnose

### Organization

1. **Separate concerns** - Keep schemas, SQL, and specs in separate directories
2. **Version control** - Track test specs alongside SQL code
3. **Seed for reproducibility** - Use fixed seeds for deterministic tests
4. **Document dependencies** - Note which schemas depend on others

## Troubleshooting

See [Getting Started - Troubleshooting](./GETTING_STARTED.md#troubleshooting) for common issues.

### Common Issues

| Issue | Solution |
|-------|----------|
| "Docker not available" | Ensure Docker is running |
| "Timeout waiting for Kafka" | Increase timeout or check Docker resources |
| "No records produced" | Verify SQL syntax and topic names |
| "Schema not found" | Check schema file paths in test spec |

### Getting Help

- Debug logging: `RUST_LOG=debug velo-test run ...`
- Validate SQL: `velo-test validate sql/app.sql`
- Check schemas: Ensure YAML is valid and fields match SQL sources
