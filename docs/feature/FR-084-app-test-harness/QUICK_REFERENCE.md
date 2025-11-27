# FR-084: Test Harness Quick Reference

## CLI Commands

```bash
# Run tests
velo-test run app.sql --spec test_spec.yaml

# Run single query
velo-test run app.sql --spec test_spec.yaml --query my_query

# Validate SQL only
velo-test validate app.sql

# Generate test spec
velo-test init app.sql --output test_spec.yaml

# AI-assisted init
velo-test init app.sql --ai --output test_spec.yaml

# Infer schemas
velo-test infer-schema app.sql --data-dir data/ --output schemas/

# Stress test
velo-test stress app.sql --records 100000 --duration 60

# Output formats
velo-test run app.sql --spec test_spec.yaml --output json
velo-test run app.sql --spec test_spec.yaml --output junit > results.xml
```

## Schema Field Types

| Type | Example | Constraints |
|------|---------|-------------|
| `string` | `"AAPL"` | `enum`, `pattern`, `min_length`, `max_length` |
| `integer` | `42` | `min`, `max`, `distribution` |
| `decimal` | `123.45` | `min`, `max`, `precision`, `scale` |
| `float` | `3.14` | `min`, `max` |
| `boolean` | `true` | `probability` |
| `timestamp` | `2024-01-15T10:30:00` | `range`, `start`, `end`, `distribution` |

## Schema Constraints

```yaml
# Enum
constraints:
  enum: [A, B, C]
  weights: [0.5, 0.3, 0.2]  # Optional

# Range
constraints:
  min: 0
  max: 100
  distribution: normal  # uniform, normal, log_normal

# Pattern
constraints:
  pattern: "ORD-[A-Z]{3}-[0-9]{6}"

# Timestamp
constraints:
  range: relative
  start: "-1h"
  end: "now"

# Derived
constraints:
  derived: "price * random(0.99, 1.01)"

# Foreign Key
constraints:
  references:
    schema: customers
    field: id
```

## Assertions

| Type | Example |
|------|---------|
| `record_count` | `operator: equals, expected: 100` |
| `schema_contains` | `fields: [a, b, c]` |
| `no_nulls` | `fields: [a, b]` |
| `field_values` | `field: price, operator: greater_than, value: 0` |
| `field_in_set` | `field: status, values: [A, B, C]` |
| `aggregate_check` | `expression: "SUM(qty)", operator: equals, expected: 1000` |
| `join_coverage` | `left: orders, right: customers, key: id, min_match_rate: 0.9` |
| `execution_time` | `max_ms: 5000, min_ms: 100` |
| `memory_usage` | `max_mb: 100, max_growth_bytes: 52428800` |

## Operators

`equals`, `not_equals`, `greater_than`, `less_than`, `greater_than_or_equal`, `less_than_or_equal`, `between`, `in`

## Test Spec Structure

```yaml
application: my_app
description: Optional description

# Default settings
default_timeout_ms: 30000
default_records: 1000

# Global config overrides (optional)
config:
  bootstrap.servers: "localhost:9092"

queries:
  - name: query_name
    timeout_ms: 60000  # Override default
    inputs:
      - source: source_name
        records: 500           # Generate records
      # OR
      - source: source_name
        from_previous: true    # Chain from previous query
      # OR
      - source: source_name
        from_file: data.csv    # Load from file
    assertions:
      - type: record_count
        greater_than: 0
      - type: schema_contains
        fields: [field1, field2]
      - type: no_nulls
        fields: [field1]
```

## Rust API

```rust
// Infrastructure
let mut infra = TestHarnessInfra::with_kafka("localhost:9092");
infra.start().await?;

// Data generation
let mut gen = SchemaDataGenerator::new(Some(42));
let records = gen.generate(&schema, 1000)?;

// Foreign keys
gen.load_reference_data("table", "field", values);
gen.load_reference_data_from_records("table", "field", &records);

// Schema registry
let id = infra.register_schema("subject", schema_json).await?;
let schema = infra.get_schema(id).await?;

// Cleanup
infra.stop().await?;
```

## Debugging

```bash
# Verbose output
RUST_LOG=debug velo-test run app.sql --spec test_spec.yaml --verbose
```

## File Structure

```
my_app/
├── sql/app.sql
├── schemas/*.schema.yaml
├── data/*.csv
├── configs/*.yaml
└── test_spec.yaml
```
