# Test Harness Quick Reference

## Getting Started

```bash
# Interactive quickstart wizard (recommended for new users)
velo-test quickstart app.sql
```

The quickstart wizard guides you through:
1. Validating SQL syntax
2. Finding/generating test specs
3. Setting up schemas
4. Running your first tests

## CLI Commands (Workflow Order)

```bash
# 1. Validate SQL syntax
velo-test validate app.sql

# 2. Generate test spec
velo-test init app.sql                            # Interactive mode
velo-test init app.sql -y                         # Use defaults
velo-test init app.sql --output test_spec.yaml    # Specify output

# 3. Infer schemas from data files
velo-test infer-schema app.sql                    # Auto-discover data/
velo-test infer-schema app.sql --data-dir data/   # Specify directory

# 4. Run tests
velo-test run app.sql                             # Auto-discover spec
velo-test run app.sql --spec test_spec.yaml       # Specify spec
velo-test run app.sql --query my_query            # Run single query

# 5. Debug failures
velo-test debug app.sql                           # Interactive debugger

# 6. Stress test
velo-test stress app.sql                          # Use defaults (100k records)
velo-test stress app.sql --records 1000000        # Custom record count

# 7. Add observability
velo-test annotate app.sql                        # Interactive mode
velo-test annotate app.sql --monitoring ./mon     # Generate monitoring configs

# 8. Generate CI/CD script
velo-test scaffold .                              # Current directory
```

## Non-Interactive Mode (`-y` flag)

All commands support `-y` to skip prompts and use sensible defaults:

```bash
velo-test run app.sql -y           # Auto-discover, no prompts
velo-test init app.sql -y          # Generate with defaults
velo-test stress app.sql -y        # 100k records, 60s duration
velo-test annotate app.sql -y      # Infer app name from filename
```

## Output Formats

```bash
velo-test run app.sql --output json     # JSON output
velo-test run app.sql --output junit    # JUnit XML for CI/CD
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
| `throughput` | `min_records_per_second: 100, max_records_per_second: 10000` |
| `file_exists` | `path: ./out.csv, min_size_bytes: 100` |
| `file_row_count` | `path: ./out.csv, format: csv, equals: 100` |
| `file_contains` | `path: ./out.csv, format: csv, field: symbol, expected_values: [A, B], mode: all` |
| `file_matches` | `actual_path: ./out.csv, expected_path: ./expected.csv, format: csv, ignore_order: true` |

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

## File-Based Source/Sink Types

```yaml
# File input source
inputs:
  - source: trades
    source_type:
      type: file
      path: ./input.csv
      format: csv           # csv, csv_no_header, json_lines, json
      watch: false

# File output sink
output:
  sink_type:
    type: file
    path: ./output.csv
    format: csv
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

# Keep containers running for debugging
velo-test run app.sql --spec test_spec.yaml --keep-containers
# Then use 'docker ps' to find containers, 'docker stop <id>' to cleanup
```

## Project Structure

```
my_app/
├── sql/app.sql
├── schemas/*.schema.yaml
├── data/*.csv
├── configs/*.yaml
└── test_spec.yaml
```

## SQL Annotations (Annotate Command)

The `annotate` command analyzes SQL and generates:
- Annotation templates based on query analysis
- Prometheus/Grafana/Tempo monitoring infrastructure

### Auto-Generated Metrics

| Query Pattern | Metric Type | Example |
|--------------|-------------|---------|
| `COUNT(*)` | counter | `velo_app_query_count` |
| `SUM(field)` | gauge | `velo_app_total_amount` |
| `AVG(field)` | gauge | `velo_app_avg_price` |
| Window + latency field | histogram | `velo_app_latency_seconds` |

### Generated Monitoring Files

```
monitoring/
├── prometheus.yml                        # Scrape configs
├── grafana/
│   ├── dashboards/
│   │   └── app-dashboard.json           # Auto-generated panels
│   └── provisioning/
│       ├── dashboards/dashboard.yml     # Dashboard provisioning
│       └── datasources/
│           ├── prometheus.yml           # Prometheus datasource
│           └── tempo.yml                # Tempo tracing
└── tempo/
    └── tempo.yaml                       # Tracing config
```

### Annotation Categories

```sql
-- @app, @version, @description, @phase

-- Deployment
-- @deployment.node_id, @deployment.node_name, @deployment.region

-- Observability
-- @observability.metrics.enabled, @observability.tracing.enabled
-- @observability.profiling.enabled (off, dev, prod)

-- Job Processing
-- @job_mode (simple, transactional, adaptive)
-- @batch_size, @num_partitions, @partitioning_strategy

-- Metrics
-- @metric, @metric_type (counter, gauge, histogram)
-- @metric_help, @metric_labels, @metric_field, @metric_buckets

-- SLA
-- @sla.latency.p99, @sla.availability
-- @data_retention, @compliance
```
