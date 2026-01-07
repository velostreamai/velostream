# Velostream Quickstart

Run your first streaming SQL query in 5 minutes. **No Docker. No Kafka.**

## Prerequisites

- Rust toolchain (for building velo-test)
- That's it!

## Quick Start

### 1. Build velo-test (one-time, ~2 minutes)

```bash
cd /path/to/velostream
cargo build --release --bin velo-test
```

### 2. Run hello world

```bash
cd demo/quickstart
../../target/release/velo-test run hello_world.sql -y
```

### 3. Check the output

```bash
cat output/hello_world_output.csv
```

You should see:
```csv
id,name,timestamp,value
1,Alice,2024-01-01T10:00:00,100
2,Bob,2024-01-01T10:01:00,200
3,Carol,2024-01-01T10:02:00,150
4,Dave,2024-01-01T10:03:00,300
5,Eve,2024-01-01T10:04:00,250
```

**Congratulations!** You just ran a streaming SQL query.

## What Just Happened?

Velostream read `hello_world_input.csv`, processed it through your SQL query, and wrote the results to `output/hello_world_output.csv`. No Kafka, no Docker, no complex setup.

## Progressive Examples

Work through these in order. Each adds ONE new concept:

### SQL Basics (Run actual queries)

| File | Concept | What You'll Learn |
|------|---------|-------------------|
| `hello_world.sql` | Passthrough | CREATE STREAM, SELECT *, EMIT CHANGES |
| `01_filter.sql` | Filtering | WHERE clause, comparison operators |
| `02_transform.sql` | Transformations | Column aliases, calculations, UPPER() |
| `03_aggregate.sql` | Aggregations | COUNT, SUM, AVG, GROUP BY, CREATE TABLE |
| `04_window.sql` | Window Functions | LAG, OVER clause, PARTITION BY |

### Smart Commands (Generate artifacts)

| File | Concept | What You'll Learn |
|------|---------|-------------------|
| [05_generate_test_spec.md](./05_generate_test_spec.md) | Test Specs | `velo-test init`, `velo-test infer-schema` |
| [06_generate_runner.md](./06_generate_runner.md) | Runner Scripts | `velo-test scaffold` |
| [07_observability.md](./07_observability.md) | Monitoring | `velo-test annotate`, Prometheus, Grafana |
| [08_deploy.md](./08_deploy.md) | Deployment | `velo-sql deploy-app`, `velo-cli` |

### Run Each Example

```bash
# Filter: only rows where value > 150
../../target/release/velo-test run 01_filter.sql -y
cat output/01_filter_output.csv

# Transform: add calculated columns
../../target/release/velo-test run 02_transform.sql -y
cat output/02_transform_output.csv

# Aggregate: count and sum by category
../../target/release/velo-test run 03_aggregate.sql -y
cat output/03_aggregate_output.csv

# Window: compare each row to previous value
../../target/release/velo-test run 04_window.sql -y
cat output/04_window_output.csv
```

### Run All Examples

```bash
./velo-test.sh
```

## Understanding the SQL

Open any `.sql` file to see detailed comments explaining every line:

```sql
-- Example from hello_world.sql:
CREATE STREAM hello_world AS
SELECT *                          -- Select all columns
FROM input_data                   -- Read from source
EMIT CHANGES                      -- Output each row as it arrives
WITH (
    'input_data.type' = 'file_source',  -- Read from file
    'input_data.path' = './hello_world_input.csv',
    ...
);
```

## Next Steps

Once you're comfortable with these examples:

1. **File I/O Patterns** → [`../test_harness_examples/file_io/`](../test_harness_examples/file_io/)
2. **Getting Started with Kafka** → [`../test_harness_examples/getting_started/`](../test_harness_examples/getting_started/)
3. **SQL Tiers (basic → advanced)** → [`../test_harness_examples/tier1_basic/`](../test_harness_examples/tier1_basic/) through `tier6_edge_cases/`
4. **Production Demo** → [`../trading/`](../trading/)

See the [Learning Path](../../docs/test-harness/LEARNING_PATH.md) for a structured progression.

## Troubleshooting

### "command not found: velo-test"
Build it first: `cargo build --release --bin velo-test`

### "file not found" error
Make sure you're in the `demo/quickstart/` directory when running.

### Need help?
See [docs/test-harness/GETTING_STARTED.md](../../docs/test-harness/GETTING_STARTED.md)
