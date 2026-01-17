# Lesson 5: Generating Test Specifications

Now that you've written SQL queries, let's learn how to generate test specifications automatically.

## The `init` Command

Instead of writing test specs by hand, use `velo-test init`:

```bash
# Generate a test spec for hello_world.sql
../../target/release/velo-test init hello_world.sql -y
```

This creates `hello_world.test.yaml` with:
- Detected query names
- Input/output configurations
- Placeholder assertions

## Example Output

```yaml
# Auto-generated test specification
application: hello_world
description: Auto-generated test spec for hello_world.sql

queries:
  - name: hello_world
    inputs:
      - source: input_data
        source_type:
          type: file
          path: ./hello_world_input.csv
          format: csv
    output:
      sink_type:
        type: file
        path: ./hello_world_output.csv
        format: csv
    assertions:
      - type: file_exists
        path: ./hello_world_output.csv
```

## Try It

```bash
# Generate spec for each example
../../target/release/velo-test init 01_filter.sql -y
../../target/release/velo-test init 02_transform.sql -y

# View the generated specs
cat 01_filter.test.yaml
cat 02_transform.test.yaml
```

## Interactive Mode

Without `-y`, the wizard asks questions:

```bash
../../target/release/velo-test init 03_aggregate.sql

# Prompts:
# - Record count for test data?
# - Add custom assertions?
# - Schema file locations?
```

## Infer Schemas from Data

If you have sample data, infer schemas:

```bash
# Infer schema from CSV
../../target/release/velo-test infer-schema hello_world_input.csv

# Output: hello_world_input.schema.yaml with field types
```

## Next Steps

- [06_generate_runner.md](./06_generate_runner.md) - Generate a runner script
- [07_observability.md](./07_observability.md) - Add monitoring and dashboards
