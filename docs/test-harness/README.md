# SQL Application Test Harness

The Velostream SQL Application Test Harness (`velo-test`) is a comprehensive testing framework for validating streaming SQL applications before deployment.

## 5-Minute Quickstart (No Docker Required!)

```bash
# Build velo-test (one-time)
cargo build --release --bin velo-test

# Run your first streaming SQL
cd demo/quickstart
../../target/release/velo-test run hello_world.sql

# Check the output
cat hello_world_output.csv
```

See [demo/quickstart/](../../demo/quickstart/) for progressive examples or [LEARNING_PATH.md](./LEARNING_PATH.md) for the full learning progression.

## Overview

The test harness enables you to:

- **Validate SQL syntax** without running against real infrastructure
- **Generate realistic test data** from schema definitions
- **Run automated tests** with declarative assertions
- **Measure performance** with stress testing
- **Integrate with CI/CD** via JUnit XML output

## Documentation

| Document | Description |
|----------|-------------|
| [Learning Path](./LEARNING_PATH.md) | Structured progression from beginner to advanced |
| [Getting Started](./GETTING_STARTED.md) | Step-by-step tutorial for your first test |
| [Developer Guide](./DEVELOPER_GUIDE.md) | Comprehensive guide with examples |
| [User Guide](./USER_GUIDE.md) | Complete reference for all features |
| [Quick Reference](./QUICK_REFERENCE.md) | Cheat sheet for common operations |
| [Assertions Reference](./ASSERTIONS.md) | All assertion types and options |
| [Schema Reference](./SCHEMAS.md) | Schema definition format and constraints |
| [CLI Reference](./CLI_REFERENCE.md) | All CLI commands and options |
| [AI Features](./AI_FEATURES.md) | AI-powered schema inference, test generation, and failure analysis |

## Quick Start

```bash
# Interactive quickstart wizard (recommended for new users)
velo-test quickstart my_app.sql

# Or step-by-step:
velo-test validate my_app.sql              # 1. Check syntax
velo-test init my_app.sql                  # 2. Generate test spec
velo-test run my_app.sql                   # 3. Run tests (auto-discovers spec)

# Non-interactive mode for CI/CD
velo-test run my_app.sql -y --output junit > results.xml
```

### All Commands

| Command | Description |
|---------|-------------|
| `quickstart` | Interactive wizard - guides you through testing |
| `validate` | Check SQL syntax |
| `init` | Generate test spec |
| `infer-schema` | Generate schemas from data files |
| `run` | Execute tests |
| `debug` | Interactive debugger with stepping |
| `stress` | Performance testing |
| `annotate` | Generate observability annotations |
| `scaffold` | Generate runner script |

All commands support `-y` for non-interactive mode with sensible defaults.

## Example Project Structure

```
my_streaming_app/
├── sql/
│   └── app.sql              # Your SQL application
├── schemas/
│   ├── orders.schema.yaml   # Input schema definitions
│   └── users.schema.yaml
├── data/
│   └── test_input.csv       # Optional static test data
└── test_spec.yaml           # Test specification
```

## Example Test Specification

```yaml
application: market_aggregator
description: Tests for market data aggregation

queries:
  - name: basic_aggregation
    inputs:
      - source: market_data
        records: 1000
        schema: schemas/market_data.schema.yaml
    assertions:
      - type: record_count
        greater_than: 0
      - type: schema_contains
        fields: [symbol, avg_price, trade_count]
      - type: no_nulls
        fields: [symbol]
```

## Features

### Data Generation
- Schema-driven test data generation
- Support for enums, ranges, patterns, distributions
- Foreign key relationships between schemas
- Timestamp generation (relative and absolute)

### Assertions
- Record count validation
- Schema structure verification
- Null checking
- Value range validation
- Aggregate computations
- Join coverage analysis
- Performance metrics (execution time, memory usage, throughput)

### Output Formats
- Human-readable text (default)
- JSON for programmatic parsing
- JUnit XML for CI/CD integration

### Infrastructure
- Kafka integration via testcontainers
- In-memory schema registry
- File-based testing (no infrastructure needed)
- Automatic topic management

## Developer Documentation

For extending or contributing to the test harness, see the [Developer Documentation](../developer/test-harness/README.md).

## Demo Examples

### Quickstart (No Docker)

```bash
cd demo/quickstart
../../target/release/velo-test run hello_world.sql
```

### Test Harness Examples (with Kafka)

Working examples are available in `demo/test_harness_examples/`:

```bash
# Try the quickstart wizard on an example
velo-test quickstart demo/test_harness_examples/tier1_basic/01_passthrough.sql

# Or run directly with auto-discovery
velo-test run demo/test_harness_examples/tier1_basic/01_passthrough.sql

# Run all tiers
cd demo/test_harness_examples
./velo-test.sh run
```

## Support

- [GitHub Issues](https://github.com/your-org/velostream/issues) - Bug reports and feature requests
- [Feature Documentation](../feature/FR-084-app-test-harness/) - Original feature specification
