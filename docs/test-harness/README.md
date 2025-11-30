# SQL Application Test Harness

The Velostream SQL Application Test Harness (`velo-test`) is a comprehensive testing framework for validating streaming SQL applications before deployment.

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
| [Getting Started](./GETTING_STARTED.md) | Step-by-step tutorial for your first test |
| [Developer Guide](./DEVELOPER_GUIDE.md) | Comprehensive guide with examples |
| [User Guide](./USER_GUIDE.md) | Complete reference for all features |
| [Quick Reference](./QUICK_REFERENCE.md) | Cheat sheet for common operations |
| [Assertions Reference](./ASSERTIONS.md) | All assertion types and options |
| [Schema Reference](./SCHEMAS.md) | Schema definition format and constraints |
| [CLI Reference](./CLI_REFERENCE.md) | All CLI commands and options |

## Quick Start

```bash
# Validate SQL syntax
velo-test validate my_app.sql

# Run tests with a spec file
velo-test run my_app.sql --spec test_spec.yaml

# Generate a test spec from SQL
velo-test init my_app.sql --output test_spec.yaml

# Run stress tests
velo-test stress my_app.sql --records 100000 --duration 60
```

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
- Performance metrics

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

Working examples are available in `demo/test_harness_examples/`:

```bash
# Navigate to examples
cd demo/test_harness_examples/getting_started

# Run the market aggregation example
../../target/release/velo-test run ./sql/market_aggregation.sql --spec ./test_spec.yaml
```

## Support

- [GitHub Issues](https://github.com/your-org/velostream/issues) - Bug reports and feature requests
- [Feature Documentation](../feature/FR-084-app-test-harness/) - Original feature specification
