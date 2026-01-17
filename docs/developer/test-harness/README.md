# Test Harness Developer Documentation

This documentation is for developers who want to understand, extend, or contribute to the Velostream SQL Application Test Harness.

## Overview

The test harness is a modular framework located in `src/velostream/test_harness/`. It provides infrastructure for testing streaming SQL applications.

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](./ARCHITECTURE.md) | System design and module structure |
| [Extending](./EXTENDING.md) | Adding new assertions, generators, and features |
| [Internals](./INTERNALS.md) | Deep dive into implementation details |
| [Testing](./TESTING.md) | How to test the test harness itself |

## Quick Links

### User Documentation
- [User Guide](../../test-harness/README.md) - End-user documentation
- [Getting Started](../../test-harness/GETTING_STARTED.md) - Tutorial
- [Quick Reference](../../test-harness/QUICK_REFERENCE.md) - Cheat sheet

### Source Code
- `src/velostream/test_harness/` - Main module
- `src/bin/velo-test.rs` - CLI binary
- `tests/integration/test_harness_integration_test.rs` - Integration tests

## Module Overview

```
src/velostream/test_harness/
├── mod.rs              # Module exports
├── cli.rs              # CLI argument parsing
├── spec.rs             # Test specification types
├── executor.rs         # Query execution engine
├── generator.rs        # Test data generation
├── schema.rs           # Schema parsing
├── assertions.rs       # Assertion implementations
├── report.rs           # Report generation
├── infra.rs            # Infrastructure (Kafka, etc.)
├── ai.rs               # AI-powered features
├── stress.rs           # Stress testing
├── config_override.rs  # Configuration management
├── dlq.rs              # Dead Letter Queue support
├── fault_injection.rs  # Chaos testing
├── file_io.rs          # File-based I/O
├── table_state.rs      # CTAS table tracking
├── spec_generator.rs   # Auto-generate test specs
├── inference.rs        # Schema inference
├── utils.rs            # Shared utilities
└── error.rs            # Error types
```

## Key Abstractions

### TestSpec
Represents a complete test specification including queries, inputs, and assertions.

### QueryExecutor
Executes SQL queries against the infrastructure and captures outputs.

### AssertionRunner
Validates captured outputs against assertion definitions.

### SchemaDataGenerator
Generates test data from schema definitions with constraints.

### TestHarnessInfra
Manages test infrastructure (Kafka topics, schema registry).

## Getting Started (Development)

```bash
# Build the test harness
cargo build --release

# Run unit tests
cargo test --lib test_harness

# Run integration tests (requires Docker)
cargo test --tests integration::test_harness_integration_test

# Run the CLI
./target/release/velo-test --help
```

## Contributing

1. Read the [Architecture](./ARCHITECTURE.md) document
2. Follow the patterns in existing modules
3. Add tests for new functionality
4. Update documentation as needed
5. Run pre-commit checks before submitting
