# Velostream Development Guide for Claude

## Behaviour

- **🚫 CRITICAL: DO NOT commit changes unless the user has explicitly reviewed them first**
- Use ast-grep for large scale refactoring:
- Dont make assumptions about the behaviour of the project.
- Always run all of the tests, demos, examples and /bin' and check the documentation.
- Always check the code formatting.
- Always check the code for errors.
- Always check the code for bugs.
- Always check the code for security issues.
- Always check the code for performance issues.
- Always check the code for correctness.
- Never assume that it is ok to perform a 'workaround' with consent first
- Always look for opportunities to improve the code. (refactoring, code restructuring, etc.)
- NEVER use Mocks in the codebase - only in Tests where it is needed
- Always fix ALL compilation errors before marking any task as complete.
- Always fix ALL test failures before marking any task as complete.
- Always ensure that the code compiles and passes all tests before marking any task as complete.
- Always ensure that the code passes all pre-commit checks before marking any task as complete.
- Always ensure that the code passes all CI/CD checks before marking any task as complete.
- Always check CLIPPY errors and fix them before marking any task as complete.
- When fixing a BUG - reproduce the bug in a test, applying the fix and then verify the test passes
- Look at run-commit.sh for pre-commit checks and ensure that all checks pass before marking any task as complete.

## Project Overview

Velostream is a high-performance streaming SQL engine written in Rust that provides real-time data processing
capabilities with pluggable serialization formats (JSON, Avro, Protobuf). The project emphasizes performance, precision,
and compatibility, particularly for financial analytics use cases.

## Key Components

### SQL Engine (`src/velostream/sql/`)

- **Parser**: Streaming SQL query parsing with support for windows, aggregations, joins
- **Execution Engine**: High-performance query execution with pluggable processors
- **Types System**: FieldValue-based type system for SQL execution and serialization
- **Aggregation**: Windowed and continuous aggregation processing
- **Windowing**: Tumbling, sliding, and session windows with emit modes

### Serialization (`src/velostream/serialization/`)

- **Pluggable Formats**: JSON, Avro, and Protobuf (all always available)
- **Type Conversion**: Direct FieldValue serialization with enhanced error chaining
- **Financial Precision**: ScaledInteger support for exact financial arithmetic

### Kafka Integration (`src/velostream/kafka/`)

- **Consumers/Producers**: High-performance Kafka integration with configurable serialization
- **Schema Support**: Avro schema registry integration
- **Performance Presets**: Optimized configurations for different use cases

### Test Harness (separate repo: `velo-test`)

The SQL application test harness has been extracted to its own repository: `https://github.com/velostreamai/velo-test`

For local development, clone it as a sibling directory:
```
../velo-test/    # git clone https://github.com/velostreamai/velo-test.git
../velostream/   # this repo
```

The `velo-test` project uses `.cargo/config.toml` to patch the velostream dependency to the local path, so changes to velostream are immediately reflected when building velo-test.

See `demo/test_harness_examples/` for comprehensive test spec examples across 8 tiers of complexity.

### Velo Studio (separate repo: `velo-studio`)

The SQL Studio web application is in its own repository: `https://github.com/velostreamai/velo-studio`

Same sibling directory layout applies for local development.

## Development Commands

### Testing

```bash
# Run all tests
cargo test

# Unit test
cargo test --tests --verbose -- --skip integration:: --skip performance:: --skip comprehensive

# Run specific test module
cargo test unit::sql::execution::types -- --nocapture

# Run financial precision benchmarks
cargo test financial_precision_benchmark -- --nocapture

# Test specific SQL functionality
cargo test windowing_test -- --nocapture
```

### Building

```bash
# Build the project (all serialization formats included)
cargo build

# Build specific binaries
cargo build --bin velo-sql
```

### Code Formatting

```bash
# Format all code (required for CI/CD)
cargo fmt --all

# Check formatting without making changes
cargo fmt --all -- --check

# CRITICAL: Always run formatting check before committing
cargo fmt --all -- --check

# Run complete pre-commit verification to ensure CI passes
cargo fmt --all -- --check && cargo check

# Run clippy to catch additional linting issues
cargo clippy --no-default-features

# Run clippy with strict warnings (for high code quality)
cargo clippy --no-default-features -- -D warnings
```

### Git Workflow

```bash
# Commit after completing significant work units
# Examples: 1 day's work, project milestone, or architectural phase
git add .
git commit -m "feat: implement Day X - [Feature Name]

Key achievements:
- [Achievement 1]
- [Achievement 2] 
- [Performance metrics if applicable]

🤖 Generated with Claude Code"

# Push to remote after successful local testing
git push origin branch-name
```

### Performance Testing

#### Baseline Comparison Benchmarks

```bash
# All scenarios, release build (recommended for final benchmarks)
./benchmarks/run_baseline.sh

# Choose mode and scenarios flexibly
./benchmarks/run_baseline_flexible.sh release 1    # Release, scenario 1 only
./benchmarks/run_baseline_flexible.sh debug        # Debug, all scenarios
./benchmarks/run_baseline_flexible.sh profile 2    # Profile, scenario 2 only

# Quick iteration with minimal recompilation
./benchmarks/run_baseline_quick.sh

# Multiple compilation modes
./benchmarks/run_baseline_options.sh debug         # Fast compile
./benchmarks/run_baseline_options.sh release       # Optimized runtime
./benchmarks/run_baseline_options.sh profile       # With debug symbols
```

**Documentation:** See [`docs/benchmarks/`](docs/benchmarks/) for:

- [`SCRIPTS_README.md`](docs/benchmarks/SCRIPTS_README.md) - Complete reference guide
- [`BASELINE_TESTING.md`](docs/benchmarks/BASELINE_TESTING.md) - Detailed methodology
- [`BASELINE_QUICK_REFERENCE.md`](docs/benchmarks/BASELINE_QUICK_REFERENCE.md) - Quick cheat sheet

## Schema Configuration

### Kafka Schema Support

Velostream now supports comprehensive schema configuration for Kafka data sources:

**Avro Schema Configuration**:

```yaml
# Inline schema
avro.schema: |
  {
    "type": "record",
    "name": "ExampleRecord", 
    "fields": [{"name": "id", "type": "long"}]
  }

# Schema file  
avro.schema.file: "./schemas/example.avsc"
```

**Protobuf Schema Configuration**:

```yaml
# Inline schema
protobuf.schema: |
  syntax = "proto3";
  message ExampleRecord {
    int64 id = 1;
  }

# Schema file
protobuf.schema.file: "./schemas/example.proto"
```

**Key Benefits**:

- **Schema Enforcement**: Avro/Protobuf now require proper schemas (no more hardcoded fallbacks)
- **Multiple Config Keys**: Support for various naming conventions (`avro.schema`, `value.avro.schema`, etc.)
- **File Support**: Load schemas from external files for better maintainability
- **Financial Precision**: Built-in support for decimal logical types in schemas
- **Production Ready**: Schema Registry integration for centralized schema management

See [docs/kafka-schema-configuration.md](docs/developer/kafka-schema-configuration.md) for complete configuration guide.

## Environment Variables

Velostream supports environment variable configuration using the `VELOSTREAM_` prefix. Environment variables take
precedence over configuration file settings (12-factor app style).

### Kafka Configuration

| Environment Variable               | Description                                                                 | Default          |
|------------------------------------|-----------------------------------------------------------------------------|------------------|
| `VELOSTREAM_KAFKA_BROKERS`         | Kafka broker endpoints (comma-separated)                                    | `localhost:9092` |
| `VELOSTREAM_KAFKA_TOPIC`           | Default Kafka topic name                                                    | (from config)    |
| `VELOSTREAM_KAFKA_GROUP_ID`        | Consumer group ID                                                           | `velo-sql-{job}` |
| `VELOSTREAM_BROKER_ADDRESS_FAMILY` | Broker address resolution: `v4` (IPv4 only), `v6` (IPv6 only), `any` (both) | `v4`             |

### Server Configuration

| Environment Variable           | Description                                            | Default |
|--------------------------------|--------------------------------------------------------|---------|
| `VELOSTREAM_MAX_JOBS`          | Maximum concurrent jobs                                | `100`   |
| `VELOSTREAM_ENABLE_MONITORING` | Enable performance monitoring (`true`/`false`/`1`/`0`) | `false` |
| `VELOSTREAM_JOB_TIMEOUT_SECS`  | Job timeout in seconds                                 | `86400` |
| `VELOSTREAM_TABLE_CACHE_SIZE`  | Table registry cache size                              | `100`   |

### Event-Time Configuration

| Environment Variable                | Description                                                                               | Default            |
|-------------------------------------|-------------------------------------------------------------------------------------------|--------------------|
| `VELOSTREAM_EVENT_TIME_FALLBACK`    | Behavior when `_EVENT_TIME` is accessed but not set: `processing_time`, `warn`, or `null` | `processing_time`  |

### Retry Configuration

| Environment Variable              | Description                               | Default       |
|-----------------------------------|-------------------------------------------|---------------|
| `VELOSTREAM_RETRY_INTERVAL_SECS`  | Base retry interval in seconds            | `1`           |
| `VELOSTREAM_RETRY_MULTIPLIER`     | Exponential backoff multiplier            | `2.0`         |
| `VELOSTREAM_RETRY_MAX_DELAY_SECS` | Maximum retry delay in seconds            | `60`          |
| `VELOSTREAM_RETRY_STRATEGY`       | Retry strategy (`fixed` or `exponential`) | `exponential` |

### Topic Configuration

| Environment Variable                    | Description                      | Default |
|-----------------------------------------|----------------------------------|---------|
| `VELOSTREAM_DEFAULT_PARTITIONS`         | Default Kafka topic partitions   | `6`     |
| `VELOSTREAM_DEFAULT_REPLICATION_FACTOR` | Default topic replication factor | `3`     |

### Transactional Configuration

| Environment Variable                       | Description                                              | Default |
|--------------------------------------------|----------------------------------------------------------|---------|
| `VELOSTREAM_ALLOW_TRANSACTIONAL_FALLBACK`  | Allow fallback to non-transactional when init fails      | `false` |

**Important**: When `@job_mode: transactional` is configured, the system requires exactly-once semantics. If the
transactional producer fails to initialize (e.g., on testcontainers), the job will **fail by default** to prevent
silent degradation of guarantees.

Set `VELOSTREAM_ALLOW_TRANSACTIONAL_FALLBACK=true` only for testing with testcontainers where transaction coordinator
support is limited. **Never use in production** as it breaks exactly-once semantics.

### Usage Examples

```bash
# Docker/Kubernetes deployment
export VELOSTREAM_KAFKA_BROKERS="broker1:9092,broker2:9092,broker3:9092"
export VELOSTREAM_MAX_JOBS=500
export VELOSTREAM_ENABLE_MONITORING=true

# Test harness with testcontainers (dynamic broker)
export VELOSTREAM_KAFKA_BROKERS="localhost:${KAFKA_PORT}"

# Production with IPv6 or mixed network (disable IPv4-only default)
export VELOSTREAM_BROKER_ADDRESS_FAMILY=any
```

**Note on `VELOSTREAM_BROKER_ADDRESS_FAMILY`**: Defaults to `v4` (IPv4 only) because testcontainers and Docker often
advertise `localhost` in Kafka broker metadata, which can resolve to IPv6 `::1` on some systems while the container only
listens on IPv4. Set to `any` for production environments with proper DNS or IPv6 support.

### Resolution Chain

Configuration values are resolved in this order (highest priority first):

1. Environment variable: `VELOSTREAM_{KEY}`
2. Configuration file property (from YAML, SQL WITH clause, etc.)
3. Default value

The `PropertyResolver` utility (`src/velostream/sql/config/resolver.rs`) provides type-safe resolution with logging.

### YAML Variable Substitution

YAML configuration files support inline environment variable substitution using `${VAR:default}` syntax:

```yaml
# Environment variable takes precedence, falls back to default
consumer_config:
  bootstrap.servers: "${VELOSTREAM_KAFKA_BROKERS:localhost:9092}"

# Supported patterns:
# ${VAR}          - Required env var (empty string if not set)
# ${VAR:default}  - Optional env var with default value
```

This follows **12-factor app** principles where configuration is externalized and explicit.

## SQL Grammar Rules for Claude

### ⭐ CRITICAL: SQL Syntax is NOT Optional

When writing SQL for this project, **DO NOT GUESS**. The parser has strict grammar rules that must be followed exactly.

**Truth Sources (in order of preference)**:

1. [`docs/sql/COPY_PASTE_EXAMPLES.md`](docs/sql/COPY_PASTE_EXAMPLES.md) - Working examples for all query types
2. [`docs/sql/PARSER_GRAMMAR.md`](docs/sql/PARSER_GRAMMAR.md) - Formal EBNF grammar and AST structure
3. [`docs/claude/SQL_GRAMMAR_RULES.md`](docs/claude/SQL_GRAMMAR_RULES.md) - Rules specifically for Claude
4. [`docs/user-guides/sql-annotations.md`](docs/user-guides/sql-annotations.md) - SQL annotations reference (@job_mode,
   @metric, etc.)
5. `tests/unit/sql/parser/*_test.rs` - Unit tests with exact syntax examples

**Before writing ANY SQL**:

- ✅ Search for similar example in COPY_PASTE_EXAMPLES.md
- ✅ Verify syntax matches PARSER_GRAMMAR.md
- ✅ Check CLAUDE SQL_GRAMMAR_RULES.md for common mistakes
- ✅ grep tests for pattern: `grep -r "your_pattern" tests/unit/sql/parser/`

### The Two Window Systems

Velostream has **TWO different window mechanisms in different parts of the AST**:

| Feature        | Time Window                                            | ROWS Window                                                         |
|----------------|--------------------------------------------------------|---------------------------------------------------------------------|
| **Syntax**     | `WINDOW TUMBLING(...)`                                 | `ROWS WINDOW BUFFER N ROWS`                                         |
| **Location**   | SELECT statement (top-level)                           | OVER clause (inside function)                                       |
| **Use Case**   | Time-bucketed aggregations                             | Row-count window functions                                          |
| **Watermarks** | ✅ YES                                                  | ❌ NO                                                                |
| **Example**    | `GROUP BY symbol WINDOW TUMBLING(INTERVAL '5' MINUTE)` | `AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS PARTITION BY symbol)` |

### Most Common SQL Mistakes

```
❌ ROWS BUFFER 100 ROWS          → ✅ ROWS WINDOW BUFFER 100 ROWS
❌ ROWS WINDOW BUFFER 100        → ✅ ROWS WINDOW BUFFER 100 ROWS
❌ SELECT * WHERE x > 100        → ✅ SELECT * FROM table WHERE x > 100
❌ AVG(price) OVER ROWS WINDOW   → ✅ AVG(price) OVER (ROWS WINDOW BUFFER 100 ROWS)
❌ GROUP BY...PARTITION BY...    → ✅ PARTITION BY only in ROWS WINDOW, GROUP BY at top level
```

### Clause Order (Must be Exact)

```sql
SELECT...
    FROM...
    [
WHERE...]
    [
GROUP BY...]
    [
HAVING...]
    [WINDOW...] -- Time-based windows (optional)
    [
ORDER BY...]
    [LIMIT...]
```

**NOT optional.** If clauses appear out of order, the query will fail.

See full details in:

- [`docs/sql/PARSER_GRAMMAR.md`](docs/sql/PARSER_GRAMMAR.md) - Complete formal grammar
- [`docs/claude/SQL_GRAMMAR_RULES.md`](docs/claude/SQL_GRAMMAR_RULES.md) - Rules for Claude (14 specific rules)
- [`docs/user-guides/sql-annotations.md`](docs/user-guides/sql-annotations.md) - SQL annotations (@job_mode, @metric,
  @observability, etc.)

## Code Organization

### Module Structure Guidelines

**IMPORTANT**: Use `mod.rs` files ONLY for module construction and re-exports, NOT for struct/class definitions.

✅ **Correct mod.rs usage**:

```rust
// mod.rs should only contain:
pub mod error;          // Import submodules
pub mod data_source;
pub mod reader;

// Re-export types for convenience
pub use error::DataSourceError;
pub use data_source::KafkaDataSource;
```

❌ **Incorrect mod.rs usage**:

```rust
// DO NOT define structs/classes in mod.rs
pub struct MyStruct {
    /* ... */
}
impl MyStruct { /* ... */ }
```

**Best Practice**: Create dedicated files for each major struct/class and import them in mod.rs.

### Type System Architecture

```rust
// Unified FieldValue Types (for both execution and serialization)
FieldValue::ScaledInteger(i64, u8)  // 42x faster than f64, financial precision
FieldValue::Float(f64)              // Standard floating point
FieldValue::Integer(i64)            // Standard integer
FieldValue::String(String)          // Text data
FieldValue::Boolean(bool)           // Boolean values
FieldValue::Timestamp(NaiveDateTime) // Date/time values
```

### Serialization Patterns

```rust
// ScaledInteger serialization for compatibility:
// JSON: "123.4567" (decimal string)
// Avro: "123.4567" (string field)  
// Protobuf: Decimal{units: 1234567, scale: 4} (structured)
```

## Critical Implementation Details

### Financial Arithmetic

- **Internal Representation**: Scaled integers (e.g., $123.45 stored as 123450 with scale=3)
- **Arithmetic Operations**: Direct integer operations preserve exact precision
- **Display Formatting**: Converts back to decimal representation with trailing zero removal
- **Type Coercion**: Automatic scaling alignment for operations between different scales

### Pattern Matching Requirements

When adding new FieldValue variants, ensure all pattern matches are updated:

- `src/velostream/sql/execution/types.rs` - Core type operations
- `src/velostream/sql/execution/aggregation/` - Aggregation functions
- `src/velostream/serialization/mod.rs` - Serialization conversion
- Binary files: `src/bin/*.rs` - Server implementations

### Serialization Compatibility Strategy

- **JSON**: Decimal strings for universal parsing (`"123.4567"`)
- **Avro**: String fields with decimal logical type support
- **Protobuf**: Structured Decimal message with units/scale fields (industry standard)

## Common Tasks

### Adding New SQL Functions

1. Update `src/velostream/sql/execution/expression/functions.rs`
2. Add pattern matches for all FieldValue variants
3. Implement arithmetic preserving ScaledInteger precision
4. Add tests in `tests/unit/sql/functions/`

### Adding New Aggregation Functions

1. Update `src/velostream/sql/execution/aggregation/accumulator.rs`
2. Handle ScaledInteger accumulation with proper scaling
3. Add tests in `tests/unit/sql/execution/aggregation/`

### Adding New Serialization Support

1. Implement conversion functions in `src/velostream/serialization/mod.rs`
2. Handle ScaledInteger → compatible format mapping
3. Add comprehensive tests in `tests/unit/serialization/`

### Adding New Configuration Features

1. Update appropriate module in `src/velostream/sql/config/`
2. Ensure public visibility for methods used in tests
3. Add comprehensive tests in `tests/unit/sql/config/`
4. Update imports in test files as needed

## Testing Strategy

### Test Organization

**CRITICAL**: Tests MUST be organized into dedicated test files outside of implementation modules:

- **NEVER add `#[cfg(test)]` blocks inside implementation files** - All tests must be in `tests/` directory
- **NEVER add `#[test]` functions inside src files** - They belong in `tests/unit/` or `tests/integration/`
- **Dedicated test files**: Tests are located in `tests/unit/` and `tests/integration/`
- **Test module structure**: Mirrors source structure for easy navigation

```
tests/
├── unit/
│   ├── sql/
│   │   ├── config/                    # Configuration system tests
│   │   │   ├── builder_test.rs        # Builder pattern tests
│   │   │   ├── connection_string_test.rs  # URI parsing tests
│   │   │   ├── validation_test.rs     # Validation system tests
│   │   │   └── environment_test.rs    # Environment config tests
│   │   ├── execution/                 # SQL execution tests
│   │   ├── functions/                 # SQL function tests
│   │   └── parser/                    # SQL parser tests
│   └── kafka/                         # Kafka integration tests
└── integration/                       # End-to-end tests
```

### Writing Tests - CORRECT Structure

When adding new functionality:

1. **Create test file** in `tests/unit/[module_path]/[feature]_test.rs`
2. **⚠️ CRITICAL: Register test in mod.rs** - Add `pub mod [feature]_test;` to the parent `mod.rs`
3. **Use proper imports** at the top of the test file
4. **Write comprehensive test cases** covering:
    - Happy path scenarios
    - Error conditions
    - Edge cases
    - Performance considerations (if applicable)

**⚠️ CRITICAL: Always Register New Test Files in mod.rs**

When you create a new test file, you MUST add it to the parent module's `mod.rs` file, otherwise Cargo won't discover or
run your tests!

Example workflow:

```bash
# 1. Create new test file
touch tests/unit/table/new_feature_test.rs

# 2. IMMEDIATELY add to mod.rs (DO NOT SKIP THIS!)
echo "pub mod new_feature_test;" >> tests/unit/table/mod.rs

# 3. Verify test is discovered
cargo test new_feature_test --no-default-features -- --list
```

Example test file structure:

```rust
// tests/unit/sql/query_analyzer_test.rs
use velostream::velostream::sql::{
    query_analyzer::{QueryAnalyzer, QueryAnalysis},
    ast::{StreamingQuery, StreamSource, SelectField},
    SqlError,
};
use std::collections::HashMap;

#[test]
fn test_query_analyzer_select() {
    // Test implementation
}

#[test]
fn test_query_analyzer_error_handling() {
    // Test implementation
}
```

Example mod.rs registration:

```rust
// tests/unit/sql/mod.rs
pub mod query_analyzer_test;  // ← ADD THIS LINE for each new test file
pub mod parser_test;
pub mod execution_test;
```

### Unit Tests

- **Type Operations**: All arithmetic, casting, formatting
- **SQL Functions**: Builtin functions with all type combinations
- **Aggregation**: Window functions, GROUP BY, HAVING clauses
- **Serialization**: Round-trip compatibility tests
- **Configuration**: URI parsing, validation, environment config

### Integration Tests

- **End-to-End SQL**: Complete query processing
- **Performance**: Benchmark critical paths
- **Compatibility**: Cross-system serialization verification

### Performance Tests

- **Financial Benchmarks**: ScaledInteger vs f64 vs Decimal
- **Aggregation Performance**: Large dataset processing
- **Serialization Speed**: Format comparison benchmarks

## Debugging Tips

### Common Issues

1. **Pattern Match Exhaustiveness**: New FieldValue variants need matches everywhere
2. **Scale Alignment**: Different scales in ScaledInteger arithmetic
3. **Serialization Round-trips**: Ensure exact precision preservation
4. **Performance Regressions**: Monitor financial arithmetic benchmarks
5. **CI/CD Formatting Failures**: Always run `cargo fmt --all -- --check` before committing

### Critical Development Rule

**NEVER mark tasks as completed when code doesn't compile.** Always verify compilation and basic functionality before
marking work as done. This is essential for maintaining code quality and avoiding wasted time.

### Useful Debug Commands

```bash
# Debug specific test with full output
RUST_BACKTRACE=1 cargo test test_name --no-default-features -- --nocapture

# Performance debugging
cargo test financial_precision_benchmark::performance_benchmarks -- --nocapture

# Check for missing pattern matches
cargo check
```

## Pre-Commit Checks

### 🚀 Quick Pre-Commit Runner

When asked to run pre-commit checks, execute this comprehensive verification sequence:

```bash
echo "🧹 Running Velostream pre-commit checks..."
echo "⚡ Stage 1: Fast Feedback Checks"

echo "1️⃣ Checking code formatting..."
cargo fmt --all -- --check || {
    echo "❌ Formatting check failed. Run 'cargo fmt --all' to fix."
    exit 1
}
echo "✅ Code formatting passed"

echo "2️⃣ Checking compilation..."
cargo check --all-targets --no-default-features || {
    echo "❌ Compilation failed."
    exit 1
}
echo "✅ Compilation passed"

echo "3️⃣ Running clippy linting..."
cargo clippy --all-targets --no-default-features || {
    echo "❌ Clippy linting failed."
    exit 1
}
echo "✅ Clippy linting passed"

echo "4️⃣ Verifying test registration..."
# Check if any *_test.rs files exist that aren't registered in mod.rs
UNREGISTERED=$(find tests/unit -name "*_test.rs" -type f | while read test_file; do
    test_name=$(basename "$test_file" .rs)
    mod_file=$(dirname "$test_file")/mod.rs
    if [ -f "$mod_file" ] && ! grep -q "pub mod $test_name" "$mod_file"; then
        echo "⚠️  $test_file not registered in $mod_file"
    fi
done)
if [ -n "$UNREGISTERED" ]; then
    echo "❌ Found unregistered test files:"
    echo "$UNREGISTERED"
    echo "Run: echo 'pub mod <test_name>;' >> <mod_file>"
    exit 1
fi
echo "✅ All test files registered"

echo "5️⃣ Running unit tests..."
cargo test --lib --no-default-features --quiet || {
    echo "❌ Unit tests failed."
    exit 1
}
echo "✅ Unit tests passed"

echo "🔄 Stage 2: Comprehensive Validation"

echo "6️⃣ Testing example compilation..."
cargo build --examples --no-default-features || {
    echo "❌ Example compilation failed."
    exit 1
}
echo "✅ Examples compiled successfully"

echo "7️⃣ Testing binary compilation..."
cargo build --bins --no-default-features || {
    echo "❌ Binary compilation failed."
    exit 1
}
echo "✅ Binaries compiled successfully"

echo "8️⃣ Running comprehensive test suite..."
cargo test --tests --no-default-features --quiet -- --skip integration:: --skip performance:: || {
    echo "❌ Comprehensive tests failed."
    exit 1
}
echo "✅ Comprehensive tests passed"

echo "9️⃣ Running documentation tests..."
cargo test --doc --no-default-features --quiet || {
    echo "❌ Documentation tests failed."
    exit 1
}
echo "✅ Documentation tests passed"

echo ""
echo "🎉 ALL PRE-COMMIT CHECKS PASSED!"
echo "✅ Code is ready for commit and push"
echo "📊 Summary:"
echo "   • Code formatting: ✅"
echo "   • Compilation: ✅"
echo "   • Clippy linting: ✅"
echo "   • Test registration: ✅"
echo "   • Unit tests: ✅"
echo "   • Examples: ✅"
echo "   • Binaries: ✅"
echo "   • Comprehensive tests: ✅"
echo "   • Documentation tests: ✅"
```

### 🔧 Individual Check Commands

#### Code Formatting (CRITICAL - GitHub Actions requirement)

```bash
# Check formatting
cargo fmt --all -- --check

# Fix formatting if needed
cargo fmt --all
```

#### Compilation and Linting

```bash
# Check compilation
cargo check --all-targets --no-default-features

# Run clippy (with strict warnings)
cargo clippy --all-targets --no-default-features -- -D warnings
```

#### Testing

```bash
# Unit tests only
cargo test --lib --no-default-features

# Comprehensive tests (excludes performance/integration)
cargo test --tests --no-default-features -- --skip integration:: --skip performance::

# Documentation tests
cargo test --doc --no-default-features
```

#### Build Verification

```bash
# Examples
cargo build --examples --no-default-features

# Binaries
cargo build --bins --no-default-features
```

### 📋 Pre-Commit Checklist

Before every commit, ensure:

- [ ] **Code formatting** passes (`cargo fmt --all -- --check`)
- [ ] **Compilation** succeeds (`cargo check --all-targets --no-default-features`)
- [ ] **Clippy linting** passes (`cargo clippy --all-targets --no-default-features`)
- [ ] **New test files registered** in `mod.rs` (verify with `cargo test --list`)
- [ ] **Unit tests** pass (`cargo test --lib --no-default-features`)
- [ ] **Examples compile** (`cargo build --examples --no-default-features`)
- [ ] **Binaries compile** (`cargo build --bins --no-default-features`)
- [ ] **Comprehensive tests** pass (excluding performance/integration)
- [ ] **Documentation tests** pass (`cargo test --doc --no-default-features`)

### 🎯 One-Line Complete Check

For quick verification, run this single command:

```bash
cargo fmt --all -- --check && cargo check && cargo test --no-default-features && cargo build --examples --no-default-features && cargo build --bins --no-default-features
```

### 🚨 Critical Rules

1. **NEVER commit when code doesn't compile**
2. **ALWAYS run formatting check** - GitHub Actions will fail without it
3. **Run comprehensive checks** before pushing to prevent CI failures
4. **Fix all clippy warnings** for code quality
5. **Ensure all tests pass** before marking work complete

### 🔄 CI/CD Pipeline Match

These checks mirror the GitHub Actions pipeline:

- **Stage 1**: Fast feedback (formatting, compilation, clippy, unit tests)
- **Stage 2**: Comprehensive validation (full test suite, examples, binaries)

Running these locally ensures CI/CD success and maintains code quality standards.

## Architecture Principles

### Performance First

- **Zero-Copy Where Possible**: Minimize allocations in hot paths
- **Integer Arithmetic**: ScaledInteger for financial calculations
- **Efficient Serialization**: Direct binary formats over text when possible

### Precision Over Speed (for Financial Data)

- **Exact Arithmetic**: Never compromise precision for performance
- **Deterministic Results**: Same inputs always produce identical outputs
- **Regulatory Compliance**: Meet financial industry precision requirements

### Compatibility

- **Standard Formats**: Use industry-standard serialization patterns
- **Cross-Language**: Ensure other systems can consume data
- **Schema Evolution**: Support backward-compatible changes