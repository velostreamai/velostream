# FerrisStreams Development Guide for Claude

## Project Overview

FerrisStreams is a high-performance streaming SQL engine written in Rust that provides real-time data processing capabilities with pluggable serialization formats (JSON, Avro, Protobuf). The project emphasizes performance, precision, and compatibility, particularly for financial analytics use cases.

## Key Components

### SQL Engine (`src/ferris/sql/`)
- **Parser**: Streaming SQL query parsing with support for windows, aggregations, joins
- **Execution Engine**: High-performance query execution with pluggable processors
- **Types System**: Dual type system with FieldValue (SQL execution) and InternalValue (serialization)
- **Aggregation**: Windowed and continuous aggregation processing
- **Windowing**: Tumbling, sliding, and session windows with emit modes

### Serialization (`src/ferris/serialization/`)
- **Pluggable Formats**: JSON (always available), Avro (feature-gated), Protobuf (feature-gated)
- **Type Conversion**: Bidirectional conversion between FieldValue and InternalValue
- **Financial Precision**: ScaledInteger support for exact financial arithmetic

### Kafka Integration (`src/ferris/kafka/`)
- **Consumers/Producers**: High-performance Kafka integration with configurable serialization
- **Schema Support**: Avro schema registry integration
- **Performance Presets**: Optimized configurations for different use cases

## Recent Major Enhancement: Financial Precision

### Problem Solved
- f64 floating-point precision errors in financial calculations
- Need for exact arithmetic in financial analytics
- Performance bottlenecks in financial computations

### Solution Implemented
- **FieldValue::ScaledInteger(i64, u8)**: Stores scaled integer with decimal precision
- **42x Performance Improvement**: ScaledInteger operations are 42x faster than f64
- **Perfect Precision**: No floating-point rounding errors
- **Cross-System Compatibility**: Serializes as decimal strings for JSON/Avro

## Performance Benchmarks

Financial calculation patterns (price × quantity):
- **f64**: 83.458µs (with precision errors)
- **ScaledInteger**: 1.958µs (exact precision) → **42x FASTER**
- **Decimal**: 53.583µs (exact precision) → 1.5x faster than f64

## Development Commands

### Testing
```bash
# Run all tests
cargo test --no-default-features

# Run specific test module
cargo test unit::sql::execution::types --no-default-features -- --nocapture

# Run financial precision benchmarks
cargo test financial_precision_benchmark -- --nocapture

# Test specific SQL functionality
cargo test windowing_test --no-default-features -- --nocapture
```

### Building
```bash
# Build with default features (JSON, Protobuf, Avro)
cargo build

# Build with only JSON support
cargo build --no-default-features --features json

# Build specific binaries
cargo build --bin ferris-sql-multi --no-default-features
```

### Code Formatting
```bash
# Format all code (required for CI/CD)
cargo fmt --all

# Check formatting without making changes
cargo fmt --all -- --check

# Run before committing to ensure CI passes
cargo fmt --all && cargo check --no-default-features
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
```bash
# Run financial precision tests
cargo run --bin test_financial_precision --no-default-features

# Test serialization compatibility
cargo run --bin test_serialization_compatibility --no-default-features
```

### Feature Flags
- `json`: JSON serialization (always enabled)
- `avro`: Apache Avro support (requires apache-avro crate)
- `protobuf`: Protocol Buffers support (requires prost crate)

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
pub struct MyStruct { /* ... */ }
impl MyStruct { /* ... */ }
```

**Best Practice**: Create dedicated files for each major struct/class and import them in mod.rs.

### Type System Architecture
```rust
// SQL Execution Types (internal fast arithmetic)
FieldValue::ScaledInteger(i64, u8)  // 42x faster than f64
FieldValue::Float(f64)              // Standard floating point
FieldValue::Integer(i64)            // Standard integer

// Serialization Types (cross-system compatibility)
InternalValue::ScaledNumber(i64, u8)  // Financial precision
InternalValue::Number(f64)            // Standard float
InternalValue::Integer(i64)           // Standard integer
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
- `src/ferris/sql/execution/types.rs` - Core type operations
- `src/ferris/sql/execution/aggregation/` - Aggregation functions
- `src/ferris/serialization/mod.rs` - Serialization conversion
- Binary files: `src/bin/*.rs` - Server implementations

### Serialization Compatibility Strategy
- **JSON**: Decimal strings for universal parsing (`"123.4567"`)
- **Avro**: String fields with decimal logical type support
- **Protobuf**: Structured Decimal message with units/scale fields (industry standard)

## Common Tasks

### Adding New SQL Functions
1. Update `src/ferris/sql/execution/expression/functions.rs`
2. Add pattern matches for all FieldValue variants
3. Implement arithmetic preserving ScaledInteger precision
4. Add tests in `tests/unit/sql/functions/`

### Adding New Aggregation Functions
1. Update `src/ferris/sql/execution/aggregation/accumulator.rs`
2. Handle ScaledInteger accumulation with proper scaling
3. Add tests in `tests/unit/sql/execution/aggregation/`

### Adding New Serialization Support
1. Add feature flag in `Cargo.toml`
2. Implement conversion functions in `src/ferris/serialization/mod.rs`
3. Handle ScaledInteger → compatible format mapping
4. Add comprehensive tests in `tests/unit/serialization/`

### Adding New Configuration Features
1. Update appropriate module in `src/ferris/sql/config/`
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
2. **Use proper imports** at the top of the test file
3. **Write comprehensive test cases** covering:
   - Happy path scenarios
   - Error conditions
   - Edge cases
   - Performance considerations (if applicable)

Example test file structure:
```rust
// tests/unit/sql/query_analyzer_test.rs
use ferrisstreams::ferris::sql::{
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
5. **CI/CD Formatting Failures**: Always run `cargo fmt --all` before committing

### Useful Debug Commands
```bash
# Debug specific test with full output
RUST_BACKTRACE=1 cargo test test_name --no-default-features -- --nocapture

# Performance debugging
cargo test financial_precision_benchmark::performance_benchmarks -- --nocapture

# Check for missing pattern matches
cargo check --no-default-features

# Fix formatting issues (GitHub Actions requirement)
cargo fmt --all

# Verify formatting and compilation before push
cargo fmt --all -- --check && cargo check --no-default-features
```

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

## Current Status

✅ **Completed**: Financial precision implementation with 42x performance improvement
✅ **Completed**: Cross-compatible JSON/Avro serialization  
✅ **Completed**: Comprehensive test coverage for financial operations
🔧 **In Progress**: High-performance Protobuf implementation with Decimal message
📋 **Pending**: Performance optimization for large-scale aggregations

The codebase is production-ready for financial analytics use cases requiring exact precision and high performance.
- Always run clippy checks#