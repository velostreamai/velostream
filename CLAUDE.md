# FerrisStreams Development Guide for Claude


## Behaviour
Dont make assumptions about the behaviour of the project.
Always run all of the tests, demos, examples and /bin' and check the documentation.
Always check the code formatting.
Always check the code for errors.
Always check the code for bugs.
Always check the code for security issues.
Always check the code for performance issues.
Always check the code for correctness.
Always look for opportunities to improve the code. (refactoring, code restructuring, etc.)


## Project Overview

FerrisStreams is a high-performance streaming SQL engine written in Rust that provides real-time data processing capabilities with pluggable serialization formats (JSON, Avro, Protobuf). The project emphasizes performance, precision, and compatibility, particularly for financial analytics use cases.

## Key Components

### SQL Engine (`src/ferris/sql/`)
- **Parser**: Streaming SQL query parsing with support for windows, aggregations, joins
- **Execution Engine**: High-performance query execution with pluggable processors
- **Types System**: FieldValue-based type system for SQL execution and serialization
- **Aggregation**: Windowed and continuous aggregation processing
- **Windowing**: Tumbling, sliding, and session windows with emit modes

### Serialization (`src/ferris/serialization/`)
- **Pluggable Formats**: JSON, Avro, and Protobuf (all always available)
- **Type Conversion**: Direct FieldValue serialization with enhanced error chaining
- **Financial Precision**: ScaledInteger support for exact financial arithmetic

### Kafka Integration (`src/ferris/kafka/`)
- **Consumers/Producers**: High-performance Kafka integration with configurable serialization
- **Schema Support**: Avro schema registry integration
- **Performance Presets**: Optimized configurations for different use cases

## Recent Major Enhancements

### Compression Independence in Batch Configuration (Latest)

**Problem Solved**
- Batch strategies were overriding explicit compression settings
- Users couldn't configure compression independently from batch optimizations
- Need for fine-grained control over compression vs automatic optimization

**Solution Implemented**
- **Suggestion vs Override Pattern**: Batch strategies suggest compression only when none is explicitly set
- **Full Independence**: Explicit compression settings are never overridden by batch configurations
- **Intelligent Defaults**: When no compression is specified, batch strategies provide optimal suggestions
- **Comprehensive Logging**: Detailed logging shows final applied compression settings

**Key Benefits**
```rust
// Explicit compression is always preserved
props.insert("compression.type".to_string(), "zstd".to_string());
let batch_config = BatchConfig {
    strategy: BatchStrategy::MemoryBased(1024 * 1024), // Would suggest gzip
    // ... 
};
// Result: compression.type remains "zstd" (user choice preserved)
```

See [docs/COMPRESSION_INDEPENDENCE.md](docs/developer/COMPRESSION_INDEPENDENCE.md) for complete documentation.

### SerializationError Enhancement: Comprehensive Error Chaining

**Problem Solved**
- Limited error diagnostics for serialization failures
- Loss of original error context in error chains
- Difficult debugging of cross-format serialization issues

**Solution Implemented**
- **Enhanced Error Variants**: 6 new structured error types with full source chain preservation
- **JSON/Avro/Protobuf Support**: All serialization formats now use enhanced error variants
- **Error Chain Traversal**: Full error source chain information for debugging
- **100% Backward Compatibility**: Existing error handling patterns continue to work

**Enhanced Error Types**
```rust
SerializationError::JsonError { message, source }          // JSON serialization with source
SerializationError::AvroError { message, source }          // Avro serialization with source  
SerializationError::ProtobufError { message, source }      // Protobuf serialization with source
SerializationError::TypeConversionError { message, from_type, to_type, source }
SerializationError::SchemaValidationError { message, source }
SerializationError::EncodingError { message, source }
```

### Financial Precision Enhancement

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
cargo test
2
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
cargo build --bin ferris-sql-multi
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
```bash
# Run financial precision tests
cargo run --bin test_financial_precision 
# Test serialization compatibility
cargo run --bin test_serialization_compatibility ```


## Schema Configuration

### Kafka Schema Support
FerrisStreams now supports comprehensive schema configuration for Kafka data sources:

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

See [docs/KAFKA_SCHEMA_CONFIGURATION.md](docs/developer/KAFKA_SCHEMA_CONFIGURATION.md) for complete configuration guide.

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
1. Implement conversion functions in `src/ferris/serialization/mod.rs`
2. Handle ScaledInteger → compatible format mapping
3. Add comprehensive tests in `tests/unit/serialization/`

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
5. **CI/CD Formatting Failures**: Always run `cargo fmt --all -- --check` before committing

### Critical Development Rule
**NEVER mark tasks as completed when code doesn't compile.** Always verify compilation and basic functionality before marking work as done. This is essential for maintaining code quality and avoiding wasted time.

### Useful Debug Commands
```bash
# Debug specific test with full output
RUST_BACKTRACE=1 cargo test test_name --no-default-features -- --nocapture

# Performance debugging
cargo test financial_precision_benchmark::performance_benchmarks -- --nocapture

# Check for missing pattern matches
cargo check 
# CRITICAL: Fix formatting issues (GitHub Actions requirement)
cargo fmt --all

# MANDATORY: Complete pre-commit verification sequence
cargo fmt --all -- --check && cargo check && cargo test --no-default-features && cargo build --examples --no-default-features && cargo build --bins --no-default-features
# Run this comprehensive check before every commit to ensure:
# 1. Code formatting is correct
# 2. All code compiles successfully  
# 3. All tests pass (both unit and integration)
# 4. All examples compile
# 5. All binaries compile
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

✅ **Completed**: Enhanced SerializationError system with comprehensive error chaining
✅ **Completed**: Financial precision implementation with 42x performance improvement
✅ **Completed**: Cross-compatible JSON/Avro/Protobuf serialization with enhanced errors
✅ **Completed**: Comprehensive test coverage (255+ tests passing)
✅ **Completed**: All demos, examples, and doctests verified compliant
✅ **Completed**: High-performance Protobuf implementation with Decimal message
✅ **Completed**: Performance test compilation issues resolved (Phase 3 benchmarks)
✅ **Completed**: Complete pre-commit validation pipeline passing
✅ **Completed**: Type system conflicts resolved (WatermarkStrategy, CircuitBreakerConfig)
✅ **Completed**: Circuit breaker pattern fixes with proper closure handling
✅ **Completed**: Production-ready CI/CD compliance validation

### Latest Achievement: Performance Test Infrastructure Completion

**Problem Solved**: Critical compilation failures in performance testing infrastructure
- Fixed complex type conflicts between `config::WatermarkStrategy` and `watermarks::WatermarkStrategy`
- Resolved CircuitBreakerConfig field mismatches and missing properties
- Fixed Rust closure borrowing issues in circuit breaker patterns
- Updated SystemTime to DateTime<Utc> conversions for proper StreamRecord compatibility

**Technical Implementation**:
- **Type System Fixes**: Proper module imports with aliases for conflicting types
- **Circuit Breaker Enhancement**: Added missing fields (failure_rate_window, min_calls_in_window, failure_rate_threshold)
- **Closure Pattern Fixes**: Used `move` keyword and variable extraction to resolve borrowing conflicts
- **Stream Processing**: Fixed enum variant usage and struct field access patterns

**Key Benefits**:
```rust
// Fixed type conflicts with proper module imports
use config::{WatermarkStrategy as ConfigWatermarkStrategy};
use watermarks::{WatermarkStrategy, WatermarkManager};

// Enhanced circuit breaker configuration
CircuitBreakerConfig {
    failure_threshold: 5,
    recovery_timeout: Duration::from_secs(60),
    failure_rate_window: Duration::from_secs(60),    // Added
    min_calls_in_window: 10,                         // Added
    failure_rate_threshold: 50.0,                    // Added
}

// Fixed closure borrowing patterns
let has_field = record.fields.get("id").is_some();
let result = circuit_breaker.execute(move || {
    let _processing = has_field;  // No borrowing conflict
    Ok(())
}).await;
```

The codebase is now **production-ready** for financial analytics use cases requiring exact precision and high performance. All performance testing infrastructure is operational and validated for continuous integration.
- Always run clippy checks#