# Test Categorization Guide

This document explains the test categorization system used in Velostream for optimizing CI/CD performance and developer workflows.

## Test Categories

### Fast Tests (Default - Run Always)
- **Library tests**: `cargo test --lib` (~3 seconds)
- **Unit tests**: Most unit tests (~10-15 seconds)
- **Doctests**: `cargo test --doc` (~4 seconds)
- **Total**: ~20 seconds

### Comprehensive Tests (Slow - Conditional)
- **Comprehensive failure scenarios**: Multi-job processor tests with realistic retry/backoff patterns
- **Runtime**: ~40-44 seconds per test
- **Purpose**: Production-ready failure handling validation

## Usage

### Running Fast Tests Only (Default)
```bash
# All fast tests - skips comprehensive tests by default
cargo test

# Or explicitly exclude comprehensive tests  
cargo test --lib
cargo test --tests  # Comprehensive tests ignored by default
```

### Running Comprehensive Tests
```bash
# Enable comprehensive tests with feature flag
cargo test --features comprehensive-tests

# Or run only comprehensive tests that are normally ignored
cargo test -- --ignored
```

### Running All Tests
```bash
# Run everything including comprehensive tests
cargo test --features comprehensive-tests
```

## CI/CD Integration

### GitHub Actions Optimization

#### Option 1: Separate Jobs (Recommended)
```yaml
name: Tests
on: [push, pull_request]

jobs:
  fast-tests:
    name: Fast Tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run fast tests
        run: |
          cargo test --lib
          cargo test --tests  # Comprehensive tests ignored
          cargo test --doc

  comprehensive-tests:
    name: Comprehensive Tests
    runs-on: ubuntu-latest
    needs: fast-tests  # Only run if fast tests pass
    if: github.ref == 'refs/heads/master' || github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4
      - name: Run comprehensive tests
        run: cargo test --features comprehensive-tests
        timeout-minutes: 10
```

#### Option 2: Conditional Comprehensive Tests
```yaml
- name: Fast tests
  run: cargo test
  
- name: Comprehensive tests (master branch only)
  if: github.ref == 'refs/heads/master'
  run: cargo test --features comprehensive-tests
```

## Test Implementation

### Adding Comprehensive Tests
Mark tests that take >30 seconds with the comprehensive tag:

```rust
#[tokio::test]
#[cfg_attr(not(feature = "comprehensive-tests"), ignore = "comprehensive: Slow test with 40+ second runtime - use cargo test --features comprehensive-tests")]
async fn test_comprehensive_failure_scenarios() {
    // Long-running test that simulates realistic failure conditions
    // with proper retry patterns, exponential backoff, timeouts, etc.
}
```

### Test Structure
```rust
// Fast test - always runs
#[tokio::test]
async fn test_basic_functionality() {
    // Quick validation test
}

// Comprehensive test - only with feature flag
#[tokio::test] 
#[cfg_attr(not(feature = "comprehensive-tests"), ignore = "comprehensive: Long runtime")]
async fn test_production_scenario() {
    // Thorough production-ready test
}
```

## Benefits

### Developer Experience
- **Fast feedback**: See basic compilation/functionality issues in 20 seconds
- **Optional deep testing**: Run comprehensive tests when needed
- **CI efficiency**: No wasted time on slow tests if basics fail

### CI/CD Performance
- **Fail-fast**: Basic issues caught quickly
- **Resource optimization**: Expensive tests only when appropriate
- **Parallel execution**: Fast and comprehensive tests can run in parallel

### Test Quality
- **Clear categorization**: Obvious distinction between basic and thorough tests
- **Production validation**: Comprehensive tests ensure production readiness
- **Maintained coverage**: All tests still run, just conditionally

## Current Implementation

### Comprehensive Tests (40+ seconds each):
- `test_simple_processor_comprehensive_failure_scenarios`
- `test_transactional_processor_comprehensive_failure_scenarios`

These tests validate:
- **7 failure scenarios**: Source failures, sink failures, disk full, network partition, partial batch failures, shutdown signals, empty batch handling
- **3 processing strategies**: LogAndContinue, RetryWithBackoff, FailBatch  
- **Realistic timing**: 5-second timeouts, 300ms delays, exponential backoff
- **Production patterns**: Proper retry logic, error recovery, graceful degradation

## Maintenance

### Adding New Comprehensive Tests
1. Identify tests with >30 second runtime
2. Add the comprehensive test annotation
3. Update this documentation
4. Consider CI/CD impact

### Monitoring Performance
- Fast tests should stay under 30 seconds total
- Comprehensive tests are expected to be slow
- Monitor CI/CD duration and adjust categories as needed

## Future Extensions

### Potential Additional Categories
- `integration`: Tests requiring external services
- `performance`: Benchmark/performance validation tests  
- `network`: Tests requiring network access
- `resource-intensive`: High CPU/memory tests

### Implementation Pattern
```rust
#[cfg_attr(not(feature = "category-name"), ignore = "category: Description")]
```

This tagging system provides a foundation for flexible test execution strategies while maintaining comprehensive validation coverage.