# Testing and Benchmarks Guide

**Complete guide to running tests and benchmarks in FerrisStreams**

This guide provides comprehensive instructions for executing the complete test suite and performance benchmarks in FerrisStreams, organized by purpose and execution time.

---

## 🚀 **Quick Start - Essential Commands**

### Core Development Tests (3-5 minutes)
```bash
# Fast feedback loop for development
cargo test --tests -- --skip integration:: --skip performance:: --skip comprehensive

# Include doctests for documentation validation
cargo test --tests --doc -- --skip integration:: --skip performance:: --skip comprehensive
```

### Complete Validation (8-15 minutes)
```bash
# Full test suite including comprehensive scenarios
cargo test --no-default-features

# Format check (required before commits)
cargo fmt --all -- --check
```

---

## 📋 **Test Categories**

### 1. **Fast Tests** ⚡ (3-5 minutes)
**Purpose**: Immediate feedback during development
**Coverage**: ~95% of functionality, all unit tests

```bash
# Library tests only (fastest)
cargo test --lib --no-default-features                    # ~11 seconds, 189 tests

# Unit tests (fast development cycle)
cargo test --tests -- --skip integration:: --skip performance:: --skip comprehensive  # ~3-4 minutes

# Documentation examples
cargo test --doc --no-default-features                    # ~4 seconds, 45 tests
```

### 2. **Integration Tests** 🔗 (2-3 minutes)
**Purpose**: End-to-end system validation
**Coverage**: Multi-component interaction testing

```bash
# All integration tests
cargo test integration:: --no-default-features

# Specific integration categories
cargo test --test ferris_sql_multi_test --no-default-features      # SQL server integration
cargo test --test phase2_configurable_serialization_test --no-default-features  # Serialization
```

### 3. **Performance Tests** 📊 (1-2 minutes)
**Purpose**: Performance regression detection
**Coverage**: Throughput and latency validation

```bash
# Quick performance validation
cargo test performance:: --no-default-features

# Financial precision benchmarks (key performance indicator)
cargo test financial_precision_benchmark --no-default-features -- --nocapture
```

### 4. **Comprehensive Tests** 🧪 (5-10 minutes)
**Purpose**: Production-ready failure scenario validation
**Coverage**: Complex failure recovery, timeout handling

```bash
# Full comprehensive test suite
cargo test comprehensive --no-default-features

# Specific comprehensive scenarios
cargo test test_simple_processor_comprehensive_failure_scenarios --no-default-features -- --nocapture
cargo test test_transactional_processor_comprehensive_failure_scenarios --no-default-features -- --nocapture
```

---

## 🔧 **Specialized Test Commands**

### SQL Batch Configuration Testing
```bash
# Test all batch strategies with WITH clause parsing
cargo run --bin test_batch_with_clause --no-default-features

# Performance validation of SQL batch configuration
cargo run --bin test_sql_batch_performance --no-default-features

# Simple batch configuration validation
cargo run --bin test_simple_processor_batch_configuration --no-default-features
```

### Performance Benchmarking
```bash
# Financial precision benchmark (163M+ records/sec)
cargo run --bin test_financial_precision --no-default-features

# Multi-job server performance
cargo run --bin test_streamrecord_performance --no-default-features

# Final performance validation
cargo run --bin test_final_performance --no-default-features
```

### Configuration Validation
```bash
# Kafka configuration testing
cargo run --bin test_kafka_configuration --no-default-features

# Schema registry validation
cargo run --bin test_schema_registry --no-default-features

# Configuration validator tool
cargo run --bin ferris-config-validator --no-default-features
```

---

## 🎯 **Performance Benchmarks Deep Dive**

### Key Performance Indicators

#### 1. **Financial Precision Benchmarks**
```bash
cargo test financial_precision_benchmark --no-default-features -- --nocapture
```
**Expected Results:**
- **f64 Aggregation**: 163M+ records/sec (with precision errors)
- **ScaledInteger**: 85M+ records/sec (exact precision) - **42x faster than f64 for financial calculations**
- **Decimal**: 275K+ records/sec (maximum precision)

#### 2. **SQL Query Performance**
```bash
cargo test performance:: --no-default-features -- --nocapture
```
**Expected Results:**
- **Simple SELECT**: ~142,000 records/sec (~7.05µs per record)
- **Filtered queries**: ~155,000 records/sec (~6.46µs per record)
- **GROUP BY**: ~57,700 records/sec (~17.3µs per record)
- **Window functions**: ~20,300 records/sec (~49.2µs per record)

#### 3. **Batch Processing Performance**
```bash
cargo run --bin test_sql_batch_performance --no-default-features
```
**Expected Results:**
- **Baseline (Single Record)**: 323,424 records/sec
- **Time Window Batch**: 365,018 records/sec (1.1x improvement - BEST)
- **All batch strategies**: 350K-365K records/sec range

---

## 🔍 **Test Execution Patterns**

### Development Workflow
```bash
# 1. Fast development cycle (run frequently)
cargo test --lib                                          # 11 seconds - immediate compilation feedback

# 2. Feature validation (run before commits)
cargo test --tests -- --skip comprehensive               # 3-4 minutes - thorough unit testing

# 3. Pre-commit validation (run before pushing)
cargo fmt --all -- --check && cargo test --no-default-features  # 8-15 minutes - complete validation
```

### CI/CD Execution (GitHub Actions)
```bash
# Fast Pipeline (always runs)
cargo test --tests -- --skip integration:: --skip performance:: --skip comprehensive

# Comprehensive Pipeline (master branch + labeled PRs)
cargo test --no-default-features
cargo test comprehensive --no-default-features
```

### Release Validation
```bash
# Complete pre-release validation sequence
cargo fmt --all -- --check          # Code formatting
cargo clippy --no-default-features  # Code quality
cargo test --no-default-features    # All tests
cargo build --examples              # Example compilation
cargo build --bins                  # Binary compilation
```

---

## 📊 **Performance Monitoring & Thresholds**

### GitHub Actions Performance Thresholds
The CI/CD system monitors performance regressions with these thresholds:

#### Streaming Performance
- **JSON Throughput**: >150 msg/s (CI-optimized threshold)
- **Raw Throughput**: >300 msg/s (CI-optimized threshold)
- **P95 Latency**: <1000ms (acceptable for CI environment)

#### SQL Performance
- **Baseline Throughput**: >200 rec/s (simple queries)
- **Complex Aggregation**: >50 rec/s (GROUP BY with complex expressions)

### Local Development Targets
Higher performance expected in local development:
- **JSON Processing**: 1000+ msg/s
- **Raw Bytes**: 5000+ msg/s
- **Financial Calculations**: 85M+ rec/s (ScaledInteger)

---

## 🛠️ **Troubleshooting**

### Common Issues

#### Tests Timing Out
```bash
# Increase timeout for comprehensive tests (expected behavior)
cargo test comprehensive --no-default-features -- --test-threads=1

# Run specific failing test with detailed output
RUST_BACKTRACE=1 cargo test test_name --no-default-features -- --nocapture
```

#### Performance Regression
```bash
# Compare performance between versions
cargo run --bin test_sql_batch_performance --no-default-features  # Current performance
git checkout previous_version
cargo run --bin test_sql_batch_performance --no-default-features  # Previous performance
```

#### Compilation Errors
```bash
# Check formatting first (most common issue)
cargo fmt --all -- --check

# Validate basic compilation
cargo check --no-default-features

# Full compilation validation
cargo build --bins --examples --no-default-features
```

---

## 📈 **Performance Test Results Archive**

### Historical Performance Data

#### September 2025 - Post-Objective 2 Completion
- **SQL Batch Configuration**: All 5 strategies validated
- **Peak Performance**: 365,018 records/sec (Time Window batch strategy)
- **Financial Processing**: 163M+ records/sec (f64), 85M+ records/sec (ScaledInteger)
- **Test Coverage**: 189 unit tests passing (100% success rate)

#### Key Performance Achievements
- **Performance Test Consolidation**: 83% code reduction achieved
- **GitHub Actions Optimization**: Test execution time reduced from 30s to 22ms
- **Financial Precision**: 42x performance improvement vs f64 for exact calculations

---

## 🎯 **Best Practices**

### For Development
1. **Run fast tests frequently** during development (`cargo test --lib`)
2. **Use comprehensive tests** before major commits
3. **Monitor performance** with benchmark commands after optimization work
4. **Check formatting** before every commit (`cargo fmt --all -- --check`)

### For CI/CD
1. **Fast tests** provide immediate feedback (3-5 minutes)
2. **Comprehensive tests** ensure production readiness (master branch)
3. **Performance thresholds** catch regressions automatically
4. **Parallel execution** maximizes CI efficiency

### For Release
1. **Complete test suite** must pass (`cargo test --no-default-features`)
2. **All binaries** must compile (`cargo build --bins --examples`)
3. **Performance benchmarks** should meet or exceed baselines
4. **Documentation tests** must validate (`cargo test --doc`)

This guide ensures comprehensive testing coverage while optimizing for developer productivity and CI/CD efficiency.