# Infrastructure Performance Tests - Consolidation Plan

**Status**: Phase 2 Planning
**Date**: November 21, 2024

## Overview

This document outlines the consolidation strategy for 12 infrastructure performance tests currently in `examples/performance/`. These tests validate low-level implementation quality and are distinct from SQL operations performance tests.

## Current File Inventory

### Infrastructure Tests (12 files, 147KB)

| File | Size | Purpose | Complexity | Status |
|------|------|---------|-----------|--------|
| phase4_batch_benchmark.rs | 19K | Batch strategy optimization | HIGH (async, Kafka simulation) | ⏳ Pending |
| datasource_performance_test.rs | 16K | Abstraction layer overhead | HIGH (Kafka required) | ⏳ Pending |
| json_performance_test.rs | 15K | JSON serialization throughput | HIGH (Kafka required) | ⏳ Pending |
| raw_bytes_performance_test.rs | 15K | Raw byte processing | HIGH (Kafka required) | ⏳ Pending |
| latency_performance_test.rs | 12K | End-to-end latency measurement | HIGH (Kafka required) | ⏳ Pending |
| table_performance_benchmark.rs | 12K | Table lookup O(1) validation | MEDIUM | ⏳ Pending |
| simple_async_optimization_test.rs | 9.0K | Async pattern optimization | HIGH (Kafka required) | ⏳ Pending |
| simple_zero_copy_test.rs | 8.0K | Zero-copy pattern validation | HIGH (Kafka required) | ⏳ Pending |
| resource_monitoring_test.rs | 7.0K | Resource usage tracking | MEDIUM | ⏳ Pending |
| join_performance.rs | 6.0K | JOIN operation benchmarks | MEDIUM (criterion-based) | ❌ RETIRE |
| quick_performance_test.rs | 5.9K | Quick regression validation | LOW | ❌ RETIRE |
| **stream_table_join_baseline_benchmark.rs** | **13K** | **Moved to Tier 1** | **N/A** | **✅ DONE** |

## Consolidation Strategy

### Phase 2A: Non-Kafka Low-Complexity Tests (Immediate)

These tests can be moved directly to `tests/performance/analysis/infrastructure/` with minimal conversion:

1. **table_performance_benchmark.rs** → `table_lookup.rs`
   - Measures OptimizedTableImpl performance
   - No Kafka dependency
   - Can use #[test] directly
   - Purpose: Validate O(1) lookup optimization

2. **resource_monitoring_test.rs** → `resource_profiling.rs`
   - Tracks memory/CPU usage
   - No Kafka dependency
   - Can use #[test] directly
   - Purpose: Detect resource regression

### Phase 2B: Retirement (Immediate)

These tests are superseded and can be removed:

1. **join_performance.rs**
   - Reason: Superseded by stream_table_join.rs (now in Tier 1)
   - Use case: JOIN benchmarking now covered by dedicated operation tests

2. **quick_performance_test.rs**
   - Reason: Superseded by comprehensive_baseline_comparison.rs
   - Use case: Regression validation now handled by comprehensive test

### Phase 2C: Kafka-Dependent Tests (Deferred - Phase 2B)

These tests require Kafka running and complex async setup. Strategy:

**Option A: Keep as Examples** (Recommended)
- Leave in `examples/performance/` for users who want to run locally with Kafka
- Add documentation about Kafka setup requirements
- Don't try to convert to #[test] format
- Reason: They're useful as runnable examples, not regression tests

**Option B: Create Test Wrapper** (Alternative)
- Create minimal #[test] wrapper in infrastructure/
- Wrapper checks if Kafka available, skips if not
- Points to examples/ for full testing
- Adds complexity without much benefit

**Option C: Extract Core Logic** (Future)
- Extract performance measurement logic from Kafka integration
- Create standalone benchmarks with mock data
- Requires significant refactoring
- Better long-term but high effort

**Recommendation**: Keep as examples (Option A). Document in README.md

## Implementation Plan

### Step 1: Create Infrastructure Directory Structure
```
tests/performance/analysis/infrastructure/
├── mod.rs
├── README.md
├── table_lookup.rs              [MOVE from examples - table_performance_benchmark.rs]
└── resource_profiling.rs        [MOVE from examples - resource_monitoring_test.rs]
```

### Step 2: Move & Convert Simple Tests

**table_lookup.rs**: OptimizedTableImpl Performance
- Source: `examples/performance/table_performance_benchmark.rs` (12KB)
- Status: Convert main() → #[test]
- Key metrics:
  - O(1) lookup performance
  - Query caching effectiveness
  - Column indexing speed
  - Streaming throughput
  - Memory efficiency

**resource_profiling.rs**: Resource Usage Monitoring
- Source: `examples/performance/resource_monitoring_test.rs` (7KB)
- Status: Convert to #[test]
- Key metrics:
  - CPU usage tracking
  - Memory allocation patterns
  - GC pressure detection
  - Resource efficiency ratios

### Step 3: Retire Redundant Tests
```bash
# Delete from examples/performance/
rm join_performance.rs
rm quick_performance_test.rs
```

Rationale:
- **join_performance.rs**: Stream-Table JOIN now has dedicated Tier 1 benchmark
- **quick_performance_test.rs**: Comprehensive baseline comparison is better regression test

### Step 4: Document Kafka Tests in README

Create `tests/performance/analysis/infrastructure/README.md`:
- List of Kafka-dependent tests
- How to run them locally with Kafka
- Which tests require what infrastructure
- Recommendation: Optional testing, not CI/CD

## Kafka-Dependent Tests (Keep in examples/)

These will remain as examples with documentation:

1. **phase4_batch_benchmark.rs** (19KB)
   - Tests MegaBatch, RingBatchBuffer, ParallelBatchProcessor
   - Requires Kafka simulation
   - CLI: `cargo run --example phase4_batch_benchmark` (quick|default|production)

2. **datasource_performance_test.rs** (16KB)
   - Tests abstraction layer overhead
   - Requires Kafka
   - CLI: `cargo run --example datasource_performance_test`

3. **json_performance_test.rs** (15KB)
   - Tests JSON serialization throughput
   - Requires Kafka
   - CLI: `cargo run --example json_performance_test`

4. **raw_bytes_performance_test.rs** (15KB)
   - Tests raw byte processing
   - Requires Kafka
   - CLI: `cargo run --example raw_bytes_performance_test`

5. **latency_performance_test.rs** (12KB)
   - Tests end-to-end latency
   - Requires Kafka
   - CLI: `cargo run --example latency_performance_test`

6. **simple_async_optimization_test.rs** (9KB)
   - Tests async pattern optimization
   - Requires Kafka
   - CLI: `cargo run --example simple_async_optimization_test`

7. **simple_zero_copy_test.rs** (8KB)
   - Tests zero-copy patterns
   - Requires Kafka
   - CLI: `cargo run --example simple_zero_copy_test`

## Benefits of This Approach

1. **Clear Separation**: Infrastructure tests vs SQL operations
2. **CI/CD Integration**: Simple tests run in CI, Kafka tests optional
3. **User Flexibility**: Kafka tests available as examples for interested users
4. **Low Refactoring**: Don't try to force-convert complex Kafka tests
5. **Documentation**: Clear guidance on what requires what infrastructure
6. **Maintainability**: Examples stay simple, test suite stays lean

## Timeline

- **Phase 2A (Immediate)**: Move simple tests, retire redundant tests
  - Effort: 2-3 hours
  - Files affected: 4 (table_lookup, resource_profiling, remove 2)

- **Phase 2B (Deferred)**: Document Kafka tests strategy
  - Effort: 1 hour
  - Files affected: 1 (README.md)

- **Phase 3 (Parallel)**: Measure Tier 2-4 SQL operations
  - Can proceed independently
  - Doesn't depend on Phase 2B completion

## Risk Mitigation

**Risk**: Kafka tests become unmaintained if left in examples/
**Mitigation**: Document clearly, mark as optional for local testing only

**Risk**: Simple tests miss edge cases not covered by Kafka version
**Mitigation**: Kafka tests provide option for deeper testing

**Risk**: Users confused about which tests to run when
**Mitigation**: Create comprehensive README explaining each test category

## Success Criteria

- ✅ Simple infrastructure tests moved to `tests/performance/analysis/infrastructure/`
- ✅ Redundant tests removed from examples/
- ✅ All infrastructure tests compile and pass
- ✅ README documents purpose of each test
- ✅ Documentation explains how to run Kafka tests locally
- ✅ No regression in existing test suite

## References

- Phase 1 Complete: Stream-Table JOIN reorganized into Tier 1 operations
- Goal: Enable iterative performance measurement tied to SQL operations ranking
- Next: Phase 3 - Measure Tier 2-4 operations while Phase 2B documentation is created

---

**Status**: Ready to implement Phase 2A (simple tests) and Phase 2B (documentation)
