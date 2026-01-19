# Benchmarking Guide

Velostream includes comprehensive benchmarking infrastructure to measure and validate performance across different execution models, streaming scenarios, and optimization strategies.

## 📊 Benchmark Categories

### 1. **FR-082 Comprehensive Baseline Comparison** (Latest)
Real-time performance comparison across execution models with 5 realistic SQL scenarios.

**Files:**
- [`SCRIPTS_README.md`](SCRIPTS_README.md) - Complete reference guide for baseline scripts
- [`BASELINE_TESTING.md`](BASELINE_TESTING.md) - Detailed testing methodology and setup
- [`BASELINE_QUICK_REFERENCE.md`](BASELINE_QUICK_REFERENCE.md) - Quick cheat sheet

**Key Features:**
- 5 SQL scenarios (Pure SELECT, ROWS WINDOW, GROUP BY, TUMBLING WINDOW standard, TUMBLING WINDOW emit changes)
- 4 implementations (SQL Engine baseline, V1 single-threaded, V2 1-core, V2 4-core)
- Flexible execution with configurable modes and scenarios
- Comprehensive throughput measurements (records/sec)
- Performance ratios vs SQL Engine baseline

**Quick Start:**
```bash
# All scenarios, release build (recommended)
./run_baseline.sh

# Flexible: choose mode and scenarios
./run_baseline_flexible.sh release 1    # Release, scenario 1 only
./run_baseline_flexible.sh debug        # Debug, all scenarios
```

**Performance Expectations:**
| Mode | Compile | Test | Total |
|------|---------|------|-------|
| Release (all) | 60s | 8–12s | ~70s |
| Debug (all) | 15s | 30–40s | ~45s |
| Release (single) | 60s | 2–3s | ~62s |
| Debug (single) | 15s | 6–8s | ~21s |

---

### 2. **FR-085 Stream-Stream Join Benchmarks** (NEW)
Performance benchmarks for interval-based stream-stream join infrastructure.

**File:** `tests/performance/analysis/sql_operations/tier3_advanced/interval_stream_join.rs`

**Key Features:**
- JoinCoordinator throughput (direct API and SQL engine)
- JoinStateStore store/lookup/expiration performance
- Memory-bounded operation validation
- High cardinality stress testing

**Quick Start:**
```bash
# Run all interval join benchmarks
cargo test --release --no-default-features interval_stream_join -- --nocapture

# Run specific benchmark
cargo test --release --no-default-features test_interval_stream_join_performance -- --nocapture
```

**Performance Results (2026-01-18):**

| Component | Throughput | Notes |
|-----------|------------|-------|
| JoinStateStore | 5.3M rec/sec | BTreeMap operations |
| High Cardinality | 1.7M rec/sec | Unique keys stress test |
| JoinCoordinator | 607K rec/sec | Full join pipeline |
| SQL Engine (sync) | 364K rec/sec | End-to-end execution |
| SQL Engine (async) | 228K rec/sec | Async pipeline |

---

### 3. **Phase 4 Batch Strategy Benchmarks**
Validates batch processing optimizations and streaming strategy performance.

**File:** [`phase4-batch-benchmarks.md`](phase4-batch-benchmarks.md)

**Coverage:**
- RingBatchBuffer allocation reuse (20-30% improvement)
- ParallelBatchProcessor multi-core scalability (2-4x improvement)
- MegaBatch strategy throughput (targeting 8.37M+ rec/sec)
- Table loading performance with large batches

**Run:**
```bash
# Full benchmark (1M records)
cargo run --bin phase4_batch_benchmark --no-default-features

# Quick test (100K records)
cargo run --bin phase4_batch_benchmark --no-default-features quick

# Production scale (10M records)
cargo run --bin phase4_batch_benchmark --no-default-features production
```

---

## 🎯 Benchmark Selection Guide

**Choose FR-082 Baseline Comparison if you want to:**
- Compare execution models (V1 vs V2)
- Measure real-world SQL workload performance
- Validate end-to-end streaming throughput
- Get quick feedback during development
- Run specific scenarios for targeted testing

**Choose FR-085 Stream-Stream Join Benchmarks if you want to:**
- Measure stream-stream join throughput
- Validate JoinCoordinator and JoinStateStore performance
- Test memory-bounded join operations
- Benchmark high cardinality scenarios
- Compare sync vs async SQL execution paths

**Choose Phase 4 Batch Benchmarks if you want to:**
- Validate batch optimization strategies
- Test allocation reuse improvements
- Measure multi-core scalability
- Validate table loading performance
- Optimize batch processing pipeline

---

## 📈 Performance Monitoring

All benchmarks produce:
1. **Throughput metrics** (records/sec) for each implementation
2. **Performance ratios** vs baseline for easy comparison
3. **Detailed logs** of record counts (sent vs processed)
4. **Timing breakdown** for compilation and test execution

---

## 🔄 CI/CD Integration

The FR-082 baseline comparison is designed for CI/CD:

```bash
# In your CI/CD pipeline
./run_baseline.sh

# Or for faster feedback in development
./run_baseline_options.sh debug
```

Results are printed to stdout and can be captured for analysis or archiving.

---

## 📚 Related Documentation

- **[SQL Functions](../sql/functions/)** - Query capabilities tested in benchmarks
- **[Architecture](../architecture/)** - Execution model details
- **[Performance Tuning](../performance/)** - Optimization strategies
- **[Development Guide](../../CLAUDE.md)** - Testing and validation procedures

---

## 🚀 Performance Targets

### Throughput Goals

| Scenario | Baseline | V1 (1-thread) | V2 @ 1-core | V2 @ 4-core |
|----------|----------|---------------|------------|------------|
| Pure SELECT | ~100K+ | ~95K+ | ~100K+ | ~140K+ |
| ROWS WINDOW | ~80K+ | ~75K+ | ~80K+ | ~120K+ |
| GROUP BY | ~60K+ | ~55K+ | ~60K+ | ~100K+ |
| TUMBLING WINDOW | ~50K+ | ~45K+ | ~50K+ | ~80K+ |
| EMIT CHANGES | ~40K+ | ~35K+ | ~40K+ | ~70K+ |

### Optimization Targets

- **Baseline Suite**: 3-5x faster release builds than debug
- **Batch Processing**: 20-30% improvement from allocation reuse
- **Multi-core**: 2-4x improvement on 4-core systems
- **Window Operations**: Consistent latency regardless of batch size

---

## 🛠️ Troubleshooting

**Slow compilation on release builds?**
- Normal for LTO (Link-Time Optimization)
- Use debug mode for faster iteration: `./run_baseline_flexible.sh debug`
- Or fast-release mode: `./run_baseline_flexible.sh fast-release`

**Out of memory?**
- Release builds with LTO use significant RAM
- Use debug mode or single scenarios
- Option: Add swap space

**Results seem inconsistent?**
- Close other applications
- Run multiple times (first run warms cache)
- Use `--test-threads=1` (already configured in scripts)

**Need more details?**
- See `SCRIPTS_README.md` troubleshooting section
- Check `BASELINE_TESTING.md` for methodology details
- Review `BASELINE_QUICK_REFERENCE.md` for quick answers

---

## 📝 Version History

- **v1.0 (Nov 14, 2025)**: Initial release with FR-082 baseline comparison scripts
  - 4 flexible baseline testing scripts
  - 3 comprehensive documentation files
  - Support for all 5 SQL scenarios
  - Configurable compilation modes (release/debug/profile/fast-release)

---

**Happy benchmarking! 🚀**
