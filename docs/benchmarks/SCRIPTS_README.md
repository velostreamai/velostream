# Baseline Testing Scripts - Complete Guide

## Overview

This directory contains 4 optimized shell scripts for running the FR-082 comprehensive baseline comparison test with flexible control over:
- **Compilation mode** (release, debug, profile, incremental)
- **Test coverage** (all 5 scenarios or individual scenarios)
- **Event count** (100K default, supports 1m, 10m, 500k, etc.)
- **Performance targets** (fastest runtime, fastest compile, best cache usage)

## Scripts at a Glance

### 1. `run_baseline_flexible.sh` ⭐ **RECOMMENDED**

**Most versatile - choose mode, scenarios, AND event count**

```bash
cd benchmarks
./run_baseline_flexible.sh [mode] [scenario] [-events <count>]
```

**Parameters:**
- `mode`: `release` (default), `debug`, `profile`, `fast-release`
- `scenario`: `all` (default), `1`, `2`, `3`, `4`, `5`
- `-events <count>`: Event count (default: 100,000)
  - Supports: plain numbers, `1m`, `10m`, `500k`, etc.

**Examples:**
```bash
cd benchmarks

# All scenarios, release build, 100K events (fastest runtime)
./run_baseline_flexible.sh

# Single scenario, debug build, 100K events (fastest compile)
./run_baseline_flexible.sh debug 1

# Scenario 4 with profiling, 100K events
./run_baseline_flexible.sh profile 4

# All scenarios, fast incremental release, 1M events
./run_baseline_flexible.sh fast-release all -events 1m

# Scenario 3, debug mode, 500K events
./run_baseline_flexible.sh debug 3 -events 500k
```

**Performance:**
- Single scenario + release: **60s compile → 2-3s test**
- Single scenario + debug: **15s compile → 6-8s test**
- All scenarios + release: **60s compile → 8-12s test**
- All scenarios + debug: **15s compile → 30-40s test**

---

### 2. `run_baseline.sh` 🏆

**Simplest - release build, all scenarios, optional event count**

```bash
cd benchmarks
./run_baseline.sh [event_count]
```

**Parameters:**
- `event_count`: Event count (default: 100,000)
  - Supports: plain numbers, `1m`, `10m`, `500k`, etc.

**Examples:**
```bash
cd benchmarks

# Default 100K events
./run_baseline.sh

# 1M events for stress testing
./run_baseline.sh 1m

# 500K events
./run_baseline.sh 500k
```

**Best for:** Final benchmarks, CI/CD, production measurements

**Performance (100K events):**
- Compile: ~60s
- Test: ~8-12s
- Runtime: 3-5x faster than debug

---

### 3. `run_baseline_quick.sh` ⚡

**Incremental compilation - uses cache aggressively, optional event count**

```bash
cd benchmarks
./run_baseline_quick.sh [event_count]
```

**Parameters:**
- `event_count`: Event count (default: 100,000)
  - Supports: plain numbers, `1m`, `10m`, `500k`, etc.

**Examples:**
```bash
cd benchmarks

# Default 100K events
./run_baseline_quick.sh

# 1M events for stress testing
./run_baseline_quick.sh 1m

# 500K events
./run_baseline_quick.sh 500k
```

**Best for:** Rapid iteration when code hasn't changed much

**Performance (100K events):**
- First run: ~15s compile
- Cache hits: ~5-10s compile
- Test: ~30-40s (debug-like speeds but with some optimization)
- Subsequent runs: **VERY FAST** if no code changes

---

### 4. `run_baseline_options.sh` 🔧

**Multiple compilation modes - all scenarios, optional event count**

```bash
cd benchmarks
./run_baseline_options.sh [mode] [event_count]
```

**Parameters:**
- `mode`: Compilation mode (default: `release`)
  - `release`: Full optimizations (LTO, codegen-units=1)
  - `debug`: No optimizations (fastest compile)
  - `profile`: Release + debug symbols (for flamegraph)
  - `fast-release`: Incremental with parallel compilation
- `event_count`: Event count (default: 100,000)
  - Supports: plain numbers, `1m`, `10m`, `500k`, etc.

**Examples:**
```bash
cd benchmarks

# Release mode, 100K events (default)
./run_baseline_options.sh

# Debug mode, 100K events
./run_baseline_options.sh debug

# Release mode, 1M events
./run_baseline_options.sh release 1m

# Debug mode, 500K events
./run_baseline_options.sh debug 500k

# Profile mode, 100K events
./run_baseline_options.sh profile
```

**Best for:** Exploring different compilation strategies and event counts

---

## Quick Decision Tree

```
cd benchmarks

Want to test SINGLE scenario?
├─ YES, want fastest runtime? → ./run_baseline_flexible.sh release 1
├─ YES, want fastest compile? → ./run_baseline_flexible.sh debug 1
└─ NO (all scenarios)
   ├─ Want fastest runtime? → ./run_baseline.sh
   ├─ Want fastest compile? → ./run_baseline_flexible.sh debug
   ├─ Want cache optimization? → ./run_baseline_quick.sh
   └─ Want profiling? → ./run_baseline_flexible.sh profile
```

---

## Scenario Details

| Scenario | Description | Use Case |
|----------|-------------|----------|
| **1** | Pure SELECT (no aggregation) | Baseline throughput |
| **2** | ROWS window | Window function performance |
| **3** | Pure GROUP BY | Aggregation performance |
| **4** | TUMBLING window (standard emit) | Complex windowing |
| **5** | TUMBLING window (emit changes) | Late-arriving data handling |

Each scenario tests 5 implementations:
- SQL Engine (baseline)
- V1 SimpleJobProcessor (single-threaded, best-effort)
- Transactional JobProcessor (single-threaded, at-least-once)
- V2 AdaptiveJobProcessor @ 1-core (minimal parallelism)
- V2 AdaptiveJobProcessor @ 4-core (maximum parallelism)

---

## Compilation Modes Explained

### `release` (Default)
- **LTO enabled** (Link-Time Optimization)
- **codegen-units = 1** (maximum optimization)
- **Symbols stripped** (reduced binary size)
- **Best runtime performance** (3-5x faster than debug)
- Compile time: ~60 seconds

### `debug`
- **No optimizations**
- **Full debug symbols**
- **Fastest compilation**
- Compile time: ~15 seconds
- Runtime: Baseline (1x speed)

### `profile`
- **Release optimizations**
- **Debug symbols preserved**
- **Use with flamegraph/perf**
- Compile time: ~60 seconds
- Runtime: Same as release (3-5x faster)

### `fast-release`
- **Incremental compilation cache**
- **Parallel build jobs (4)**
- **Fewer codegen units (256 instead of 1)**
- **Slightly lower optimization than release**
- Compile time: ~15s (cache hit) or ~60s (cache miss)
- Runtime: Still very fast (2-3x faster than debug)

---

## Performance Expectations

### Complete Benchmark (All 5 Scenarios × 5 Implementations = 25 tests)

| Mode | Compile | Test | Total | Best For |
|------|---------|------|----------|----------|
| Release | 60s | 8–12s | ~70–72s | **Final results** |
| Debug | 15s | 30–40s | ~45–55s | Quick feedback |
| Quick (cold) | 15s | 30–40s | ~45–55s | First iteration |
| Quick (warm) | 5–10s | 30–40s | ~35–50s | Subsequent runs |
| Profile | 60s | 8–12s | ~70–72s | Profiling analysis |

### Single Scenario Test (1 Scenario × 5 Implementations = 5 tests)

| Mode | Compile | Test | Total | Best For |
|------|---------|------|----------|----------|
| Release | 60s | 2–3s | ~62–63s | Single scenario verification |
| Debug | 15s | 6–8s | ~21–23s | **Fastest feedback** |
| Profile | 60s | 2–3s | ~62–63s | Single scenario profiling |

---

## Common Workflows

### Development & Debugging
```bash
cd benchmarks

# Quick iteration with single scenario
./run_baseline_flexible.sh debug 1

# Or all scenarios with quick compile
./run_baseline_flexible.sh debug
```

### Before Commit
```bash
cd benchmarks

# Full benchmark with best performance
./run_baseline.sh

# Or just verify one scenario
./run_baseline_flexible.sh release 1
```

### Performance Analysis
```bash
cd benchmarks

# With profiling symbols for flamegraph
./run_baseline_flexible.sh profile

# Or specific scenario for detailed analysis
./run_baseline_flexible.sh profile 2
```

### CI/CD Verification
```bash
cd benchmarks

# In Makefile or CI config
./run_baseline.sh
```

---

## Output Format

Each test prints:
- Scenario name and SQL description
- SQL Engine baseline throughput
- Records sent vs processed for each implementation
- Throughput for each processor implementation
- Performance ratios vs SQL Engine baseline
- Best performing implementation

Example:
```
🔬 SCENARIO 0: Pure SELECT
─────────────────────────────────────────────────────────────
  ✓ SQL Engine: 105433 rec/sec
  Records sent: 10000 | Processed: 10000
  ✓ V1 (1-thread): 98765 rec/sec
  ✓ V2 @ 1-core: 102345 rec/sec
  ✓ V2 @ 4-core: 145678 rec/sec
```

---

## Troubleshooting

### Scripts Not Found
```bash
chmod +x run_baseline*.sh
```

### Out of Memory (Release Build)
- Release builds with LTO use significant RAM
- Option 1: Use debug mode: `./run_baseline_flexible.sh debug`
- Option 2: Use single scenario: `./run_baseline_flexible.sh release 1`
- Option 3: Add swap space

### Results Seem Inconsistent
- Close other applications
- Run multiple times (first run is usually slower due to cache warming)
- Use `--test-threads=1` (already in scripts) to prevent CPU contention

### Slow Compilation
- First run of release builds takes longer (LTO is slow but worth it)
- Use `fast-release` mode if code hasn't changed: `./run_baseline_flexible.sh fast-release`
- Or use debug for quick feedback: `./run_baseline_flexible.sh debug`

### Very Large Output
- All output is expected - 25 benchmarks × multiple metrics
- Redirect to file if needed: `./run_baseline.sh > results.txt 2>&1`

---

## Related Documentation

- **BASELINE_TESTING.md** - Comprehensive reference with all details
- **BASELINE_QUICK_REFERENCE.md** - Quick TL;DR cheat sheet
- **CLAUDE.md** - General development guide
- **V2_CODE_LOCATIONS.txt** - V2 processor architecture
- **tests/performance/analysis/comprehensive_baseline_comparison.rs** - Test source code

---

## Implementation Details

All scripts use:
- `--no-default-features` - Reduce compilation time and binary size
- `--test-threads=1` - Single-threaded execution for accurate timing
- `--nocapture` - Show all test output
- Release build with LTO - Maximum runtime performance

The tests themselves are in: `tests/performance/analysis/comprehensive_baseline_comparison.rs`

---

## Tips for Best Results

1. **Close other applications** before running to prevent CPU contention
2. **Run on a consistent machine** for comparable results
3. **First run is slower** - allow cargo to warm up caches
4. **Use `release` mode** for production/final benchmarks
5. **Use `debug` mode** for rapid iteration during development
6. **Use `profile` mode** when doing performance analysis
7. **Single scenarios** are great for testing changes to specific code paths
8. **Save output** to file for later analysis: `./run_baseline.sh > log.txt 2>&1`

---

## Version History

- **v1.0** (Nov 14, 2025): Initial release with 4 scripts and flexible options
  - Added `run_baseline_flexible.sh` for maximum flexibility
  - `run_baseline.sh` for quick benchmark runs
  - `run_baseline_quick.sh` for cache-optimized iteration
  - `run_baseline_options.sh` for mode exploration

---

## Questions?

See the documentation files or examine the script source code directly:
- Each script has detailed comments explaining what it does
- Help is available: `./run_baseline_flexible.sh help`

---

**Happy benchmarking! 🚀**
