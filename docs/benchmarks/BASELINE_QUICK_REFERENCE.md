# Baseline Testing - Quick Reference

## TL;DR - Choose Your Script

### 🏆 Best Overall Performance
```bash
cd benchmarks
./run_baseline.sh
```
**60s compile → 8-12s test** (all 5 scenarios, 3-5x faster than debug)

### ⚡ Fastest Iteration (if code unchanged)
```bash
cd benchmarks
./run_baseline_quick.sh
```
**~15s first run → ~5-10s on cache hits** (incremental compilation, all scenarios)

### 🐞 Quick Debug/Dev
```bash
cd benchmarks
./run_baseline_options.sh debug
```
**15s compile → 30-40s test** (best for quick feedback loops, all scenarios)

### 🎯 Flexible: Choose Mode + Scenarios
```bash
cd benchmarks
./run_baseline_flexible.sh [mode] [scenario]
```
Pick compilation mode AND which scenario(s) to run:
- `./run_baseline_flexible.sh release 1` - Scenario 1 only, fast runtime
- `./run_baseline_flexible.sh debug 4` - Scenario 4 only, fast compile
- `./run_baseline_flexible.sh release` - All scenarios, fast runtime (default)

---

## Quick Comparison

| Script | Compile | Runtime | When to Use |
|--------|---------|---------|-------------|
| `run_baseline.sh` | 60s | 8–12s (all) | **Recommended for final benchmarks** |
| `run_baseline_quick.sh` | 15s* | 30–40s (all) | Quick iterations with cache hits |
| `run_baseline_flexible.sh release 1` | 60s | 2–3s (one) | **Single scenario testing, fast** |
| `run_baseline_flexible.sh debug 1` | 15s | 6–8s (one) | Single scenario, quick compile |
| `run_baseline_options.sh release` | 60s | 8–12s (all) | Same as `run_baseline.sh` |
| `run_baseline_options.sh debug` | 15s | 30–40s (all) | Quick dev feedback |
| `run_baseline_options.sh profile` | 60s | 8–12s (all) | Flamegraph profiling |

\* ~60s if code changed, ~5–10s if unchanged (cache hit)
(all) = all 5 scenarios × 5 implementations = 25 benchmarks
(one) = 1 scenario × 5 implementations = 5 benchmarks

---

## Typical Workflow

### During Development
```bash
cd benchmarks

# Fast iteration
./run_baseline_quick.sh

# Or for quick feedback
./run_baseline_options.sh debug
```

### Before Commit
```bash
cd benchmarks

# Verify actual performance
./run_baseline.sh
```

### Performance Analysis
```bash
cd benchmarks

# With profiling data
./run_baseline_options.sh profile
```

---

## Output Explains Itself

The test automatically prints:
- Records sent vs processed for each implementation
- Throughput (records/sec) for all 25 combinations (5 scenarios × 5 impls)
- Performance ratios vs SQL Engine baseline
- Total test runtime

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

**Q: Script not found?**
```bash
chmod +x run_baseline*.sh
```

**Q: Too slow on first run?**
- Normal for release builds with LTO
- Try `run_baseline_quick.sh` for faster compile
- Or `run_baseline_options.sh debug` for quickest

**Q: Out of memory?**
- Release builds with LTO use more RAM
- Use debug mode: `./run_baseline_options.sh debug`
- Or add swap space

**Q: Results seem wrong?**
- Ensure `--test-threads=1` (no parallel tests)
- Close other apps
- Run multiple times to warm up cache

---

## What Gets Tested

5 scenarios × 5 implementations = 25 benchmarks
- SQL Engine (baseline)
- V1 SimpleJobProcessor
- Transactional JobProcessor
- V2 AdaptiveJobProcessor @ 1-core
- V2 AdaptiveJobProcessor @ 4-core

Each test measures:
- Records processed
- Throughput (rec/sec)
- Performance vs baseline

See `BASELINE_TESTING.md` for full details.
