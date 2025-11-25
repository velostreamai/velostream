# Baseline Testing - Quick Reference

## TL;DR - Choose Your Script

### 🏆 Best Overall Performance
```bash
cd benchmarks
./run_baseline.sh              # 100K events (default)
./run_baseline.sh 1m           # 1M events for stress testing
```
**60s compile → 8-12s test** (all 5 scenarios, 3-5x faster than debug)

### ⚡ Fastest Iteration (if code unchanged)
```bash
cd benchmarks
./run_baseline_quick.sh        # 100K events (default)
./run_baseline_quick.sh 500k   # 500K events
```
**~15s first run → ~5-10s on cache hits** (incremental compilation, all scenarios)

### 🐞 Quick Debug/Dev
```bash
cd benchmarks
./run_baseline_options.sh debug           # 100K events (default)
./run_baseline_options.sh debug 1m        # 1M events
```
**15s compile → 30-40s test** (best for quick feedback loops, all scenarios)

### 🎯 Flexible: Choose Mode + Scenarios + Events
```bash
cd benchmarks
./run_baseline_flexible.sh [mode] [scenario] [-events <count>]
```
Pick compilation mode, scenario(s), AND event count:
- `./run_baseline_flexible.sh release 1` - Scenario 1, 100K events (default)
- `./run_baseline_flexible.sh release 1 -events 1m` - Scenario 1, 1M events
- `./run_baseline_flexible.sh debug all -events 500k` - All scenarios, 500K events
- `./run_baseline_flexible.sh release` - All scenarios, 100K events (default)

---

## Quick Comparison

| Script | Compile | Runtime | Events | When to Use |
|--------|---------|---------|--------|-------------|
| `run_baseline.sh` | 60s | 8–12s (all) | 100K default | **Recommended for final benchmarks** |
| `run_baseline.sh 1m` | 60s | 40–60s (all) | 1M stress test | Load testing, scaling analysis |
| `run_baseline_quick.sh` | 15s* | 30–40s (all) | 100K default | Quick iterations with cache hits |
| `run_baseline_quick.sh 500k` | 15s* | 2–3m (all) | 500K load test | Medium load testing |
| `run_baseline_flexible.sh release 1` | 60s | 2–3s (one) | 100K default | **Single scenario testing, fast** |
| `run_baseline_flexible.sh release 1 -events 1m` | 60s | 10–15s (one) | 1M stress | Single scenario stress test |
| `run_baseline_flexible.sh debug 1` | 15s | 6–8s (one) | 100K default | Single scenario, quick compile |
| `run_baseline_options.sh release` | 60s | 8–12s (all) | 100K default | Same as `run_baseline.sh` |
| `run_baseline_options.sh debug` | 15s | 30–40s (all) | 100K default | Quick dev feedback |
| `run_baseline_options.sh profile` | 60s | 8–12s (all) | 100K default | Flamegraph profiling |

\* ~60s if code changed, ~5–10s if unchanged (cache hit)
(all) = all 5 scenarios × implementations
(one) = 1 scenario × implementations

**Supported Event Count Formats:**
- Plain numbers: `100000`, `500000`
- Millions: `1m`, `10m` (case-insensitive)
- Thousands: `1k`, `500k` (case-insensitive)

---

## Typical Workflow

### During Development (100K events)
```bash
cd benchmarks

# Fast iteration
./run_baseline_quick.sh

# Or for quick feedback
./run_baseline_options.sh debug
```

### Before Commit (100K events)
```bash
cd benchmarks

# Verify actual performance
./run_baseline.sh
```

### Load Testing / Scaling Analysis
```bash
cd benchmarks

# Test with 1M events to see scaling behavior
./run_baseline.sh 1m

# Or with 500K events
./run_baseline_quick.sh 500k
```

### Performance Analysis (100K events)
```bash
cd benchmarks

# With profiling data
./run_baseline_options.sh profile
```

### Custom Event Counts
```bash
cd benchmarks

# Any of these work:
./run_baseline.sh 100000        # Explicit number
./run_baseline.sh 10m           # 10 million events
./run_baseline_options.sh release 5m    # Mode + 5M events
./run_baseline_flexible.sh release 1 -events 1m   # Mode, scenario, 1M events
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
