# FR-082: Comprehensive Baseline Comparison Testing

Scripts for running the comprehensive baseline comparison test with optimal performance settings.

## Quick Start

### Fastest Runtime Performance (Recommended)
```bash
cd benchmarks
./run_baseline.sh
```

Produces:
- **Compilation time**: ~60 seconds (release build)
- **Runtime performance**: 3-5x faster than debug build
- **Output**: Full benchmark results with record counts for all 5 scenarios × 5 implementations

### Multiple Modes
```bash
cd benchmarks
./run_baseline_options.sh [mode]
```

Available modes:

| Mode | Compile Time | Runtime Speed | Best For |
|------|--------------|---------------|----------|
| `release` (default) | ~60s | 3-5x faster | Production benchmarks, final results |
| `debug` | ~15s | Baseline (1x) | Quick iterations, debugging |
| `profile` | ~60s | 3-5x faster | Flamegraph profiling analysis |
| `fast-release` | ~15s* | 3-5x faster | Incremental testing (with cache) |

\* If no code changed; ~60s if code was modified

## Usage Examples

### Basic Usage (100K events default)
```bash
cd benchmarks

# Release mode (fastest runtime, recommended)
./run_baseline.sh

# Or explicitly with options script
./run_baseline_options.sh release

# Quick iteration (fast compile, acceptable runtime)
./run_baseline_options.sh debug

# For detailed profiling
./run_baseline_options.sh profile

# Fast incremental builds (uses cache)
./run_baseline_options.sh fast-release

# Show help
./run_baseline_options.sh help
```

### Custom Event Counts
```bash
cd benchmarks

# Using run_baseline.sh with different event counts
./run_baseline.sh 1m          # 1 million events
./run_baseline.sh 500k        # 500K events
./run_baseline.sh 10m         # 10 million events

# Using run_baseline_options.sh
./run_baseline_options.sh release 1m     # Release mode, 1M events
./run_baseline_options.sh debug 500k     # Debug mode, 500K events
./run_baseline_options.sh profile 5m     # Profile mode, 5M events

# Using run_baseline_quick.sh
./run_baseline_quick.sh 1m               # Quick mode, 1M events

# Using run_baseline_flexible.sh
./run_baseline_flexible.sh release all -events 1m        # All scenarios, 1M events
./run_baseline_flexible.sh debug 3 -events 500k          # Scenario 3, 500K events
```

**Supported Formats:**
- Plain numbers: `100000`, `500000`
- Millions: `1m`, `10m`, `100m` (case-insensitive)
- Thousands: `1k`, `100k`, `500k` (case-insensitive)

## What Gets Tested

The comprehensive baseline test measures:

**5 Scenarios:**
1. Scenario 1: Pure SELECT (baseline)
2. Scenario 2: ROWS window
3. Scenario 3: Pure GROUP BY
4. Scenario 4: TUMBLING window (standard emit)
5. Scenario 5: TUMBLING window (emit changes)

**5 Implementations:**
1. SQL Engine (baseline)
2. V1 SimpleJobProcessor (single-threaded, best-effort)
3. Transactional JobProcessor (single-threaded, at-least-once)
4. V2 AdaptiveJobProcessor @ 1-core
5. V2 AdaptiveJobProcessor @ 4-core

**Output shows:**
- Records sent and processed for each test
- Throughput (records/second) for each implementation
- Comparative ratios vs SQL Engine baseline

## Performance Tuning

### Compilation Optimization
The `--release` flag enables:
- Link-Time Optimization (LTO)
- Codegen units = 1 (maximum optimization)
- Optimization level = 3
- Symbol stripping

This makes compilation slower but runtime performance significantly faster.

### Single-Threaded Test Execution
The `--test-threads=1` flag ensures:
- Tests run sequentially (not in parallel)
- Accurate timing measurements
- No CPU contention between test instances
- Reproducible results

### No Default Features
`--no-default-features` reduces:
- Compilation time
- Binary size
- Runtime overhead

## Typical Results

On a modern machine:

```
Release Build:
  Compilation: ~60 seconds
  Test runtime: ~8-12 seconds total
  Memory: ~500MB peak

Debug Build:
  Compilation: ~15 seconds
  Test runtime: ~30-40 seconds total
  Memory: ~400MB peak
```

## Continuous Integration

For CI/CD pipelines, use:
```bash
cd benchmarks
./run_baseline.sh
```

The script:
- Returns exit code 0 on success
- Returns exit code 1 on failure
- Produces clear output
- Shows all benchmark metrics

## Troubleshooting

### "Command not found"
Make sure you're in the benchmarks directory:
```bash
cd benchmarks
```

The scripts should already be executable, but if needed:
```bash
chmod +x run_baseline*.sh
```

### Slow compilation
- Check available disk space (needs ~2GB)
- Ensure you're not running other heavy processes
- Use `fast-release` mode if code hasn't changed

### High memory usage
- This is normal for release builds with LTO
- Ensure machine has at least 4GB RAM available
- If OOM occurs, use debug mode instead

## Related Documentation

- See `CLAUDE.md` for general development guide
- See `tests/performance/analysis/comprehensive_baseline_comparison.rs` for test implementation
- See `V2_CODE_LOCATIONS.txt` for V2 processor architecture
