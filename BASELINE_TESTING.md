# FR-082: Comprehensive Baseline Comparison Testing

Scripts for running the comprehensive baseline comparison test with optimal performance settings.

## Quick Start

### Fastest Runtime Performance (Recommended)
```bash
./run_baseline.sh
```

Produces:
- **Compilation time**: ~60 seconds (release build)
- **Runtime performance**: 3-5x faster than debug build
- **Output**: Full benchmark results with record counts for all 5 scenarios × 4 implementations

### Multiple Modes
```bash
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

```bash
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

## What Gets Tested

The comprehensive baseline test measures:

**5 Scenarios:**
1. Scenario 0: Pure SELECT (baseline)
2. Scenario 1: ROWS window
3. Scenario 2: Pure GROUP BY
4. Scenario 3a: TUMBLING window (standard emit)
5. Scenario 3b: TUMBLING window (emit changes)

**4 Implementations:**
1. SQL Engine (no job server)
2. V1 SimpleJobProcessor (single-threaded)
3. V2 AdaptiveJobProcessor @ 1-core
4. V2 AdaptiveJobProcessor @ 4-core

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
./run_baseline.sh
```

The script:
- Returns exit code 0 on success
- Returns exit code 1 on failure
- Produces clear output
- Shows all benchmark metrics

## Troubleshooting

### "Command not found"
Make sure the script is executable:
```bash
chmod +x run_baseline.sh
chmod +x run_baseline_options.sh
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
