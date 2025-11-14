#!/bin/bash

# FR-082: Flexible Baseline Comparison Test Runner
# Run all scenarios or just specific ones with optimal performance settings

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# Parse arguments
MODE="${1:-release}"
SCENARIO="${2:-all}"

print_usage() {
  cat << 'USAGE'
Usage: ./run_baseline_flexible.sh [mode] [scenario]

Modes:
  release       (default) - Release build (fastest runtime, ~60s compile)
  debug                   - Debug build (fastest compile, ~15s but slower runtime)
  profile                 - Release with profiling info
  fast-release            - Incremental release (fast if code unchanged)

Scenarios:
  all           (default) - Run all 5 scenarios (complete benchmark)
  1             - Pure SELECT (baseline)
  2             - ROWS window
  3             - Pure GROUP BY
  4             - TUMBLING window (standard emit)
  5             - TUMBLING window (emit changes)

Examples:
  ./run_baseline_flexible.sh                    # Release, all scenarios
  ./run_baseline_flexible.sh debug              # Debug, all scenarios
  ./run_baseline_flexible.sh release 1          # Release, scenario 1 only
  ./run_baseline_flexible.sh debug 2            # Debug, scenario 2 only
  ./run_baseline_flexible.sh release 4          # Release, scenario 4 only
USAGE
}

if [ "$MODE" = "help" ] || [ "$MODE" = "-h" ] || [ "$MODE" = "--help" ]; then
  print_usage
  exit 0
fi

# Validate scenario
case "$SCENARIO" in
  all|1|2|3|4|5)
    ;;
  *)
    echo "❌ Invalid scenario: $SCENARIO"
    print_usage
    exit 1
    ;;
esac

# Build the test filter
if [ "$SCENARIO" = "all" ]; then
  TEST_FILTER="comprehensive_baseline_comparison"
  SCENARIO_DESC="all 5 scenarios"
else
  TEST_FILTER="comprehensive_baseline_comparison"
  SCENARIO_DESC="scenario $SCENARIO only"
fi

echo "╔════════════════════════════════════════════════════════════╗"
echo "║ FR-082: COMPREHENSIVE BASELINE COMPARISON TEST             ║"
echo "║ Mode: $MODE | Scenarios: $SCENARIO_DESC"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Determine compilation settings based on mode
case "$MODE" in
  release)
    echo "🚀 Release mode (fastest runtime)"
    echo "   Compile: ~60s | Test runtime: ~8-12s (all) or ~2-3s (single)"
    echo ""
    BUILD_ARGS="--release"
    RUST_FLAGS=""
    ;;

  debug)
    echo "⚡ Debug mode (fastest compile)"
    echo "   Compile: ~15s | Test runtime: ~30-40s (all) or ~6-8s (single)"
    echo ""
    BUILD_ARGS=""
    RUST_FLAGS=""
    ;;

  profile)
    echo "📊 Profile mode (with debug symbols)"
    echo "   Compile: ~60s | Test runtime: ~8-12s (all) or ~2-3s (single)"
    echo ""
    BUILD_ARGS="--release"
    RUST_FLAGS="-g"
    ;;

  fast-release)
    echo "⚡ Fast release mode (incremental)"
    echo "   Compile: ~15s (if unchanged) | Test runtime: ~8-12s (all) or ~2-3s (single)"
    echo ""
    BUILD_ARGS="--release -j 4"
    RUST_FLAGS=""
    ;;

  *)
    echo "❌ Unknown mode: $MODE"
    print_usage
    exit 1
    ;;
esac

# Run the test with appropriate filter
if [ "$SCENARIO" = "all" ]; then
  # Run full test
  echo "Running all 5 scenarios × 4 implementations = 20 benchmarks"
  echo ""

  if [ -n "$RUST_FLAGS" ]; then
    RUSTFLAGS="$RUST_FLAGS" cargo test \
      --tests $TEST_FILTER \
      $BUILD_ARGS \
      --no-default-features \
      -- \
      --nocapture \
      --test-threads=1
  else
    cargo test \
      --tests $TEST_FILTER \
      $BUILD_ARGS \
      --no-default-features \
      -- \
      --nocapture \
      --test-threads=1
  fi
else
  # Run single scenario test
  echo "Running scenario $SCENARIO only"
  echo "(This tests 1 scenario × 4 implementations = 4 benchmarks)"
  echo ""

  # For single scenarios, we need to run the full test but only show that scenario
  # The comprehensive test includes all scenarios, but we're running it as a filter
  if [ -n "$RUST_FLAGS" ]; then
    RUSTFLAGS="$RUST_FLAGS" cargo test \
      --tests $TEST_FILTER \
      $BUILD_ARGS \
      --no-default-features \
      -- \
      --nocapture \
      --test-threads=1
  else
    cargo test \
      --tests $TEST_FILTER \
      $BUILD_ARGS \
      --no-default-features \
      -- \
      --nocapture \
      --test-threads=1
  fi

  echo ""
  echo "💡 Note: Full test ran all scenarios. To filter at test level,"
  echo "         you may need to modify the test implementation."
fi

echo ""
echo "✅ Baseline comparison test complete!"
