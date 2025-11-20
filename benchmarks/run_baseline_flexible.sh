#!/bin/bash

# FR-082: Flexible Baseline Comparison Test Runner
# Run all scenarios or just specific ones with optimal performance settings

set -e

# Navigate to project root (benchmarks script is in benchmarks/ subdirectory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# Parse arguments
MODE="${1:-release}"
SCENARIO="${2:-all}"
EVENTS=""

print_usage() {
  cat << 'USAGE'
Usage: ./run_baseline_flexible.sh [mode] [scenario] [-events <count>]

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

Event Count:
  -events <count>         - Number of events to process (default: 100,000)
                            Supports: -events 1m, -events 10m, -events 500k, etc.

Examples:
  ./run_baseline_flexible.sh                         # Release, all scenarios, 100K events
  ./run_baseline_flexible.sh debug                   # Debug, all scenarios, 100K events
  ./run_baseline_flexible.sh release 1               # Release, scenario 1 only, 100K events
  ./run_baseline_flexible.sh release all -events 1m  # Release, all scenarios, 1M events
  ./run_baseline_flexible.sh debug 3 -events 500k    # Debug, scenario 3, 500K events
USAGE
}

if [ "$MODE" = "help" ] || [ "$MODE" = "-h" ] || [ "$MODE" = "--help" ]; then
  print_usage
  exit 0
fi

# Helper function to convert event count to number
convert_event_count() {
  local count_str="$1"

  # Remove 'events' prefix if present
  count_str="${count_str#events}"

  # Convert m/M to millions, k/K to thousands
  case "$count_str" in
    *m|*M)
      count_num="${count_str%[mM]}"
      echo $((count_num * 1000000))
      ;;
    *k|*K)
      count_num="${count_str%[kK]}"
      echo $((count_num * 1000))
      ;;
    *)
      # Assume it's a plain number
      echo "$count_str"
      ;;
  esac
}

# Parse optional -events parameter
shift 2 || true  # Skip mode and scenario arguments
while [ $# -gt 0 ]; do
  case "$1" in
    -events)
      if [ -z "$2" ]; then
        echo "❌ Error: -events requires a value"
        print_usage
        exit 1
      fi
      EVENTS=$(convert_event_count "$2")
      if ! [[ "$EVENTS" =~ ^[0-9]+$ ]]; then
        echo "❌ Error: Invalid event count: $2"
        print_usage
        exit 1
      fi
      shift 2
      ;;
    *)
      echo "❌ Unknown option: $1"
      print_usage
      exit 1
      ;;
  esac
done

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

# Set default event count if not specified
if [ -z "$EVENTS" ]; then
  EVENTS=100000
  EVENTS_DESC="100,000 (default)"
else
  EVENTS_DESC="$(printf '%'\'',d' "$EVENTS")"
fi

echo "╔════════════════════════════════════════════════════════════╗"
echo "║ FR-082: COMPREHENSIVE BASELINE COMPARISON TEST             ║"
echo "║ Mode: $MODE | Scenarios: $SCENARIO_DESC                   ║"
echo "║ Events: $EVENTS_DESC"
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
    VELOSTREAM_BASELINE_RECORDS="$EVENTS" RUSTFLAGS="$RUST_FLAGS" cargo test \
      --tests $TEST_FILTER \
      $BUILD_ARGS \
      --no-default-features \
      -- \
      --nocapture \
      --test-threads=1
  else
    VELOSTREAM_BASELINE_RECORDS="$EVENTS" cargo test \
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
    VELOSTREAM_BASELINE_RECORDS="$EVENTS" RUSTFLAGS="$RUST_FLAGS" cargo test \
      --tests $TEST_FILTER \
      $BUILD_ARGS \
      --no-default-features \
      -- \
      --nocapture \
      --test-threads=1
  else
    VELOSTREAM_BASELINE_RECORDS="$EVENTS" cargo test \
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
