#!/bin/bash

# FR-082: Baseline Comparison Test Runner with Multiple Modes
# Choose between fastest compilation vs fastest runtime

set -e

# Navigate to project root (benchmarks script is in benchmarks/ subdirectory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# Default to release mode (fastest runtime)
MODE="${1:-release}"
EVENTS="${2:-100000}"

# Helper function to convert event count
convert_event_count() {
  local count_str="$1"
  count_str="${count_str#events}"
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
      echo "$count_str"
      ;;
  esac
}

print_usage() {
  echo "Usage: ./run_baseline_options.sh [mode] [event_count]"
  echo ""
  echo "Modes:"
  echo "  release       (default) - Release build (fastest runtime, ~60s compile)"
  echo "  debug                   - Debug build (fastest compile, ~15s but slower runtime)"
  echo "  profile                 - Release with profiling info (for flamegraph analysis)"
  echo "  fast-release            - Incremental release (fast compile if code unchanged)"
  echo ""
  echo "Event Count (default: 100,000):"
  echo "  100000        - 100K events"
  echo "  1m            - 1M events"
  echo "  10m           - 10M events"
  echo "  500k          - 500K events"
  echo ""
}

if [ "$MODE" = "help" ] || [ "$MODE" = "-h" ] || [ "$MODE" = "--help" ]; then
  print_usage
  exit 0
fi

# Convert event count if needed
if [[ "$EVENTS" =~ ^[0-9]+$ ]]; then
  EVENTS_NUM="$EVENTS"
else
  EVENTS_NUM=$(convert_event_count "$EVENTS")
fi

# Validate
if ! [[ "$EVENTS_NUM" =~ ^[0-9]+$ ]]; then
  echo "❌ Error: Invalid event count: $EVENTS"
  print_usage
  exit 1
fi

EVENTS_DESC="$(printf '%'\'',d' "$EVENTS_NUM")"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║ FR-082: COMPREHENSIVE BASELINE COMPARISON TEST             ║"
echo "║ Mode: $MODE | Events: $EVENTS_DESC"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

case "$MODE" in
  release)
    echo "🚀 Release mode (fastest runtime, ~60s compile)"
    echo "   - LTO enabled"
    echo "   - Codegen units = 1"
    echo "   - 3-5x faster than debug"
    echo ""
    VELOSTREAM_BASELINE_RECORDS="$EVENTS_NUM" cargo test \
      --tests comprehensive_baseline_comparison \
      --release \
      --no-default-features \
      -- \
      --nocapture \
      --test-threads=1
    ;;

  debug)
    echo "⚡ Debug mode (fastest compile, ~15s but slower runtime)"
    echo "   - No optimizations"
    echo "   - Full debug symbols"
    echo "   - Best for quick iterations"
    echo ""
    VELOSTREAM_BASELINE_RECORDS="$EVENTS_NUM" cargo test \
      --tests comprehensive_baseline_comparison \
      --no-default-features \
      -- \
      --nocapture \
      --test-threads=1
    ;;

  profile)
    echo "📊 Release with profiling (for flamegraph analysis)"
    echo "   - Release optimizations"
    echo "   - Debug symbols preserved"
    echo "   - Can be used with perf/flamegraph"
    echo ""
    VELOSTREAM_BASELINE_RECORDS="$EVENTS_NUM" RUSTFLAGS="-g" cargo test \
      --tests comprehensive_baseline_comparison \
      --release \
      --no-default-features \
      -- \
      --nocapture \
      --test-threads=1
    ;;

  fast-release)
    echo "⚡ Fast release (incremental, only recompile changed code)"
    echo "   - Uses incremental compilation cache"
    echo "   - ~15s if code unchanged, ~60s if changed"
    echo ""
    VELOSTREAM_BASELINE_RECORDS="$EVENTS_NUM" cargo test \
      --tests comprehensive_baseline_comparison \
      --release \
      --no-default-features \
      -j 4 \
      -- \
      --nocapture \
      --test-threads=1
    ;;

  *)
    echo "❌ Unknown mode: $MODE"
    print_usage
    exit 1
    ;;
esac

echo ""
echo "✅ Baseline comparison test complete!"
