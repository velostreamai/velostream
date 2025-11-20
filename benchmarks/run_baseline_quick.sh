#!/bin/bash

# FR-082: ULTRA-FAST Baseline Comparison Test
# For rapid iteration with minimal recompilation
# Uses incremental compilation cache aggressively

set -e

# Navigate to project root (benchmarks script is in benchmarks/ subdirectory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# Parse arguments
EVENTS="${1:-100000}"

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

# Convert event count if needed
if [[ "$EVENTS" =~ ^[0-9]+$ ]]; then
  EVENTS_NUM="$EVENTS"
else
  EVENTS_NUM=$(convert_event_count "$EVENTS")
fi

# Validate
if ! [[ "$EVENTS_NUM" =~ ^[0-9]+$ ]]; then
  echo "❌ Error: Invalid event count: $EVENTS"
  echo "   Usage: $0 [event_count]"
  echo "   Examples: $0 100000, $0 1m, $0 10m, $0 500k"
  exit 1
fi

EVENTS_DESC="$(printf '%'\'',d' "$EVENTS_NUM")"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║ FR-082: BASELINE COMPARISON (QUICK MODE)                   ║"
echo "║ Optimized for fastest possible iteration                   ║"
echo "║ Events: $EVENTS_DESC"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Use incremental compilation + parallel jobs for fastest recompilation
# CARGO_INCREMENTAL=1 is already default, but explicit for clarity
echo "⚡ Using incremental compilation cache..."
echo ""

VELOSTREAM_BASELINE_RECORDS="$EVENTS_NUM" \
RUSTFLAGS="-C codegen-units=256 -C opt-level=2" \
CARGO_BUILD_JOBS=4 \
cargo test \
  --tests comprehensive_baseline_comparison \
  --no-default-features \
  -- \
  --nocapture \
  --test-threads=1

echo ""
echo "✅ Quick baseline test complete!"
echo ""
echo "💡 Tip: On subsequent runs with no code changes, this will be ~5-10 seconds!"
