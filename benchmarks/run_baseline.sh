#!/bin/bash

# FR-082: Comprehensive Baseline Comparison Test Runner
# Optimized for fastest possible performance and compilation

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

EVENTS_DESC="$EVENTS_NUM"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║ FR-082: COMPREHENSIVE BASELINE COMPARISON TEST             ║"
echo "║ Optimized for Maximum Performance                          ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "📊 Test Parameters:"
echo "   Build Mode: Release (LTO enabled, codegen-units=1)"
echo "   Scenarios: All 5 scenarios"
echo "   Event Count: $EVENTS_DESC"
echo "   Implementations: 4 (SimpleJp, TransactionalJp, AdaptiveJp@1c, AdaptiveJp@4c)"
echo ""
echo "⏱️  Expected Runtime: ~60s compile + ~8-12s test"
echo ""

# Build with release optimizations (fastest runtime)
# Using --release gives:
# - LTO enabled
# - Codegen units = 1 (best optimization)
# - Strip symbols
# - Optimization level = 3
echo "🚀 Building with release optimizations..."
echo "   This takes ~30-60s but gives 3-5x faster runtime performance"
echo ""

VELOSTREAM_BASELINE_RECORDS="$EVENTS_NUM" cargo test \
  --tests comprehensive_baseline_comparison \
  --release \
  --no-default-features \
  -- \
  --nocapture \
  --test-threads=1

echo ""
echo "✅ Baseline comparison test complete!"
