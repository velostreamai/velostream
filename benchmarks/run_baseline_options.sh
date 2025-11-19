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

print_usage() {
  echo "Usage: ./run_baseline_options.sh [mode]"
  echo ""
  echo "Modes:"
  echo "  release       (default) - Release build (fastest runtime, ~60s compile)"
  echo "  debug                   - Debug build (fastest compile, ~15s but slower runtime)"
  echo "  profile                 - Release with profiling info (for flamegraph analysis)"
  echo "  fast-release            - Incremental release (fast compile if code unchanged)"
  echo ""
}

if [ "$MODE" = "help" ] || [ "$MODE" = "-h" ] || [ "$MODE" = "--help" ]; then
  print_usage
  exit 0
fi

echo "╔════════════════════════════════════════════════════════════╗"
echo "║ FR-082: COMPREHENSIVE BASELINE COMPARISON TEST             ║"
echo "║ Mode: $MODE"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

case "$MODE" in
  release)
    echo "🚀 Release mode (fastest runtime, ~60s compile)"
    echo "   - LTO enabled"
    echo "   - Codegen units = 1"
    echo "   - 3-5x faster than debug"
    echo ""
    cargo test \
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
    cargo test \
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
    RUSTFLAGS="-g" cargo test \
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
    cargo test \
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
