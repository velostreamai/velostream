#!/bin/bash

# FR-082: ULTRA-FAST Baseline Comparison Test
# For rapid iteration with minimal recompilation
# Uses incremental compilation cache aggressively

set -e

# Navigate to project root (benchmarks script is in benchmarks/ subdirectory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║ FR-082: BASELINE COMPARISON (QUICK MODE)                   ║"
echo "║ Optimized for fastest possible iteration                   ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Use incremental compilation + parallel jobs for fastest recompilation
# CARGO_INCREMENTAL=1 is already default, but explicit for clarity
echo "⚡ Using incremental compilation cache..."
echo ""

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
