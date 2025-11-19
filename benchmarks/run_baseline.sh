#!/bin/bash

# FR-082: Comprehensive Baseline Comparison Test Runner
# Optimized for fastest possible performance and compilation

set -e

# Navigate to project root (benchmarks script is in benchmarks/ subdirectory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║ FR-082: COMPREHENSIVE BASELINE COMPARISON TEST             ║"
echo "║ Optimized for Maximum Performance                          ║"
echo "╚════════════════════════════════════════════════════════════╝"
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

cargo test \
  --tests comprehensive_baseline_comparison \
  --release \
  --no-default-features \
  -- \
  --nocapture \
  --test-threads=1

echo ""
echo "✅ Baseline comparison test complete!"
