#!/bin/bash
# One Billion Row Challenge (1BRC) — Velostream Edition
#
# Usage:
#   cd demo/1brc && ./run-1brc.sh         # Default: 1M rows
#   cd demo/1brc && ./run-1brc.sh 100     # 100M rows
#   cd demo/1brc && ./run-1brc.sh 1000    # 1B rows
#
# Also works from project root:
#   ./demo/1brc/run-1brc.sh

set -euo pipefail

ROWS=${1:-1}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Find a binary: project target → PATH
find_binary() {
    local name="$1"
    if [[ -f "$PROJECT_ROOT/target/release/$name" ]]; then
        echo "$PROJECT_ROOT/target/release/$name"
    elif command -v "$name" &>/dev/null; then
        command -v "$name"
    else
        echo "Error: $name not found. Build with: cargo build --release --bin $name" >&2
        echo "Or add bin/ to PATH." >&2
        exit 1
    fi
}

VELO_1BRC=$(find_binary velo-1brc)
VELO_TEST=$(find_binary velo-test)

echo "=== 1BRC Velostream Edition ==="
echo "Rows: ${ROWS}M"

# Step 1: Generate data + expected results
echo ""
echo "--- Step 1: Generating measurements + expected results ---"
"$VELO_1BRC" generate \
    --rows "$ROWS" \
    --output "$SCRIPT_DIR/measurements.txt" \
    --expected-output "$SCRIPT_DIR/expected.csv" \
    --seed 42

# Step 2: Process through SQL engine with test harness validation
echo ""
echo "--- Step 2: Processing through Velostream SQL + validating ---"
"$VELO_TEST" run "$SCRIPT_DIR/1brc.sql" --spec "$SCRIPT_DIR/test_spec.yaml" -y

echo ""
echo "--- Done ---"
if [ -f "$SCRIPT_DIR/1brc_results.csv" ]; then
    LINES=$(wc -l < "$SCRIPT_DIR/1brc_results.csv")
    echo "Results written to $SCRIPT_DIR/1brc_results.csv ($LINES lines)"
    echo "First 10 rows:"
    head -10 "$SCRIPT_DIR/1brc_results.csv"
fi
