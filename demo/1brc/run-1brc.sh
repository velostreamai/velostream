#!/bin/bash
# One Billion Row Challenge (1BRC) — Velostream Edition
#
# Usage (from project root):
#   ./demo/1brc/run-1brc.sh         # Default: 1M rows
#   ./demo/1brc/run-1brc.sh 100     # 100M rows
#   ./demo/1brc/run-1brc.sh 1000    # 1B rows

set -euo pipefail

ROWS=${1:-1}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== 1BRC Velostream Edition ==="
echo "Rows: ${ROWS}M"

# Step 1: Generate data + expected results
echo ""
echo "--- Step 1: Generating measurements + expected results ---"
./target/release/velo-1brc generate \
    --rows "$ROWS" \
    --output measurements.txt \
    --expected-output expected.csv \
    --seed 42

# Step 2: Process through SQL engine with test harness validation
echo ""
echo "--- Step 2: Processing through Velostream SQL + validating ---"
./target/release/velo-test run "$SCRIPT_DIR/1brc.sql" --spec "$SCRIPT_DIR/test_spec.yaml" -y

echo ""
echo "--- Done ---"
if [ -f ./1brc_results.csv ]; then
    LINES=$(wc -l < ./1brc_results.csv)
    echo "Results written to ./1brc_results.csv ($LINES lines)"
    echo "First 10 rows:"
    head -10 ./1brc_results.csv
fi
