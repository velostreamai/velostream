#!/bin/bash
# One Billion Row Challenge (1BRC) — Velostream Edition
#
# Usage:
#   ./demo/1brc/run-1brc.sh         # Default: 1M rows
#   ./demo/1brc/run-1brc.sh 100     # 100M rows
#   ./demo/1brc/run-1brc.sh 1000    # 1B rows

set -euo pipefail

ROWS=${1:-1}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== 1BRC Velostream Edition ==="
echo "Rows: ${ROWS}M"

# Step 1: Generate data
echo ""
echo "--- Step 1: Generating measurements ---"
./target/release/velo-1brc generate --rows "$ROWS" --output measurements.txt

# Step 2: Process through SQL engine
echo ""
echo "--- Step 2: Processing through Velostream SQL ---"
./target/release/velo-sql deploy-app --file "$SCRIPT_DIR/1brc.sql"

echo ""
echo "--- Done ---"
if [ -f ./1brc_results.csv ]; then
    echo "Results written to ./1brc_results.csv"
    echo "First 10 rows:"
    head -10 ./1brc_results.csv
fi
