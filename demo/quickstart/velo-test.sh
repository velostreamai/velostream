#!/bin/bash
# Velostream Quickstart Runner
# Runs all quickstart SQL examples

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Find velo-test binary
VELO_TEST="../../target/release/velo-test"
if [[ ! -x "$VELO_TEST" ]]; then
    if command -v velo-test &>/dev/null; then
        VELO_TEST="velo-test"
    elif [[ -f "../../Cargo.toml" ]]; then
        echo "Building velo-test (one-time)..."
        (cd ../.. && cargo build --release --bin velo-test)
    else
        echo "Error: velo-test not found. Add bin/ to PATH or install velostream."
        exit 1
    fi
fi

# Create output directory
mkdir -p output

echo "=========================================="
echo " Velostream Quickstart"
echo "=========================================="
echo

SQL_FILES=(
    "hello_world.sql"
    "01_filter.sql"
    "02_transform.sql"
    "03_aggregate.sql"
    "04_window.sql"
)

for sql in "${SQL_FILES[@]}"; do
    if [[ -f "$sql" ]]; then
        echo "Running: $sql"
        "$VELO_TEST" run "$sql" -y 2>&1 | grep -E "(Deployed|completed|failed|Error)" | head -3
        echo
    fi
done

echo "=========================================="
echo " Output files:"
echo "=========================================="
ls -la output/*.csv 2>/dev/null || echo "No output files found"
