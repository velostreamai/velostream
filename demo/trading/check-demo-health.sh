#!/bin/bash
# Health Check Script for Trading Demo
# Validates all components are running and processing data
#
# This script is a thin wrapper around `velo-test health` which provides
# comprehensive health checking in Rust with better error handling and
# structured output.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Find velo-test binary
VELO_TEST="${VELO_TEST:-../../target/release/velo-test}"
if [[ ! -f "$VELO_TEST" ]]; then
    VELO_TEST="../../target/debug/velo-test"
fi
if [[ ! -f "$VELO_TEST" ]]; then
    VELO_TEST="velo-test"
fi

if ! command -v "$VELO_TEST" &> /dev/null && [[ ! -f "$VELO_TEST" ]]; then
    echo "Error: velo-test not found"
    echo "Build it with: cargo build --release"
    exit 1
fi

# Default broker
BROKER="${KAFKA_BROKER:-localhost:9092}"

# Parse arguments
OUTPUT="text"
CHECKS=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --json)
            OUTPUT="json"
            shift
            ;;
        --broker)
            BROKER="$2"
            shift 2
            ;;
        --check)
            CHECKS="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Build command
CMD="$VELO_TEST health --broker $BROKER --output $OUTPUT"
if [[ -n "$CHECKS" ]]; then
    CMD="$CMD --check $CHECKS"
fi

# Run health check
exec $CMD
