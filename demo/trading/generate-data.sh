#!/bin/bash
# Data generator for trading demo
# Uses velo-test --data-only to generate schema-aware data for all apps with raw input topics
#
# Usage:
#   ./generate-data.sh [OPTIONS]
#
# Options:
#   --kafka BROKER       Kafka broker address (default: localhost:9092)
#   --iterations N       Number of data generation batches (default: 10)
#   --delay SECONDS      Delay between batches (default: 30)
#   --velo-test PATH     Path to velo-test binary (default: auto-detect)
#   --once               Run a single batch and exit
#   --log FILE           Log file path (default: /tmp/velo_stress.log)

set -euo pipefail

# Defaults
KAFKA_BROKER="localhost:9092"
ITERATIONS=10
DELAY=30
VELO_TEST=""
ONCE=false
LOG_FILE="/tmp/velo_stress.log"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --kafka)      KAFKA_BROKER="$2"; shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --delay)      DELAY="$2"; shift 2 ;;
        --velo-test)  VELO_TEST="$2"; shift 2 ;;
        --once)       ONCE=true; shift ;;
        --log)        LOG_FILE="$2"; shift 2 ;;
        *)            echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Auto-detect velo-test binary
if [[ -z "$VELO_TEST" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
    if [[ -x "$PROJECT_ROOT/target/release/velo-test" ]]; then
        VELO_TEST="$PROJECT_ROOT/target/release/velo-test"
    elif [[ -x "$PROJECT_ROOT/target/debug/velo-test" ]]; then
        VELO_TEST="$PROJECT_ROOT/target/debug/velo-test"
    else
        echo "Error: velo-test binary not found. Build with: cargo build --release --bin velo-test"
        exit 1
    fi
fi

# Ensure we're in the trading demo directory (velo-test resolves schemas relative to CWD)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Apps with raw input topics that need data generation:
#   app_market_data.sql   -> in_market_data_stream
#   app_risk.sql          -> in_trading_positions_stream
#   app_trading_signals.sql -> in_order_book_stream, in_market_data_stream_a, in_market_data_stream_b
APPS=(
    "apps/app_market_data.sql"
    "apps/app_risk.sql"
    "apps/app_trading_signals.sql"
)

generate_batch() {
    local batch_num=$1
    for app in "${APPS[@]}"; do
        local app_name=$(basename "$app" .sql)
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Generating data: $app_name (batch $batch_num)" >> "$LOG_FILE"
        RUST_LOG=info "$VELO_TEST" run "$app" \
            --data-only \
            --kafka "$KAFKA_BROKER" \
            --timeout-ms 120000 \
            -y \
            >> "$LOG_FILE" 2>&1 || {
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] Warning: $app_name data generation failed" >> "$LOG_FILE"
            }
    done
}

echo "Trading demo data generator"
echo "  Kafka:      $KAFKA_BROKER"
echo "  Apps:       ${#APPS[@]} (market_data, risk, trading_signals)"
echo "  Iterations: $(if $ONCE; then echo 1; else echo $ITERATIONS; fi)"
echo "  Log:        $LOG_FILE"
echo ""

if $ONCE; then
    echo "Generating single batch..."
    generate_batch 1
    echo "Done."
else
    echo "Starting continuous data generation..."
    for i in $(seq 1 "$ITERATIONS"); do
        echo "Batch $i of $ITERATIONS"
        generate_batch "$i"
        if [[ $i -lt $ITERATIONS ]]; then
            sleep "$DELAY"
        fi
    done
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Data generation complete" >> "$LOG_FILE"
    echo "Done. Generated $ITERATIONS batches."
fi
