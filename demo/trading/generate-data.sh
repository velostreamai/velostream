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

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

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
    elif command -v velo-test &>/dev/null; then
        VELO_TEST="$(command -v velo-test)"
    else
        echo "Error: velo-test binary not found. Build with: cargo build --release --bin velo-test"
        echo "Or add bin/ to PATH."
        exit 1
    fi
fi

# Ensure we're in the trading demo directory (velo-test resolves schemas relative to CWD)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Data generation configuration per app
# Format: app_file|topic|data_type|records|timeframe
declare -a DATA_SPECS=(
    "apps/app_market_data.sql|in_market_data_stream|Market Ticks (OHLCV)|1000|last 1 hour"
    "apps/app_risk.sql|in_trading_positions_stream|Trading Positions|500|last 1 hour"
    "apps/app_trading_signals.sql|in_order_book_stream|Order Book Depth|1000|last 1 hour"
    "apps/app_trading_signals.sql|in_market_data_stream_a|Exchange A Quotes|1000|last 1 hour"
    "apps/app_trading_signals.sql|in_market_data_stream_b|Exchange B Quotes|1000|last 1 hour"
)

# Unique apps for iteration
APPS=(
    "apps/app_market_data.sql"
    "apps/app_risk.sql"
    "apps/app_trading_signals.sql"
)

print_data_summary() {
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}                        DATA GENERATION SUMMARY${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    printf "${YELLOW}%-30s %-25s %8s  %-12s${NC}\n" "TOPIC" "DATA TYPE" "RECORDS" "TIMEFRAME"
    echo -e "${CYAN}──────────────────────────────────────────────────────────────────────────────${NC}"

    local total_records=0
    for spec in "${DATA_SPECS[@]}"; do
        IFS='|' read -r app topic data_type records timeframe <<< "$spec"
        printf "%-30s %-25s %8s  %-12s\n" "$topic" "$data_type" "$records" "$timeframe"
        total_records=$((total_records + records))
    done

    echo -e "${CYAN}──────────────────────────────────────────────────────────────────────────────${NC}"
    printf "${GREEN}%-30s %-25s %8s  %-12s${NC}\n" "TOTAL PER BATCH" "" "$total_records" ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

generate_batch() {
    local batch_num=$1
    local batch_start=$(date +%s)

    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} ${YELLOW}Batch $batch_num${NC} starting (all apps in parallel)..."

    # Launch all apps in parallel — each writes to independent input topics
    declare -a PIDS=()
    declare -a APP_NAMES=()

    for app in "${APPS[@]}"; do
        local app_name=$(basename "$app" .sql)

        # Count topics for this app
        local topic_count=0
        local topics=""
        for spec in "${DATA_SPECS[@]}"; do
            IFS='|' read -r spec_app topic data_type records timeframe <<< "$spec"
            if [[ "$spec_app" == "$app" ]]; then
                topic_count=$((topic_count + 1))
                topics="$topics $topic"
            fi
        done

        echo -e "  ${GREEN}▶${NC} $app_name (${topic_count} topics:${topics})"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Generating data: $app_name (batch $batch_num)" >> "$LOG_FILE"

        # Derive spec file from app file: apps/app_foo.sql -> tests/app_foo.test.yaml
        local spec_file="tests/${app_name}.test.yaml"
        local app_log="/tmp/velo_gen_${app_name}.log"

        RUST_LOG=warn "$VELO_TEST" run "$app" \
            --spec "$spec_file" \
            --data-only \
            --kafka "$KAFKA_BROKER" \
            --timeout-ms 120000 \
            -y \
            > "$app_log" 2>&1 &

        PIDS+=($!)
        APP_NAMES+=("$app_name")
    done

    # Wait for all apps to finish and report results
    local all_ok=true
    for i in "${!PIDS[@]}"; do
        if wait "${PIDS[$i]}"; then
            echo -e "    ${GREEN}✓${NC} ${APP_NAMES[$i]} completed"
            cat "/tmp/velo_gen_${APP_NAMES[$i]}.log" >> "$LOG_FILE"
        else
            echo -e "    ${RED}✗${NC} ${APP_NAMES[$i]} failed (check $LOG_FILE)"
            cat "/tmp/velo_gen_${APP_NAMES[$i]}.log" >> "$LOG_FILE"
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Warning: ${APP_NAMES[$i]} data generation failed" >> "$LOG_FILE"
            all_ok=false
        fi
    done

    local batch_end=$(date +%s)
    local batch_duration=$((batch_end - batch_start))
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} Batch $batch_num completed in ${batch_duration}s"
    echo ""
}

# Header
echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              VELOSTREAM TRADING DEMO - DATA GENERATOR                     ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  Kafka:       ${CYAN}$KAFKA_BROKER${NC}"
echo -e "  Iterations:  ${CYAN}$(if $ONCE; then echo 1; else echo $ITERATIONS; fi)${NC} batches"
echo -e "  Delay:       ${CYAN}${DELAY}s${NC} between batches"
echo -e "  Log:         ${CYAN}$LOG_FILE${NC}"
echo ""

print_data_summary

if $ONCE; then
    echo -e "${YELLOW}Running single batch...${NC}"
    echo ""
    generate_batch 1
    echo -e "${GREEN}Done.${NC}"
else
    total_batches=$ITERATIONS
    echo -e "${YELLOW}Starting continuous data generation ($total_batches batches)...${NC}"
    echo ""

    for i in $(seq 1 "$ITERATIONS"); do
        generate_batch "$i"
        if [[ $i -lt $ITERATIONS ]]; then
            echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} Waiting ${DELAY}s before next batch..."
            sleep "$DELAY"
            echo ""
        fi
    done

    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Data generation complete" >> "$LOG_FILE"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}Done. Generated $ITERATIONS batches.${NC}"
fi
