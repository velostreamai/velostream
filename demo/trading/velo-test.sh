#!/bin/bash
# =============================================================================
# Trading Demo - velo-test Runner
# =============================================================================
# This script runs velo-test against the trading demo SQL applications.
#
# Usage:
#   ./velo-test.sh                    # List available apps
#   ./velo-test.sh app_market_data    # Run market data pipeline tests
#   ./velo-test.sh validate           # Validate all SQL syntax only
#   ./velo-test.sh all                # Run all app tests
#   ./velo-test.sh health             # Check infrastructure health
#
# Debug/Step-Through:
#   ./velo-test.sh app_market_data --step           # Step through each query
#   ./velo-test.sh app_market_data --query market_data_ts  # Run single query
#   ./velo-test.sh app_market_data --keep           # Keep Kafka running after test
#   ./velo-test.sh app_market_data -v               # Verbose output
#
# Options:
#   --step                Step through queries one at a time (interactive)
#   --keep                Keep testcontainers running after test (for debugging)
#   -v, --verbose         Enable verbose output
#   --kafka <servers>     Use external Kafka instead of testcontainers
#   --data-only           Only publish test data, don't run SQL (use with --kafka)
#   --timeout <ms>        Timeout per query in milliseconds (default: 90000)
#   --query <name>        Run only a specific query
#
# Requirements:
#   - velo-test binary (build with: cargo build --release)
#   - Docker (for testcontainers, unless --kafka is specified)
# =============================================================================

set -e

# Use Redpanda by default (faster startup: ~3s vs ~10s for Confluent Kafka)
# Note: Single-broker testcontainers don't support Kafka transactions reliably,
# so SQL files should use @job_mode: simple or adaptive for testing
export VELOSTREAM_TEST_CONTAINER="${VELOSTREAM_TEST_CONTAINER:-redpanda}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if velo-test is available
VELO_TEST="${VELO_TEST:-../../target/release/velo-test}"
if [[ ! -f "$VELO_TEST" ]]; then
    VELO_TEST="velo-test"
fi

if ! command -v "$VELO_TEST" &> /dev/null && [[ ! -f "$VELO_TEST" ]]; then
    echo -e "${RED}Error: velo-test not found${NC}"
    echo "Build it with: cargo build --release"
    echo "Or set VELO_TEST environment variable to the binary path"
    exit 1
fi

# Convert to absolute path
if [[ -f "$VELO_TEST" ]]; then
    VELO_TEST="$(cd "$(dirname "$VELO_TEST")" && pwd)/$(basename "$VELO_TEST")"
fi

# Default values
TIMEOUT_MS=90000
KAFKA_SERVERS=""
QUERY_FILTER=""
STEP_MODE=""
KEEP_CONTAINERS=""
VERBOSE=""
DATA_ONLY=""

# Show help
show_help() {
    echo -e "${CYAN}Trading Demo Test Runner${NC}"
    echo ""
    echo -e "${YELLOW}Usage:${NC}"
    echo "  ./velo-test.sh                    List available apps"
    echo "  ./velo-test.sh <app_name>         Run specific app tests"
    echo "  ./velo-test.sh validate           Validate all SQL syntax"
    echo "  ./velo-test.sh all                Run all app tests"
    echo "  ./velo-test.sh health             Check infrastructure health"
    echo ""
    echo -e "${YELLOW}Available Apps:${NC}"
    for sql in apps/*.sql; do
        name=$(basename "$sql" .sql)
        echo "  $name"
    done
    echo ""
    echo -e "${YELLOW}Debug Options:${NC}"
    echo "  --step              Step through queries one at a time"
    echo "  --keep              Keep Kafka container running after test"
    echo "  --reuse             Reuse existing Kafka container (faster for repeated runs)"
    echo "  -v, --verbose       Enable verbose output"
    echo "  --query <name>      Run only a specific query"
    echo ""
    echo -e "${YELLOW}Other Options:${NC}"
    echo "  --kafka <servers>   Use external Kafka instead of testcontainers"
    echo "  --data-only         Only publish test data (no SQL execution)"
    echo "  --timeout <ms>      Timeout per query (default: 90000)"
    echo "  -h, --help          Show this help"
}

# Parse arguments
COMMAND=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help|help)
            show_help
            exit 0
            ;;
        --kafka)
            KAFKA_SERVERS="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT_MS="$2"
            shift 2
            ;;
        --query|-q)
            QUERY_FILTER="$2"
            shift 2
            ;;
        --step)
            STEP_MODE="--step"
            shift
            ;;
        --keep)
            KEEP_CONTAINERS="--keep-containers"
            shift
            ;;
        --reuse)
            REUSE_CONTAINERS="--reuse-containers"
            shift
            ;;
        -v|--verbose)
            VERBOSE="--verbose"
            shift
            ;;
        --data-only)
            DATA_ONLY="--data-only"
            shift
            ;;
        *)
            COMMAND="$1"
            shift
            ;;
    esac
done

# Validate SQL files only
run_validate() {
    echo -e "${CYAN}Validating SQL files...${NC}"
    local errors=0
    for sql in apps/*.sql; do
        name=$(basename "$sql" .sql)
        echo -n "  Validating $name... "
        if $VELO_TEST validate "$sql" 2>/dev/null; then
            echo -e "${GREEN}✓${NC}"
        else
            echo -e "${RED}✗${NC}"
            ((errors++))
        fi
    done

    if [[ $errors -eq 0 ]]; then
        echo -e "${GREEN}All SQL files valid!${NC}"
    else
        echo -e "${RED}$errors file(s) had errors${NC}"
        exit 1
    fi
}

# Run a single app test
run_app() {
    local app_name="$1"
    local sql_file="apps/${app_name}.sql"
    local spec_file="tests/${app_name}.test.yaml"

    if [[ ! -f "$sql_file" ]]; then
        echo -e "${RED}Error: SQL file not found: $sql_file${NC}"
        exit 1
    fi

    echo -e "${CYAN}Running: $app_name${NC}"
    echo "  SQL:  $sql_file"
    echo "  Spec: $spec_file"
    echo ""

    # Build command
    local cmd="$VELO_TEST run $sql_file"
    cmd="$cmd --timeout-ms $TIMEOUT_MS"
    cmd="$cmd --schemas schemas"

    if [[ -f "$spec_file" ]]; then
        cmd="$cmd --spec $spec_file"
    fi

    if [[ -n "$KAFKA_SERVERS" ]]; then
        cmd="$cmd --kafka $KAFKA_SERVERS"
    else
        cmd="$cmd --use-testcontainers"
    fi

    if [[ -n "$QUERY_FILTER" ]]; then
        cmd="$cmd --query $QUERY_FILTER"
    fi

    if [[ -n "$STEP_MODE" ]]; then
        cmd="$cmd $STEP_MODE"
    fi

    if [[ -n "$KEEP_CONTAINERS" ]]; then
        cmd="$cmd $KEEP_CONTAINERS"
    fi

    if [[ -n "$REUSE_CONTAINERS" ]]; then
        cmd="$cmd $REUSE_CONTAINERS"
    fi

    if [[ -n "$VERBOSE" ]]; then
        cmd="$cmd $VERBOSE"
    fi

    if [[ -n "$DATA_ONLY" ]]; then
        cmd="$cmd $DATA_ONLY"
    fi

    echo -e "${BLUE}$cmd${NC}"
    echo ""

    # Run with RUST_LOG=info for readable output (debug for verbose)
    if [[ -n "$VERBOSE" ]]; then
        RUST_LOG=debug $cmd
    else
        RUST_LOG=info $cmd
    fi
}

# Run all app tests using velo-test run-all (handles aggregation in Rust)
run_all() {
    echo -e "${CYAN}Running all trading demo apps...${NC}"
    echo ""

    # Build command - velo-test run-all handles aggregation, summary tables, etc.
    local cmd="$VELO_TEST run-all ."
    cmd="$cmd --pattern 'apps/*.sql'"
    cmd="$cmd --skip '*.annotated.sql'"
    cmd="$cmd --timeout-ms $TIMEOUT_MS"

    if [[ -n "$KAFKA_SERVERS" ]]; then
        cmd="$cmd --kafka $KAFKA_SERVERS"
    else
        cmd="$cmd --use-testcontainers"
    fi

    if [[ -n "$KEEP_CONTAINERS" ]]; then
        cmd="$cmd $KEEP_CONTAINERS"
    fi

    if [[ -n "$REUSE_CONTAINERS" ]]; then
        cmd="$cmd $REUSE_CONTAINERS"
    fi

    if [[ -n "$VERBOSE" ]]; then
        cmd="$cmd $VERBOSE"
    fi

    echo -e "${BLUE}$cmd${NC}"
    echo ""

    # Run with RUST_LOG=info for readable output
    if [[ -n "$VERBOSE" ]]; then
        RUST_LOG=debug $cmd
    else
        RUST_LOG=info $cmd
    fi
}

# Run health check
run_health() {
    local broker="${KAFKA_SERVERS:-localhost:9092}"
    local output="text"

    if [[ -n "$VERBOSE" ]]; then
        output="text"
    fi

    echo -e "${CYAN}Running infrastructure health check...${NC}"
    echo ""

    $VELO_TEST health --broker "$broker" --output "$output"
}

# Main
case "$COMMAND" in
    ""|list)
        show_help
        ;;
    validate)
        run_validate
        ;;
    all)
        run_all
        ;;
    health)
        run_health
        ;;
    app_*)
        run_app "$COMMAND"
        ;;
    *)
        # Check if it's a valid app name
        if [[ -f "apps/${COMMAND}.sql" ]]; then
            run_app "$COMMAND"
        else
            echo -e "${RED}Unknown command or app: $COMMAND${NC}"
            echo ""
            show_help
            exit 1
        fi
        ;;
esac
