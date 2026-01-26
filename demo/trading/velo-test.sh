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

# Run all app tests
run_all() {
    echo -e "${CYAN}Running all trading demo apps...${NC}"
    echo ""

    local passed=0
    local failed=0
    local total_assertions=0
    local total_assertions_passed=0
    local total_queries=0
    local total_queries_passed=0
    local start_time=$(date +%s)

    # Arrays to track per-app results
    declare -a app_names
    declare -a app_results
    declare -a app_assertions
    declare -a app_durations
    declare -a app_queries

    # Exclude .annotated.sql files - they use a different test spec format
    for sql in apps/*.sql; do
        # Skip annotated files
        if [[ "$sql" == *".annotated.sql" ]]; then
            continue
        fi
        name=$(basename "$sql" .sql)
        app_names+=("$name")
        echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

        # Capture output to parse assertion counts
        local output
        if output=$(run_app "$name" 2>&1); then
            ((passed++))
            app_results+=("✅")
        else
            ((failed++))
            app_results+=("❌")
        fi
        echo "$output"

        # Parse assertion counts from output (format: "Assertions: N total, N passed, N failed")
        local assertions_line=$(echo "$output" | grep -E "^Assertions:" | tail -1)
        if [[ -n "$assertions_line" ]]; then
            local assertions_total=$(echo "$assertions_line" | grep -oE "[0-9]+ total" | grep -oE "[0-9]+")
            local assertions_passed=$(echo "$assertions_line" | grep -oE "[0-9]+ passed" | grep -oE "[0-9]+")
            if [[ -n "$assertions_total" ]]; then
                total_assertions=$((total_assertions + assertions_total))
                total_assertions_passed=$((total_assertions_passed + assertions_passed))
                app_assertions+=("$assertions_passed/$assertions_total")
            else
                app_assertions+=("-")
            fi
        else
            app_assertions+=("-")
        fi

        # Parse query counts from output (format: "Queries: N total, N passed, N failed")
        local queries_line=$(echo "$output" | grep -E "^Queries:" | tail -1)
        if [[ -n "$queries_line" ]]; then
            local queries_total=$(echo "$queries_line" | grep -oE "[0-9]+ total" | grep -oE "[0-9]+")
            local queries_passed=$(echo "$queries_line" | grep -oE "[0-9]+ passed" | grep -oE "[0-9]+")
            if [[ -n "$queries_total" ]]; then
                total_queries=$((total_queries + queries_total))
                total_queries_passed=$((total_queries_passed + queries_passed))
                app_queries+=("$queries_passed/$queries_total")
            else
                app_queries+=("-")
            fi
        else
            app_queries+=("-")
        fi

        # Parse duration from output (format: "Duration: 14468ms")
        local duration_line=$(echo "$output" | grep -E "^Duration:" | tail -1)
        if [[ -n "$duration_line" ]]; then
            local duration_ms=$(echo "$duration_line" | grep -oE "[0-9]+ms" | grep -oE "[0-9]+")
            if [[ -n "$duration_ms" ]]; then
                # Convert to seconds with 1 decimal place
                local duration_s=$(echo "scale=1; $duration_ms / 1000" | bc)
                app_durations+=("${duration_s}s")
            else
                app_durations+=("-")
            fi
        else
            app_durations+=("-")
        fi

        echo ""
    done

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Trading Demo Test Summary${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    # Show per-app results table
    printf "  %-25s %-10s %-14s %-10s %s\n" "App" "Queries" "Assertions" "Duration" "Status"
    printf "  %-25s %-10s %-14s %-10s %s\n" "─────────────────────────" "──────────" "──────────────" "──────────" "──────"
    for i in "${!app_names[@]}"; do
        printf "  %-25s %-10s %-14s %-10s %s\n" "${app_names[$i]}" "${app_queries[$i]}" "${app_assertions[$i]}" "${app_durations[$i]}" "${app_results[$i]}"
    done
    printf "  %-25s %-10s %-14s %-10s %s\n" "─────────────────────────" "──────────" "──────────────" "──────────" "──────"

    # Show totals row
    local total_status="✅"
    if [[ $failed -gt 0 ]]; then
        total_status="❌"
    fi
    printf "  %-25s %-10s %-14s %-10s %s\n" "Total" "$total_queries_passed/$total_queries" "$total_assertions_passed/$total_assertions" "${duration}s" "$total_status"
    echo ""

    if [[ $failed -eq 0 ]]; then
        echo -e "${GREEN}🎉 ALL TESTS PASSED!${NC}"
    else
        echo -e "${RED}❌ $failed APP(S) FAILED${NC}"
    fi

    if [[ $failed -gt 0 ]]; then
        exit 1
    fi
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
