#!/usr/bin/env bash
# =============================================================================
# Velostream Test Harness Runner
# =============================================================================
# Runs all test tiers with proper configuration
#
# Usage:
#   ./run-tests.sh              # Run all tiers
#   ./run-tests.sh tier1        # Run specific tier
#   ./run-tests.sh tier4 30     # Run specific test (30_lag_lead)
#   ./run-tests.sh --list       # List all tests
#   ./run-tests.sh --help       # Show help
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VELO_TEST="${SCRIPT_DIR}/../../target/release/velo-test"
SCHEMAS_DIR="${SCRIPT_DIR}/schemas"
TIMEOUT=90

# Counters
PASSED=0
FAILED=0
SKIPPED=0
TOTAL=0

# Check if test is blocked (returns reason or empty)
is_blocked() {
    local test_key="$1"
    case "$test_key" in
        "tier1_basic/05_distinct") echo "Issue #1 - SELECT DISTINCT not implemented" ;;
        "tier1_basic/06_order_by") echo "Issue #2 - ORDER BY not applied" ;;
        "tier1_basic/07_limit") echo "Issue #3 - LIMIT context persistence" ;;
        "tier2_aggregations/12_tumbling_window") echo "Issue #4 - Needs watermarks" ;;
        "tier2_aggregations/13_sliding_window") echo "Issue #4 - Needs watermarks" ;;
        "tier2_aggregations/14_session_window") echo "Issue #4 - Needs watermarks" ;;
        "tier2_aggregations/15_compound_keys") echo "Issue #4 - Needs watermarks" ;;
        "tier3_joins/20_stream_table_join") echo "Issue #6 - Config mismatch" ;;
        "tier3_joins/21_stream_stream_join") echo "Issue #5 - Runtime panic" ;;
        "tier3_joins/22_multi_join") echo "Issue #6 - File source config" ;;
        "tier3_joins/23_right_join") echo "Issue #8 - Missing schema" ;;
        "tier3_joins/24_full_outer_join") echo "Issue #8 - Missing schema" ;;
        "tier5_complex/40_pipeline") echo "Issues #4, #5 - Multi-stage + panic" ;;
        "tier5_complex/41_subqueries") echo "Issues #5, #6 - Reference table + panic" ;;
        "tier5_complex/43_complex_filter") echo "Issue #7 - Schema mismatch" ;;
        "tier5_complex/44_union") echo "Issue #8 - Missing schema" ;;
        "tier6_edge_cases/50_nulls") echo "Issue #7 - Schema mismatch" ;;
        "tier6_edge_cases/51_empty") echo "Issue #7 - Schema mismatch" ;;
        "tier6_edge_cases/52_large_volume") echo "Issue #7 - Schema mismatch" ;;
        "tier6_edge_cases/53_late_arrivals") echo "Issue #7 - Schema mismatch" ;;
        "tier7_serialization/60_json_format") echo "Issue #8 - Missing schema" ;;
        "tier7_serialization/61_avro_format") echo "Issue #8 - Missing schema" ;;
        "tier7_serialization/62_protobuf_format") echo "Issue #8 - Missing schema" ;;
        "tier7_serialization/63_format_conversion") echo "Issue #8 - Missing schema" ;;
        "tier8_fault_tolerance/70_dlq_basic") echo "Issue #8 - Missing schema" ;;
        "tier8_fault_tolerance/72_fault_injection") echo "Issue #8 - Missing schema" ;;
        "tier8_fault_tolerance/73_debug_mode") echo "Issue #8 - Missing schema" ;;
        "tier8_fault_tolerance/74_stress_test") echo "Issue #8 - Missing schema" ;;
        *) echo "" ;;
    esac
}

# Print usage
usage() {
    echo "Velostream Test Harness Runner"
    echo ""
    echo "Usage:"
    echo "  $0                    Run all tiers"
    echo "  $0 tier1              Run specific tier (tier1, tier2, etc.)"
    echo "  $0 tier4 30           Run specific test (30_lag_lead)"
    echo "  $0 --list             List all tests"
    echo "  $0 --skip-blocked     Skip known blocked tests"
    echo "  $0 --help             Show this help"
    echo ""
    echo "Options:"
    echo "  --skip-blocked        Skip tests known to be blocked by issues"
    echo "  --timeout N           Set timeout in seconds (default: 90)"
    echo "  --verbose             Show full test output"
    echo ""
    echo "Tiers:"
    echo "  tier1_basic           Basic SQL operations"
    echo "  tier2_aggregations    Aggregation functions"
    echo "  tier3_joins           Join operations"
    echo "  tier4_window_functions Window functions"
    echo "  tier5_complex         Complex queries"
    echo "  tier6_edge_cases      Edge cases"
    echo "  tier7_serialization   Serialization formats"
    echo "  tier8_fault_tolerance Fault tolerance"
}

# List all tests
list_tests() {
    echo "Available Tests:"
    echo "================"
    for tier_dir in "${SCRIPT_DIR}"/tier*/; do
        tier_name=$(basename "$tier_dir")
        echo ""
        echo -e "${BLUE}${tier_name}${NC}"
        for sql_file in "${tier_dir}"*.sql; do
            if [[ -f "$sql_file" ]]; then
                test_name=$(basename "$sql_file" .sql)
                test_key="${tier_name}/${test_name}"
                blocked_reason=$(is_blocked "$test_key")
                if [[ -n "$blocked_reason" ]]; then
                    echo -e "  ${YELLOW}⚠ ${test_name}${NC} - ${blocked_reason}"
                else
                    echo -e "  ${GREEN}✓ ${test_name}${NC}"
                fi
            fi
        done
    done
}

# Run a single test
run_test() {
    local tier_dir="$1"
    local sql_file="$2"
    local tier_name=$(basename "$tier_dir")
    local test_name=$(basename "$sql_file" .sql)
    local test_yaml="${tier_dir}/${test_name}.test.yaml"
    local test_key="${tier_name}/${test_name}"

    ((TOTAL++)) || true

    # Check if test is blocked
    local blocked_reason=$(is_blocked "$test_key")
    if [[ -n "$blocked_reason" ]] && [[ "$SKIP_BLOCKED" == "true" ]]; then
        echo -e "${YELLOW}⏭ SKIP${NC} ${tier_name}/${test_name} - ${blocked_reason}"
        ((SKIPPED++)) || true
        return 0
    fi

    # Check if test spec exists
    if [[ ! -f "$test_yaml" ]]; then
        echo -e "${YELLOW}⏭ SKIP${NC} ${tier_name}/${test_name} - No test spec"
        ((SKIPPED++)) || true
        return 0
    fi

    echo -ne "▶ Running ${tier_name}/${test_name}... "

    # Run the test
    local output
    local exit_code=0

    cd "$tier_dir"
    output=$(timeout "$TIMEOUT" "$VELO_TEST" run "$sql_file" \
        --spec "$test_yaml" \
        --schemas "$SCHEMAS_DIR" 2>&1) || exit_code=$?
    cd "$SCRIPT_DIR"

    # Check result
    if echo "$output" | grep -q "ALL TESTS PASSED"; then
        echo -e "${GREEN}✅ PASSED${NC}"
        ((PASSED++)) || true
        return 0
    elif [[ $exit_code -eq 124 ]]; then
        echo -e "${RED}❌ TIMEOUT${NC}"
        ((FAILED++)) || true
        if [[ "$VERBOSE" == "true" ]]; then
            echo "$output" | tail -20
        fi
        return 1
    else
        echo -e "${RED}❌ FAILED${NC}"
        ((FAILED++)) || true
        if [[ "$VERBOSE" == "true" ]]; then
            echo "$output" | tail -30
        elif echo "$output" | grep -q "Error:"; then
            echo "  $(echo "$output" | grep "Error:" | head -1)"
        fi
        return 1
    fi
}

# Run all tests in a tier
run_tier() {
    local tier_dir="$1"
    local tier_name=$(basename "$tier_dir")

    echo ""
    echo -e "${BLUE}════════════════════════════════════════${NC}"
    echo -e "${BLUE}  ${tier_name}${NC}"
    echo -e "${BLUE}════════════════════════════════════════${NC}"

    for sql_file in "${tier_dir}"*.sql; do
        if [[ -f "$sql_file" ]]; then
            run_test "$tier_dir" "$(basename "$sql_file")" || true
        fi
    done
}

# Parse arguments
SKIP_BLOCKED=false
VERBOSE=false
TARGET_TIER=""
TARGET_TEST=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            usage
            exit 0
            ;;
        --list|-l)
            list_tests
            exit 0
            ;;
        --skip-blocked)
            SKIP_BLOCKED=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        tier*)
            TARGET_TIER="$1"
            shift
            if [[ $# -gt 0 ]] && [[ ! "$1" =~ ^-- ]]; then
                TARGET_TEST="$1"
                shift
            fi
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check velo-test binary
if [[ ! -x "$VELO_TEST" ]]; then
    echo -e "${RED}Error: velo-test binary not found at ${VELO_TEST}${NC}"
    echo "Run: cargo build --release --bin velo-test"
    exit 1
fi

# Check schemas directory
if [[ ! -d "$SCHEMAS_DIR" ]]; then
    echo -e "${RED}Error: Schemas directory not found at ${SCHEMAS_DIR}${NC}"
    exit 1
fi

echo "🧪 Velostream Test Harness Runner"
echo "════════════════════════════════════════"
echo "Binary: $VELO_TEST"
echo "Schemas: $SCHEMAS_DIR"
echo "Timeout: ${TIMEOUT}s"
[[ "$SKIP_BLOCKED" == "true" ]] && echo "Mode: Skipping blocked tests"
echo ""

# Run tests
if [[ -n "$TARGET_TEST" ]]; then
    # Run specific test - find matching tier directory
    tier_dir=""
    for d in "${SCRIPT_DIR}/${TARGET_TIER}"*/; do
        if [[ -d "$d" ]]; then
            tier_dir="$d"
            break
        fi
    done
    if [[ -d "$tier_dir" ]]; then
        sql_file=$(ls "${tier_dir}${TARGET_TEST}"*.sql 2>/dev/null | head -1)
        if [[ -f "$sql_file" ]]; then
            run_test "$tier_dir" "$(basename "$sql_file")"
        else
            echo -e "${RED}Error: Test ${TARGET_TEST} not found in ${TARGET_TIER}${NC}"
            exit 1
        fi
    else
        echo -e "${RED}Error: Tier ${TARGET_TIER} not found${NC}"
        exit 1
    fi
elif [[ -n "$TARGET_TIER" ]]; then
    # Run specific tier - find matching tier directory
    tier_dir=""
    for d in "${SCRIPT_DIR}/${TARGET_TIER}"*/; do
        if [[ -d "$d" ]]; then
            tier_dir="$d"
            break
        fi
    done
    if [[ -d "$tier_dir" ]]; then
        run_tier "$tier_dir"
    else
        echo -e "${RED}Error: Tier ${TARGET_TIER} not found${NC}"
        exit 1
    fi
else
    # Run all tiers
    for tier_dir in "${SCRIPT_DIR}"/tier*/; do
        if [[ -d "$tier_dir" ]]; then
            run_tier "$tier_dir"
        fi
    done
fi

# Print summary
echo ""
echo "════════════════════════════════════════"
echo "📊 Summary"
echo "════════════════════════════════════════"
echo -e "  ${GREEN}Passed:${NC}  $PASSED"
echo -e "  ${RED}Failed:${NC}  $FAILED"
echo -e "  ${YELLOW}Skipped:${NC} $SKIPPED"
echo "  ─────────────────"
echo "  Total:   $TOTAL"
echo ""

if [[ $FAILED -eq 0 ]]; then
    if [[ $PASSED -gt 0 ]]; then
        echo -e "${GREEN}🎉 All executed tests passed!${NC}"
    else
        echo -e "${YELLOW}⚠ No tests were executed${NC}"
    fi
    exit 0
else
    echo -e "${RED}❌ $FAILED test(s) failed${NC}"
    exit 1
fi
