#!/bin/bash

################################################################################
# Velostream SQL Operations Performance Benchmark Collector
#
# This script runs all SQL operation performance tests organized by tier
# and collects metrics for updating STREAMING_SQL_OPERATION_RANKING.md
#
# Usage:
#   ./run_all_sql_operations.sh                    # Run all tests, debug mode
#   ./run_all_sql_operations.sh release            # Run all tests, release mode
#   ./run_all_sql_operations.sh debug tier1        # Run only Tier 1, debug mode
#   ./run_all_sql_operations.sh release tier2      # Run only Tier 2, release mode
#
# Output: Creates benchmarks/sql_operations_results_TIMESTAMP.txt with all metrics
################################################################################

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="$RESULTS_DIR/sql_operations_results_${TIMESTAMP}.txt"

# Command-line arguments
MODE="${1:-debug}"
TIER_FILTER="${2:-all}"

# Validate mode
if [[ "$MODE" != "debug" && "$MODE" != "release" ]]; then
    echo "❌ Invalid mode: $MODE"
    echo "Usage: $0 [debug|release] [all|tier1|tier2|tier3|tier4]"
    exit 1
fi

# Validate tier filter
if [[ "$TIER_FILTER" != "all" && "$TIER_FILTER" != "tier1" && "$TIER_FILTER" != "tier2" && "$TIER_FILTER" != "tier3" && "$TIER_FILTER" != "tier4" ]]; then
    echo "❌ Invalid tier filter: $TIER_FILTER"
    echo "Valid options: all, tier1, tier2, tier3, tier4"
    exit 1
fi

# Create results directory
mkdir -p "$RESULTS_DIR"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

################################################################################
# Helper Functions
################################################################################

print_header() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════════════════════╗"
    echo "║ $1"
    echo "╚════════════════════════════════════════════════════════════════════════════╝"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_to_file() {
    echo "$1" >> "$RESULTS_FILE"
}

log_section_to_file() {
    echo "" >> "$RESULTS_FILE"
    echo "════════════════════════════════════════════════════════════════" >> "$RESULTS_FILE"
    echo "$1" >> "$RESULTS_FILE"
    echo "════════════════════════════════════════════════════════════════" >> "$RESULTS_FILE"
    echo "" >> "$RESULTS_FILE"
}

run_tier_tests() {
    local tier=$1
    local tier_name=$2
    local operations=()

    case "$tier" in
        "tier1")
            operations=("select_where" "rows_window" "group_by_continuous" "tumbling_window" "stream_table_join")
            tier_name="Tier 1: Essential Operations (90-100% Probability)"
            ;;
        "tier2")
            operations=("scalar_subquery" "timebased_join" "having_clause")
            tier_name="Tier 2: Common Operations (60-89% Probability)"
            ;;
        "tier3")
            operations=("exists_subquery" "stream_stream_join" "in_subquery" "correlated_subquery")
            tier_name="Tier 3: Advanced Operations (30-59% Probability)"
            ;;
        "tier4")
            operations=("any_all_operators" "recursive_ctes")
            tier_name="Tier 4: Specialized Operations (10-29% Probability)"
            ;;
    esac

    print_section "$tier_name"
    log_section_to_file "$tier_name"

    for op in "${operations[@]}"; do
        run_operation_test "$tier" "$op"
    done
}

run_operation_test() {
    local tier=$1
    local operation=$2
    local test_name

    echo -n "Running $operation... "

    # Handle special cases for test function names
    case "$operation" in
        "stream_table_join")
            test_name="test_stream_table_join_baseline_performance"
            ;;
        "recursive_ctes")
            test_name="test_recursive_cte_performance"
            ;;
        *)
            test_name="test_${operation}_performance"
            ;;
    esac

    # Determine build flags
    local build_flag=""
    if [[ "$MODE" == "release" ]]; then
        build_flag="--release"
    fi

    # Run the test and capture output using pattern matching
    # Note: build was done upfront, so per-test timeout can be shorter than before
    # (150 seconds accounts for test execution time, not build time)
    local test_output
    if test_output=$(cd "$PROJECT_ROOT" && timeout 150 cargo test "$test_name" --no-default-features $build_flag -- --nocapture --test-threads=1 2>&1); then
        # Check if any tests actually ran by looking for the emoji markers
        if echo "$test_output" | grep -q "🚀"; then
            print_success "$operation"

            # Extract just the BENCHMARK_RESULT line and display it concisely
            local benchmark_line=$(echo "$test_output" | grep "🚀 BENCHMARK_RESULT")
            if [ -n "$benchmark_line" ]; then
                # Show concise output: just the emoji + BENCHMARK_RESULT data (strip test path)
                # Format: 🚀 operation_name | tier | metrics
                echo "$benchmark_line" | sed 's/.*test result://' | grep -o "🚀 BENCHMARK_RESULT.*" | sed 's/🚀 /  🚀 /'
            fi

            # Log all output related to the test results
            log_to_file "📊 Operation: $operation ($tier)"
            log_to_file "─────────────────────────────────────────────────────────────"

            # Capture all BENCHMARK_RESULT lines (they start with 🚀 emoji) for report parsing
            # Extract just the emoji + BENCHMARK_RESULT part, stripping the test path prefix
            echo "$test_output" | grep "BENCHMARK_RESULT" | sed 's/.*🚀 /🚀 /' >> "$RESULTS_FILE" 2>/dev/null || true

            log_to_file ""
        else
            print_error "$operation"
            log_to_file "❌ $operation FAILED - Test not found or didn't run"
            log_to_file "Expected test: $test_name"
            log_to_file ""
        fi
    else
        print_error "$operation"
        log_to_file "❌ $operation FAILED - Timeout or error"
        log_to_file "Expected test: $test_name"
        log_to_file "Error output:"
        echo "$test_output" | tail -30 >> "$RESULTS_FILE"
        log_to_file ""
    fi
}

################################################################################
# Main Execution
################################################################################

print_header "Velostream SQL Operations Performance Benchmark Collector"

echo "Configuration:"
echo "  Mode: $MODE"
echo "  Tier Filter: $TIER_FILTER"
echo "  Results File: $RESULTS_FILE"
echo ""

# Build once upfront to avoid rebuild overhead on each test
echo "Building project ($MODE mode)..."
if [[ "$MODE" == "release" ]]; then
    cd "$PROJECT_ROOT" && cargo test --no-default-features --release --no-run 2>&1 | grep -E "(Compiling|Finished)" || true
else
    cd "$PROJECT_ROOT" && cargo test --no-default-features --no-run 2>&1 | grep -E "(Compiling|Finished)" || true
fi
echo "Build complete."
echo ""

# Initialize results file
cat > "$RESULTS_FILE" << 'EOF'
╔════════════════════════════════════════════════════════════════════════════╗
║           Velostream SQL Operations Performance Benchmarks                 ║
║                                                                            ║
║  This report contains comprehensive performance metrics for all SQL       ║
║  operations organized by tier and use case probability.                   ║
║                                                                            ║
║  Use these results to update: docs/sql/STREAMING_SQL_OPERATION_RANKING.md ║
╚════════════════════════════════════════════════════════════════════════════╝

EOF

echo "Timestamp: $(date)" >> "$RESULTS_FILE"
echo "Mode: $MODE" >> "$RESULTS_FILE"
echo "Tier Filter: $TIER_FILTER" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

# Run tests based on tier filter
if [[ "$TIER_FILTER" == "all" || "$TIER_FILTER" == "tier1" ]]; then
    print_success "Running Tier 1 Essential Operations"
    run_tier_tests "tier1"
fi

if [[ "$TIER_FILTER" == "all" || "$TIER_FILTER" == "tier2" ]]; then
    print_success "Running Tier 2 Common Operations"
    run_tier_tests "tier2"
fi

if [[ "$TIER_FILTER" == "all" || "$TIER_FILTER" == "tier3" ]]; then
    print_success "Running Tier 3 Advanced Operations"
    run_tier_tests "tier3"
fi

if [[ "$TIER_FILTER" == "all" || "$TIER_FILTER" == "tier4" ]]; then
    print_success "Running Tier 4 Specialized Operations"
    run_tier_tests "tier4"
fi

# Summary
print_section "Benchmark Collection Complete ✅"

log_section_to_file "SUMMARY"
log_to_file ""
log_to_file "All performance tests completed successfully!"
log_to_file ""
log_to_file "Results saved to: $RESULTS_FILE"
log_to_file ""
log_to_file "Next steps:"
log_to_file "  1. Review results: cat $RESULTS_FILE"
log_to_file "  2. Extract metrics and update docs/sql/STREAMING_SQL_OPERATION_RANKING.md"
log_to_file "  3. Commit results: git add docs/ && git commit -m 'Update performance metrics'"
log_to_file ""

echo ""
echo "📊 Results Summary:"
echo "─────────────────────────────────────────────────────────────────"
echo ""

# Automatically generate and display the clean summary
SUMMARY_SCRIPT="$SCRIPT_DIR/generate_performance_summary.sh"
if [ -f "$SUMMARY_SCRIPT" ]; then
    echo ""
    "$SUMMARY_SCRIPT" "$RESULTS_FILE"
    echo ""
else
    echo "⚠️  Summary generator not found at $SUMMARY_SCRIPT"
    echo ""
    tail -30 "$RESULTS_FILE"
    echo ""
fi

echo "─────────────────────────────────────────────────────────────────"
echo ""
echo "Complete results saved to:"
echo -e "${GREEN}$RESULTS_FILE${NC}"
echo ""
echo "To view results:"
echo "  cat $RESULTS_FILE"
echo ""
echo "To extract performance data for documentation:"
echo "  grep -E 'BENCHMARK_RESULT' $RESULTS_FILE"
echo ""
