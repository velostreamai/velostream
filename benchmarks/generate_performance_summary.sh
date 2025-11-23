#!/bin/bash

################################################################################
# Generate Clean Performance Summary from Benchmark Results
#
# This script extracts and displays performance metrics from benchmark output
# in a clean, organized table format
#
# Usage:
#   ./generate_performance_summary.sh <results_file>
#   ./generate_performance_summary.sh sql_operations_results_20251123_102400.txt
#
################################################################################

RESULTS_FILE="${1:-}"

if [ -z "$RESULTS_FILE" ]; then
    echo "❌ Usage: $0 <results_file>"
    echo "Example: $0 sql_operations_results_20251123_102400.txt"
    exit 1
fi

if [ ! -f "$RESULTS_FILE" ]; then
    echo "❌ File not found: $RESULTS_FILE"
    exit 1
fi

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo "        Velostream SQL Operations Performance Summary"
echo "════════════════════════════════════════════════════════════════════════════"
echo ""

# Count unique operations (look for BENCHMARK_RESULT, ignoring emoji prefix)
op_count=$(grep "BENCHMARK_RESULT" "$RESULTS_FILE" | sed 's/^.*BENCHMARK_RESULT/BENCHMARK_RESULT/' | cut -d'|' -f2 | sort -u | wc -l)
echo "📊 Results from: $RESULTS_FILE"
echo "   Total operations: $op_count"
echo ""

# Display results in table format
# Format: 🚀 BENCHMARK_RESULT | operation | tier | SQL Sync: X | SQL Async: Y | ...
echo "──────────────────────────────────────────────────────────────────────────────────────────────"
printf "%-30s | %-15s | %s\n" "Operation" "Tier" "Throughput (rec/sec)"
echo "──────────────────────────────────────────────────────────────────────────────────────────────"

grep "BENCHMARK_RESULT" "$RESULTS_FILE" | sed 's/^.*BENCHMARK_RESULT/BENCHMARK_RESULT/' | while IFS='|' read -r prefix op tier results; do
    op=$(echo "$op" | xargs)        # trim whitespace
    tier=$(echo "$tier" | xargs)    # trim whitespace
    results=$(echo "$results" | xargs)  # trim whitespace
    printf "%-30s | %-15s | %s\n" "$op" "$tier" "$results"
done

echo "────────────────────────────────────────────────────────────────────────────"
echo ""

# Summary statistics
result_count=$(grep -c "BENCHMARK_RESULT" "$RESULTS_FILE")
echo "📈 Performance Metrics:"
echo "   Results captured: $result_count lines"
echo "   Expected: 14 operations"
echo ""

if [ "$op_count" -eq 14 ]; then
    echo -e "${GREEN}✅ All 14 SQL operations completed successfully!${NC}"
else
    echo -e "${BLUE}ℹ️  $op_count of 14 operations found${NC}"
fi

echo ""
echo "════════════════════════════════════════════════════════════════════════════"
echo ""
echo "📋 Tier Breakdown:"
echo "   Tier 1 (Essential): select_where, rows_window, group_by_continuous,"
echo "                       tumbling_window, stream_table_join"
echo "   Tier 2 (Common):    scalar_subquery, timebased_join, having_clause"
echo "   Tier 3 (Advanced):  exists_subquery, stream_stream_join, in_subquery,"
echo "                       correlated_subquery"
echo "   Tier 4 (Specialized): any_all_operators, recursive_ctes"
echo ""
