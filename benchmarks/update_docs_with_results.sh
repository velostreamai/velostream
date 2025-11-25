#!/bin/bash

################################################################################
# Update Documentation with Latest Benchmark Results
#
# This script automatically extracts performance metrics from the latest
# benchmark results file and updates the STREAMING_SQL_OPERATION_RANKING.md
# documentation with a clean performance summary table.
#
# Usage:
#   ./update_docs_with_results.sh [results_file]
#   ./update_docs_with_results.sh                              # Use latest file
#   ./update_docs_with_results.sh sql_operations_results_20251123_151824.txt
#
# The script identifies the section to update using markers:
#   <!-- BENCHMARK_TABLE_START -->
#   <!-- BENCHMARK_TABLE_END -->
#
################################################################################

set -e

# Get the results file (latest if not specified)
RESULTS_FILE="${1:-}"

if [ -z "$RESULTS_FILE" ]; then
    # Find the most recent results file
    RESULTS_FILE=$(ls -t /Users/navery/RustroverProjects/velostream/benchmarks/results/sql_operations_results_*.txt 2>/dev/null | head -1)
    if [ -z "$RESULTS_FILE" ]; then
        echo "❌ No benchmark results file found"
        echo "Usage: $0 [results_file]"
        exit 1
    fi
    echo "📊 Using latest results file: $(basename "$RESULTS_FILE")"
else
    # Check if file exists
    if [ ! -f "$RESULTS_FILE" ]; then
        echo "❌ File not found: $RESULTS_FILE"
        exit 1
    fi
fi

DOCS_FILE="/Users/navery/RustroverProjects/velostream/docs/sql/STREAMING_SQL_OPERATION_RANKING.md"

if [ ! -f "$DOCS_FILE" ]; then
    echo "❌ Documentation file not found: $DOCS_FILE"
    exit 1
fi

echo "📝 Extracting benchmark results..."

# Extract BENCHMARK_RESULT lines and convert to markdown table format
# Format: 🚀 BENCHMARK_RESULT | operation | tier | SQL Sync: X | SQL Async: Y | ...
TABLE_DATA=$(grep "BENCHMARK_RESULT" "$RESULTS_FILE" | sed 's/.*🚀 //' | awk -F'|' '
BEGIN {
    print "| Operation | Tier | Peak Throughput (evt/sec) |"
    print "|-----------|------|--------------------------|"
}
{
    # Trim whitespace from fields
    op = $2; gsub(/^[ \t]+|[ \t]+$/, "", op)
    tier = $3; gsub(/^[ \t]+|[ \t]+$/, "", tier)
    metrics = $4; gsub(/^[ \t]+|[ \t]+$/, "", metrics)

    # Extract the numeric values from the metrics string
    # Format: "SQL Sync: 136172 | SQL Async: 107147 | SimpleJp: 115571 | ..."
    # We want to find the peak value
    peak = 0
    n = split(metrics, parts, "|")
    for (i = 1; i <= n; i++) {
        part = parts[i]
        if (match(part, /[0-9]+/)) {
            val = substr(part, RSTART, RLENGTH)
            if (val+0 > peak) peak = val+0
        }
    }

    printf "| %-20s | %-5s | **%s** |\n", op, tier, peak
}')

echo "Generated table:"
echo "$TABLE_DATA"
echo ""

# Check if documentation file has markers
if ! grep -q "<!-- BENCHMARK_TABLE_START -->" "$DOCS_FILE" 2>/dev/null; then
    echo "⚠️  Documentation file does not have markers for automatic updates"
    echo "Please add these markers to: $DOCS_FILE"
    echo ""
    echo "Add these lines around the Quick Reference table:"
    echo "<!-- BENCHMARK_TABLE_START -->"
    echo "[table content]"
    echo "<!-- BENCHMARK_TABLE_END -->"
    exit 1
fi

echo "✅ Found markers in documentation file"
echo ""

# Create a temporary file with the updated content
TEMP_FILE="/tmp/streaming_sql_ranking_update.md"

# Extract everything before the marker
sed -n '0,/<!-- BENCHMARK_TABLE_START -->/p' "$DOCS_FILE" > "$TEMP_FILE"

# Add the updated table
echo "" >> "$TEMP_FILE"
echo "$TABLE_DATA" >> "$TEMP_FILE"
echo "" >> "$TEMP_FILE"

# Extract everything after the end marker
sed -n '/<!-- BENCHMARK_TABLE_END -->/,$p' "$DOCS_FILE" >> "$TEMP_FILE"

# Replace the original file
cp "$TEMP_FILE" "$DOCS_FILE"
rm "$TEMP_FILE"

echo "✅ Documentation updated successfully!"
echo ""
echo "📄 Updated file: $DOCS_FILE"
echo "📊 Results from: $(basename "$RESULTS_FILE")"
echo ""
echo "Next steps:"
echo "  1. Review changes: git diff docs/sql/STREAMING_SQL_OPERATION_RANKING.md"
echo "  2. Commit if satisfied: git add docs/ && git commit -m 'perf: Update benchmark results'"
