#!/bin/bash

################################################################################
# Complete Benchmark Workflow: Run Tests → Update Documentation
#
# This script automates the entire workflow:
# 1. Run all SQL operation benchmarks
# 2. Extract results with BOTH fastest JobProcessor AND SQL Sync performance
# 3. Update documentation
# 4. Generate summary report
#
# Usage:
#   ./run_and_update_benchmarks.sh [MODE] [TIER_FILTER]
#   ./run_and_update_benchmarks.sh                    # Default: release all
#   ./run_and_update_benchmarks.sh debug tier1        # Debug mode, tier1 only
#   ./run_and_update_benchmarks.sh release all        # Release mode, all tiers
#
# Output:
#   - Console: Clean benchmark output
#   - Results file: benchmarks/results/sql_operations_results_YYYYMMDD_HHMMSS.txt
#   - Updated docs: docs/sql/STREAMING_SQL_OPERATION_RANKING.md
#   - Summary report: benchmarks/results/LATEST_SUMMARY.txt
#
################################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Determine script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Configuration
MODE="${1:-release}"
TIER_FILTER="${2:-all}"
RESULTS_DIR="$SCRIPT_DIR/results"
LATEST_SUMMARY="$RESULTS_DIR/LATEST_SUMMARY.txt"

# Validate inputs
if [[ ! "$MODE" =~ ^(debug|release)$ ]]; then
    echo -e "${RED}❌ Invalid mode: $MODE${NC}"
    echo "Usage: $0 [debug|release] [all|tier1|tier2|tier3|tier4]"
    exit 1
fi

if [[ ! "$TIER_FILTER" =~ ^(all|tier1|tier2|tier3|tier4)$ ]]; then
    echo -e "${RED}❌ Invalid tier filter: $TIER_FILTER${NC}"
    echo "Usage: $0 [debug|release] [all|tier1|tier2|tier3|tier4]"
    exit 1
fi

# Ensure results directory exists
mkdir -p "$RESULTS_DIR"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Complete Benchmark Workflow: Test → Update Docs           ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "Configuration:"
echo -e "  Mode:        ${BOLD}$MODE${NC}"
echo -e "  Tier Filter: ${BOLD}$TIER_FILTER${NC}"
echo -e "  Results Dir: ${BOLD}$RESULTS_DIR${NC}"
echo ""

# ============================================================================
# STEP 1: Run Benchmarks
# ============================================================================
echo -e "${GREEN}📊 STEP 1: Running SQL Operation Benchmarks${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

cd "$SCRIPT_DIR"
./run_all_sql_operations.sh "$MODE" "$TIER_FILTER"

# Find the latest results file
RESULTS_FILE=$(ls -t "$RESULTS_DIR"/sql_operations_results_*.txt 2>/dev/null | head -1)

if [ -z "$RESULTS_FILE" ]; then
    echo -e "${RED}❌ No results file generated${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✅ Benchmarks completed${NC}"
echo -e "   Results file: $(basename "$RESULTS_FILE")"
echo ""

# ============================================================================
# STEP 2: Extract Results with Both SQL Sync and Fastest JobProcessor
# ============================================================================
echo -e "${GREEN}📈 STEP 2: Extracting Performance Metrics${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Parse and create summary with both SQL Sync and fastest JobProcessor
python3 << PYTHON_SCRIPT
import re
from pathlib import Path

results_file = "$RESULTS_FILE"
script_dir = "$SCRIPT_DIR"

# JobProcessor implementations (exclude SQL Sync and SQL Async which are SQL implementations)
job_processors = ['SimpleJp', 'TransactionalJp', 'AdaptiveJp (1c)', 'AdaptiveJp (4c)']

with open(results_file, 'r') as f:
    content = f.read()

lines = [line for line in content.split('\n') if 'BENCHMARK_RESULT' in line]

results = []
for line in lines:
    # Remove emoji and prefix
    line_clean = re.sub(r'^.*🚀\s*', '', line).strip()
    parts = [p.strip() for p in line_clean.split('|')]

    if len(parts) < 4:
        continue

    operation = parts[1].strip()
    tier = parts[2].strip()

    # Parse metrics
    metrics = {}
    for i in range(3, len(parts)):
        metric_str = parts[i]
        match = re.match(r'([^:]+):\s*(\d+)', metric_str)
        if match:
            name = match.group(1).strip()
            value = int(match.group(2))
            metrics[name] = value

    # Find SQL Sync value
    sql_sync = metrics.get('SQL Sync', 0)

    # Find fastest JobProcessor
    jp_metrics = {k: v for k, v in metrics.items() if any(jp in k for jp in job_processors)}
    fastest_jp = max(jp_metrics.items(), key=lambda x: x[1]) if jp_metrics else ('N/A', 0)
    fastest_jp_name = fastest_jp[0]
    fastest_jp_value = fastest_jp[1]

    # Peak overall
    peak = max(metrics.values()) if metrics else 0

    results.append({
        'operation': operation,
        'tier': tier,
        'sql_sync': sql_sync,
        'fastest_jp': fastest_jp_name,
        'fastest_jp_value': fastest_jp_value,
        'peak': peak,
        'metrics': metrics
    })

results.sort(key=lambda x: x['operation'])

# Generate summary table
summary_lines = [
    "| Operation | Tier | SQL Sync (evt/sec) | Fastest JobProcessor | Peak (evt/sec) |",
    "|-----------|------|-------------------|----------------------|----------------|"
]

for r in results:
    summary_lines.append(
        f"| {r['operation']:<20} | {r['tier']:<5} | **{r['sql_sync']:>8,}** | {r['fastest_jp']:<20} (**{r['fastest_jp_value']:,}**) | **{r['peak']:>10,}** |"
    )

summary_table = '\n'.join(summary_lines)

print("Performance Metrics Summary:")
print("=" * 140)
print(summary_table)
print("=" * 140)
print()
print(f"Total operations benchmarked: {len(results)}")
print()
print("Columns:")
print("  • SQL Sync: Synchronous SQL Engine performance")
print("  • Fastest JobProcessor: Best-performing job processor (SimpleJp, TransactionalJp, or AdaptiveJp)")
print("  • Peak: Highest throughput across all implementations")
print()

# Save summary to file
summary_file = Path(script_dir) / 'results' / 'LATEST_SUMMARY.txt'
with open(summary_file, 'w') as f:
    f.write("Benchmark Summary - Both SQL Sync and Fastest JobProcessor\n")
    f.write("=" * 140 + "\n\n")
    f.write(summary_table)
    f.write("\n\n")
    f.write("=" * 140 + "\n")
    f.write(f"Generated: {Path(results_file).name}\n")
    f.write(f"Results file: {results_file}\n")

print(f"Summary saved to: {summary_file}")
print()

PYTHON_SCRIPT

echo -e "${GREEN}✅ Metrics extracted${NC}"
echo ""

# ============================================================================
# STEP 3: Update Documentation
# ============================================================================
echo -e "${GREEN}📝 STEP 3: Updating Documentation${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if python3 ./update_docs_from_results.py "$RESULTS_FILE"; then
    echo ""
    echo -e "${GREEN}✅ Documentation updated${NC}"
    echo ""
else
    echo ""
    echo -e "${YELLOW}⚠️  Documentation update failed (continuing anyway)${NC}"
    echo ""
fi

# ============================================================================
# STEP 4: Generate Final Report
# ============================================================================
echo -e "${GREEN}📋 STEP 4: Generating Final Report${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

cat > "$LATEST_SUMMARY" << EOF
╔════════════════════════════════════════════════════════════════════════════╗
║          BENCHMARK WORKFLOW COMPLETION REPORT
╚════════════════════════════════════════════════════════════════════════════╝

Workflow Completed: $(date '+%Y-%m-%d %H:%M:%S')

CONFIGURATION:
  Mode:           $MODE
  Tier Filter:    $TIER_FILTER

RESULTS:
  Results File:   $(basename "$RESULTS_FILE")
  Location:       $RESULTS_FILE

OUTPUTS GENERATED:
  ✅ Benchmark Results
  ✅ Documentation Updated (docs/sql/STREAMING_SQL_OPERATION_RANKING.md)
  ✅ Performance Summary Report

NEXT STEPS:
  1. Review changes:
     $ git diff docs/sql/STREAMING_SQL_OPERATION_RANKING.md

  2. View detailed summary:
     $ cat "$LATEST_SUMMARY"

  3. Commit results:
     $ git add docs/
     $ git commit -m "perf: Update benchmark results - $(date +%Y%m%d)"

  4. (Optional) Push to remote:
     $ git push origin branch-name

KEY METRICS TRACKED:
  • SQL Sync Performance        (Synchronous SQL Engine)
  • Fastest JobProcessor        (SimpleJp, TransactionalJp, or AdaptiveJp)
  • Peak Throughput Across All  (Maximum performance)

═════════════════════════════════════════════════════════════════════════════

For detailed metrics, see:
  - $LATEST_SUMMARY
  - Results file: $RESULTS_FILE
  - Updated docs: docs/sql/STREAMING_SQL_OPERATION_RANKING.md

═════════════════════════════════════════════════════════════════════════════
EOF

cat "$LATEST_SUMMARY"

echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║           ✅ WORKFLOW COMPLETE                               ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "Summary available at:"
echo -e "  ${BOLD}$LATEST_SUMMARY${NC}"
echo ""
echo -e "Review and commit changes:"
echo -e "  ${BOLD}git diff docs/sql/STREAMING_SQL_OPERATION_RANKING.md${NC}"
echo ""
