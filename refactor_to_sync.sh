#!/bin/bash

# Script to refactor test files from async execute_with_record().await to sync execute_with_record_sync()
# This script makes surgical replacements only for the specific patterns identified

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Color codes for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Velostream Test Refactoring Script${NC}"
echo -e "${BLUE}Async execute_with_record() → Sync execute_with_record_sync()${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Array of all files to refactor
files=(
    "tests/integration/alias_reuse_trading_integration_test.rs"
    "tests/integration/emit_functionality_test.rs"
    "tests/integration/execution_engine_test.rs"
    "tests/integration/post_cleanup_validation_test.rs"
    "tests/integration/sql_integration_test.rs"
    "tests/performance/analysis/comprehensive_baseline_comparison.rs"
    "tests/performance/analysis/phase5_week8_profiling.rs"
    "tests/performance/analysis/scenario_0_pure_select_baseline.rs"
    "tests/performance/analysis/scenario_1_rows_window_baseline.rs"
    "tests/performance/analysis/scenario_3a_tumbling_standard_baseline.rs"
    "tests/performance/analysis/scenario_3b_tumbling_emit_changes_baseline.rs"
    "tests/performance/analysis/test_helpers.rs"
    "tests/performance/unit/rows_window_emit_changes_sql_benchmarks.rs"
    "tests/performance/unit/time_window_sql_benchmarks.rs"
    "tests/unit/server/v2/phase6_all_scenarios_release_benchmark.rs"
    "tests/unit/server/v2/phase6_batch_profiling.rs"
    "tests/unit/server/v2/phase6_lockless_stp.rs"
    "tests/unit/server/v2/phase6_output_overhead_analysis.rs"
    "tests/unit/server/v2/phase6_profiling_breakdown.rs"
    "tests/unit/server/v2/phase6_raw_engine_performance.rs"
    "tests/unit/server/v2/phase6_stp_bottleneck_analysis.rs"
    "tests/unit/sql/execution/aggregation/group_by_test.rs"
    "tests/unit/sql/execution/common_test_utils.rs"
    "tests/unit/sql/execution/core/basic_execution_test.rs"
    "tests/unit/sql/execution/core/csas_ctas_test.rs"
    "tests/unit/sql/execution/core/error_handling_test.rs"
    "tests/unit/sql/execution/expression/arithmetic/operator_test.rs"
    "tests/unit/sql/execution/expression/expression_evaluation_test.rs"
    "tests/unit/sql/execution/phase_1a_streaming_test.rs"
    "tests/unit/sql/execution/processors/join/join_test.rs"
    "tests/unit/sql/execution/processors/join/subquery_join_test.rs"
    "tests/unit/sql/execution/processors/join/subquery_on_condition_test.rs"
    "tests/unit/sql/execution/processors/join/test_helpers.rs"
    "tests/unit/sql/execution/processors/limit/limit_test.rs"
    "tests/unit/sql/execution/processors/window/shared_test_utils.rs"
    "tests/unit/sql/execution/processors/window/windowing_test.rs"
    "tests/unit/sql/functions/advanced_analytics_functions_test.rs"
    "tests/unit/sql/functions/date_functions_test.rs"
    "tests/unit/sql/functions/header_functions_test.rs"
    "tests/unit/sql/functions/interval_test.rs"
    "tests/unit/sql/functions/math_functions_test.rs"
    "tests/unit/sql/functions/new_functions_test.rs"
    "tests/unit/sql/functions/statistical_functions_test.rs"
    "tests/unit/sql/functions/window_functions_test.rs"
    "tests/unit/sql/parser/case_when_test.rs"
    "tests/unit/sql/parser/emit_mode_test.rs"
    "tests/unit/sql/system/system_columns_test.rs"
    "tests/unit/sql/types/advanced_types_test.rs"
    "tests/unit/sql/types/headers_test.rs"
)

total_files=0
total_replacements=0
files_modified=0

# Create backup directory
BACKUP_DIR="${SCRIPT_DIR}/.refactor_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"
echo -e "${YELLOW}Backup directory created: $BACKUP_DIR${NC}"
echo ""

# Function to count occurrences in a file
count_pattern() {
    local file="$1"
    local pattern="$2"
    grep -o "$pattern" "$file" 2>/dev/null | wc -l | tr -d ' '
}

# Process each file
for file in "${files[@]}"; do
    if [ ! -f "$file" ]; then
        echo -e "${YELLOW}⚠️  File not found: $file${NC}"
        continue
    fi

    total_files=$((total_files + 1))

    # Create backup
    backup_path="$BACKUP_DIR/$(dirname "$file")"
    mkdir -p "$backup_path"
    cp "$file" "$backup_path/$(basename "$file")"

    # Count patterns before replacement
    count_before=$(count_pattern "$file" "execute_with_record.*\.await")

    if [ "$count_before" -eq 0 ]; then
        echo -e "📄 $file - ${BLUE}No changes needed${NC}"
        continue
    fi

    echo -e "${GREEN}📝 Processing: $file${NC}"
    echo -e "   Found $count_before execute_with_record().await calls"

    # Create temporary file for processing
    temp_file="${file}.tmp"

    # Step 1: Replace execute_with_record(&query, &record).await with execute_with_record_sync(&query, &record)
    perl -pe 's/execute_with_record\(([^)]+)\)\.await/execute_with_record_sync($1)/g' "$file" > "$temp_file"

    # Step 2: Replace execute_with_record(&self.query, &record).await with execute_with_record_sync(&self.query, &record)
    # (This is actually covered by the first pattern, but we'll be explicit)
    perl -pe 's/execute_with_record_sync\(([^)]+)\)\.await/execute_with_record_sync($1)/g' "$temp_file" > "${temp_file}.2"
    mv "${temp_file}.2" "$temp_file"

    # Move temp file to original
    mv "$temp_file" "$file"

    # Count after replacement
    count_after=$(count_pattern "$file" "execute_with_record_sync")

    echo -e "   ${GREEN}✓${NC} Replaced $count_after occurrences"
    echo ""

    total_replacements=$((total_replacements + count_after))
    files_modified=$((files_modified + 1))
done

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Refactoring Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Files processed: ${GREEN}$total_files${NC}"
echo -e "Files modified: ${GREEN}$files_modified${NC}"
echo -e "Total replacements: ${GREEN}$total_replacements${NC}"
echo -e "Backup location: ${YELLOW}$BACKUP_DIR${NC}"
echo ""

echo -e "${YELLOW}Note: This script only replaced execute_with_record().await calls.${NC}"
echo -e "${YELLOW}Manual review needed for:${NC}"
echo -e "${YELLOW}  - Removing #[tokio::test] attributes${NC}"
echo -e "${YELLOW}  - Changing async fn to fn (when no other async calls)${NC}"
echo -e "${YELLOW}  - Verifying test logic still correct${NC}"
echo ""

echo -e "${BLUE}Next steps:${NC}"
echo -e "1. Review changes: ${GREEN}git diff${NC}"
echo -e "2. Verify compilation: ${GREEN}cargo check${NC}"
echo -e "3. Run tests: ${GREEN}cargo test --no-default-features${NC}"
echo -e "4. If issues occur, restore from: ${YELLOW}$BACKUP_DIR${NC}"
echo ""
