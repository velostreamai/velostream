#!/bin/bash

# Script to identify test functions that may no longer need async/await
# after the execute_with_record_sync() refactoring

set -e

cd /Users/navery/RustroverProjects/velostream

echo "=========================================="
echo "Identifying Async Removal Candidates"
echo "=========================================="
echo ""
echo "Searching for test functions that:"
echo "  1. Are marked with #[tokio::test]"
echo "  2. Have 'async fn' signature"
echo "  3. Only use execute_with_record_sync() (no other .await calls)"
echo ""

# Array of files to check
files=(
    "tests/integration/alias_reuse_trading_integration_test.rs"
    "tests/integration/emit_functionality_test.rs"
    "tests/integration/execution_engine_test.rs"
    "tests/integration/post_cleanup_validation_test.rs"
    "tests/integration/sql_integration_test.rs"
    "tests/unit/sql/execution/core/basic_execution_test.rs"
    "tests/unit/sql/execution/core/error_handling_test.rs"
    "tests/unit/sql/execution/expression/arithmetic/operator_test.rs"
    "tests/unit/sql/functions/math_functions_test.rs"
    "tests/unit/sql/functions/date_functions_test.rs"
    "tests/unit/sql/functions/statistical_functions_test.rs"
    "tests/unit/sql/parser/case_when_test.rs"
    "tests/unit/sql/parser/emit_mode_test.rs"
)

total_candidates=0

for file in "${files[@]}"; do
    if [ ! -f "$file" ]; then
        continue
    fi

    # Look for async test functions
    while IFS= read -r line_num; do
        # Get the test function context (including the function definition)
        test_context=$(sed -n "${line_num},$((line_num + 20))p" "$file")

        # Check if this is an async fn
        if echo "$test_context" | grep -q "async fn"; then
            # Get the function name
            func_name=$(echo "$test_context" | grep "async fn" | head -1 | sed 's/.*async fn \([a-zA-Z0-9_]*\).*/\1/')

            # Count .await occurrences in next 50 lines
            await_count=$(sed -n "${line_num},$((line_num + 50))p" "$file" | grep -c "\.await" || true)

            if [ "$await_count" -eq 0 ]; then
                echo "✓ CANDIDATE: $file::$func_name"
                echo "  → Can remove #[tokio::test] and async fn"
                echo ""
                total_candidates=$((total_candidates + 1))
            fi
        fi
    done < <(grep -n "#\[tokio::test\]" "$file" | cut -d: -f1)
done

echo "=========================================="
echo "Summary"
echo "=========================================="
echo "Total async removal candidates found: $total_candidates"
echo ""
echo "To remove async from a test:"
echo "  1. Change #[tokio::test] → #[test]"
echo "  2. Change async fn test_name() → fn test_name()"
echo "  3. Verify no .await calls remain"
echo ""
