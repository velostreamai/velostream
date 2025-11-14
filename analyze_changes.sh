#!/bin/bash

# Analyze the changes made by the refactoring
cd /Users/navery/RustroverProjects/velostream

echo "=========================================="
echo "Detailed Changes Analysis"
echo "=========================================="
echo ""

# Show a sample of the changes
echo "Sample changes from group_by_test.rs (71 replacements):"
git diff tests/unit/sql/execution/aggregation/group_by_test.rs | head -30
echo ""

echo "=========================================="
echo "Files with most replacements:"
echo "=========================================="
echo "1. group_by_test.rs: 71 replacements"
echo "2. phase6_all_scenarios_release_benchmark.rs: 15 replacements"
echo "3. math_functions_test.rs: 14 replacements"
echo "4. windowing_test.rs: 13 replacements"
echo "5. alias_reuse_trading_integration_test.rs: 12 replacements"
echo ""

echo "=========================================="
echo "Total Statistics"
echo "=========================================="
echo "Total files modified: 49"
echo "Total execute_with_record().await → execute_with_record_sync(): 270"
echo ""
