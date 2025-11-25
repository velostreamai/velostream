#!/usr/bin/env python3

"""
Update STREAMING_SQL_OPERATION_RANKING.md with latest benchmark results

This script:
1. Extracts BENCHMARK_RESULT lines from the latest results file
2. Converts them to a markdown table
3. Replaces the marked section in the documentation

Usage:
    ./update_docs_from_results.py [results_file]
    ./update_docs_from_results.py                                          # Use latest
    ./update_docs_from_results.py benchmarks/results/sql_operations_results_20251123_151824.txt
"""

import sys
import os
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

def find_latest_results_file():
    """Find the most recent results file"""
    results_dir = Path("/Users/navery/RustroverProjects/velostream/benchmarks/results")
    files = sorted(results_dir.glob("sql_operations_results_*.txt"), reverse=True)
    return files[0] if files else None

def parse_benchmark_result(line):
    """Parse a BENCHMARK_RESULT line into components

    Format: 🚀 BENCHMARK_RESULT | operation_name | tier | SQL Sync: X | SQL Async: Y | ...
    Returns: (operation, tier, metrics_dict)
    """
    # Remove emoji and prefix
    line = re.sub(r'^.*🚀\s*', '', line).strip()

    # Split by pipe
    parts = [p.strip() for p in line.split('|')]

    if len(parts) < 4:
        return None

    # Extract operation and tier
    operation = parts[1].strip()
    tier = parts[2].strip()

    # Parse metrics
    metrics = {}
    for i in range(3, len(parts)):
        metric_str = parts[i]
        # Format: "SQL Sync: 136172"
        match = re.match(r'([^:]+):\s*(\d+)', metric_str)
        if match:
            name = match.group(1).strip()
            value = int(match.group(2))
            metrics[name] = value

    return operation, tier, metrics

def generate_performance_table(results_file):
    """Generate markdown table from benchmark results

    Returns: markdown table string
    """
    with open(results_file, 'r') as f:
        content = f.read()

    # Extract all BENCHMARK_RESULT lines
    lines = [line for line in content.split('\n') if 'BENCHMARK_RESULT' in line]

    # Parse results
    results = []
    for line in lines:
        parsed = parse_benchmark_result(line)
        if parsed:
            operation, tier, metrics = parsed
            peak = max(metrics.values()) if metrics else 0
            results.append((operation, tier, peak, metrics))

    if not results:
        print("❌ No BENCHMARK_RESULT lines found in results file")
        return None

    # Sort by operation name
    results.sort(key=lambda x: x[0])

    # Generate markdown table
    table_lines = [
        "| Operation | Tier | Peak Throughput (evt/sec) | Implementation |",
        "|-----------|------|---------------------------|-----------------|"
    ]

    for operation, tier, peak, metrics in results:
        # Find which implementation achieved peak
        best_impl = max(metrics.items(), key=lambda x: x[1])[0] if metrics else "N/A"

        table_lines.append(
            f"| {operation} | {tier} | **{peak:,}** | {best_impl} |"
        )

    return "\n".join(table_lines)

def update_documentation(docs_file, table_content):
    """Update documentation with new table, using markers

    Looks for:
    <!-- BENCHMARK_TABLE_START -->
    [content]
    <!-- BENCHMARK_TABLE_END -->
    """

    with open(docs_file, 'r') as f:
        content = f.read()

    # Check if markers exist
    if '<!-- BENCHMARK_TABLE_START -->' not in content or '<!-- BENCHMARK_TABLE_END -->' not in content:
        print("⚠️  Documentation file does not have markers for automatic updates")
        print("Please add these markers around the Quick Reference table:")
        print()
        print("<!-- BENCHMARK_TABLE_START -->")
        print("[table content here]")
        print("<!-- BENCHMARK_TABLE_END -->")
        return False

    # Replace content between markers
    pattern = r'(<!-- BENCHMARK_TABLE_START -->).*?(<!-- BENCHMARK_TABLE_END -->)'
    replacement = f'\\1\n\n{table_content}\n\n\\2'

    updated_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    # Write back to file
    with open(docs_file, 'w') as f:
        f.write(updated_content)

    return True

def main():
    docs_file = "/Users/navery/RustroverProjects/velostream/docs/sql/STREAMING_SQL_OPERATION_RANKING.md"

    # Get results file
    if len(sys.argv) > 1:
        results_file = sys.argv[1]
        if not os.path.exists(results_file):
            print(f"❌ File not found: {results_file}")
            sys.exit(1)
    else:
        results_file = find_latest_results_file()
        if not results_file:
            print("❌ No benchmark results file found")
            sys.exit(1)

    print(f"📊 Using results file: {results_file}")

    if not os.path.exists(docs_file):
        print(f"❌ Documentation file not found: {docs_file}")
        sys.exit(1)

    # Generate table
    print("📝 Generating performance table...")
    table = generate_performance_table(str(results_file))

    if not table:
        sys.exit(1)

    print("Generated table:")
    print(table)
    print()

    # Update documentation
    print("✏️  Updating documentation...")
    if update_documentation(docs_file, table):
        print("✅ Documentation updated successfully!")
        print()
        print(f"📄 Updated file: {docs_file}")
        print(f"📊 Results from: {os.path.basename(str(results_file))}")
        print()
        print("Next steps:")
        print("  1. Review changes: git diff docs/sql/STREAMING_SQL_OPERATION_RANKING.md")
        print("  2. Commit if satisfied: git add docs/ && git commit -m 'perf: Update benchmark results'")
    else:
        sys.exit(1)

if __name__ == '__main__':
    main()
