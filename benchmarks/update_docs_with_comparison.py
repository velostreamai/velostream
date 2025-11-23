#!/usr/bin/env python3

"""
Update STREAMING_SQL_OPERATION_RANKING.md with Velostream vs Flink comparison

This script extracts benchmark results and creates a comprehensive comparison table
showing Velostream performance alongside Flink/ksqlDB baseline metrics.

Usage:
    ./update_docs_with_comparison.py [results_file]
    ./update_docs_with_comparison.py                  # Uses latest results
"""

import sys
import re
from pathlib import Path

# Flink/ksqlDB baseline data (2024 benchmarks - approximate)
FLINK_BASELINES = {
    'select_where': {
        'flink': 150,
        'description': 'SELECT with WHERE clause - basic filtering and projection'
    },
    'rows_window': {
        'flink': 140,
        'description': 'ROWS WINDOW with analytic functions (LAG, LEAD, ROW_NUMBER)'
    },
    'group_by_continuous': {
        'flink': 18,
        'description': 'Continuous GROUP BY aggregation without time windows'
    },
    'tumbling_window': {
        'flink': 25,
        'description': 'Tumbling time windows with GROUP BY and aggregate functions'
    },
    'stream_table_join': {
        'flink': 18,
        'description': 'Stream enrichment via JOIN with slowly-changing reference table'
    },
    'scalar_subquery': {
        'flink': 120,
        'description': 'Scalar subqueries returning single values in expressions'
    },
    'timebased_join': {
        'flink': 15,
        'description': 'Time-based stream JOINs with WITHIN INTERVAL temporal constraint'
    },
    'having_clause': {
        'flink': 20,
        'description': 'Post-aggregation filtering using HAVING with aggregate conditions'
    },
    'exists_subquery': {
        'flink': 65,
        'description': 'EXISTS and NOT EXISTS subquery conditions'
    },
    'stream_stream_join': {
        'flink': 12,
        'description': 'Stream-to-stream temporal JOINs with shared time windows'
    },
    'in_subquery': {
        'flink': 80,
        'description': 'IN and NOT IN membership testing against subquery results'
    },
    'correlated_subquery': {
        'flink': 55,
        'description': 'Correlated subqueries referencing outer query columns'
    },
    'any_all_operators': {
        'flink': 70,
        'description': 'ANY and ALL comparison operators with subqueries'
    },
    'recursive_ctes': {
        'flink': 15,
        'description': 'Recursive Common Table Expressions for hierarchical queries'
    }
}

def parse_benchmark_result(line):
    """Parse a BENCHMARK_RESULT line into components"""
    line = re.sub(r'^.*🚀\s*', '', line).strip()
    parts = [p.strip() for p in line.split('|')]

    if len(parts) < 4:
        return None

    operation = parts[1].strip()
    tier = parts[2].strip()

    metrics = {}
    for i in range(3, len(parts)):
        metric_str = parts[i]
        match = re.match(r'([^:]+):\s*(\d+)', metric_str)
        if match:
            name = match.group(1).strip()
            value = int(match.group(2))
            metrics[name] = value

    return operation, tier, metrics

def generate_comparison_table(results_file):
    """Generate comparison table with Velostream vs Flink metrics"""

    with open(results_file, 'r') as f:
        content = f.read()

    lines = [line for line in content.split('\n') if 'BENCHMARK_RESULT' in line]

    results = []
    for line in lines:
        parsed = parse_benchmark_result(line)
        if parsed:
            operation, tier, metrics = parsed
            peak = max(metrics.values()) if metrics else 0
            sql_sync = metrics.get('SQL Sync', 0)

            # Get Flink baseline (stored in thousands, convert to actual evt/sec)
            flink_data = FLINK_BASELINES.get(operation, {})
            flink_baseline_k = flink_data.get('flink', 0)
            flink_baseline = flink_baseline_k * 1000  # Convert thousands to actual value

            # Calculate multiplier
            multiplier = (sql_sync / flink_baseline) if flink_baseline > 0 else 0
            multiplier_str = f"{multiplier:.1f}x" if multiplier > 0 else "N/A"

            results.append({
                'operation': operation,
                'tier': tier,
                'velostream': sql_sync,
                'peak': peak,
                'flink': flink_baseline,  # Stored as actual evt/sec value (not thousands)
                'multiplier': multiplier_str,
                'description': flink_data.get('description', 'SQL operation test')
            })

    results.sort(key=lambda x: x['operation'])

    # Generate main comparison table
    table_lines = [
        "| Operation | Tier | Velostream (evt/sec) | Flink Baseline | Multiplier | Status |",
        "|-----------|------|----------------------|-----------------|------------|--------|"
    ]

    for r in results:
        # Determine status based on multiplier
        try:
            mult_val = float(r['multiplier'].rstrip('x'))
            if mult_val >= 1.0:
                status = "✅ Exceeds"
            elif mult_val >= 0.8:
                status = "✅ Good"
            elif mult_val >= 0.6:
                status = "⚠️ Acceptable"
            else:
                status = "⚠️ Below Target"
        except:
            status = "N/A"

        table_lines.append(
            f"| {r['operation']:<20} | {r['tier']:<5} | **{r['velostream']:>6,}** | {r['flink']/1000:>6.0f}K | {r['multiplier']:>10} | {status:<12} |"
        )

    return "\n".join(table_lines), results

def update_documentation(docs_file, table_content):
    """Update documentation with comparison table"""

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

def find_latest_results():
    """Find the most recent results file"""
    results_dir = Path("/Users/navery/RustroverProjects/velostream/benchmarks/results")
    files = sorted(results_dir.glob("sql_operations_results_*.txt"), reverse=True)
    return files[0] if files else None

def main():
    docs_file = "/Users/navery/RustroverProjects/velostream/docs/sql/STREAMING_SQL_OPERATION_RANKING.md"

    # Get results file
    if len(sys.argv) > 1:
        results_file = sys.argv[1]
        if not Path(results_file).exists():
            print(f"❌ File not found: {results_file}")
            sys.exit(1)
    else:
        results_file = find_latest_results()
        if not results_file:
            print("❌ No benchmark results file found")
            sys.exit(1)

    print(f"📊 Using results file: {results_file}")

    if not Path(docs_file).exists():
        print(f"❌ Documentation file not found: {docs_file}")
        sys.exit(1)

    # Generate comparison table
    print("📝 Generating Velostream vs Flink comparison table...")
    table, results_data = generate_comparison_table(str(results_file))

    print("\nGenerated Comparison Table:")
    print("=" * 140)
    print(table)
    print("=" * 140)
    print()

    # Print summary stats
    total = len(results_data)
    exceeds = sum(1 for r in results_data if "Exceeds" in str(r.get('multiplier', '')))
    print(f"Summary: {total} operations")
    print(f"  ✅ Exceeds Flink baseline: {exceeds}")
    print()

    # Update documentation
    print("✏️  Updating documentation...")
    if update_documentation(docs_file, table):
        print("✅ Documentation updated successfully!")
        print()
        print(f"📄 Updated file: {docs_file}")
        print(f"📊 Results from: {Path(results_file).name}")
        print()
        print("Next steps:")
        print("  1. Review changes: git diff docs/sql/STREAMING_SQL_OPERATION_RANKING.md")
        print("  2. Commit if satisfied: git add docs/ && git commit -m 'perf: Update benchmark comparison'")
    else:
        sys.exit(1)

if __name__ == '__main__':
    main()
