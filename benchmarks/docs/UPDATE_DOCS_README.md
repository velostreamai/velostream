# Automatic Documentation Updates from Benchmark Results

This directory contains scripts to automatically extract benchmark results and update the `STREAMING_SQL_OPERATION_RANKING.md` documentation with the latest performance metrics.

## Quick Start

After running benchmarks:

```bash
# Run all benchmarks
./run_all_sql_operations.sh release all

# Update documentation with latest results
./update_docs_from_results.py
```

That's it! The documentation will be updated automatically.

## Tools Available

### 1. Python Script (Recommended)

**File**: `update_docs_from_results.py`

```bash
# Use the most recent results file
./update_docs_from_results.py

# Use a specific results file
./update_docs_from_results.py benchmarks/results/sql_operations_results_20251123_151824.txt

# Use results from specific date
./update_docs_from_results.py results/sql_operations_results_20251123_*.txt
```

**Features**:
- Parses BENCHMARK_RESULT lines from results file
- Extracts performance metrics for each operation
- Identifies best-performing implementation for each operation
- Updates documentation using markers (no manual editing needed)
- Automatic formatting with thousands separators

### 2. Bash Script

**File**: `update_docs_with_results.sh`

```bash
# Use the most recent results file
./update_docs_with_results.sh

# Use a specific results file
./update_docs_with_results.sh sql_operations_results_20251123_151824.txt
```

**Features**:
- Same functionality as Python script
- Bash-only (no Python required)
- Simpler AWK-based parsing

## How It Works

### The Marker System

The documentation file contains markers that identify where to insert the performance table:

```markdown
<!-- BENCHMARK_TABLE_START -->

| Operation | Tier | Peak Throughput | Implementation |
| ... |

<!-- BENCHMARK_TABLE_END -->
```

When you run `update_docs_from_results.py`, it:

1. Finds the latest (or specified) results file
2. Parses BENCHMARK_RESULT lines
3. Generates a markdown table with:
   - Operation name (alphabetically sorted)
   - Tier level
   - Peak throughput (in thousands format)
   - Implementation that achieved the peak
4. Replaces everything between the markers with the new table
5. Preserves all other documentation content

### Example Output

```
| Operation | Tier | Peak Throughput (evt/sec) | Implementation |
|-----------|------|---------------------------|-----------------|
| select_where | tier1 | **136,172** | SQL Sync |
| rows_window | tier1 | **158,025** | SQL Sync |
| group_by_continuous | tier1 | **50,639** | SQL Sync |
| ... |
```

## Workflow: Benchmarks → Documentation Update

### Step 1: Run Benchmarks

```bash
cd benchmarks
./run_all_sql_operations.sh release all
```

Output:
- Console: Clean test output with performance metrics
- Results file: `results/sql_operations_results_YYYYMMDD_HHMMSS.txt`

### Step 2: Update Documentation

```bash
./update_docs_from_results.py
```

Output:
- Extracted table with all 14 operations
- Updated `docs/sql/STREAMING_SQL_OPERATION_RANKING.md`
- Ready to review and commit

### Step 3: Review Changes

```bash
git diff docs/sql/STREAMING_SQL_OPERATION_RANKING.md
```

### Step 4: Commit Results

```bash
git add docs/
git commit -m "perf: Update benchmark results - $(date +%Y%m%d)"
```

## File Format Details

### Input: BENCHMARK_RESULT Format

Results files contain lines like:

```
🚀 BENCHMARK_RESULT | select_where | tier1 | SQL Sync: 136172 | SQL Async: 107147 | SimpleJp: 115571 | TransactionalJp: 115917 | AdaptiveJp (1c): 123872 | AdaptiveJp (4c): 123246
```

**Structure**:
- Emoji prefix: `🚀` (for visual identification)
- Operation name: `select_where`
- Tier: `tier1`
- Metrics: 6 processor implementations with throughput values
- Delimiter: `|` (pipe character)

### Output: Markdown Table

Extracted table:

```markdown
| Operation | Tier | Peak Throughput (evt/sec) | Implementation |
|-----------|------|---------------------------|-----------------|
| select_where | tier1 | **136,172** | SQL Sync |
```

**Columns**:
- **Operation**: SQL operation name
- **Tier**: Tier level (tier1-tier4)
- **Peak Throughput**: Maximum throughput across all 6 processor implementations (formatted with commas)
- **Implementation**: Which processor implementation achieved the peak

## Troubleshooting

### "No benchmark results file found"

**Solution**: Run benchmarks first:
```bash
./run_all_sql_operations.sh release all
```

### "Documentation file does not have markers"

**Solution**: Add markers to the documentation file (should already be there in updated versions):
```markdown
<!-- BENCHMARK_TABLE_START -->
[table content]
<!-- BENCHMARK_TABLE_END -->
```

### "File not found" error

**Solution**: Use full path or ensure you're in the benchmarks directory:
```bash
cd /Users/navery/RustroverProjects/velostream/benchmarks
./update_docs_from_results.py
```

### Table looks incomplete

**Solution**: Check if the results file has complete BENCHMARK_RESULT lines:
```bash
grep "BENCHMARK_RESULT" benchmarks/results/sql_operations_results_*.txt | head -5
```

## Integration with CI/CD

### GitHub Actions Example

```yaml
- name: Run SQL Operation Benchmarks
  run: |
    cd benchmarks
    VELOSTREAM_PERF_RECORDS=1000 ./run_all_sql_operations.sh release all

- name: Update Documentation
  run: |
    cd benchmarks
    ./update_docs_from_results.py

- name: Commit Results
  if: github.event_name == 'push' && github.ref == 'refs/heads/main'
  run: |
    git config user.name "Performance Bot"
    git config user.email "bot@example.com"
    git add docs/
    git commit -m "perf: Update benchmark results" || true
    git push
```

## Performance Tips

### Faster Updates

If you only need to update the documentation (not run benchmarks):

```bash
./update_docs_from_results.py  # Uses latest results file
```

### Batch Updates

Update documentation for multiple result files:

```bash
for file in results/sql_operations_results_*.txt; do
    echo "Processing: $file"
    ./update_docs_from_results.py "$file"
done
```

## Manual Updates

If you need to edit the table manually:

1. Locate the markers in `docs/sql/STREAMING_SQL_OPERATION_RANKING.md`
2. Edit content between markers
3. Next automatic run will replace your changes

To prevent automatic replacement, remove the markers (not recommended).

## Documentation Links

- **Main Documentation**: `docs/sql/STREAMING_SQL_OPERATION_RANKING.md`
- **Benchmark README**: `benchmarks/SQL_OPERATIONS_BENCHMARK_README.md`
- **Script Documentation**: This file
- **Results Location**: `benchmarks/results/sql_operations_results_YYYYMMDD_HHMMSS.txt`

## Version History

- **v1.0** (Nov 2025): Initial release with Python and Bash scripts
  - Automated marker-based updates
  - 14 SQL operations support
  - 6 processor implementations tracked
  - Automatic peak detection and formatting

---

**Last Updated**: November 2025
**Compatible With**: Velostream main branch
