# File I/O Demo

This example demonstrates the file-based input/output capabilities of the FR-084 Test Harness.

## Features Demonstrated

1. **File-based Source** - Reading test data from CSV files instead of Kafka
2. **File-based Sink** - Writing output to CSV files
3. **File Assertions** - Validating output files:
   - `file_exists` - Verify output file was created with optional size checks
   - `file_row_count` - Verify number of rows in output
   - `file_contains` - Check specific values exist in output
   - `file_matches` - Compare output to expected file

## Files

- `input_trades.csv` - Sample trade data input
- `expected_output.csv` - Expected output for comparison
- `passthrough.sql` - Simple SQL passthrough query
- `test_spec.yaml` - Test specification with file assertions

## Running the Test

```bash
# From this directory
velo-test run passthrough.sql --spec test_spec.yaml

# Or using full paths
velo-test run demo/test_harness_examples/tier1_basic/file_io/passthrough.sql \
  --spec demo/test_harness_examples/tier1_basic/file_io/test_spec.yaml
```

## Test Specification Format

The test spec demonstrates the new file-based source/sink types:

```yaml
inputs:
  - source: trades
    source_type:
      type: file
      path: ./input_trades.csv
      format: csv
      watch: false
output:
  sink_type:
    type: file
    path: ./output_trades.csv
    format: csv
```

## File Assertions

### file_exists
Verifies the output file exists with optional size constraints:
```yaml
- type: file_exists
  path: ./output.csv
  min_size_bytes: 100
  max_size_bytes: 10000
```

### file_row_count
Verifies the number of data rows:
```yaml
- type: file_row_count
  path: ./output.csv
  format: csv
  equals: 6        # Exact count
  # or greater_than: 5
  # or less_than: 10
```

### file_contains
Verifies specific values exist in a field:
```yaml
- type: file_contains
  path: ./output.csv
  format: csv
  field: symbol
  expected_values:
    - AAPL
    - GOOGL
  mode: all  # or 'any'
```

### file_matches
Compares output to an expected file:
```yaml
- type: file_matches
  actual_path: ./output.csv
  expected_path: ./expected.csv
  format: csv
  ignore_order: true
  ignore_fields:
    - timestamp
  numeric_tolerance: 0.01
```
