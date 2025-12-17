# File I/O Demo

This example demonstrates file-based SQL processing with the FR-084 Test Harness.
**No Kafka required!**

## What It Does

The SQL query reads trade data from CSV, enriches it with:
- **trade_value**: Calculated as `price * volume`
- **trade_size**: Categorized as LARGE (>$100k), MEDIUM (>$50k), or SMALL

```sql
SELECT
    symbol, price, volume,
    price * volume AS trade_value,
    CASE
        WHEN price * volume > 100000 THEN 'LARGE'
        WHEN price * volume > 50000 THEN 'MEDIUM'
        ELSE 'SMALL'
    END AS trade_size,
    timestamp
FROM trades
WHERE volume >= 50
```

## Files

- `input_trades.csv` - Sample trade data (6 records)
- `expected_output.csv` - Expected enriched output
- `passthrough.sql` - SQL query with file source/sink config
- `test_spec.yaml` - Test specification with file assertions

## Running the Demo

```bash
cd demo/test_harness_examples/file_io

# Run the test
velo-test run passthrough.sql --spec test_spec.yaml

# Or validate SQL syntax only
velo-test validate passthrough.sql
```

## Sample Input

```csv
symbol,price,volume,timestamp
AAPL,150.25,100,2024-01-15T10:00:00
GOOGL,2750.50,50,2024-01-15T10:00:01
MSFT,350.00,150,2024-01-15T10:00:03
```

## Sample Output

```csv
symbol,price,volume,trade_value,trade_size,timestamp
AAPL,150.25,100,15025.0,SMALL,2024-01-15T10:00:00
GOOGL,2750.50,50,137525.0,LARGE,2024-01-15T10:00:01
MSFT,350.00,150,52500.0,MEDIUM,2024-01-15T10:00:03
```

## Features Demonstrated

1. **File-based Source** - Reading test data from CSV files
2. **File-based Sink** - Writing output to CSV files
3. **SQL Processing** - Calculated fields, CASE expressions, WHERE filtering
4. **File Assertions**:
   - `file_exists` - Verify output file was created
   - `file_row_count` - Verify number of rows
   - `file_contains` - Check specific values exist
   - `file_matches` - Compare output to expected file

## SQL WITH Clause for File I/O

```sql
WITH (
    -- File source
    'trades.type' = 'file_source',
    'trades.path' = './input_trades.csv',
    'trades.format' = 'csv',

    -- File sink
    'enriched_trades.type' = 'file_sink',
    'enriched_trades.path' = './output_trades.csv',
    'enriched_trades.format' = 'csv'
)
```

## Test Specification Format

```yaml
queries:
  - name: enriched_trades
    inputs:
      - source: trades
        source_type:
          type: file
          path: ./input_trades.csv
          format: csv
    output:
      sink_type:
        type: file
        path: ./output_trades.csv
        format: csv
    assertions:
      - type: file_row_count
        equals: 6
      - type: file_contains
        field: trade_size
        expected_values: [SMALL, MEDIUM, LARGE]
      - type: file_matches
        expected_path: ./expected_output.csv
        numeric_tolerance: 0.01
```
