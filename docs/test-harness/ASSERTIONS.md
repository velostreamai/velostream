# Assertions Reference

This document describes all available assertion types in the SQL Test Harness.

## Record Assertions

### record_count

Validates the number of output records.

```yaml
- type: record_count
  operator: equals          # equals, greater_than, less_than, between
  expected: 100             # For equals/greater_than/less_than
  # OR for between:
  min: 50
  max: 150
  message: "Should produce expected number of records"
```

**Operators:**
- `equals` - Exact match
- `not_equals` - Not equal to expected
- `greater_than` - Strictly greater
- `less_than` - Strictly less
- `greater_than_or_equal` - Greater or equal
- `less_than_or_equal` - Less or equal
- `between` - Within range (inclusive)

### schema_contains

Validates that output contains specified fields.

```yaml
- type: schema_contains
  fields:
    - symbol
    - price
    - volume
  message: "Output must contain required fields"
```

### no_nulls

Validates that specified fields have no null values.

```yaml
- type: no_nulls
  fields: [symbol, price, order_id]
  message: "Key fields should never be null"
```

## Value Assertions

### field_values

Validates field values meet a condition.

```yaml
- type: field_values
  field: price
  operator: greater_than
  value: 0
  message: "All prices must be positive"
```

```yaml
- type: field_values
  field: discount
  operator: between
  min: 0.0
  max: 1.0
  message: "Discount must be between 0 and 100%"
```

### field_in_set

Validates field values are within an allowed set.

```yaml
- type: field_in_set
  field: status
  values: [PENDING, APPROVED, REJECTED]
  message: "Status must be one of allowed values"
```

### unique_values

Validates field values are unique.

```yaml
- type: unique_values
  field: order_id
  message: "Order IDs must be unique"
```

## Aggregate Assertions

### aggregate_check

Validates aggregate computations on output.

```yaml
- type: aggregate_check
  expression: "SUM(total_volume)"
  operator: greater_than
  expected: 0
  message: "Total volume should be positive"
```

```yaml
- type: aggregate_check
  expression: "AVG(price)"
  operator: between
  min: 100.0
  max: 500.0
  message: "Average price should be in expected range"
```

**Supported aggregate functions:**
- `SUM(field)` - Sum of values
- `AVG(field)` - Average of values
- `COUNT(*)` - Count of records
- `MIN(field)` - Minimum value
- `MAX(field)` - Maximum value

## Join Assertions

### join_coverage

Validates join effectiveness between two sources.

```yaml
- type: join_coverage
  left_source: orders
  right_source: customers
  join_key: customer_id
  min_match_rate: 0.9       # At least 90% of left records should match
  message: "Most orders should have matching customers"
```

**Configuration:**
- `left_source` - Name of the left (driving) source
- `right_source` - Name of the right (lookup) source
- `join_key` - Field name to join on
- `min_match_rate` - Minimum percentage of left records that must match (0.0 to 1.0)

## Performance Assertions

### execution_time

Validates query execution time.

```yaml
- type: execution_time
  max_ms: 5000              # Maximum execution time in milliseconds
  min_ms: 100               # Optional: minimum time (detect if skipping work)
  message: "Query should complete within 5 seconds"
```

### memory_usage

Validates memory consumption during execution.

```yaml
- type: memory_usage
  max_mb: 100               # Maximum peak memory in megabytes
  max_bytes: 104857600      # Alternative: max in bytes
  max_growth_bytes: 52428800 # Maximum memory growth during execution
  message: "Memory usage should stay reasonable"
```

### throughput

Validates processing throughput (records per second).

```yaml
- type: throughput
  min_records_per_second: 100       # Minimum required throughput
  max_records_per_second: 10000     # Maximum allowed throughput (for rate limiting)
  expected_records_per_second: 500  # Expected rate with tolerance
  tolerance_percent: 20             # Tolerance percentage (default: 20%)
  message: "Processing should maintain good throughput"
```

**Parameters:**
- `min_records_per_second` - Minimum throughput required (records/sec)
- `max_records_per_second` - Maximum throughput allowed (records/sec)
- `expected_records_per_second` - Expected throughput with tolerance
- `tolerance_percent` - Percentage tolerance for expected rate (default: 20%)

**Use cases:**
- Performance regression testing
- Rate limiting validation
- Capacity planning verification

## File Assertions

### file_exists

Validates output file exists with optional size constraints.

```yaml
- type: file_exists
  path: ./output/results.csv
  min_size_bytes: 100       # Optional: minimum file size
  max_size_bytes: 10000000  # Optional: maximum file size
  message: "Output file should be created"
```

### file_row_count

Validates number of data rows in output file.

```yaml
- type: file_row_count
  path: ./output/results.csv
  format: csv               # csv, csv_no_header, json_lines, json
  operator: equals
  expected: 100
  message: "Output should contain expected row count"
```

### file_contains

Validates file contains specific values.

```yaml
- type: file_contains
  path: ./output/results.csv
  format: csv
  field: symbol
  expected_values: [AAPL, GOOGL, MSFT]
  mode: all                 # all (must have all values) or any (at least one)
  message: "Output should contain expected symbols"
```

### file_matches

Compares output file to expected content.

```yaml
- type: file_matches
  actual_path: ./output/results.csv
  expected_path: ./expected/results.csv
  format: csv
  ignore_order: true        # Ignore row order when comparing
  numeric_tolerance: 0.01   # Tolerance for numeric comparisons
  message: "Output should match expected results"
```

## Data Quality Assertions

### no_duplicates

Validates no duplicate records based on key fields.

```yaml
- type: no_duplicates
  key_fields: [order_id]    # Fields that form the unique key
  message: "Should have no duplicate orders"
```

### ordering

Validates records are ordered correctly.

```yaml
- type: ordering
  field: event_time
  direction: ascending      # ascending or descending
  message: "Records should be ordered by event time"
```

### completeness

Validates data completeness compared to input.

```yaml
- type: completeness
  expected_fields: [symbol, price, volume, event_time]
  min_completeness: 0.95    # At least 95% of records have all fields
  message: "Data should be mostly complete"
```

## Advanced Assertions

### dlq_count

Validates Dead Letter Queue error counts.

```yaml
- type: dlq_count
  max_errors: 0             # Maximum allowed errors
  error_types: [deserialization, schema_validation]  # Optional: specific types
  message: "No records should fail processing"
```

### error_rate

Validates error rate stays within bounds.

```yaml
- type: error_rate
  max_rate: 0.01            # Maximum 1% error rate
  message: "Error rate should be below 1%"
```

### table_freshness

Validates CTAS table freshness.

```yaml
- type: table_freshness
  table: user_aggregates
  max_lag_ms: 5000          # Maximum lag in milliseconds
  message: "Table should be fresh within 5 seconds"
```

## Custom Assertions

For complex validation logic, use the Rust API directly:

```rust
use velostream::test_harness::assertions::{AssertionResult, AssertionRunner};

let custom_assertion = |output: &CapturedOutput| -> AssertionResult {
    // Custom validation logic
    let passed = output.records.iter()
        .all(|r| r.get("custom_field").is_some());

    AssertionResult {
        assertion_type: "custom_check".to_string(),
        passed,
        message: "Custom validation".to_string(),
        expected: Some("all records have custom_field".to_string()),
        actual: Some(format!("{} records checked", output.records.len())),
    }
};
```

## Assertion Composition

Multiple assertions can be combined to create comprehensive test cases:

```yaml
assertions:
  # Basic output validation
  - type: record_count
    operator: greater_than
    expected: 0

  # Schema validation
  - type: schema_contains
    fields: [id, name, value]

  # Data quality
  - type: no_nulls
    fields: [id, name]

  - type: unique_values
    field: id

  # Business logic validation
  - type: field_values
    field: value
    operator: greater_than
    value: 0

  - type: aggregate_check
    expression: "SUM(value)"
    operator: greater_than
    expected: 1000

  # Performance validation
  - type: execution_time
    max_ms: 5000

  - type: memory_usage
    max_mb: 100
```

## Best Practices

1. **Start with basic assertions** - Validate record count and schema first
2. **Check for nulls** - Always validate required fields are not null
3. **Add business logic checks** - Validate field values and aggregates
4. **Include performance checks** - Set reasonable time and memory limits
5. **Use descriptive messages** - Make assertion failures easy to understand
6. **Test edge cases** - Include tests for empty inputs, single records, etc.
