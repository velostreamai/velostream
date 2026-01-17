# Tier 1: Basic Queries

Foundational streaming SQL patterns - the building blocks for all streaming applications.

## Examples

| File | Description |
|------|-------------|
| `01_passthrough.sql` | SELECT * - pass all records unchanged |
| `02_projection.sql` | SELECT specific columns with aliases |
| `03_filter.sql` | WHERE clause filtering |
| `04_casting.sql` | CAST type conversions |
| `05_distinct.sql` | Remove duplicate values |
| `06_order_by.sql` | Sort output records |
| `07_limit.sql` | Restrict row count |

## Key Concepts

### Passthrough (SELECT *)
```sql
CREATE STREAM output AS
SELECT * FROM input EMIT CHANGES;
```
All records flow through unchanged. Useful for:
- Data routing
- Format conversion
- Testing pipelines

### Projection (SELECT columns)
```sql
CREATE STREAM output AS
SELECT
    id AS record_id,
    value * 1.1 AS adjusted_value
FROM input EMIT CHANGES;
```
Select and transform specific fields:
- Column aliasing with AS
- Calculated fields
- Removing unnecessary columns

### Filtering (WHERE)
```sql
CREATE STREAM output AS
SELECT * FROM input
WHERE amount > 100 AND active = true
EMIT CHANGES;
```
Filter records by conditions:
- Comparison operators (=, >, <, >=, <=, !=)
- Logical operators (AND, OR, NOT)
- NULL checks (IS NULL, IS NOT NULL)

### Type Casting (CAST)
```sql
SELECT
    CAST(amount AS INTEGER) AS amount_int,
    CAST(id AS STRING) AS id_string
FROM input;
```
Convert between types:
- INTEGER, DECIMAL, FLOAT
- STRING, BOOLEAN
- TIMESTAMP

## Running Examples

```bash
# Run individual example
velo-test run 01_passthrough.sql

# Run with debug mode
velo-test debug 01_passthrough.sql
```

## Next Steps

After mastering basic queries, proceed to:
- **Tier 2**: Aggregations and windowing
- **Tier 3**: Joins (stream-table, stream-stream)
