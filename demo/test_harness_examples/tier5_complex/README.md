# Tier 5: Complex Queries

Advanced patterns including pipelines, subqueries, CASE expressions, and complex filtering.

## Examples

| File | Description |
|------|-------------|
| `40_pipeline.sql` | Multi-stage transformation pipeline |
| `41_subqueries.sql` | IN (SELECT ...) patterns |
| `42_case.sql` | Conditional CASE expressions |
| `43_complex_filter.sql` | Compound WHERE predicates |
| `44_union.sql` | Combine multiple streams |

## Key Concepts

### Multi-Stage Pipelines

Chain multiple transformations:

```sql
-- Stage 1: Clean data
CREATE STREAM cleaned AS
SELECT ... FROM raw WHERE valid = true EMIT CHANGES;

-- Stage 2: Aggregate
CREATE TABLE aggregated AS
SELECT ... FROM cleaned GROUP BY ... EMIT CHANGES;

-- Stage 3: Flag results
CREATE STREAM flagged AS
SELECT ..., CASE WHEN total > 100000 THEN 'HIGH' ELSE 'LOW' END AS tier
FROM aggregated EMIT CHANGES;
```

### CASE Expressions

Conditional logic in SELECT:

```sql
SELECT
    user_id,
    action,
    CASE
        WHEN action = 'purchase' THEN 'converter'
        WHEN action IN ('add_to_cart', 'search') THEN 'engaged'
        WHEN duration_ms > 30000 THEN 'interested'
        ELSE 'browser'
    END AS engagement_level,
    CASE
        WHEN duration_ms < 5000 THEN 'bounce'
        WHEN duration_ms < 30000 THEN 'short'
        ELSE 'long'
    END AS duration_category
FROM user_activity;
```

### Subqueries with File-Based Reference Tables

Filter using subquery results from a reference table loaded from CSV:

**Step 1: Define the reference table** (loaded from `configs/customers_table.yaml`):
```sql
CREATE TABLE vip_customers AS
SELECT customer_id, name, tier, credit_limit
FROM vip_source
WITH ('vip_source.config_file' = '../configs/customers_table.yaml');
```

**Step 2: Use IN (SELECT ...) to filter**:
```sql
CREATE STREAM vip_orders AS
SELECT o.order_id, o.customer_id, o.product_id,
       o.quantity * o.unit_price AS order_total,
       o.status
FROM all_orders o
WHERE o.customer_id IN (
    SELECT customer_id FROM vip_customers WHERE tier IN ('gold', 'platinum')
)
WITH (...);
```

**Step 3: Declare dependency in test spec** (`41_subqueries.test.yaml`):
```yaml
queries:
  - name: vip_orders
    dependencies:
      - vip_customers  # Reference table must be loaded first
    inputs:
      - source: all_orders
        schema: order_event
        records: 200
    assertions:
      - type: record_count
        greater_than: 0
        less_than: 200  # Should filter out non-VIP orders
```

**Result**: 200 input orders → ~94 filtered VIP orders (only gold/platinum tier)

### Complex Filtering

Combine multiple predicates:

```sql
SELECT * FROM orders
WHERE
    quantity BETWEEN 1 AND 100
    AND unit_price > 10.00
    AND status IN ('confirmed', 'processing', 'shipped')
    AND (region = 'US' OR region = 'EU' OR region = 'APAC')
    AND priority IN ('high', 'medium')
    AND customer_id > 0
    AND category IS NOT NULL
EMIT CHANGES;
```

### UNION

Combine multiple streams:

```sql
CREATE STREAM all_transactions AS
SELECT 'credit' AS source, ... FROM credit_transactions
UNION ALL
SELECT 'debit' AS source, ... FROM debit_transactions
UNION ALL
SELECT 'wire' AS source, ... FROM wire_transfers
EMIT CHANGES;
```

## Running Examples

```bash
velo-test run 40_pipeline.sql

# Debug multi-stage pipeline
velo-test debug 40_pipeline.sql
```

## Next Steps

- **Tier 6**: Edge cases (nulls, empty datasets, late arrivals)
- **Tier 7**: Serialization formats (JSON, Avro, Protobuf)
