# Tier 3: Joins

Combine streams with tables and other streams for data enrichment.

## Examples

| File | Description |
|------|-------------|
| `20_stream_table_join.sql` | Enrich stream with reference table |
| `21_stream_stream_join.sql` | Join two streams with time window |
| `22_multi_join.sql` | Multiple table joins |
| `23_right_join.sql` | RIGHT JOIN for table-side records |
| `24_full_outer_join.sql` | FULL OUTER JOIN for reconciliation |

## Key Concepts

### Stream-Table Join

Enrich streaming data with static lookup tables:

```sql
CREATE STREAM enriched_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.quantity * o.unit_price AS order_total,
    p.product_name,
    p.category
FROM orders o
LEFT JOIN products p ON o.product_id = p.product_id
EMIT CHANGES;
```

**Use cases:**
- Customer enrichment
- Product catalog lookup
- Geographic data enrichment

### Stream-Stream Join

Join two streams with temporal constraints:

```sql
CREATE STREAM matched_events AS
SELECT
    o.order_id,
    o.customer_id,
    s.shipment_id,
    s.tracking_number
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
    AND s.event_time BETWEEN o.event_time
        AND o.event_time + INTERVAL '24' HOUR
EMIT CHANGES;
```

**Use cases:**
- Order-shipment matching
- Click-conversion correlation
- Fraud detection (multiple events)

### Multi-Table Join

Join with multiple reference tables:

```sql
CREATE STREAM fully_enriched AS
SELECT
    o.order_id,
    c.customer_name,
    c.tier AS customer_tier,
    p.product_name,
    p.category AS product_category
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON o.product_id = p.product_id
EMIT CHANGES;
```

### Join Types

| Type | Description |
|------|-------------|
| `JOIN` / `INNER JOIN` | Only matching records |
| `LEFT JOIN` | All from left, matching from right |
| `RIGHT JOIN` | All from right, matching from left |
| `FULL OUTER JOIN` | All records from both sides |

## Configuration

```sql
WITH (
    -- Join timeout for stream-stream joins
    'join.timeout' = '30s',

    -- Table caching for performance
    'cache.enabled' = 'true',
    'cache.ttl_seconds' = '3600'
);
```

## Running Examples

```bash
velo-test run 20_stream_table_join.sql
```

## Next Steps

- **Tier 4**: Window functions for per-row analytics
- **Tier 5**: Complex queries with CASE and subqueries
