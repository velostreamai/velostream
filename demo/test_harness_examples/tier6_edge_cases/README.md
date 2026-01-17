# Tier 6: Edge Cases

Handle real-world data challenges: nulls, empty datasets, high volume, and late arrivals.

## Examples

| File | Description |
|------|-------------|
| `50_nulls.sql` | NULL handling with COALESCE |
| `51_empty.sql` | Empty dataset graceful handling |
| `52_large_volume.sql` | High-volume processing (100k+) |
| `53_late_arrivals.sql` | Out-of-order event handling |

## Key Concepts

### Null Handling

Use COALESCE for defaults:

```sql
SELECT
    sensor_id,
    COALESCE(temperature, 0.0) AS temperature,
    COALESCE(humidity, 50.0) AS humidity,
    COALESCE(status, 'unknown') AS status,
    -- Null indicator flags
    CASE WHEN temperature IS NULL THEN 1 ELSE 0 END AS temp_missing
FROM sensor_readings;
```

**Best practices:**
- Always handle potential nulls
- Provide sensible defaults
- Track null occurrences for data quality

### Empty Dataset Handling

Design queries that handle zero records gracefully:

```sql
-- Aggregations return nothing for empty groups (not zeros)
CREATE TABLE category_totals AS
SELECT
    category PRIMARY KEY,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY category
WINDOW TUMBLING(1m)
EMIT CHANGES;
```

### Large Volume Processing

Test at scale with stress mode:

```bash
# Generate 10,000 records
velo-test stress 52_large_volume.sql --records 10000

# Generate 100,000 records
velo-test stress 52_large_volume.sql --records 100000
```

### Late Arrival Handling

Configure watermarks for out-of-order data:

```sql
CREATE TABLE sensor_aggregates AS
SELECT ...
FROM sensor_events
GROUP BY sensor_id
WINDOW TUMBLING(1m)
EMIT CHANGES
WITH (
    'event.time.field' = 'event_time',
    'watermark.strategy' = 'bounded_out_of_orderness',
    'watermark.max_out_of_orderness' = '30s',
    'late.data.strategy' = 'dead_letter'
);
```

**Watermark strategies:**
- `monotonic`: Events always arrive in order
- `bounded_out_of_orderness`: Allow configured lateness
- `processing_time`: Use current time (ignore event time)

**Late data strategies:**
- `drop`: Discard late events (default)
- `dead_letter`: Send to DLQ for analysis
- `allow`: Process anyway (may cause retractions)

## Data Quality Patterns

### Track Missing Data

```sql
SELECT
    _window_start,
    COUNT(*) AS total_records,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_count,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS null_pct
FROM input
GROUP BY ...
```

### Lateness Tracking

```sql
SELECT
    sensor_id,
    event_time,
    processing_time,
    CASE
        WHEN processing_time > event_time + INTERVAL '30' SECOND THEN 'late'
        ELSE 'on_time'
    END AS arrival_status
FROM sensor_events;
```

## Running Examples

```bash
velo-test run 50_nulls.sql
velo-test stress 52_large_volume.sql --records 50000
```

## Next Steps

- **Tier 7**: Serialization formats
- **Tier 8**: Fault tolerance and debugging
