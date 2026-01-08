# Tier 2: Aggregations & Windowing

Aggregate functions and time-based windowing for streaming analytics.

## Examples

| File | Description |
|------|-------------|
| `10_count.sql` | COUNT(*) and COUNT(column) |
| `11_sum_avg.sql` | SUM, AVG, MIN, MAX functions |
| `12_tumbling_window.sql` | Fixed-size time windows |
| `13_sliding_window.sql` | Overlapping time windows |
| `14_session_window.sql` | Activity-based sessions |
| `15_compound_keys.sql` | Multiple PRIMARY KEY fields |

## Key Concepts

### Aggregate Functions

```sql
CREATE TABLE stats AS
SELECT
    category PRIMARY KEY,
    COUNT(*) AS total_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount
FROM input
GROUP BY category
EMIT CHANGES;
```

### Tumbling Windows

Fixed-size, non-overlapping time buckets:

```sql
CREATE TABLE hourly_stats AS
SELECT
    symbol PRIMARY KEY,
    SUM(volume) AS total_volume,
    _window_start AS window_start,
    _window_end AS window_end
FROM trades
GROUP BY symbol
WINDOW TUMBLING(1h)
EMIT CHANGES;
```

### Sliding Windows

Overlapping windows for moving averages:

```sql
-- 5-minute window, advancing every 1 minute
CREATE TABLE moving_avg AS
SELECT
    symbol PRIMARY KEY,
    AVG(price) AS moving_avg_5m
FROM trades
GROUP BY symbol
WINDOW SLIDING(5m, 1m)
EMIT CHANGES;
```

### Session Windows

Group by activity with gap timeout:

```sql
-- New session after 30 seconds of inactivity
CREATE TABLE sessions AS
SELECT
    user_id PRIMARY KEY,
    COUNT(*) AS actions_in_session
FROM user_activity
GROUP BY user_id
WINDOW SESSION(30s)
EMIT CHANGES;
```

### Compound Keys

Multiple fields as Kafka message key:

```sql
CREATE TABLE regional_stats AS
SELECT
    region PRIMARY KEY,
    category PRIMARY KEY,
    SUM(amount) AS total
FROM orders
GROUP BY region, category
EMIT CHANGES;
-- Kafka key: "US|Electronics" (pipe-delimited)
```

## CREATE TABLE vs CREATE STREAM

- **CREATE TABLE**: Use for GROUP BY aggregations (maintains state)
- **CREATE STREAM**: Use for transformations without aggregation

## Running Examples

```bash
velo-test run 12_tumbling_window.sql
```

## Next Steps

- **Tier 3**: Joins for data enrichment
- **Tier 4**: Window functions (LAG, LEAD, ROW_NUMBER)
