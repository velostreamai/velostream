# Tier 4: Window Functions

Per-row analytics using ROWS WINDOW BUFFER for streaming data.

## Examples

| File | Description |
|------|-------------|
| `30_lag_lead.sql` | Previous/next row values |
| `31_row_number.sql` | ROW_NUMBER, RANK, DENSE_RANK |
| `32_running_agg.sql` | Cumulative SUM, AVG, COUNT |
| `33_rows_buffer.sql` | ROWS WINDOW BUFFER patterns |

## Key Concepts

### ROWS WINDOW BUFFER

Unlike time-based windows, ROWS WINDOW BUFFER operates on a fixed number of rows:

```sql
AVG(price) OVER (
    ROWS WINDOW BUFFER 100 ROWS
    PARTITION BY symbol
    ORDER BY event_time
) AS moving_avg_100
```

**Components:**
- `ROWS WINDOW BUFFER N ROWS`: Buffer size (required)
- `PARTITION BY`: Group by key (optional)
- `ORDER BY`: Row ordering (optional)

### LAG and LEAD

Access previous or next row values:

```sql
SELECT
    symbol,
    price,
    LAG(price, 1) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS prev_price,
    price - LAG(price, 1) OVER (...) AS price_change
FROM market_data;
```

**Use cases:**
- Price change detection
- Trend analysis
- Session boundary detection

### Ranking Functions

```sql
SELECT
    symbol,
    price,
    ROW_NUMBER() OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
        ORDER BY event_time
    ) AS row_num,
    RANK() OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
        ORDER BY price DESC
    ) AS price_rank
FROM market_data;
```

| Function | Description |
|----------|-------------|
| `ROW_NUMBER()` | Unique sequential number |
| `RANK()` | Rank with gaps for ties |
| `DENSE_RANK()` | Rank without gaps |

### Running Aggregates

Cumulative calculations over the buffer:

```sql
SELECT
    symbol,
    SUM(volume) OVER (
        ROWS WINDOW BUFFER 1000 ROWS
        PARTITION BY symbol
    ) AS cumulative_volume,
    AVG(price) OVER (
        ROWS WINDOW BUFFER 100 ROWS
        PARTITION BY symbol
    ) AS moving_avg
FROM market_data;
```

## Buffer Size Considerations

- **Small buffers (50-100)**: Quick moving averages
- **Medium buffers (100-500)**: Trend detection
- **Large buffers (1000+)**: Full session analysis

## Running Examples

```bash
velo-test run 30_lag_lead.sql
```

## Next Steps

- **Tier 5**: Complex queries with CASE, pipelines
- **Tier 6**: Edge cases (nulls, empty, late arrivals)
