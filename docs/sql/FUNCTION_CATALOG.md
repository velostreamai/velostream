# Velostream SQL Function Catalog

Auto-generated catalog of all available SQL functions.

## Table of Contents

- [Aggregate Functions](#aggregate-functions)
- [Math Functions](#math-functions)
- [String Functions](#string-functions)
- [Date/Time Functions](#datetime-functions)
- [Conditional Functions](#conditional-functions)
- [JSON Functions](#json-functions)
- [Array/Map Functions](#arraymap-functions)
- [Scalar/Utility Functions](#scalarutility-functions)

---

## Aggregate Functions

| Function | Aliases | Aggregate | Window |
|----------|---------|-----------|--------|
| COUNT | - | ✓ | - |
| SUM | - | ✓ | - |
| AVG | - | ✓ | - |
| MIN | - | ✓ | - |
| MAX | - | ✓ | - |
| APPROX_COUNT_DISTINCT | - | ✓ | - |
| COUNT_DISTINCT | - | ✓ | - |
| FIRST | FIRST_VALUE | ✓ | ✓ |
| LAST | LAST_VALUE | ✓ | ✓ |
| STRING_AGG | GROUP_CONCAT, LISTAGG, COLLECT | ✓ | - |
| MEDIAN | - | ✓ | - |
| STDDEV | STDDEV_SAMP | ✓ | - |
| STDDEV_POP | - | ✓ | - |
| VARIANCE | VAR_SAMP | ✓ | - |
| VAR_POP | - | ✓ | - |
| PERCENTILE_CONT | - | ✓ | - |
| PERCENTILE_DISC | - | ✓ | - |
| CORR | - | ✓ | - |
| COVAR_POP | - | ✓ | - |
| COVAR_SAMP | - | ✓ | - |
| REGR_SLOPE | - | ✓ | - |
| REGR_INTERCEPT | - | ✓ | - |
| REGR_R2 | - | ✓ | - |

## Math Functions

| Function | Aliases | Aggregate | Window |
|----------|---------|-----------|--------|
| ABS | - | - | - |
| ROUND | - | - | - |
| CEIL | CEILING | - | - |
| FLOOR | - | - | - |
| SQRT | - | - | - |
| POWER | POW | - | - |
| MOD | - | - | - |
| LEAST | - | - | - |
| GREATEST | - | - | - |

## String Functions

| Function | Aliases | Aggregate | Window |
|----------|---------|-----------|--------|
| UPPER | - | - | - |
| LOWER | - | - | - |
| SUBSTRING | - | - | - |
| REPLACE | - | - | - |
| TRIM | - | - | - |
| LTRIM | - | - | - |
| RTRIM | - | - | - |
| LENGTH | LEN | - | - |
| CONCAT | - | - | - |
| SPLIT | - | - | - |
| JOIN | - | - | - |
| LEFT | - | - | - |
| RIGHT | - | - | - |
| POSITION | - | - | - |
| REGEXP | - | - | - |

## Date/Time Functions

| Function | Aliases | Aggregate | Window |
|----------|---------|-----------|--------|
| NOW | - | - | - |
| CURRENT_TIMESTAMP | - | - | - |
| TIMESTAMP | - | - | - |
| EXTRACT | - | - | - |
| DATE_FORMAT | - | - | - |
| DATEDIFF | - | - | - |
| TUMBLE_START | - | - | - |
| TUMBLE_END | - | - | - |
| FROM_UNIXTIME | - | - | - |
| UNIX_TIMESTAMP | - | - | - |

## Conditional Functions

| Function | Aliases | Aggregate | Window |
|----------|---------|-----------|--------|
| COALESCE | - | - | - |
| NULLIF | - | - | - |
| CAST | - | - | - |

## JSON Functions

| Function | Aliases | Aggregate | Window |
|----------|---------|-----------|--------|
| JSON_EXTRACT | - | - | - |
| JSON_VALUE | - | - | - |

## Array/Map Functions

| Function | Aliases | Aggregate | Window |
|----------|---------|-----------|--------|
| ARRAY | - | - | - |
| STRUCT | - | - | - |
| MAP | - | - | - |
| ARRAY_LENGTH | - | - | - |
| ARRAY_CONTAINS | - | - | - |
| MAP_KEYS | - | - | - |
| MAP_VALUES | - | - | - |

## Scalar/Utility Functions

| Function | Aliases | Aggregate | Window |
|----------|---------|-----------|--------|
| HEADER | - | - | - |
| HEADER_KEYS | - | - | - |
| HAS_HEADER | - | - | - |
| SET_HEADER | - | - | - |
| REMOVE_HEADER | - | - | - |

---

## Summary

- **Total Functions**: 75
- **Aggregate Functions**: 24
- **Window Functions**: 2 (FIRST_VALUE, LAST_VALUE)
- **Aliases**: 7 (STDDEV_SAMP, CEILING, POW, LEN, VAR_SAMP)

## Usage

All functions are case-insensitive. The following are equivalent:

```sql
SELECT COUNT(*), count(*), Count(*) FROM stream;
SELECT ABS(-5), abs(-5), Abs(-5);
```

### Aggregate Functions

Aggregate functions operate over groups of records:

```sql
SELECT symbol, AVG(price), SUM(quantity)
FROM trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE);
```

### Statistical Functions

Advanced statistical functions for analytics:

```sql
SELECT
    STDDEV(price) as price_stddev,
    VARIANCE(volume) as volume_variance,
    CORR(price, volume) as price_volume_correlation,
    REGR_R2(price, volume) as r_squared
FROM trades
GROUP BY symbol;
```

### String Functions

```sql
SELECT
    UPPER(name),
    SUBSTRING(description, 1, 100),
    CONCAT(first_name, ' ', last_name)
FROM users;
```

### Date/Time Functions

```sql
SELECT
    EXTRACT(HOUR FROM event_time) as hour,
    DATE_FORMAT(created_at, '%Y-%m-%d') as date_str,
    UNIX_TIMESTAMP(event_time) as epoch
FROM events;
```

### Header Functions (Kafka-specific)

```sql
SELECT
    HEADER('correlation-id') as correlation_id,
    HAS_HEADER('trace-id') as has_trace,
    HEADER_KEYS() as all_headers
FROM kafka_stream;
```
