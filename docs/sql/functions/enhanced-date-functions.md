# Enhanced Date Functions in Velostream

Velostream now supports comprehensive date and time manipulation with enhanced `EXTRACT` and `DATEDIFF` functions.

## EXTRACT Function Enhancements

The `EXTRACT` function now supports additional time units for extracting specific components from timestamps.

### Syntax
```sql
EXTRACT(unit FROM timestamp)
```

### Supported Units

#### Basic Units
- `YEAR` - Extract year (e.g., 2023)
- `MONTH` - Extract month (1-12)
- `DAY` - Extract day of month (1-31)
- `HOUR` - Extract hour (0-23)
- `MINUTE` - Extract minute (0-59)
- `SECOND` - Extract second (0-59)

#### Extended Units (New)
- `EPOCH` - Unix timestamp in seconds
- `WEEK` - ISO week number (1-53)
- `QUARTER` - Quarter of the year (1-4)
- `DOW` / `DAYOFWEEK` - Day of week (0=Sunday, 6=Saturday)
- `DOY` / `DAYOFYEAR` - Day of year (1-366)
- `MILLISECOND` / `MILLISECONDS` - Millisecond component (0-999)
- `MICROSECOND` / `MICROSECONDS` - Microsecond component (0-999999)
- `NANOSECOND` / `NANOSECONDS` - Nanosecond component (0-999999999)

### Examples

```sql
-- Basic timestamp extraction
SELECT 
    EXTRACT('YEAR', _timestamp) as year,
    EXTRACT('MONTH', _timestamp) as month,
    EXTRACT('DAY', _timestamp) as day,
    EXTRACT('HOUR', _timestamp) as hour
FROM sensor_data;

-- Advanced extractions  
SELECT
    EXTRACT('EPOCH', _timestamp) as unix_seconds,
    EXTRACT('WEEK', _timestamp) as iso_week,
    EXTRACT('QUARTER', _timestamp) as quarter,
    EXTRACT('DOW', _timestamp) as day_of_week,
    EXTRACT('DOY', _timestamp) as day_of_year
FROM events_stream;

-- Precision components
SELECT
    EXTRACT('MILLISECOND', _timestamp) as ms,
    EXTRACT('MICROSECOND', _timestamp) as us,
    EXTRACT('NANOSECOND', _timestamp) as ns
FROM high_precision_logs;
```

## DATEDIFF Function Enhancements

The `DATEDIFF` function now supports additional time units for calculating differences between timestamps.

### Syntax
```sql
DATEDIFF(unit, start_timestamp, end_timestamp)
```

### Supported Units

#### Basic Units
- `MILLISECONDS` / `MS` - Difference in milliseconds
- `SECONDS` / `SECOND` - Difference in seconds  
- `MINUTES` / `MINUTE` - Difference in minutes
- `HOURS` / `HOUR` - Difference in hours
- `DAYS` / `DAY` - Difference in days

#### Extended Units (New)
- `WEEKS` / `WEEK` - Difference in weeks
- `MONTHS` / `MONTH` - Difference in months (calendar-aware)
- `QUARTERS` / `QUARTER` - Difference in quarters
- `YEARS` / `YEAR` - Difference in years (calendar-aware)

### Examples

```sql
-- Basic time differences
SELECT 
    order_id,
    DATEDIFF('SECONDS', created_at, NOW()) as seconds_ago,
    DATEDIFF('MINUTES', created_at, NOW()) as minutes_ago,
    DATEDIFF('HOURS', created_at, NOW()) as hours_ago
FROM orders_stream;

-- Advanced time differences
SELECT
    user_id,
    DATEDIFF('WEEKS', registration_date, NOW()) as weeks_since_signup,
    DATEDIFF('MONTHS', last_login, NOW()) as months_inactive,
    DATEDIFF('QUARTERS', first_purchase, NOW()) as quarters_as_customer,
    DATEDIFF('YEARS', birth_date, NOW()) as age_years
FROM user_activity;

-- Processing delay analysis
SELECT
    message_id,
    DATEDIFF('MILLISECONDS', _timestamp, NOW()) as processing_delay_ms,
    DATEDIFF('SECONDS', created_time, processed_time) as total_processing_time
FROM message_processing_stream
WHERE DATEDIFF('MINUTES', _timestamp, NOW()) < 5;
```

## Calendar-Aware Calculations

The enhanced `DATEDIFF` function provides calendar-aware calculations for months, quarters, and years:

### Month Differences
- Takes into account varying month lengths
- Considers day-of-month for accurate calculations
- Example: Jan 31 to Feb 28 = 0 months, Jan 31 to Mar 1 = 1 month

### Quarter Differences  
- Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec
- Calculates full quarter transitions

### Year Differences
- Anniversary-based calculation
- Accounts for leap years and different month/day combinations

## Practical Use Cases

### 1. Real-time Analytics
```sql
-- Current hour analysis
SELECT 
    EXTRACT('HOUR', NOW()) as current_hour,
    COUNT(*) as events_this_hour
FROM events_stream
WHERE EXTRACT('HOUR', _timestamp) = EXTRACT('HOUR', NOW())
GROUP BY EXTRACT('HOUR', NOW());
```

### 2. Time-based Filtering
```sql
-- Last quarter's data
SELECT *
FROM sales_stream 
WHERE DATEDIFF('QUARTERS', _timestamp, NOW()) = 0;

-- This week's events
SELECT *
FROM activity_log
WHERE EXTRACT('WEEK', _timestamp) = EXTRACT('WEEK', NOW())
  AND EXTRACT('YEAR', _timestamp) = EXTRACT('YEAR', NOW());
```

### 3. Performance Monitoring
```sql
-- Processing time analysis
SELECT
    service_name,
    AVG(DATEDIFF('MILLISECONDS', request_start, request_end)) as avg_response_ms,
    MAX(DATEDIFF('SECONDS', request_start, request_end)) as max_response_sec,
    COUNT(*) as request_count
FROM api_requests_stream
WHERE DATEDIFF('HOURS', _timestamp, NOW()) < 1
GROUP BY service_name;
```

### 4. User Engagement Analysis
```sql
-- User activity patterns
SELECT
    user_id,
    EXTRACT('DOW', login_time) as day_of_week,
    EXTRACT('QUARTER', registration_date) as signup_quarter,
    DATEDIFF('MONTHS', registration_date, last_activity) as months_active
FROM user_sessions
WHERE DATEDIFF('DAYS', last_activity, NOW()) < 30;
```

## Precision and Performance Notes

- **Millisecond Precision**: All timestamp functions work with millisecond precision
- **Calendar Calculations**: Month/quarter/year calculations are calendar-aware
- **Performance**: EXTRACT operations are optimized for streaming data
- **Timezone**: All calculations are performed in UTC
- **Edge Cases**: Handles leap years, month boundaries, and DST transitions correctly

## Error Handling

The enhanced functions provide clear error messages for:
- Invalid time units
- Malformed timestamps  
- Out-of-range values
- Type mismatches

```sql
-- This will produce a clear error message
SELECT EXTRACT('INVALID_UNIT', _timestamp) FROM stream; 
-- Error: "Unsupported EXTRACT part: INVALID_UNIT. Supported parts: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, DOW, DOY, EPOCH, WEEK, QUARTER, MILLISECOND, MICROSECOND, NANOSECOND"
```

## Compatibility

These enhancements are:
- ✅ Backward compatible with existing queries
- ✅ Compatible with Avro serialization
- ✅ Tested with comprehensive test suite
- ✅ Support both streaming and batch processing scenarios