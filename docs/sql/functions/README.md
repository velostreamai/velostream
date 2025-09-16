# VeloStream SQL Functions Reference

Quick lookup for all available SQL functions organized by category. Click any function for detailed examples.

## 🔥 Most Used Functions

| Function | Purpose | Example |
|----------|---------|---------|
| `COUNT(*)` | Count all records | `COUNT(*) as total_orders` |
| `SUM(column)` | Add numbers | `SUM(amount) as total_revenue` |
| `AVG(column)` | Calculate average | `AVG(price) as avg_price` |
| `UPPER(text)` | Convert to uppercase | `UPPER(name)` |
| `CONCAT(a, b)` | Join text | `CONCAT(first, ' ', last)` |
| `NOW()` | Current timestamp | `NOW() as processed_at` |
| `COALESCE(a, b)` | First non-null value | `COALESCE(phone, 'N/A')` |
| `CASE WHEN` | Conditional logic | `CASE WHEN amount > 100 THEN 'High' ELSE 'Low' END` |
| `ROUND(num, places)` | Round numbers | `ROUND(amount, 2)` |
| `SUBSTRING(text, start, len)` | Extract text portion | `SUBSTRING(email, 1, 10)` |

[→ See detailed examples](essential.md)

## 📊 Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COUNT(*)` | Count all rows | `COUNT(*) as total` |
| `COUNT(column)` | Count non-null values | `COUNT(email) as customers_with_email` |
| `COUNT_DISTINCT(column)` | Count unique values | `COUNT_DISTINCT(customer_id)` |
| `SUM(column)` | Sum numeric values | `SUM(amount) as total_revenue` |
| `AVG(column)` | Average of values | `AVG(rating) as avg_rating` |
| `MIN(column)` | Minimum value | `MIN(price) as lowest_price` |
| `MAX(column)` | Maximum value | `MAX(date) as latest_date` |
| `STDDEV(column)` | Standard deviation | `STDDEV(amount) as price_volatility` |
| `VARIANCE(column)` | Variance | `VARIANCE(score) as score_variance` |
| `FIRST(column)` | First value in group | `FIRST(status) as initial_status` |
| `LAST(column)` | Last value in group | `LAST(status) as final_status` |
| `STRING_AGG(column, sep)` | Concatenate strings | `STRING_AGG(name, ', ') as all_names` |

[→ Complete aggregation reference](aggregation.md)

## 🔤 String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `CONCAT(a, b, c)` | Join multiple strings | `CONCAT('Customer: ', first_name, ' ', last_name)` |
| `LENGTH(text)` | String length | `LENGTH(description)` |
| `LEN(text)` | String length (alias) | `LEN(product_code)` |
| `UPPER(text)` | Convert to uppercase | `UPPER(product_name)` |
| `LOWER(text)` | Convert to lowercase | `LOWER(email)` |
| `TRIM(text)` | Remove whitespace | `TRIM(description)` |
| `LTRIM(text)` | Remove left whitespace | `LTRIM(text)` |
| `RTRIM(text)` | Remove right whitespace | `RTRIM(text)` |
| `REPLACE(text, old, new)` | Replace text | `REPLACE(phone, '-', '')` |
| `SUBSTRING(text, start, len)` | Extract substring | `SUBSTRING(email, 1, 10)` |
| `LEFT(text, len)` | Left portion | `LEFT(product_code, 3)` |
| `RIGHT(text, len)` | Right portion | `RIGHT(order_id, 4)` |
| `POSITION(substring, text)` | Find position | `POSITION('@', email)` |

[→ Complete string reference](string.md)

## 📅 Date/Time Functions

| Function | Description | Example |
|----------|-------------|---------|
| `NOW()` | Current timestamp | `NOW() as processed_at` |
| `CURRENT_TIMESTAMP` | Current timestamp (alias) | `CURRENT_TIMESTAMP` |
| `DATE_FORMAT(date, format)` | Format date | `DATE_FORMAT(_timestamp, '%Y-%m-%d')` |
| `EXTRACT(part, date)` | Extract date part | `EXTRACT('YEAR', order_date)` |
| `DATEDIFF(unit, start, end)` | Date difference | `DATEDIFF('days', start_date, end_date)` |
| `YEAR(date)` | Extract year | `YEAR(order_date)` |
| `MONTH(date)` | Extract month | `MONTH(order_date)` |
| `DAY(date)` | Extract day | `DAY(order_date)` |
| `HOUR(timestamp)` | Extract hour | `HOUR(event_timestamp)` |

[→ Complete date/time reference](date-time.md)

## 🔢 Math Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(number)` | Absolute value | `ABS(balance_change)` |
| `ROUND(number, places)` | Round to decimal places | `ROUND(amount, 2)` |
| `CEIL(number)` | Round up | `CEIL(price)` |
| `CEILING(number)` | Round up (alias) | `CEILING(price)` |
| `FLOOR(number)` | Round down | `FLOOR(score)` |
| `MOD(a, b)` | Modulo operation | `MOD(order_id, 10)` |
| `POWER(base, exp)` | Exponentiation | `POWER(quantity, 2)` |
| `POW(base, exp)` | Exponentiation (alias) | `POW(discount, 2)` |
| `SQRT(number)` | Square root | `SQRT(area)` |

[→ Complete math reference](math.md)

## 🪟 Window Functions

| Function | Description | Example |
|----------|-------------|---------|
| `ROW_NUMBER()` | Sequential numbering | `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date)` |
| `RANK()` | Ranking with gaps | `RANK() OVER (ORDER BY amount DESC)` |
| `DENSE_RANK()` | Ranking without gaps | `DENSE_RANK() OVER (ORDER BY score DESC)` |
| `PERCENT_RANK()` | Percentile rank | `PERCENT_RANK() OVER (ORDER BY amount)` |
| `LAG(column, offset)` | Previous row value | `LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY date)` |
| `LEAD(column, offset)` | Next row value | `LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY date)` |
| `FIRST_VALUE(column)` | First value in partition | `FIRST_VALUE(amount) OVER (PARTITION BY customer_id ORDER BY date)` |
| `LAST_VALUE(column)` | Last value in partition | `LAST_VALUE(amount) OVER (PARTITION BY customer_id ORDER BY date)` |
| `NTH_VALUE(column, n)` | Nth value in partition | `NTH_VALUE(amount, 2) OVER (PARTITION BY customer_id ORDER BY date)` |
| `CUME_DIST()` | Cumulative distribution | `CUME_DIST() OVER (ORDER BY amount)` |
| `NTILE(n)` | Divide into buckets | `NTILE(4) OVER (ORDER BY amount)` |

[→ Complete window functions reference](window.md)

## 🔧 Utility Functions

| Function | Description | Example |
|----------|-------------|---------|
| `COALESCE(a, b, c)` | First non-null value | `COALESCE(preferred_name, first_name, 'Unknown')` |
| `NULLIF(a, b)` | NULL if values equal | `NULLIF(discount_rate, 0.0)` |
| `CASE WHEN ... THEN ... ELSE ... END` | Conditional logic | `CASE WHEN amount > 100 THEN 'High' ELSE 'Low' END` |
| `TIMESTAMP()` | Processing timestamp | `TIMESTAMP() as processed_at` |

[→ Complete utility functions](advanced.md)

## 📋 JSON Functions

| Function | Description | Example |
|----------|-------------|---------|
| `JSON_VALUE(json, path)` | Extract JSON value | `JSON_VALUE(payload, '$.user_id')` |
| `JSON_QUERY(json, path)` | Extract JSON object | `JSON_QUERY(data, '$.customer')` |
| `JSON_EXISTS(json, path)` | Check if path exists | `JSON_EXISTS(payload, '$.order_id')` |

[→ Complete JSON reference](json.md)

## Quick Function Search

**Need to...**
- **Count records?** → `COUNT(*)`
- **Calculate total?** → `SUM(column)`
- **Find average?** → `AVG(column)`
- **Convert text case?** → `UPPER(text)` or `LOWER(text)`
- **Join text?** → `CONCAT(a, b)` or `a || b`
- **Handle nulls?** → `COALESCE(a, b, c)`
- **Format numbers?** → `ROUND(num, places)`
- **Extract date parts?** → `EXTRACT('YEAR', date)`
- **Get current time?** → `NOW()`
- **Conditional logic?** → `CASE WHEN ... THEN ... END`

## Function Categories Navigation

| Category | File | Purpose |
|----------|------|---------|
| **Essential** | [essential.md](essential.md) | Most commonly used functions |
| **Aggregation** | [aggregation.md](aggregation.md) | SUM, COUNT, AVG, etc. |
| **String** | [string.md](string.md) | Text manipulation |
| **Date/Time** | [date-time.md](date-time.md) | Date and time operations |
| **Math** | [math.md](math.md) | Mathematical calculations |
| **Window** | [window.md](window.md) | ROW_NUMBER, RANK, LAG, LEAD |
| **JSON** | [json.md](json.md) | JSON processing |
| **Advanced** | [advanced.md](advanced.md) | Utility and specialized functions |

---

**💡 Pro Tip**: Start with [essential.md](essential.md) for the 10 most commonly used functions, then explore category-specific references as needed.