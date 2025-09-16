# EMIT Modes SQL Reference

Complete guide to EMIT clauses in VeloStream SQL for controlling when GROUP BY aggregation results are emitted.

## Overview

VeloStream implements KSQL-style `EMIT` clauses that control the timing and behavior of result emission in streaming GROUP BY queries. The EMIT mode determines when aggregated results are sent to output topics.

## Syntax

```sql
SELECT aggregate_expressions...
FROM source_table
GROUP BY grouping_columns
[WINDOW window_specification]
[EMIT {CHANGES | FINAL}]
[WITH (properties...)]
```

## EMIT Modes

### `EMIT CHANGES` - Continuous Aggregation

**Purpose**: Real-time streaming aggregation with immediate result updates

**Behavior**:
- Emits updated aggregation results **immediately** for every input record
- Works like Change Data Capture (CDC) - shows incremental updates  
- Each input record can trigger result emission for affected groups
- Works with or without WINDOW clauses

**Use Cases**:
- Real-time dashboards
- Live monitoring and alerting
- Streaming analytics requiring immediate updates
- Change data capture scenarios

**Example**:
```sql
-- Real-time order monitoring
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent
FROM orders_stream
GROUP BY customer_id
EMIT CHANGES;
```

**Output Behavior**:
```
Input: {"customer_id": "A", "amount": 100}
Output: {"customer_id": "A", "order_count": 1, "total_spent": 100}

Input: {"customer_id": "A", "amount": 50} 
Output: {"customer_id": "A", "order_count": 2, "total_spent": 150}

Input: {"customer_id": "B", "amount": 200}
Output: {"customer_id": "B", "order_count": 1, "total_spent": 200}
```

### `EMIT FINAL` - Windowed Aggregation

**Purpose**: Batch-style aggregation that emits complete results only when windows close

**Behavior**:
- Accumulates data within time/count windows
- Emits results **only when window boundaries are reached**
- **Requires WINDOW clause** - validated automatically
- More efficient for high-throughput batch processing

**Use Cases**:
- Hourly/daily/weekly reports
- Time-series analysis  
- Batch aggregation jobs
- Statistical analysis with complete datasets

**Example**:
```sql
-- Hourly sales summary  
SELECT 
    category,
    COUNT(*) as sales_count,
    SUM(amount) as hourly_revenue,
    AVG(amount) as avg_sale_amount
FROM sales_stream
GROUP BY category
WINDOW TUMBLING(1h)
EMIT FINAL;
```

**Output Behavior**:
```
-- Data accumulates for 1 hour, then emits complete results:
Window 1 (09:00-10:00):
{"category": "electronics", "sales_count": 245, "hourly_revenue": 12450.00, "avg_sale_amount": 50.82}
{"category": "books", "sales_count": 89, "hourly_revenue": 1780.50, "avg_sale_amount": 20.01}

Window 2 (10:00-11:00):  
{"category": "electronics", "sales_count": 198, "hourly_revenue": 9890.00, "avg_sale_amount": 49.95}
{"category": "books", "sales_count": 156, "hourly_revenue": 3120.00, "avg_sale_amount": 20.00}
```

## Default Behavior (No EMIT Clause)

When no EMIT clause is specified, VeloStream uses intelligent defaults:

### Without WINDOW Clause → Continuous Aggregation
```sql
-- Defaults to EMIT CHANGES behavior
SELECT customer_id, COUNT(*) as orders
FROM orders_stream  
GROUP BY customer_id;
-- Results emitted immediately for each input
```

### With WINDOW Clause → Windowed Aggregation  
```sql
-- Defaults to EMIT FINAL behavior
SELECT customer_id, COUNT(*) as orders
FROM orders_stream
GROUP BY customer_id  
WINDOW TUMBLING(5m);
-- Results emitted when 5-minute windows close
```

## EMIT Mode Validation

The system automatically validates EMIT clause usage:

### ✅ Valid Combinations
```sql
-- ✅ EMIT CHANGES (always valid)
SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id EMIT CHANGES;

-- ✅ EMIT CHANGES with WINDOW (overrides windowed behavior)
SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id 
WINDOW TUMBLING(1h) EMIT CHANGES;

-- ✅ EMIT FINAL with WINDOW (requires WINDOW clause)  
SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id
WINDOW TUMBLING(1h) EMIT FINAL;
```

### ❌ Invalid Combinations
```sql
-- ❌ EMIT FINAL without WINDOW clause
SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id EMIT FINAL;
-- Error: EMIT FINAL can only be used with windowed aggregations (queries with WINDOW clause)
```

## Performance Characteristics

| EMIT Mode | Throughput | Memory Usage | Latency | Best For |
|-----------|------------|--------------|---------|----------|
| **EMIT CHANGES** | 5-12K msgs/sec | Low-Medium | Low (immediate) | Real-time dashboards |
| **EMIT FINAL** | 5-12K msgs/sec | Low (bounded) | Medium-High (window-dependent) | Batch reports |
| **Default (no WINDOW)** | 5-12K msgs/sec | Low-Medium | Low (immediate) | General streaming |
| **Default (with WINDOW)** | 5-12K msgs/sec | Low (bounded) | Medium-High (window-dependent) | Time-series analysis |

## Advanced Examples

### Override Windowed Behavior with EMIT CHANGES
```sql
-- Window for partitioning, but emit changes immediately
SELECT 
    region,
    product_category, 
    COUNT(*) as running_count,
    SUM(amount) as running_total
FROM sales_stream
GROUP BY region, product_category
WINDOW TUMBLING(15m)  -- Provides time-based partitioning
EMIT CHANGES;  -- But emit updates immediately, not just when window closes
```

### Complex Windowed Aggregation
```sql
-- Daily analytics with multiple time windows
SELECT 
    DATE(transaction_time) as date,
    merchant_id,
    COUNT(*) as daily_transactions,
    SUM(amount) as daily_revenue,
    AVG(amount) as avg_transaction_size,
    COUNT_DISTINCT(customer_id) as unique_customers
FROM payment_stream
GROUP BY DATE(transaction_time), merchant_id  
WINDOW TUMBLING(1d)
EMIT FINAL;
```

### Real-time Alerting with EMIT CHANGES
```sql
-- High-frequency trading alerts
SELECT 
    symbol,
    COUNT(*) as trade_count,
    AVG(price) as avg_price,
    MAX(price) as high_price,
    MIN(price) as low_price
FROM trades_stream
WHERE volume > 1000  -- Only high-volume trades
GROUP BY symbol
EMIT CHANGES;  -- Immediate alerts for risk management
```

## Integration with Job Management

### Multi-Job SQL Server
```sql
START JOB realtime_metrics AS
SELECT 
    category,
    COUNT(*) as item_count,
    SUM(price) as total_price,
    AVG(price) as avg_price
FROM products_topic
GROUP BY category
EMIT CHANGES  -- Real-time updates
WITH ('output.topic' = 'metrics_output');
```

### Batch Processing Jobs
```sql  
START JOB hourly_reports AS
SELECT 
    region,
    product_line,
    COUNT(*) as sales_count,
    SUM(revenue) as total_revenue
FROM sales_topic  
GROUP BY region, product_line
WINDOW TUMBLING(1h)
EMIT FINAL  -- Complete hourly results only
WITH ('output.topic' = 'reports_output');
```

## Best Practices

1. **Choose EMIT mode based on use case**:
   - Use `EMIT CHANGES` for real-time monitoring and dashboards
   - Use `EMIT FINAL` for batch reports and time-series analysis

2. **Leverage intelligent defaults**:
   - Omit EMIT clause when default behavior matches requirements
   - System chooses optimal mode based on query structure

3. **Validate window requirements**:
   - `EMIT FINAL` requires `WINDOW` clause  
   - System validates automatically and provides clear error messages

4. **Consider performance implications**:
   - Both modes have similar throughput (~5-12K msgs/sec)
   - `EMIT CHANGES` has lower latency but potentially higher output volume
   - `EMIT FINAL` has bounded memory usage within windows

5. **Test with realistic data volumes**:
   - Validate memory usage with expected number of groups
   - Monitor output topic load for high-frequency EMIT CHANGES

## Error Messages

Common validation errors and solutions:

```
ParseError: "EMIT FINAL can only be used with windowed aggregations (queries with WINDOW clause)"
→ Solution: Add WINDOW clause or use EMIT CHANGES instead

ParseError: "Expected CHANGES or FINAL after EMIT"  
→ Solution: Specify either EMIT CHANGES or EMIT FINAL

ExecutionError: "EMIT FINAL requires WINDOW clause for proper aggregation boundaries"
→ Solution: Add appropriate WINDOW specification (TUMBLING/SLIDING)
```

---

*This comprehensive guide covers all EMIT mode functionality in VeloStream SQL. For additional examples, see SQL_REFERENCE_GROUP_BY.md and performance benchmarks in PERFORMANCE_BENCHMARK_RESULTS.MD.*