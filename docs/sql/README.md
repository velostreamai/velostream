# VeloStream SQL - Your First Query in 2 Minutes

Get productive with VeloStream SQL in under 2 minutes. This guide provides working examples you can copy and run immediately.

## ⚡ Quick Start (30 seconds)

**Basic SELECT query:**
```sql
SELECT customer_id, amount, order_date
FROM orders
WHERE amount > 100.0
LIMIT 10;
```

**Real-time filtering:**
```sql
SELECT *
FROM user_events
WHERE event_type = 'purchase' AND amount > 50.0;
```

**Simple aggregation:**
```sql
SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_spent
FROM orders
GROUP BY customer_id;
```

## 🚀 Complete Setup (1 minute)

### Step 1: Create your first streaming job
```sql
CREATE STREAM daily_sales AS
SELECT
    customer_id,
    SUM(amount) as daily_total,
    COUNT(*) as order_count
FROM kafka_orders_source
WHERE order_date = CURRENT_DATE
GROUP BY customer_id
INTO kafka_results_sink;
```

### Step 2: Configure Kafka data source
```sql
-- Add this WITH clause to your CREATE STREAM
WITH (
    'kafka_orders_source.type' = 'kafka_source',
    'kafka_orders_source.brokers' = 'localhost:9092',
    'kafka_orders_source.topic' = 'orders',
    'kafka_results_sink.type' = 'kafka_sink',
    'kafka_results_sink.brokers' = 'localhost:9092',
    'kafka_results_sink.topic' = 'daily_sales'
);
```

### Step 3: Run your job
```bash
velo-sql-multi --query-file daily_sales.sql
```

**You're now processing streaming data in real-time!**

## 📋 Most Common Tasks (30 seconds each)

### Filter Data
```sql
-- Multiple conditions with AND/OR
SELECT * FROM events
WHERE (status = 'active' OR priority = 'high')
  AND created_date > '2024-01-01';
```
[→ Complete filtering guide](by-task/filter-data.md)

### Aggregate Data
```sql
-- Group and calculate totals
SELECT category, COUNT(*), SUM(amount), AVG(amount)
FROM transactions
GROUP BY category
HAVING SUM(amount) > 1000;
```
[→ Complete aggregation guide](by-task/aggregate-data.md)

### Join Data Streams
```sql
-- Combine related data
SELECT o.order_id, o.amount, c.customer_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
WHERE o.status = 'completed';
```
[→ Complete joins guide](by-task/join-streams.md)

### Time Windows
```sql
-- 1-hour tumbling windows
SELECT customer_id, SUM(amount) as hourly_total
FROM orders
WINDOW TUMBLING (INTERVAL '1' HOUR)
GROUP BY customer_id;
```
[→ Complete windowing guide](by-task/window-analysis.md)

## 🎯 Task-Oriented Guides

**I want to...**
- [Filter streaming data](by-task/filter-data.md) - WHERE clauses, complex conditions
- [Calculate totals and averages](by-task/aggregate-data.md) - GROUP BY, aggregation functions
- [Combine data from multiple streams](by-task/join-streams.md) - Stream joins
- [Analyze data in time windows](by-task/window-analysis.md) - TUMBLING, SLIDING, SESSION
- [Detect patterns and alerts](by-task/detect-patterns.md) - Pattern detection
- [Transform and clean data](by-task/transform-data.md) - Data transformation

## 📚 Reference Guides

### Quick Lookups
- [Essential Functions](functions/essential.md) - Top 10 most-used functions
- [Function Reference](functions/) - Complete function library
- [Real-World Examples](examples/) - Copy-paste patterns for common use cases

### Complete Documentation
- [Complete SQL Syntax](reference/complete-syntax.md) - Comprehensive syntax reference
- [GROUP BY Reference](reference/group-by.md) - GROUP BY and aggregation operations
- [EMIT Modes Reference](reference/emit-modes.md) - Window emission control
- [SQL Validator](tools/validator.md) - Query validation and testing
- [Native Deployment](deployment/native-deployment.md) - Production deployment guide
- [Data Sources Integration](integration/data-sources.md) - Kafka and schema setup

## 🔗 Quick Navigation

| Need | Go To |
|------|-------|
| **First time using SQL** | [Hello World](quickstart/hello-world.md) |
| **Basic filtering** | [Filter Data](by-task/filter-data.md) |
| **Calculations** | [Aggregate Data](by-task/aggregate-data.md) |
| **Function help** | [Functions](functions/essential.md) |
| **Working examples** | [Examples](examples/) |
| **Complete reference** | [Complete SQL Syntax](reference/complete-syntax.md) |

---

**🎯 Goal: Get productive in under 2 minutes!** If any task takes longer, [let us know](https://github.com/anthropics/claude-code/issues) so we can improve this guide.