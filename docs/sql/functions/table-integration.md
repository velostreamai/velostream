# Table SQL Integration Functions

This document covers SQL functions specifically enhanced or designed for use with Velostream's Table architecture, including subquery integration and cross-table operations.

## Table-Specific Functions

### SqlQueryable Integration

Velostream Tables implement the `SqlQueryable` trait, enabling direct SQL function execution against Table state:

```rust
pub trait SqlQueryable {
    fn sql_scalar(&self, query: &str) -> Result<FieldValue, SqlError>;
    fn sql_exists(&self, where_clause: &str) -> Result<bool, SqlError>;
    fn sql_filter(&self, where_clause: &str) -> Result<Vec<HashMap<String, FieldValue>>, SqlError>;
    fn sql_in(&self, field: &str, values: &[FieldValue]) -> Result<bool, SqlError>;
}
```

### Subquery Functions

#### EXISTS - Table State Validation

```sql
-- Check if user has premium status from user_profiles table
SELECT
    trade_id,
    amount,
    EXISTS (SELECT 1 FROM user_profiles WHERE user_id = trades.user_id AND account_type = 'premium') as is_premium_user
FROM trades_stream;

-- Validate position exists before processing trade
SELECT *
FROM incoming_trades t
WHERE EXISTS (SELECT 1 FROM user_positions p WHERE p.user_id = t.user_id AND p.symbol = t.symbol);
```

#### Scalar Subqueries - Configuration and Reference Data

```sql
-- Get dynamic configuration values from config table
SELECT
    user_id,
    transaction_amount,
    (SELECT daily_limit FROM user_limits WHERE user_id = transactions.user_id) as daily_limit,
    CASE
        WHEN transaction_amount > (SELECT daily_limit FROM user_limits WHERE user_id = transactions.user_id)
        THEN 'REJECTED'
        ELSE 'APPROVED'
    END as status
FROM transactions_stream;

-- Reference data lookup with scalar subqueries
SELECT
    order_id,
    product_id,
    quantity,
    quantity * (SELECT price FROM product_catalog WHERE product_id = orders.product_id) as total_amount
FROM orders_stream;
```

#### IN Subqueries - Dynamic Set Membership

```sql
-- Filter based on dynamic whitelist from Table
SELECT *
FROM api_requests
WHERE user_id IN (SELECT user_id FROM active_users WHERE status = 'verified');

-- Process only orders for premium products
SELECT
    order_id,
    product_id,
    quantity * 0.9 as discounted_quantity  -- Premium discount
FROM orders_stream
WHERE product_id IN (SELECT product_id FROM product_catalog WHERE tier = 'premium');
```

#### ANY/ALL Subqueries - Threshold Comparisons

```sql
-- Find transactions exceeding any risk threshold
SELECT *
FROM transactions_stream t
WHERE t.amount > ANY (SELECT risk_threshold FROM risk_config WHERE category = t.transaction_type);

-- Ensure transaction meets all compliance requirements
SELECT *
FROM high_value_transactions t
WHERE t.amount >= ALL (SELECT minimum_amount FROM compliance_rules WHERE region = t.region);
```

## Velostream Path Expression Syntax

### Overview

Velostream provides powerful path expression syntax for accessing nested data structures in Tables and streaming records. This syntax supports both direct field access and advanced wildcard patterns for complex data navigation.

### Basic Path Syntax

#### Direct Field Access
```sql
-- Simple field access
SELECT customer.name FROM orders_table;

-- Nested field access with dot notation
SELECT customer.profile.email FROM users_table;
SELECT order.items.product.name FROM orders_table;

-- Array element access by index
SELECT order.items[0].product_name FROM orders_table;
SELECT metrics.values[1] FROM analytics_table;
```

#### Field Access Examples
```sql
-- Financial data access
SELECT
    portfolio.positions.AAPL.shares,
    portfolio.positions.AAPL.price,
    portfolio.positions.MSFT.shares * portfolio.positions.MSFT.price as msft_value
FROM user_portfolios;

-- IoT sensor data access
SELECT
    device.sensors.temperature.value,
    device.sensors.humidity.reading,
    device.location.coordinates.lat,
    device.location.coordinates.lng
FROM sensor_data_table;
```

### Wildcard Path Expressions

#### Single-Level Wildcard (`*`)
```sql
-- Match any field at a specific level
SELECT * FROM table WHERE positions.*.shares > 100;

-- Extract all position symbols
SELECT portfolio_id, positions.*.symbol FROM portfolios_table;

-- Find all high-value positions across symbols
SELECT user_id, positions.*.shares * positions.*.price as position_values
FROM user_portfolios
WHERE positions.*.shares * positions.*.price > 10000;
```

#### Multi-Level Wildcard (`**`) - Future Enhancement
```sql
-- Planned: Deep recursive search (not yet implemented)
SELECT ** FROM table WHERE **.price > 500;  -- Find price fields at any depth
SELECT user_id, **.email FROM users WHERE **.email IS NOT NULL;  -- Find email fields anywhere
```

#### Array Wildcard (`[*]`)
```sql
-- Access all array elements
SELECT order_id, items[*].product_name FROM orders_table;
SELECT user_id, transactions[*].amount FROM user_transactions;

-- Filter array elements
SELECT order_id FROM orders_table WHERE items[*].quantity > 5;
SELECT user_id FROM portfolios WHERE positions[*].value > 1000;
```

#### Array Slicing (`[start:end]`) - Future Enhancement
```sql
-- Planned: Array range access (not yet implemented)
SELECT order_id, items[0:3].product_name FROM orders_table;  -- First 3 items
SELECT user_id, transactions[-5:].amount FROM user_data;     -- Last 5 transactions
```

### Comparison Operations in Path Expressions

#### Numeric Comparisons
```sql
-- Greater than comparisons
SELECT user_id FROM portfolios WHERE positions.*.shares > 100;
SELECT device_id FROM sensors WHERE readings.*.temperature > 75.0;

-- Less than comparisons
SELECT user_id FROM portfolios WHERE positions.*.risk_score < 0.3;
SELECT order_id FROM orders WHERE items.*.discount < 0.1;

-- Range comparisons
SELECT user_id FROM portfolios
WHERE positions.*.shares > 50 AND positions.*.shares < 500;
```

#### String and Type Comparisons
```sql
-- String matching in paths
SELECT user_id FROM users WHERE profile.*.status = 'active';
SELECT order_id FROM orders WHERE shipping.*.carrier = 'FedEx';

-- Existence checks
SELECT user_id FROM users WHERE profile.preferences.* IS NOT NULL;
SELECT order_id FROM orders WHERE items.*.warranty_info IS NOT NULL;
```

### Advanced Path Expression Patterns

#### Conditional Path Access
```sql
-- Access paths based on conditions
SELECT
    user_id,
    CASE
        WHEN account_type = 'premium' THEN profile.premium.*.benefits
        ELSE profile.standard.*.features
    END as available_features
FROM users_table;
```

#### Path Expression Aggregation
```sql
-- Aggregate across wildcard paths
SELECT
    user_id,
    COUNT(positions.*) as total_positions,
    SUM(positions.*.shares * positions.*.price) as portfolio_value,
    AVG(positions.*.risk_score) as avg_risk
FROM user_portfolios
GROUP BY user_id;
```

#### Nested Wildcard Combinations
```sql
-- Complex nested access patterns
SELECT
    user_id,
    accounts.*.portfolios.*.positions.*.shares as all_shares,
    accounts.*.portfolios.*.total_value
FROM complex_user_data
WHERE accounts.*.portfolios.*.positions.*.shares > 1000;
```

### Wildcard Functions with Tables

#### sql_wildcard_values() Function
```sql
-- Extract matching values using wildcard patterns
SELECT
    user_id,
    sql_wildcard_values('positions.*.shares > 100') as large_positions,
    sql_wildcard_values('positions.*.price') as all_prices
FROM user_portfolios_table;

-- Risk analysis with wildcard aggregation
SELECT
    portfolio_id,
    sql_wildcard_values('holdings.*.risk_level > 7') as high_risk_holdings,
    sql_wildcard_values('holdings.*.sector') as all_sectors
FROM investment_portfolios;
```

#### Performance-Optimized Wildcard Queries
```sql
-- Efficient wildcard queries with early termination
SELECT user_id FROM portfolios
WHERE EXISTS(SELECT 1 FROM sql_wildcard_values('positions.*.shares > 1000'));

-- Memory-efficient batch processing
SELECT
    batch_id,
    COUNT(sql_wildcard_values('items.*.quantity > 10')) as high_quantity_items
FROM order_batches
GROUP BY batch_id;
```

### Type-Specific Path Access

#### Financial Data Patterns
```sql
-- Financial precision with ScaledInteger paths
SELECT
    trade_id,
    execution.fills.*.price,           -- ScaledInteger for exact precision
    execution.fills.*.quantity,
    execution.fills.*.price * execution.fills.*.quantity as fill_values
FROM trade_executions
WHERE execution.fills.*.venue = 'NYSE';
```

#### JSON and Structured Data
```sql
-- JSON path expressions
SELECT
    event_id,
    JSON_VALUE(payload, '$.user.profile.email') as user_email,
    JSON_VALUE(payload, '$.transaction.items[*].name') as item_names
FROM event_stream_table
WHERE JSON_EXISTS(payload, '$.user.profile');
```

#### Array and List Processing
```sql
-- Process array elements with path expressions
SELECT
    order_id,
    items[*].product_id,
    items[*].quantity,
    SUM(items[*].quantity * items[*].unit_price) as order_total
FROM orders_table
WHERE items[*].category = 'electronics'
GROUP BY order_id;
```

### Error Handling in Path Expressions

#### Graceful Path Resolution
```sql
-- Handle missing paths gracefully
SELECT
    user_id,
    COALESCE(
        profile.preferences.notifications,
        profile.settings.notifications,
        'default'
    ) as notification_setting
FROM users_table;
```

#### Path Validation
```sql
-- Validate path existence before access
SELECT user_id, profile.email
FROM users_table
WHERE profile.email IS NOT NULL
  AND LENGTH(profile.email) > 0;

-- Safe array access
SELECT order_id, items[0].product_name
FROM orders_table
WHERE ARRAY_LENGTH(items) > 0;
```

### Performance Guidelines for Path Expressions

#### Optimization Best Practices
1. **Specific Path Access**: Use exact paths when possible for better performance
2. **Early Filtering**: Apply WHERE clauses early to reduce data processing
3. **Index-Friendly Patterns**: Structure paths to leverage Table indexing
4. **Memory Efficiency**: Use CompactTable for path-heavy data structures

#### Performance Examples
```sql
-- ✅ Optimized: Specific path access
SELECT user_id, profile.email FROM users WHERE profile.email LIKE '%@company.com';

-- ❌ Less optimal: Wide wildcard search
SELECT user_id, profile.* FROM users WHERE profile.* LIKE '%@company.com';

-- ✅ Optimized: Targeted wildcard with filter
SELECT user_id FROM portfolios WHERE positions.AAPL.shares > 100;

-- ❌ Less optimal: Broad wildcard without specificity
SELECT user_id FROM portfolios WHERE positions.*.* > 100;
```

### Nested Structure Access

```sql
-- Access nested JSON fields in Table records
SELECT
    user_id,
    JSON_VALUE(profile_data, '$.preferences.notifications') as notification_pref,
    JSON_VALUE(profile_data, '$.settings.theme') as theme_setting
FROM user_profiles_table
WHERE JSON_EXISTS(profile_data, '$.preferences');
```

## Performance Optimized Table Functions

### CompactTable Integration

Functions optimized for CompactTable's memory-efficient storage:

```sql
-- Efficient field access with schema sharing
SELECT
    COALESCE(preferred_name, first_name, 'Unknown') as display_name,
    CONCAT(first_name, ' ', last_name) as full_name
FROM users_compact_table;

-- Optimized aggregation with string interning
SELECT
    status,
    COUNT(*) as status_count,
    AVG(amount) as avg_amount
FROM orders_compact_table
GROUP BY status;  -- Benefits from string interning
```

### Financial Precision Functions

ScaledInteger support for exact financial calculations:

```sql
-- Exact financial arithmetic with ScaledInteger
SELECT
    trade_id,
    shares,
    price,  -- ScaledInteger for exact precision
    shares * price as total_value,  -- 42x faster than f64
    ROUND(shares * price, 2) as display_total
FROM trades_table;

-- Portfolio value aggregation with precision
SELECT
    user_id,
    SUM(shares * price) as portfolio_value,  -- Exact precision maintained
    COUNT(*) as position_count
FROM user_positions_table
GROUP BY user_id
HAVING SUM(shares * price) > 100000;  -- $100K threshold
```

## Cross-Table Function Patterns

### Multi-Source Aggregation

```sql
-- Combine data from multiple Table sources
SELECT
    t.symbol,
    COUNT(*) as trade_count,
    SUM(t.quantity) as total_volume,
    AVG(t.price) as avg_price,
    (SELECT sector FROM symbol_metadata s WHERE s.symbol = t.symbol) as sector,
    EXISTS (SELECT 1 FROM high_volume_symbols h WHERE h.symbol = t.symbol) as is_high_volume
FROM trades_table t
GROUP BY t.symbol;
```

### Real-time Join Patterns

```sql
-- Real-time enrichment with Table lookups
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    c.account_type,
    c.credit_limit,
    CASE
        WHEN o.amount > c.credit_limit THEN 'CREDIT_CHECK_REQUIRED'
        WHEN c.account_type = 'premium' THEN 'FAST_TRACK'
        ELSE 'STANDARD'
    END as processing_priority
FROM orders_stream o
JOIN customers_table c ON o.customer_id = c.customer_id;
```

## Error Handling for Table Functions

### Graceful Degradation

```sql
-- Handle missing Table data gracefully
SELECT
    trade_id,
    symbol,
    quantity,
    COALESCE(
        (SELECT price FROM market_data WHERE symbol = trades.symbol),
        (SELECT last_known_price FROM symbol_cache WHERE symbol = trades.symbol),
        0.0
    ) as price
FROM trades_stream;
```

### Validation Functions

```sql
-- Validate Table state before processing
SELECT *
FROM incoming_orders o
WHERE EXISTS (SELECT 1 FROM inventory_table i WHERE i.product_id = o.product_id AND i.quantity >= o.quantity)
  AND EXISTS (SELECT 1 FROM customer_table c WHERE c.customer_id = o.customer_id AND c.status = 'active');
```

## Advanced Table Function Use Cases

### Dynamic Configuration Management

```sql
-- Real-time configuration updates
SELECT
    user_id,
    request_type,
    CASE
        WHEN request_count > (SELECT rate_limit FROM config_table WHERE user_tier = users.tier)
        THEN 'RATE_LIMITED'
        ELSE 'ALLOWED'
    END as request_status
FROM api_requests
JOIN users_table ON api_requests.user_id = users_table.user_id;
```

### Fraud Detection Patterns

```sql
-- Real-time fraud detection with Table state
SELECT
    transaction_id,
    user_id,
    amount,
    'SUSPICIOUS' as flag
FROM transactions_stream t
WHERE amount > (
    SELECT avg_transaction_amount * 3
    FROM user_spending_patterns p
    WHERE p.user_id = t.user_id
)
AND NOT EXISTS (
    SELECT 1 FROM trusted_merchants m
    WHERE m.merchant_id = t.merchant_id
);
```

### Compliance and Audit

```sql
-- Audit trail with Table integration
SELECT
    event_id,
    user_id,
    action,
    timestamp,
    EXISTS (SELECT 1 FROM authorized_users a WHERE a.user_id = events.user_id AND a.permission = events.action) as is_authorized,
    (SELECT compliance_level FROM user_compliance WHERE user_id = events.user_id) as compliance_level
FROM audit_events
WHERE timestamp > NOW() - INTERVAL 1 DAY;
```

## Performance Guidelines

### Table Function Optimization

1. **Use EXISTS over IN for existence checks** - Better performance for large Tables
2. **Leverage CompactTable for reference data** - 40-60% memory reduction
3. **Cache scalar subquery results** - Consider Table state stability
4. **Use specific field access** - Avoid `SELECT *` in subqueries

### Query Pattern Best Practices

```sql
-- ✅ Optimized: Specific field access
SELECT user_id, amount
FROM transactions
WHERE user_id IN (SELECT user_id FROM premium_users);

-- ❌ Less optimal: Full record selection
SELECT *
FROM transactions
WHERE user_id IN (SELECT * FROM premium_users);

-- ✅ Optimized: EXISTS for validation
SELECT order_id, amount
FROM orders o
WHERE EXISTS (SELECT 1 FROM inventory i WHERE i.product_id = o.product_id);

-- ❌ Less optimal: IN with multiple fields
SELECT order_id, amount
FROM orders o
WHERE product_id IN (SELECT product_id FROM inventory WHERE quantity > 0);
```

## Integration with Streaming Context

### ProcessorContext Table Access

```rust
// Tables available in SQL execution context
pub struct ProcessorContext {
    pub state_tables: HashMap<String, Arc<dyn SqlQueryable + Send + Sync>>,
}
```

### Real-time Table Updates

```sql
-- Process updates with live Table state
SELECT
    event_id,
    user_id,
    event_type,
    CASE
        WHEN event_type = 'purchase' AND (SELECT account_balance FROM user_accounts WHERE user_id = events.user_id) >= 0
        THEN 'APPROVED'
        WHEN event_type = 'purchase'
        THEN 'INSUFFICIENT_FUNDS'
        ELSE 'PROCESSED'
    END as status
FROM user_events_stream;
```

This Table SQL integration provides production-ready capabilities for complex real-time analytics, combining the performance benefits of Velostream's Table architecture with full SQL functionality.

## Summary

Table integration functions enable:
- **Real-time subquery execution** against Table state
- **Cross-table analytics** with consistent SQL syntax
- **Financial precision** with ScaledInteger support
- **Memory efficiency** through CompactTable optimization
- **Production performance** with sub-millisecond Table access

These capabilities position Velostream as a comprehensive streaming SQL solution for enterprise real-time analytics requirements.