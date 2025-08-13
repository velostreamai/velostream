# Subquery Quick Reference Guide

## Quick Syntax Guide

### Scalar Subqueries
```sql
SELECT user_id, (SELECT max_limit FROM config) as limit FROM users;
```

### EXISTS
```sql
SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE id = orders.customer_id);
```

### NOT EXISTS  
```sql
SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM blocked_users WHERE user_id = users.id);
```

### IN
```sql
SELECT * FROM products WHERE category_id IN (SELECT id FROM active_categories);
```

### NOT IN
```sql
SELECT * FROM transactions WHERE account_id NOT IN (SELECT id FROM frozen_accounts);
```

### ANY/SOME
```sql
SELECT * FROM products WHERE price > ANY (SELECT competitor_price FROM market_data);
```

### ALL
```sql
SELECT * FROM users WHERE score >= ALL (SELECT min_score FROM requirements);
```

## Common Patterns

### Real-time Filtering
```sql
-- Filter active users with recent activity
SELECT user_id, last_action 
FROM user_activity ua
WHERE user_id IN (
    SELECT id FROM users 
    WHERE status = 'active' 
    AND last_login > NOW() - INTERVAL 1 DAY
);
```

### Dynamic Configuration
```sql
-- Apply rate limits based on user tier
SELECT 
    request_id,
    user_id,
    CASE 
        WHEN request_count > (SELECT rate_limit FROM config WHERE tier = user_tier)
        THEN 'BLOCKED'
        ELSE 'ALLOWED'
    END as status
FROM user_requests;
```

### Fraud Detection
```sql
-- Flag suspicious transactions
SELECT transaction_id, amount, 'SUSPICIOUS' as flag
FROM transactions t
WHERE amount > (
    SELECT AVG(amount) * 3 
    FROM user_history 
    WHERE user_id = t.user_id
)
AND NOT EXISTS (
    SELECT 1 FROM trusted_merchants 
    WHERE merchant_id = t.merchant_id
);
```

### Data Validation
```sql
-- Ensure referential integrity
SELECT order_id, customer_id, product_id
FROM orders o
WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)
  AND EXISTS (SELECT 1 FROM products p WHERE p.id = o.product_id);
```

## Performance Tips

1. **Use EXISTS instead of IN** when checking for existence:
   ```sql
   -- Preferred
   WHERE EXISTS (SELECT 1 FROM table WHERE condition)
   
   -- Avoid for large result sets
   WHERE column IN (SELECT column FROM large_table)
   ```

2. **Scalar subqueries for configuration**:
   ```sql
   -- Good for relatively static configuration values
   SELECT *, (SELECT timeout FROM config) as timeout FROM requests;
   ```

3. **NOT EXISTS for exclusion**:
   ```sql
   -- Efficient for filtering out records
   WHERE NOT EXISTS (SELECT 1 FROM blacklist WHERE user_id = target.user_id)
   ```

## Error Handling

### Common Errors
- `"IN/NOT IN subqueries must be used with binary operators"`
- `"Only SELECT queries are supported in scalar subqueries"`
- `"Parse error: Expected ')' after subquery"`

### Solutions
1. Ensure proper parentheses around subqueries
2. Use SELECT statements in subqueries
3. Match subquery type with usage context

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Scalar Subqueries | ✅ Mock | Returns `1` |
| EXISTS | ✅ Mock | Returns `true` |
| NOT EXISTS | ✅ Mock | Returns `false` |
| IN (positive int) | ✅ Mock | Returns `true` |
| IN (non-empty string) | ✅ Mock | Returns `true` |
| NOT IN | ✅ Mock | Opposite of IN |
| ANY/ALL | ✅ Mock | Based on subquery type |

**Note**: Current implementation uses mock execution. Production enhancement needed for real subquery execution against streaming data.