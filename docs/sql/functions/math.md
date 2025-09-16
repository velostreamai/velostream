# Math Functions

Complete reference for mathematical functions in VeloStream. Use these functions for numeric calculations, rounding, and mathematical operations in your streaming queries.

## Basic Arithmetic Functions

### ABS - Absolute Value

```sql
-- Calculate absolute differences
SELECT
    transaction_id,
    balance_change,
    ABS(balance_change) as absolute_change,
    CASE
        WHEN balance_change > 0 THEN 'Credit'
        WHEN balance_change < 0 THEN 'Debit'
        ELSE 'No Change'
    END as transaction_type
FROM transactions;

-- Find largest price movements
SELECT
    symbol,
    current_price,
    previous_price,
    current_price - previous_price as price_change,
    ABS(current_price - previous_price) as absolute_price_change
FROM stock_prices
ORDER BY ABS(current_price - previous_price) DESC;
```

## Rounding Functions

### ROUND - Round to Decimal Places

```sql
-- Round monetary amounts
SELECT
    order_id,
    amount,
    ROUND(amount) as rounded_amount,
    ROUND(amount, 2) as rounded_to_cents,
    ROUND(amount * 1.1, 2) as amount_with_tax
FROM orders;

-- Statistical rounding
SELECT
    product_id,
    AVG(rating) as avg_rating,
    ROUND(AVG(rating), 1) as rounded_rating,
    ROUND(AVG(rating) * 2) / 2 as half_star_rating  -- Round to nearest 0.5
FROM product_reviews
GROUP BY product_id;
```

### CEIL/CEILING - Round Up

```sql
-- Calculate required containers/pages
SELECT
    order_id,
    total_items,
    items_per_box,
    CEIL(total_items / items_per_box) as boxes_needed
FROM order_shipping;

-- Pricing tiers
SELECT
    usage_amount,
    CEIL(usage_amount / 100) as billing_units,
    CEIL(usage_amount / 100) * 10 as total_cost
FROM usage_metrics;
```

### FLOOR - Round Down

```sql
-- Age groups and time buckets
SELECT
    customer_id,
    age,
    FLOOR(age / 10) * 10 as age_decade
FROM customers;

-- Discount tiers
SELECT
    order_amount,
    FLOOR(order_amount / 100) as discount_tier,
    FLOOR(order_amount / 100) * 5 as discount_percentage
FROM orders;
```

## Advanced Mathematical Operations

### MOD - Modulo (Remainder)

```sql
-- Distribute orders across workers
SELECT
    order_id,
    MOD(order_id, 4) as worker_assignment,
    CASE MOD(order_id, 4)
        WHEN 0 THEN 'Worker A'
        WHEN 1 THEN 'Worker B'
        WHEN 2 THEN 'Worker C'
        ELSE 'Worker D'
    END as assigned_worker
FROM orders;

-- Find even/odd patterns
SELECT
    customer_id,
    order_count,
    MOD(order_count, 2) as remainder,
    CASE MOD(order_count, 2)
        WHEN 0 THEN 'Even number of orders'
        ELSE 'Odd number of orders'
    END as order_pattern
FROM customer_order_counts;
```

### POWER/POW - Exponentiation

```sql
-- Calculate compound growth
SELECT
    year,
    initial_revenue,
    growth_rate,
    POWER(1 + growth_rate, year - 2020) as growth_multiplier,
    initial_revenue * POWER(1 + growth_rate, year - 2020) as projected_revenue
FROM revenue_projections;

-- Area and volume calculations
SELECT
    product_id,
    length,
    width,
    height,
    length * width as area,
    POWER(length, 2) as length_squared,
    length * width * height as volume
FROM product_dimensions;
```

### SQRT - Square Root

```sql
-- Calculate distances and standard deviations
SELECT
    location_id,
    area,
    SQRT(area) as approximate_side_length,
    CASE
        WHEN SQRT(area) > 100 THEN 'Large'
        WHEN SQRT(area) > 50 THEN 'Medium'
        ELSE 'Small'
    END as size_category
FROM locations;

-- Statistical calculations
SELECT
    product_category,
    AVG(price) as mean_price,
    VARIANCE(price) as price_variance,
    SQRT(VARIANCE(price)) as price_std_dev
FROM products
GROUP BY product_category;
```

## Financial Calculations

### Interest and Growth Calculations

```sql
-- Compound interest calculation
SELECT
    account_id,
    principal_amount,
    interest_rate,
    years,
    principal_amount * POWER(1 + interest_rate, years) as compound_value,
    principal_amount * POWER(1 + interest_rate, years) - principal_amount as interest_earned
FROM investment_accounts;

-- Monthly payment calculation (simplified)
SELECT
    loan_id,
    loan_amount,
    annual_rate,
    term_months,
    loan_amount * (annual_rate / 12) * POWER(1 + annual_rate / 12, term_months) /
    (POWER(1 + annual_rate / 12, term_months) - 1) as monthly_payment
FROM loans;
```

### Percentage Calculations

```sql
-- Calculate percentage changes
SELECT
    product_id,
    old_price,
    new_price,
    new_price - old_price as price_change,
    ROUND((new_price - old_price) * 100.0 / old_price, 2) as percent_change,
    ABS(ROUND((new_price - old_price) * 100.0 / old_price, 2)) as absolute_percent_change
FROM price_changes
WHERE old_price > 0;
```

## Statistical Functions

### Distribution Analysis

```sql
-- Analyze score distributions
SELECT
    student_id,
    test_score,
    class_average,
    POWER(test_score - class_average, 2) as squared_deviation,
    ABS(test_score - class_average) as absolute_deviation,
    CASE
        WHEN ABS(test_score - class_average) > 20 THEN 'Outlier'
        WHEN ABS(test_score - class_average) > 10 THEN 'Above/Below Average'
        ELSE 'Near Average'
    END as performance_category
FROM test_results;
```

### Risk Calculations

```sql
-- Portfolio risk metrics
SELECT
    portfolio_id,
    AVG(daily_return) as mean_return,
    SQRT(VARIANCE(daily_return)) as volatility,
    MIN(daily_return) as worst_day,
    MAX(daily_return) as best_day,
    SQRT(VARIANCE(daily_return)) / ABS(AVG(daily_return)) as coefficient_of_variation
FROM portfolio_returns
GROUP BY portfolio_id
HAVING COUNT(*) >= 30;  -- At least 30 days of data
```

## Real-World Examples

### E-commerce Pricing Strategy

```sql
-- Dynamic pricing calculations
SELECT
    product_id,
    base_price,
    demand_factor,
    competition_factor,
    inventory_factor,
    -- Calculate dynamic price with multiple factors
    ROUND(
        base_price *
        POWER(demand_factor, 0.3) *
        POWER(competition_factor, 0.4) *
        POWER(inventory_factor, 0.3),
        2
    ) as dynamic_price,
    -- Calculate discount percentage needed to match competitor
    ROUND(
        (1 - competitor_price / base_price) * 100,
        1
    ) as discount_needed_pct
FROM pricing_factors;
```

### Inventory Management

```sql
-- Calculate reorder points and quantities
SELECT
    product_id,
    current_stock,
    daily_sales_rate,
    lead_time_days,
    -- Safety stock calculation
    CEIL(daily_sales_rate * SQRT(lead_time_days) * 1.65) as safety_stock,
    -- Reorder point
    CEIL(daily_sales_rate * lead_time_days +
         daily_sales_rate * SQRT(lead_time_days) * 1.65) as reorder_point,
    -- Economic order quantity (simplified)
    CEIL(SQRT(2 * annual_demand * order_cost / holding_cost)) as eoq
FROM inventory_metrics;
```

### Performance Scoring

```sql
-- Calculate normalized performance scores
SELECT
    employee_id,
    sales_amount,
    target_amount,
    team_average,
    team_std_dev,
    -- Performance as percentage of target
    ROUND(sales_amount * 100.0 / target_amount, 1) as target_achievement_pct,
    -- Z-score calculation
    ROUND((sales_amount - team_average) / team_std_dev, 2) as z_score,
    -- Performance tier based on standard deviations
    CASE
        WHEN (sales_amount - team_average) / team_std_dev > 2 THEN 'Exceptional'
        WHEN (sales_amount - team_average) / team_std_dev > 1 THEN 'Above Average'
        WHEN (sales_amount - team_average) / team_std_dev > -1 THEN 'Average'
        WHEN (sales_amount - team_average) / team_std_dev > -2 THEN 'Below Average'
        ELSE 'Needs Improvement'
    END as performance_tier
FROM sales_performance;
```

### Gaming and Scoring Systems

```sql
-- Calculate game scores with bonuses
SELECT
    player_id,
    base_score,
    time_bonus_factor,
    accuracy_rate,
    difficulty_multiplier,
    -- Calculate final score with exponential bonuses
    ROUND(
        base_score *
        POWER(time_bonus_factor, 0.5) *
        POWER(accuracy_rate, 2) *
        difficulty_multiplier
    ) as final_score,
    -- Level calculation
    FLOOR(SQRT(base_score / 1000)) + 1 as player_level
FROM game_sessions;
```

### Engineering Calculations

```sql
-- Sensor calibration and engineering units
SELECT
    sensor_id,
    raw_reading,
    calibration_offset,
    calibration_scale,
    -- Convert raw reading to engineering units
    ROUND((raw_reading + calibration_offset) * calibration_scale, 3) as calibrated_value,
    -- Calculate moving average of squared deviations
    AVG(POWER(raw_reading - AVG(raw_reading) OVER (
        PARTITION BY sensor_id
        ORDER BY reading_timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ), 2)) OVER (
        PARTITION BY sensor_id
        ORDER BY reading_timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as variance_10_readings
FROM sensor_readings;
```

## Performance Tips

### Efficient Mathematical Operations

```sql
-- ✅ Good: Use simple operations when possible
SELECT amount * 2 FROM orders;  -- Faster than POWER(amount, 2) for squaring

-- ✅ Good: Pre-calculate constants
SELECT
    amount,
    amount * 1.08 as amount_with_tax  -- Pre-calculated tax multiplier
FROM orders;

-- ⚠️ Careful: Complex mathematical operations on large datasets
SELECT POWER(SQRT(ABS(amount)), 3) FROM large_table;  -- Consider pre-processing
```

### Precision Considerations

```sql
-- Handle division by zero
SELECT
    numerator,
    denominator,
    CASE
        WHEN denominator = 0 THEN NULL
        ELSE ROUND(numerator / denominator, 4)
    END as safe_division
FROM calculations;

-- Maintain precision in financial calculations
SELECT
    order_amount,
    ROUND(order_amount * 0.08, 2) as tax_amount,  -- Always round money to 2 decimals
    ROUND(order_amount * 1.08, 2) as total_with_tax
FROM orders;
```

## Common Patterns

### Bucketing and Binning

```sql
-- Create value buckets
SELECT
    customer_id,
    total_spent,
    FLOOR(total_spent / 1000) * 1000 as spending_bucket,
    CASE
        WHEN total_spent < 100 THEN 'Low Spender'
        WHEN total_spent < 500 THEN 'Medium Spender'
        ELSE 'High Spender'
    END as spending_category
FROM customer_totals;
```

### Normalization

```sql
-- Normalize values to 0-100 scale
SELECT
    product_id,
    rating,
    min_rating,
    max_rating,
    ROUND(
        (rating - min_rating) * 100.0 / (max_rating - min_rating),
        1
    ) as normalized_rating_0_100
FROM product_ratings
CROSS JOIN (
    SELECT MIN(rating) as min_rating, MAX(rating) as max_rating
    FROM product_ratings
) rating_bounds;
```

### Ranking and Scoring

```sql
-- Calculate weighted scores
SELECT
    candidate_id,
    technical_score,
    communication_score,
    experience_score,
    ROUND(
        technical_score * 0.5 +
        communication_score * 0.3 +
        experience_score * 0.2,
        2
    ) as weighted_total_score
FROM interview_scores;
```

## Quick Reference

| Function | Purpose | Example |
|----------|---------|--------|
| `ABS(number)` | Absolute value | `ABS(-5)` → 5 |
| `ROUND(number, places)` | Round to decimal places | `ROUND(3.14159, 2)` → 3.14 |
| `CEIL(number)` | Round up | `CEIL(3.1)` → 4 |
| `CEILING(number)` | Round up (alias) | `CEILING(3.1)` → 4 |
| `FLOOR(number)` | Round down | `FLOOR(3.9)` → 3 |
| `MOD(a, b)` | Remainder of a/b | `MOD(10, 3)` → 1 |
| `POWER(base, exp)` | Raise to power | `POWER(2, 3)` → 8 |
| `POW(base, exp)` | Raise to power (alias) | `POW(2, 3)` → 8 |
| `SQRT(number)` | Square root | `SQRT(16)` → 4 |

## Mathematical Constants

While not built-in functions, you can use these common constants:

```sql
-- Common mathematical constants
SELECT
    radius,
    2 * 3.14159 * radius as circumference,  -- π ≈ 3.14159
    3.14159 * POWER(radius, 2) as area,     -- π * r²
    POWER(2.71828, rate * time) as exponential_growth  -- e ≈ 2.71828
FROM circles;
```

## Next Steps

- [Aggregation functions](aggregation.md) - Statistical functions like STDDEV, VARIANCE
- [Essential functions](essential.md) - Most commonly used functions
- [Window functions](window.md) - Mathematical operations over row sets