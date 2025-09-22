-- Test simple CTE
WITH hourly_aggregates AS (
    SELECT symbol, AVG(price) as avg_price
    FROM market_data
    GROUP BY symbol
    WINDOW TUMBLING(1h)
)
SELECT * FROM hourly_aggregates WHERE avg_price > 100;