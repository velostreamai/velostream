-- Test SQL for GROUP BY with window functions and time expressions
SELECT
    symbol,
    EXTRACT(HOUR FROM event_time) as hour_of_day,
    COUNT(*) as trade_count,
    AVG(price) as avg_price
FROM market_data
GROUP BY symbol, EXTRACT(HOUR FROM event_time)
WINDOW TUMBLING(1h);