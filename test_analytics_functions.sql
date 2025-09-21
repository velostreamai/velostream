-- Test current analytics functions status
SELECT
    symbol,
    price,
    -- Standard functions (should work)
    AVG(price) OVER (PARTITION BY symbol ORDER BY event_time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma_5,
    STDDEV(price) OVER (PARTITION BY symbol ORDER BY event_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as volatility,
    -- Missing percentile functions (should fail)
    PERCENTILE_CONT(0.5) OVER (PARTITION BY symbol ORDER BY price) as median_price,
    PERCENTILE_DISC(0.95) OVER (PARTITION BY symbol ORDER BY price) as price_95th
FROM market_data;