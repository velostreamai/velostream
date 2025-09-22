-- Test simple EXTRACT function
SELECT
    symbol,
    EXTRACT(HOUR FROM event_time) as hour_of_day
FROM market_data;