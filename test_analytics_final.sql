-- Test advanced analytics functions in VeloStream
SELECT
    symbol,
    price,
    volume,
    -- PERCENTILE functions
    PERCENTILE_CONT(0.5) OVER (PARTITION BY symbol ORDER BY price) as median_price,
    PERCENTILE_DISC(0.95) OVER (PARTITION BY symbol ORDER BY price) as price_95th,

    -- Correlation and covariance functions
    CORR(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_correlation,
    COVAR_POP(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_covar_pop,
    COVAR_SAMP(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_covar_samp,

    -- Regression functions
    REGR_SLOPE(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_slope,
    REGR_INTERCEPT(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_intercept,
    REGR_R2(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_r_squared

FROM market_data;