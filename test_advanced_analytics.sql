-- Test comprehensive advanced analytics functions status
SELECT
    symbol,
    price,
    volume,
    event_time,

    -- Standard statistical functions (should work)
    AVG(price) OVER (PARTITION BY symbol ORDER BY event_time ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma_5,
    STDDEV(price) OVER (PARTITION BY symbol ORDER BY event_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as volatility,
    VARIANCE(price) OVER (PARTITION BY symbol ORDER BY event_time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as price_variance,

    -- Percentile functions (newly implemented)
    PERCENTILE_CONT(0.5) OVER (PARTITION BY symbol ORDER BY price) as median_price,
    PERCENTILE_DISC(0.95) OVER (PARTITION BY symbol ORDER BY price) as price_95th,

    -- Correlation and covariance functions (newly implemented)
    CORR(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_correlation,
    COVAR_POP(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_covar_pop,
    COVAR_SAMP(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_covar_samp,

    -- Regression analysis functions (newly implemented)
    REGR_SLOPE(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_slope,
    REGR_INTERCEPT(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_intercept,
    REGR_R2(price, volume) OVER (PARTITION BY symbol ORDER BY event_time) as price_volume_r_squared

FROM market_data
ORDER BY symbol, event_time;