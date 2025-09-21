-- Test simple analytics functions parsing
SELECT
    -- PERCENTILE functions
    PERCENTILE_CONT(0.5) as median_test,
    PERCENTILE_DISC(0.95) as percentile_test,

    -- Correlation and covariance functions
    CORR(100, 200) as correlation_test,
    COVAR_POP(100, 200) as covar_pop_test,
    COVAR_SAMP(100, 200) as covar_samp_test,

    -- Regression functions
    REGR_SLOPE(100, 200) as slope_test,
    REGR_INTERCEPT(100, 200) as intercept_test,
    REGR_R2(100, 200) as r_squared_test;