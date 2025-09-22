### Statistical Analytics Functions
| Function | Purpose | Example |
|----------|---------|--------|
| `PERCENTILE_CONT(p)` | Interpolated percentile | `PERCENTILE_CONT(0.5) OVER (ORDER BY price)` |
| `PERCENTILE_DISC(p)` | Discrete percentile | `PERCENTILE_DISC(0.95) OVER (ORDER BY price)` |
| `CORR(y, x)` | Correlation coefficient | `CORR(price, volume) OVER (...)` |
| `COVAR_POP(y, x)` | Population covariance | `COVAR_POP(price, volume) OVER (...)` |
| `COVAR_SAMP(y, x)` | Sample covariance | `COVAR_SAMP(price, volume) OVER (...)` |
| `REGR_SLOPE(y, x)` | Linear regression slope | `REGR_SLOPE(price, time) OVER (...)` |
| `REGR_INTERCEPT(y, x)` | Linear regression intercept | `REGR_INTERCEPT(price, time) OVER (...)` |
| `REGR_R2(y, x)` | Coefficient of determination | `REGR_R2(price, time) OVER (...)` |