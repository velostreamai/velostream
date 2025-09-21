# Financial Trading Analytics

Copy-paste SQL queries for real-time financial trading analysis and risk management.

**NEW**: Enhanced with latest SQL parser features including table aliases in window functions, INTERVAL-based window frames, and SQL standard EXTRACT syntax.

## Market Data Analysis

### Real-Time Price Monitoring

```sql
-- Live stock price analysis with moving averages
SELECT
    symbol,
    price,
    volume,
    trade_timestamp,
    -- Moving averages
    AVG(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as sma_20,
    AVG(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) as sma_50,
    -- Price change indicators
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp) as prev_price,
    (price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp)) * 100.0 /
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp) as price_change_pct,
    -- Volume analysis
    AVG(volume) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as avg_volume_10
FROM market_data
WHERE trade_timestamp > NOW() - INTERVAL '1' HOUR;
```

### Advanced Multi-Table Analytics (NEW)

```sql
-- Enterprise-grade trading analytics with table aliases and INTERVAL frames
SELECT
    p.trader_id,
    p.symbol,
    m.price,
    m.volume,
    m.event_time,
    -- Table aliases in window PARTITION BY (NEW FEATURE)
    LAG(m.price, 1) OVER (PARTITION BY p.trader_id ORDER BY m.event_time) as prev_trader_price,
    LEAD(m.price, 1) OVER (PARTITION BY p.trader_id ORDER BY m.event_time) as next_trader_price,
    RANK() OVER (PARTITION BY m.symbol ORDER BY m.volume DESC) as volume_rank,
    -- Time-based rolling windows with INTERVAL syntax (NEW FEATURE)
    AVG(m.price) OVER (
        PARTITION BY p.trader_id
        ORDER BY m.event_time
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) as hourly_avg_price,
    COUNT(*) OVER (
        PARTITION BY m.symbol
        ORDER BY m.event_time
        RANGE BETWEEN INTERVAL '15' MINUTE PRECEDING AND CURRENT ROW
    ) as trades_last_15min,
    STDDEV(m.price) OVER (
        PARTITION BY p.trader_id
        ORDER BY m.event_time
        RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
    ) as daily_price_volatility,
    -- SQL standard EXTRACT syntax (NEW FEATURE)
    EXTRACT(HOUR FROM m.event_time) as trade_hour,
    EXTRACT(DOW FROM m.event_time) as day_of_week,
    EXTRACT(EPOCH FROM (m.event_time - p.event_time)) as position_age_seconds
FROM market_data m
JOIN positions p ON m.symbol = p.symbol
WHERE m.event_time >= '2024-01-01T00:00:00Z'
    AND p.quantity > 100
    AND m.price BETWEEN 50.0 AND 500.0
ORDER BY p.trader_id, m.event_time DESC;
```

### Market Volatility Detection

```sql
-- Detect high volatility periods
SELECT
    symbol,
    trade_timestamp,
    price,
    -- Price volatility calculation
    STDDEV(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as volatility_30_periods,
    -- Price range analysis
    MAX(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) - MIN(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as price_range_20,
    -- Volatility alerts
    CASE
        WHEN STDDEV(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) > AVG(STDDEV(price)) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 199 PRECEDING AND 30 PRECEDING
        ) * 2 THEN 'HIGH_VOLATILITY'
        ELSE 'NORMAL'
    END as volatility_alert
FROM market_data
WHERE trade_timestamp > NOW() - INTERVAL '4' HOURS;
```

## Risk Management

### Position Risk Analysis

```sql
-- Real-time portfolio risk assessment
SELECT
    portfolio_id,
    symbol,
    position_size,
    current_price,
    entry_price,
    -- P&L calculation
    (current_price - entry_price) * position_size as unrealized_pnl,
    (current_price - entry_price) * 100.0 / entry_price as return_pct,
    -- Risk metrics
    CASE
        WHEN position_size > 0 THEN (entry_price - current_price) * position_size
        ELSE (current_price - entry_price) * ABS(position_size)
    END as potential_loss,
    -- Position concentration
    ABS(position_size * current_price) * 100.0 / SUM(ABS(position_size * current_price)) OVER (
        PARTITION BY portfolio_id
    ) as position_weight_pct,
    -- Risk alerts
    CASE
        WHEN ABS((current_price - entry_price) * 100.0 / entry_price) > 10 THEN 'LARGE_MOVE'
        WHEN ABS(position_size * current_price) > 100000 THEN 'LARGE_POSITION'
        ELSE 'NORMAL'
    END as risk_flag
FROM positions p
JOIN market_data m ON p.symbol = m.symbol
WHERE m.trade_timestamp > NOW() - INTERVAL '5' MINUTES;
```

### Stop Loss Monitoring

```sql
-- Monitor stop loss triggers
SELECT
    portfolio_id,
    symbol,
    position_size,
    entry_price,
    current_price,
    stop_loss_price,
    -- Stop loss analysis
    CASE
        WHEN position_size > 0 AND current_price <= stop_loss_price THEN 'STOP_LOSS_TRIGGERED'
        WHEN position_size < 0 AND current_price >= stop_loss_price THEN 'STOP_LOSS_TRIGGERED'
        WHEN position_size > 0 AND current_price <= stop_loss_price * 1.02 THEN 'APPROACHING_STOP_LOSS'
        WHEN position_size < 0 AND current_price >= stop_loss_price * 0.98 THEN 'APPROACHING_STOP_LOSS'
        ELSE 'SAFE'
    END as stop_loss_status,
    -- Distance to stop loss
    CASE
        WHEN position_size > 0 THEN (current_price - stop_loss_price) * 100.0 / current_price
        ELSE (stop_loss_price - current_price) * 100.0 / current_price
    END as distance_to_stop_pct,
    -- Potential loss at stop
    CASE
        WHEN position_size > 0 THEN (stop_loss_price - entry_price) * position_size
        ELSE (entry_price - stop_loss_price) * ABS(position_size)
    END as stop_loss_amount
FROM positions p
JOIN market_data m ON p.symbol = m.symbol
WHERE m.trade_timestamp > NOW() - INTERVAL '1' MINUTE
  AND p.stop_loss_price IS NOT NULL;
```

## Technical Analysis

### Support and Resistance Levels

```sql
-- Identify support and resistance levels
SELECT
    symbol,
    trade_timestamp,
    price,
    -- Recent high/low levels
    MAX(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) as resistance_100,
    MIN(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) as support_100,
    -- Proximity to levels
    (price - MIN(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    )) * 100.0 / price as distance_from_support_pct,
    (MAX(price) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) - price) * 100.0 / price as distance_from_resistance_pct,
    -- Level tests
    CASE
        WHEN ABS(price - MAX(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        )) / price < 0.01 THEN 'TESTING_RESISTANCE'
        WHEN ABS(price - MIN(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
        )) / price < 0.01 THEN 'TESTING_SUPPORT'
        ELSE 'NORMAL'
    END as level_test
FROM market_data
WHERE trade_timestamp > NOW() - INTERVAL '2' HOURS;
```

### Momentum Indicators

```sql
-- RSI and momentum analysis
SELECT
    symbol,
    trade_timestamp,
    price,
    -- Price changes
    price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp) as price_change,
    -- RSI calculation components
    AVG(CASE
        WHEN price > LAG(price, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp)
        THEN price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp)
        ELSE 0
    END) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as avg_gain_14,
    AVG(CASE
        WHEN price < LAG(price, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp)
        THEN LAG(price, 1) OVER (PARTITION BY symbol ORDER BY trade_timestamp) - price
        ELSE 0
    END) OVER (
        PARTITION BY symbol
        ORDER BY trade_timestamp
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as avg_loss_14,
    -- Momentum signals
    CASE
        WHEN COUNT(*) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) = 7 AND price > LAG(price, 6) OVER (PARTITION BY symbol ORDER BY trade_timestamp)
        THEN 'BULLISH_MOMENTUM'
        WHEN COUNT(*) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) = 7 AND price < LAG(price, 6) OVER (PARTITION BY symbol ORDER BY trade_timestamp)
        THEN 'BEARISH_MOMENTUM'
        ELSE 'NEUTRAL'
    END as momentum_signal
FROM market_data
WHERE trade_timestamp > NOW() - INTERVAL '3' HOURS;
```

## Trade Execution Analysis

### Order Book Analysis

```sql
-- Real-time order book depth analysis
SELECT
    symbol,
    timestamp,
    -- Bid side analysis
    SUM(CASE WHEN side = 'BID' THEN quantity ELSE 0 END) as total_bid_quantity,
    COUNT(CASE WHEN side = 'BID' THEN 1 END) as bid_levels,
    MAX(CASE WHEN side = 'BID' THEN price ELSE 0 END) as best_bid,
    -- Ask side analysis
    SUM(CASE WHEN side = 'ASK' THEN quantity ELSE 0 END) as total_ask_quantity,
    COUNT(CASE WHEN side = 'ASK' THEN 1 END) as ask_levels,
    MIN(CASE WHEN side = 'ASK' THEN price ELSE 999999 END) as best_ask,
    -- Spread analysis
    MIN(CASE WHEN side = 'ASK' THEN price ELSE 999999 END) -
    MAX(CASE WHEN side = 'BID' THEN price ELSE 0 END) as bid_ask_spread,
    (MIN(CASE WHEN side = 'ASK' THEN price ELSE 999999 END) -
     MAX(CASE WHEN side = 'BID' THEN price ELSE 0 END)) * 100.0 /
    MAX(CASE WHEN side = 'BID' THEN price ELSE 0 END) as spread_pct,
    -- Market depth ratio
    SUM(CASE WHEN side = 'BID' THEN quantity ELSE 0 END) * 100.0 /
    (SUM(CASE WHEN side = 'BID' THEN quantity ELSE 0 END) +
     SUM(CASE WHEN side = 'ASK' THEN quantity ELSE 0 END)) as bid_ratio_pct
FROM order_book
WHERE timestamp > NOW() - INTERVAL '10' MINUTES
GROUP BY symbol, timestamp
ORDER BY symbol, timestamp;
```

### Trade Execution Quality

```sql
-- Analyze trade execution performance
SELECT
    order_id,
    symbol,
    order_type,
    side,
    order_quantity,
    filled_quantity,
    avg_fill_price,
    order_timestamp,
    fill_timestamp,
    -- Execution metrics
    filled_quantity * 100.0 / order_quantity as fill_rate_pct,
    DATEDIFF('seconds', order_timestamp, fill_timestamp) as execution_time_seconds,
    -- Price improvement analysis
    CASE
        WHEN side = 'BUY' AND avg_fill_price < expected_price THEN 'POSITIVE_SLIPPAGE'
        WHEN side = 'SELL' AND avg_fill_price > expected_price THEN 'POSITIVE_SLIPPAGE'
        WHEN side = 'BUY' AND avg_fill_price > expected_price THEN 'NEGATIVE_SLIPPAGE'
        WHEN side = 'SELL' AND avg_fill_price < expected_price THEN 'NEGATIVE_SLIPPAGE'
        ELSE 'NO_SLIPPAGE'
    END as slippage_direction,
    ABS(avg_fill_price - expected_price) * filled_quantity as slippage_cost,
    -- Market impact
    ABS(avg_fill_price - expected_price) * 100.0 / expected_price as market_impact_pct
FROM trade_executions
WHERE fill_timestamp > NOW() - INTERVAL '1' HOUR
  AND filled_quantity > 0;
```

## Portfolio Analytics

### Portfolio Performance

```sql
-- Real-time portfolio performance tracking
SELECT
    portfolio_id,
    -- Position summary
    COUNT(*) as total_positions,
    SUM(CASE WHEN position_size > 0 THEN 1 ELSE 0 END) as long_positions,
    SUM(CASE WHEN position_size < 0 THEN 1 ELSE 0 END) as short_positions,
    -- Market value
    SUM(position_size * current_price) as total_market_value,
    SUM(ABS(position_size * current_price)) as gross_market_value,
    -- P&L calculation
    SUM((current_price - entry_price) * position_size) as total_unrealized_pnl,
    SUM((current_price - entry_price) * position_size) * 100.0 /
    SUM(ABS(entry_price * position_size)) as total_return_pct,
    -- Risk metrics
    STDDEV((current_price - entry_price) * position_size) as position_risk,
    MAX(ABS(position_size * current_price)) as largest_position_value,
    MAX(ABS(position_size * current_price)) * 100.0 /
    SUM(ABS(position_size * current_price)) as concentration_risk_pct
FROM positions p
JOIN market_data m ON p.symbol = m.symbol
WHERE m.trade_timestamp > NOW() - INTERVAL '5' MINUTES
GROUP BY portfolio_id;
```

### Sector Exposure

```sql
-- Portfolio sector exposure analysis
SELECT
    p.portfolio_id,
    s.sector,
    COUNT(*) as positions_count,
    SUM(ABS(p.position_size * m.current_price)) as sector_exposure,
    SUM(ABS(p.position_size * m.current_price)) * 100.0 / portfolio_total.total_value as sector_weight_pct,
    SUM((m.current_price - p.entry_price) * p.position_size) as sector_pnl,
    AVG((m.current_price - p.entry_price) * 100.0 / p.entry_price) as avg_sector_return_pct
FROM positions p
JOIN market_data m ON p.symbol = m.symbol
JOIN stock_metadata s ON p.symbol = s.symbol
JOIN (
    SELECT
        portfolio_id,
        SUM(ABS(position_size * current_price)) as total_value
    FROM positions p2
    JOIN market_data m2 ON p2.symbol = m2.symbol
    WHERE m2.trade_timestamp > NOW() - INTERVAL '5' MINUTES
    GROUP BY portfolio_id
) portfolio_total ON p.portfolio_id = portfolio_total.portfolio_id
WHERE m.trade_timestamp > NOW() - INTERVAL '5' MINUTES
GROUP BY p.portfolio_id, s.sector
ORDER BY p.portfolio_id, sector_weight_pct DESC;
```

## Algorithmic Trading

### Signal Generation

```sql
-- Generate trading signals based on technical indicators
SELECT
    symbol,
    trade_timestamp,
    price,
    -- Moving average signals
    CASE
        WHEN AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) > AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ) THEN 'MA_BULLISH'
        ELSE 'MA_BEARISH'
    END as ma_signal,
    -- Volume confirmation
    CASE
        WHEN volume > AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) * 1.5 THEN 'HIGH_VOLUME'
        ELSE 'NORMAL_VOLUME'
    END as volume_signal,
    -- Breakout detection
    CASE
        WHEN price > MAX(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) THEN 'BREAKOUT_UP'
        WHEN price < MIN(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) THEN 'BREAKOUT_DOWN'
        ELSE 'NO_BREAKOUT'
    END as breakout_signal,
    -- Combined signal
    CASE
        WHEN (AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) > AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        )) AND volume > AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) * 1.2 THEN 'BUY'
        WHEN (AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) < AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        )) AND volume > AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY trade_timestamp
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) * 1.2 THEN 'SELL'
        ELSE 'HOLD'
    END as trading_signal
FROM market_data
WHERE trade_timestamp > NOW() - INTERVAL '2' HOURS;
```

## Compliance and Reporting

### Trading Limits Monitoring

```sql
-- Monitor trading limits and compliance
SELECT
    trader_id,
    portfolio_id,
    symbol,
    -- Position limits
    ABS(position_size * current_price) as current_position_value,
    position_limit,
    ABS(position_size * current_price) * 100.0 / position_limit as position_utilization_pct,
    -- Daily trading limits
    SUM(ABS(trade_quantity * trade_price)) OVER (
        PARTITION BY trader_id
        ORDER BY trade_timestamp
        RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
    ) as daily_trade_volume,
    daily_trade_limit,
    SUM(ABS(trade_quantity * trade_price)) OVER (
        PARTITION BY trader_id
        ORDER BY trade_timestamp
        RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
    ) * 100.0 / daily_trade_limit as daily_limit_utilization_pct,
    -- Compliance alerts
    CASE
        WHEN ABS(position_size * current_price) > position_limit * 0.9 THEN 'POSITION_LIMIT_WARNING'
        WHEN SUM(ABS(trade_quantity * trade_price)) OVER (
            PARTITION BY trader_id
            ORDER BY trade_timestamp
            RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
        ) > daily_trade_limit * 0.9 THEN 'DAILY_LIMIT_WARNING'
        ELSE 'COMPLIANT'
    END as compliance_status
FROM trading_activity t
JOIN positions p ON t.symbol = p.symbol AND t.trader_id = p.trader_id
JOIN market_data m ON t.symbol = m.symbol
JOIN trader_limits tl ON t.trader_id = tl.trader_id
WHERE t.trade_timestamp > NOW() - INTERVAL '1' DAY
  AND m.trade_timestamp > NOW() - INTERVAL '5' MINUTES;
```

## Performance Tips

1. **Use time-based partitioning** for market data tables
2. **Index on symbol + timestamp** for efficient time series queries
3. **Consider materialized views** for complex moving averages
4. **Use appropriate data types** for financial precision
5. **Implement data retention policies** for historical data

## Common Patterns

```sql
-- Pattern: Price Change Detection
CASE
    WHEN current_price > prev_price * 1.05 THEN 'SIGNIFICANT_UP'
    WHEN current_price < prev_price * 0.95 THEN 'SIGNIFICANT_DOWN'
    ELSE 'NORMAL'
END as price_movement;

-- Pattern: Volume Spike Detection
CASE
    WHEN volume > avg_volume * 2 THEN 'VOLUME_SPIKE'
    ELSE 'NORMAL_VOLUME'
END as volume_alert;

-- Pattern: Risk Level Classification
CASE
    WHEN position_risk_pct > 10 THEN 'HIGH_RISK'
    WHEN position_risk_pct > 5 THEN 'MEDIUM_RISK'
    ELSE 'LOW_RISK'
END as risk_level;
```