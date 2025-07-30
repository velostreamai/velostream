-- SQL Application: Real-Time Trading Analytics
-- Version: 3.0.1
-- Description: High-frequency trading data analysis and risk management system
-- Author: Quantitative Trading Team
-- Dependencies: kafka-market-data, kafka-trades, kafka-orders
-- Tag: latency:ultra-low
-- Tag: compliance:regulatory

-- Name: Price Movement Detector
-- Property: sensitivity=0.05
-- Property: window_size=1m
START JOB price_movement_detection AS
SELECT 
    symbol,
    price,
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price,
    (price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp)) / LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) * 100 as price_change_pct,
    volume,
    timestamp() as detection_time
FROM market_data
WHERE ABS((price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp)) / LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) * 100) > 5.0
WITH ('output.topic' = 'price_alerts');

-- Name: Volume Spike Analyzer
-- Property: volume_multiplier=3
-- Property: lookback_period=5m
START JOB volume_spike_analysis AS
SELECT 
    symbol,
    volume,
    AVG(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as avg_volume_20,
    volume / AVG(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as volume_ratio,
    price,
    timestamp() as spike_time
FROM market_data
WHERE volume > 3 * AVG(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
WITH ('output.topic' = 'volume_spikes');

-- Name: Risk Management Monitor
-- Property: max_position_size=1000000
-- Property: max_daily_loss=50000
START JOB risk_monitoring AS
SELECT 
    trader_id,
    symbol,
    position_size,
    current_pnl,
    SUM(current_pnl) OVER (PARTITION BY trader_id) as total_pnl,
    ABS(position_size * price) as position_value,
    CASE 
        WHEN ABS(position_size * price) > 1000000 THEN 'POSITION_LIMIT_EXCEEDED'
        WHEN SUM(current_pnl) OVER (PARTITION BY trader_id) < -50000 THEN 'DAILY_LOSS_LIMIT_EXCEEDED'
        WHEN ABS(position_size * price) > 500000 THEN 'POSITION_WARNING'
        ELSE 'WITHIN_LIMITS'
    END as risk_status
FROM trading_positions p
JOIN market_data m ON p.symbol = m.symbol
WHERE ABS(position_size * price) > 100000 OR current_pnl < -10000
WITH ('output.topic' = 'risk_alerts');

-- Name: Order Flow Imbalance
-- Property: imbalance_threshold=0.7
-- Property: min_volume=10000
START JOB order_flow_imbalance AS
SELECT 
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) as buy_volume,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) as sell_volume,
    SUM(quantity) as total_volume,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) / SUM(quantity) as buy_ratio,
    timestamp() as analysis_time
FROM order_book_updates
WHERE timestamp >= timestamp() - INTERVAL '1' MINUTE
GROUP BY symbol
HAVING SUM(quantity) > 10000 
    AND (SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) / SUM(quantity) > 0.7 
         OR SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) / SUM(quantity) > 0.7)
WITH ('output.topic' = 'order_imbalance_alerts');

-- Name: Arbitrage Opportunities
-- Property: min_spread_bps=10
-- Property: min_volume=50000
START JOB arbitrage_detection AS
SELECT 
    a.symbol,
    a.exchange as exchange_a,
    b.exchange as exchange_b,
    a.bid_price as bid_a,
    b.ask_price as ask_b,
    (a.bid_price - b.ask_price) as spread,
    (a.bid_price - b.ask_price) / b.ask_price * 10000 as spread_bps,
    LEAST(a.bid_size, b.ask_size) as available_volume,
    timestamp() as opportunity_time
FROM market_data a
JOIN market_data b ON a.symbol = b.symbol AND a.exchange != b.exchange
WHERE a.bid_price > b.ask_price 
    AND (a.bid_price - b.ask_price) / b.ask_price * 10000 > 10
    AND LEAST(a.bid_size, b.ask_size) > 50000
WITH ('output.topic' = 'arbitrage_opportunities');