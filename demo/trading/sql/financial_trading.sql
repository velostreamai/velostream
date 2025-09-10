-- SQL Application: Real-Time Trading Analytics (FR-047 Compliant)
-- Version: 4.0.0
-- Description: High-frequency trading data analysis and risk management system
-- Author: Quantitative Trading Team
-- Data Sources: Named sources with configuration-based approach
-- Tag: latency:ultra-low
-- Tag: compliance:regulatory

-- ====================================================================================
-- PRICE MOVEMENT DETECTION: Real-time price change alerts (FR-047)
-- ====================================================================================
-- Detects significant price movements (>5%) and generates alerts
-- Uses ScaledInteger for exact financial precision (42x faster than f64)

CREATE STREAM price_movement_alerts AS
SELECT 
    symbol,
    price,
    LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price,
    (price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp)) / LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) * 100 as price_change_pct,
    volume,
    EXTRACT(EPOCH FROM NOW()) * 1000 as detection_time
FROM market_data_stream
WHERE ABS((price - LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp)) / LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) * 100) > 5.0
INTO price_alerts_sink
WITH (
    'market_data_stream.type' = 'kafka_source',
    'market_data_stream.config_file' = 'configs/market_data_source.yaml',
    'price_alerts_sink.type' = 'kafka_sink',
    'price_alerts_sink.config_file' = 'configs/price_alerts_sink.yaml'
);

-- ====================================================================================
-- VOLUME SPIKE ANALYSIS: Detects unusual trading volume patterns (FR-047)
-- ====================================================================================
-- Identifies volume spikes 3x above 20-period moving average

CREATE STREAM volume_spike_detection AS
SELECT 
    symbol,
    volume,
    AVG(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as avg_volume_20,
    volume / AVG(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as volume_ratio,
    price,
    EXTRACT(EPOCH FROM NOW()) * 1000 as spike_time
FROM market_data_stream
WHERE volume > 3 * AVG(volume) OVER (PARTITION BY symbol ORDER BY timestamp ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING)
INTO volume_spikes_sink
WITH (
    'market_data_stream.type' = 'kafka_source',
    'market_data_stream.config_file' = 'configs/market_data_source.yaml',
    'volume_spikes_sink.type' = 'kafka_sink',
    'volume_spikes_sink.config_file' = 'configs/volume_spikes_sink.yaml'
);

-- ====================================================================================
-- RISK MANAGEMENT MONITOR: Real-time position and loss monitoring (FR-047)
-- ====================================================================================

CREATE STREAM risk_management_monitor AS
SELECT 
    p.trader_id,
    p.symbol,
    p.position_size,
    p.current_pnl,
    SUM(p.current_pnl) OVER (PARTITION BY p.trader_id) as total_pnl,
    ABS(p.position_size * m.price) as position_value,
    CASE 
        WHEN ABS(p.position_size * m.price) > 1000000 THEN 'POSITION_LIMIT_EXCEEDED'
        WHEN SUM(p.current_pnl) OVER (PARTITION BY p.trader_id) < -50000 THEN 'DAILY_LOSS_LIMIT_EXCEEDED'
        WHEN ABS(p.position_size * m.price) > 500000 THEN 'POSITION_WARNING'
        ELSE 'WITHIN_LIMITS'
    END as risk_status,
    m.price as current_price,
    timestamp() as risk_check_time
FROM trading_positions_stream p
JOIN market_data_stream m ON p.symbol = m.symbol
WHERE ABS(p.position_size * m.price) > 100000 OR p.current_pnl < -10000
INTO risk_alerts_sink
WITH (
    'trading_positions_stream.type' = 'kafka_source',
    'trading_positions_stream.config_file' = 'configs/trading_positions_source.yaml',
    'market_data_stream.type' = 'kafka_source',
    'market_data_stream.config_file' = 'configs/market_data_source.yaml',
    'risk_alerts_sink.type' = 'kafka_sink',
    'risk_alerts_sink.config_file' = 'configs/risk_alerts_sink.yaml'
);

-- ====================================================================================
-- ORDER FLOW IMBALANCE: Detects institutional trading patterns (FR-047)
-- ====================================================================================

CREATE STREAM order_flow_imbalance_detection AS
SELECT 
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) as buy_volume,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) as sell_volume,
    SUM(quantity) as total_volume,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) / SUM(quantity) as buy_ratio,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) / SUM(quantity) as sell_ratio,
    timestamp() as analysis_time
FROM order_book_stream
WHERE timestamp >= timestamp() - INTERVAL '1' MINUTE
GROUP BY symbol
HAVING SUM(quantity) > 10000 
    AND (SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) / SUM(quantity) > 0.7 
         OR SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) / SUM(quantity) > 0.7)
INTO order_imbalance_sink
WITH (
    'order_book_stream.type' = 'kafka_source',
    'order_book_stream.config_file' = 'configs/order_book_source.yaml',
    'order_imbalance_sink.type' = 'kafka_sink',
    'order_imbalance_sink.config_file' = 'configs/order_imbalance_sink.yaml'
);

-- ====================================================================================
-- ARBITRAGE OPPORTUNITIES: Cross-exchange price discrepancy detection (FR-047)
-- ====================================================================================

CREATE STREAM arbitrage_opportunities_detection AS
SELECT 
    a.symbol,
    a.exchange as exchange_a,
    b.exchange as exchange_b,
    a.bid_price as bid_a,
    b.ask_price as ask_b,
    (a.bid_price - b.ask_price) as spread,
    (a.bid_price - b.ask_price) / b.ask_price * 10000 as spread_bps,
    LEAST(a.bid_size, b.ask_size) as available_volume,
    (a.bid_price - b.ask_price) * LEAST(a.bid_size, b.ask_size) as potential_profit,
    timestamp() as opportunity_time
FROM market_data_stream_a a
JOIN market_data_stream_b b ON a.symbol = b.symbol
WHERE a.bid_price > b.ask_price 
    AND (a.bid_price - b.ask_price) / b.ask_price * 10000 > 10
    AND LEAST(a.bid_size, b.ask_size) > 50000
INTO arbitrage_sink
WITH (
    'market_data_stream_a.type' = 'kafka_source',
    'market_data_stream_a.config_file' = 'configs/market_data_exchange_a_source.yaml',
    'market_data_stream_b.type' = 'kafka_source',
    'market_data_stream_b.config_file' = 'configs/market_data_exchange_b_source.yaml',
    'arbitrage_sink.type' = 'kafka_sink',
    'arbitrage_sink.config_file' = 'configs/arbitrage_opportunities_sink.yaml'
);