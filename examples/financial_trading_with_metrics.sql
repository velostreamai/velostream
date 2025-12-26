-- Financial Trading Stream with SQL-Native Observability
-- Demonstrates FR-073 SQL-native metric annotations for real-time trading analytics
--
-- Metrics Defined:
-- 1. Volume spike detection counter
-- 2. Current price gauge
-- 3. Trade volume histogram
-- 4. High-value trade counter
--
-- Run with: velo-sql deploy-app --file examples/financial_trading_with_metrics.sql

-- ===========================================================================
-- Stream 1: Volume Spike Detection
-- ===========================================================================

-- @metric: velo_trading_volume_spikes_total
-- @metric_type: counter
-- @metric_help: "Number of volume spikes detected (>2x hourly average)"
-- @metric_labels: symbol, exchange
-- @metric_condition: volume > hourly_avg_volume * 2.0
CREATE STREAM volume_spike_alerts AS
SELECT
    symbol,
    exchange,
    volume,
    hourly_avg_volume,
    (volume / hourly_avg_volume) as spike_ratio,
    price,
    event_time
FROM market_data
WHERE volume > hourly_avg_volume * 2.0;

-- ===========================================================================
-- Stream 2: Real-Time Price Monitoring
-- ===========================================================================

-- Counter: Total trades processed
-- @metric: velo_trades_processed_total
-- @metric_type: counter
-- @metric_help: "Total trades processed by symbol and exchange"
-- @metric_labels: symbol, exchange

-- Gauge: Current trading price
-- @metric: velo_current_price_dollars
-- @metric_type: gauge
-- @metric_help: "Current trading price in dollars"
-- @metric_field: price
-- @metric_labels: symbol, exchange

-- Histogram: Trade volume distribution
-- @metric: velo_trade_volume_shares
-- @metric_type: histogram
-- @metric_help: "Distribution of trade volumes in shares"
-- @metric_field: volume
-- @metric_labels: symbol, exchange
-- @metric_buckets: 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000
CREATE STREAM price_monitoring AS
SELECT
    symbol,
    exchange,
    price,
    volume,
    bid_price,
    ask_price,
    spread,
    event_time
FROM market_data;

-- ===========================================================================
-- Stream 3: High-Value Trade Detection
-- ===========================================================================

-- @metric: velo_high_value_trades_total
-- @metric_type: counter
-- @metric_help: "Trades exceeding $1M in notional value"
-- @metric_labels: symbol, exchange, trader_tier
-- @metric_condition: notional_value > 1000000
CREATE STREAM high_value_trades AS
SELECT
    symbol,
    exchange,
    trader_id,
    CASE
        WHEN trader_volume_rank <= 10 THEN 'institutional'
        WHEN trader_volume_rank <= 100 THEN 'professional'
        ELSE 'retail'
    END as trader_tier,
    price,
    volume,
    (price * volume) as notional_value,
    event_time
FROM enriched_market_data
WHERE (price * volume) > 1000000;

-- ===========================================================================
-- Stream 4: Trading Latency Monitoring
-- ===========================================================================

-- @metric: velo_trade_latency_seconds
-- @metric_type: histogram
-- @metric_help: "Order-to-execution latency in seconds"
-- @metric_field: latency_seconds
-- @metric_labels: exchange, order_type
-- @metric_buckets: 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0
CREATE STREAM trading_latency AS
SELECT
    exchange,
    order_type,
    symbol,
    EXTRACT(EPOCH FROM (execution_time - order_time)) as latency_seconds,
    event_time
FROM trade_executions
WHERE execution_time IS NOT NULL
  AND order_time IS NOT NULL;

-- ===========================================================================
-- Stream 5: Market Maker Spread Monitoring
-- ===========================================================================

-- @metric: velo_bid_ask_spread_basis_points
-- @metric_type: gauge
-- @metric_help: "Current bid-ask spread in basis points"
-- @metric_field: spread_bps
-- @metric_labels: symbol, exchange
-- @metric_condition: spread_bps > 0
CREATE STREAM spread_monitoring AS
SELECT
    symbol,
    exchange,
    bid_price,
    ask_price,
    ((ask_price - bid_price) / bid_price * 10000) as spread_bps,
    event_time
FROM market_data
WHERE bid_price > 0 AND ask_price > 0;

-- ===========================================================================
-- Stream 6: Order Book Imbalance Detection
-- ===========================================================================

-- @metric: velo_order_imbalance_events_total
-- @metric_type: counter
-- @metric_help: "Order book imbalance events (>2:1 buy/sell ratio)"
-- @metric_labels: symbol, exchange, imbalance_direction
-- @metric_condition: imbalance_ratio > 2.0 OR imbalance_ratio < 0.5
CREATE STREAM order_imbalance_alerts AS
SELECT
    symbol,
    exchange,
    bid_volume,
    ask_volume,
    (bid_volume / NULLIF(ask_volume, 0)) as imbalance_ratio,
    CASE
        WHEN bid_volume > ask_volume * 2 THEN 'buy_side'
        WHEN ask_volume > bid_volume * 2 THEN 'sell_side'
        ELSE 'balanced'
    END as imbalance_direction,
    event_time
FROM order_book_snapshots
WHERE bid_volume > 0 AND ask_volume > 0;

-- ===========================================================================
-- Expected Prometheus Metrics Output
-- ===========================================================================

-- # HELP velo_trading_volume_spikes_total Number of volume spikes detected (>2x hourly average)
-- # TYPE velo_trading_volume_spikes_total counter
-- velo_trading_volume_spikes_total{symbol="AAPL",exchange="NASDAQ"} 42
-- velo_trading_volume_spikes_total{symbol="GOOGL",exchange="NASDAQ"} 15

-- # HELP velo_trades_processed_total Total trades processed by symbol and exchange
-- # TYPE velo_trades_processed_total counter
-- velo_trades_processed_total{symbol="AAPL",exchange="NASDAQ"} 150234

-- # HELP velo_current_price_dollars Current trading price in dollars
-- # TYPE velo_current_price_dollars gauge
-- velo_current_price_dollars{symbol="AAPL",exchange="NASDAQ"} 175.23

-- # HELP velo_trade_volume_shares Distribution of trade volumes in shares
-- # TYPE velo_trade_volume_shares histogram
-- velo_trade_volume_shares_bucket{symbol="AAPL",exchange="NASDAQ",le="100"} 450
-- velo_trade_volume_shares_bucket{symbol="AAPL",exchange="NASDAQ",le="500"} 1250
-- velo_trade_volume_shares_bucket{symbol="AAPL",exchange="NASDAQ",le="1000"} 2300
-- velo_trade_volume_shares_sum{symbol="AAPL",exchange="NASDAQ"} 15234567
-- velo_trade_volume_shares_count{symbol="AAPL",exchange="NASDAQ"} 5430

-- # HELP velo_high_value_trades_total Trades exceeding $1M in notional value
-- # TYPE velo_high_value_trades_total counter
-- velo_high_value_trades_total{symbol="AAPL",exchange="NASDAQ",trader_tier="institutional"} 127

-- # HELP velo_trade_latency_seconds Order-to-execution latency in seconds
-- # TYPE velo_trade_latency_seconds histogram
-- velo_trade_latency_seconds_bucket{exchange="NASDAQ",order_type="market",le="0.001"} 5430
-- velo_trade_latency_seconds_bucket{exchange="NASDAQ",order_type="market",le="0.005"} 7234

-- # HELP velo_bid_ask_spread_basis_points Current bid-ask spread in basis points
-- # TYPE velo_bid_ask_spread_basis_points gauge
-- velo_bid_ask_spread_basis_points{symbol="AAPL",exchange="NASDAQ"} 2.5

-- # HELP velo_order_imbalance_events_total Order book imbalance events (>2:1 buy/sell ratio)
-- # TYPE velo_order_imbalance_events_total counter
-- velo_order_imbalance_events_total{symbol="AAPL",exchange="NASDAQ",imbalance_direction="buy_side"} 23

-- ===========================================================================
-- Prometheus Alert Examples
-- ===========================================================================

-- Alert: High volume spike rate
-- rate(velo_trading_volume_spikes_total[5m]) > 10

-- Alert: Wide bid-ask spread
-- velo_bid_ask_spread_basis_points > 50

-- Alert: High trade latency (p99)
-- histogram_quantile(0.99, rate(velo_trade_latency_seconds_bucket[5m])) > 0.1

-- Alert: Sustained order book imbalance
-- rate(velo_order_imbalance_events_total{imbalance_direction="buy_side"}[5m]) > 5
