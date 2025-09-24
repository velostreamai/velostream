-- CTAS Trading Analytics - File-Based Data Sources
-- Uses CREATE TABLE AS SELECT to create analytical tables from CSV files
-- This demonstrates CTAS functionality with realistic trading data

-- ====================================================================================
-- MARKET DATA ANALYTICS TABLE
-- ====================================================================================
-- Creates a comprehensive market data analytics table from market data CSV file

CREATE TABLE market_data_analytics
AS SELECT
    symbol,
    exchange,
    price,
    volume,
    (ask_price - bid_price) as spread,
    (ask_price - bid_price) / price * 10000 as spread_bps,
    volume * price as notional_value,
    CASE
        WHEN volume > 100000 THEN 'HIGH'
        WHEN volume > 50000 THEN 'MEDIUM'
        ELSE 'LOW'
    END as volume_category,
    timestamp as market_timestamp
FROM market_data_stream
WHERE price > 0 AND volume > 0
WITH (
    "config_file" = "demo/trading/configs/file_market_data_source.yaml"
);

-- ====================================================================================
-- PORTFOLIO SUMMARY TABLE
-- ====================================================================================
-- Creates portfolio analytics from trading positions file

CREATE TABLE portfolio_summary
AS SELECT
    trader_id,
    COUNT(DISTINCT symbol) as num_positions,
    COUNT(DISTINCT sector) as num_sectors,
    SUM(position_size * avg_price) as gross_exposure,
    SUM(current_pnl) as total_pnl,
    SUM(ABS(position_size * avg_price)) as total_exposure,
    AVG(current_pnl) as avg_position_pnl,
    CASE
        WHEN SUM(current_pnl) > 50000 THEN 'TOP_PERFORMER'
        WHEN SUM(current_pnl) > 10000 THEN 'PROFITABLE'
        WHEN SUM(current_pnl) > -10000 THEN 'NEUTRAL'
        ELSE 'UNDERPERFORMING'
    END as performance_tier
FROM trading_positions_stream
GROUP BY trader_id
HAVING COUNT(*) > 0
WITH (
    "config_file" = "demo/trading/configs/file_positions_source.yaml"
);

-- ====================================================================================
-- RISK ANALYTICS TABLE
-- ====================================================================================
-- Creates comprehensive risk analytics from positions data

CREATE TABLE risk_analytics
AS SELECT
    trader_id,
    symbol,
    sector,
    position_size,
    current_pnl,
    ABS(position_size * avg_price) as position_exposure,
    current_pnl / ABS(position_size * avg_price) * 100 as return_pct,
    CASE
        WHEN ABS(position_size * avg_price) > 1000000 THEN 'LARGE_POSITION'
        WHEN ABS(position_size * avg_price) > 500000 THEN 'MEDIUM_POSITION'
        ELSE 'SMALL_POSITION'
    END as position_size_category,
    CASE
        WHEN current_pnl < -50000 THEN 'HIGH_LOSS'
        WHEN current_pnl < -10000 THEN 'MODERATE_LOSS'
        WHEN current_pnl > 50000 THEN 'HIGH_PROFIT'
        WHEN current_pnl > 10000 THEN 'MODERATE_PROFIT'
        ELSE 'NEUTRAL'
    END as pnl_category,
    sector || '_' || CASE
        WHEN ABS(position_size * avg_price) > 500000 THEN 'HIGH_EXPOSURE'
        ELSE 'NORMAL_EXPOSURE'
    END as risk_profile
FROM trading_positions_stream
WHERE position_size != 0
WITH (
    "config_file" = "demo/trading/configs/file_positions_source.yaml"
);

-- ====================================================================================
-- TRADING PERFORMANCE TABLE
-- ====================================================================================
-- Creates trading performance metrics from order history file

CREATE TABLE trading_performance
AS SELECT
    trader_id,
    symbol,
    DATE_TRUNC('day', timestamp) as trading_date,
    COUNT(*) as num_trades,
    SUM(quantity * price) as total_notional,
    AVG(price) as avg_execution_price,
    COUNT(DISTINCT symbol) as symbols_traded,
    SUM(CASE WHEN side = 'BUY' THEN quantity ELSE 0 END) as buy_quantity,
    SUM(CASE WHEN side = 'SELL' THEN quantity ELSE 0 END) as sell_quantity,
    SUM(commission) as total_commission,
    SUM(
        CASE WHEN side = 'SELL'
        THEN quantity * price - commission
        ELSE -(quantity * price + commission)
        END
    ) as trading_pnl
FROM order_history_stream
WHERE quantity > 0 AND price > 0
GROUP BY trader_id, symbol, DATE_TRUNC('day', timestamp)
HAVING COUNT(*) > 0
WITH (
    "config_file" = "demo/trading/configs/file_order_history_source.yaml"
);

-- ====================================================================================
-- SECTOR CONCENTRATION TABLE
-- ====================================================================================
-- Analyzes sector concentration risk across all positions

CREATE TABLE sector_concentration
AS SELECT
    sector,
    COUNT(DISTINCT trader_id) as num_traders,
    COUNT(DISTINCT symbol) as num_symbols,
    SUM(ABS(position_size * avg_price)) as sector_exposure,
    AVG(current_pnl) as avg_sector_pnl,
    SUM(current_pnl) as total_sector_pnl,
    SUM(current_pnl) / SUM(ABS(position_size * avg_price)) * 100 as sector_return_pct,
    STDDEV(current_pnl) as pnl_volatility,
    CASE
        WHEN SUM(ABS(position_size * avg_price)) > 10000000 THEN 'HIGH_CONCENTRATION'
        WHEN SUM(ABS(position_size * avg_price)) > 5000000 THEN 'MODERATE_CONCENTRATION'
        ELSE 'LOW_CONCENTRATION'
    END as concentration_risk
FROM trading_positions_stream
WHERE sector IS NOT NULL AND position_size != 0
GROUP BY sector
ORDER BY sector_exposure DESC
WITH (
    "config_file" = "demo/trading/configs/file_positions_source.yaml"
);

-- ====================================================================================
-- TOP MOVERS TABLE
-- ====================================================================================
-- Identifies top performing and worst performing stocks by price change

CREATE TABLE top_movers
AS SELECT
    symbol,
    exchange,
    price as current_price,
    volume,
    spread_bps,
    notional_value,
    volume_category,
    ROW_NUMBER() OVER (ORDER BY price DESC) as price_rank,
    PERCENT_RANK() OVER (ORDER BY volume) as volume_percentile,
    CASE
        WHEN volume_category = 'HIGH' AND spread_bps < 5 THEN 'LIQUID_ACTIVE'
        WHEN volume_category = 'HIGH' AND spread_bps >= 5 THEN 'ACTIVE_WIDE_SPREAD'
        WHEN volume_category = 'MEDIUM' THEN 'MODERATELY_LIQUID'
        ELSE 'LOW_LIQUIDITY'
    END as liquidity_profile
FROM market_data_analytics
WHERE volume > 0
ORDER BY notional_value DESC
LIMIT 50
WITH (
    "retention" = "7 days",
    "compression" = "snappy"
);

-- ====================================================================================
-- RISK MONITORING SUMMARY TABLE
-- ====================================================================================
-- Creates a comprehensive risk monitoring dashboard

CREATE TABLE risk_monitoring_summary
AS SELECT
    'PORTFOLIO_OVERVIEW' as metric_type,
    COUNT(DISTINCT trader_id) as total_traders,
    SUM(total_pnl) as portfolio_pnl,
    SUM(gross_exposure) as total_gross_exposure,
    SUM(total_exposure) as total_net_exposure,
    AVG(total_pnl) as avg_trader_pnl,
    COUNT(CASE WHEN performance_tier = 'TOP_PERFORMER' THEN 1 END) as top_performers,
    COUNT(CASE WHEN performance_tier = 'UNDERPERFORMING' THEN 1 END) as underperformers,
    SUM(total_pnl) / SUM(gross_exposure) * 100 as portfolio_roi_pct
FROM portfolio_summary

UNION ALL

SELECT
    'RISK_METRICS' as metric_type,
    COUNT(*) as total_positions,
    SUM(CASE WHEN position_size_category = 'LARGE_POSITION' THEN 1 ELSE 0 END) as large_positions,
    SUM(CASE WHEN pnl_category IN ('HIGH_LOSS', 'MODERATE_LOSS') THEN 1 ELSE 0 END) as losing_positions,
    SUM(CASE WHEN pnl_category IN ('HIGH_PROFIT', 'MODERATE_PROFIT') THEN 1 ELSE 0 END) as winning_positions,
    SUM(position_exposure) as total_position_exposure,
    AVG(return_pct) as avg_position_return,
    STDDEV(return_pct) as return_volatility,
    COUNT(DISTINCT sector) as num_sectors
FROM risk_analytics
WITH (
    "retention" = "30 days",
    "refresh_interval" = "1 hour"
);