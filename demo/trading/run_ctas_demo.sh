#!/bin/bash

# CTAS File-Based Trading Demo
# Demonstrates CREATE TABLE AS SELECT functionality with CSV file data sources

echo "🏦 Velostream CTAS Trading Demo - File-Based Analytics"
echo "====================================================="

# Set working directory to project root
cd "$(dirname "$0")/../.."

# Check prerequisites
echo "📋 Step 1: Checking Prerequisites..."
if [[ ! -f "target/debug/test_ctas_integration" ]]; then
    echo "🔨 Building CTAS integration test..."
    cargo build --bin test_ctas_integration
fi

if [[ ! -f "demo/trading/data/market_data.csv" ]]; then
    echo "❌ Error: Market data CSV file not found!"
    exit 1
fi

echo "✅ Prerequisites checked"

# Display available data
echo ""
echo "📊 Step 2: Available Data Files"
echo "==============================="
echo "Market Data: $(tail -n +2 demo/trading/data/market_data.csv | wc -l) price records"
echo "Positions: $(tail -n +2 demo/trading/data/trading_positions.csv | wc -l) trader positions"
echo "Orders: $(tail -n +2 demo/trading/data/order_history.csv | wc -l) order records"

# Show sample data
echo ""
echo "🔍 Sample Market Data (first 3 rows):"
head -4 demo/trading/data/market_data.csv | column -t -s','

echo ""
echo "🔍 Sample Trading Positions (first 3 rows):"
head -4 demo/trading/data/trading_positions.csv | column -t -s','

# Test individual CTAS queries
echo ""
echo "🎯 Step 3: Testing CTAS Table Creation"
echo "======================================"

# Create a simple CTAS test for market data analytics
echo "Creating market_data_analytics table..."
cat > /tmp/ctas_market_test.sql << 'EOF'
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
    END as volume_category
FROM market_data_stream
WHERE price > 0 AND volume > 0
WITH (
    "config_file" = "demo/trading/configs/file_market_data_source.yaml"
);
EOF

# Test portfolio summary
echo "Creating portfolio_summary table..."
cat > /tmp/ctas_portfolio_test.sql << 'EOF'
CREATE TABLE portfolio_summary
AS SELECT
    trader_id,
    COUNT(DISTINCT symbol) as num_positions,
    COUNT(DISTINCT sector) as num_sectors,
    SUM(position_size * avg_price) as gross_exposure,
    SUM(current_pnl) as total_pnl,
    AVG(current_pnl) as avg_position_pnl
FROM trading_positions_stream
GROUP BY trader_id
HAVING COUNT(*) > 0
WITH (
    "config_file" = "demo/trading/configs/file_positions_source.yaml"
);
EOF

# Test risk analytics
echo "Creating risk_analytics table..."
cat > /tmp/ctas_risk_test.sql << 'EOF'
CREATE TABLE risk_analytics
AS SELECT
    trader_id,
    symbol,
    sector,
    position_size,
    current_pnl,
    ABS(position_size * avg_price) as position_exposure,
    CASE
        WHEN ABS(position_size * avg_price) > 1000000 THEN 'LARGE_POSITION'
        WHEN ABS(position_size * avg_price) > 500000 THEN 'MEDIUM_POSITION'
        ELSE 'SMALL_POSITION'
    END as position_size_category
FROM trading_positions_stream
WHERE position_size != 0
WITH (
    "config_file" = "demo/trading/configs/file_positions_source.yaml"
);
EOF

echo ""
echo "📈 Step 4: CTAS Query Analysis"
echo "=============================="
echo "The CTAS queries demonstrate:"
echo ""
echo "• Market Data Analytics:"
echo "  - Price and volume analysis"
echo "  - Spread calculations (bid-ask spreads in basis points)"
echo "  - Volume categorization (HIGH/MEDIUM/LOW)"
echo "  - Notional value calculations"
echo ""
echo "• Portfolio Summary:"
echo "  - Trader-level aggregations"
echo "  - Position counts and sector diversification"
echo "  - Gross exposure and P&L totals"
echo "  - Performance metrics"
echo ""
echo "• Risk Analytics:"
echo "  - Position-level risk assessment"
echo "  - Exposure categorization"
echo "  - Sector and trader risk profiling"
echo "  - Large position identification"
echo ""

# Show actual SQL from the main file
echo "🔍 Available CTAS Tables in ctas_file_trading.sql:"
grep -n "CREATE TABLE" demo/trading/sql/ctas_file_trading.sql | head -8

echo ""
echo "💡 To execute these CTAS queries:"
echo ""
echo "1. Start Kafka infrastructure:"
echo "   cd demo/trading"
echo "   docker-compose -f kafka-compose.yml up -d"
echo ""
echo "2. Use the CTAS executor with individual queries:"
echo "   cat /tmp/ctas_market_test.sql | ../../target/debug/test_ctas_integration"
echo ""
echo "3. Or use the full SQL file:"
echo "   cat demo/trading/sql/ctas_file_trading.sql | ../../target/debug/test_ctas_integration"
echo ""
echo "4. For production use, integrate with velo-sql server:"
echo "   ../../target/release/velo-sql --config demo/trading/configs/"
echo ""

# Display the content of the main SQL file for reference
echo "📄 Full CTAS SQL Available at: demo/trading/sql/ctas_file_trading.sql"
echo "   $(wc -l < demo/trading/sql/ctas_file_trading.sql) lines of SQL"
echo "   $(grep -c "CREATE TABLE" demo/trading/sql/ctas_file_trading.sql) table definitions"
echo "   $(grep -c "WITH" demo/trading/sql/ctas_file_trading.sql) configuration blocks"

echo ""
echo "🎉 CTAS File-Based Trading Demo Ready!"
echo "✅ CSV data files with realistic trading data"
echo "✅ YAML configuration files for file sources"
echo "✅ Comprehensive CTAS SQL queries"
echo "✅ Integration test framework available"
echo ""
echo "The demo showcases CREATE TABLE AS SELECT (CTAS) functionality"
echo "for creating analytical tables from CSV file data sources,"
echo "perfect for financial trading analytics and risk management."