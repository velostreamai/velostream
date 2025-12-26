# 🚀 Financial Trading Demo Summary

## What's Included

### ✅ Complete Demo Components

1. **Data Generation** (via `velo-test` and `@data.*` hints)
   - Generates realistic market data for 5 major stocks (AAPL, GOOG, MSFT, AMZN, META)
   - Uses `@data.*` annotations in SQL files for data generation configuration
   - Supports random_walk distribution for realistic price movements
   - Log-normal distribution for trading volume

2. **SQL Analytics** (`apps/app_market_data.sql`)
   - Real-time market data ingestion with event-time processing
   - 1-second OHLCV (Open-High-Low-Close-Volume) candles
   - Stream-table joins for instrument enrichment
   - Watermark-based late data handling

3. **Demo Script** (`start-demo.sh`)
   - Automated setup and execution with flexible options
   - Kafka topic creation and validation
   - SQL job deployment (streaming queries)
   - Data generation via `velo-test stress`
   - Real-time monitoring
   - Graceful cleanup
   - Quick start mode, interactive mode

4. **Visualization Dashboards**
   - Trading app business metrics (via Grafana)
   - Velostream runtime/telemetry (via Grafana)

5. **Documentation** (`README.md`)
   - Complete setup instructions
   - Configuration options
   - Troubleshooting guide
   - Advanced usage scenarios

## 🏃‍♂️ Quick Start

```bash
# 1. Run the demo
cd demo/trading
./start-demo.sh

# Quick 1-minute demo
./start-demo.sh -q

# Or use velo-test directly for testing
../../target/release/velo-test run apps/app_market_data.sql -y
```

## 📊 Generated Data Streams

- **in_market_data_stream**: Raw market data feed (source)
- **market_data_ts**: Event-time processed market data
- **tick_buckets**: 1-second OHLCV candles
- **enriched_market_data**: Market data with instrument metadata

## 🎯 Demo Highlights

- **SQL-Defined Data Generation**: Uses `@data.*` hints embedded in SQL files
- **Realistic Data**: Random walk for prices, log-normal for volume
- **Real-time Processing**: Sub-second latency stream processing
- **Complex Analytics**: Window functions, joins, aggregations
- **Visual Monitoring**: Interactive charts and alerts
- **Production Ready**: Dockerized, scalable architecture

## 🔧 Customization

### Data Generation (in SQL files)

Edit `@data.*` annotations in `apps/app_market_data.sql`:

```sql
-- @data.symbol: enum ["AAPL", "GOOG", "MSFT", "AMZN", "META"]
-- @data.price: range [150, 400], distribution: random_walk, volatility: 0.02
-- @data.volume: range [1000, 500000], distribution: log_normal
```

### Testing Individual Apps

```bash
# Validate SQL syntax
../../target/release/velo-test validate apps/app_market_data.sql

# Run with test data
../../target/release/velo-test run apps/app_market_data.sql -y

# Debug step-by-step
../../target/release/velo-test debug apps/app_market_data.sql

# Stress test with high volume
../../target/release/velo-test stress apps/app_market_data.sql --records 100000 --duration 60
```

Perfect for demonstrating Velostream capabilities in financial services!
