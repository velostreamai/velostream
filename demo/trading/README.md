# 🏦 Velostream Financial Trading Demo

A comprehensive demonstration of real-time financial trading analytics using Velostream, featuring market data processing, risk management, and arbitrage detection.

## 📋 Overview

This demo showcases:
- **Real-time market data generation** for 8 major tech stocks (AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, NFLX)
- **Live trading analytics** including price movement detection, volume spike analysis, and risk monitoring
- **Arbitrage opportunity detection** across multiple exchanges
- **Interactive visualization dashboard** with real-time charts and alerts
- **Comprehensive SQL-based stream processing** with complex joins and window functions
- **Self-contained demo** that references main project artifacts

## 📁 Demo Structure

```
demo/trading/
├── src/                         # Demo-specific source code
│   └── trading_data_generator.rs   # Market data generator
├── sql/                         # SQL applications
│   └── financial_trading.sql       # Trading analytics queries
├── monitoring/                  # Grafana + Prometheus config
├── *.sh                        # Demo scripts
├── Cargo.toml                  # Demo build configuration
├── Makefile                    # Build system
├── velo-cli -> ../../target/release/velo-cli  # Symlink to main CLI
└── README.md                   # This file

# References main project artifacts:
../../target/release/velo-sql-multi        # Multi-job SQL server (primary)
```

## 🚀 Quick Start

### Prerequisites

1. **Docker & Docker Compose** (for Kafka)
2. **Rust toolchain** (latest stable) - Install from https://rustup.rs/
3. **Python 3.8+** (optional, for visualization dashboard)

### Start the Demo (Single Command!)

```bash
cd demo/trading
./start-demo.sh
```

**That's it!** The script will automatically:
- ✅ Validate all prerequisites (Rust, Docker, ports)
- ✅ Build binaries if needed (first run takes ~5 minutes)
- ✅ Start Kafka and create necessary topics
- ✅ Deploy 8 streaming SQL queries
- ✅ Generate realistic trading data (default: 10 minutes)
- ✅ Display monitoring dashboard URLs

**Quick Options:**
```bash
# Quick 1-minute demo
./start-demo.sh -q
./start-demo.sh --quick

# Custom duration (in minutes)
./start-demo.sh 5

# Interactive/foreground mode
./start-demo.sh -i

# Release builds (optimized)
./start-demo.sh -r 30

# Setup dashboard environment
./start-demo.sh -d

# Show monitoring info early (before deployment)
./start-demo.sh -m

# Combine options
./start-demo.sh -q -d    # Quick demo + dashboard setup
./start-demo.sh -q -m    # Quick demo + early monitoring display

# Note: Monitoring info is ALWAYS displayed at the end automatically
```

**To stop the demo:**
```bash
./stop-demo.sh
```

**Get help:**
```bash
./start-demo.sh --help
```

### 2. Monitor with Velostream CLI

The CLI tool is automatically built by `start-demo.sh`. Use it to monitor all components:

```bash
# Check overall health
../../target/debug/velo-cli health

# Monitor in real-time (refreshes every 5 seconds)
../../target/debug/velo-cli status --refresh 5

# View Kafka topics and data
../../target/debug/velo-cli kafka --topics

# Monitor jobs and tasks
../../target/debug/velo-cli jobs
```

**Tip**: Create a symlink for convenience:
```bash
ln -sf ../../target/debug/velo-cli velo-cli
./velo-cli health
```

### 3. Access Grafana Dashboards

The demo **automatically starts Grafana and Prometheus** as part of the Docker Compose infrastructure. You can access them immediately once the demo is running:

```bash
# Access Grafana at http://localhost:3000
# Login: admin / admin

# Access Prometheus at http://localhost:9090

# Kafka UI at http://localhost:8090
```

**Available Dashboards:**
- **Velostream Trading Demo** - Real-time trading analytics, alerts, and market data
- **Velostream Overview** - System health, throughput, and resource usage
- **Kafka Metrics** - Broker performance, topic statistics, and consumer lag

> **Note**: Grafana, Prometheus, and Kafka UI are always available when running the demo. All URLs and dashboard info are **automatically displayed** at the end of startup. Use the `-m` flag if you want to see this info early (before deployment starts).

### 4. Launch the Python Visualization Dashboard

The dashboard can be set up automatically when starting the demo:

```bash
# Option 1: Setup dashboard when starting demo
./start-demo.sh -d

# Option 2: Manual setup (if needed)
python3 -m venv dashboard_env
source dashboard_env/bin/activate
pip install -r requirements.txt
```

Then in a separate terminal:

```bash
cd demo/trading

# Activate virtual environment and start dashboard
source dashboard_env/bin/activate
python3 dashboard.py
```

**Important:** You must activate the virtual environment each time you want to run the dashboard. Look for the `(dashboard_env)` prefix in your terminal prompt.

## 📊 What You'll See

### Generated Data Streams

1. **Market Data** (`market_data` topic)
   - Real-time price updates with bid/ask spreads
   - Volume data with occasional spikes
   - Multiple exchanges (NYSE, NASDAQ, BATS)

2. **Trading Positions** (`trading_positions` topic)
   - 20 simulated traders with various positions
   - Real-time P&L calculations
   - Position sizes and average prices

3. **Order Book Updates** (`order_book_updates` topic)
   - Buy/sell orders with prices and quantities
   - Market and limit order types
   - Order flow imbalances

### Real-time Analytics

The demo runs 5 sophisticated SQL jobs:

#### 1. Price Movement Detection
```sql
-- Detects significant price movements (>5%)
-- Triggers: price_alerts topic
```

#### 2. Volume Spike Analysis
```sql
-- Identifies volume spikes (3x normal)
-- Uses: 20-period moving average
-- Triggers: volume_spikes topic
```

#### 3. Risk Management Monitor
```sql
-- Monitors trader positions and P&L
-- Checks: position limits, daily loss limits
-- Triggers: risk_alerts topic
```

#### 4. Order Flow Imbalance Detection
```sql
-- Detects buy/sell imbalances (>70%)
-- Time window: 1 minute
-- Triggers: order_imbalance_alerts topic
```

#### 5. Arbitrage Opportunities
```sql
-- Finds price differences across exchanges
-- Minimum spread: 10 basis points
-- Triggers: arbitrage_opportunities topic
```

### Visualization Dashboard

The Python dashboard provides:
- **Real-time price charts** for all 8 stocks
- **Volume tracking** with spike detection
- **Live alerts panel** showing the latest trading signals
- **Market statistics** with current prices and spreads

## 🔧 Configuration

### Environment Variables

```bash
# Kafka configuration
export KAFKA_BROKERS="localhost:9092"

# Demo duration (minutes)
export DEMO_DURATION=10

# SQL server port
export SQL_SERVER_PORT=8080
```

### Customizing the Demo

#### Modify Stock Universe
Edit `src/trading_data_generator.rs` lines 135-144:

```rust
let stock_configs = vec![
    ("AAPL", 175.0, 0.25),  // (symbol, price, volatility)
    ("GOOGL", 140.0, 0.30),
    // Add more stocks...
];
```

#### Adjust Alert Thresholds
Edit `sql/financial_trading.sql`:

```sql
-- Price movement threshold (currently 5%)
WHERE ABS(price_change_pct) > 5.0

-- Volume spike multiplier (currently 3x)
WHERE volume > 3 * avg_volume_20

-- Risk limits
WHEN ABS(position_size * price) > 1000000 THEN 'POSITION_LIMIT_EXCEEDED'
```

#### Change Data Generation Frequency
In `trading_data_generator.rs` main loop (lines 300-318):

```rust
// Generate market data every iteration (1 second)
self.generate_market_data().await?;

// Generate positions every 5 seconds
if iteration % 5 == 0 {
    self.generate_trading_positions().await?;
}
```

## 📈 Understanding the Data

### Market Data Structure
```json
{
  "symbol": "AAPL",
  "exchange": "NYSE",
  "price": 175.23,
  "bid_price": 175.21,
  "ask_price": 175.25,
  "bid_size": 5000,
  "ask_size": 3000,
  "volume": 125000,
  "timestamp": 1699123456789
}
```

### Trading Position Structure
```json
{
  "trader_id": "TRADER_001",
  "symbol": "AAPL",
  "position_size": 1000,
  "avg_price": 174.50,
  "current_pnl": 730.00,
  "timestamp": 1699123456789
}
```

### Alert Structure
```json
{
  "symbol": "AAPL",
  "price_change_pct": 5.2,
  "price": 175.23,
  "prev_price": 166.45,
  "detection_time": 1699123456789
}
```

## 🐛 Troubleshooting

### Common Issues & Solutions

#### ❌ "Rust/Cargo is not installed"
```bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

#### ❌ "Docker is not running"
```bash
# macOS/Windows: Start Docker Desktop application
# Linux: Start Docker daemon
sudo systemctl start docker
```

#### ❌ "Port already in use" (9092, 3000, 9090, etc.)
```bash
# Find what's using the port
lsof -i :9092

# Stop the conflicting service, then run:
./stop-demo.sh
./start-demo.sh
```

#### ❌ "Build failed" or compilation errors
```bash
# Update Rust to latest stable
rustup update stable

# Clean and rebuild
./stop-demo.sh
cd ../..
cargo clean
cd demo/trading
./start-demo.sh
```

#### ❌ Kafka Connection Errors
```bash
# Check if Kafka is running
docker ps | grep kafka

# Restart Kafka infrastructure
docker-compose -f kafka-compose.yml restart
```

#### ❌ "0 records processed" - Jobs not processing data
```bash
# Check if data generator is running
ps aux | grep trading_data_generator

# Verify data in topics
docker exec simple-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market_data_stream \
  --from-beginning --max-messages 5

# Restart with clean state
./stop-demo.sh
./start-demo.sh
```

#### ❌ Python Dashboard Issues
```bash
# Setup dashboard environment
./start-demo.sh -d

# If still having issues, recreate environment
rm -rf dashboard_env
python3 -m venv dashboard_env
source dashboard_env/bin/activate
pip install -r requirements.txt
python3 dashboard.py
```

#### ❌ Out of disk space
```bash
# Clean up Docker volumes
docker system prune -a --volumes

# Clean Rust build artifacts
cargo clean
```

### Getting Help

**Check logs:**
```bash
tail -f /tmp/velo_deployment.log      # SQL job logs
tail -f /tmp/trading_generator.log    # Data generator logs
docker-compose -f kafka-compose.yml logs kafka  # Kafka logs
```

**Health check:**
```bash
./check-demo-health.sh
```

**Complete reset:**
```bash
./stop-demo.sh
docker-compose -f kafka-compose.yml down -v
./start-demo.sh
```

### Performance Tips

1. **For High-Frequency Testing**: Reduce sleep interval in data generator
2. **For Low-Resource Systems**: Decrease number of traders or stocks
3. **For Extended Runs**: Increase Kafka retention settings

## 📊 Monitoring & Observability

### Grafana & Prometheus (Always Available)

The demo **automatically starts** comprehensive monitoring infrastructure via Docker Compose. Access it anytime during your demo:

**🔗 Access Points:**
- **Grafana**: http://localhost:3000 (login: `admin` / `admin`)
- **Prometheus**: http://localhost:9090
- **Kafka UI**: http://localhost:8090

**💡 Tip**: All monitoring endpoints are displayed automatically at the end of startup. Use `./start-demo.sh -m` to see them earlier (before deployment starts).

**🏦 Velostream Trading Demo Dashboard:**
- Real-time trading alerts and price movements
- Volume spike detection and analysis
- Risk management metrics
- Arbitrage opportunity tracking
- SQL query performance monitoring

**📈 Velostream Overview Dashboard:**
- System health and component status
- Stream processing throughput
- Memory and CPU usage
- Kafka cluster health metrics

**🔗 Kafka Metrics Dashboard:**
- Topic throughput and message rates
- Consumer group lag monitoring
- Partition and replica statistics
- Disk usage and log size tracking

### Velostream CLI Tool

The `velo-cli` is automatically built when you run `start-demo.sh` and provides comprehensive monitoring:

```bash
# Quick health check of all components
../../target/debug/velo-cli health

# Detailed status with verbose output
../../target/debug/velo-cli status --verbose

# Real-time monitoring (refresh every 5 seconds)
../../target/debug/velo-cli status --refresh 5

# Check Kafka cluster and topics
../../target/debug/velo-cli kafka --topics --groups

# Monitor Docker containers
../../target/debug/velo-cli docker --velo-only

# View Velostream processes
../../target/debug/velo-cli processes

# Monitor active jobs and streaming tasks
../../target/debug/velo-cli jobs

# Check specific job types
../../target/debug/velo-cli jobs --sql          # SQL processing jobs
../../target/debug/velo-cli jobs --generators   # Data generators
../../target/debug/velo-cli jobs --topics       # Topic activity & message counts

# Get help for any command
../../target/debug/velo-cli --help
```

**Tip**: Create a local symlink for shorter commands:
```bash
ln -sf ../../target/debug/velo-cli velo-cli
./velo-cli health  # Much easier!
```

### Kafka Topic Monitoring
```bash
# List all topics  
docker exec $(docker-compose -f kafka-compose.yml ps -q kafka) kafka-topics --list --bootstrap-server localhost:9092

# Check topic details
docker exec $(docker-compose -f kafka-compose.yml ps -q kafka) kafka-topics --describe --topic market_data --bootstrap-server localhost:9092

# Monitor message flow
docker exec $(docker-compose -f kafka-compose.yml ps -q kafka) kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups
```

### SQL Job Monitoring
```bash
# Multi-job server is completely app-agnostic - no hardcoded jobs
# Trading demo deploys financial trading analytics via deploy-app command
# Check server logs for job status
../../target/release/velo-sql-multi --help

# HTTP endpoints for job monitoring:
# curl http://localhost:8080/jobs
# curl http://localhost:8080/stats  
# curl http://localhost:8080/health
```

## 🎯 Demo Scenarios

### Scenario 1: Market Volatility Event
1. Watch for price movement alerts when volatility spikes
2. Observe volume increases accompanying price movements
3. Monitor risk alerts as positions become more volatile

### Scenario 2: Arbitrage Detection
1. Look for price differences between NYSE and NASDAQ
2. Identify arbitrage opportunities in the alerts
3. Notice spread variations across exchanges

### Scenario 3: Risk Management
1. Monitor trader positions approaching limits
2. Watch for daily loss limit breaches
3. Observe risk status changes in real-time

## 🔧 Advanced Usage

### Custom SQL Jobs
Add your own analytics by creating new SQL files:

```sql
-- my_custom_analysis.sql
START JOB my_analysis AS
SELECT 
    symbol,
    AVG(price) as avg_price,
    COUNT(*) as trade_count
FROM market_data
WHERE timestamp >= timestamp() - INTERVAL '5' MINUTE
GROUP BY symbol
WITH ('output.topic' = 'my_analysis_results');
```

Load with:
```bash
curl -X POST "http://localhost:8080/sql" \
    -H "Content-Type: text/plain" \
    -d "$(cat my_custom_analysis.sql)"
```

### Production Deployment
For production use, see:
- `DEPLOYMENT_SUMMARY.md` - Docker deployment guide
- `k8s/` directory - Kubernetes configurations
- `monitoring/` directory - Prometheus and Grafana setup

## 📚 Related Documentation

- [SQL Application Guide](../../SQL_APPLICATION_GUIDE.md)
- [Kafka Configuration](../../docs/developer/STREAMING_KAFKA_API.md)
- [Performance Optimization](../../docs/developer/ADVANCED_PERFORMANCE_OPTIMIZATIONS.md)
- [Docker Deployment](../../docs/DOCKER_DEPLOYMENT_GUIDE.md)

## 🤝 Contributing

Found an issue or want to enhance the demo? Please:
1. Check existing issues in the main repository
2. Create detailed bug reports with steps to reproduce
3. Submit pull requests with improvements

---

🎉 **Enjoy exploring real-time financial analytics with Velostream!**