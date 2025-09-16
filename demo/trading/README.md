# 🏦 VeloStream Financial Trading Demo

A comprehensive demonstration of real-time financial trading analytics using VeloStream, featuring market data processing, risk management, and arbitrage detection.

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
2. **Rust toolchain** (latest stable)
3. **Python 3.8+** (for visualization dashboard)

### 1. Build the Project (First Time)

```bash
cd demo/trading

# Option 1: Use Makefile (recommended)
make build

# Option 2: Use build script
./build_cli.sh
```

This will build the main VeloStream project and create symlinks for easy access.

### 2. Start the Demo

```bash
./run_demo.sh
```

The script will:
- Check/build binaries if needed
- Start Kafka and create necessary topics
- Launch the multi-job SQL server with 5 trading analytics jobs
- Generate realistic trading data for 5 minutes
- Display sample alerts and data

**Quick Options:**
```bash
make run           # Build and run full demo
make run-short     # Build and run 1-minute demo
DEMO_DURATION=1 ./run_demo.sh  # Custom duration
```

**To stop the demo:**
```bash
./stop_demo.sh
```

### 2. Monitor with VeloStream CLI

Build and use the CLI tool to monitor all components:

```bash
# Build the CLI tool (creates convenient ./velo-cli symlink)
./build_cli.sh

# Check overall health
./velo-cli health

# Monitor in real-time (refreshes every 5 seconds)
./velo-cli status --refresh 5

# View Kafka topics and data
./velo-cli kafka --topics

# Monitor jobs and tasks (NEW!)
./velo-cli jobs
```

### 3. Access Grafana Dashboards

The demo automatically starts Grafana with pre-configured dashboards:

```bash
# Access Grafana at http://localhost:3000
# Login: admin / admin
```

**Available Dashboards:**
- **VeloStream Trading Demo** - Real-time trading analytics, alerts, and market data
- **VeloStream Overview** - System health, throughput, and resource usage  
- **Kafka Metrics** - Broker performance, topic statistics, and consumer lag

### 4. Launch the Python Visualization Dashboard

In a separate terminal:

```bash
cd demo/trading

# Setup virtual environment and install dependencies
./setup_dashboard.sh

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

### Common Issues

#### Kafka Connection Errors
```bash
# Check if Kafka is running
docker-compose ps kafka

# Restart Kafka
docker-compose restart kafka zookeeper
```

#### SQL Server Not Starting
```bash
# Check port availability
netstat -an | grep 8080

# Use different port
export SQL_SERVER_PORT=8081
```

#### Python Dependencies
```bash
# The setup script creates a virtual environment automatically
./setup_dashboard.sh

# If you need to manually recreate the environment:
rm -rf dashboard_env
python3 -m venv dashboard_env
source dashboard_env/bin/activate
pip install -r requirements.txt

# Or use conda (alternative)
conda create -n trading_dashboard python=3.9
conda activate trading_dashboard
pip install -r requirements.txt
```

#### Dashboard Not Updating
- **Check virtual environment**: Ensure you activated it with `source dashboard_env/bin/activate`
- **Dependencies missing**: Run `./setup_dashboard.sh` to install/update dependencies
- **No data**: Ensure Kafka topics have data: `docker exec $(docker-compose -f kafka-compose.yml ps -q kafka) kafka-console-consumer --bootstrap-server localhost:9092 --topic market_data --max-messages 5`
- **Connection issues**: Verify dashboard is connecting to correct Kafka brokers
- **Font cache**: First run may take 30-60 seconds while matplotlib builds font cache

### Performance Tips

1. **For High-Frequency Testing**: Reduce sleep interval in data generator
2. **For Low-Resource Systems**: Decrease number of traders or stocks
3. **For Extended Runs**: Increase Kafka retention settings

## 📊 Monitoring & Observability

### Grafana Dashboards

The demo includes comprehensive Grafana dashboards accessible at **http://localhost:3000** (admin/admin):

**🏦 VeloStream Trading Demo Dashboard:**
- Real-time trading alerts and price movements
- Volume spike detection and analysis
- Risk management metrics
- Arbitrage opportunity tracking
- SQL query performance monitoring

**📈 VeloStream Overview Dashboard:**
- System health and component status
- Stream processing throughput
- Memory and CPU usage
- Kafka cluster health metrics

**🔗 Kafka Metrics Dashboard:**
- Topic throughput and message rates
- Consumer group lag monitoring  
- Partition and replica statistics
- Disk usage and log size tracking

### VeloStream CLI Tool

The `velo-cli` provides comprehensive monitoring and management capabilities:

```bash
# Build the CLI tool first (only needed once)
./build_cli.sh

# Quick health check of all components
./velo-cli health

# Detailed status with verbose output
./velo-cli status --verbose

# Real-time monitoring (refresh every 5 seconds)
./velo-cli status --refresh 5

# Check Kafka cluster and topics
./velo-cli kafka --topics --groups

# Monitor Docker containers
./velo-cli docker --velo-only

# View VeloStream processes
./velo-cli processes

# Monitor active jobs and streaming tasks
./velo-cli jobs

# Check specific job types
./velo-cli jobs --sql          # SQL processing jobs
./velo-cli jobs --generators   # Data generators
./velo-cli jobs --topics       # Topic activity & message counts

# Get help for any command
./velo-cli --help
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

🎉 **Enjoy exploring real-time financial analytics with VeloStream!**