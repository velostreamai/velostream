# 🏦 Velostream Financial Trading Demo

A comprehensive demonstration of real-time financial trading analytics using Velostream, featuring market data processing, risk management, and arbitrage detection.

## 📋 Overview

This demo showcases:
- **Real-time market data generation** using `@data.*` hints embedded in SQL files
- **Live trading analytics** including OHLCV candles and instrument enrichment
- **SQL-based stream processing** with window functions, joins, and watermarks
- **Test harness integration** with `velo-test` for testing, debugging, and stress testing

## 📁 Demo Structure

```
demo/trading/
├── apps/                        # SOURCE: hand-written SQL with @data.* and @metric hints
│   ├── app_market_data.sql
│   ├── app_compliance.sql
│   └── ...
├── deploy/                      # GENERATED: output of velo-dashboard-generate.sh (.gitignored)
│   ├── apps/                    #   Annotated SQL (deployed by start-demo.sh)
│   └── monitoring/              #   Generated dashboards + prometheus.yml
├── monitoring/                  # CURATED: hand-written monitoring configs
│   ├── grafana/
│   │   ├── provisioning/        #   Grafana provisioning (datasources, dashboard providers)
│   │   └── dashboards/          #   Hand-curated dashboards (overview, ops, tracing, etc.)
│   └── tempo/                   #   Distributed tracing config
├── schemas/                     # Schema definitions (optional, @data.* hints preferred)
├── configs/                     # Kafka source/sink configs
├── tests/                       # Test specifications
├── *.sh                         # Demo scripts
├── Makefile                     # Build system
└── README.md                    # This file

# Binaries (from PATH, target/release/, or release archive bin/):
# velo-test   - Test harness (data generation, testing, debugging)
# velo-sql    - Multi-job SQL server
```

**Source vs Generated:** `apps/` contains the hand-written SQL source files. Running
`./velo-dashboard-generate.sh` produces `deploy/` which contains annotated SQL
(with deployment/observability annotations injected) and generated monitoring
configs (per-app Grafana dashboards, combined prometheus.yml). The `deploy/`
directory is `.gitignored` — regenerate it after editing `apps/*.sql`.

## 🚀 Quick Start

### Prerequisites

1. **Docker & Docker Compose** (for Kafka)
2. **Rust toolchain** (only if building from source) - Install from https://rustup.rs/

### Start the Demo (Single Command!)

```bash
cd demo/trading
./start-demo.sh
```

**That's it!** The script will automatically:
- ✅ Validate all prerequisites (Rust, Docker, ports)
- ✅ Build binaries if needed (first run takes ~5 minutes)
- ✅ Generate `deploy/` artifacts if missing (dashboards, annotated SQL, prometheus config)
- ✅ Start Kafka and create necessary topics
- ✅ Deploy annotated SQL from `deploy/apps/`
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

# Show monitoring info early (before deployment)
./start-demo.sh -m

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

Use the CLI tool to monitor all components:

```bash
# Check overall health
velo-cli health

# Monitor in real-time (refreshes every 5 seconds)
velo-cli status --refresh 5

# View Kafka topics and data
velo-cli kafka --topics

# Monitor jobs and tasks
velo-cli jobs
```

> **PATH setup**: Run `source ../../setup-env.sh` (release archive) or `export PATH="$PATH:../../target/release"` (built from source) to use bare command names.

### 3. Access Grafana Dashboards

The demo **automatically starts Grafana and Prometheus** as part of the Docker Compose infrastructure. You can access them immediately once the demo is running:

```bash
# Access Grafana at http://localhost:3000
# Login: admin / admin

# Access Prometheus at http://localhost:9090

# Kafka UI at http://localhost:8090
```

**Available Dashboards:**

Dashboards are organized into two Grafana folders:
- **Velostream** (curated) — hand-written dashboards checked into `monitoring/grafana/dashboards/`:
  - Velostream Overview, Velostream Ops, Kafka Metrics, Distributed Tracing, Error Tracking
- **Velostream - Generated** — per-app dashboards produced by `./velo-dashboard-generate.sh`:
  - One dashboard per SQL app (compliance, market data, price analytics, risk, trading signals)

> **Note**: Grafana, Prometheus, and Kafka UI are always available when running the demo. All URLs and dashboard info are **automatically displayed** at the end of startup. Use the `-m` flag if you want to see this info early (before deployment starts).
>
> If generated dashboards are missing, run `./velo-dashboard-generate.sh` to populate `deploy/`.

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

#### Modify Stock Universe and Data Generation
Edit `@data.*` hints in `apps/app_market_data.sql`:

```sql
-- @data.symbol.type: string
-- @data.symbol: enum ["AAPL", "GOOG", "MSFT", "AMZN", "META"], weights: [0.25, 0.25, 0.2, 0.15, 0.15]

-- @data.price.type: decimal(4)
-- @data.price: range [150, 400], distribution: random_walk, volatility: 0.02, drift: 0.0001, group_by: symbol

-- @data.volume.type: integer
-- @data.volume: range [1000, 500000], distribution: log_normal
```

#### Adjust Record Count and Duration
Edit global hints in the SQL file:

```sql
-- @data.record_count: 10000
-- @data.time_simulation: sequential
-- @data.time_start: "-1h"
-- @data.time_end: "now"
```

#### Add New Stocks
Simply add to the enum list:

```sql
-- @data.symbol: enum ["AAPL", "GOOG", "MSFT", "AMZN", "META", "TSLA", "NVDA"]
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
docker-compose -f docker-compose.yml restart
```

#### ❌ "0 records processed" - Jobs not processing data
```bash
# Check if data generator is running
ps aux | grep velo-test

# Verify data in topics
docker exec simple-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic in_market_data_stream \
  --from-beginning --max-messages 5

# Restart with clean state
./stop-demo.sh
./start-demo.sh
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
tail -f /tmp/velo_stress.log          # Data generator logs (velo-test stress)
docker-compose -f docker-compose.yml logs kafka  # Kafka logs
```

**Health check:**
```bash
./check-demo-health.sh
```

**Complete reset:**
```bash
./stop-demo.sh
docker-compose -f docker-compose.yml down -v
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

**🔍 Velostream Telemetry & Tracing Dashboard (NEW!):**
- **Deserialization latency** (avg, p95, p99)
- **SQL processing latency** (p50, p95, p99)
- **Serialization latency** (avg, p95, p99)
- **Pipeline throughput** (records/sec for each stage)
- **Error rates** and success metrics
- **Pipeline time distribution** (where time is spent)
- **Cumulative record counts**
- Real-time visualization of the complete data pipeline

> 📊 **Access**: Open Grafana (http://localhost:3000) → Search for "Velostream Telemetry & Tracing"

### Velostream CLI Tool

The `velo-cli` provides comprehensive monitoring:

```bash
# Quick health check of all components
velo-cli health

# Detailed status with verbose output
velo-cli status --verbose

# Real-time monitoring (refresh every 5 seconds)
velo-cli status --refresh 5

# Check Kafka cluster and topics
velo-cli kafka --topics --groups

# Monitor Docker containers
velo-cli docker --velo-only

# View Velostream processes
velo-cli processes

# Monitor active jobs and streaming tasks
velo-cli jobs

# Check specific job types
velo-cli jobs --sql          # SQL processing jobs
velo-cli jobs --generators   # Data generators
velo-cli jobs --topics       # Topic activity & message counts

# Get help for any command
velo-cli --help
```

### Kafka Topic Monitoring
```bash
# List all topics  
docker exec $(docker-compose -f docker-compose.yml ps -q kafka) kafka-topics --list --bootstrap-server localhost:9092

# Check topic details
docker exec $(docker-compose -f docker-compose.yml ps -q kafka) kafka-topics --describe --topic market_data --bootstrap-server localhost:9092

# Monitor message flow
docker exec $(docker-compose -f docker-compose.yml ps -q kafka) kafka-consumer-groups --bootstrap-server localhost:9092 --describe --all-groups
```

### SQL Job Monitoring
```bash
# Multi-job server is completely app-agnostic - no hardcoded jobs
# Trading demo deploys financial trading analytics via deploy-app command
# Check server logs for job status
velo-sql --help

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
- `monitoring/` directory - Hand-curated Prometheus, Grafana, and Tempo configs
- `deploy/` directory - Generated artifacts (run `./velo-dashboard-generate.sh` first)

## 🎲 Data Generation Options

The trading demo supports **two data generation approaches** for different use cases:

### Comparison: Test Harness vs Data Generator

| Aspect | Test Harness | Trading Data Generator |
|--------|--------------|------------------------|
| **Primary Use** | Automated testing & CI/CD | Live demos & presentations |
| **Price Simulation** | Random within range | Geometric Brownian Motion (realistic) |
| **Correlations** | Independent random values | Correlated bid/ask, volume/volatility |
| **Event Patterns** | Stateless (each record independent) | Stateful (price follows previous) |
| **Volatility Spikes** | Random probability | Realistic event-driven |
| **Setup Required** | Just `velo-test` binary | Rust build + Docker Compose |
| **Best For** | SQL validation, CI pipelines | Stakeholder demos, dashboard testing |

### Option 1: Test Harness (Recommended for Testing)

Use the test harness when you want to:
- Validate SQL syntax without running Kafka
- Run automated tests in CI/CD pipelines
- Generate quick test data for specific queries
- Assert output schemas and record counts

```bash
# Navigate to trading demo
cd demo/trading

# List available apps
./velo-test.sh

# Validate SQL syntax (no Docker required)
./velo-test.sh validate

# Run a specific app test (requires Docker for testcontainers)
./velo-test.sh app_market_data

# Run with step-through debugging
./velo-test.sh app_market_data --step

# Run specific query within an app
./velo-test.sh app_market_data --query market_data_ts

# Keep containers running for debugging
./velo-test.sh app_market_data --keep

# Run all apps
./velo-test.sh all
```

**Schemas are already provided** in `schemas/`:
- `market_data.schema.yaml` / `in_market_data_stream.schema.yaml` - Market data events
- `trading_positions.schema.yaml` - Position updates
- `order_book.schema.yaml` - Order book events

**Test specs** in `tests/`:
- `app_market_data.test.yaml` - Assertions for market data pipeline
- `app_price_analytics.test.yaml` - Assertions for price analytics
- `app_risk.test.yaml` - Assertions for risk monitoring
- `app_trading_signals.test.yaml` - Assertions for trading signals

### Option 2: Trading Data Generator (Recommended for Demos)

Use `velo-test stress` for data generation when you want to:
- Show realistic price movements on dashboards
- Demonstrate to stakeholders with believable data
- Run extended demos (10+ minutes)
- See correlated market behaviors

```bash
# Start full demo (includes data generation via velo-test)
./start-demo.sh

# Or run data generation manually after starting Kafka
velo-test stress apps/app_market_data.sql \
  --records 60000 --duration 600 --kafka localhost:9092 -y
```

**Data generation features (via @data.* hints):**
- **Random Walk Distribution** for realistic price paths
- **5 major tech stocks** with configurable weights
- **Log-normal volume** distribution for realistic trading activity
- **Sequential timestamps** for time-series analysis

## 🧪 Testing with Test Harness

The test harness validates SQL applications with automated testing. No Kafka infrastructure required for validation.

### Quick Start

```bash
# Validate SQL syntax (no Docker/Kafka required)
./velo-test.sh validate

# Run full test suite (requires Docker for Kafka testcontainers)
./velo-test.sh

# Quick smoke test with minimal data
./velo-test.sh smoke

# High-volume stress test
./velo-test.sh stress
```

### Test Artifacts

| File | Description |
|------|-------------|
| `apps/app_market_data.sql` | SQL with embedded `@data.*` hints for data generation |
| `test_spec.yaml` | Test specification with assertions |
| `schemas/*.schema.yaml` | Optional schema files (overrides `@data.*` hints) |
| `velo-test.sh` | Test runner script |

**Note:** Data generation hints can be embedded directly in SQL files using `@data.*` annotations. Schema YAML files are optional and take precedence when both exist.

### Running Specific Tests

```bash
# Test a specific query
./velo-test.sh run --query market_data_ts

# Output in JUnit XML format (for CI/CD)
./velo-test.sh run --output junit > results.xml

# Output in JSON format
./velo-test.sh run --output json > results.json
```

### AI-Assisted Schema Generation

If you need to regenerate schemas or create new test specs:

```bash
# Set your Anthropic API key
export ANTHROPIC_API_KEY="your-key-here"

# Generate schemas with AI assistance
velo-test infer-schema sql/financial_trading.sql --ai --output schemas/

# Generate test spec from SQL analysis
velo-test init sql/financial_trading.sql --ai --output test_spec.yaml
```

See [TESTING.md](TESTING.md) for detailed testing documentation.

## 📚 Related Documentation

- [SQL Application Guide](../../SQL_APPLICATION_GUIDE.md)
- [Kafka Configuration](../../docs/developer/STREAMING_KAFKA_API.md)
- [Performance Optimization](../../docs/developer/ADVANCED_PERFORMANCE_OPTIMIZATIONS.md)
- [Docker Deployment](../../docs/DOCKER_DEPLOYMENT_GUIDE.md)
- [FR-084 Test Harness](../../docs/feature/FR-084-app-test-harness/README.md)

## 🤝 Contributing

Found an issue or want to enhance the demo? Please:
1. Check existing issues in the main repository
2. Create detailed bug reports with steps to reproduce
3. Submit pull requests with improvements

---

🎉 **Enjoy exploring real-time financial analytics with Velostream!**