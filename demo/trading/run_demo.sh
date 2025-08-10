#!/bin/bash

# Financial Trading Demo Script
# Demonstrates real-time trading analytics with FerrisStreams

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
DEMO_DURATION="${DEMO_DURATION:-5}" # minutes
SQL_SERVER_PORT="${SQL_SERVER_PORT:-8080}"

echo -e "${BLUE}🏦 FerrisStreams Financial Trading Demo${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""

# Function to check if a service is running
check_service() {
    local service_name=$1
    local port=$2
    local host=${3:-localhost}
    
    if nc -z "$host" "$port" 2>/dev/null; then
        echo -e "${GREEN}✓ $service_name is running on $host:$port${NC}"
        return 0
    else
        echo -e "${RED}✗ $service_name is not running on $host:$port${NC}"
        return 1
    fi
}

# Function to wait for service
wait_for_service() {
    local service_name=$1
    local port=$2
    local host=${3:-localhost}
    local max_attempts=${4:-30}
    
    echo -e "${YELLOW}⏳ Waiting for $service_name to start...${NC}"
    
    for i in $(seq 1 $max_attempts); do
        if nc -z "$host" "$port" 2>/dev/null; then
            echo -e "${GREEN}✓ $service_name is ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
    done
    
    echo -e "${RED}✗ $service_name failed to start within ${max_attempts} attempts${NC}"
    return 1
}

# Check prerequisites
echo -e "${YELLOW}🔍 Checking prerequisites...${NC}"

# Check if Kafka is running
if ! check_service "Kafka" 9092; then
    echo -e "${YELLOW}Starting Kafka with Docker Compose...${NC}"
    docker-compose -f kafka-compose.yml up -d
    wait_for_service "Kafka" 9092
fi

# Check if Grafana is running
if ! check_service "Grafana" 3000; then
    echo -e "${YELLOW}Starting Grafana Dashboard...${NC}"
    # Grafana should already be started with docker-compose above
    wait_for_service "Grafana" 3000
fi

# Check if Prometheus is running
if ! check_service "Prometheus" 9090; then
    echo -e "${YELLOW}Starting Prometheus...${NC}"
    # Prometheus should already be started with docker-compose above
    wait_for_service "Prometheus" 9090
fi

# Create Kafka topics
echo -e "${YELLOW}📋 Creating Kafka topics...${NC}"

# Topics for trading data
TOPICS=(
    "market_data"
    "trading_positions" 
    "order_book_updates"
    "price_alerts"
    "volume_spikes"
    "risk_alerts"
    "order_imbalance_alerts"
    "arbitrage_opportunities"
)

for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"
    docker exec $(docker-compose -f kafka-compose.yml ps -q kafka) kafka-topics \
        --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --partitions 3 \
        --replication-factor 1 || true
done

echo -e "${GREEN}✓ Kafka topics created${NC}"

# Build the project
echo -e "${YELLOW}🔨 Building FerrisStreams...${NC}"
cd ../..
cargo build --release
cd demo/trading

# Start the multi-job SQL server with financial trading analysis
echo -e "${YELLOW}🚀 Starting Multi-Job SQL server with Financial Trading Analytics...${NC}"
../../target/release/ferris-sql-multi server --brokers "$KAFKA_BROKERS" --port "$SQL_SERVER_PORT" &
SQL_SERVER_PID=$!

echo -e "${GREEN}✓ Financial trading analytics server started${NC}"
echo "ℹ️  Multi-job server ready for financial trading analytics deployment"
sleep 2

# Start data generator in background first (to populate topics)
echo -e "${YELLOW}📊 Starting trading data generator...${NC}"
if [ -f "target/release/trading_data_generator" ]; then
    ./target/release/trading_data_generator "$KAFKA_BROKERS" "$DEMO_DURATION" &
else
    echo "Building trading data generator from local source..."
    cargo build --release
    ./target/release/trading_data_generator "$KAFKA_BROKERS" "$DEMO_DURATION" &
fi
GENERATOR_PID=$!

# Wait for some initial data to be generated
echo -e "${YELLOW}⏳ Allowing data generator to populate topics...${NC}"
sleep 5

# Deploy financial trading analytics jobs (now that data is available)
echo -e "${YELLOW}📊 Deploying financial trading analytics jobs...${NC}"
../../target/release/ferris-sql-multi deploy-app --file sql/financial_trading.sql --brokers "$KAFKA_BROKERS"
echo -e "${GREEN}✓ All 5 financial trading analytics jobs deployed${NC}"
sleep 3

# Start monitoring consumers
echo -e "${YELLOW}👀 Starting data consumers for monitoring...${NC}"

# Function to consume and display messages
consume_topic() {
    local topic=$1
    local display_name=$2
    
    echo -e "${BLUE}📈 Consuming $display_name...${NC}"
    docker exec $(docker-compose -f kafka-compose.yml ps -q kafka) kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning \
        --max-messages 10 \
        --timeout-ms 30000 || true
}

# Monitor key topics
echo -e "${YELLOW}🔍 Monitoring trading data streams...${NC}"
echo "Press Ctrl+C to stop monitoring individual topics"

sleep 5  # Let some data generate first

echo -e "\n${BLUE}=== Market Data Sample ===${NC}"
consume_topic "market_data" "Market Data"

echo -e "\n${BLUE}=== Price Alerts Sample ===${NC}"
consume_topic "price_alerts" "Price Movement Alerts"

echo -e "\n${BLUE}=== Volume Spikes Sample ===${NC}"
consume_topic "volume_spikes" "Volume Spike Alerts"

echo -e "\n${BLUE}=== Risk Alerts Sample ===${NC}"
consume_topic "risk_alerts" "Risk Management Alerts"

echo -e "\n${BLUE}=== Arbitrage Opportunities Sample ===${NC}"
consume_topic "arbitrage_opportunities" "Arbitrage Opportunities"

# Show job status
echo -e "\n${YELLOW}📊 Job Status Check...${NC}"
echo "ℹ️  Multi-job SQL server is running with 5 financial trading analytics jobs"
echo "ℹ️  Jobs: price_alerts, volume_spikes, risk_alerts, order_imbalance_alerts, arbitrage_opportunities"

# Wait for demo duration
echo -e "\n${YELLOW}⏰ Demo running for $DEMO_DURATION minutes...${NC}"
wait $GENERATOR_PID

echo -e "\n${GREEN}✅ Demo completed successfully!${NC}"

# Cleanup
echo -e "${YELLOW}🧹 Cleaning up...${NC}"
echo "Stopping SQL server..."
kill $SQL_SERVER_PID 2>/dev/null || true

echo -e "${GREEN}🎉 Financial Trading Demo completed!${NC}"
echo ""
echo -e "${BLUE}Summary:${NC}"
echo "• Generated realistic trading data for 8 major stocks"
echo "• Deployed 5 concurrent SQL analytics jobs with multi-job server"
echo "• Processed real-time market data, positions, and order book updates"  
echo "• Detected price movements, volume spikes, and risk conditions"
echo "• Identified arbitrage opportunities across exchanges"
echo "• All data is available in Kafka topics for further analysis"
echo ""
echo -e "${BLUE}🎯 Access Points:${NC}"
echo "• Kafka UI: http://localhost:8090 (monitor topics and messages)"
echo "• Grafana Dashboards: http://localhost:3000 (admin/admin)"
echo "  - FerrisStreams Trading Demo (real-time trading analytics)"
echo "  - FerrisStreams Overview (system health and performance)"
echo "  - Kafka Metrics (broker and topic statistics)"
echo "• Prometheus: http://localhost:9090 (metrics and monitoring)"
echo "• Python Dashboard: Run 'source dashboard_env/bin/activate && python3 dashboard.py'"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "• Monitor with CLI: ./ferris-cli status --verbose (run ./build_cli.sh first)"
echo "• View Kafka topics: docker exec \$(docker-compose -f kafka-compose.yml ps -q kafka) kafka-topics --list --bootstrap-server localhost:9092"
echo "• SQL server logs show demo job execution"
echo "• Check logs: docker-compose logs"