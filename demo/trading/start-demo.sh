#!/bin/bash
# Resilient Trading Demo Startup Script
# Ensures proper initialization order and validates each step

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAFKA_BROKER="localhost:9092"
DOCKER_BROKER="broker:9092"
SIMULATION_DURATION=${1:-10}  # Default 10 minutes

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Velostream Trading Demo Startup${NC}"
echo -e "${BLUE}========================================${NC}"

# Function: Print step
print_step() {
    echo -e "\n${BLUE}▶ $1${NC}"
}

# Function: Check status
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1${NC}"
    else
        echo -e "${RED}✗ $1 failed${NC}"
        exit 1
    fi
}

# Function: Wait for condition with timeout
wait_for() {
    local timeout=$1
    local interval=$2
    local check_command=$3
    local description=$4

    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if eval "$check_command" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ $description (${elapsed}s)${NC}"
            return 0
        fi
        sleep $interval
        elapsed=$((elapsed + interval))
        echo -n "."
    done
    echo -e "${RED}✗ $description timed out after ${timeout}s${NC}"
    return 1
}

# Step 1: Check Docker
print_step "Step 1: Checking Docker"
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}✗ Docker is not running${NC}"
    exit 1
fi
check_status "Docker is running"

# Step 2: Start Kafka
print_step "Step 2: Starting Kafka infrastructure"
docker-compose -f kafka-compose.yml up -d
check_status "Kafka containers started"

# Step 3: Wait for Kafka to be ready
print_step "Step 3: Waiting for Kafka to be ready"
wait_for 60 2 "docker exec simple-kafka kafka-broker-api-versions --bootstrap-server localhost:9092" "Kafka broker ready"

# Step 4: Validate/Create topics
print_step "Step 4: Ensuring all required topics exist"

REQUIRED_TOPICS=(
    "market_data_stream:12"
    "market_data_ts:12"
    "market_data_stream_a:12"
    "market_data_stream_b:12"
    "trading_positions_stream:8"
    "order_book_stream:12"
)

for topic_spec in "${REQUIRED_TOPICS[@]}"; do
    IFS=':' read -r topic partitions <<< "$topic_spec"

    if docker exec simple-kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -q "^${topic}$"; then
        echo -e "${GREEN}✓ Topic '$topic' exists${NC}"
    else
        echo -e "${YELLOW}⚠ Creating topic '$topic' with $partitions partitions${NC}"
        docker exec simple-kafka kafka-topics --create \
            --topic "$topic" \
            --bootstrap-server localhost:9092 \
            --partitions "$partitions" \
            --replication-factor 1
        check_status "Topic '$topic' created"
    fi
done

# Step 5: Build binaries if needed
print_step "Step 5: Building project binaries"
if [ ! -f "target/debug/trading_data_generator" ] || [ ! -f "../../target/debug/velo-sql-multi" ]; then
    echo "Building trading data generator..."
    cargo build --bin trading_data_generator
    check_status "Trading data generator built"
fi

# Step 6: Reset consumer groups (for clean demo start)
print_step "Step 6: Resetting consumer groups for clean start"
echo -e "${YELLOW}⚠ Deleting existing consumer groups...${NC}"
for group in $(docker exec simple-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list 2>/dev/null | grep "velo-sql"); do
    docker exec simple-kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --group "$group" \
        --delete 2>/dev/null || true
    echo -e "${GREEN}✓ Deleted consumer group: $group${NC}"
done

# Step 7: Start data generator
print_step "Step 7: Starting trading data generator"
echo "Simulation duration: ${SIMULATION_DURATION} minutes"
./target/debug/trading_data_generator $KAFKA_BROKER $SIMULATION_DURATION > /tmp/trading_generator.log 2>&1 &
GENERATOR_PID=$!
echo -e "${GREEN}✓ Data generator started (PID: $GENERATOR_PID)${NC}"

# Step 8: Verify data is flowing
print_step "Step 8: Verifying data flow"
sleep 5  # Give generator time to produce first batch
wait_for 30 2 "docker exec simple-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic market_data_stream --max-messages 1 --timeout-ms 1000" "Data flowing to market_data_stream"

# Step 9: Deploy SQL application
print_step "Step 9: Deploying SQL application"
echo "Deploying 8 streaming queries..."
../../target/debug/velo-sql-multi deploy-app --file sql/financial_trading.sql > /tmp/velo_deployment.log 2>&1 &
DEPLOY_PID=$!
echo -e "${GREEN}✓ Deployment started (PID: $DEPLOY_PID)${NC}"

# Step 10: Monitor deployment
print_step "Step 10: Monitoring deployment"
sleep 10  # Give deployment time to initialize

# Check if deployment is still running
if ! ps -p $DEPLOY_PID > /dev/null 2>&1; then
    echo -e "${RED}✗ Deployment process exited prematurely${NC}"
    echo "Check /tmp/velo_deployment.log for errors"
    exit 1
fi

echo -e "${GREEN}✓ Deployment process running${NC}"

# Step 11: Summary
print_step "Demo Started Successfully!"
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Status Summary${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Data Generator:     PID $GENERATOR_PID (log: /tmp/trading_generator.log)"
echo -e "SQL Deployment:     PID $DEPLOY_PID (log: /tmp/velo_deployment.log)"
echo -e "Simulation Duration: ${SIMULATION_DURATION} minutes"
echo ""
echo -e "${BLUE}Monitoring Commands:${NC}"
echo -e "  tail -f /tmp/velo_deployment.log    # Watch SQL job logs"
echo -e "  tail -f /tmp/trading_generator.log  # Watch generator logs"
echo ""
echo -e "${BLUE}Kafka Commands:${NC}"
echo -e "  docker exec simple-kafka kafka-topics --list --bootstrap-server localhost:9092"
echo -e "  docker exec simple-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic market_data_ts --from-beginning --max-messages 10"
echo ""
echo -e "${BLUE}Stop Demo:${NC}"
echo -e "  ./stop-demo.sh"
echo -e "${GREEN}========================================${NC}"
