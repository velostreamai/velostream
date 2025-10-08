#!/bin/bash
# Velostream Trading Demo - Unified Launcher
# Consolidates all demo functionality into a single script

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
SIMULATION_DURATION=10
USE_RELEASE_BUILD=false
SETUP_DASHBOARD=false
INTERACTIVE_MODE=false
QUICK_START=false

# Parse command-line arguments
show_usage() {
    cat << EOF
${BLUE}Velostream Trading Demo - Unified Launcher${NC}

Usage: $0 [OPTIONS] [DURATION]

OPTIONS:
    -h, --help              Show this help message
    -r, --release           Use release builds (optimized, slower compile)
    -d, --dashboard         Setup Python dashboard environment
    -i, --interactive       Run in interactive/foreground mode
    -q, --quick             Quick 1-minute demo

DURATION:
    Number of minutes to run simulation (default: 10)
    Use 0 for infinite duration

EXAMPLES:
    $0                      # Run 10-minute demo in background
    $0 5                    # Run 5-minute demo
    $0 -q                   # Quick 1-minute demo
    $0 -r 30                # 30-minute demo with release builds
    $0 -d                   # Setup dashboard and run demo
    $0 -i                   # Interactive mode (foreground)

MONITORING:
    tail -f /tmp/velo_deployment.log     # Watch SQL job logs
    tail -f /tmp/trading_generator.log   # Watch generator logs
    ./check-demo-health.sh               # Check demo health
    ./stop-demo.sh                       # Stop all demo processes

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            ;;
        -r|--release)
            USE_RELEASE_BUILD=true
            shift
            ;;
        -d|--dashboard)
            SETUP_DASHBOARD=true
            shift
            ;;
        -i|--interactive)
            INTERACTIVE_MODE=true
            shift
            ;;
        -q|--quick)
            QUICK_START=true
            SIMULATION_DURATION=1
            shift
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
        *)
            SIMULATION_DURATION=$1
            shift
            ;;
    esac
done

# Set build paths based on release flag
if [ "$USE_RELEASE_BUILD" = true ]; then
    BUILD_DIR="target/release"
    VELO_BUILD_DIR="../../target/release"
    BUILD_FLAG="--release"
else
    BUILD_DIR="target/debug"
    VELO_BUILD_DIR="../../target/debug"
    BUILD_FLAG=""
fi

# Configuration
KAFKA_BROKER="localhost:9092"
DOCKER_BROKER="broker:9092"

echo -e "${BLUE}========================================${NC}"
if [ "$QUICK_START" = true ]; then
    echo -e "${BLUE}Velostream Trading Demo - Quick Start${NC}"
else
    echo -e "${BLUE}Velostream Trading Demo Startup${NC}"
fi
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Duration: ${SIMULATION_DURATION} minute(s)${NC}"
if [ "$USE_RELEASE_BUILD" = true ]; then
    echo -e "${BLUE}Build: Release (optimized)${NC}"
else
    echo -e "${BLUE}Build: Debug (fast compile)${NC}"
fi
if [ "$INTERACTIVE_MODE" = true ]; then
    echo -e "${BLUE}Mode: Interactive (foreground)${NC}"
else
    echo -e "${BLUE}Mode: Background${NC}"
fi
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

# Optional: Setup Dashboard
if [ "$SETUP_DASHBOARD" = true ]; then
    print_step "Setting up Python Dashboard"

    # Check Python version
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}✗ Python 3 is not installed${NC}"
        exit 1
    fi

    # Create virtual environment if it doesn't exist
    if [ ! -d "dashboard_env" ]; then
        echo -e "${YELLOW}Creating Python virtual environment...${NC}"
        python3 -m venv dashboard_env
        check_status "Virtual environment created"
    fi

    # Install dependencies
    if [ -f "requirements.txt" ]; then
        echo -e "${YELLOW}Installing Python dependencies...${NC}"
        source dashboard_env/bin/activate && pip install -q -r requirements.txt
        check_status "Dashboard dependencies installed"
    fi

    echo -e "${GREEN}✓ Dashboard setup complete${NC}"
    echo -e "${YELLOW}To use: source dashboard_env/bin/activate && python3 dashboard.py${NC}"
fi

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
if [ ! -f "$BUILD_DIR/trading_data_generator" ] || [ ! -f "$VELO_BUILD_DIR/velo-sql-multi" ]; then
    echo "Building trading data generator..."
    cargo build $BUILD_FLAG --bin trading_data_generator
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
done

# Step 7: Start data generator
print_step "Step 7: Starting trading data generator"
echo "Simulation duration: ${SIMULATION_DURATION} minutes"
./$BUILD_DIR/trading_data_generator $KAFKA_BROKER $SIMULATION_DURATION > /tmp/trading_generator.log 2>&1 &
GENERATOR_PID=$!
echo -e "${GREEN}✓ Data generator started (PID: $GENERATOR_PID)${NC}"

# Step 8: Verify data is flowing
print_step "Step 8: Verifying data flow"
wait_for 30 2 "docker exec simple-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic market_data_stream --max-messages 1 --timeout-ms 1000" "Data flowing to market_data_stream"

# Step 9: Deploy SQL application
print_step "Step 9: Deploying SQL application"
echo "Deploying 8 streaming queries..."

if [ "$INTERACTIVE_MODE" = true ]; then
    # Run in foreground for interactive mode
    echo -e "${YELLOW}Running in interactive mode (foreground)...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
    $VELO_BUILD_DIR/velo-sql-multi deploy-app --file sql/financial_trading.sql
else
    # Run in background
    $VELO_BUILD_DIR/velo-sql-multi deploy-app --file sql/financial_trading.sql > /tmp/velo_deployment.log 2>&1 &
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
    echo -e "  ./check-demo-health.sh              # Check demo health"
    echo ""
    echo -e "${BLUE}Kafka Commands:${NC}"
    echo -e "  docker exec simple-kafka kafka-topics --list --bootstrap-server localhost:9092"
    echo -e "  docker exec simple-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic market_data_ts --from-beginning --max-messages 10"
    echo ""
    if [ "$SETUP_DASHBOARD" = true ]; then
        echo -e "${BLUE}Dashboard:${NC}"
        echo -e "  source dashboard_env/bin/activate && python3 dashboard.py"
        echo ""
    fi
    echo -e "${BLUE}Stop Demo:${NC}"
    echo -e "  ./stop-demo.sh"
    echo -e "${GREEN}========================================${NC}"
fi
