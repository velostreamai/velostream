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
INTERACTIVE_MODE=false
QUICK_START=false
ENABLE_METRICS=false
FORCE_REBUILD=false

# Parse command-line arguments
show_usage() {
    cat << EOF
${BLUE}Velostream Trading Demo - Unified Launcher${NC}

Usage: $0 [OPTIONS] [DURATION]

OPTIONS:
    -h, --help              Show this help message
    -r, --release           Use release builds (optimized, slower compile)
    -i, --interactive       Run in interactive/foreground mode
    -q, --quick             Quick 1-minute demo
    -m, --with-metrics      Display monitoring info early (before deployment)
    -y, --yes               Force rebuild of all binaries (cargo clean + build)

DURATION:
    Number of minutes to run simulation (default: 10)
    Use 0 for infinite duration

EXAMPLES:
    $0                      # Run 10-minute demo in background
    $0 5                    # Run 5-minute demo
    $0 -q                   # Quick 1-minute demo
    $0 -r 30                # 30-minute demo with release builds
    $0 -i                   # Interactive mode (foreground)
    $0 -m                   # Show monitoring info before deployment starts
    $0 -q -m                # Quick demo with early metrics display
    $0 -y                   # Force clean rebuild of all binaries
    $0 -y -r                # Force rebuild with release optimization

NOTE:
    Monitoring dashboards (Grafana/Prometheus/Kafka UI) are ALWAYS started
    automatically and info is displayed at the end. Use -m to see it earlier.

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
        -i|--interactive)
            INTERACTIVE_MODE=true
            shift
            ;;
        -q|--quick)
            QUICK_START=true
            SIMULATION_DURATION=1
            shift
            ;;
        -m|--with-metrics)
            ENABLE_METRICS=true
            shift
            ;;
        -y|--yes)
            FORCE_REBUILD=true
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

# Function: Print timestamp
print_timestamp() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

# Function: Print binary info
print_binary_info() {
    local binary_path=$1
    local binary_name=$(basename "$binary_path")

    if [ -f "$binary_path" ]; then
        local mod_time=$(date -r "$binary_path" '+%Y-%m-%d %H:%M:%S')
        local size=$(ls -lh "$binary_path" | awk '{print $5}')
        echo -e "${GREEN}Binary: $binary_name${NC}"
        echo -e "  Path:     $binary_path"
        echo -e "  Modified: $mod_time"
        echo -e "  Size:     $size"
    else
        echo -e "${YELLOW}Binary: $binary_name (will be built)${NC}"
        echo -e "  Path:     $binary_path"
    fi
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

# Prerequisite validation
print_step "Validating Prerequisites"

# Check Rust/Cargo
if ! command -v cargo &> /dev/null; then
    echo -e "${RED}✗ Rust/Cargo is not installed${NC}"
    echo -e "${YELLOW}Install from: https://rustup.rs/${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Rust/Cargo found${NC}"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed${NC}"
    echo -e "${YELLOW}Install from: https://www.docker.com/get-started${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker found${NC}"

# Check Docker Compose
if ! docker compose version &> /dev/null && ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}✗ Docker Compose is not installed${NC}"
    echo -e "${YELLOW}Install from: https://docs.docker.com/compose/install/${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose found${NC}"

# Check for port conflicts
REQUIRED_PORTS=(9092 2181 3000 9090 8090)
PORT_CONFLICTS=()
for port in "${REQUIRED_PORTS[@]}"; do
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 || nc -z localhost $port 2>/dev/null; then
        PORT_CONFLICTS+=($port)
    fi
done

if [ ${#PORT_CONFLICTS[@]} -gt 0 ]; then
    echo -e "${YELLOW}⚠ Warning: The following ports are already in use:${NC}"
    for port in "${PORT_CONFLICTS[@]}"; do
        echo -e "${YELLOW}  - Port $port${NC}"
    done
    echo -e "${YELLOW}The demo may not start correctly. Stop conflicting services first.${NC}"
    echo -e "${YELLOW}Press Ctrl+C to cancel, or wait 5 seconds to continue anyway...${NC}"
    sleep 5
fi

# Step 1: Check Docker is running
print_step "Step 1: Checking Docker"
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}✗ Docker is not running${NC}"
    exit 1
fi
check_status "Docker is running"

# Step 2: Start Kafka
print_step "Step 2: Starting Kafka infrastructure"
echo -e "${YELLOW}This may take a few minutes on first run (downloading Docker images)...${NC}"
if ! docker-compose -f kafka-compose.yml up -d; then
    echo -e "${RED}✗ Failed to start Kafka containers${NC}"
    echo -e "${YELLOW}This could be due to:${NC}"
    echo -e "${YELLOW}  - Network connectivity issues (can't pull Docker images)${NC}"
    echo -e "${YELLOW}  - Port conflicts (check ports 9092, 2181, 3000, 9090, 8090)${NC}"
    echo -e "${YELLOW}  - Insufficient disk space${NC}"
    echo -e "${YELLOW}Try: docker-compose -f kafka-compose.yml logs${NC}"
    exit 1
fi
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
    "in_market_data_stream:12"
    "in_market_data_stream_a:12"
    "in_market_data_stream_b:12"
    "in_trading_positions_stream:8"
    "in_order_book_stream:12"
    "price_movement_debug:1"
    "price_movement_debug_2:1"
    "tick_buckets:1"
    "advanced_price_movement_alerts:1"
    "volume_spike_analysis:1"
    "trading_positions_with_event_time:1"
    "comprehensive_risk_monitor:1"
    "order_flow_imbalance_detection:1"
    "arbitrage_opportunities_detection:1"
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

# Step 5: Build binaries (always check for updates)
if [ "$FORCE_REBUILD" = true ]; then
    print_step "Step 5: Force rebuilding project binaries (clean + build)"
    echo -e "${YELLOW}⚠ Force rebuild requested - this will take longer${NC}"
else
    print_step "Step 5: Building project binaries (Cargo rebuilds only if source changed)"
fi

# Force rebuild if requested
if [ "$FORCE_REBUILD" = true ]; then
    cd ../..
    echo "Running cargo clean..."
    cargo clean
    check_status "Build artifacts cleaned"
    cd demo/trading
fi

# Build main Velostream binary
echo ""
echo "Building/checking main Velostream project..."
cd ../..

VELO_BINARY_PATH="$BUILD_DIR/velo-sql-multi"
print_binary_info "$VELO_BINARY_PATH"

print_timestamp "Starting velo-sql-multi build..."
if ! cargo build $BUILD_FLAG --bin velo-sql-multi; then
    echo -e "${RED}✗ Failed to build velo-sql-multi${NC}"
    echo -e "${YELLOW}Try: cargo clean && cargo build --bin velo-sql-multi${NC}"
    echo -e "${YELLOW}Or update Rust: rustup update stable${NC}"
    exit 1
fi
print_timestamp "Completed velo-sql-multi build"
check_status "velo-sql-multi ready"

# Show updated binary info
print_binary_info "$VELO_BINARY_PATH"

cd demo/trading

# Build trading data generator
echo ""
echo "Building/checking trading data generator..."

GENERATOR_BINARY_PATH="$BUILD_DIR/trading_data_generator"
print_binary_info "$GENERATOR_BINARY_PATH"

print_timestamp "Starting trading_data_generator build..."
if ! cargo build $BUILD_FLAG --bin trading_data_generator; then
    echo -e "${RED}✗ Failed to build trading_data_generator${NC}"
    echo -e "${YELLOW}Try: cargo clean && cargo build --bin trading_data_generator${NC}"
    exit 1
fi
print_timestamp "Completed trading_data_generator build"
check_status "Trading data generator ready"

# Show updated binary info
print_binary_info "$GENERATOR_BINARY_PATH"

echo ""
echo -e "${GREEN}✓ All binaries up-to-date${NC}"

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

# Show binary info before execution
echo ""
print_binary_info "$GENERATOR_BINARY_PATH"
echo ""

print_timestamp "Launching trading_data_generator..."
echo -e "${BLUE}Command: ./$BUILD_DIR/trading_data_generator $KAFKA_BROKER $SIMULATION_DURATION${NC}"
./$BUILD_DIR/trading_data_generator $KAFKA_BROKER $SIMULATION_DURATION > /tmp/trading_generator.log 2>&1 &
GENERATOR_PID=$!
print_timestamp "Data generator started (PID: $GENERATOR_PID)"
echo -e "${GREEN}✓ Data generator started (PID: $GENERATOR_PID)${NC}"

# Step 8: Verify data is flowing
print_step "Step 8: Verifying data flow"
wait_for 30 2 "docker exec simple-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic in_market_data_stream --max-messages 1 --timeout-ms 1000" "Data flowing to in_market_data_stream"

# Step 9: Deploy SQL application
print_step "Step 9: Deploying SQL application"

# Show binary info before execution
echo ""
print_binary_info "$VELO_BINARY_PATH"
echo ""

if [ "$ENABLE_METRICS" = true ]; then
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}📊 Monitoring Infrastructure${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}✓ Grafana and Prometheus are starting with the demo${NC}"
    echo ""
    echo -e "${YELLOW}Access Points:${NC}"
    echo -e "  • Grafana:    http://localhost:3000 ${BLUE}(admin/admin)${NC}"
    echo -e "  • Prometheus: http://localhost:9090"
    echo -e "  • Kafka UI:   http://localhost:8090"
    echo ""
    echo -e "${YELLOW}Pre-configured Dashboards:${NC}"
    echo -e "  • Velostream Trading Demo - Real-time analytics & alerts"
    echo -e "  • Velostream Overview - System health & throughput"
    echo -e "  • Kafka Metrics - Broker performance & topic stats"
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
fi

if [ "$INTERACTIVE_MODE" = true ]; then
    # Run in foreground for interactive mode
    echo "Deploying 8 streaming queries with observability enabled..."
    echo -e "${YELLOW}Running in interactive mode (foreground)...${NC}"
    echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
    echo ""
    print_timestamp "Launching velo-sql-multi (interactive mode)..."
    echo -e "${BLUE}Command: $VELO_BUILD_DIR/velo-sql-multi deploy-app --file sql/financial_trading.sql --enable-tracing --enable-metrics --metrics-port 9091 --enable-profiling${NC}"
    echo ""
    # Set deployment context environment variables for per-node observability
    export NODE_ID="velostream-prod-node-1"
    export NODE_NAME="velostream-trading-engine"
    export REGION="us-east-1"
    export APP_VERSION="1.0.0"

    $VELO_BUILD_DIR/velo-sql-multi deploy-app \
        --file sql/financial_trading.sql \
        --enable-tracing \
        --enable-metrics \
        --metrics-port 9091 \
        --enable-profiling
else
    # Run in background (deploy-app mode)
    echo "Deploying 8 streaming queries with observability enabled..."
    print_timestamp "Launching velo-sql-multi (background mode)..."
    echo -e "${BLUE}Command: $VELO_BUILD_DIR/velo-sql-multi deploy-app --file sql/financial_trading.sql --enable-tracing --enable-metrics --metrics-port 9091 --enable-profiling${NC}"
    echo ""
    # Set deployment context environment variables for per-node observability
    export NODE_ID="velostream-prod-node-1"
    export NODE_NAME="velostream-trading-engine"
    export REGION="us-east-1"
    export APP_VERSION="1.0.0"

    $VELO_BUILD_DIR/velo-sql-multi deploy-app \
        --file sql/financial_trading.sql \
        --enable-tracing \
        --enable-metrics \
        --metrics-port 9091 \
        --enable-profiling \
        > /tmp/velo_deployment.log 2>&1 &
    DEPLOY_PID=$!
    print_timestamp "Deployment started (PID: $DEPLOY_PID)"
    echo -e "${GREEN}✓ Deployment started with observability (PID: $DEPLOY_PID)${NC}"

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
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}📊 Monitoring Dashboards (Already Running)${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "  • Grafana:    ${GREEN}http://localhost:3000${NC} ${BLUE}(admin/admin)${NC}"
    echo -e "  • Prometheus: ${GREEN}http://localhost:9090${NC}"
    echo -e "  • Kafka UI:   ${GREEN}http://localhost:8090${NC}"
    echo ""
    echo -e "${BLUE}🔍 Observability (Enabled)${NC}"
    echo -e "  • Velostream Metrics: ${GREEN}http://localhost:9091/metrics${NC} ${BLUE}(Prometheus format)${NC}"
    echo -e "  • Distributed Tracing: ${GREEN}ENABLED${NC} ${BLUE}(100% sampling)${NC}"
    echo -e "  • Performance Profiling: ${GREEN}ENABLED${NC}"
    echo ""
    echo -e "${YELLOW}Pre-configured Grafana Dashboards:${NC}"
    echo -e "  • Velostream Trading Demo - Real-time analytics & alerts"
    echo -e "  • Velostream Overview - System health & throughput"
    echo -e "  • Kafka Metrics - Broker performance & topic stats"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "${BLUE}Stop Demo:${NC}"
    echo -e "  ./stop-demo.sh"
    echo -e "${GREEN}========================================${NC}"
fi
