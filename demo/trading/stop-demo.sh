#!/bin/bash

# Stop Financial Trading Demo Script
# Cleanly shuts down all demo components

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
CLEAN_DATA=false
for arg in "$@"; do
    case $arg in
        -c|--clean)
            CLEAN_DATA=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -c, --clean    Clean all data (Kafka topics, Prometheus data)"
            echo "  -h, --help     Show this help message"
            exit 0
            ;;
    esac
done

echo -e "${BLUE}🛑 Stopping Velostream Financial Trading Demo${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Function to kill processes by name
kill_processes() {
    local process_name=$1
    local pids=$(pgrep -f "$process_name" 2>/dev/null || true)
    
    if [ -n "$pids" ]; then
        echo -e "${YELLOW}🔪 Stopping $process_name processes...${NC}"
        echo "$pids" | xargs kill -TERM 2>/dev/null || true
        sleep 2
        
        # Force kill if still running
        local remaining_pids=$(pgrep -f "$process_name" 2>/dev/null || true)
        if [ -n "$remaining_pids" ]; then
            echo -e "${YELLOW}⚡ Force killing $process_name processes...${NC}"
            echo "$remaining_pids" | xargs kill -KILL 2>/dev/null || true
        fi
        echo -e "${GREEN}✓ $process_name processes stopped${NC}"
    else
        echo -e "${GREEN}✓ No $process_name processes running${NC}"
    fi
}

# Stop background processes
echo -e "${YELLOW}🧹 Stopping background processes...${NC}"
kill_processes "velo-sql"
kill_processes "velo-test"

# Stop Docker services
echo -e "${YELLOW}🐳 Stopping Docker services...${NC}"

# Check if docker-compose services are running
COMPOSE_CMD="docker-compose"
if docker compose version &>/dev/null; then
    COMPOSE_CMD="docker compose"
fi

if $COMPOSE_CMD ps -q 2>/dev/null | grep -q .; then
    echo "Stopping docker services..."
    $COMPOSE_CMD down
    echo -e "${GREEN}✓ Main services stopped${NC}"
fi

# Stop any other Kafka/Zookeeper containers
echo -e "${YELLOW}🔍 Checking for other Kafka containers...${NC}"
KAFKA_CONTAINERS=$(docker ps -q --filter "name=kafka" --filter "name=zookeeper" --filter "name=velo" 2>/dev/null || true)

if [ -n "$KAFKA_CONTAINERS" ]; then
    echo "Stopping remaining Kafka/Zookeeper containers..."
    echo "$KAFKA_CONTAINERS" | xargs docker stop 2>/dev/null || true
    echo "$KAFKA_CONTAINERS" | xargs docker rm 2>/dev/null || true
    echo -e "${GREEN}✓ Additional containers stopped${NC}"
else
    echo -e "${GREEN}✓ No additional containers found${NC}"
fi

# Clean up CLI symlink if it exists
if [ -L "./velo-cli" ]; then
    echo -e "${YELLOW}🧹 Cleaning up CLI symlink...${NC}"
    rm ./velo-cli
    echo -e "${GREEN}✓ CLI symlink removed${NC}"
fi

# Clean data if requested
if [ "$CLEAN_DATA" = true ]; then
    echo -e "${YELLOW}🧹 Cleaning all demo data (--clean requested)...${NC}"

    # Clear Kafka topics if Kafka is running
    if docker ps -q --filter "name=simple-kafka" 2>/dev/null | grep -q .; then
        echo -e "${YELLOW}📦 Deleting Kafka topics...${NC}"

        # Get all velo-related topics
        TOPICS=$(docker exec simple-kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -E "^(in_|market_data|tick_|enriched_|price_|volume_|order_|arbitrage_|trading_|risk_|compliant_|active_)" || true)

        for topic in $TOPICS; do
            docker exec simple-kafka kafka-topics --delete --topic "$topic" --bootstrap-server localhost:9092 2>/dev/null || true
            echo -e "  ${GREEN}✓ Deleted topic '$topic'${NC}"
        done
        echo -e "${GREEN}✓ Kafka topics cleared${NC}"
    else
        echo -e "${YELLOW}ℹ️  Kafka not running, skipping topic cleanup${NC}"
    fi

    # Clear Prometheus data - remove container AND volume
    echo -e "${YELLOW}📊 Clearing Prometheus data...${NC}"
    docker rm -f velo-prometheus 2>/dev/null || true
    # Remove the data volume to clear all historical metrics
    docker volume rm trading_prometheus_data 2>/dev/null || true
    docker volume rm demo_trading_prometheus_data 2>/dev/null || true
    echo -e "${GREEN}✓ Prometheus container and data volume removed${NC}"

    # Also clear Grafana data volume for clean dashboards
    docker rm -f velo-grafana 2>/dev/null || true
    docker volume rm trading_grafana_data 2>/dev/null || true
    docker volume rm demo_trading_grafana_data 2>/dev/null || true
    echo -e "${GREEN}✓ Grafana container and data volume removed${NC}"

    # Clean up log files
    echo -e "${YELLOW}📄 Cleaning up log files...${NC}"
    rm -f /tmp/velo_*.log /tmp/demo_output.log 2>/dev/null || true
    echo -e "${GREEN}✓ Log files cleaned${NC}"

    echo -e "${GREEN}✓ All demo data cleaned${NC}"
fi

# Verify everything is stopped
echo -e "${YELLOW}🔍 Verifying clean shutdown...${NC}"

# Check for remaining processes
REMAINING_PROCESSES=$(ps aux | grep -E "(velo|trading|kafka)" | grep -v grep | grep -v stop_demo.sh || true)
if [ -n "$REMAINING_PROCESSES" ]; then
    echo -e "${YELLOW}⚠️  Some processes may still be running:${NC}"
    echo "$REMAINING_PROCESSES"
else
    echo -e "${GREEN}✓ No demo processes running${NC}"
fi

# Check for remaining containers
REMAINING_CONTAINERS=$(docker ps -q 2>/dev/null || true)
if [ -n "$REMAINING_CONTAINERS" ]; then
    echo -e "${YELLOW}ℹ️  Other Docker containers still running (not demo-related)${NC}"
    docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
else
    echo -e "${GREEN}✓ No Docker containers running${NC}"
fi

# Prompt to clean data if not already requested
if [ "$CLEAN_DATA" = false ]; then
    echo ""
    echo -e "${YELLOW}📦 Demo data (Kafka topics, Prometheus/Grafana volumes) is still preserved.${NC}"
    read -r -p "   Clean all stored data? [y/N] " response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}🧹 Cleaning all demo data...${NC}"

        # Clear Kafka topics if Kafka is still reachable
        if docker ps -q --filter "name=simple-kafka" 2>/dev/null | grep -q .; then
            echo -e "${YELLOW}📦 Deleting Kafka topics...${NC}"
            TOPICS=$(docker exec simple-kafka kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null | grep -E "^(in_|market_data|tick_|enriched_|price_|volume_|order_|arbitrage_|trading_|risk_|compliant_|active_)" || true)
            for topic in $TOPICS; do
                docker exec simple-kafka kafka-topics --delete --topic "$topic" --bootstrap-server localhost:9092 2>/dev/null || true
                echo -e "  ${GREEN}✓ Deleted topic '$topic'${NC}"
            done
            echo -e "${GREEN}✓ Kafka topics cleared${NC}"
        fi

        # Clear Prometheus data
        docker rm -f velo-prometheus 2>/dev/null || true
        docker volume rm trading_prometheus_data demo_trading_prometheus_data 2>/dev/null || true
        echo -e "${GREEN}✓ Prometheus data removed${NC}"

        # Clear Grafana data
        docker rm -f velo-grafana 2>/dev/null || true
        docker volume rm trading_grafana_data demo_trading_grafana_data 2>/dev/null || true
        echo -e "${GREEN}✓ Grafana data removed${NC}"

        # Clean log files
        rm -f /tmp/velo_*.log /tmp/demo_output.log 2>/dev/null || true
        echo -e "${GREEN}✓ Log files cleaned${NC}"
    else
        echo -e "${GREEN}✓ Data preserved for next run${NC}"
    fi
fi

echo ""
echo -e "${GREEN}🎉 Financial Trading Demo stopped successfully!${NC}"
echo ""
echo -e "${BLUE}To restart:${NC}"
echo "• Run: ./start-demo.sh"
echo "• Quick start: ./start-demo.sh -q"
echo "• With dashboard: ./start-demo.sh -d"