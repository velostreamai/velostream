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
kill_processes "velo-sql"
kill_processes "velo-test"

# Stop Docker services
echo -e "${YELLOW}🐳 Stopping Docker services...${NC}"

# Check if kafka-compose.yml services are running
if docker-compose -f kafka-compose.yml ps -q 2>/dev/null | grep -q .; then
    echo "Stopping Kafka setup..."
    docker-compose -f kafka-compose.yml down
    echo -e "${GREEN}✓ Kafka setup stopped${NC}"
fi

# Check if main docker-compose services are running  
if docker-compose ps -q 2>/dev/null | grep -q .; then
    echo "Stopping main docker-compose services..."
    docker-compose down
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

echo ""
echo -e "${GREEN}🎉 Financial Trading Demo stopped successfully!${NC}"
echo ""
echo -e "${BLUE}Status:${NC}"
echo "• All demo processes terminated"
echo "• All Docker services stopped" 
echo "• System ready for next demo run"
echo ""
echo -e "${BLUE}To restart:${NC}"
echo "• Run: ./start-demo.sh"
echo "• Quick start: ./start-demo.sh -q"
echo "• With dashboard: ./start-demo.sh -d"