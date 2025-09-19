#!/bin/bash

# Velostream Trading Demo - Quick Start
# Builds everything and runs a 1-minute demo

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🚀 Velostream Trading Demo - Quick Start${NC}"
echo -e "${BLUE}===========================================${NC}"
echo ""

# Check if we have make
if command -v make >/dev/null 2>&1; then
    echo -e "${YELLOW}📦 Building main project...${NC}"
    make build
    
    echo -e "${YELLOW}🎬 Starting 1-minute trading demo...${NC}"
    echo -e "${YELLOW}    (To run longer, use: DEMO_DURATION=5 ./run_demo.sh)${NC}"
    echo ""
    
    DEMO_DURATION=1 ./run_demo.sh
else
    echo -e "${YELLOW}📦 Building CLI (no make found)...${NC}"
    ./build_cli.sh
    
    echo -e "${YELLOW}🎬 Starting 1-minute trading demo...${NC}"
    echo ""
    
    DEMO_DURATION=1 ./run_demo.sh
fi

echo -e "${GREEN}🎉 Quick start demo completed!${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "• Full demo: ${YELLOW}./run_demo.sh${NC}"
echo "• CLI monitoring: ${YELLOW}./velo-cli status${NC}"
echo "• Grafana dashboards: ${YELLOW}http://localhost:3000${NC} (admin/admin)"
echo "• Stop services: ${YELLOW}./stop_demo.sh${NC}"