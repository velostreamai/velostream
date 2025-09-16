#!/bin/bash

# Setup script for VeloStream Trading Dashboard

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🏦 VeloStream Trading Dashboard Setup${NC}"
echo -e "${BLUE}========================================${NC}"

# Check Python version
echo -e "${YELLOW}🐍 Checking Python version...${NC}"
python3 --version

# Check if pip is available
if ! command -v pip &> /dev/null; then
    if ! command -v pip3 &> /dev/null; then
        echo "❌ pip not found. Please install pip first."
        exit 1
    else
        alias pip=pip3
    fi
fi

# Create virtual environment if it doesn't exist
if [ ! -d "dashboard_env" ]; then
    echo -e "${YELLOW}🔧 Creating Python virtual environment...${NC}"
    python3 -m venv dashboard_env
    echo -e "${GREEN}✓ Virtual environment created${NC}"
fi

# Install Python dependencies in virtual environment
echo -e "${YELLOW}📦 Installing Python dependencies in virtual environment...${NC}"
source dashboard_env/bin/activate && pip install -r requirements.txt

echo -e "${GREEN}✅ Dashboard setup complete!${NC}"
echo ""
echo -e "${BLUE}Usage:${NC}"
echo "  source dashboard_env/bin/activate       # Activate virtual environment"
echo "  python3 dashboard.py                    # Use default Kafka brokers (localhost:9092)"
echo "  python3 dashboard.py --brokers HOST:PORT # Use custom Kafka brokers"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "1. Start the trading demo: ./run_demo.sh"
echo "2. In another terminal, activate environment: source dashboard_env/bin/activate"
echo "3. Run the dashboard: python3 dashboard.py"
echo "4. Watch real-time trading analytics!"
echo ""
echo -e "${YELLOW}💡 Remember: Always activate the virtual environment first!${NC}"
echo "Look for (dashboard_env) in your terminal prompt."