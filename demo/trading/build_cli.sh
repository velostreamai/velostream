#!/bin/bash

# Build VeloStream CLI Tool
# Makes the velo-cli available for use in the trading demo

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🔨 Building VeloStream CLI Tool${NC}"
echo -e "${BLUE}=================================${NC}"
echo ""

# Check if we're in the right directory
if [ ! -f "../../Cargo.toml" ]; then
    echo -e "${RED}❌ Error: Must be run from demo/trading directory${NC}"
    echo "Current directory: $(pwd)"
    exit 1
fi

# Build the CLI in release mode
echo -e "${YELLOW}📦 Building velo-cli in release mode...${NC}"
cd ../..
cargo build --release --bin velo-cli

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Build successful!${NC}"
else
    echo -e "${RED}❌ Build failed!${NC}"
    exit 1
fi

# Go back to demo directory
cd demo/trading

# Create symlink for easy access
echo -e "${YELLOW}🔗 Creating convenient access link...${NC}"
if [ -L "./velo-cli" ]; then
    rm ./velo-cli
fi
ln -s ../../target/release/velo-cli ./velo-cli

# Make sure it's executable
chmod +x ../../target/release/velo-cli

echo -e "${GREEN}🎉 VeloStream CLI ready!${NC}"
echo ""
echo -e "${BLUE}Usage:${NC}"
echo "• Quick health check: ${YELLOW}./velo-cli health${NC}"
echo "• System status: ${YELLOW}./velo-cli status --verbose${NC}"
echo "• Real-time monitoring: ${YELLOW}./velo-cli status --refresh 5${NC}"
echo "• Kafka topics: ${YELLOW}./velo-cli kafka --topics${NC}"
echo "• Docker containers: ${YELLOW}./velo-cli docker --velo-only${NC}"
echo "• Full help: ${YELLOW}./velo-cli --help${NC}"
echo ""
echo -e "${GREEN}✨ You can now use './velo-cli' directly from the demo/trading directory!${NC}"