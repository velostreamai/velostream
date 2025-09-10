#!/bin/bash

# FerrisStreams SQL Deployment Script
set -e

echo "🚀 FerrisStreams SQL Deployment"
echo "================================"

# Configuration
BINARY_NAME="ferris-sql"
BUILD_MODE="release"
INSTALL_DIR="/usr/local/bin"
CONFIG_FILE="configs/ferris-default.yaml"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dev)
            BUILD_MODE="debug"
            echo "📝 Development mode enabled"
            shift
            ;;
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --dev              Build in debug mode"
            echo "  --install-dir DIR  Installation directory (default: $INSTALL_DIR)"
            echo "  --help             Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check prerequisites
echo "🔍 Checking prerequisites..."

if ! command -v cargo &> /dev/null; then
    echo "❌ Rust/Cargo not found. Please install Rust first."
    exit 1
fi

if ! command -v kafka-console-consumer &> /dev/null; then
    echo "⚠️  Kafka tools not found. Make sure Kafka is installed and accessible."
fi

# Build the binary
echo "🔨 Building $BINARY_NAME..."
if [ "$BUILD_MODE" = "release" ]; then
    cargo build --release --bin $BINARY_NAME
    BINARY_PATH="target/release/$BINARY_NAME"
else
    cargo build --bin $BINARY_NAME
    BINARY_PATH="target/debug/$BINARY_NAME"
fi

if [ ! -f "$BINARY_PATH" ]; then
    echo "❌ Build failed. Binary not found at $BINARY_PATH"
    exit 1
fi

echo "✅ Build successful: $BINARY_PATH"

# Test the binary
echo "🧪 Testing binary..."
if ! $BINARY_PATH --help &> /dev/null; then
    echo "❌ Binary test failed"
    exit 1
fi

echo "✅ Binary test passed"

# Install (optional)
if [ -w "$INSTALL_DIR" ] || [ "$EUID" -eq 0 ]; then
    echo "📦 Installing to $INSTALL_DIR..."
    cp "$BINARY_PATH" "$INSTALL_DIR/"
    chmod +x "$INSTALL_DIR/$BINARY_NAME"
    echo "✅ Installation complete: $INSTALL_DIR/$BINARY_NAME"
else
    echo "⚠️  Cannot install to $INSTALL_DIR (permission denied)"
    echo "💡 You can manually copy: cp $BINARY_PATH $INSTALL_DIR/"
    echo "💡 Or run with sudo: sudo $0"
fi

# Configuration
if [ -f "$CONFIG_FILE" ]; then
    echo "📝 Configuration file found: $CONFIG_FILE"
else
    echo "⚠️  Configuration file not found: $CONFIG_FILE"
    echo "💡 A sample configuration is available in the repository"
fi

# Usage examples
echo ""
echo "🎉 Deployment Complete!"
echo "======================"
echo ""
echo "Usage Examples:"
echo ""
echo "1. Execute a SQL query:"
echo "   $BINARY_NAME execute \\"
echo "     --query \"SELECT * FROM orders WHERE amount > 100\" \\"
echo "     --topic orders \\"
echo "     --brokers localhost:9092"
echo ""
echo "2. Start SQL server:"
echo "   $BINARY_NAME server \\"
echo "     --brokers localhost:9092 \\"
echo "     --port 8080"
echo ""
echo "3. Start with custom config:"
echo "   RUST_LOG=info $BINARY_NAME server \\"
echo "     --brokers kafka1:9092,kafka2:9092 \\"
echo "     --group-id production_sql"
echo ""

# Check Kafka connectivity (if possible)
if command -v kafka-topics &> /dev/null; then
    echo "🔗 Testing Kafka connectivity..."
    if kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; then
        echo "✅ Successfully connected to Kafka at localhost:9092"
        echo "📋 Available topics:"
        kafka-topics --bootstrap-server localhost:9092 --list | head -5
    else
        echo "⚠️  Could not connect to Kafka at localhost:9092"
        echo "💡 Make sure Kafka is running or adjust the broker address"
    fi
fi

echo ""
echo "📚 Documentation:"
echo "   - SQL Reference: docs/SQL_REFERENCE_GUIDE.md"
echo "   - Deployment Guide: docs/SQL_DEPLOYMENT_GUIDE.md"
echo "   - Feature Status: docs/SQL_FEATURE_REQUEST.md"
echo ""
echo "🎯 Ready to process Kafka streams with SQL!"