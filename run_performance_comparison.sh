#!/bin/bash

# Performance Comparison Script
# Runs both JSON and raw bytes performance tests for comparison

set -e

echo "🚀 Kafka Performance Testing Suite"
echo "=================================="
echo ""

# Check if Kafka is running
echo "🔍 Checking Kafka availability..."
if ! timeout 5 bash -c '</dev/tcp/localhost/9092' 2>/dev/null; then
    echo "❌ Kafka is not running on localhost:9092"
    echo "Please start Kafka before running performance tests"
    echo ""
    echo "Quick start with Docker:"
    echo "docker-compose up -d"
    exit 1
fi
echo "✅ Kafka is available"
echo ""

# Run JSON performance test
echo "📊 Running JSON Performance Test (with serialization overhead)..."
echo "================================================================"
cargo run --example json_performance_test --release
echo ""

# Brief pause between tests
sleep 3

# Run raw bytes performance test  
echo "🔥 Running Raw Bytes Performance Test (no serialization)..."
echo "=========================================================="
cargo run --example raw_bytes_performance_test --release
echo ""

# Brief pause
sleep 2

# Run latency test
echo "⚡ Running Latency Performance Test..."
echo "====================================="
cargo run --example latency_performance_test --release
echo ""

# Run advanced optimization tests
echo "🔧 Running Advanced Optimization Tests..."
echo "========================================"
echo "Zero-Copy Memory Optimization:"
cargo run --example simple_zero_copy_test --release
echo ""

echo "Async I/O Optimization:"
cargo run --example simple_async_optimization_test --release
echo ""

echo "Resource Monitoring:"
cargo run --example resource_monitoring_test --release
echo ""

echo "🏁 Performance comparison completed!"
echo ""
echo "💡 Key Differences:"
echo "   • JSON Test: Uses typed messages with JSON serialization"
echo "   • Raw Test: Uses raw bytes with consumer.raw_stream()"
echo "   • Raw test should show significantly higher throughput"
echo "   • Both use optimized Kafka configurations for maximum performance"