#!/bin/bash

# FerrisStreams SQL Docker Deployment Script
set -e

echo "🚀 FerrisStreams SQL Docker Deployment"
echo "====================================="

# Configuration
COMPOSE_FILE="docker-compose.yml"
PROJECT_NAME="ferrisstreams-sql"
MONITORING_ENABLED=false
CLEANUP=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --monitoring)
            MONITORING_ENABLED=true
            echo "📊 Monitoring enabled"
            shift
            ;;
        --cleanup)
            CLEANUP=true
            echo "🧹 Cleanup mode enabled"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --monitoring    Enable Prometheus and Grafana monitoring"
            echo "  --cleanup       Remove all containers and volumes"
            echo "  --help          Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Cleanup if requested
if [ "$CLEANUP" = true ]; then
    echo "🧹 Cleaning up existing deployment..."
    docker-compose -p $PROJECT_NAME down -v --remove-orphans
    docker system prune -f
    echo "✅ Cleanup completed"
    exit 0
fi

# Check prerequisites
echo "🔍 Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose not found. Please install Docker Compose first."
    exit 1
fi

# Check if docker daemon is running
if ! docker info &> /dev/null; then
    echo "❌ Docker daemon is not running. Please start Docker first."
    exit 1
fi

echo "✅ Prerequisites check passed"

# Check available resources
echo "🔧 Checking system resources..."
AVAILABLE_MEMORY=$(docker run --rm alpine free -m | awk 'NR==2{printf "%.0f", $7}')
if [ "$AVAILABLE_MEMORY" -lt 4096 ]; then
    echo "⚠️  Warning: Available memory is ${AVAILABLE_MEMORY}MB. Recommended: 4GB+"
fi

# Build images
echo "🔨 Building FerrisStreams SQL images..."
docker-compose build ferris-sql-single ferris-sql-multi data-producer

if [ $? -ne 0 ]; then
    echo "❌ Build failed. Please check the build logs."
    exit 1
fi

echo "✅ Images built successfully"

# Start services
echo "🚀 Starting FerrisStreams SQL infrastructure..."

if [ "$MONITORING_ENABLED" = true ]; then
    echo "📊 Starting with monitoring services..."
    docker-compose -p $PROJECT_NAME --profile monitoring up -d
else
    echo "🎯 Starting core services..."
    docker-compose -p $PROJECT_NAME up -d kafka kafka-ui ferris-sql-single ferris-sql-multi
fi

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check service health
echo "🏥 Checking service health..."
SERVICES=("ferris-kafka" "ferris-sql-single" "ferris-sql-multi")

for SERVICE in "${SERVICES[@]}"; do
    if docker ps --filter "name=$SERVICE" --filter "status=running" | grep -q $SERVICE; then
        echo "✅ $SERVICE is running"
    else
        echo "❌ $SERVICE is not running"
        docker-compose -p $PROJECT_NAME logs $SERVICE | tail -20
    fi
done

# Test Kafka connectivity
echo "🔗 Testing Kafka connectivity..."
if docker exec ferris-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &> /dev/null; then
    echo "✅ Kafka is accessible"
else
    echo "❌ Kafka connectivity test failed"
    docker-compose -p $PROJECT_NAME logs kafka | tail -20
fi

# Display service URLs
echo ""
echo "🎉 Deployment Complete!"
echo "====================="
echo ""
echo "📋 Service URLs:"
echo "  • Kafka UI:               http://localhost:8090"
echo "  • SQL Single Server:      http://localhost:8080"
echo "  • SQL Multi-Job Server:   http://localhost:8081"

if [ "$MONITORING_ENABLED" = true ]; then
    echo "  • Prometheus:             http://localhost:9093"
    echo "  • Grafana:                http://localhost:3000 (admin/ferris123)"
fi

echo ""
echo "🔧 Service Management:"
echo "  • View logs:              docker-compose -p $PROJECT_NAME logs <service>"
echo "  • Stop services:          docker-compose -p $PROJECT_NAME down"
echo "  • Restart service:        docker-compose -p $PROJECT_NAME restart <service>"
echo ""

# Example usage
echo "📚 Example Usage:"
echo ""
echo "1. Execute a SQL query:"
echo "   docker exec ferris-sql-single ferris-sql execute \\"
echo "     --query \"SELECT * FROM orders WHERE amount > 100\" \\"
echo "     --topic orders \\"
echo "     --brokers kafka:9092"
echo ""
echo "2. Deploy a SQL application:"
echo "   docker exec ferris-sql-multi ferris-sql-multi deploy-app \\"
echo "     --file /app/examples/ecommerce_analytics.sql \\"
echo "     --brokers kafka:9092 \\"
echo "     --default-topic orders"
echo ""
echo "3. Access data producer:"
echo "   docker exec -it ferris-data-producer bash"
echo ""

# Show next steps
echo "🎯 Next Steps:"
echo "  1. Create test topics and data using Kafka UI"
echo "  2. Deploy SQL applications from /examples"
echo "  3. Monitor job execution through logs and metrics"
if [ "$MONITORING_ENABLED" = true ]; then
    echo "  4. Set up custom Grafana dashboards for your metrics"
fi
echo ""

echo "📖 Documentation:"
echo "  • Docker Deployment Guide: docs/DOCKER_DEPLOYMENT_GUIDE.md"
echo "  • SQL Reference:            docs/SQL_REFERENCE_GUIDE.md"
echo "  • Multi-Job Guide:          MULTI_JOB_SQL_GUIDE.md"
echo ""

echo "🎊 FerrisStreams SQL is ready for streaming analytics!"