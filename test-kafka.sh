#!/bin/bash

echo "Testing Kafka Docker setup (KRaft mode)..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

echo "Starting Kafka environment..."
# Start only the Kafka-related services, skip data-producer for now
docker-compose up -d kafka kafka-ui

# Wait for services to be ready
echo "Waiting for services to start (60 seconds)..."
sleep 60  # Increased wait time for KRaft mode initialization

# Check if containers are running
echo "Checking container status:"
docker-compose ps

# Create a test topic
echo -e "\nCreating test topic..."
docker exec -it kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# List topics
echo -e "\nListing topics:"
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

echo -e "\nKafka environment is ready!"
echo "You can produce messages with:"
echo "docker exec -it kafka kafka-console-producer --topic test-topic --bootstrap-server localhost:9092"
echo -e "\nYou can consume messages with:"
echo "docker exec -it kafka kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092"
echo -e "\nTo stop the environment, run: docker-compose down"
