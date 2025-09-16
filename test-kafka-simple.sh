#!/bin/bash

echo "Starting simple Kafka setup (KRaft mode)..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

# Stop any existing containers
echo "Cleaning up any existing containers..."
docker rm -f velo-kafka velo-kafka-ui 2>/dev/null || true

# Start Kafka with KRaft
echo "Starting Kafka..."
docker run -d \
  --name velo-kafka \
  -p 9092:9092 \
  -p 9093:9093 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_NODE_ID=1 \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  confluentinc/cp-kafka:7.4.3

# Wait for Kafka to start
echo "Waiting for Kafka to start (30 seconds)..."
sleep 30

# Check if Kafka is running
if docker exec velo-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "✅ Kafka is running successfully!"
else
    echo "❌ Kafka failed to start properly"
    docker logs velo-kafka --tail 50
    exit 1
fi

# Start Kafka UI
echo "Starting Kafka UI..."
docker run -d \
  --name velo-kafka-ui \
  --link velo-kafka:kafka \
  -p 8090:8080 \
  -e KAFKA_CLUSTERS_0_NAME=local \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092 \
  -e DYNAMIC_CONFIG_ENABLED=true \
  provectuslabs/kafka-ui:latest

# Create a test topic
echo -e "\nCreating test topic..."
docker exec velo-kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# List topics
echo -e "\nListing topics:"
docker exec velo-kafka kafka-topics --list --bootstrap-server localhost:9092

echo -e "\n✅ Simple Kafka environment is ready!"
echo "Kafka: localhost:9092"
echo "Kafka UI: http://localhost:8090"
echo -e "\nTo produce messages:"
echo "docker exec -it velo-kafka kafka-console-producer --topic test-topic --bootstrap-server localhost:9092"
echo -e "\nTo consume messages:"
echo "docker exec -it velo-kafka kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092"
echo -e "\nTo stop:"
echo "docker rm -f velo-kafka velo-kafka-ui"