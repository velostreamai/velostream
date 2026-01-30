#!/bin/bash
# Simple data generator for trading demo
# Generates JSON records directly to Kafka

KAFKA_BROKER="${1:-localhost:9092}"
RECORDS="${2:-1000}"
TOPIC="${3:-in_market_data_stream}"

echo "Generating $RECORDS records to $TOPIC on $KAFKA_BROKER..."

# Generate market data records
for i in $(seq 1 $RECORDS); do
    SYMBOLS=("AAPL" "GOOGL" "MSFT" "AMZN" "META" "NVDA" "TSLA")
    EXCHANGES=("NYSE" "NASDAQ" "CBOE")
    
    SYMBOL=${SYMBOLS[$((RANDOM % ${#SYMBOLS[@]}))]}
    EXCHANGE=${EXCHANGES[$((RANDOM % ${#EXCHANGES[@]}))]}
    PRICE=$(echo "scale=4; 100 + $RANDOM / 327" | bc)
    VOLUME=$((RANDOM % 100000 + 1000))
    BID_PRICE=$(echo "scale=4; $PRICE - 0.01" | bc)
    ASK_PRICE=$(echo "scale=4; $PRICE + 0.01" | bc)
    TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S.000000000")
    
    echo "{\"symbol\":\"$SYMBOL\",\"exchange\":\"$EXCHANGE\",\"price\":$PRICE,\"volume\":$VOLUME,\"bid_price\":$BID_PRICE,\"ask_price\":$ASK_PRICE,\"timestamp\":\"$TIMESTAMP\"}"
done | docker exec -i simple-kafka kafka-console-producer --broker-list localhost:9092 --topic $TOPIC 2>/dev/null

echo "Done! Generated $RECORDS records to $TOPIC"
