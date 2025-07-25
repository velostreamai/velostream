#!/bin/bash
# Test script for type-safe Kafka functionality

set -e

echo "🔧 Testing Type-Safe Kafka Implementation"
echo "========================================="

# Check if Kafka is running
if ! nc -z localhost 9092 2>/dev/null; then
    echo "❌ Kafka is not running on localhost:9092"
    echo "💡 Start Kafka with: docker-compose up -d"
    echo "💡 Or use: ./test-kafka.sh to start Kafka and wait for readiness"
    exit 1
fi

echo "✅ Kafka is running on localhost:9092"
echo ""

# Compile tests
echo "🔨 Compiling typed Kafka tests..."
cargo test typed_kafka_func_test --no-run
echo "✅ Compilation successful"
echo ""

# Run specific functional tests with output
echo "🧪 Running Type-Safe Kafka Functional Tests"
echo "============================================="

echo ""
echo "Test 1: Basic Producer/Consumer with JSON serialization"
echo "-------------------------------------------------------"
cargo test typed_kafka_func_test::test_typed_producer_consumer_basic -- --nocapture

echo ""
echo "Test 2: Builder Pattern Usage"  
echo "------------------------------"
cargo test typed_kafka_func_test::test_typed_producer_builder -- --nocapture

echo ""
echo "Test 3: KafkaConsumable Trait"
echo "------------------------------" 
cargo test typed_kafka_func_test::test_consumable_trait -- --nocapture

echo ""
echo "Test 4: Multiple Message Types"
echo "-------------------------------"
cargo test typed_kafka_func_test::test_multiple_message_types -- --nocapture

echo ""
echo "Test 5: Custom Topic Routing"
echo "-----------------------------"
cargo test typed_kafka_func_test::test_send_to_specific_topic -- --nocapture

echo ""
echo "Test 6: TypedMessage API"
echo "------------------------"
cargo test typed_kafka_func_test::test_typed_message_methods -- --nocapture

echo ""
echo "Test 7: Error Scenarios"
echo "------------------------"
cargo test typed_kafka_func_test::test_error_scenarios -- --nocapture

echo ""
echo "🎉 All Type-Safe Kafka tests completed successfully!"
echo ""
echo "✨ Key Features Verified:"
echo "   - Compile-time type safety"
echo "   - Automatic serialization/deserialization" 
echo "   - Builder pattern support"
echo "   - Multiple serialization formats"
echo "   - Error handling and timeouts"
echo "   - Custom topic routing"
echo "   - Convenience trait methods"
echo ""
echo "📖 See docs/TYPE_SAFE_KAFKA.md for documentation and examples"