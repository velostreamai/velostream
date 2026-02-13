use super::*; // Use the re-exported items from integration::mod
use tokio::time::{Duration, sleep};
use velostream::velostream::kafka::consumer_config::IsolationLevel;

/// Test transactional producer with commit
#[tokio::test]
#[ignore] // Disabled: transaction tests are slow and depend on external Kafka
#[serial]
async fn test_transactional_producer_commit() {
    if !is_kafka_running() {
        return;
    }

    // Add minimal delay for CI environment to avoid transaction ID conflicts
    sleep(Duration::from_millis(100)).await;

    let topic = format!("transaction-commit-{}", Uuid::new_v4());
    let transaction_id = format!("tx-{}", Uuid::new_v4());

    // Create transactional producer with longer timeouts for CI
    let config = ProducerConfig::new("localhost:9092", &topic)
        .transactional(transaction_id.clone())
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(2)) // Fast timeout for tests
        .request_timeout(Duration::from_secs(3)); // Fast request timeout

    let producer = match KafkaProducer::<String, TestMessage, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to create transactional producer: {:?}", e);
            eprintln!("This might indicate Kafka transaction support is not properly configured");
            return; // Skip test if transactions not supported
        }
    };

    // Begin transaction with better error handling
    if let Err(e) = producer.begin_transaction().await {
        eprintln!("Failed to begin transaction: {:?}", e);
        eprintln!("This might indicate Kafka transaction coordinator is not available");
        return; // Skip test if transaction operations not supported
    }

    // Send messages within transaction
    let messages = [
        TestMessage::new(1, "Transaction message 1"),
        TestMessage::new(2, "Transaction message 2"),
        TestMessage::new(3, "Transaction message 3"),
    ];

    for (i, message) in messages.iter().enumerate() {
        producer
            .send(
                Some(&format!("tx-key-{}", i)),
                message,
                Headers::new(),
                None,
            )
            .await
            .expect("Failed to send transactional message");
    }

    // Commit transaction
    producer
        .commit_transaction()
        .await
        .expect("Failed to commit transaction");

    // Verify messages are available after commit
    let group_id = format!("tx-consumer-{}", Uuid::new_v4());
    let consumer =
        FastConsumer::<String, TestMessage, JsonSerializer, JsonSerializer>::from_brokers(
            "localhost:9092",
            &group_id,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    sleep(Duration::from_secs(1)).await;

    let mut received_count = 0;
    for _attempt in 1..=5 {
        match consumer.poll(Duration::from_secs(2)).await {
            Ok(message) => {
                received_count += 1;
                let content = &message.value().content;
                assert!(content.starts_with("Transaction message"));
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Consumer error: {:?}", e),
        }
    }

    assert_eq!(
        received_count, 3,
        "Should receive exactly 3 committed messages"
    );
}

/// Test transactional producer with abort
#[tokio::test]
#[ignore]
#[serial]
async fn test_transactional_producer_abort() {
    if !is_kafka_running() {
        return;
    }

    sleep(Duration::from_millis(100)).await;

    let topic = format!("transaction-abort-{}", Uuid::new_v4());
    let transaction_id = format!("tx-abort-{}", Uuid::new_v4());

    let config = ProducerConfig::new("localhost:9092", &topic)
        .transactional(transaction_id)
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(3));

    let producer = match KafkaProducer::<String, TestMessage, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to create transactional producer: {:?}", e);
            return;
        }
    };

    if let Err(e) = producer.begin_transaction().await {
        eprintln!("Failed to begin transaction: {:?}", e);
        return;
    }

    // Send messages within transaction
    let messages = [
        TestMessage::new(10, "Aborted message 1"),
        TestMessage::new(11, "Aborted message 2"),
    ];

    for (i, message) in messages.iter().enumerate() {
        producer
            .send(
                Some(&format!("abort-key-{}", i)),
                message,
                Headers::new(),
                None,
            )
            .await
            .expect("Failed to send message");
    }

    // Abort transaction instead of committing
    producer
        .abort_transaction()
        .await
        .expect("Failed to abort transaction");

    // Verify messages are NOT visible after abort
    let group_id = format!("tx-abort-consumer-{}", Uuid::new_v4());

    let consumer_config = ConsumerConfig::new("localhost:9092", &group_id)
        .isolation_level(IsolationLevel::ReadCommitted);

    let consumer =
        FastConsumer::<String, TestMessage, JsonSerializer, JsonSerializer>::with_config(
            consumer_config,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    sleep(Duration::from_secs(1)).await;

    let mut received_count = 0;
    for _attempt in 1..=3 {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(_) => {
                received_count += 1;
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Consumer error: {:?}", e),
        }
    }

    assert_eq!(
        received_count, 0,
        "Should receive NO messages after abort with READ_COMMITTED isolation"
    );
}

/// Test exactly-once semantics (EOS) with offset commit in transaction
#[tokio::test]
#[ignore]
#[serial]
async fn test_exactly_once_semantics() {
    if !is_kafka_running() {
        return;
    }

    sleep(Duration::from_millis(100)).await;

    let input_topic = format!("eos-input-{}", Uuid::new_v4());
    let output_topic = format!("eos-output-{}", Uuid::new_v4());
    let transaction_id = format!("eos-tx-{}", Uuid::new_v4());

    // Create input producer to send initial messages
    let input_producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &input_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create input producer");

    // Send input messages
    for i in 1..=3 {
        input_producer
            .send(
                Some(&format!("input-key-{}", i)),
                &TestMessage::new(i, &format!("Input message {}", i)),
                Headers::new(),
                None,
            )
            .await
            .expect("Failed to send input message");
    }
    input_producer.flush(5000).expect("Failed to flush");

    sleep(Duration::from_secs(1)).await;

    // Create transactional producer for output
    let producer_config = ProducerConfig::new("localhost:9092", &output_topic)
        .transactional(transaction_id)
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(3));

    let producer = match KafkaProducer::<String, TestMessage, _, _>::with_config(
        producer_config,
        JsonSerializer,
        JsonSerializer,
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to create transactional producer: {:?}", e);
            return;
        }
    };

    // Create consumer with READ_COMMITTED isolation
    let consumer_group_id = format!("eos-consumer-{}", Uuid::new_v4());
    let consumer_config = ConsumerConfig::new("localhost:9092", &consumer_group_id)
        .isolation_level(IsolationLevel::ReadCommitted);

    let consumer =
        FastConsumer::<String, TestMessage, JsonSerializer, JsonSerializer>::with_config(
            consumer_config,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create consumer");

    consumer
        .subscribe(&[&input_topic])
        .expect("Failed to subscribe");

    sleep(Duration::from_secs(1)).await;

    let mut processed_count = 0;

    // Process messages with exactly-once semantics
    for _attempt in 1..=5 {
        match consumer.poll(Duration::from_secs(2)).await {
            Ok(message) => {
                // Begin transaction for each message
                producer
                    .begin_transaction()
                    .await
                    .expect("Failed to begin transaction");

                // Transform the message
                let input_content = &message.value().content;
                let processed_message = TestMessage::new(
                    message.value().id + 100,
                    &format!("Processed: {}", input_content),
                );

                // Send transformed message
                producer
                    .send(message.key(), &processed_message, Headers::new(), None)
                    .await
                    .expect("Failed to send processed message");

                // Get current positions for transaction
                // Note: Producer API currently expects group_id string, not ConsumerGroupMetadata
                // TODO: Update producer to accept ConsumerGroupMetadata for proper EOS implementation
                match consumer.position() {
                    Ok(offsets) => {
                        // Send offsets as part of transaction (exactly-once guarantee)
                        producer
                            .send_offsets_to_transaction(&offsets, consumer.group_id())
                            .await
                            .expect("Failed to send offsets to transaction");

                        // Commit transaction (both message and offset atomically)
                        producer
                            .commit_transaction()
                            .await
                            .expect("Failed to commit transaction");

                        processed_count += 1;
                    }
                    Err(e) => {
                        eprintln!("Failed to get consumer position: {:?}", e);
                        producer.abort_transaction().await.ok();
                        break;
                    }
                }
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => {
                println!("Consumer error: {:?}", e);
                break;
            }
        }
    }

    assert_eq!(processed_count, 3, "Should process exactly 3 messages");

    // Verify output messages with exactly-once guarantee
    let output_group_id = format!("eos-output-consumer-{}", Uuid::new_v4());
    let output_consumer =
        FastConsumer::<String, TestMessage, JsonSerializer, JsonSerializer>::from_brokers(
            "localhost:9092",
            &output_group_id,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create output consumer");

    output_consumer
        .subscribe(&[&output_topic])
        .expect("Failed to subscribe to output");

    sleep(Duration::from_secs(1)).await;

    let mut output_count = 0;
    for _attempt in 1..=5 {
        match output_consumer.poll(Duration::from_secs(2)).await {
            Ok(message) => {
                output_count += 1;
                assert!(message.value().content.starts_with("Processed:"));
                assert!(message.value().id > 100);
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Output consumer error: {:?}", e),
        }
    }

    assert_eq!(
        output_count, 3,
        "Should receive exactly 3 processed messages (no duplicates)"
    );
}

/// Test consumer group metadata retrieval
#[tokio::test]
#[ignore]
#[serial]
async fn test_consumer_group_metadata() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("metadata-test-{}", Uuid::new_v4());
    let group_id = format!("metadata-group-{}", Uuid::new_v4());

    // Create consumer
    let consumer =
        FastConsumer::<String, TestMessage, JsonSerializer, JsonSerializer>::from_brokers(
            "localhost:9092",
            &group_id,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Wait for consumer to join group
    sleep(Duration::from_millis(500)).await;

    // Verify group_id() returns correct value
    assert_eq!(consumer.group_id(), group_id);

    // Verify group_metadata() returns metadata after subscription
    let metadata = consumer.group_metadata();
    assert!(
        metadata.is_some(),
        "Group metadata should be available after subscription"
    );

    if let Some(_meta) = metadata {
        println!("Consumer group metadata retrieved:");
        println!("  Group ID: {}", consumer.group_id());
        println!("  Metadata available: true");
    }
}

/// Test position() method for getting current partition positions
#[tokio::test]
#[ignore]
#[serial]
async fn test_consumer_position() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("position-test-{}", Uuid::new_v4());
    let group_id = format!("position-group-{}", Uuid::new_v4());

    // Create producer and send some messages
    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    for i in 1..=5 {
        producer
            .send(
                Some(&format!("key-{}", i)),
                &TestMessage::new(i, &format!("Message {}", i)),
                Headers::new(),
                None,
            )
            .await
            .expect("Failed to send message");
    }
    producer.flush(5000).expect("Failed to flush");

    sleep(Duration::from_millis(500)).await;

    // Create consumer
    let consumer =
        FastConsumer::<String, TestMessage, JsonSerializer, JsonSerializer>::from_brokers(
            "localhost:9092",
            &group_id,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    sleep(Duration::from_millis(500)).await;

    // Consume some messages
    let mut consumed = 0;
    for _ in 0..3 {
        if let Ok(_) = consumer.poll(Duration::from_secs(2)).await {
            consumed += 1;
        }
    }

    assert!(consumed > 0, "Should have consumed at least one message");

    // Get current positions
    match consumer.position() {
        Ok(positions) => {
            println!("Current consumer positions:");
            println!("  Consumed {} messages", consumed);
            println!("  Position data available: true");
            assert!(
                positions.count() > 0,
                "Position should include partition information"
            );
        }
        Err(e) => {
            panic!("Failed to get consumer position: {:?}", e);
        }
    }
}

/// Test transactional producer with multiple transactions
#[tokio::test]
#[ignore]
#[serial]
async fn test_multiple_transactions() {
    if !is_kafka_running() {
        return;
    }

    sleep(Duration::from_millis(100)).await;

    let topic = format!("multi-tx-{}", Uuid::new_v4());
    let transaction_id = format!("multi-tx-{}", Uuid::new_v4());

    let config = ProducerConfig::new("localhost:9092", &topic)
        .transactional(transaction_id)
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(3));

    let producer = match KafkaProducer::<String, TestMessage, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to create transactional producer: {:?}", e);
            return;
        }
    };

    // Execute multiple transactions
    for tx_num in 1..=3 {
        producer
            .begin_transaction()
            .await
            .expect("Failed to begin transaction");

        producer
            .send(
                Some(&format!("tx{}-key", tx_num)),
                &TestMessage::new(tx_num, &format!("Transaction {} message", tx_num)),
                Headers::new(),
                None,
            )
            .await
            .expect("Failed to send message");

        producer
            .commit_transaction()
            .await
            .expect("Failed to commit transaction");
    }

    // Verify all 3 transactions were committed
    let group_id = format!("multi-tx-consumer-{}", Uuid::new_v4());
    let consumer =
        FastConsumer::<String, TestMessage, JsonSerializer, JsonSerializer>::from_brokers(
            "localhost:9092",
            &group_id,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    sleep(Duration::from_secs(1)).await;

    let mut received_count = 0;
    for _attempt in 1..=5 {
        match consumer.poll(Duration::from_secs(2)).await {
            Ok(_) => {
                received_count += 1;
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Consumer error: {:?}", e),
        }
    }

    assert_eq!(
        received_count, 3,
        "Should receive messages from all 3 transactions"
    );
}
