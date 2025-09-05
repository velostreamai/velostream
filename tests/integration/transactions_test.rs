use super::*; // Use the re-exported items from integration::mod
use ferrisstreams::ferris::kafka::consumer_config::IsolationLevel;
use futures::StreamExt;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// Test transactional producer with commit
#[tokio::test]
#[serial]
async fn test_transactional_producer_commit() {
    if !is_kafka_running() {
        return;
    }

    // Add delay for CI environment to avoid transaction ID conflicts
    sleep(Duration::from_secs(2)).await;

    let topic = format!("transaction-commit-{}", Uuid::new_v4());
    let transaction_id = format!("tx-{}", Uuid::new_v4());

    // Create transactional producer with longer timeouts for CI
    let config = ProducerConfig::new("localhost:9092", &topic)
        .transactional(transaction_id.clone())
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(30)) // Longer timeout for CI
        .request_timeout(Duration::from_secs(10)); // Longer request timeout

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
    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
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

    assert_eq!(received_count, 3, "Should receive all committed messages");
}

/// Test transactional producer with abort
#[tokio::test]
#[serial]
async fn test_transactional_producer_abort() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("transaction-abort-{}", Uuid::new_v4());
    let transaction_id = format!("tx-abort-{}", Uuid::new_v4());

    // Create transactional producer
    let config = ProducerConfig::new("localhost:9092", &topic)
        .transactional(transaction_id.clone())
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(10));

    let producer = KafkaProducer::<String, TestMessage, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create transactional producer");

    // Begin transaction
    producer
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Send messages within transaction
    let messages = [
        TestMessage::new(1, "Aborted message 1"),
        TestMessage::new(2, "Aborted message 2"),
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
            .expect("Failed to send transactional message");
    }

    // Abort transaction instead of committing
    producer
        .abort_transaction()
        .await
        .expect("Failed to abort transaction");

    // Verify messages are NOT available after abort
    let group_id = format!("abort-consumer-{}", Uuid::new_v4());
    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    sleep(Duration::from_secs(1)).await;

    let mut received_count = 0;
    for _attempt in 1..=3 {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(_) => received_count += 1,
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Consumer error: {:?}", e),
        }
    }

    assert_eq!(received_count, 0, "Should not receive any aborted messages");
}

/// Test exactly-once semantics with multiple producers
#[tokio::test]
#[serial]
async fn test_exactly_once_semantics() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("exactly-once-{}", Uuid::new_v4());
    let transaction_id_1 = format!("tx-eos-1-{}", Uuid::new_v4());
    let transaction_id_2 = format!("tx-eos-2-{}", Uuid::new_v4());

    // Create two transactional producers
    let config1 = ProducerConfig::new("localhost:9092", &topic)
        .transactional(transaction_id_1)
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(10));

    let config2 = ProducerConfig::new("localhost:9092", &topic)
        .transactional(transaction_id_2)
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(10));

    let producer1 = Arc::new(
        KafkaProducer::<String, TestMessage, _, _>::with_config(
            config1,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create producer 1"),
    );

    let producer2 = Arc::new(
        KafkaProducer::<String, TestMessage, _, _>::with_config(
            config2,
            JsonSerializer,
            JsonSerializer,
        )
        .expect("Failed to create producer 2"),
    );

    // Concurrent transactions
    let producer1_clone: Arc<KafkaProducer<String, TestMessage, _, _>> = Arc::clone(&producer1);
    let producer2_clone: Arc<KafkaProducer<String, TestMessage, _, _>> = Arc::clone(&producer2);

    let handle1 = tokio::spawn(async move {
        producer1_clone
            .begin_transaction()
            .await
            .expect("Failed to begin tx1");

        for i in 0..3 {
            let message = TestMessage::new(i, &format!("Producer1 message {}", i));
            producer1_clone
                .send(
                    Some(&format!("p1-key-{}", i)),
                    &message,
                    Headers::new(),
                    None,
                )
                .await
                .expect("Failed to send from producer1");
        }

        producer1_clone
            .commit_transaction()
            .await
            .expect("Failed to commit tx1");
    });

    let handle2 = tokio::spawn(async move {
        producer2_clone
            .begin_transaction()
            .await
            .expect("Failed to begin tx2");

        for i in 0..2 {
            let message = TestMessage::new(i + 10, &format!("Producer2 message {}", i));
            producer2_clone
                .send(
                    Some(&format!("p2-key-{}", i)),
                    &message,
                    Headers::new(),
                    None,
                )
                .await
                .expect("Failed to send from producer2");
        }

        producer2_clone
            .commit_transaction()
            .await
            .expect("Failed to commit tx2");
    });

    // Wait for both transactions to complete
    handle1.await.expect("Producer1 task failed");
    handle2.await.expect("Producer2 task failed");

    // Verify all messages are received exactly once
    let group_id = format!("eos-consumer-{}", Uuid::new_v4());
    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    sleep(Duration::from_secs(2)).await;

    let mut received_messages = Vec::new();
    for _attempt in 1..=10 {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(message) => {
                received_messages.push(message.value().content.clone());
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Consumer error: {:?}", e),
        }
    }

    // Should receive exactly 5 messages (3 from producer1 + 2 from producer2)
    assert_eq!(
        received_messages.len(),
        5,
        "Should receive exactly 5 messages"
    );

    // Verify no duplicates
    let mut unique_messages = received_messages.clone();
    unique_messages.sort();
    unique_messages.dedup();
    assert_eq!(
        unique_messages.len(),
        received_messages.len(),
        "Should have no duplicate messages"
    );
}

/// Test transaction timeout and recovery
#[tokio::test]
#[serial]
async fn test_transaction_timeout() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("transaction-timeout-{}", Uuid::new_v4());
    let transaction_id = format!("tx-timeout-{}", Uuid::new_v4());

    // Create transactional producer with short timeout
    let config = ProducerConfig::new("localhost:9092", &topic)
        .transactional(transaction_id.clone())
        .transaction_timeout(Duration::from_secs(5))
        .idempotence(true)
        .acks(AckMode::All);

    let producer = KafkaProducer::<String, TestMessage, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create transactional producer");

    // Begin transaction
    producer
        .begin_transaction()
        .await
        .expect("Failed to begin transaction");

    // Send a message
    let message = TestMessage::new(1, "Timeout test message");
    producer
        .send(
            Some(&"timeout-key".to_string()),
            &message,
            Headers::new(),
            None,
        )
        .await
        .expect("Failed to send message");

    // Wait longer than transaction timeout
    sleep(Duration::from_secs(6)).await;

    // Try to commit - should handle timeout gracefully
    match producer.commit_transaction().await {
        Ok(_) => println!("Transaction committed successfully"),
        Err(e) => {
            println!("Transaction failed as expected due to timeout: {:?}", e);
            // Try to abort the transaction to clean up
            let _ = producer.abort_transaction().await;
        }
    }

    // Verify producer can start a new transaction after timeout
    producer
        .begin_transaction()
        .await
        .expect("Failed to begin new transaction after timeout");

    let recovery_message = TestMessage::new(2, "Recovery message");
    producer
        .send(
            Some(&"recovery-key".to_string()),
            &recovery_message,
            Headers::new(),
            None,
        )
        .await
        .expect("Failed to send recovery message");

    producer
        .commit_transaction()
        .await
        .expect("Failed to commit recovery transaction");
}

/// Test true exactly-once semantics with consumer-producer coordination
#[tokio::test]
#[serial]
async fn test_exactly_once_consumer_producer_coordination() {
    if !is_kafka_running() {
        return;
    }

    let input_topic = format!("eos-input-{}", Uuid::new_v4());
    let output_topic = format!("eos-output-{}", Uuid::new_v4());
    let group_id = format!("eos-processor-{}", Uuid::new_v4());
    let transaction_id = format!("eos-tx-{}", Uuid::new_v4());

    // Create source data producer (non-transactional)
    let source_producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &input_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create source producer");

    // Send source messages
    let input_messages = [
        TestMessage::new(1, "Source message 1"),
        TestMessage::new(2, "Source message 2"),
        TestMessage::new(3, "Source message 3"),
    ];

    for (i, message) in input_messages.iter().enumerate() {
        source_producer
            .send(
                Some(&format!("source-key-{}", i)),
                message,
                Headers::new(),
                None,
            )
            .await
            .expect("Failed to send source message");
    }
    source_producer
        .flush(5000)
        .expect("Failed to flush source messages");

    // Create exactly-once processor (consumer + transactional producer)
    let consumer_config = ConsumerConfig::new("localhost:9092", &group_id)
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_commit(false, Duration::from_secs(5));

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    let producer_config = ProducerConfig::new("localhost:9092", &output_topic)
        .transactional(&transaction_id)
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(10));

    let producer = KafkaProducer::<String, TestMessage, _, _>::with_config(
        producer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create transactional producer");

    consumer
        .subscribe(&[&input_topic])
        .expect("Failed to subscribe");

    sleep(Duration::from_secs(1)).await;

    // Process messages with exactly-once semantics
    let mut processed_count = 0;
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

                // Get current offsets for transaction coordination
                let offsets = consumer.current_offsets().expect("Failed to get offsets");

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
    let output_consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
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

    let mut output_messages = Vec::new();
    for _attempt in 1..=5 {
        match output_consumer.poll(Duration::from_secs(1)).await {
            Ok(message) => {
                output_messages.push(message.value().content.clone());
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Output consumer error: {:?}", e),
        }
    }

    // Verify exactly-once processing
    assert_eq!(
        output_messages.len(),
        3,
        "Should have exactly 3 output messages"
    );

    for (i, output_content) in output_messages.iter().enumerate() {
        assert!(
            output_content.starts_with("Processed: Source message"),
            "Message {} should be processed: {}",
            i,
            output_content
        );
    }

    // Verify no duplicates
    let mut unique_messages = output_messages.clone();
    unique_messages.sort();
    unique_messages.dedup();
    assert_eq!(
        unique_messages.len(),
        output_messages.len(),
        "Should have no duplicate messages in exactly-once processing"
    );
}

/// Test exactly-once processing with failure recovery
#[tokio::test]
#[serial]
async fn test_exactly_once_with_failure_recovery() {
    if !is_kafka_running() {
        return;
    }

    let input_topic = format!("eos-recovery-input-{}", Uuid::new_v4());
    let output_topic = format!("eos-recovery-output-{}", Uuid::new_v4());
    let group_id = format!("eos-recovery-group-{}", Uuid::new_v4());
    let transaction_id = format!("eos-recovery-tx-{}", Uuid::new_v4());

    // Setup source data
    let source_producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &input_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create source producer");

    let input_message = TestMessage::new(42, "Recovery test message");
    source_producer
        .send(
            Some(&"recovery-key".to_string()),
            &input_message,
            Headers::new(),
            None,
        )
        .await
        .expect("Failed to send source message");
    source_producer
        .flush(5000)
        .expect("Failed to flush source message");

    // Create processor components
    let consumer_config = ConsumerConfig::new("localhost:9092", &group_id)
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_commit(false, Duration::from_secs(5));

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    let producer_config = ProducerConfig::new("localhost:9092", &output_topic)
        .transactional(&transaction_id)
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(10));

    let producer = KafkaProducer::<String, TestMessage, _, _>::with_config(
        producer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create transactional producer");

    consumer
        .subscribe(&[&input_topic])
        .expect("Failed to subscribe");

    sleep(Duration::from_secs(1)).await;

    // First processing attempt - simulate failure after sending but before commit
    let mut processing_attempts = 0;

    match consumer.poll(Duration::from_secs(2)).await {
        Ok(message) => {
            processing_attempts += 1;

            producer
                .begin_transaction()
                .await
                .expect("Failed to begin transaction");

            let processed_message = TestMessage::new(
                message.value().id + 1000,
                &format!("Recovery processed: {}", message.value().content),
            );

            producer
                .send(message.key(), &processed_message, Headers::new(), None)
                .await
                .expect("Failed to send processed message");

            // Simulate failure by aborting transaction instead of committing
            producer
                .abort_transaction()
                .await
                .expect("Failed to abort transaction");

            println!("Simulated failure: transaction aborted");
        }
        Err(e) => panic!("Failed to consume message for recovery test: {:?}", e),
    }

    // Second processing attempt - successful processing
    match consumer.poll(Duration::from_secs(2)).await {
        Ok(message) => {
            processing_attempts += 1;

            producer
                .begin_transaction()
                .await
                .expect("Failed to begin transaction");

            let processed_message = TestMessage::new(
                message.value().id + 1000,
                &format!("Recovery processed: {}", message.value().content),
            );

            producer
                .send(message.key(), &processed_message, Headers::new(), None)
                .await
                .expect("Failed to send processed message");

            let offsets = consumer.current_offsets().expect("Failed to get offsets");
            producer
                .send_offsets_to_transaction(&offsets, consumer.group_id())
                .await
                .expect("Failed to send offsets to transaction");

            producer
                .commit_transaction()
                .await
                .expect("Failed to commit transaction");

            println!("Recovery successful: transaction committed");
        }
        Err(e) => panic!("Failed to consume message for recovery: {:?}", e),
    }

    assert_eq!(
        processing_attempts, 2,
        "Should have processed message twice (failure + recovery)"
    );

    // Verify only one output message exists (exactly-once despite retry)
    let output_group_id = format!("eos-recovery-output-{}", Uuid::new_v4());
    let output_consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
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
    let mut final_message = None;

    for _attempt in 1..=3 {
        match output_consumer.poll(Duration::from_secs(1)).await {
            Ok(message) => {
                output_count += 1;
                final_message = Some(message.value().content.clone());
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Output consumer error: {:?}", e),
        }
    }

    assert_eq!(
        output_count, 1,
        "Should have exactly one output message despite retry"
    );
    assert!(
        final_message.unwrap().contains("Recovery processed"),
        "Output should contain processed message"
    );
}

/// Test exactly-once semantics using consumer stream instead of polling
#[tokio::test]
#[serial]
async fn test_exactly_once_with_consumer_stream() {
    if !is_kafka_running() {
        return;
    }

    let input_topic = format!("eos-stream-input-{}", Uuid::new_v4());
    let output_topic = format!("eos-stream-output-{}", Uuid::new_v4());
    let group_id = format!("eos-stream-group-{}", Uuid::new_v4());
    let transaction_id = format!("eos-stream-tx-{}", Uuid::new_v4());

    // Create source data
    let source_producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &input_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create source producer");

    let input_messages = [
        TestMessage::new(10, "Stream message A"),
        TestMessage::new(20, "Stream message B"),
        TestMessage::new(30, "Stream message C"),
    ];

    for (i, message) in input_messages.iter().enumerate() {
        source_producer
            .send(
                Some(&format!("stream-key-{}", i)),
                message,
                Headers::new(),
                None,
            )
            .await
            .expect("Failed to send source message");
    }
    source_producer
        .flush(5000)
        .expect("Failed to flush source messages");

    // Create exactly-once processor using stream
    let consumer_config = ConsumerConfig::new("localhost:9092", &group_id)
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_commit(false, Duration::from_secs(5));

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    let producer_config = ProducerConfig::new("localhost:9092", &output_topic)
        .transactional(&transaction_id)
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(10));

    let producer = match KafkaProducer::<String, TestMessage, _, _>::with_config(
        producer_config,
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

    consumer
        .subscribe(&[&input_topic])
        .expect("Failed to subscribe");

    sleep(Duration::from_secs(1)).await;

    // Process messages using stream with exactly-once semantics
    let mut stream = consumer.stream();
    let mut processed_count = 0;

    while let Some(message_result) = stream.next().await {
        match message_result {
            Ok(message) => {
                // Begin transaction for each message with error handling
                if let Err(e) = producer.begin_transaction().await {
                    eprintln!("Failed to begin transaction: {:?}", e);
                    eprintln!("This might indicate Kafka transaction coordinator is not available");
                    return; // Skip test if transaction operations not supported
                }

                // Transform the message
                let input_content = &message.value().content;
                let processed_message = TestMessage::new(
                    message.value().id + 500,
                    &format!("Stream processed: {}", input_content),
                );

                // Send transformed message
                producer
                    .send(message.key(), &processed_message, Headers::new(), None)
                    .await
                    .expect("Failed to send processed message");

                // Get current offsets for transaction coordination
                let offsets = consumer.current_offsets().expect("Failed to get offsets");

                // Send offsets as part of transaction
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

                // Stop after processing all expected messages
                if processed_count >= 3 {
                    break;
                }
            }
            Err(e) => {
                println!("Stream processing error: {:?}", e);
                // Abort any ongoing transaction
                let _ = producer.abort_transaction().await;
                break;
            }
        }
    }

    assert_eq!(
        processed_count, 3,
        "Should process exactly 3 messages via stream"
    );

    // Verify output messages
    let output_group_id = format!("eos-stream-output-{}", Uuid::new_v4());
    let output_consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
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

    let mut output_messages = Vec::new();
    for _attempt in 1..=5 {
        match output_consumer.poll(Duration::from_secs(1)).await {
            Ok(message) => {
                output_messages.push(message.value().content.clone());
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Output consumer error: {:?}", e),
        }
    }

    // Verify exactly-once processing via stream
    assert_eq!(
        output_messages.len(),
        3,
        "Should have exactly 3 output messages from stream processing"
    );

    for (i, output_content) in output_messages.iter().enumerate() {
        assert!(
            output_content.starts_with("Stream processed: Stream message"),
            "Message {} should be stream processed: {}",
            i,
            output_content
        );
    }

    // Verify no duplicates
    let mut unique_messages = output_messages.clone();
    unique_messages.sort();
    unique_messages.dedup();
    assert_eq!(
        unique_messages.len(),
        output_messages.len(),
        "Should have no duplicate messages in stream-based exactly-once processing"
    );
}

/// Test exactly-once with stream and error handling/recovery
#[tokio::test]
#[serial]
async fn test_exactly_once_stream_with_error_handling() {
    if !is_kafka_running() {
        return;
    }

    let input_topic = format!("eos-stream-error-input-{}", Uuid::new_v4());
    let output_topic = format!("eos-stream-error-output-{}", Uuid::new_v4());
    let group_id = format!("eos-stream-error-group-{}", Uuid::new_v4());
    let transaction_id = format!("eos-stream-error-tx-{}", Uuid::new_v4());

    // Create source data including a "bad" message
    let source_producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &input_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create source producer");

    let input_messages = [
        TestMessage::new(1, "Good message 1"),
        TestMessage::new(2, "Bad message"), // This will trigger simulated error
        TestMessage::new(3, "Good message 2"),
    ];

    for (i, message) in input_messages.iter().enumerate() {
        source_producer
            .send(
                Some(&format!("error-key-{}", i)),
                message,
                Headers::new(),
                None,
            )
            .await
            .expect("Failed to send source message");
    }
    source_producer
        .flush(5000)
        .expect("Failed to flush source messages");

    // Create processor
    let consumer_config = ConsumerConfig::new("localhost:9092", &group_id)
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_commit(false, Duration::from_secs(5));

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    let producer_config = ProducerConfig::new("localhost:9092", &output_topic)
        .transactional(&transaction_id)
        .idempotence(true)
        .acks(AckMode::All)
        .transaction_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(10));

    let producer = KafkaProducer::<String, TestMessage, _, _>::with_config(
        producer_config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create transactional producer");

    consumer
        .subscribe(&[&input_topic])
        .expect("Failed to subscribe");

    sleep(Duration::from_secs(1)).await;

    // Process with stream and error handling
    let mut stream = consumer.stream();
    let mut processed_count = 0;
    let mut error_count = 0;

    while let Some(message_result) = stream.next().await {
        match message_result {
            Ok(message) => {
                let input_content = &message.value().content;

                // Simulate processing error for "bad" messages
                if input_content.contains("Bad message") {
                    error_count += 1;
                    println!("Simulating processing error for: {}", input_content);

                    // Begin transaction but then abort due to processing error
                    producer
                        .begin_transaction()
                        .await
                        .expect("Failed to begin transaction");
                    producer
                        .abort_transaction()
                        .await
                        .expect("Failed to abort transaction");
                    continue;
                }

                // Normal processing for good messages
                producer
                    .begin_transaction()
                    .await
                    .expect("Failed to begin transaction");

                let processed_message = TestMessage::new(
                    message.value().id + 1000,
                    &format!("Stream error test: {}", input_content),
                );

                producer
                    .send(message.key(), &processed_message, Headers::new(), None)
                    .await
                    .expect("Failed to send processed message");

                let offsets = consumer.current_offsets().expect("Failed to get offsets");
                producer
                    .send_offsets_to_transaction(&offsets, consumer.group_id())
                    .await
                    .expect("Failed to send offsets to transaction");

                producer
                    .commit_transaction()
                    .await
                    .expect("Failed to commit transaction");

                processed_count += 1;

                // Stop after processing expected messages
                if processed_count >= 2 {
                    // Only good messages should be processed
                    break;
                }
            }
            Err(e) => {
                println!("Stream error: {:?}", e);
                let _ = producer.abort_transaction().await;
                break;
            }
        }
    }

    assert_eq!(processed_count, 2, "Should process exactly 2 good messages");
    assert_eq!(error_count, 1, "Should encounter exactly 1 bad message");

    // Verify only good messages were committed
    let output_group_id = format!("eos-stream-error-output-{}", Uuid::new_v4());
    let output_consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
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

    let mut output_messages = Vec::new();
    for _attempt in 1..=5 {
        match output_consumer.poll(Duration::from_secs(1)).await {
            Ok(message) => {
                output_messages.push(message.value().content.clone());
            }
            Err(KafkaClientError::NoMessage) => break,
            Err(e) => println!("Output consumer error: {:?}", e),
        }
    }

    // Verify only good messages were processed and committed
    assert_eq!(
        output_messages.len(),
        2,
        "Should have exactly 2 output messages (bad message not committed)"
    );

    for output_content in &output_messages {
        assert!(
            output_content.contains("Good message"),
            "Output should only contain good messages: {}",
            output_content
        );
        assert!(
            !output_content.contains("Bad message"),
            "Output should not contain bad messages: {}",
            output_content
        );
    }
}
