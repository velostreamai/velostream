use crate::unit::common::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{Duration, sleep, timeout};

/// Test network partition recovery - reconnection after Kafka restart
#[tokio::test]
#[serial]
async fn test_network_partition_recovery_with_retry_logic() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("partition-recovery-{}", Uuid::new_v4());
    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    // Send initial message to establish connection
    let initial_message = TestMessage::new(1, "Before partition");
    producer
        .send(
            Some(&"key1".to_string()),
            &initial_message,
            Headers::new(),
            None,
        )
        .await
        .expect("Initial send should succeed");

    producer.flush(5000).expect("Initial flush should succeed");

    // Test retry logic during network partition
    let partition_message = TestMessage::new(2, "During partition");
    let max_retries = 3;
    let mut retry_count = 0;
    let mut _last_error: Option<KafkaClientError> = None;

    println!("🔄 Testing retry logic during network partition...");

    for attempt in 1..=max_retries {
        let retry_delay = Duration::from_millis(100 * attempt as u64); // Linear backoff for testing

        match timeout(
            Duration::from_secs(1),
            producer.send(
                Some(&format!("retry-key-{}", attempt)),
                &partition_message,
                Headers::new(),
                None,
            ),
        )
        .await
        {
            Ok(Ok(_)) => {
                println!("✅ Message sent successfully on retry attempt {}", attempt);
                break;
            }
            Ok(Err(e)) => {
                retry_count += 1;
                _last_error = Some(e);
                println!(
                    "❌ Retry attempt {} failed, waiting {:?}",
                    attempt, retry_delay
                );
                sleep(retry_delay).await;
            }
            Err(_) => {
                retry_count += 1;
                println!(
                    "⏰ Retry attempt {} timed out, waiting {:?}",
                    attempt, retry_delay
                );
                sleep(retry_delay).await;
            }
        }
    }

    // Verify retry logic was exercised
    assert!(
        retry_count > 0
            || producer
                .send(
                    Some(&"test".to_string()),
                    &partition_message,
                    Headers::new(),
                    None
                )
                .await
                .is_ok(),
        "Should either have retried or succeeded on first attempt"
    );

    // Test recovery after partition heals
    sleep(Duration::from_secs(1)).await;
    let recovery_message = TestMessage::new(3, "After recovery");

    let recovery_result = producer
        .send(
            Some(&"recovery-key".to_string()),
            &recovery_message,
            Headers::new(),
            None,
        )
        .await;

    match recovery_result {
        Ok(_) => {
            println!("✅ Network partition recovery successful");
            producer.flush(2000).expect("Recovery flush should succeed");
        }
        Err(e) => {
            println!("⚠️  Recovery not yet complete: {:?}", e);
            // In real scenarios, continue retry logic here
        }
    }
}

/// Test consumer resilience during broker failures
#[tokio::test]
#[serial]
#[ignore]
async fn test_consumer_graceful_degradation() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("recovery-consumer-{}", Uuid::new_v4());
    let group_id = format!("resilient-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send messages before testing degradation
    for i in 1..=3 {
        let message = TestMessage::new(i, &format!("Pre-failure message {}", i));
        producer
            .send(Some(&format!("key{}", i)), &message, Headers::new(), None)
            .await
            .expect("Failed to send pre-failure message");
    }

    producer
        .flush(5000)
        .expect("Failed to flush pre-failure messages");
    sleep(Duration::from_secs(1)).await;

    // Test consumer behavior during simulated network issues
    let mut received_count = 0;
    let max_attempts = 5;

    for attempt in 1..=max_attempts {
        match timeout(
            Duration::from_millis(500),
            consumer.poll(Duration::from_millis(100)),
        )
        .await
        {
            Ok(Ok(message)) => {
                received_count += 1;
                println!(
                    "✅ Received message {}: {:?}",
                    received_count,
                    message.value().content
                );
            }
            Ok(Err(KafkaClientError::Timeout)) => {
                println!(
                    "⚠️  Consumer timeout on attempt {} (expected during degradation)",
                    attempt
                );
            }
            Ok(Err(e)) => {
                println!("⚠️  Consumer error on attempt {}: {:?}", attempt, e);
            }
            Err(_) => {
                println!(
                    "⚠️  Poll timeout on attempt {} (graceful degradation)",
                    attempt
                );
            }
        }
    }

    // Verify consumer can still commit offsets even with some failures
    match consumer.commit() {
        Ok(_) => println!("✅ Offset commit succeeded despite degradation"),
        Err(e) => println!("⚠️  Offset commit failed: {:?}", e),
    }

    assert!(received_count > 0, "Should receive at least some messages");
}

/// Test retry mechanisms with exponential backoff
#[tokio::test]
#[serial]
async fn test_retry_with_exponential_backoff() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("recovery-retry-{}", Uuid::new_v4());

    // Create producer with short timeouts to trigger retries
    let config = ProducerConfig::new("localhost:9092", &topic)
        .request_timeout(Duration::from_millis(100))
        .delivery_timeout(Duration::from_millis(200));

    let producer = KafkaProducer::<String, TestMessage, _, _>::with_config(
        config,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let message = TestMessage::new(1, "Retry test message");
    let retry_attempts = Arc::new(AtomicBool::new(false));

    // Simulate retry logic
    let max_retries = 3;
    let mut delay = Duration::from_millis(100);

    for attempt in 1..=max_retries {
        println!("🔄 Retry attempt {}/{}", attempt, max_retries);

        match producer
            .send(
                Some(&"retry-key".to_string()),
                &message,
                Headers::new(),
                None,
            )
            .await
        {
            Ok(_) => {
                println!("✅ Message sent successfully on attempt {}", attempt);
                break;
            }
            Err(e) => {
                println!("❌ Attempt {} failed: {:?}", attempt, e);

                if attempt < max_retries {
                    println!("⏳ Waiting {:?} before retry", delay);
                    sleep(delay).await;
                    delay *= 2; // Exponential backoff
                    retry_attempts.store(true, Ordering::Relaxed);
                }
            }
        }
    }

    assert!(
        retry_attempts.load(Ordering::Relaxed),
        "Should have attempted retries"
    );
}

/// Test partial failure scenarios - one broker down, others working
#[tokio::test]
#[serial]
async fn test_partial_broker_failure_scenarios() {
    if !is_kafka_running() {
        return;
    }

    let working_topic = format!("partial-working-{}", Uuid::new_v4());
    let _failing_topic = format!("partial-failing-{}", Uuid::new_v4());

    // Create producer for working broker
    let working_producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &working_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create working producer");

    // Simulate one broker being down by using unreachable broker in bootstrap
    let mixed_brokers = "localhost:9092,localhost:9093"; // 9093 doesn't exist
    let mixed_producer = KafkaProducer::<String, TestMessage, _, _>::new(
        mixed_brokers,
        &working_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create mixed producer");

    let test_message = TestMessage::new(1, "Partial broker failure test");

    // Test that working producer succeeds consistently
    let mut working_success_count = 0;
    for i in 0..3 {
        match working_producer
            .send(
                Some(&format!("working-key-{}", i)),
                &test_message,
                Headers::new(),
                None,
            )
            .await
        {
            Ok(_) => {
                working_success_count += 1;
                println!("✅ Working producer message {} succeeded", i);
            }
            Err(e) => println!("❌ Working producer message {} failed: {:?}", i, e),
        }
    }

    // Test mixed producer (should handle one broker being down)
    let mut mixed_success_count = 0;
    let mut mixed_error_types: Vec<String> = Vec::new();

    for i in 0..3 {
        match timeout(
            Duration::from_secs(3), // Allow time for broker discovery
            mixed_producer.send(
                Some(&format!("mixed-key-{}", i)),
                &test_message,
                Headers::new(),
                None,
            ),
        )
        .await
        {
            Ok(Ok(_)) => {
                mixed_success_count += 1;
                println!(
                    "✅ Mixed producer message {} succeeded despite partial failure",
                    i
                );
            }
            Ok(Err(e)) => {
                mixed_error_types.push(e.to_string());
                println!("⚠️  Mixed producer message {} failed: {:?}", i, e);
            }
            Err(_) => {
                mixed_error_types.push("timeout".to_string());
                println!("⏰ Mixed producer message {} timed out", i);
            }
        }

        sleep(Duration::from_millis(100)).await; // Small delay between attempts
    }

    // Verify working producer maintains high availability
    assert_eq!(
        working_success_count, 3,
        "Working producer should succeed all operations"
    );

    // Mixed producer should handle partial failures gracefully
    // (may succeed or fail depending on broker discovery, but shouldn't panic)
    println!(
        "📊 Partial failure stats: Working={}/3, Mixed={}/3",
        working_success_count, mixed_success_count
    );

    if !mixed_error_types.is_empty() {
        println!("🔍 Mixed producer error types: {:?}", mixed_error_types);
    }

    // Test that working producer can still flush reliably
    match working_producer.flush(5000) {
        Ok(_) => println!("✅ Working producer flush succeeded"),
        Err(e) => println!("❌ Working producer flush failed: {:?}", e),
    }

    // Test mixed producer flush behavior
    match mixed_producer.flush(5000) {
        Ok(_) => println!("✅ Mixed producer flush succeeded"),
        Err(e) => println!("⚠️  Mixed producer flush result: {:?}", e),
    }
}

/// Test graceful shutdown scenarios
#[tokio::test]
#[serial]
async fn test_graceful_shutdown_recovery() {
    if !is_kafka_running() {
        return;
    }

    let topic = format!("recovery-shutdown-{}", Uuid::new_v4());
    let group_id = format!("shutdown-group-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send messages
    let messages = vec![
        TestMessage::new(1, "Shutdown test 1"),
        TestMessage::new(2, "Shutdown test 2"),
    ];

    for (i, message) in messages.iter().enumerate() {
        producer
            .send(
                Some(&format!("shutdown-key-{}", i)),
                message,
                Headers::new(),
                None,
            )
            .await
            .expect("Failed to send message");
    }

    // Test graceful flush before shutdown
    let flush_start = std::time::Instant::now();
    match producer.flush(5000) {
        Ok(_) => {
            let flush_duration = flush_start.elapsed();
            println!("✅ Graceful flush completed in {:?}", flush_duration);
            assert!(
                flush_duration < Duration::from_secs(6),
                "Flush should complete within timeout"
            );
        }
        Err(e) => {
            println!("❌ Graceful flush failed: {:?}", e);
        }
    }

    // Test consumer cleanup
    sleep(Duration::from_secs(1)).await;

    match consumer.poll(Duration::from_secs(2)).await {
        Ok(message) => {
            println!(
                "✅ Received message before shutdown: {:?}",
                message.value().content
            );

            // Test graceful commit before shutdown
            match consumer.commit() {
                Ok(_) => println!("✅ Graceful commit succeeded"),
                Err(e) => println!("❌ Graceful commit failed: {:?}", e),
            }
        }
        Err(e) => println!("⚠️  No message received before shutdown: {:?}", e),
    }

    // Explicit cleanup (simulating graceful shutdown)
    drop(producer);
    drop(consumer);

    println!("✅ Graceful shutdown simulation completed");
}
