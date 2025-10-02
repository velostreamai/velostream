use super::*; // Use the re-exported items from integration::mod

#[tokio::test]
#[serial]
async fn test_multiple_user_workflow() {
    if !is_kafka_running() {
        return;
    }

    let broker = "localhost:9092";
    let topic = format!("users-advanced-{}", Uuid::new_v4());
    let group_id = format!("advanced-users-{}", Uuid::new_v4());

    let producer =
        KafkaProducer::<String, User, _, _>::new(broker, &topic, JsonSerializer, JsonSerializer)
            .expect("Failed to create producer");

    let consumer =
        KafkaConsumer::<String, User, _, _>::new(broker, &group_id, JsonSerializer, JsonSerializer)
            .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Create diverse user data
    let users = vec![
        User {
            id: 1,
            name: "Alice".to_string(),
            email: Some("alice@test.com".to_string()),
        },
        User {
            id: 2,
            name: "Bob".to_string(),
            email: None,
        },
        User {
            id: 3,
            name: "Charlie".to_string(),
            email: Some("charlie@test.com".to_string()),
        },
        User {
            id: 4,
            name: "Diana".to_string(),
            email: Some("diana@test.com".to_string()),
        },
        User {
            id: 5,
            name: "Eve".to_string(),
            email: None,
        },
    ];

    // Send all users
    println!("Sending {} users to topic: {}", users.len(), topic);
    for (i, user) in users.iter().enumerate() {
        let key = format!("user-{}", user.id);
        match producer.send(Some(&key), user, Headers::new(), None).await {
            Ok(_) => println!("Successfully sent user {} ({})", i + 1, user.name),
            Err(e) => {
                eprintln!("Failed to send user {}: {:?}", user.name, e);
                panic!("Failed to send user");
            }
        }
    }

    println!("Flushing producer...");
    producer.flush(5000).expect("Failed to flush producer");
    println!("Producer flushed successfully");

    // Give Kafka more time to process messages and create topic if needed
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Receive and verify all users with robust retry logic
    let mut received_users = Vec::new();
    let max_attempts = 50; // More attempts for slow environments
    let mut consecutive_timeouts = 0;

    println!("Starting to consume messages from topic: {}", topic);

    for attempt in 0..max_attempts {
        match consumer.poll(Duration::from_secs(3)).await {
            Ok(message) => {
                println!(
                    "Received message {} of {}",
                    received_users.len() + 1,
                    users.len()
                );
                received_users.push(message.into_value());
                consecutive_timeouts = 0; // Reset timeout counter on success
                if received_users.len() >= users.len() {
                    println!("Successfully received all {} messages", users.len());
                    break;
                }
            }
            Err(e) => {
                consecutive_timeouts += 1;
                if attempt % 5 == 0 {
                    // Log every 5th attempt
                    println!(
                        "Attempt {}/{}: Poll timeout or error: {:?}. Received {} of {} messages.",
                        attempt + 1,
                        max_attempts,
                        e,
                        received_users.len(),
                        users.len()
                    );
                }

                // Be more patient - only break if we've had many consecutive timeouts
                // and either we've received some messages OR we've tried for a long time
                if consecutive_timeouts >= 8 && (!received_users.is_empty() || attempt > 30) {
                    println!(
                        "Giving up after {} consecutive timeouts",
                        consecutive_timeouts
                    );
                    break;
                }

                // Brief pause between attempts
                if attempt < max_attempts - 1 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    // Only commit if we actually received messages
    if !received_users.is_empty() {
        consumer.commit().expect("Failed to commit");
    }

    // Provide better error messages for debugging
    if received_users.len() != users.len() {
        eprintln!(
            "Expected {} users but received {} users",
            users.len(),
            received_users.len()
        );
        eprintln!("Received users: {:?}", received_users);
        eprintln!("Expected users: {:?}", users);
    }

    assert_eq!(
        received_users.len(),
        users.len(),
        "Expected to receive {} users but got {}. This might indicate Kafka connectivity issues or message delivery delays.",
        users.len(),
        received_users.len()
    );

    for user in &users {
        assert!(received_users.contains(user), "Missing user: {:?}", user);
    }
}

#[tokio::test]
#[serial]
async fn test_cross_topic_messaging() {
    if !is_kafka_running() {
        return;
    }

    let broker = "localhost:9092";
    let user_topic = format!("users-cross-{}", Uuid::new_v4());
    let product_topic = format!("products-cross-{}", Uuid::new_v4());
    let order_topic = format!("orders-cross-{}", Uuid::new_v4());
    let group_id = format!("cross-topic-{}", Uuid::new_v4());

    // Create producers for different message types
    let user_producer = KafkaProducer::<String, User, _, _>::new(
        broker,
        &user_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create user producer");

    let product_producer = KafkaProducer::<String, Product, _, _>::new(
        broker,
        &product_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create product producer");

    let order_producer = KafkaProducer::<String, OrderEvent, _, _>::new(
        broker,
        &order_topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create order producer");

    // Create consumers
    let user_consumer =
        KafkaConsumer::<String, User, _, _>::new(broker, &group_id, JsonSerializer, JsonSerializer)
            .expect("Failed to create user consumer");

    let product_consumer = KafkaConsumer::<String, Product, _, _>::new(
        broker,
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create product consumer");

    let order_consumer = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker,
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create order consumer");

    user_consumer
        .subscribe(&[&user_topic])
        .expect("Failed to subscribe to users");
    product_consumer
        .subscribe(&[&product_topic])
        .expect("Failed to subscribe to products");
    order_consumer
        .subscribe(&[&order_topic])
        .expect("Failed to subscribe to orders");

    // Send different message types
    let user = User {
        id: 100,
        name: "Cross User".to_string(),
        email: Some("cross@test.com".to_string()),
    };
    let product = Product {
        id: "prod-100".to_string(),
        name: "Cross Product".to_string(),
        price: 99.99,
        available: true,
    };
    let order = OrderEvent::new("order-100", "customer-100", 199.98, OrderStatus::Created);

    user_producer
        .send(Some(&"user-100".to_string()), &user, Headers::new(), None)
        .await
        .expect("Failed to send user");
    product_producer
        .send(
            Some(&"prod-100".to_string()),
            &product,
            Headers::new(),
            None,
        )
        .await
        .expect("Failed to send product");
    order_producer
        .send(Some(&"order-100".to_string()), &order, Headers::new(), None)
        .await
        .expect("Failed to send order");

    user_producer
        .flush(5000)
        .expect("Failed to flush user producer");
    product_producer
        .flush(5000)
        .expect("Failed to flush product producer");
    order_producer
        .flush(5000)
        .expect("Failed to flush order producer");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify each consumer receives only its message type
    let mut user_received = false;
    let mut product_received = false;
    let mut order_received = false;

    for _ in 0..15 {
        if let Ok(message) = user_consumer.poll(Duration::from_millis(300)).await {
            if message.value().id == 100 {
                user_received = true;
            }
        }

        if let Ok(message) = product_consumer.poll(Duration::from_millis(300)).await {
            if message.value().id == "prod-100" {
                product_received = true;
            }
        }

        if let Ok(message) = order_consumer.poll(Duration::from_millis(300)).await {
            if message.value().order_id == "order-100" {
                order_received = true;
            }
        }

        if user_received && product_received && order_received {
            break;
        }
    }

    // Only commit if messages were received (otherwise we get NoOffset error)
    if user_received {
        user_consumer
            .commit()
            .expect("Failed to commit user consumer");
    }
    if product_received {
        product_consumer
            .commit()
            .expect("Failed to commit product consumer");
    }
    if order_received {
        order_consumer
            .commit()
            .expect("Failed to commit order consumer");
    }

    assert!(user_received, "Should receive user message");
    assert!(product_received, "Should receive product message");
    assert!(order_received, "Should receive order message");
}

#[tokio::test]
#[serial]
async fn test_high_throughput_scenario() {
    if !is_kafka_running() {
        return;
    }

    let broker = "localhost:9092";
    let topic = format!("high-throughput-{}", Uuid::new_v4());
    let group_id = format!("throughput-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, OrderEvent, _, _>::new(
        broker,
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker,
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    let message_count = 50;
    let start_time = std::time::Instant::now();

    // Send many messages rapidly
    for i in 0..message_count {
        let order = OrderEvent::new(
            &format!("order-{}", i),
            &format!("customer-{}", i % 10),
            (i as f64) * 10.0,
            match i % 4 {
                0 => OrderStatus::Created,
                1 => OrderStatus::Paid,
                2 => OrderStatus::Shipped,
                _ => OrderStatus::Delivered,
            },
        );

        producer
            .send(Some(&format!("key-{}", i)), &order, Headers::new(), None)
            .await
            .expect("Failed to send order");

        // Small delay to avoid overwhelming
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    producer.flush(5000).expect("Failed to flush producer");
    let send_duration = start_time.elapsed();

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Consume all messages
    let consume_start = std::time::Instant::now();
    let mut received_count = 0;

    for _ in 0..message_count * 2 {
        match consumer.poll(Duration::from_millis(500)).await {
            Ok(_) => {
                received_count += 1;
                if received_count >= message_count {
                    break;
                }
            }
            Err(_) => {
                if received_count > 0 {
                    break; // Got some messages, that's good enough
                }
            }
        }
    }

    let consume_duration = consume_start.elapsed();

    // Only commit if we actually received messages
    if received_count > 0 {
        consumer.commit().expect("Failed to commit");
    }

    println!("High throughput test:");
    println!("  Sent {} messages in {:?}", message_count, send_duration);
    println!(
        "  Received {} messages in {:?}",
        received_count, consume_duration
    );
    println!(
        "  Send rate: {:.1} msg/sec",
        message_count as f64 / send_duration.as_secs_f64()
    );

    // We should receive at least 80% of messages in reasonable time
    let success_rate = received_count as f64 / message_count as f64;
    assert!(
        success_rate > 0.8,
        "Should receive most messages, got {:.1}%",
        success_rate * 100.0
    );
    assert!(
        send_duration < Duration::from_secs(10),
        "Sending should be reasonably fast"
    );
}

#[tokio::test]
#[serial]
async fn test_complex_enum_serialization() {
    if !is_kafka_running() {
        return;
    }

    // Add delay for CI environment to reduce resource contention
    tokio::time::sleep(Duration::from_secs(2)).await;

    let broker = "localhost:9092";
    let topic = format!("enum-complex-{}", Uuid::new_v4());
    let group_id = format!("enum-{}", Uuid::new_v4());

    let producer = KafkaProducer::<String, OrderEvent, _, _>::new(
        broker,
        &topic,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create producer");

    let consumer = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker,
        &group_id,
        JsonSerializer,
        JsonSerializer,
    )
    .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Give consumer time to establish connection and subscription
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Test each enum variant
    let orders = vec![
        OrderEvent::new("order-1", "customer-1", 100.0, OrderStatus::Created),
        OrderEvent::new("order-2", "customer-2", 200.0, OrderStatus::Paid),
        OrderEvent::new("order-3", "customer-3", 150.0, OrderStatus::Shipped),
        OrderEvent::new("order-4", "customer-4", 75.0, OrderStatus::Delivered),
        OrderEvent::new("order-5", "customer-5", 300.0, OrderStatus::Cancelled),
    ];

    for order in &orders {
        producer
            .send(Some(&order.order_id), order, Headers::new(), None)
            .await
            .expect("Failed to send order");
    }

    producer.flush(5000).expect("Failed to flush producer");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut received_orders = Vec::new();
    let mut attempts = 0;
    let max_attempts = 30; // Increased attempts for CI environment

    while received_orders.len() < orders.len() && attempts < max_attempts {
        match consumer.poll(Duration::from_secs(2)).await {
            // Increased poll timeout
            Ok(message) => {
                received_orders.push(message.into_value());
            }
            Err(e) => {
                // Log error for debugging in CI
                println!("Poll attempt {} failed: {:?}", attempts + 1, e);
                // Small delay before retry
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        attempts += 1;
    }

    // Only commit if we actually received messages
    if !received_orders.is_empty() {
        consumer.commit().expect("Failed to commit");
    }

    // More informative assertion for CI debugging
    if received_orders.len() != orders.len() {
        println!(
            "Expected {} orders, received {} orders",
            orders.len(),
            received_orders.len()
        );
        println!("Sent orders: {:?}", orders);
        println!("Received orders: {:?}", received_orders);
    }
    assert_eq!(received_orders.len(), orders.len());

    // Verify all enum variants were serialized/deserialized correctly
    for order in orders {
        assert!(received_orders.contains(&order));
    }

    // Verify we have all enum variants
    let statuses: std::collections::HashSet<_> =
        received_orders.iter().map(|o| &o.status).collect();
    assert_eq!(statuses.len(), 5, "Should have all 5 order status variants");
}

#[tokio::test]
#[serial]
async fn test_concurrent_producers() {
    if !is_kafka_running() {
        return;
    }

    let broker = "localhost:9092";
    let topic = format!("concurrent-test-{}", Uuid::new_v4());
    let group_id = format!("concurrent-{}", Uuid::new_v4());

    // Create multiple producers
    let producer1 =
        KafkaProducer::<String, User, _, _>::new(broker, &topic, JsonSerializer, JsonSerializer)
            .expect("Failed to create producer1");
    let producer2 =
        KafkaProducer::<String, User, _, _>::new(broker, &topic, JsonSerializer, JsonSerializer)
            .expect("Failed to create producer2");

    let consumer =
        KafkaConsumer::<String, User, _, _>::new(broker, &group_id, JsonSerializer, JsonSerializer)
            .expect("Failed to create consumer");

    consumer.subscribe(&[&topic]).expect("Failed to subscribe");

    // Send from multiple producers concurrently
    let user1 = User {
        id: 201,
        name: "Concurrent User 1".to_string(),
        email: Some("user1@concurrent.com".to_string()),
    };
    let user2 = User {
        id: 202,
        name: "Concurrent User 2".to_string(),
        email: Some("user2@concurrent.com".to_string()),
    };

    let key1 = "concurrent-1".to_string();
    let key2 = "concurrent-2".to_string();
    let send1 = producer1.send(Some(&key1), &user1, Headers::new(), None);
    let send2 = producer2.send(Some(&key2), &user2, Headers::new(), None);

    // Wait for both sends to complete
    let (result1, result2) = tokio::join!(send1, send2);

    result1.expect("Producer1 should succeed");
    result2.expect("Producer2 should succeed");

    producer1.flush(5000).expect("Failed to flush producer1");
    producer2.flush(5000).expect("Failed to flush producer2");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify both messages received
    let mut received = Vec::new();
    for _ in 0..10 {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(message) => {
                received.push(message.into_value());
                if received.len() >= 2 {
                    break;
                }
            }
            Err(_) => continue,
        }
    }

    // Only commit if we actually received messages
    if !received.is_empty() {
        consumer.commit().expect("Failed to commit consumer");
    }

    assert_eq!(received.len(), 2, "Should receive exactly 2 messages");
    assert!(received.contains(&user1), "Should receive user1");
    assert!(received.contains(&user2), "Should receive user2");
}
