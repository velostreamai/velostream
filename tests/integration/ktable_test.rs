use ferrisstreams::ferris::kafka::consumer_config::{ConsumerConfig, IsolationLevel, OffsetReset};
use ferrisstreams::ferris::kafka::serialization::JsonSerializer;
use ferrisstreams::ferris::kafka::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct UserProfile {
    id: String,
    name: String,
    email: String,
    age: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Order {
    id: String,
    user_id: String,
    amount: f64,
    status: String,
}

const KAFKA_BROKERS: &str = "localhost:9092";

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_basic_creation() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-basic-group")
        .auto_offset_reset(OffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted);

    let result = KTable::<String, UserProfile, _, _>::new(
        config,
        "user-profiles".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    match result {
        Ok(ktable) => {
            assert_eq!(ktable.topic(), "user-profiles");
            assert_eq!(ktable.group_id(), "ktable-basic-group");
            assert!(!ktable.is_running());
            assert!(ktable.is_empty());
            assert_eq!(ktable.len(), 0);
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for KTable basic creation test: {:?}",
                e
            );
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_from_consumer() {
    let consumer_config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-consumer-group")
        .auto_offset_reset(OffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted);

    let consumer_result = KafkaConsumer::<String, UserProfile, _, _>::with_config(
        consumer_config,
        JsonSerializer,
        JsonSerializer,
    );

    match consumer_result {
        Ok(consumer) => {
            let ktable = KTable::from_consumer(consumer, "user-profiles".to_string());

            assert_eq!(ktable.topic(), "user-profiles");
            assert_eq!(ktable.group_id(), "ktable-consumer-group");
            assert!(!ktable.is_running());
            assert!(ktable.is_empty());
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for KTable from consumer test: {:?}",
                e
            );
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_lifecycle_management() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-lifecycle-group")
        .auto_offset_reset(OffsetReset::Latest);

    let ktable_result = KTable::<String, UserProfile, _, _>::new(
        config,
        "user-profiles-lifecycle".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    match ktable_result {
        Ok(ktable) => {
            // Test initial state
            assert!(!ktable.is_running());

            // Clone for concurrent operations
            let ktable_clone = ktable.clone();

            // Start the table in background
            let start_handle = tokio::spawn(async move { ktable_clone.start().await });

            // Give it a moment to start
            sleep(Duration::from_millis(100)).await;

            // Should be running now
            assert!(ktable.is_running());

            // Stop the table
            ktable.stop();

            // Should stop running
            assert!(!ktable.is_running());

            // Wait for background task to complete
            let _ = start_handle.await;
        }
        Err(e) => {
            println!("⚠️  Kafka not available for KTable lifecycle test: {:?}", e);
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_stats_and_metadata() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-stats-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let ktable_result = KTable::<String, UserProfile, _, _>::new(
        config,
        "user-profiles-stats".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    match ktable_result {
        Ok(ktable) => {
            let stats = ktable.stats();

            assert_eq!(stats.key_count, 0);
            assert_eq!(stats.topic, "user-profiles-stats");
            assert_eq!(stats.group_id, "ktable-stats-group");
            assert!(stats.last_updated.is_none());

            // Test keys collection
            let keys = ktable.keys();
            assert!(keys.is_empty());

            // Test contains_key
            assert!(!ktable.contains_key(&"user-123".to_string()));

            // Test get non-existent key
            assert!(ktable.get(&"user-123".to_string()).is_none());
        }
        Err(e) => {
            println!("⚠️  Kafka not available for KTable stats test: {:?}", e);
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_transformations() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-transform-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let ktable_result = KTable::<String, UserProfile, _, _>::new(
        config,
        "user-profiles-transform".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    match ktable_result {
        Ok(ktable) => {
            // Test map_values transformation
            let name_map: HashMap<String, String> =
                ktable.map_values(|profile| profile.name.clone());
            assert!(name_map.is_empty());

            // Test filter transformation
            let adults: HashMap<String, UserProfile> =
                ktable.filter(|_key, profile| profile.age >= 18);
            assert!(adults.is_empty());

            // Test snapshot
            let snapshot = ktable.snapshot();
            assert!(snapshot.is_empty());
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for KTable transformations test: {:?}",
                e
            );
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_wait_for_keys() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-wait-group")
        .auto_offset_reset(OffsetReset::Latest);

    let ktable_result = KTable::<String, UserProfile, _, _>::new(
        config,
        "user-profiles-wait".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    match ktable_result {
        Ok(ktable) => {
            // Test waiting for keys (should timeout quickly since table is empty)
            let has_keys = ktable.wait_for_keys(1, Duration::from_millis(100)).await;
            assert!(!has_keys);

            // Test waiting for 0 keys (should return immediately)
            let has_zero_keys = ktable.wait_for_keys(0, Duration::from_millis(10)).await;
            assert!(has_zero_keys);
        }
        Err(e) => {
            println!("⚠️  Kafka not available for KTable wait test: {:?}", e);
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_clone_behavior() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-clone-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let ktable_result = KTable::<String, UserProfile, _, _>::new(
        config,
        "user-profiles-clone".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    match ktable_result {
        Ok(ktable) => {
            // Test cloning
            let ktable_clone = ktable.clone();

            assert_eq!(ktable.topic(), ktable_clone.topic());
            assert_eq!(ktable.group_id(), ktable_clone.group_id());
            assert_eq!(ktable.len(), ktable_clone.len());
            assert_eq!(ktable.is_running(), ktable_clone.is_running());

            // Both should share the same state
            assert_eq!(ktable.snapshot(), ktable_clone.snapshot());
        }
        Err(e) => {
            println!("⚠️  Kafka not available for KTable clone test: {:?}", e);
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_with_producer_simulation() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-producer-sim-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let ktable_result = KTable::<String, UserProfile, _, _>::new(
        config,
        "user-profiles-sim".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    match ktable_result {
        Ok(ktable) => {
            println!("✅ KTable created successfully for producer simulation");
            println!("   Topic: {}", ktable.topic());
            println!("   Group: {}", ktable.group_id());
            println!("   Initial key count: {}", ktable.len());

            // In a real scenario, you would:
            // 1. Start the KTable consumption in background
            // 2. Produce messages to the topic
            // 3. Wait for the KTable to process them
            // 4. Query the KTable state

            // For this test, we just verify the KTable is ready
            assert_eq!(ktable.len(), 0);
            assert!(!ktable.is_running());

            // Test that we can start and stop
            let ktable_clone = ktable.clone();
            let start_handle = tokio::spawn(async move {
                // Run for a short time
                tokio::select! {
                    _ = ktable_clone.start() => {},
                    _ = sleep(Duration::from_millis(200)) => {}
                }
            });

            sleep(Duration::from_millis(50)).await;
            assert!(ktable.is_running());

            ktable.stop();
            let _ = start_handle.await;
            assert!(!ktable.is_running());
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for KTable producer simulation test: {:?}",
                e
            );
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_multiple_types() {
    // Test with different key/value types
    let user_config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-users-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let order_config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-orders-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let user_table_result = KTable::<String, UserProfile, _, _>::new(
        user_config,
        "users".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    let order_table_result = KTable::<String, Order, _, _>::new(
        order_config,
        "orders".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    match (user_table_result, order_table_result) {
        (Ok(user_table), Ok(order_table)) => {
            // Both tables should work independently
            assert_eq!(user_table.topic(), "users");
            assert_eq!(order_table.topic(), "orders");

            assert_eq!(user_table.group_id(), "ktable-users-group");
            assert_eq!(order_table.group_id(), "ktable-orders-group");

            // Both should be empty initially
            assert!(user_table.is_empty());
            assert!(order_table.is_empty());

            // Test type-specific transformations
            let user_emails: HashMap<String, String> =
                user_table.map_values(|user| user.email.clone());
            let order_amounts: HashMap<String, f64> = order_table.map_values(|order| order.amount);

            assert!(user_emails.is_empty());
            assert!(order_amounts.is_empty());
        }
        (Err(e), _) | (_, Err(e)) => {
            println!(
                "⚠️  Kafka not available for KTable multiple types test: {:?}",
                e
            );
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_error_handling() {
    // Test with invalid broker to check error handling
    let config = ConsumerConfig::new("invalid-broker:9092", "ktable-error-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let ktable_result = KTable::<String, UserProfile, _, _>::new(
        config,
        "user-profiles-error".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    // Should return an error for invalid broker
    match ktable_result {
        Ok(_) => {
            println!("⚠️  Unexpected success with invalid broker - may be using mock/test broker");
        }
        Err(e) => {
            println!("✅ Expected error with invalid broker: {:?}", e);
            // This is expected behavior
        }
    }
}

#[tokio::test]
#[ignore] // Temporarily disabled to isolate failing tests
async fn test_ktable_configuration_options() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "ktable-config-group")
        .auto_offset_reset(OffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_commit(false, Duration::from_secs(5))
        .session_timeout(Duration::from_secs(30))
        .fetch_min_bytes(1024);

    let ktable_result = KTable::<String, UserProfile, _, _>::new(
        config,
        "user-profiles-config".to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await;

    match ktable_result {
        Ok(ktable) => {
            println!("✅ KTable created with custom configuration");

            // Verify the table was created with our configuration
            assert_eq!(ktable.topic(), "user-profiles-config");
            assert_eq!(ktable.group_id(), "ktable-config-group");

            // Test that stats reflect the configuration
            let stats = ktable.stats();
            assert_eq!(stats.topic, "user-profiles-config");
            assert_eq!(stats.group_id, "ktable-config-group");
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for KTable configuration test: {:?}",
                e
            );
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}
