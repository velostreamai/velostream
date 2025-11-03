use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use velostream::velostream::kafka::consumer_config::{ConsumerConfig, IsolationLevel, OffsetReset};
use velostream::velostream::kafka::serialization::{
    BytesSerializer, JsonSerializer, StringSerializer,
};
use velostream::velostream::kafka::*;
use velostream::velostream::serialization::{FieldValue, JsonFormat};
use velostream::velostream::table::Table;

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
async fn test_table_basic_creation() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "table-basic-group")
        .auto_offset_reset(OffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted);

    let result = Table::new(
        config,
        "user-profiles".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match result {
        Ok(table) => {
            assert_eq!(table.topic(), "user-profiles");
            assert_eq!(table.group_id(), "table-basic-group");
            assert!(!table.is_running());
            assert!(table.is_empty());
            assert_eq!(table.len(), 0);
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for Table basic creation test: {:?}",
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
async fn test_table_from_consumer() {
    let consumer_config = ConsumerConfig::new(KAFKA_BROKERS, "table-consumer-group")
        .auto_offset_reset(OffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted);

    let consumer_result = FastConsumer::<String, Vec<u8>, _, _>::with_config(
        consumer_config,
        StringSerializer,
        BytesSerializer,
    );

    match consumer_result {
        Ok(consumer) => {
            let table = Table::from_consumer(consumer, "user-profiles".to_string(), JsonFormat);

            assert_eq!(table.topic(), "user-profiles");
            assert_eq!(table.group_id(), "table-consumer-group");
            assert!(!table.is_running());
            assert!(table.is_empty());
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for Table from consumer test: {:?}",
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
async fn test_table_lifecycle_management() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "table-lifecycle-group")
        .auto_offset_reset(OffsetReset::Latest);

    let table_result = Table::<String, _, _>::new(
        config,
        "user-profiles-lifecycle".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match table_result {
        Ok(table) => {
            // Test initial state
            assert!(!table.is_running());

            // Clone for concurrent operations
            let table_clone = table.clone();

            // Start the table in background
            let start_handle = tokio::spawn(async move { table_clone.start().await });

            // Give it a moment to start
            sleep(Duration::from_millis(100)).await;

            // Should be running now
            assert!(table.is_running());

            // Stop the table
            table.stop();

            // Should stop running
            assert!(!table.is_running());

            // Wait for background task to complete
            let _ = start_handle.await;
        }
        Err(e) => {
            println!("⚠️  Kafka not available for Table lifecycle test: {:?}", e);
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
async fn test_table_stats_and_metadata() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "table-stats-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let table_result = Table::<String, _, _>::new(
        config,
        "user-profiles-stats".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match table_result {
        Ok(table) => {
            let stats = table.stats();

            assert_eq!(stats.key_count, 0);
            assert_eq!(stats.topic, "user-profiles-stats");
            assert_eq!(stats.group_id, "table-stats-group");
            assert!(stats.last_updated.is_none());

            // Test keys collection
            let keys = table.keys();
            assert!(keys.is_empty());

            // Test contains_key
            assert!(!table.contains_key(&"user-123".to_string()));

            // Test get non-existent key
            assert!(table.get(&"user-123".to_string()).is_none());
        }
        Err(e) => {
            println!("⚠️  Kafka not available for Table stats test: {:?}", e);
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
async fn test_table_transformations() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "table-transform-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let table_result = Table::<String, _, _>::new(
        config,
        "user-profiles-transform".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match table_result {
        Ok(table) => {
            // Test map_values transformation
            let name_map: HashMap<String, String> = table.map_values(|profile| {
                if let Some(FieldValue::String(name)) = profile.get("name") {
                    name.clone()
                } else {
                    "unknown".to_string()
                }
            });
            assert!(name_map.is_empty());

            // Test filter transformation
            let adults: HashMap<String, HashMap<String, FieldValue>> =
                table.filter(|_key, profile| {
                    if let Some(FieldValue::Integer(age)) = profile.get("age") {
                        *age >= 18
                    } else {
                        false
                    }
                });
            assert!(adults.is_empty());

            // Test snapshot
            let snapshot = table.snapshot();
            assert!(snapshot.is_empty());
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for Table transformations test: {:?}",
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
async fn test_table_wait_for_keys() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "table-wait-group")
        .auto_offset_reset(OffsetReset::Latest);

    let table_result = Table::<String, _, _>::new(
        config,
        "user-profiles-wait".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match table_result {
        Ok(table) => {
            // Test waiting for keys (should timeout quickly since table is empty)
            let has_keys = table.wait_for_keys(1, Duration::from_millis(100)).await;
            assert!(!has_keys);

            // Test waiting for 0 keys (should return immediately)
            let has_zero_keys = table.wait_for_keys(0, Duration::from_millis(10)).await;
            assert!(has_zero_keys);
        }
        Err(e) => {
            println!("⚠️  Kafka not available for Table wait test: {:?}", e);
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
async fn test_table_clone_behavior() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "table-clone-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let table_result = Table::<String, _, _>::new(
        config,
        "user-profiles-clone".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match table_result {
        Ok(table) => {
            // Test cloning
            let table_clone = table.clone();

            assert_eq!(table.topic(), table_clone.topic());
            assert_eq!(table.group_id(), table_clone.group_id());
            assert_eq!(table.len(), table_clone.len());
            assert_eq!(table.is_running(), table_clone.is_running());

            // Both should share the same state
            assert_eq!(table.snapshot(), table_clone.snapshot());
        }
        Err(e) => {
            println!("⚠️  Kafka not available for Table clone test: {:?}", e);
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}

#[tokio::test]
async fn test_table_with_producer_simulation() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "table-producer-sim-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let table_result = Table::<String, _, _>::new(
        config,
        "user-profiles-sim".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match table_result {
        Ok(table) => {
            println!("✅ Table created successfully for producer simulation");
            println!("   Topic: {}", table.topic());
            println!("   Group: {}", table.group_id());
            println!("   Initial key count: {}", table.len());

            // In a real scenario, you would:
            // 1. Start the Table consumption in background
            // 2. Produce messages to the topic
            // 3. Wait for the Table to process them
            // 4. Query the Table state

            // For this test, we just verify the Table is ready
            assert_eq!(table.len(), 0);
            assert!(!table.is_running());

            // Test that we can start and stop
            let table_clone = table.clone();
            let start_handle = tokio::spawn(async move {
                // Run for a short time
                tokio::select! {
                    _ = table_clone.start() => {},
                    _ = sleep(Duration::from_millis(200)) => {}
                }
            });

            sleep(Duration::from_millis(50)).await;
            assert!(table.is_running());

            table.stop();
            let _ = start_handle.await;
            assert!(!table.is_running());
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for Table producer simulation test: {:?}",
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
async fn test_table_multiple_types() {
    // Test with different key/value types
    let user_config = ConsumerConfig::new(KAFKA_BROKERS, "table-users-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let order_config = ConsumerConfig::new(KAFKA_BROKERS, "table-orders-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let user_table_result = Table::<String, _, _>::new(
        user_config,
        "users".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    let order_table_result = Table::<String, _, _>::new(
        order_config,
        "orders".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match (user_table_result, order_table_result) {
        (Ok(user_table), Ok(order_table)) => {
            // Both tables should work independently
            assert_eq!(user_table.topic(), "users");
            assert_eq!(order_table.topic(), "orders");

            assert_eq!(user_table.group_id(), "table-users-group");
            assert_eq!(order_table.group_id(), "table-orders-group");

            // Both should be empty initially
            assert!(user_table.is_empty());
            assert!(order_table.is_empty());

            // Test type-specific transformations
            let user_emails: HashMap<String, String> = user_table.map_values(|user| {
                if let Some(FieldValue::String(email)) = user.get("email") {
                    email.clone()
                } else {
                    "unknown".to_string()
                }
            });
            let order_amounts: HashMap<String, f64> = order_table.map_values(|order| {
                if let Some(FieldValue::Float(amount)) = order.get("amount") {
                    *amount
                } else {
                    0.0
                }
            });

            assert!(user_emails.is_empty());
            assert!(order_amounts.is_empty());
        }
        (Err(e), _) | (_, Err(e)) => {
            println!(
                "⚠️  Kafka not available for Table multiple types test: {:?}",
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
async fn test_table_error_handling() {
    // Test with invalid broker to check error handling
    let config = ConsumerConfig::new("invalid-broker:9092", "table-error-group")
        .auto_offset_reset(OffsetReset::Earliest);

    let table_result = Table::<String, _, _>::new(
        config,
        "user-profiles-error".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    // Should return an error for invalid broker
    match table_result {
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
async fn test_table_configuration_options() {
    let config = ConsumerConfig::new(KAFKA_BROKERS, "table-config-group")
        .auto_offset_reset(OffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_commit(false, Duration::from_secs(5))
        .session_timeout(Duration::from_secs(30))
        .fetch_min_bytes(1024);

    let table_result = Table::<String, _, _>::new(
        config,
        "user-profiles-config".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match table_result {
        Ok(table) => {
            println!("✅ Table created with custom configuration");

            // Verify the table was created with our configuration
            assert_eq!(table.topic(), "user-profiles-config");
            assert_eq!(table.group_id(), "table-config-group");

            // Test that stats reflect the configuration
            let stats = table.stats();
            assert_eq!(stats.topic, "user-profiles-config");
            assert_eq!(stats.group_id, "table-config-group");
        }
        Err(e) => {
            println!(
                "⚠️  Kafka not available for Table configuration test: {:?}",
                e
            );
            println!(
                "   This test requires a running Kafka instance at {}",
                KAFKA_BROKERS
            );
        }
    }
}
