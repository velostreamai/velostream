use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use velostream::velostream::kafka::consumer_config::{ConsumerConfig, IsolationLevel, OffsetReset};
use velostream::velostream::kafka::serialization::JsonSerializer;
use velostream::Table;

/// Simple user data structure
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct User {
    id: String,
    name: String,
    email: String,
}

const KAFKA_BROKERS: &str = "localhost:9092";
const USERS_TOPIC: &str = "users";

/// Simple Table example demonstrating basic usage
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Simple Table Example");
    println!("{}", "=".repeat(40));

    // 1. Create Table configuration
    println!("⚙️  Creating Table configuration...");
    let config = ConsumerConfig::new(KAFKA_BROKERS, "simple-table-group")
        .auto_offset_reset(OffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_commit(false, Duration::from_secs(5));

    // 2. Create Table
    println!("🏗️  Creating Table for users...");
    let user_table = match Table::<String, User, _, _>::new(
        config,
        USERS_TOPIC.to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await
    {
        Ok(table) => {
            println!("✅ Table created successfully");
            table
        }
        Err(e) => {
            eprintln!("❌ Failed to create Table: {:?}", e);
            eprintln!("   Make sure Kafka is running at {}", KAFKA_BROKERS);
            eprintln!("   You can start Kafka with Docker:");
            eprintln!("   docker run -p 9092:9092 apache/kafka:2.13-3.7.0");
            return Err(e.into());
        }
    };

    // 3. Start Table consumption in background
    println!("▶️  Starting Table background consumption...");
    let table_clone = user_table.clone();
    let consumption_handle = tokio::spawn(async move {
        if let Err(e) = table_clone.start().await {
            eprintln!("❌ Table consumption error: {:?}", e);
        }
    });

    // 4. Give it a moment to start
    sleep(Duration::from_millis(500)).await;

    // 5. Check if Table is running
    if user_table.is_running() {
        println!("✅ Table is running and consuming messages");
    } else {
        println!("⚠️  Table is not running");
    }

    // 6. Wait for some data to load (if any exists)
    println!("⏳ Waiting for user data to load...");
    let has_data = user_table.wait_for_keys(1, Duration::from_secs(5)).await;

    if has_data {
        println!("✅ Found {} users in the table", user_table.len());
        display_users(&user_table);
    } else {
        println!("ℹ️  No users found in the table");
        println!("   To add users, you can use the Kafka console producer:");
        println!(
            "   kafka-console-producer.sh --topic {} --bootstrap-server {}",
            USERS_TOPIC, KAFKA_BROKERS
        );
        println!("   Then send JSON messages like:");
        println!("   {{\"id\":\"user1\",\"name\":\"John Doe\",\"email\":\"john@example.com\"}}");
    }

    // 7. Demonstrate basic operations
    println!("\n🔍 Demonstrating Table operations:");

    // Check if specific user exists
    let user_id = "user1";
    if user_table.contains_key(&user_id.to_string()) {
        println!("✅ User '{}' exists in table", user_id);
        if let Some(user) = user_table.get(&user_id.to_string()) {
            println!("   Details: {} <{}>", user.name, user.email);
        }
    } else {
        println!("❌ User '{}' not found in table", user_id);
    }

    // Show table statistics
    let stats = user_table.stats();
    println!("\n📊 Table Statistics:");
    println!("   Topic: {}", stats.topic);
    println!("   Group: {}", stats.group_id);
    println!("   Keys: {}", stats.key_count);
    if let Some(last_updated) = stats.last_updated {
        println!("   Last Updated: {:?}", last_updated);
    } else {
        println!("   Last Updated: Never");
    }

    // Demonstrate transformations
    if !user_table.is_empty() {
        println!("\n🔄 Demonstrating transformations:");

        // Map values to extract just names
        let names = user_table.map_values(|user| user.name.clone());
        println!("   User names: {:?}", names.values().collect::<Vec<_>>());

        // Filter users (example: names starting with 'J')
        let j_users = user_table.filter(|_key, user| user.name.starts_with('J'));
        println!("   Users with names starting with 'J': {}", j_users.len());

        // Get snapshot of all data
        let snapshot = user_table.snapshot();
        println!("   Snapshot contains {} entries", snapshot.len());
    }

    // 8. Monitor for a bit (or until Ctrl+C)
    println!("\n👀 Monitoring table for changes... (Press Ctrl+C to stop)");

    let initial_count = user_table.len();
    let monitor_duration = Duration::from_secs(30);
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < monitor_duration {
        // Check for interrupt
        if let Ok(_) =
            tokio::time::timeout(Duration::from_millis(100), tokio::signal::ctrl_c()).await
        {
            println!("\n🛑 Received shutdown signal");
            break;
        }

        // Check for changes
        let current_count = user_table.len();
        if current_count != initial_count {
            println!(
                "📈 Table size changed: {} -> {} users",
                initial_count, current_count
            );
            display_users(&user_table);
        }

        sleep(Duration::from_secs(1)).await;
    }

    // 9. Cleanup
    println!("\n🧹 Cleaning up...");
    user_table.stop();
    consumption_handle.abort();

    println!("✅ Simple Table example completed!");
    Ok(())
}

/// Display all users in the Table
fn display_users(user_table: &Table<String, User, JsonSerializer, JsonSerializer>) {
    println!("\n👥 Users in Table:");
    println!("{}", "-".repeat(40));

    if user_table.is_empty() {
        println!("   (No users)");
        return;
    }

    for user_id in user_table.keys() {
        if let Some(user) = user_table.get(&user_id) {
            println!("   {}: {} <{}>", user_id, user.name, user.email);
        }
    }
    println!();
}
