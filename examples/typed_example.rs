use serde::{Deserialize, Serialize};
use std::time::Duration;
use velostream::velostream::kafka::Headers;
use velostream::{JsonSerializer, FastConsumer, KafkaProducer};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct User {
    id: u32,
    name: String,
    email: String,
}

impl User {
    fn new(id: u32, name: &str, email: &str) -> Self {
        Self {
            id,
            name: name.to_string(),
            email: email.to_string(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    println!("🚀 Type-Safe Kafka Example Starting...");

    // Create producer for User messages
    let producer = match KafkaProducer::<String, User, _, _>::new(
        "localhost:9092",
        "users",
        JsonSerializer,
        JsonSerializer,
    ) {
        Ok(p) => {
            println!("✅ Producer created successfully");
            p
        }
        Err(e) => {
            println!("❌ Failed to create producer: {}", e);
            println!("💡 Make sure Kafka is running on localhost:9092");
            return Ok(());
        }
    };

    // Create consumer for User messages
    let consumer = match FastConsumer::<String, User>::new(
        "localhost:9092",
        "user-processor",
        JsonSerializer,
        JsonSerializer,
    ) {
        Ok(c) => c,
        Err(e) => {
            println!("❌ Failed to create consumer: {}", e);
            return Ok(());
        }
    };

    consumer.subscribe(&["users"]).expect("Failed to subscribe");
    println!("✅ Consumer subscribed to 'users' topic");

    // Example: Send a few user messages
    let users = vec![
        User::new(1, "Alice Smith", "alice@example.com"),
        User::new(2, "Bob Jones", "bob@example.com"),
        User::new(3, "Carol Brown", "carol@example.com"),
    ];

    println!("\n📤 Sending users...");
    for user in &users {
        let key = format!("user-{}", user.id);
        match producer.send(Some(&key), user, Headers::new(), None).await {
            Ok(_) => println!("  ✅ Sent user: {}", user.name),
            Err(e) => println!("  ❌ Failed to send user {}: {}", user.name, e),
        }
    }

    // Flush to ensure all messages are sent
    if let Err(e) = producer.flush(5000) {
        println!("❌ Failed to flush producer: {}", e);
    }

    println!("\n📥 Consuming messages...");
    let mut received_count = 0;
    let max_messages = 5;

    while received_count < max_messages {
        match consumer.poll(Duration::from_secs(2)).await {
            Ok(message) => {
                println!("📦 Received user: {:?}", message.value());
                if let Some(key) = message.key() {
                    println!("   🔑 Key: {}", key);
                }

                // Demonstrate type safety - we get a User struct directly!
                let user = message.value();
                println!(
                    "   📧 Email domain: {}",
                    user.email.split('@').nth(1).unwrap_or("unknown")
                );

                received_count += 1;
            }
            Err(e) => {
                println!("⏰ Timeout or error: {:?}", e);
                break;
            }
        }
    }

    // Commit the consumer state
    if let Err(e) = consumer.commit() {
        println!("❌ Failed to commit: {}", e);
    }

    println!("\n🎉 Type-safe Kafka example completed!");
    println!("💡 Key benefits demonstrated:");
    println!("   - Compile-time type safety");
    println!("   - Automatic serialization/deserialization");
    println!("   - Clean, intuitive API");
    println!("   - No manual byte array handling");

    Ok(())
}
