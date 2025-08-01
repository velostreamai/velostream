use crate::ferris::{
    error::FerrisResult,
    kafka::{Headers, JsonSerializer, KafkaProducer},
};
use serde::{Deserialize, Serialize};

// Import the module structure
mod ferris;

#[derive(Serialize, Deserialize, Debug)]
struct TestMessage {
    id: u32,
    content: String,
}

#[tokio::main]
async fn main() -> FerrisResult<()> {
    // Initialize the logger
    pretty_env_logger::init();

    println!("Kafka Producer Example");

    // Create a KafkaProducer instance
    let producer = match KafkaProducer::<String, TestMessage, _, _>::new(
        "localhost:9092",
        "test-topic",
        JsonSerializer,
        JsonSerializer,
    ) {
        Ok(p) => {
            println!("Successfully created Kafka producer");
            p
        }
        Err(e) => {
            println!("Failed to create Kafka producer: {}", e);
            return Ok(());
        }
    };

    // Create and send a test message
    let message = TestMessage {
        id: 1,
        content: "Hello, Kafka!".to_string(),
    };

    // Send the message
    match producer
        .send(
            Some(&"test-key".to_string()),
            &message,
            Headers::new(),
            None,
        )
        .await
    {
        Ok(_) => println!("✅ Message sent successfully: {:?}", message),
        Err(e) => println!("❌ Failed to send message: {}", e),
    }

    // Flush to ensure message is sent
    if let Err(e) = producer.flush(5000) {
        println!("❌ Failed to flush producer: {}", e);
    } else {
        println!("✅ Producer flushed successfully");
    }

    println!("Kafka Producer Example completed");
    Ok(())
}
