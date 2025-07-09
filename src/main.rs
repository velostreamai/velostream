
use log::{info, warn, error, debug, trace, LevelFilter};
use env_logger::Env;
use crate::ferris::kafka::{KafkaProducer, LoggingProducerContext};

// Import the module structure
mod ferris;


#[tokio::main]
async fn main() -> Result<(), rdkafka::error::KafkaError> {
    // Initialize the logger
    pretty_env_logger::init();

    println!("Kafka Producer Example");

    // Create a KafkaProducer instance
    let producer = match KafkaProducer::<LoggingProducerContext>::new("localhost:9092", "test-topic") {
        Ok(p) => {
            info!("Successfully created Kafka producer");
            p
        },
        Err(e) => {
            error!("Failed to create Kafka producer: {}", e);
            return Err(e);
        }
    };

    // Example: Send a message with a key
    info!("Sending message with key...");
    if let Err(e) = producer.send(Some("example-key"), "Hello, Kafka!", None).await {
        error!("Failed to send message: {}", e);
        return Err(e);
    }

    // Example: Send a message without a key
    info!("Sending message without key...");
    if let Err(e) = producer.send(None, "Another message without a key", None).await {
        error!("Failed to send message: {}", e);
        return Err(e);
    }

    // Example: Send a message to a different topic
    info!("Sending message to a different topic...");
    if let Err(e) = producer.send_to_topic("another-topic", Some("different-key"), "Message to another topic", None).await {
        error!("Failed to send message to different topic: {}", e);
        return Err(e);
    }

    // Flush any pending messages
    info!("Flushing producer...");
    if let Err(e) = producer.flush(5000) {
        error!("Failed to flush producer: {}", e);
        return Err(e);
    }

    info!("All messages sent successfully!");
    println!("Kafka Producer example completed successfully");

    Ok(())
}
