use futures::StreamExt; // Add this import
use serde::{Deserialize, Serialize};
use std::time::Duration;
use velostream::{FastConsumer, JsonSerializer};

#[derive(Debug, Serialize, Deserialize)]
struct MyMessage {
    id: String,
    data: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a consumer
    let consumer = FastConsumer::<String, MyMessage, JsonSerializer, JsonSerializer>::from_brokers(
        "localhost:9092",
        "metadata-example-group",
        JsonSerializer,
        JsonSerializer,
    )?;

    consumer.subscribe(&["test-topic"])?;

    // Demonstrate both polling and streaming with metadata
    println!("=== Polling Example ===");
    match consumer.poll(Duration::from_secs(5)).await {
        Ok(message) => {
            // Access all metadata at once
            println!("{}", message.metadata_string());

            // Or access individual fields
            println!("Partition: {}", message.partition());
            println!("Offset: {}", message.offset());

            // Get formatted timestamp
            if let Some(ts) = message.timestamp_string() {
                println!("Timestamp: {}", ts);
            }

            // Check if it's the first message in partition
            if message.is_first() {
                println!("This is the first message in the partition!");
            }
        }
        Err(e) => println!("Error polling: {}", e),
    }

    // Streaming example with metadata
    println!("\n=== Streaming Example ===");
    consumer
        .stream()
        .take(5) // Using StreamExt::take instead of Iterator::take
        .for_each(|result| async move {
            match result {
                Ok(message) => {
                    println!(
                        "\nProcessing message from partition {}",
                        message.partition()
                    );
                    println!(
                        "Offset {} in partition {}",
                        message.offset(),
                        message.partition()
                    );

                    if let Some(ts) = message.timestamp() {
                        println!(
                            "Message from timestamp: {}",
                            chrono::DateTime::from_timestamp_millis(ts)
                                .unwrap()
                                .format("%Y-%m-%d %H:%M:%S")
                        );
                    }
                }
                Err(e) => eprintln!("Error: {}", e),
            }
        })
        .await;

    Ok(())
}
