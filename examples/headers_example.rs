use ferrisstreams::{
    KafkaProducer, KafkaConsumer, JsonSerializer
};
use ferrisstreams::ferris::kafka::Headers;
use serde::{Serialize, Deserialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
struct OrderEvent {
    order_id: u64,
    customer_id: String,
    amount: f64,
    status: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = "localhost:9092";
    let topic = "headers-demo-topic";
    let group_id = "headers-demo-group";

    // Create producer with key/value serializers
    let producer = KafkaProducer::<String, OrderEvent, _, _>::new(
        broker, 
        topic, 
        JsonSerializer, 
        JsonSerializer
    )?;

    // Create consumer with key/value serializers
    let consumer = KafkaConsumer::<String, OrderEvent, _, _>::new(
        broker, 
        group_id, 
        JsonSerializer, 
        JsonSerializer
    )?;

    consumer.subscribe(&[topic])?;

    // Create a test message
    let order = OrderEvent {
        order_id: 12345,
        customer_id: "cust_001".to_string(),
        amount: 99.99,
        status: "pending".to_string(),
    };

    // Create headers with metadata
    let headers = Headers::new()
        .insert("source", "web-frontend")
        .insert("version", "1.2.3")
        .insert("trace-id", "abc-123-def")
        .insert("user-agent", "Mozilla/5.0");

    println!("Sending message with headers:");
    println!("Key: order-{}", order.order_id);
    println!("Value: {:?}", order);
    println!("Headers: {:?}", headers);

    // Send message with headers
    producer.send(
        Some(&format!("order-{}", order.order_id)), 
        &order, 
        headers, 
        None
    ).await?;
    
    producer.flush(5000)?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Consume the message
    println!("\nConsuming message...");
    match consumer.poll_message(Duration::from_secs(5)).await {
        Ok(message) => {
            println!("Received message:");
            println!("Key: {:?}", message.key());
            println!("Value: {:?}", message.value());
            println!("Headers:");
            
            // Access individual headers
            if let Some(source) = message.headers().get("source") {
                println!("  source: {}", source);
            }
            if let Some(version) = message.headers().get("version") {
                println!("  version: {}", version);
            }
            if let Some(trace_id) = message.headers().get("trace-id") {
                println!("  trace-id: {}", trace_id);
            }
            if let Some(user_agent) = message.headers().get("user-agent") {
                println!("  user-agent: {}", user_agent);
            }
            
            // Iterate over all headers
            println!("All headers:");
            for (key, value) in message.headers().iter() {
                match value {
                    Some(v) => println!("  {}: {}", key, v),
                    None => println!("  {}: <null>", key),
                }
            }
        }
        Err(e) => {
            eprintln!("Error consuming message: {}", e);
        }
    }

    consumer.commit()?;
    println!("Example completed successfully!");
    
    Ok(())
}