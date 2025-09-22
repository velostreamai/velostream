use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::{interval, sleep};
use velostream::velostream::kafka::consumer_config::{ConsumerConfig, IsolationLevel, OffsetReset};
use velostream::velostream::kafka::ktable::KTable;
use velostream::velostream::kafka::producer_config::{AckMode, ProducerConfig};
use velostream::velostream::kafka::serialization::JsonSerializer;
use velostream::{Headers, KafkaConsumer, KafkaProducer};

/// User profile stored in the KTable
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct UserProfile {
    user_id: String,
    name: String,
    email: String,
    age: u32,
    subscription_tier: String,
}

/// Order event from the stream
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Order {
    order_id: String,
    user_id: String,
    product: String,
    amount: f64,
    timestamp: u64,
}

/// Enriched order with user profile information
#[derive(Serialize, Deserialize, Debug, Clone)]
struct EnrichedOrder {
    order_id: String,
    user_id: String,
    product: String,
    amount: f64,
    timestamp: u64,
    // Enriched fields from user profile
    user_name: String,
    user_email: String,
    subscription_tier: String,
    discount_eligible: bool,
}

const KAFKA_BROKERS: &str = "localhost:9092";
const USER_PROFILES_TOPIC: &str = "user-profiles";
const ORDERS_TOPIC: &str = "orders";
const ENRICHED_ORDERS_TOPIC: &str = "enriched-orders";

/// Demonstrates KTable usage for stream-table joins
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 KTable Example: Stream-Table Join for Order Enrichment");
    println!("{}", "=".repeat(60));

    // Check if we should populate sample data
    let args: Vec<String> = std::env::args().collect();
    let populate_data = args.len() > 1 && args[1] == "--populate";

    if populate_data {
        println!("📝 Populating sample data...");
        populate_sample_data().await?;
        sleep(Duration::from_secs(2)).await;
    }

    // 1. Create User Profile KTable
    println!("🏗️  Creating User Profile KTable...");
    let user_table = create_user_profile_table().await?;

    // 2. Start KTable consumption in background
    println!("▶️  Starting KTable background consumption...");
    let table_clone = user_table.clone();
    let table_handle = tokio::spawn(async move {
        if let Err(e) = table_clone.start().await {
            eprintln!("❌ KTable error: {:?}", e);
        }
    });

    // 3. Wait for KTable to populate
    println!("⏳ Waiting for user profiles to load...");
    let loaded = user_table.wait_for_keys(1, Duration::from_secs(10)).await;
    if loaded {
        println!("✅ User profiles loaded: {} users", user_table.len());

        // Show current user profiles
        display_user_profiles(&user_table).await;
    } else {
        println!("⚠️  No user profiles found. Run with --populate to add sample data.");
        println!("   Example: cargo run --example ktable_example -- --populate");
    }

    // 4. Create order stream processor
    println!("\n🔄 Starting order stream processing...");
    let processor_handle = tokio::spawn(async move {
        if let Err(e) = process_order_stream(user_table).await {
            eprintln!("❌ Order processing error: {:?}", e);
        }
    });

    // 5. Simulate order processing for demo (or wait for real orders)
    if populate_data {
        println!("📦 Generating sample orders...");
        let order_generator = tokio::spawn(async move {
            if let Err(e) = generate_sample_orders().await {
                eprintln!("❌ Order generation error: {:?}", e);
            }
        });

        // Run for 30 seconds, then shutdown
        sleep(Duration::from_secs(30)).await;

        println!("\n🛑 Shutting down...");
        order_generator.abort();
    } else {
        println!("🔍 Monitoring for incoming orders... (Press Ctrl+C to stop)");

        // Wait for interrupt signal
        tokio::signal::ctrl_c().await?;
        println!("\n🛑 Received shutdown signal...");
    }

    // Cleanup
    processor_handle.abort();
    table_handle.abort();

    println!("✅ KTable example completed!");
    Ok(())
}

/// Creates and configures the User Profile KTable
async fn create_user_profile_table(
) -> Result<KTable<String, UserProfile, JsonSerializer, JsonSerializer>, Box<dyn std::error::Error>>
{
    let config = ConsumerConfig::new(KAFKA_BROKERS, "user-profile-table-group")
        .auto_offset_reset(OffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_commit(false, Duration::from_secs(5));

    let user_table = KTable::new(
        config,
        USER_PROFILES_TOPIC.to_string(),
        JsonSerializer,
        JsonSerializer,
    )
    .await?;

    Ok(user_table)
}

/// Displays current user profiles in the KTable
async fn display_user_profiles(
    user_table: &KTable<String, UserProfile, JsonSerializer, JsonSerializer>,
) {
    let stats = user_table.stats();
    println!("\n👥 Current User Profiles ({} users):", stats.key_count);
    println!("{}", "-".repeat(50));

    for user_id in user_table.keys() {
        if let Some(profile) = user_table.get(&user_id) {
            println!(
                "📋 {}: {} ({}) - {} tier",
                user_id, profile.name, profile.email, profile.subscription_tier
            );
        }
    }
    println!();
}

/// Processes order stream and enriches with user profile data
async fn process_order_stream(
    user_table: KTable<String, UserProfile, JsonSerializer, JsonSerializer>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create order consumer
    let order_config = ConsumerConfig::new(KAFKA_BROKERS, "order-processor-group")
        .auto_offset_reset(OffsetReset::Latest)
        .isolation_level(IsolationLevel::ReadCommitted);

    let order_consumer = KafkaConsumer::<String, Order, _, _>::with_config(
        order_config,
        JsonSerializer,
        JsonSerializer,
    )?;

    order_consumer.subscribe(&[ORDERS_TOPIC])?;

    // Create enriched order producer
    let producer_config = ProducerConfig::new(KAFKA_BROKERS, ENRICHED_ORDERS_TOPIC)
        .acks(AckMode::All)
        .idempotence(true);

    let enriched_producer =
        KafkaProducer::with_config(producer_config, JsonSerializer, JsonSerializer)?;

    println!("🎯 Order processor ready - listening for orders...");

    // Process order stream
    let mut stream = order_consumer.stream();
    while let Some(message_result) = stream.next().await {
        match message_result {
            Ok(message) => {
                let order = message.value();
                println!(
                    "\n📦 Processing order: {} for user {}",
                    order.order_id, order.user_id
                );

                // Lookup user profile from KTable
                match user_table.get(&order.user_id) {
                    Some(user_profile) => {
                        // Enrich order with user profile
                        let enriched_order = EnrichedOrder {
                            order_id: order.order_id.clone(),
                            user_id: order.user_id.clone(),
                            product: order.product.clone(),
                            amount: order.amount,
                            timestamp: order.timestamp,
                            user_name: user_profile.name.clone(),
                            user_email: user_profile.email.clone(),
                            subscription_tier: user_profile.subscription_tier.clone(),
                            discount_eligible: user_profile.subscription_tier == "premium"
                                || user_profile.subscription_tier == "enterprise",
                        };

                        println!(
                            "✨ Enriched order for {} ({}) - {} tier, discount: {}",
                            enriched_order.user_name,
                            enriched_order.user_email,
                            enriched_order.subscription_tier,
                            if enriched_order.discount_eligible {
                                "eligible"
                            } else {
                                "not eligible"
                            }
                        );

                        // Send enriched order to output topic
                        match enriched_producer
                            .send(
                                Some(&enriched_order.order_id),
                                &enriched_order,
                                Headers::new(),
                                None,
                            )
                            .await
                        {
                            Ok(_) => {
                                println!("✅ Enriched order sent to {}", ENRICHED_ORDERS_TOPIC)
                            }
                            Err(e) => eprintln!("❌ Failed to send enriched order: {:?}", e),
                        }
                    }
                    None => {
                        println!(
                            "⚠️  User profile not found for user: {} (order: {})",
                            order.user_id, order.order_id
                        );
                        println!("   Available users: {:?}", user_table.keys());
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Error processing order message: {:?}", e);
            }
        }
    }

    Ok(())
}

/// Populates sample user profiles and initial data
async fn populate_sample_data() -> Result<(), Box<dyn std::error::Error>> {
    let producer_config = ProducerConfig::new(KAFKA_BROKERS, USER_PROFILES_TOPIC)
        .acks(AckMode::All)
        .idempotence(true);

    let producer = KafkaProducer::with_config(producer_config, JsonSerializer, JsonSerializer)?;

    // Sample user profiles
    let users = vec![
        UserProfile {
            user_id: "user-001".to_string(),
            name: "Alice Johnson".to_string(),
            email: "alice@example.com".to_string(),
            age: 28,
            subscription_tier: "premium".to_string(),
        },
        UserProfile {
            user_id: "user-002".to_string(),
            name: "Bob Smith".to_string(),
            email: "bob@example.com".to_string(),
            age: 35,
            subscription_tier: "basic".to_string(),
        },
        UserProfile {
            user_id: "user-003".to_string(),
            name: "Carol Wilson".to_string(),
            email: "carol@example.com".to_string(),
            age: 42,
            subscription_tier: "enterprise".to_string(),
        },
        UserProfile {
            user_id: "user-004".to_string(),
            name: "David Brown".to_string(),
            email: "david@example.com".to_string(),
            age: 31,
            subscription_tier: "basic".to_string(),
        },
    ];

    // Send user profiles to KTable topic
    for user in users {
        match producer
            .send(Some(&user.user_id), &user, Headers::new(), None)
            .await
        {
            Ok(_) => println!("✅ Sent user profile: {} ({})", user.name, user.user_id),
            Err(e) => eprintln!("❌ Failed to send user profile: {:?}", e),
        }
    }

    Ok(())
}

/// Generates sample orders for demonstration
async fn generate_sample_orders() -> Result<(), Box<dyn std::error::Error>> {
    let producer_config = ProducerConfig::new(KAFKA_BROKERS, ORDERS_TOPIC).acks(AckMode::All);

    let producer = KafkaProducer::with_config(producer_config, JsonSerializer, JsonSerializer)?;

    let users = ["user-001", "user-002", "user-003", "user-004"];
    let products = ["laptop", "smartphone", "tablet", "headphones", "monitor"];

    let mut interval = interval(Duration::from_secs(3));
    let mut order_counter = 1;

    loop {
        interval.tick().await;

        // Generate random order
        let user_id = users[order_counter % users.len()];
        let product = products[order_counter % products.len()];
        let amount = (order_counter as f64 * 99.99) % 999.99 + 50.0;

        let order = Order {
            order_id: format!("order-{:03}", order_counter),
            user_id: user_id.to_string(),
            product: product.to_string(),
            amount: (amount * 100.0).round() / 100.0, // Round to 2 decimal places
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        match producer
            .send(Some(&order.order_id), &order, Headers::new(), None)
            .await
        {
            Ok(_) => println!(
                "📦 Generated order: {} - {} ${:.2} for {}",
                order.order_id, order.product, order.amount, order.user_id
            ),
            Err(e) => eprintln!("❌ Failed to send order: {:?}", e),
        }

        order_counter += 1;

        // Stop after 10 orders for demo
        if order_counter > 10 {
            break;
        }
    }

    Ok(())
}
