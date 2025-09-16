// tests/velo/kafka/test_utils.rs
use crate::unit::test_messages::*;
use std::net::TcpStream;
use std::time::Duration;
use uuid::Uuid;
use velostream::velostream::kafka::serialization::Serializer;
use velostream::velostream::kafka::{Headers, JsonSerializer, KafkaConsumer, KafkaProducer};

/// Helper functions
pub(crate) fn is_kafka_running() -> bool {
    match TcpStream::connect("localhost:9092") {
        Ok(_) => true,
        Err(_) => {
            println!("WARNING: Kafka is not running at localhost:9092");
            println!("Start Kafka with: docker-compose up -d");
            println!("Wait for it to start with: ./test-kafka.sh");
            println!("Tests requiring Kafka will be skipped.");
            false
        }
    }
}

pub(crate) fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

pub(crate) fn init() -> bool {
    init_logger();
    is_kafka_running()
}

// Topic and group ID generators
pub(crate) fn generate_topic(prefix: &str) -> String {
    format!("{}-{}", prefix, Uuid::new_v4())
}

pub(crate) fn generate_group_id(prefix: &str) -> String {
    format!("{}-group-{}", prefix, Uuid::new_v4())
}

// Producer creation helpers
pub(crate) fn create_test_message_producer(
    topic: &str,
) -> Result<
    KafkaProducer<String, TestMessage, JsonSerializer, JsonSerializer>,
    Box<dyn std::error::Error>,
> {
    Ok(KafkaProducer::new(
        "localhost:9092",
        topic,
        JsonSerializer,
        JsonSerializer,
    )?)
}

pub(crate) fn create_user_producer(
    topic: &str,
) -> Result<KafkaProducer<String, User, JsonSerializer, JsonSerializer>, Box<dyn std::error::Error>>
{
    Ok(KafkaProducer::new(
        "localhost:9092",
        topic,
        JsonSerializer,
        JsonSerializer,
    )?)
}

pub(crate) fn create_order_producer(
    topic: &str,
) -> Result<
    KafkaProducer<String, OrderEvent, JsonSerializer, JsonSerializer>,
    Box<dyn std::error::Error>,
> {
    Ok(KafkaProducer::new(
        "localhost:9092",
        topic,
        JsonSerializer,
        JsonSerializer,
    )?)
}

// Consumer creation helpers
pub(crate) fn create_test_message_consumer(
    group_id: &str,
) -> Result<
    KafkaConsumer<String, TestMessage, JsonSerializer, JsonSerializer>,
    Box<dyn std::error::Error>,
> {
    Ok(KafkaConsumer::new(
        "localhost:9092",
        group_id,
        JsonSerializer,
        JsonSerializer,
    )?)
}

pub(crate) fn create_user_consumer(
    group_id: &str,
) -> Result<KafkaConsumer<String, User, JsonSerializer, JsonSerializer>, Box<dyn std::error::Error>>
{
    Ok(KafkaConsumer::new(
        "localhost:9092",
        group_id,
        JsonSerializer,
        JsonSerializer,
    )?)
}

pub(crate) fn create_order_consumer(
    group_id: &str,
) -> Result<
    KafkaConsumer<String, OrderEvent, JsonSerializer, JsonSerializer>,
    Box<dyn std::error::Error>,
> {
    Ok(KafkaConsumer::new(
        "localhost:9092",
        group_id,
        JsonSerializer,
        JsonSerializer,
    )?)
}

// Common test operations
pub(crate) async fn send_and_flush<K, V, KS, VS>(
    producer: &KafkaProducer<K, V, KS, VS>,
    key: Option<&K>,
    value: &V,
    headers: Headers,
) -> Result<(), Box<dyn std::error::Error>>
where
    KS: Serializer<K>,
    VS: Serializer<V>,
{
    producer.send(key, value, headers, None).await?;
    producer.flush(5000)?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    Ok(())
}

// Test patterns
pub(crate) struct TestSetup<T>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    pub topic: String,
    pub group_id: String,
    #[allow(dead_code)]
    pub producer: KafkaProducer<String, T, JsonSerializer, JsonSerializer>,
    #[allow(dead_code)]
    pub consumer: KafkaConsumer<String, T, JsonSerializer, JsonSerializer>,
}

impl TestSetup<TestMessage> {
    pub(crate) fn new_test_message(prefix: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let topic = generate_topic(prefix);
        let group_id = generate_group_id(prefix);
        let producer = create_test_message_producer(&topic)?;
        let consumer = create_test_message_consumer(&group_id)?;

        consumer.subscribe(&[&topic])?;

        Ok(Self {
            topic,
            group_id,
            producer,
            consumer,
        })
    }
}

impl TestSetup<User> {
    pub(crate) fn new_user(prefix: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let topic = generate_topic(prefix);
        let group_id = generate_group_id(prefix);
        let producer = create_user_producer(&topic)?;
        let consumer = create_user_consumer(&group_id)?;

        consumer.subscribe(&[&topic])?;

        Ok(Self {
            topic,
            group_id,
            producer,
            consumer,
        })
    }
}

impl TestSetup<OrderEvent> {
    pub(crate) fn new_order(prefix: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let topic = generate_topic(prefix);
        let group_id = generate_group_id(prefix);
        let producer = create_order_producer(&topic)?;
        let consumer = create_order_consumer(&group_id)?;

        consumer.subscribe(&[&topic])?;

        Ok(Self {
            topic,
            group_id,
            producer,
            consumer,
        })
    }
}

// Safe commit helper
pub(crate) fn safe_commit<K, V, KS, VS>(
    consumer: &KafkaConsumer<K, V, KS, VS>,
    received_count: usize,
) where
    KS: Serializer<K>,
    VS: Serializer<V>,
{
    if received_count > 0 {
        let _ = consumer.commit(); // Make commit optional
    }
}
