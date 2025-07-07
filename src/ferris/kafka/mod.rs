// Re-export the KafkaProducer from the kafka_producer module
mod kafka_producer;
mod kafka_consumer;

pub use kafka_producer::KafkaProducer;
pub use kafka_consumer::KafkaConsumer;
