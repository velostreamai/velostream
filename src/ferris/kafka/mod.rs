// Re-export the KafkaProducer from the kafka_producer module
mod kafka_producer;
mod kafka_consumer;
mod utils;

pub use kafka_producer::KafkaProducer;
pub use kafka_consumer::KafkaConsumer;
pub use kafka_producer::LoggingProducerContext;
pub use utils::convert_kafka_log_level;
