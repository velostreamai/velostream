// Kafka modules
mod kafka_producer;
mod kafka_consumer;
mod kafka_producer_def_context;
mod serialization;
mod utils;

// Re-export main API
pub use kafka_producer::{KafkaProducer, ProducerBuilder, ProducerError};
pub use kafka_consumer::{KafkaConsumer, ConsumerBuilder, ConsumerError, Message, KafkaConsumable, Headers};
pub use kafka_producer_def_context::LoggingProducerContext;
pub use serialization::{Serializer, SerializationError, JsonSerializer};
pub use utils::convert_kafka_log_level;

// Conditional exports for feature-gated serializers
#[cfg(feature = "protobuf")]
pub use serialization::ProtoSerializer;

#[cfg(feature = "avro")]
pub use serialization::AvroSerializer;
