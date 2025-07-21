// Re-export the KafkaProducer from the kafka_producer module
mod kafka_producer;
mod kafka_consumer;
mod utils;
mod kafka_producer_def_context;
mod serialization;

pub use kafka_producer::KafkaProducer;
pub use kafka_consumer::KafkaConsumer;
pub use kafka_producer_def_context::LoggingProducerContext;
pub use utils::convert_kafka_log_level;
pub use serialization::{
    // Original serialization exports
    KafkaSerialize,
    KafkaDeserialize,
    SerializationError,
    to_json,
    from_json,
    to_avro,
    from_avro,
    to_proto,
    from_proto,

    // New Serializer trait and implementations
    Serializer,
    JsonSerializer,
};

// Conditional exports for feature-gated serializers
#[cfg(feature = "protobuf")]
pub use serialization::ProtoSerializer;

#[cfg(feature = "avro")]
pub use serialization::AvroSerializer;
