//! # ferrisstreams
//!
//! A Rust-idiomatic and robust client library for Apache Kafka, designed for high-performance,
//! fault-tolerant, and flexible processing of multiple Kafka topics and data streams with full
//! support for keys, values, and headers.

// Allow certain clippy warnings for development
#![allow(clippy::derivable_impls)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::needless_doctest_main)]
#![allow(clippy::manual_strip)]
#![allow(clippy::wrong_self_convention)]
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::large_enum_variant)]
//!
//! ## Features
//!
//! - **Type-Safe Kafka Operations**: Full support for typed keys, values, and headers
//! - **Rich Headers Support**: Custom `Headers` type with clean API for message metadata
//! - **Asynchronous Processing**: Built on `rdkafka` & `tokio` for efficient, non-blocking I/O
//! - **Stream Processing**: Both polling and streaming consumption patterns
//! - **Builder Patterns**: Ergonomic APIs for creating producers and consumers
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use ferrisstreams::{KafkaProducer, KafkaConsumer, JsonSerializer, Headers};
//! use serde::{Serialize, Deserialize};
//! use std::time::Duration;
//!
//! #[derive(Serialize, Deserialize, Debug)]
//! struct MyMessage {
//!     id: u64,
//!     content: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Producer with headers
//!     let producer = KafkaProducer::<String, MyMessage, _, _>::new(
//!         "localhost:9092",
//!         "my-topic",
//!         JsonSerializer,
//!         JsonSerializer,
//!     )?;
//!
//!     let headers = Headers::new()
//!         .insert("source", "web-api")
//!         .insert("version", "1.0.0");
//!
//!     let message = MyMessage { id: 1, content: "Hello".to_string() };
//!     producer.send(Some(&"key-1".to_string()), &message, headers, None).await?;
//!
//!     // Consumer with headers
//!     let consumer = KafkaConsumer::<String, MyMessage, _, _>::new(
//!         "localhost:9092",
//!         "my-group",
//!         JsonSerializer,
//!         JsonSerializer,
//!     )?;
//!
//!     consumer.subscribe(&["my-topic"])?;
//!
//!     if let Ok(message) = consumer.poll(Duration::from_secs(5)).await {
//!         println!("Key: {:?}", message.key());
//!         println!("Value: {:?}", message.value());
//!         println!("Headers: {:?}", message.headers());
//!     }
//!
//!     Ok(())
//! }
//! ```

// Export the ferris.kafka.app module structure
pub mod ferris;

// Re-export main API at crate root for easy access
pub use ferris::kafka::{
    BytesSerializer,
    ConsumerBuilder,

    ConsumerError,

    Headers,
    // Serializers
    JsonSerializer,
    KafkaAdminClient,

    // Traits
    KafkaConsumable,
    KafkaConsumer,
    // Core types
    KafkaProducer,
    Message,
    // Builders
    ProducerBuilder,
    // Errors
    ProducerError,
    SerializationError,
    Serializer,
};
