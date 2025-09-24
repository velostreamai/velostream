//! # velostream
//!
//! A Rust-idiomatic and robust client library for Apache Kafka, designed for high-performance,
//! fault-tolerant, and flexible processing of multiple Kafka topics and data streams with full
//! support for keys, values, and headers.

// Allow certain clippy warnings for development and framework patterns
#![allow(clippy::derivable_impls)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::needless_doctest_main)]
#![allow(clippy::manual_strip)]
#![allow(clippy::wrong_self_convention)]
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::large_enum_variant)]
#![allow(dead_code)] // Framework components may not be used in all configurations
#![allow(clippy::redundant_closure)]
#![allow(clippy::for_kv_map)]
#![allow(clippy::redundant_pattern_matching)]
#![allow(clippy::collapsible_match)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::incompatible_msrv)]
#![allow(clippy::needless_update)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::match_like_matches_macro)]
#![allow(clippy::borrowed_box)]
#![allow(clippy::ptr_arg)]
#![allow(clippy::default_constructed_unit_structs)]
#![allow(clippy::assertions_on_constants)]
#![allow(clippy::unnecessary_literal_unwrap)]
#![allow(clippy::single_match)]
#![allow(unused_variables)]
#![allow(clippy::useless_format)]
#![allow(unused_imports)]
#![allow(clippy::needless_borrows_for_generic_args)]
#![allow(clippy::format_in_format_args)]
#![allow(clippy::unnecessary_get_then_check)]
#![allow(clippy::useless_vec)]
#![allow(unused_comparisons)]
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
//! use velostream::{KafkaProducer, KafkaConsumer, JsonSerializer, Headers};
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

// Export the velostream.kafka.app module structure
pub mod velostream;

// Re-export main API at crate root for easy access
pub use velostream::kafka::{
    ConsumerBuilder, Headers, JsonSerializer, KafkaConsumer, KafkaProducer, Message,
    ProducerBuilder,
};
pub use velostream::table::Table;
