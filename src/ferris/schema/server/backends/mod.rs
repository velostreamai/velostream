//! Schema Registry Backend Implementations
//!
//! This module contains concrete implementations of the SchemaRegistryBackend trait
//! for different schema registry systems.

pub mod amazon_msk;
pub mod confluent;
pub mod filesystem;
pub mod memory;
pub mod pulsar;

// Re-export all backend implementations
pub use amazon_msk::{AmazonMskSchemaRegistryBackend, AwsCredentials};
pub use confluent::{ConfluentAuth, ConfluentSchemaRegistryBackend};
pub use filesystem::FileSystemSchemaRegistryBackend;
pub use memory::{InMemorySchemaRegistryBackend, SchemaVersion};
pub use pulsar::PulsarSchemaRegistryBackend;
