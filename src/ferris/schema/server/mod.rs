//! Schema Registry Server Components
//!
//! This module contains all server-side components for schema registry
//! implementations, including pluggable backends and authentication.

// Core server functionality
pub mod registry_backend;

// Backend implementations
pub mod backends;

// Re-export main server interfaces
pub use registry_backend::{
    BackendConfig, BackendMetadata, HealthStatus, SchemaRegistryBackend,
    SchemaRegistryBackendFactory,
};

// Re-export backend implementations
pub use backends::ConfluentAuth;

// Server type alias for consistency
pub type SchemaRegistryServer = dyn SchemaRegistryBackend;
