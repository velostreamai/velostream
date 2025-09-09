//! Schema Registry Client Components
//!
//! This module contains all client-side components for interacting with
//! schema registries, including caching, reference resolution, and
//! schema discovery from heterogeneous data sources.

// Core client functionality
pub mod registry_client;
pub mod unified_client;

// Caching system
pub mod cache;
pub mod enhanced_cache;
pub mod multilevel_cache;

// Reference resolution
pub mod reference_resolver;

// Schema providers for discovery
pub mod providers;

// Re-export main client interfaces
pub use reference_resolver::SchemaReferenceResolver;
pub use registry_client::{RegistryClientConfig, SchemaRegistryClient};
pub use unified_client::UnifiedSchemaRegistryClient;
// Re-export provider types directly from registry
pub use crate::ferris::schema::registry::{ProviderMetadata, SchemaProvider};

// Re-export caching components
pub use cache::SchemaCache;
pub use enhanced_cache::EnhancedSchemaCache;
pub use multilevel_cache::MultiLevelCacheConfig;
