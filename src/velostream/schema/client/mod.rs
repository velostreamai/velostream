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
pub use multilevel_cache::MultiLevelCacheConfig;
pub use registry_client::RegistryClientConfig;
