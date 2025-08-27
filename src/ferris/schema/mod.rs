//! Schema Management System
//!
//! This module provides a comprehensive schema management system with pluggable
//! backend support and client/server architecture separation.
//!
//! ## Architecture
//!
//! The schema system is organized into client and server components:
//!
//! ### Client Components (`schema::client`)
//! - **Registry Client**: Main interface for schema operations
//! - **Unified Client**: High-level client with advanced features
//! - **Caching System**: Multi-level caching with LRU and prefetching
//! - **Reference Resolver**: Schema reference resolution with circular detection
//! - **Schema Providers**: Discovery from heterogeneous data sources
//!
//! ### Server Components (`schema::server`)
//! - **Registry Backend**: Pluggable backend trait and factory
//! - **Backend Implementations**: Confluent, FileSystem, In-Memory, etc.
//! - **Authentication**: Multiple auth patterns (Basic, Bearer, API Keys)
//! - **Performance Monitoring**: Metrics, health checks, benchmarking
//!
//! ## Core Types
//!
//! The system uses several core types for schema representation:
//! - `Schema`: Main schema definition with fields and metadata
//! - `FieldDefinition`: Individual field specifications with types
//! - `SchemaMetadata`: Versioning, compatibility, and source information
//! - `SchemaError`: Comprehensive error handling for all operations

// Core types and errors
pub mod types;
pub mod error;

// Client-side components
pub mod client;

// Server-side components
pub mod server;

// Schema evolution and compatibility
pub mod evolution;

// Legacy registry (being phased out)
pub mod registry;

// Re-export commonly used types
pub use types::{
    Schema, FieldDefinition, SchemaMetadata,
    StreamHandle, StreamMetadata, PartitionMetadata,
    CompatibilityMode,
};

pub use error::{SchemaError, SchemaResult};

// Re-export client components
pub use client::{
    SchemaRegistryClient, UnifiedSchemaRegistryClient, RegistryClientConfig,
    SchemaReferenceResolver, SchemaProvider, ProviderMetadata,
    SchemaCache, EnhancedSchemaCache,
};

// Re-export client sub-components for convenience
pub use client::providers::create_default_registry;
pub use client::cache::{CacheConfig, CacheLookupResult};

// Re-export server components
pub use server::{
    SchemaRegistryBackend, BackendConfig, BackendCapabilities, BackendMetadata,
    HealthStatus, SchemaResponse, SchemaRegistryBackendFactory, SchemaRegistryServer,
};

// Re-export evolution components
pub use evolution::{
    SchemaEvolution, EvolutionConfig, SchemaDiff, FieldModification,
};