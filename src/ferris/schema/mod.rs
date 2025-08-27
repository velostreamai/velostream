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
pub mod error;
pub mod types;

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
    CompatibilityMode, FieldDefinition, PartitionMetadata, Schema, SchemaMetadata, StreamHandle,
    StreamMetadata,
};

pub use error::{SchemaError, SchemaResult};

// Re-export client components
pub use client::{
    EnhancedSchemaCache, ProviderMetadata, RegistryClientConfig, SchemaCache, SchemaProvider,
    SchemaReferenceResolver, SchemaRegistryClient, UnifiedSchemaRegistryClient,
};

// Re-export client sub-components for convenience
pub use client::cache::{CacheConfig, CacheLookupResult};
pub use client::providers::create_default_registry;

// Re-export server components
pub use server::{
    BackendCapabilities, BackendConfig, BackendMetadata, HealthStatus, SchemaRegistryBackend,
    SchemaRegistryBackendFactory, SchemaRegistryServer, SchemaResponse,
};

// Re-export evolution components
pub use evolution::{EvolutionConfig, FieldModification, SchemaDiff, SchemaEvolution};
