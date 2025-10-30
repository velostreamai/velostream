//! Pluggable Schema Registry Backend Architecture
//!
//! Provides a unified interface for different schema registry implementations,
//! supporting Confluent Schema Registry, Apache Pulsar Schema Registry,
//! Amazon MSK Schema Registry, and custom implementations.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::velostream::schema::SchemaResult;
use crate::velostream::schema::client::registry_client::SchemaReference;

pub use super::backends::{
    AmazonMskSchemaRegistryBackend, AwsCredentials, ConfluentAuth, ConfluentSchemaRegistryBackend,
    FileSystemSchemaRegistryBackend, InMemorySchemaRegistryBackend, PulsarSchemaRegistryBackend,
    SchemaVersion,
};

/// Trait defining the interface for schema registry backends
#[async_trait]
pub trait SchemaRegistryBackend: Send + Sync {
    /// Get a schema by its unique ID
    async fn get_schema(&self, id: u32) -> SchemaResult<SchemaResponse>;

    /// Get the latest version of a schema for a subject
    async fn get_latest_schema(&self, subject: &str) -> SchemaResult<SchemaResponse>;

    /// Get a specific version of a schema for a subject
    async fn get_schema_version(&self, subject: &str, version: i32)
    -> SchemaResult<SchemaResponse>;

    /// Register a new schema version
    async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        references: Vec<SchemaReference>,
    ) -> SchemaResult<u32>;

    /// Check if a schema is compatible with the latest version
    async fn check_compatibility(&self, subject: &str, schema: &str) -> SchemaResult<bool>;

    /// Get all versions of a subject
    async fn get_versions(&self, subject: &str) -> SchemaResult<Vec<i32>>;

    /// Get all subjects
    async fn get_subjects(&self) -> SchemaResult<Vec<String>>;

    /// Delete a schema version
    async fn delete_schema_version(&self, subject: &str, version: i32) -> SchemaResult<()>;

    /// Get backend metadata
    fn metadata(&self) -> BackendMetadata;

    /// Health check
    async fn health_check(&self) -> SchemaResult<HealthStatus>;
}

/// Standard schema response format across all backends
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResponse {
    pub id: u32,
    pub subject: String,
    pub version: i32,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    pub metadata: HashMap<String, String>,
}

/// Backend metadata information
#[derive(Debug, Clone)]
pub struct BackendMetadata {
    pub backend_type: String,
    pub version: String,
    pub supported_features: Vec<String>,
    pub capabilities: BackendCapabilities,
}

/// Backend capabilities
#[derive(Debug, Clone)]
pub struct BackendCapabilities {
    pub supports_references: bool,
    pub supports_evolution: bool,
    pub supports_compatibility_check: bool,
    pub supports_deletion: bool,
    pub supports_subjects_listing: bool,
    pub max_schema_size: Option<usize>,
    pub max_references_per_schema: Option<usize>,
}

/// Health status information
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub message: String,
    pub response_time_ms: u64,
    pub details: HashMap<String, String>,
}

/// Configuration for different backend types
#[derive(Clone)]
pub enum BackendConfig {
    Confluent {
        base_url: String,
        auth: ConfluentAuth,
        timeout_seconds: u64,
        max_retries: u32,
    },
    Pulsar {
        service_url: String,
        auth_token: Option<String>,
        namespace: String,
        timeout_seconds: u64,
    },
    AmazonMsk {
        cluster_arn: String,
        region: String,
        registry_name: String,
        credentials: AwsCredentials,
    },
    Custom {
        implementation: Arc<dyn SchemaRegistryBackend>,
    },
    InMemory {
        initial_schemas: HashMap<String, Vec<SchemaVersion>>,
    },
    FileSystem {
        base_path: std::path::PathBuf,
        watch_for_changes: bool,
        auto_create_directories: bool,
    },
}

/// Factory for creating schema registry backends
pub struct SchemaRegistryBackendFactory;

impl SchemaRegistryBackendFactory {
    /// Create a backend from configuration
    pub fn create(config: BackendConfig) -> SchemaResult<Arc<dyn SchemaRegistryBackend>> {
        match config {
            BackendConfig::Confluent {
                base_url,
                auth,
                timeout_seconds,
                max_retries,
            } => Ok(Arc::new(ConfluentSchemaRegistryBackend::new(
                base_url,
                auth,
                timeout_seconds,
                max_retries,
            )?)),
            BackendConfig::Pulsar {
                service_url,
                auth_token,
                namespace,
                timeout_seconds,
            } => Ok(Arc::new(PulsarSchemaRegistryBackend::new(
                service_url,
                auth_token,
                namespace,
                timeout_seconds,
            )?)),
            BackendConfig::AmazonMsk {
                cluster_arn,
                region,
                registry_name,
                credentials,
            } => Ok(Arc::new(AmazonMskSchemaRegistryBackend::new(
                cluster_arn,
                region,
                registry_name,
                credentials,
            )?)),
            BackendConfig::Custom { implementation } => Ok(implementation),
            BackendConfig::InMemory { initial_schemas } => Ok(Arc::new(
                InMemorySchemaRegistryBackend::new(initial_schemas),
            )),
            BackendConfig::FileSystem {
                base_path,
                watch_for_changes,
                auto_create_directories,
            } => Ok(Arc::new(FileSystemSchemaRegistryBackend::new(
                base_path,
                watch_for_changes,
                auto_create_directories,
            )?)),
        }
    }

    /// Auto-detect backend type from URL
    pub fn auto_detect(url: &str) -> BackendConfig {
        if url.contains("confluent") || url.contains(":8081") {
            BackendConfig::Confluent {
                base_url: url.to_string(),
                auth: ConfluentAuth::None,
                timeout_seconds: 30,
                max_retries: 3,
            }
        } else if url.contains("pulsar") {
            BackendConfig::Pulsar {
                service_url: url.to_string(),
                auth_token: None,
                namespace: "public/default".to_string(),
                timeout_seconds: 30,
            }
        } else if url.starts_with("arn:aws:kafka") {
            BackendConfig::AmazonMsk {
                cluster_arn: url.to_string(),
                region: "us-east-1".to_string(),
                registry_name: "default".to_string(),
                credentials: AwsCredentials {
                    access_key_id: "".to_string(),
                    secret_access_key: "".to_string(),
                    session_token: None,
                },
            }
        } else {
            // Default to Confluent
            BackendConfig::Confluent {
                base_url: url.to_string(),
                auth: ConfluentAuth::None,
                timeout_seconds: 30,
                max_retries: 3,
            }
        }
    }
}
