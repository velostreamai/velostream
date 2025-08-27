//! Pluggable Schema Registry Backend Architecture
//!
//! Provides a unified interface for different schema registry implementations,
//! supporting Confluent Schema Registry, Apache Pulsar Schema Registry,
//! Amazon MSK Schema Registry, and custom implementations.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::registry_client::SchemaReference;
use super::{SchemaError, SchemaResult};

/// Trait defining the interface for schema registry backends
#[async_trait]
pub trait SchemaRegistryBackend: Send + Sync {
    /// Get a schema by its unique ID
    async fn get_schema(&self, id: u32) -> SchemaResult<SchemaResponse>;
    
    /// Get the latest version of a schema for a subject
    async fn get_latest_schema(&self, subject: &str) -> SchemaResult<SchemaResponse>;
    
    /// Get a specific version of a schema for a subject
    async fn get_schema_version(&self, subject: &str, version: i32) -> SchemaResult<SchemaResponse>;
    
    /// Register a new schema version
    async fn register_schema(&self, subject: &str, schema: &str, references: Vec<SchemaReference>) -> SchemaResult<u32>;
    
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

/// Confluent-specific authentication
#[derive(Debug, Clone)]
pub enum ConfluentAuth {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
    ApiKey { key: String, secret: String },
}

/// AWS credentials for MSK Schema Registry
#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// Schema version for in-memory storage
#[derive(Debug, Clone)]
pub struct SchemaVersion {
    pub id: u32,
    pub version: i32,
    pub schema: String,
    pub references: Vec<SchemaReference>,
    pub created_at: i64,
}

/// Factory for creating schema registry backends
pub struct SchemaRegistryBackendFactory;

impl SchemaRegistryBackendFactory {
    /// Create a backend from configuration
    pub fn create(config: BackendConfig) -> SchemaResult<Arc<dyn SchemaRegistryBackend>> {
        match config {
            BackendConfig::Confluent { base_url, auth, timeout_seconds, max_retries } => {
                Ok(Arc::new(ConfluentSchemaRegistryBackend::new(
                    base_url, auth, timeout_seconds, max_retries
                )?))
            }
            BackendConfig::Pulsar { service_url, auth_token, namespace, timeout_seconds } => {
                Ok(Arc::new(PulsarSchemaRegistryBackend::new(
                    service_url, auth_token, namespace, timeout_seconds
                )?))
            }
            BackendConfig::AmazonMsk { cluster_arn, region, registry_name, credentials } => {
                Ok(Arc::new(AmazonMskSchemaRegistryBackend::new(
                    cluster_arn, region, registry_name, credentials
                )?))
            }
            BackendConfig::Custom { implementation } => {
                Ok(implementation)
            }
            BackendConfig::InMemory { initial_schemas } => {
                Ok(Arc::new(InMemorySchemaRegistryBackend::new(initial_schemas)))
            }
            BackendConfig::FileSystem { base_path, watch_for_changes, auto_create_directories } => {
                Ok(Arc::new(FileSystemSchemaRegistryBackend::new(
                    base_path, watch_for_changes, auto_create_directories
                )?))
            }
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

/// Confluent Schema Registry backend implementation
pub struct ConfluentSchemaRegistryBackend {
    base_url: String,
    auth: ConfluentAuth,
    http_client: reqwest::Client,
    timeout_seconds: u64,
    max_retries: u32,
}

impl ConfluentSchemaRegistryBackend {
    pub fn new(base_url: String, auth: ConfluentAuth, timeout_seconds: u64, max_retries: u32) -> SchemaResult<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_seconds))
            .build()
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to create HTTP client: {}", e),
            })?;

        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            auth,
            http_client,
            timeout_seconds,
            max_retries,
        })
    }
}

#[async_trait]
impl SchemaRegistryBackend for ConfluentSchemaRegistryBackend {
    async fn get_schema(&self, id: u32) -> SchemaResult<SchemaResponse> {
        let url = format!("{}/schemas/ids/{}", self.base_url, id);
        let response = self.execute_request(reqwest::Method::GET, &url, None).await?;
        
        let confluent_response: ConfluentSchemaResponse = response.json().await
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse response: {}", e),
            })?;

        Ok(SchemaResponse {
            id: confluent_response.id,
            subject: confluent_response.subject.unwrap_or_default(),
            version: confluent_response.version.unwrap_or(1),
            schema: confluent_response.schema,
            references: confluent_response.references.unwrap_or_default(),
            metadata: HashMap::new(),
        })
    }

    async fn get_latest_schema(&self, subject: &str) -> SchemaResult<SchemaResponse> {
        let url = format!("{}/subjects/{}/versions/latest", self.base_url, subject);
        let response = self.execute_request(reqwest::Method::GET, &url, None).await?;
        
        let confluent_response: ConfluentVersionResponse = response.json().await
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse response: {}", e),
            })?;

        Ok(SchemaResponse {
            id: confluent_response.id,
            subject: confluent_response.subject,
            version: confluent_response.version,
            schema: confluent_response.schema,
            references: confluent_response.references.unwrap_or_default(),
            metadata: HashMap::new(),
        })
    }

    async fn get_schema_version(&self, subject: &str, version: i32) -> SchemaResult<SchemaResponse> {
        let url = format!("{}/subjects/{}/versions/{}", self.base_url, subject, version);
        let response = self.execute_request(reqwest::Method::GET, &url, None).await?;
        
        let confluent_response: ConfluentVersionResponse = response.json().await
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse response: {}", e),
            })?;

        Ok(SchemaResponse {
            id: confluent_response.id,
            subject: confluent_response.subject,
            version: confluent_response.version,
            schema: confluent_response.schema,
            references: confluent_response.references.unwrap_or_default(),
            metadata: HashMap::new(),
        })
    }

    async fn register_schema(&self, subject: &str, schema: &str, references: Vec<SchemaReference>) -> SchemaResult<u32> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        
        let request_body = ConfluentRegisterRequest {
            schema: schema.to_string(),
            references: if references.is_empty() { None } else { Some(references) },
            schema_type: None,
        };

        let body = serde_json::to_string(&request_body)
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to serialize request: {}", e),
            })?;

        let response = self.execute_request(reqwest::Method::POST, &url, Some(body)).await?;
        
        let result: serde_json::Value = response.json().await
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse registration response: {}", e),
            })?;

        result["id"].as_u64()
            .map(|id| id as u32)
            .ok_or_else(|| SchemaError::Provider {
                source: "confluent".to_string(),
                message: "Invalid schema ID in response".to_string(),
            })
    }

    async fn check_compatibility(&self, subject: &str, schema: &str) -> SchemaResult<bool> {
        let url = format!("{}/compatibility/subjects/{}/versions/latest", self.base_url, subject);
        
        let request_body = serde_json::json!({
            "schema": schema
        });

        let body = request_body.to_string();
        let response = self.execute_request(reqwest::Method::POST, &url, Some(body)).await?;
        
        let result: serde_json::Value = response.json().await
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse compatibility response: {}", e),
            })?;

        Ok(result["is_compatible"].as_bool().unwrap_or(false))
    }

    async fn get_versions(&self, subject: &str) -> SchemaResult<Vec<i32>> {
        let url = format!("{}/subjects/{}/versions", self.base_url, subject);
        let response = self.execute_request(reqwest::Method::GET, &url, None).await?;
        
        let versions: Vec<i32> = response.json().await
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse versions response: {}", e),
            })?;

        Ok(versions)
    }

    async fn get_subjects(&self) -> SchemaResult<Vec<String>> {
        let url = format!("{}/subjects", self.base_url);
        let response = self.execute_request(reqwest::Method::GET, &url, None).await?;
        
        let subjects: Vec<String> = response.json().await
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Failed to parse subjects response: {}", e),
            })?;

        Ok(subjects)
    }

    async fn delete_schema_version(&self, subject: &str, version: i32) -> SchemaResult<()> {
        let url = format!("{}/subjects/{}/versions/{}", self.base_url, subject, version);
        let _response = self.execute_request(reqwest::Method::DELETE, &url, None).await?;
        Ok(())
    }

    fn metadata(&self) -> BackendMetadata {
        BackendMetadata {
            backend_type: "confluent".to_string(),
            version: "7.x".to_string(),
            supported_features: vec![
                "schema_references".to_string(),
                "compatibility_checking".to_string(),
                "schema_evolution".to_string(),
                "subject_management".to_string(),
            ],
            capabilities: BackendCapabilities {
                supports_references: true,
                supports_evolution: true,
                supports_compatibility_check: true,
                supports_deletion: true,
                supports_subjects_listing: true,
                max_schema_size: Some(8 * 1024 * 1024), // 8MB
                max_references_per_schema: Some(1000),
            },
        }
    }

    async fn health_check(&self) -> SchemaResult<HealthStatus> {
        let start = std::time::Instant::now();
        let url = format!("{}/subjects", self.base_url);
        
        match self.execute_request(reqwest::Method::GET, &url, None).await {
            Ok(_) => {
                Ok(HealthStatus {
                    is_healthy: true,
                    message: "Confluent Schema Registry is healthy".to_string(),
                    response_time_ms: start.elapsed().as_millis() as u64,
                    details: HashMap::new(),
                })
            }
            Err(e) => {
                Ok(HealthStatus {
                    is_healthy: false,
                    message: format!("Health check failed: {}", e),
                    response_time_ms: start.elapsed().as_millis() as u64,
                    details: HashMap::new(),
                })
            }
        }
    }
}

impl ConfluentSchemaRegistryBackend {
    async fn execute_request(&self, method: reqwest::Method, url: &str, body: Option<String>) -> SchemaResult<reqwest::Response> {
        let mut request = self.http_client.request(method, url)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json");

        // Add authentication
        request = match &self.auth {
            ConfluentAuth::Basic { username, password } => {
                request.basic_auth(username, Some(password))
            }
            ConfluentAuth::Bearer { token } => {
                request.bearer_auth(token)
            }
            ConfluentAuth::ApiKey { key, secret } => {
                request.basic_auth(key, Some(secret))
            }
            ConfluentAuth::None => request,
        };

        // Add body if provided
        if let Some(body_content) = body {
            request = request.body(body_content);
        }

        let response = request.send().await
            .map_err(|e| SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Request failed: {}", e),
            })?;

        if !response.status().is_success() {
            return Err(SchemaError::Provider {
                source: "confluent".to_string(),
                message: format!("Request failed with status: {}", response.status()),
            });
        }

        Ok(response)
    }
}

/// Confluent API response structures
#[derive(Debug, Deserialize)]
struct ConfluentSchemaResponse {
    id: u32,
    schema: String,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    version: Option<i32>,
    #[serde(default)]
    references: Option<Vec<SchemaReference>>,
}

#[derive(Debug, Deserialize)]
struct ConfluentVersionResponse {
    id: u32,
    subject: String,
    version: i32,
    schema: String,
    #[serde(default)]
    references: Option<Vec<SchemaReference>>,
}

#[derive(Debug, Serialize)]
struct ConfluentRegisterRequest {
    schema: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    references: Option<Vec<SchemaReference>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_type: Option<String>,
}

// Placeholder implementations for other backends
pub struct PulsarSchemaRegistryBackend {
    service_url: String,
    auth_token: Option<String>,
    namespace: String,
    timeout_seconds: u64,
}

impl PulsarSchemaRegistryBackend {
    pub fn new(service_url: String, auth_token: Option<String>, namespace: String, timeout_seconds: u64) -> SchemaResult<Self> {
        Ok(Self {
            service_url,
            auth_token,
            namespace,
            timeout_seconds,
        })
    }
}

#[async_trait]
impl SchemaRegistryBackend for PulsarSchemaRegistryBackend {
    async fn get_schema(&self, _id: u32) -> SchemaResult<SchemaResponse> {
        Err(SchemaError::Provider {
            source: "pulsar".to_string(),
            message: "Pulsar Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn get_latest_schema(&self, _subject: &str) -> SchemaResult<SchemaResponse> {
        Err(SchemaError::Provider {
            source: "pulsar".to_string(),
            message: "Pulsar Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn get_schema_version(&self, _subject: &str, _version: i32) -> SchemaResult<SchemaResponse> {
        Err(SchemaError::Provider {
            source: "pulsar".to_string(),
            message: "Pulsar Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn register_schema(&self, _subject: &str, _schema: &str, _references: Vec<SchemaReference>) -> SchemaResult<u32> {
        Err(SchemaError::Provider {
            source: "pulsar".to_string(),
            message: "Pulsar Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn check_compatibility(&self, _subject: &str, _schema: &str) -> SchemaResult<bool> {
        Err(SchemaError::Provider {
            source: "pulsar".to_string(),
            message: "Pulsar Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn get_versions(&self, _subject: &str) -> SchemaResult<Vec<i32>> {
        Err(SchemaError::Provider {
            source: "pulsar".to_string(),
            message: "Pulsar Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn get_subjects(&self) -> SchemaResult<Vec<String>> {
        Err(SchemaError::Provider {
            source: "pulsar".to_string(),
            message: "Pulsar Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn delete_schema_version(&self, _subject: &str, _version: i32) -> SchemaResult<()> {
        Err(SchemaError::Provider {
            source: "pulsar".to_string(),
            message: "Pulsar Schema Registry implementation not yet complete".to_string(),
        })
    }

    fn metadata(&self) -> BackendMetadata {
        BackendMetadata {
            backend_type: "pulsar".to_string(),
            version: "2.x".to_string(),
            supported_features: vec![
                "basic_schema_storage".to_string(),
            ],
            capabilities: BackendCapabilities {
                supports_references: false,
                supports_evolution: false,
                supports_compatibility_check: false,
                supports_deletion: false,
                supports_subjects_listing: false,
                max_schema_size: Some(1024 * 1024), // 1MB
                max_references_per_schema: Some(0),
            },
        }
    }

    async fn health_check(&self) -> SchemaResult<HealthStatus> {
        Ok(HealthStatus {
            is_healthy: false,
            message: "Pulsar Schema Registry backend not implemented".to_string(),
            response_time_ms: 0,
            details: HashMap::new(),
        })
    }
}

pub struct AmazonMskSchemaRegistryBackend {
    cluster_arn: String,
    region: String,
    registry_name: String,
    credentials: AwsCredentials,
}

impl AmazonMskSchemaRegistryBackend {
    pub fn new(cluster_arn: String, region: String, registry_name: String, credentials: AwsCredentials) -> SchemaResult<Self> {
        Ok(Self {
            cluster_arn,
            region,
            registry_name,
            credentials,
        })
    }
}

#[async_trait]
impl SchemaRegistryBackend for AmazonMskSchemaRegistryBackend {
    async fn get_schema(&self, _id: u32) -> SchemaResult<SchemaResponse> {
        Err(SchemaError::Provider {
            source: "aws_msk".to_string(),
            message: "Amazon MSK Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn get_latest_schema(&self, _subject: &str) -> SchemaResult<SchemaResponse> {
        Err(SchemaError::Provider {
            source: "aws_msk".to_string(),
            message: "Amazon MSK Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn get_schema_version(&self, _subject: &str, _version: i32) -> SchemaResult<SchemaResponse> {
        Err(SchemaError::Provider {
            source: "aws_msk".to_string(),
            message: "Amazon MSK Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn register_schema(&self, _subject: &str, _schema: &str, _references: Vec<SchemaReference>) -> SchemaResult<u32> {
        Err(SchemaError::Provider {
            source: "aws_msk".to_string(),
            message: "Amazon MSK Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn check_compatibility(&self, _subject: &str, _schema: &str) -> SchemaResult<bool> {
        Err(SchemaError::Provider {
            source: "aws_msk".to_string(),
            message: "Amazon MSK Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn get_versions(&self, _subject: &str) -> SchemaResult<Vec<i32>> {
        Err(SchemaError::Provider {
            source: "aws_msk".to_string(),
            message: "Amazon MSK Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn get_subjects(&self) -> SchemaResult<Vec<String>> {
        Err(SchemaError::Provider {
            source: "aws_msk".to_string(),
            message: "Amazon MSK Schema Registry implementation not yet complete".to_string(),
        })
    }

    async fn delete_schema_version(&self, _subject: &str, _version: i32) -> SchemaResult<()> {
        Err(SchemaError::Provider {
            source: "aws_msk".to_string(),
            message: "Amazon MSK Schema Registry implementation not yet complete".to_string(),
        })
    }

    fn metadata(&self) -> BackendMetadata {
        BackendMetadata {
            backend_type: "aws_msk".to_string(),
            version: "1.x".to_string(),
            supported_features: vec![
                "aws_integration".to_string(),
                "iam_auth".to_string(),
            ],
            capabilities: BackendCapabilities {
                supports_references: false,
                supports_evolution: true,
                supports_compatibility_check: true,
                supports_deletion: false,
                supports_subjects_listing: true,
                max_schema_size: Some(2 * 1024 * 1024), // 2MB
                max_references_per_schema: Some(0),
            },
        }
    }

    async fn health_check(&self) -> SchemaResult<HealthStatus> {
        Ok(HealthStatus {
            is_healthy: false,
            message: "Amazon MSK Schema Registry backend not implemented".to_string(),
            response_time_ms: 0,
            details: HashMap::new(),
        })
    }
}

/// In-memory schema registry for testing and development
pub struct InMemorySchemaRegistryBackend {
    schemas: std::sync::RwLock<HashMap<String, Vec<SchemaVersion>>>,
    next_id: std::sync::atomic::AtomicU32,
}

impl InMemorySchemaRegistryBackend {
    pub fn new(initial_schemas: HashMap<String, Vec<SchemaVersion>>) -> Self {
        let max_id = initial_schemas
            .values()
            .flatten()
            .map(|v| v.id)
            .max()
            .unwrap_or(0);

        Self {
            schemas: std::sync::RwLock::new(initial_schemas),
            next_id: std::sync::atomic::AtomicU32::new(max_id + 1),
        }
    }
}

#[async_trait]
impl SchemaRegistryBackend for InMemorySchemaRegistryBackend {
    async fn get_schema(&self, id: u32) -> SchemaResult<SchemaResponse> {
        let schemas = self.schemas.read().unwrap();
        
        for versions in schemas.values() {
            if let Some(version) = versions.iter().find(|v| v.id == id) {
                return Ok(SchemaResponse {
                    id: version.id,
                    subject: "unknown".to_string(), // Would need reverse lookup
                    version: version.version,
                    schema: version.schema.clone(),
                    references: version.references.clone(),
                    metadata: HashMap::new(),
                });
            }
        }
        
        Err(SchemaError::NotFound {
            source: format!("schema_id:{}", id),
        })
    }

    async fn get_latest_schema(&self, subject: &str) -> SchemaResult<SchemaResponse> {
        let schemas = self.schemas.read().unwrap();
        
        if let Some(versions) = schemas.get(subject) {
            if let Some(latest) = versions.iter().max_by_key(|v| v.version) {
                return Ok(SchemaResponse {
                    id: latest.id,
                    subject: subject.to_string(),
                    version: latest.version,
                    schema: latest.schema.clone(),
                    references: latest.references.clone(),
                    metadata: HashMap::new(),
                });
            }
        }
        
        Err(SchemaError::NotFound {
            source: subject.to_string(),
        })
    }

    async fn get_schema_version(&self, subject: &str, version: i32) -> SchemaResult<SchemaResponse> {
        let schemas = self.schemas.read().unwrap();
        
        if let Some(versions) = schemas.get(subject) {
            if let Some(schema_version) = versions.iter().find(|v| v.version == version) {
                return Ok(SchemaResponse {
                    id: schema_version.id,
                    subject: subject.to_string(),
                    version: schema_version.version,
                    schema: schema_version.schema.clone(),
                    references: schema_version.references.clone(),
                    metadata: HashMap::new(),
                });
            }
        }
        
        Err(SchemaError::NotFound {
            source: format!("{}:{}", subject, version),
        })
    }

    async fn register_schema(&self, subject: &str, schema: &str, references: Vec<SchemaReference>) -> SchemaResult<u32> {
        let mut schemas = self.schemas.write().unwrap();
        let id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        let versions = schemas.entry(subject.to_string()).or_insert_with(Vec::new);
        let version = versions.len() as i32 + 1;
        
        versions.push(SchemaVersion {
            id,
            version,
            schema: schema.to_string(),
            references,
            created_at: chrono::Utc::now().timestamp(),
        });
        
        Ok(id)
    }

    async fn check_compatibility(&self, _subject: &str, _schema: &str) -> SchemaResult<bool> {
        // Simplified: always compatible in memory backend
        Ok(true)
    }

    async fn get_versions(&self, subject: &str) -> SchemaResult<Vec<i32>> {
        let schemas = self.schemas.read().unwrap();
        
        if let Some(versions) = schemas.get(subject) {
            Ok(versions.iter().map(|v| v.version).collect())
        } else {
            Ok(vec![])
        }
    }

    async fn get_subjects(&self) -> SchemaResult<Vec<String>> {
        let schemas = self.schemas.read().unwrap();
        Ok(schemas.keys().cloned().collect())
    }

    async fn delete_schema_version(&self, subject: &str, version: i32) -> SchemaResult<()> {
        let mut schemas = self.schemas.write().unwrap();
        
        if let Some(versions) = schemas.get_mut(subject) {
            versions.retain(|v| v.version != version);
        }
        
        Ok(())
    }

    fn metadata(&self) -> BackendMetadata {
        BackendMetadata {
            backend_type: "in_memory".to_string(),
            version: "1.0.0".to_string(),
            supported_features: vec![
                "testing".to_string(),
                "development".to_string(),
            ],
            capabilities: BackendCapabilities {
                supports_references: true,
                supports_evolution: false,
                supports_compatibility_check: false,
                supports_deletion: true,
                supports_subjects_listing: true,
                max_schema_size: None,
                max_references_per_schema: None,
            },
        }
    }

    async fn health_check(&self) -> SchemaResult<HealthStatus> {
        Ok(HealthStatus {
            is_healthy: true,
            message: "In-memory registry is always healthy".to_string(),
            response_time_ms: 0,
            details: HashMap::new(),
        })
    }
}

/// FileSystem-based Schema Registry Backend
/// 
/// Stores schemas as JSON files in a directory structure:
/// - `<base_path>/subjects/<subject>/versions/<version>.json`
/// - `<base_path>/schemas/<id>.json`
/// - `<base_path>/metadata.json`
pub struct FileSystemSchemaRegistryBackend {
    base_path: std::path::PathBuf,
    watch_for_changes: bool,
    auto_create_directories: bool,
    next_id: Arc<std::sync::atomic::AtomicU32>,
    metadata_cache: Arc<RwLock<HashMap<String, FileMetadata>>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FileMetadata {
    subject: String,
    version: i32,
    schema_id: u32,
    created_at: u64,
    updated_at: u64,
    size_bytes: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FileSchemaEntry {
    id: u32,
    subject: String,
    version: i32,
    schema: String,
    references: Vec<SchemaReference>,
    created_at: u64,
    updated_at: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RegistryMetadata {
    next_id: u32,
    created_at: u64,
    last_updated: u64,
    total_schemas: u64,
    subjects: Vec<String>,
}

impl FileSystemSchemaRegistryBackend {
    pub fn new(base_path: std::path::PathBuf, watch_for_changes: bool, auto_create_directories: bool) -> SchemaResult<Self> {
        let backend = Self {
            base_path,
            watch_for_changes,
            auto_create_directories,
            next_id: Arc::new(std::sync::atomic::AtomicU32::new(1)),
            metadata_cache: Arc::new(RwLock::new(HashMap::new())),
        };
        
        backend.initialize()?;
        Ok(backend)
    }
    
    fn initialize(&self) -> SchemaResult<()> {
        if self.auto_create_directories {
            std::fs::create_dir_all(&self.base_path).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to create base directory: {}", e),
            })?;
            
            std::fs::create_dir_all(self.base_path.join("subjects")).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to create subjects directory: {}", e),
            })?;
            
            std::fs::create_dir_all(self.base_path.join("schemas")).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to create schemas directory: {}", e),
            })?;
        }
        
        // Load existing metadata
        self.load_metadata()?;
        
        Ok(())
    }
    
    fn load_metadata(&self) -> SchemaResult<()> {
        let metadata_path = self.base_path.join("metadata.json");
        
        if metadata_path.exists() {
            let content = std::fs::read_to_string(&metadata_path).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to read metadata file: {}", e),
            })?;
            
            let metadata: RegistryMetadata = serde_json::from_str(&content).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to parse metadata: {}", e),
            })?;
            
            self.next_id.store(metadata.next_id, std::sync::atomic::Ordering::SeqCst);
        } else {
            // Create initial metadata
            self.save_metadata()?;
        }
        
        Ok(())
    }
    
    fn save_metadata(&self) -> SchemaResult<()> {
        let metadata_path = self.base_path.join("metadata.json");
        
        let metadata = RegistryMetadata {
            next_id: self.next_id.load(std::sync::atomic::Ordering::SeqCst),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            total_schemas: self.count_total_schemas(),
            subjects: self.list_subjects_from_filesystem().unwrap_or_default(),
        };
        
        let content = serde_json::to_string_pretty(&metadata).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to serialize metadata: {}", e),
        })?;
        
        // Atomic write using temporary file
        let temp_path = metadata_path.with_extension("json.tmp");
        std::fs::write(&temp_path, content).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to write temporary metadata: {}", e),
        })?;
        
        std::fs::rename(&temp_path, &metadata_path).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to commit metadata: {}", e),
        })?;
        
        Ok(())
    }
    
    fn count_total_schemas(&self) -> u64 {
        let schemas_dir = self.base_path.join("schemas");
        if !schemas_dir.exists() {
            return 0;
        }
        
        std::fs::read_dir(schemas_dir)
            .map(|entries| entries.filter_map(|e| e.ok()).count() as u64)
            .unwrap_or(0)
    }
    
    fn list_subjects_from_filesystem(&self) -> SchemaResult<Vec<String>> {
        let subjects_dir = self.base_path.join("subjects");
        if !subjects_dir.exists() {
            return Ok(vec![]);
        }
        
        let entries = std::fs::read_dir(subjects_dir).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to read subjects directory: {}", e),
        })?;
        
        let mut subjects = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to read directory entry: {}", e),
            })?;
            
            if entry.file_type().map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to get file type: {}", e),
            })?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    subjects.push(name.to_string());
                }
            }
        }
        
        subjects.sort();
        Ok(subjects)
    }
    
    fn get_schema_file_path(&self, id: u32) -> std::path::PathBuf {
        self.base_path.join("schemas").join(format!("{}.json", id))
    }
    
    fn get_subject_version_path(&self, subject: &str, version: i32) -> std::path::PathBuf {
        self.base_path
            .join("subjects")
            .join(subject)
            .join("versions")
            .join(format!("{}.json", version))
    }
    
    fn get_subject_latest_path(&self, subject: &str) -> std::path::PathBuf {
        self.base_path
            .join("subjects")
            .join(subject)
            .join("latest.json")
    }
    
    fn load_schema_entry(&self, path: &std::path::Path) -> SchemaResult<FileSchemaEntry> {
        if !path.exists() {
            return Err(SchemaError::NotFound {
                source: format!("Schema file not found: {}", path.to_string_lossy()),
            });
        }
        
        let content = std::fs::read_to_string(path).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to read schema file: {}", e),
        })?;
        
        serde_json::from_str(&content).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to parse schema file: {}", e),
        })
    }
    
    fn save_schema_entry(&self, path: &std::path::Path, entry: &FileSchemaEntry) -> SchemaResult<()> {
        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to create directory: {}", e),
            })?;
        }
        
        let content = serde_json::to_string_pretty(entry).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to serialize schema: {}", e),
        })?;
        
        // Atomic write using temporary file
        let temp_path = path.with_extension("json.tmp");
        std::fs::write(&temp_path, content).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to write temporary schema file: {}", e),
        })?;
        
        std::fs::rename(&temp_path, path).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to commit schema file: {}", e),
        })?;
        
        Ok(())
    }
    
    fn get_latest_version_for_subject(&self, subject: &str) -> SchemaResult<i32> {
        let subject_dir = self.base_path.join("subjects").join(subject).join("versions");
        
        if !subject_dir.exists() {
            return Err(SchemaError::NotFound {
                source: format!("Subject not found: {}", subject),
            });
        }
        
        let entries = std::fs::read_dir(subject_dir).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to read versions directory: {}", e),
        })?;
        
        let mut max_version = 0;
        for entry in entries {
            let entry = entry.map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to read directory entry: {}", e),
            })?;
            
            if let Some(name) = entry.file_name().to_str() {
                if let Some(version_str) = name.strip_suffix(".json") {
                    if let Ok(version) = version_str.parse::<i32>() {
                        max_version = max_version.max(version);
                    }
                }
            }
        }
        
        if max_version == 0 {
            return Err(SchemaError::NotFound {
                source: format!("No versions found for subject: {}", subject),
            });
        }
        
        Ok(max_version)
    }
    
    fn allocate_schema_id(&self) -> u32 {
        self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl SchemaRegistryBackend for FileSystemSchemaRegistryBackend {
    async fn get_schema(&self, id: u32) -> SchemaResult<SchemaResponse> {
        let schema_path = self.get_schema_file_path(id);
        let entry = self.load_schema_entry(&schema_path)?;
        
        Ok(SchemaResponse {
            id: entry.id,
            subject: entry.subject,
            version: entry.version,
            schema: entry.schema,
            references: entry.references,
            metadata: HashMap::new(),
        })
    }

    async fn get_latest_schema(&self, subject: &str) -> SchemaResult<SchemaResponse> {
        let latest_version = self.get_latest_version_for_subject(subject)?;
        let version_path = self.get_subject_version_path(subject, latest_version);
        let entry = self.load_schema_entry(&version_path)?;
        
        Ok(SchemaResponse {
            id: entry.id,
            subject: entry.subject,
            version: entry.version,
            schema: entry.schema,
            references: entry.references,
            metadata: HashMap::new(),
        })
    }

    async fn get_schema_version(&self, subject: &str, version: i32) -> SchemaResult<SchemaResponse> {
        let version_path = self.get_subject_version_path(subject, version);
        let entry = self.load_schema_entry(&version_path)?;
        
        Ok(SchemaResponse {
            id: entry.id,
            subject: entry.subject,
            version: entry.version,
            schema: entry.schema,
            references: entry.references,
            metadata: HashMap::new(),
        })
    }

    async fn register_schema(&self, subject: &str, schema: &str, references: Vec<SchemaReference>) -> SchemaResult<u32> {
        // Check if schema already exists for this subject
        let subject_dir = self.base_path.join("subjects").join(subject).join("versions");
        let latest_version = self.get_latest_version_for_subject(subject)
            .unwrap_or(0); // If no versions exist, start at 0
        
        let new_version = latest_version + 1;
        let schema_id = self.allocate_schema_id();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let entry = FileSchemaEntry {
            id: schema_id,
            subject: subject.to_string(),
            version: new_version,
            schema: schema.to_string(),
            references,
            created_at: now,
            updated_at: now,
        };
        
        // Save by schema ID
        let schema_path = self.get_schema_file_path(schema_id);
        self.save_schema_entry(&schema_path, &entry)?;
        
        // Save by subject/version
        let version_path = self.get_subject_version_path(subject, new_version);
        self.save_schema_entry(&version_path, &entry)?;
        
        // Update latest link
        let latest_path = self.get_subject_latest_path(subject);
        self.save_schema_entry(&latest_path, &entry)?;
        
        // Update metadata
        self.save_metadata()?;
        
        Ok(schema_id)
    }

    async fn check_compatibility(&self, subject: &str, _schema: &str) -> SchemaResult<bool> {
        // Basic compatibility check - ensure subject exists
        let _latest_version = self.get_latest_version_for_subject(subject)?;
        Ok(true) // FileSystem backend always reports compatible
    }

    async fn get_versions(&self, subject: &str) -> SchemaResult<Vec<i32>> {
        let subject_dir = self.base_path.join("subjects").join(subject).join("versions");
        
        if !subject_dir.exists() {
            return Ok(vec![]);
        }
        
        let entries = std::fs::read_dir(subject_dir).map_err(|e| SchemaError::Provider {
            source: "filesystem".to_string(),
            message: format!("Failed to read versions directory: {}", e),
        })?;
        
        let mut versions = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to read directory entry: {}", e),
            })?;
            
            if let Some(name) = entry.file_name().to_str() {
                if let Some(version_str) = name.strip_suffix(".json") {
                    if let Ok(version) = version_str.parse::<i32>() {
                        versions.push(version);
                    }
                }
            }
        }
        
        versions.sort();
        Ok(versions)
    }

    async fn get_subjects(&self) -> SchemaResult<Vec<String>> {
        self.list_subjects_from_filesystem()
    }

    async fn delete_schema_version(&self, subject: &str, version: i32) -> SchemaResult<()> {
        let version_path = self.get_subject_version_path(subject, version);
        
        if version_path.exists() {
            // Load the entry to get the schema ID
            let entry = self.load_schema_entry(&version_path)?;
            
            // Remove version file
            std::fs::remove_file(&version_path).map_err(|e| SchemaError::Provider {
                source: "filesystem".to_string(),
                message: format!("Failed to delete version file: {}", e),
            })?;
            
            // Remove schema file
            let schema_path = self.get_schema_file_path(entry.id);
            if schema_path.exists() {
                std::fs::remove_file(&schema_path).map_err(|e| SchemaError::Provider {
                    source: "filesystem".to_string(),
                    message: format!("Failed to delete schema file: {}", e),
                })?;
            }
            
            // Update latest link if this was the latest version
            if version == self.get_latest_version_for_subject(subject).unwrap_or(0) {
                let versions = self.get_versions(subject).await?;
                if let Some(&new_latest) = versions.iter().filter(|&&v| v != version).max() {
                    let new_latest_entry = self.load_schema_entry(&self.get_subject_version_path(subject, new_latest))?;
                    let latest_path = self.get_subject_latest_path(subject);
                    self.save_schema_entry(&latest_path, &new_latest_entry)?;
                } else {
                    // No versions left, remove latest link
                    let latest_path = self.get_subject_latest_path(subject);
                    if latest_path.exists() {
                        std::fs::remove_file(&latest_path).map_err(|e| SchemaError::Provider {
                            source: "filesystem".to_string(),
                            message: format!("Failed to remove latest link: {}", e),
                        })?;
                    }
                }
            }
            
            // Update metadata
            self.save_metadata()?;
        }
        
        Ok(())
    }

    fn metadata(&self) -> BackendMetadata {
        BackendMetadata {
            backend_type: "filesystem".to_string(),
            version: "1.0.0".to_string(),
            supported_features: vec![
                "local_storage".to_string(),
                "atomic_writes".to_string(),
                "version_management".to_string(),
                "subject_organization".to_string(),
            ],
            capabilities: BackendCapabilities {
                supports_references: true,
                supports_evolution: true,
                supports_compatibility_check: true,
                supports_deletion: true,
                supports_subjects_listing: true,
                max_schema_size: Some(10 * 1024 * 1024), // 10MB
                max_references_per_schema: Some(100),
            },
        }
    }

    async fn health_check(&self) -> SchemaResult<HealthStatus> {
        let mut details = HashMap::new();
        let mut is_healthy = true;
        let mut message = "FileSystem registry is healthy".to_string();
        
        // Check if base path exists and is writable
        if !self.base_path.exists() {
            is_healthy = false;
            message = "Base path does not exist".to_string();
            details.insert("base_path".to_string(), self.base_path.to_string_lossy().to_string());
        } else {
            // Try to write a test file
            let test_file = self.base_path.join(".health_check");
            match std::fs::write(&test_file, "test") {
                Ok(_) => {
                    let _ = std::fs::remove_file(&test_file);
                    details.insert("writable".to_string(), "true".to_string());
                },
                Err(_) => {
                    is_healthy = false;
                    message = "Base path is not writable".to_string();
                    details.insert("writable".to_string(), "false".to_string());
                }
            }
        }
        
        details.insert("total_schemas".to_string(), self.count_total_schemas().to_string());
        details.insert("next_id".to_string(), self.next_id.load(std::sync::atomic::Ordering::SeqCst).to_string());
        
        Ok(HealthStatus {
            is_healthy,
            message,
            response_time_ms: 1, // FileSystem operations are fast
            details,
        })
    }
}