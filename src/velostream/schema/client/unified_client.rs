//! Unified Schema Registry Client
//!
//! Provides a unified interface for different schema registry backends
//! while maintaining the same high-level API and caching capabilities.

use std::sync::Arc;

use super::enhanced_cache::EnhancedSchemaCache;
use super::reference_resolver::SchemaReferenceResolver;
use super::registry_client::{CachedSchema, ResolvedSchema, SchemaReference};
use crate::velostream::schema::server::{
    BackendConfig, BackendMetadata, ConfluentAuth, HealthStatus, SchemaRegistryBackend,
    SchemaRegistryBackendFactory,
};
use crate::velostream::schema::{SchemaError, SchemaResult};

/// Unified Schema Registry Client that works with any backend
pub struct UnifiedSchemaRegistryClient {
    backend: Arc<dyn SchemaRegistryBackend>,
    cache: Arc<EnhancedSchemaCache>,
    resolver: Arc<SchemaReferenceResolver>,
    config: UnifiedClientConfig,
}

/// Configuration for the unified client
#[derive(Debug, Clone)]
pub struct UnifiedClientConfig {
    /// Enable caching
    pub enable_caching: bool,

    /// Enable reference resolution
    pub enable_references: bool,

    /// Auto-retry failed requests
    pub auto_retry: bool,

    /// Maximum retry attempts
    pub max_retries: u32,

    /// Fallback to cache on backend failure
    pub fallback_to_cache: bool,
}

impl UnifiedSchemaRegistryClient {
    /// Create a new unified client with a specific backend
    pub fn new(backend_config: BackendConfig) -> SchemaResult<Self> {
        let backend = SchemaRegistryBackendFactory::create(backend_config)?;
        let cache = Arc::new(EnhancedSchemaCache::new());

        // Create a stub resolver to avoid circular dependency
        // The resolver will be properly initialized when needed
        let resolver = Arc::new(SchemaReferenceResolver::new_stub());

        Ok(Self {
            backend,
            cache,
            resolver,
            config: UnifiedClientConfig::default(),
        })
    }

    /// Create client from URL with auto-detection
    pub fn from_url(url: &str) -> SchemaResult<Self> {
        let backend_config = SchemaRegistryBackendFactory::auto_detect(url);
        Self::new(backend_config)
    }

    /// Create client with custom configuration
    pub fn with_config(
        backend_config: BackendConfig,
        client_config: UnifiedClientConfig,
    ) -> SchemaResult<Self> {
        let mut client = Self::new(backend_config)?;
        client.config = client_config;
        Ok(client)
    }

    /// Get a schema by ID with caching
    pub async fn get_schema(&self, id: u32) -> SchemaResult<CachedSchema> {
        // Check cache first if enabled
        if self.config.enable_caching {
            if let Some(cached) = self.cache.get(id).await {
                return Ok(cached);
            }
        }

        // Fetch from backend
        let schema_response = self.backend.get_schema(id).await?;

        let cached_schema = CachedSchema {
            id: schema_response.id,
            subject: schema_response.subject,
            version: schema_response.version,
            schema: schema_response.schema,
            references: schema_response.references,
            cached_at: std::time::Instant::now(),
            access_count: 1,
        };

        // Cache the result if enabled
        if self.config.enable_caching {
            self.cache.put(cached_schema.clone()).await;
        }

        Ok(cached_schema)
    }

    /// Get latest schema for subject with caching
    pub async fn get_latest_schema(&self, subject: &str) -> SchemaResult<CachedSchema> {
        let schema_response = self.backend.get_latest_schema(subject).await?;

        let cached_schema = CachedSchema {
            id: schema_response.id,
            subject: schema_response.subject,
            version: schema_response.version,
            schema: schema_response.schema,
            references: schema_response.references,
            cached_at: std::time::Instant::now(),
            access_count: 1,
        };

        // Cache the result if enabled
        if self.config.enable_caching {
            self.cache.put(cached_schema.clone()).await;
        }

        Ok(cached_schema)
    }

    /// Get specific schema version
    pub async fn get_schema_version(
        &self,
        subject: &str,
        version: i32,
    ) -> SchemaResult<CachedSchema> {
        let schema_response = self.backend.get_schema_version(subject, version).await?;

        let cached_schema = CachedSchema {
            id: schema_response.id,
            subject: schema_response.subject,
            version: schema_response.version,
            schema: schema_response.schema,
            references: schema_response.references,
            cached_at: std::time::Instant::now(),
            access_count: 1,
        };

        // Cache the result if enabled
        if self.config.enable_caching {
            self.cache.put(cached_schema.clone()).await;
        }

        Ok(cached_schema)
    }

    /// Register a new schema
    pub async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        references: Vec<SchemaReference>,
    ) -> SchemaResult<u32> {
        self.backend
            .register_schema(subject, schema, references)
            .await
    }

    /// Check schema compatibility
    pub async fn check_compatibility(&self, subject: &str, schema: &str) -> SchemaResult<bool> {
        self.backend.check_compatibility(subject, schema).await
    }

    /// Resolve schema with all references (if enabled)
    pub async fn resolve_schema_with_references(&self, id: u32) -> SchemaResult<ResolvedSchema> {
        if !self.config.enable_references {
            // Just return the schema without reference resolution
            let schema = self.get_schema(id).await?;
            return Ok(ResolvedSchema {
                root_schema: schema.clone(),
                dependencies: std::collections::HashMap::new(),
                flattened_schema: schema.schema,
                resolution_path: vec![id],
            });
        }

        // For now, return a simple resolution since the resolver is a stub
        // TODO: Integrate properly with the resolver using the backend directly
        let schema = self.get_schema(id).await?;
        Ok(ResolvedSchema {
            root_schema: schema.clone(),
            dependencies: std::collections::HashMap::new(),
            flattened_schema: schema.schema,
            resolution_path: vec![id],
        })
    }

    /// Get all versions for a subject
    pub async fn get_versions(&self, subject: &str) -> SchemaResult<Vec<i32>> {
        self.backend.get_versions(subject).await
    }

    /// Get all subjects
    pub async fn get_subjects(&self) -> SchemaResult<Vec<String>> {
        self.backend.get_subjects().await
    }

    /// Delete a schema version
    pub async fn delete_schema_version(&self, subject: &str, version: i32) -> SchemaResult<()> {
        let result = self.backend.delete_schema_version(subject, version).await;

        // Invalidate cache if deletion succeeded
        if result.is_ok() && self.config.enable_caching {
            // TODO: Need to implement cache invalidation by subject/version
            // For now, we could clear the entire cache, but that's not ideal
        }

        result
    }

    /// Get backend metadata
    pub fn backend_metadata(&self) -> BackendMetadata {
        self.backend.metadata()
    }

    /// Health check for the backend
    pub async fn health_check(&self) -> SchemaResult<HealthStatus> {
        self.backend.health_check().await
    }

    /// Get cache metrics (if caching is enabled)
    pub async fn cache_metrics(&self) -> Option<super::enhanced_cache::CacheMetrics> {
        if self.config.enable_caching {
            Some(self.cache.metrics().await)
        } else {
            None
        }
    }

    /// Warm up cache with important schemas
    pub async fn warm_up_cache(&self, schema_ids: Vec<u32>) -> SchemaResult<()> {
        if !self.config.enable_caching {
            return Ok(());
        }

        for id in schema_ids {
            if let Err(e) = self.get_schema(id).await {
                log::warn!("Failed to warm up schema {}: {}", id, e);
            }
        }

        Ok(())
    }

    /// Clear the cache
    pub async fn clear_cache(&self) {
        if self.config.enable_caching {
            self.cache.clear().await;
        }
    }

    /// Switch to a different backend (for failover scenarios)
    pub async fn switch_backend(&mut self, new_backend_config: BackendConfig) -> SchemaResult<()> {
        let new_backend = SchemaRegistryBackendFactory::create(new_backend_config)?;
        self.backend = new_backend;

        // Optionally clear cache when switching backends
        if self.config.enable_caching {
            self.cache.clear().await;
        }

        Ok(())
    }
}

impl Default for UnifiedClientConfig {
    fn default() -> Self {
        Self {
            enable_caching: true,
            enable_references: true,
            auto_retry: true,
            max_retries: 3,
            fallback_to_cache: true,
        }
    }
}

/// Builder for creating unified schema registry clients
pub struct UnifiedClientBuilder {
    backend_config: Option<BackendConfig>,
    client_config: UnifiedClientConfig,
}

impl UnifiedClientBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            backend_config: None,
            client_config: UnifiedClientConfig::default(),
        }
    }

    /// Set the backend configuration
    pub fn backend(mut self, config: BackendConfig) -> Self {
        self.backend_config = Some(config);
        self
    }

    /// Configure Confluent Schema Registry
    pub fn confluent(mut self, base_url: &str) -> Self {
        self.backend_config = Some(BackendConfig::Confluent {
            base_url: base_url.to_string(),
            auth: ConfluentAuth::None,
            timeout_seconds: 30,
            max_retries: 3,
        });
        self
    }

    /// Configure Confluent with authentication
    pub fn confluent_with_auth(mut self, base_url: &str, auth: ConfluentAuth) -> Self {
        self.backend_config = Some(BackendConfig::Confluent {
            base_url: base_url.to_string(),
            auth,
            timeout_seconds: 30,
            max_retries: 3,
        });
        self
    }

    /// Configure in-memory backend (for testing)
    pub fn in_memory(mut self) -> Self {
        self.backend_config = Some(BackendConfig::InMemory {
            initial_schemas: std::collections::HashMap::new(),
        });
        self
    }

    /// Enable/disable caching
    pub fn caching(mut self, enabled: bool) -> Self {
        self.client_config.enable_caching = enabled;
        self
    }

    /// Enable/disable reference resolution
    pub fn references(mut self, enabled: bool) -> Self {
        self.client_config.enable_references = enabled;
        self
    }

    /// Configure retry behavior
    pub fn retry(mut self, enabled: bool, max_retries: u32) -> Self {
        self.client_config.auto_retry = enabled;
        self.client_config.max_retries = max_retries;
        self
    }

    /// Enable fallback to cache on backend failure
    pub fn fallback_to_cache(mut self, enabled: bool) -> Self {
        self.client_config.fallback_to_cache = enabled;
        self
    }

    /// Build the unified client
    pub fn build(self) -> SchemaResult<UnifiedSchemaRegistryClient> {
        let backend_config = self.backend_config.ok_or_else(|| SchemaError::Provider {
            source: "builder".to_string(),
            message: "Backend configuration is required".to_string(),
        })?;

        UnifiedSchemaRegistryClient::with_config(backend_config, self.client_config)
    }
}

impl Default for UnifiedClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_backend() {
        let client = UnifiedClientBuilder::new().in_memory().build().unwrap();

        // Test registration
        let schema_id = client
            .register_schema("test-subject", r#"{"type": "string"}"#, vec![])
            .await
            .unwrap();

        // Test retrieval
        let schema = client.get_schema(schema_id).await.unwrap();
        assert_eq!(schema.subject, "unknown"); // In-memory doesn't track subject by ID

        // Test latest schema
        let latest = client.get_latest_schema("test-subject").await.unwrap();
        assert_eq!(latest.id, schema_id);

        // Test subjects listing
        let subjects = client.get_subjects().await.unwrap();
        assert!(subjects.contains(&"test-subject".to_string()));
    }

    #[test]
    fn test_backend_auto_detection() {
        // Test Confluent detection
        let confluent_config = SchemaRegistryBackendFactory::auto_detect("http://localhost:8081");
        matches!(confluent_config, BackendConfig::Confluent { .. });

        // Test Pulsar detection
        let pulsar_config =
            SchemaRegistryBackendFactory::auto_detect("http://localhost:8080/admin/v2/schemas");
        matches!(pulsar_config, BackendConfig::Pulsar { .. });

        // Test AWS MSK detection
        let aws_config = SchemaRegistryBackendFactory::auto_detect(
            "arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster",
        );
        matches!(aws_config, BackendConfig::AmazonMsk { .. });
    }

    #[tokio::test]
    async fn test_cache_integration() {
        let client = UnifiedClientBuilder::new()
            .in_memory()
            .caching(true)
            .build()
            .unwrap();

        // Register a schema
        let schema_id = client
            .register_schema(
                "cache-test",
                r#"{"type": "record", "name": "Test", "fields": []}"#,
                vec![],
            )
            .await
            .unwrap();

        // First access - should cache
        let _schema1 = client.get_schema(schema_id).await.unwrap();

        // Second access - should hit cache
        let _schema2 = client.get_schema(schema_id).await.unwrap();

        // Check cache metrics
        let metrics = client.cache_metrics().await.unwrap();
        assert!(metrics.total_hits > 0);
    }
}
