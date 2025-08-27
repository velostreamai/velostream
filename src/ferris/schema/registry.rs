//! Schema Registry
//!
//! Central registry for managing schemas across heterogeneous data sources.
//! Supports automatic schema discovery, caching, and provider management.

use crate::ferris::schema::{Schema, SchemaError, SchemaResult};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Central schema registry for managing schemas from multiple data sources
pub struct SchemaRegistry {
    /// Registered schema providers by scheme
    providers: HashMap<String, Arc<dyn SchemaProvider>>,
    /// Cached schemas by source URI
    schemas: Arc<RwLock<HashMap<String, CachedSchema>>>,
    /// Registry configuration
    config: RegistryConfig,
}

/// Configuration for the schema registry
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// Default TTL for cached schemas in seconds
    pub cache_ttl_seconds: u64,
    /// Maximum number of schemas to cache
    pub max_cache_size: usize,
    /// Whether to auto-discover schemas on first access
    pub auto_discovery: bool,
    /// Whether to enable schema validation
    pub validation_enabled: bool,
}

/// Cached schema entry with TTL and metadata
#[derive(Debug, Clone)]
struct CachedSchema {
    schema: Schema,
    cached_at: i64,
    ttl_seconds: u64,
    access_count: u64,
}

/// Trait for implementing schema discovery from different data sources
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Discover schema from a data source URI
    async fn discover_schema(&self, source_uri: &str) -> SchemaResult<Schema>;

    /// Check if this provider supports the given URI scheme
    fn supports_scheme(&self, scheme: &str) -> bool;

    /// Get provider metadata
    fn metadata(&self) -> ProviderMetadata;
}

/// Metadata about a schema provider
#[derive(Debug, Clone)]
pub struct ProviderMetadata {
    pub name: String,
    pub version: String,
    pub supported_schemes: Vec<String>,
    pub capabilities: Vec<String>,
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaRegistry {
    /// Create a new schema registry with default configuration
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
            schemas: Arc::new(RwLock::new(HashMap::new())),
            config: RegistryConfig::default(),
        }
    }

    /// Create a new schema registry with custom configuration
    pub fn with_config(config: RegistryConfig) -> Self {
        Self {
            providers: HashMap::new(),
            schemas: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Register a schema provider for a specific URI scheme
    pub fn register_provider(&mut self, scheme: &str, provider: Arc<dyn SchemaProvider>) {
        self.providers.insert(scheme.to_string(), provider);
    }

    /// Discover schema from a data source URI
    pub async fn discover(&self, source_uri: &str) -> SchemaResult<Schema> {
        // Check cache first
        if let Some(cached) = self.get_cached_schema(source_uri).await {
            return Ok(cached.schema);
        }

        // Extract scheme from URI
        let scheme = self.extract_scheme(source_uri)?;

        // Find appropriate provider
        let provider = self
            .providers
            .get(&scheme)
            .ok_or_else(|| SchemaError::Provider {
                source: source_uri.to_string(),
                message: format!("No provider registered for scheme: {}", scheme),
            })?;

        // Discover schema using provider
        let schema = provider.discover_schema(source_uri).await?;

        // Cache the discovered schema
        self.cache_schema(source_uri, schema.clone()).await;

        Ok(schema)
    }

    /// Get schema from cache or discover if not cached
    pub async fn get_schema(&self, source_uri: &str) -> SchemaResult<Schema> {
        if self.config.auto_discovery {
            self.discover(source_uri).await
        } else {
            self.get_cached_schema(source_uri)
                .await
                .map(|cached| cached.schema)
                .ok_or_else(|| SchemaError::NotFound {
                    source: source_uri.to_string(),
                })
        }
    }

    /// Manually cache a schema for a source URI
    pub async fn cache_schema(&self, source_uri: &str, schema: Schema) {
        let cached = CachedSchema {
            schema,
            cached_at: chrono::Utc::now().timestamp(),
            ttl_seconds: self.config.cache_ttl_seconds,
            access_count: 0,
        };

        let mut cache = self.schemas.write().await;

        // Evict expired entries if cache is full
        if cache.len() >= self.config.max_cache_size {
            self.evict_expired_entries(&mut cache).await;
        }

        cache.insert(source_uri.to_string(), cached);
    }

    /// Remove schema from cache
    pub async fn invalidate_cache(&self, source_uri: &str) {
        let mut cache = self.schemas.write().await;
        cache.remove(source_uri);
    }

    /// Clear all cached schemas
    pub async fn clear_cache(&self) {
        let mut cache = self.schemas.write().await;
        cache.clear();
    }

    /// Get cache statistics
    pub async fn cache_stats(&self) -> CacheStats {
        let cache = self.schemas.read().await;
        let total_entries = cache.len();
        let mut expired_entries = 0;
        let mut total_access_count = 0;

        let now = chrono::Utc::now().timestamp();
        for cached in cache.values() {
            if now - cached.cached_at > cached.ttl_seconds as i64 {
                expired_entries += 1;
            }
            total_access_count += cached.access_count;
        }

        CacheStats {
            total_entries,
            expired_entries,
            active_entries: total_entries - expired_entries,
            total_access_count,
            hit_rate: if total_access_count > 0 {
                (total_access_count as f64) / (total_entries as f64)
            } else {
                0.0
            },
        }
    }

    /// List all registered providers
    pub fn list_providers(&self) -> Vec<(String, ProviderMetadata)> {
        self.providers
            .iter()
            .map(|(scheme, provider)| (scheme.clone(), provider.metadata()))
            .collect()
    }

    /// Validate a schema against registry rules
    pub fn validate_schema(&self, schema: &Schema) -> SchemaResult<()> {
        if !self.config.validation_enabled {
            return Ok(());
        }

        // Basic validation rules
        if schema.fields.is_empty() {
            return Err(SchemaError::Validation {
                message: "Schema must have at least one field".to_string(),
            });
        }

        // Check for duplicate field names
        let mut field_names = std::collections::HashSet::new();
        for field in &schema.fields {
            if !field_names.insert(&field.name) {
                return Err(SchemaError::Validation {
                    message: format!("Duplicate field name: {}", field.name),
                });
            }
        }

        Ok(())
    }

    // Private helper methods

    async fn get_cached_schema(&self, source_uri: &str) -> Option<CachedSchema> {
        let mut cache = self.schemas.write().await;
        if let Some(cached) = cache.get_mut(source_uri) {
            let now = chrono::Utc::now().timestamp();

            // Check if schema has expired
            if now - cached.cached_at > cached.ttl_seconds as i64 {
                cache.remove(source_uri);
                return None;
            }

            // Update access count
            cached.access_count += 1;
            return Some(cached.clone());
        }
        None
    }

    fn extract_scheme(&self, uri: &str) -> SchemaResult<String> {
        uri.split("://")
            .next()
            .map(|s| s.to_string())
            .ok_or_else(|| SchemaError::Provider {
                source: uri.to_string(),
                message: "Invalid URI format, missing scheme".to_string(),
            })
    }

    async fn evict_expired_entries(&self, cache: &mut HashMap<String, CachedSchema>) {
        let now = chrono::Utc::now().timestamp();
        let expired_keys: Vec<_> = cache
            .iter()
            .filter(|(_, cached)| now - cached.cached_at > cached.ttl_seconds as i64)
            .map(|(key, _)| key.clone())
            .collect();

        for key in expired_keys {
            cache.remove(&key);
        }

        // If still full, evict least recently used
        if cache.len() >= self.config.max_cache_size {
            if let Some(lru_key) = cache
                .iter()
                .min_by_key(|(_, cached)| cached.cached_at)
                .map(|(key, _)| key.clone())
            {
                cache.remove(&lru_key);
            }
        }
    }
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            cache_ttl_seconds: 300, // 5 minutes
            max_cache_size: 1000,
            auto_discovery: true,
            validation_enabled: true,
        }
    }
}

/// Statistics about the schema cache
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_entries: usize,
    pub expired_entries: usize,
    pub active_entries: usize,
    pub total_access_count: u64,
    pub hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ferris::schema::{FieldDefinition, SchemaMetadata};
    use crate::ferris::sql::ast::DataType;

    struct MockSchemaProvider {
        scheme: String,
        schema: Schema,
    }

    impl MockSchemaProvider {
        fn new(scheme: String, schema: Schema) -> Self {
            Self { scheme, schema }
        }
    }

    #[async_trait]
    impl SchemaProvider for MockSchemaProvider {
        async fn discover_schema(&self, _source_uri: &str) -> SchemaResult<Schema> {
            Ok(self.schema.clone())
        }

        fn supports_scheme(&self, scheme: &str) -> bool {
            self.scheme == scheme
        }

        fn metadata(&self) -> ProviderMetadata {
            ProviderMetadata {
                name: "mock".to_string(),
                version: "1.0.0".to_string(),
                supported_schemes: vec![self.scheme.clone()],
                capabilities: vec!["discovery".to_string()],
            }
        }
    }

    #[tokio::test]
    async fn test_schema_registry_discovery() {
        let schema = Schema {
            fields: vec![FieldDefinition::required(
                "id".to_string(),
                DataType::Integer,
            )],
            version: Some("1.0.0".to_string()),
            metadata: SchemaMetadata::new("test".to_string()),
        };

        let provider = Arc::new(MockSchemaProvider::new("test".to_string(), schema.clone()));

        let mut registry = SchemaRegistry::new();
        registry.register_provider("test", provider);

        let discovered = registry.discover("test://example").await.unwrap();
        assert_eq!(discovered.fields.len(), 1);
        assert_eq!(discovered.fields[0].name, "id");
    }

    #[tokio::test]
    async fn test_schema_caching() {
        let schema = Schema {
            fields: vec![FieldDefinition::required(
                "name".to_string(),
                DataType::String,
            )],
            version: Some("1.0.0".to_string()),
            metadata: SchemaMetadata::new("test".to_string()),
        };

        let registry = SchemaRegistry::new();
        registry.cache_schema("test://cached", schema.clone()).await;

        let cached = registry.get_cached_schema("test://cached").await.unwrap();
        assert_eq!(cached.schema.fields[0].name, "name");
        assert_eq!(cached.access_count, 1);
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let registry = SchemaRegistry::new();
        let schema = Schema::new(vec![FieldDefinition::required(
            "id".to_string(),
            DataType::Integer,
        )]);

        registry.cache_schema("test://stats", schema).await;

        let stats = registry.cache_stats().await;
        assert_eq!(stats.total_entries, 1);
        assert_eq!(stats.active_entries, 1);
    }
}
