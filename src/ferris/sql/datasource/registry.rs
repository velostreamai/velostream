//! Data Source Registry
//!
//! This module provides a registry system for managing different data source implementations.
//! It allows for dynamic registration and creation of data sources and sinks based on URI schemes.

use crate::ferris::sql::datasource::kafka::{KafkaDataSink, KafkaDataSource};
use crate::ferris::sql::datasource::{
    ConnectionString, DataSink, DataSource, DataSourceError, SinkConfig, SourceConfig,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Simplified type-erased factories
type SourceFactory = Box<
    dyn Fn(SourceConfig) -> Result<Box<dyn DataSource>, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync,
>;
type SinkFactory = Box<
    dyn Fn(SinkConfig) -> Result<Box<dyn DataSink>, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync,
>;

/// Registry for managing data source and sink factories
pub struct DataSourceRegistry {
    source_factories: HashMap<String, SourceFactory>,
    sink_factories: HashMap<String, SinkFactory>,
}

impl DataSourceRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            source_factories: HashMap::new(),
            sink_factories: HashMap::new(),
        }
    }

    /// Create a registry with default implementations
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();

        // Register Kafka adapter
        registry.register_source("kafka", |config| match config {
            SourceConfig::Kafka {
                brokers,
                topic,
                group_id,
                ..
            } => {
                let mut kafka_source = KafkaDataSource::new(brokers, topic);
                if let Some(gid) = group_id {
                    kafka_source = kafka_source.with_group_id(gid);
                }
                Ok(Box::new(kafka_source))
            }
            _ => Err("Invalid configuration for Kafka source".into()),
        });

        registry.register_sink("kafka", |config| match config {
            SinkConfig::Kafka { brokers, topic, .. } => {
                let kafka_sink = KafkaDataSink::new(brokers, topic);
                Ok(Box::new(kafka_sink))
            }
            _ => Err("Invalid configuration for Kafka sink".into()),
        });

        registry
    }

    /// Register a source factory for a specific scheme
    pub fn register_source<F>(&mut self, scheme: &str, factory: F)
    where
        F: Fn(
                SourceConfig,
            ) -> Result<Box<dyn DataSource>, Box<dyn std::error::Error + Send + Sync>>
            + Send
            + Sync
            + 'static,
    {
        self.source_factories
            .insert(scheme.to_string(), Box::new(factory));
    }

    /// Register a sink factory for a specific scheme  
    pub fn register_sink<F>(&mut self, scheme: &str, factory: F)
    where
        F: Fn(SinkConfig) -> Result<Box<dyn DataSink>, Box<dyn std::error::Error + Send + Sync>>
            + Send
            + Sync
            + 'static,
    {
        self.sink_factories
            .insert(scheme.to_string(), Box::new(factory));
    }

    /// Create a data source from a URI
    pub fn create_source(&self, uri: &str) -> Result<Box<dyn DataSource>, DataSourceError> {
        let conn = ConnectionString::parse(uri)?;
        let config = conn.to_source_config()?;

        let factory = self.source_factories.get(&conn.scheme).ok_or_else(|| {
            DataSourceError::Configuration(format!(
                "No factory registered for source scheme: {}",
                conn.scheme
            ))
        })?;

        factory(config).map_err(DataSourceError::SourceSpecific)
    }

    /// Create a data sink from a URI
    pub fn create_sink(&self, uri: &str) -> Result<Box<dyn DataSink>, DataSourceError> {
        let conn = ConnectionString::parse(uri)?;
        let config = conn.to_sink_config()?;

        let factory = self.sink_factories.get(&conn.scheme).ok_or_else(|| {
            DataSourceError::Configuration(format!(
                "No factory registered for sink scheme: {}",
                conn.scheme
            ))
        })?;

        factory(config).map_err(DataSourceError::SourceSpecific)
    }

    /// List all registered source schemes
    pub fn list_source_schemes(&self) -> Vec<String> {
        self.source_factories.keys().cloned().collect()
    }

    /// List all registered sink schemes
    pub fn list_sink_schemes(&self) -> Vec<String> {
        self.sink_factories.keys().cloned().collect()
    }

    /// Check if a source scheme is supported
    pub fn supports_source(&self, scheme: &str) -> bool {
        self.source_factories.contains_key(scheme)
    }

    /// Check if a sink scheme is supported
    pub fn supports_sink(&self, scheme: &str) -> bool {
        self.sink_factories.contains_key(scheme)
    }
}

impl Default for DataSourceRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

// Global registry instance
lazy_static::lazy_static! {
    static ref GLOBAL_REGISTRY: Arc<Mutex<DataSourceRegistry>> =
        Arc::new(Mutex::new(DataSourceRegistry::with_defaults()));
}

/// Get the global data source registry
pub fn global_registry() -> Arc<Mutex<DataSourceRegistry>> {
    GLOBAL_REGISTRY.clone()
}

/// Convenience function to create a data source from URI using global registry
pub fn create_source(uri: &str) -> Result<Box<dyn DataSource>, DataSourceError> {
    let registry = global_registry();
    let registry = registry.lock().map_err(|_| {
        DataSourceError::Configuration("Failed to acquire registry lock".to_string())
    })?;
    registry.create_source(uri)
}

/// Convenience function to create a data sink from URI using global registry  
pub fn create_sink(uri: &str) -> Result<Box<dyn DataSink>, DataSourceError> {
    let registry = global_registry();
    let registry = registry.lock().map_err(|_| {
        DataSourceError::Configuration("Failed to acquire registry lock".to_string())
    })?;
    registry.create_sink(uri)
}

/// Register a source factory globally
pub fn register_global_source<F>(scheme: &str, factory: F)
where
    F: Fn(SourceConfig) -> Result<Box<dyn DataSource>, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync
        + 'static,
{
    let registry = global_registry();
    let mut registry = registry.lock().expect("Failed to acquire registry lock");
    registry.register_source(scheme, factory);
}

/// Register a sink factory globally
pub fn register_global_sink<F>(scheme: &str, factory: F)
where
    F: Fn(SinkConfig) -> Result<Box<dyn DataSink>, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync
        + 'static,
{
    let registry = global_registry();
    let mut registry = registry.lock().expect("Failed to acquire registry lock");
    registry.register_sink(scheme, factory);
}

/// Builder pattern for creating custom registries
pub struct RegistryBuilder {
    registry: DataSourceRegistry,
}

impl RegistryBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            registry: DataSourceRegistry::new(),
        }
    }

    /// Add a source factory
    pub fn with_source<F>(mut self, scheme: &str, factory: F) -> Self
    where
        F: Fn(
                SourceConfig,
            ) -> Result<Box<dyn DataSource>, Box<dyn std::error::Error + Send + Sync>>
            + Send
            + Sync
            + 'static,
    {
        self.registry.register_source(scheme, factory);
        self
    }

    /// Add a sink factory
    pub fn with_sink<F>(mut self, scheme: &str, factory: F) -> Self
    where
        F: Fn(SinkConfig) -> Result<Box<dyn DataSink>, Box<dyn std::error::Error + Send + Sync>>
            + Send
            + Sync
            + 'static,
    {
        self.registry.register_sink(scheme, factory);
        self
    }

    /// Build the registry
    pub fn build(self) -> DataSourceRegistry {
        self.registry
    }
}

impl Default for RegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ferris::sql::datasource::{SinkMetadata, SourceMetadata};
    use async_trait::async_trait;

    // Mock implementations for testing
    struct MockSource;
    struct MockSink;

    #[async_trait]
    impl DataSource for MockSource {
        async fn initialize(
            &mut self,
            _config: SourceConfig,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn fetch_schema(
            &self,
        ) -> Result<crate::ferris::schema::Schema, Box<dyn std::error::Error + Send + Sync>>
        {
            use crate::ferris::sql::ast::DataType;
            use crate::ferris::schema::{FieldDefinition, Schema};
            Ok(Schema::new(vec![FieldDefinition::required(
                "id".to_string(),
                DataType::Integer,
            )]))
        }

        async fn create_reader(
            &self,
        ) -> Result<
            Box<dyn crate::ferris::sql::datasource::DataReader>,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            todo!()
        }

        fn supports_streaming(&self) -> bool {
            true
        }

        fn supports_batch(&self) -> bool {
            false
        }

        fn metadata(&self) -> SourceMetadata {
            SourceMetadata {
                source_type: "mock".to_string(),
                version: "1.0.0".to_string(),
                supports_streaming: true,
                supports_batch: false,
                supports_schema_evolution: false,
                capabilities: vec![],
            }
        }
    }

    #[async_trait]
    impl DataSink for MockSink {
        async fn initialize(
            &mut self,
            _config: SinkConfig,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn validate_schema(
            &self,
            _schema: &crate::ferris::schema::Schema,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }

        async fn create_writer(
            &self,
        ) -> Result<
            Box<dyn crate::ferris::sql::datasource::DataWriter>,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            todo!()
        }

        fn supports_transactions(&self) -> bool {
            false
        }

        fn supports_upsert(&self) -> bool {
            false
        }

        fn metadata(&self) -> SinkMetadata {
            SinkMetadata {
                sink_type: "mock".to_string(),
                version: "1.0.0".to_string(),
                supports_transactions: false,
                supports_upsert: false,
                supports_schema_evolution: false,
                capabilities: vec![],
            }
        }
    }

    #[test]
    fn test_registry_builder() {
        let registry = RegistryBuilder::new()
            .with_source("mock", |_config| {
                Ok(Box::new(MockSource) as Box<dyn DataSource>)
            })
            .with_sink(
                "mock",
                |_config| Ok(Box::new(MockSink) as Box<dyn DataSink>),
            )
            .build();

        assert!(registry.supports_source("mock"));
        assert!(registry.supports_sink("mock"));
        assert!(!registry.supports_source("unknown"));
    }

    #[test]
    fn test_scheme_listing() {
        let registry = RegistryBuilder::new()
            .with_source("test1", |_| Ok(Box::new(MockSource) as Box<dyn DataSource>))
            .with_source("test2", |_| Ok(Box::new(MockSource) as Box<dyn DataSource>))
            .with_sink("sink1", |_| Ok(Box::new(MockSink) as Box<dyn DataSink>))
            .build();

        let sources = registry.list_source_schemes();
        let sinks = registry.list_sink_schemes();

        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&"test1".to_string()));
        assert!(sources.contains(&"test2".to_string()));

        assert_eq!(sinks.len(), 1);
        assert!(sinks.contains(&"sink1".to_string()));
    }
}
