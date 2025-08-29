//! Data Source Registry
//!
//! This module provides a registry system for managing different data source implementations.
//! It allows for dynamic registration and creation of data sources and sinks based on URI schemes.
//!
//! TODO: This module needs architectural rework to properly integrate with the generic
//! datasource implementations. The current version provides a minimal working implementation.

use crate::ferris::datasource::{DataSink, DataSource, DataSourceError};
use crate::ferris::sql::datasource::config::{SinkConfig, SourceConfig};
use crate::ferris::sql::datasource::ConnectionString;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Simplified type-erased factories using SQL config types
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
    /// TODO: Implement with generic datasource implementations
    pub fn with_defaults() -> Self {
        // TODO: Add factory registrations when async architecture is resolved
        Self::new()
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

// TODO: Add tests when registry implementation is complete
