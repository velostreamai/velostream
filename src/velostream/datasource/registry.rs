//! Data Source Registry
//!
//! This module provides a registry system for managing different data source implementations.
//! It allows for dynamic registration and creation of data sources and sinks based on URI schemes.
//!
//! This is the generic registry that works with any data processing system.

use crate::velostream::datasource::{
    config::{ConnectionString, SinkConfig, SourceConfig},
    DataSink, DataSource, DataSourceError,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

// Type-erased factories using generic config types
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
        // TODO: Register default implementations when available
        // registry.register_source("kafka", |config| { ... });
        // registry.register_source("file", |config| { ... });
        // registry.register_sink("kafka", |config| { ... });
        // registry.register_sink("file", |config| { ... });

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

        factory(config).map_err(DataSourceError::SinkSpecific)
    }

    /// List all registered source schemes
    pub fn source_schemes(&self) -> Vec<String> {
        self.source_factories.keys().cloned().collect()
    }

    /// List all registered sink schemes
    pub fn sink_schemes(&self) -> Vec<String> {
        self.sink_factories.keys().cloned().collect()
    }

    /// Check if a source scheme is registered
    pub fn has_source_scheme(&self, scheme: &str) -> bool {
        self.source_factories.contains_key(scheme)
    }

    /// Check if a sink scheme is registered
    pub fn has_sink_scheme(&self, scheme: &str) -> bool {
        self.sink_factories.contains_key(scheme)
    }
}

impl Default for DataSourceRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Global registry instance
static GLOBAL_REGISTRY: OnceLock<Arc<Mutex<DataSourceRegistry>>> = OnceLock::new();

/// Get or initialize the global registry
pub fn global_registry() -> &'static Arc<Mutex<DataSourceRegistry>> {
    GLOBAL_REGISTRY.get_or_init(|| Arc::new(Mutex::new(DataSourceRegistry::with_defaults())))
}

/// Initialize the global registry with a custom registry
pub fn initialize_global_registry(registry: DataSourceRegistry) {
    let _ = GLOBAL_REGISTRY.set(Arc::new(Mutex::new(registry)));
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

/// Register a source factory in the global registry
pub fn register_global_source<F>(scheme: &str, factory: F)
where
    F: Fn(SourceConfig) -> Result<Box<dyn DataSource>, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync
        + 'static,
{
    let registry = global_registry();
    if let Ok(mut registry) = registry.lock() {
        registry.register_source(scheme, factory);
    }
}

/// Register a sink factory in the global registry
pub fn register_global_sink<F>(scheme: &str, factory: F)
where
    F: Fn(SinkConfig) -> Result<Box<dyn DataSink>, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync
        + 'static,
{
    let registry = global_registry();
    if let Ok(mut registry) = registry.lock() {
        registry.register_sink(scheme, factory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = DataSourceRegistry::new();
        assert!(registry.source_schemes().is_empty());
        assert!(registry.sink_schemes().is_empty());
    }

    #[test]
    fn test_global_registry_access() {
        let registry = global_registry();
        assert!(registry.lock().is_ok());
    }

    #[test]
    fn test_create_source_no_factory() {
        let registry = DataSourceRegistry::new();
        let result = registry.create_source("unknown://localhost/test");
        assert!(result.is_err());
        if let Err(DataSourceError::Configuration(msg)) = result {
            assert!(msg.contains("No factory registered"));
        }
    }

    #[test]
    fn test_create_sink_no_factory() {
        let registry = DataSourceRegistry::new();
        let result = registry.create_sink("unknown://localhost/test");
        assert!(result.is_err());
        if let Err(DataSourceError::Configuration(msg)) = result {
            assert!(msg.contains("No factory registered"));
        }
    }
}
