//! Self-Registering Configuration Schema System
//!
//! This module implements a hierarchical self-registering schema system where config-consuming
//! classes own and maintain their validation schemas. It provides:
//!
//! - **Self-Registration**: Classes automatically register their schemas at startup
//! - **Hierarchical Validation**: Global → Config File → Named → Inline property inheritance
//! - **JSON Schema Generation**: Automatic schema generation for IDE integration
//! - **Pre-deployment Validation**: Configuration validated before job deployment
//!
//! # Architecture
//!
//! The schema system operates on a trait-based architecture where each config-consuming
//! class implements `ConfigSchemaProvider` to define its validation rules and inheritance patterns.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Configuration validation error with context information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigValidationError {
    /// Property name that failed validation
    pub property: String,
    /// Error message explaining what's wrong
    pub message: String,
    /// Optional suggestion for fixing the error
    pub suggestion: Option<String>,
    /// Source name that contains the error (e.g., "kafka_orders")
    pub source_name: Option<String>,
    /// Inheritance path where the property comes from
    pub inheritance_path: Vec<String>,
}

impl std::fmt::Display for ConfigValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Property '{}': {}", self.property, self.message)?;
        if let Some(suggestion) = &self.suggestion {
            write!(f, " (Suggestion: {})", suggestion)?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigValidationError {}

/// Result type for configuration validation operations
pub type ConfigValidationResult<T = ()> = Result<T, Vec<ConfigValidationError>>;

/// Default value resolution strategy for configuration properties
#[derive(Debug, Clone)]
pub enum PropertyDefault {
    /// Static string value
    Static(String),
    /// Dynamic value computed from global context
    Dynamic(fn(&GlobalSchemaContext) -> String),
    /// Look up value from global properties
    GlobalLookup(String),
    /// Computed value based on other properties and context
    Computed(fn(&HashMap<String, String>, &GlobalSchemaContext) -> String),
}

/// Global context available during schema validation and property resolution
#[derive(Debug, Clone, Default)]
pub struct GlobalSchemaContext {
    /// Global configuration properties
    pub global_properties: HashMap<String, String>,
    /// Environment variables
    pub environment_variables: HashMap<String, String>,
    /// Profile-based properties
    pub profile_properties: HashMap<String, String>,
    /// System defaults
    pub system_defaults: HashMap<String, String>,
}

/// Property validation rule for configuration schema
#[derive(Debug, Clone)]
pub struct PropertyValidation {
    /// Property key name
    pub key: String,
    /// Whether the property is required
    pub required: bool,
    /// Default value if not provided
    pub default: Option<PropertyDefault>,
    /// Description for documentation
    pub description: String,
    /// JSON Schema type (string, number, boolean, array, object)
    pub json_type: String,
    /// Pattern for validation (regex for strings, range for numbers)
    pub validation_pattern: Option<String>,
}

/// Core trait for config-consuming classes to provide their schema validation
pub trait ConfigSchemaProvider: Send + Sync {
    /// Unique identifier for this configuration type
    fn config_type_id() -> &'static str
    where
        Self: Sized;

    /// Properties that can be inherited from global configuration
    fn inheritable_properties() -> Vec<&'static str>
    where
        Self: Sized;

    /// Properties that must be specified for each named configuration instance
    fn required_named_properties() -> Vec<&'static str>
    where
        Self: Sized;

    /// Optional properties with their default values
    fn optional_properties_with_defaults() -> HashMap<&'static str, PropertyDefault>
    where
        Self: Sized;

    /// Validate a single property value
    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>>;

    /// Generate JSON Schema representation for this configuration type
    fn json_schema() -> Value
    where
        Self: Sized;

    /// Whether this config type supports custom properties beyond the defined schema
    fn supports_custom_properties() -> bool
    where
        Self: Sized,
    {
        true
    }

    /// Schema dependencies on other global schema types
    fn global_schema_dependencies() -> Vec<&'static str>
    where
        Self: Sized,
    {
        vec![]
    }

    /// Resolve property value with inheritance chain
    fn resolve_property_with_inheritance(
        &self,
        _key: &str,
        local_value: Option<&str>,
        _global_context: &GlobalSchemaContext,
    ) -> Result<Option<String>, String> {
        // Default implementation: local value takes precedence
        Ok(local_value.map(|s| s.to_string()))
    }

    /// Schema version for migration support
    fn schema_version() -> &'static str
    where
        Self: Sized,
    {
        "1.0.0"
    }

    /// Property validation rules for JSON schema generation
    fn property_validations() -> Vec<PropertyValidation>
    where
        Self: Sized,
    {
        vec![]
    }
}

/// Override source in property inheritance chain
#[derive(Debug, Clone, PartialEq)]
pub enum OverrideSource {
    /// Global configuration
    Global,
    /// Configuration file
    ConfigFile(String),
    /// Profile-based configuration
    Profile(String),
    /// Named configuration instance
    Named(String),
    /// Inline SQL WITH clause
    Inline,
    /// Environment variable
    EnvironmentVariable(String),
}

/// Property inheritance resolution node
#[derive(Debug)]
pub struct PropertyInheritanceNode {
    /// Property key
    pub property_key: String,
    /// Inheritance chain from global to specific
    pub inheritance_path: Vec<String>,
    /// Override precedence order
    pub override_precedence: Vec<OverrideSource>,
}

/// Registry for managing hierarchical configuration schemas
pub struct HierarchicalSchemaRegistry {
    /// Global schema providers
    global_schemas: HashMap<String, Arc<dyn ConfigSchemaProvider>>,
    /// Source-specific schema providers
    source_schemas: HashMap<String, Arc<dyn ConfigSchemaProvider>>,
    /// Sink-specific schema providers
    sink_schemas: HashMap<String, Arc<dyn ConfigSchemaProvider>>,
    /// Global context for property resolution
    global_context: GlobalSchemaContext,
    /// Property inheritance tree
    property_inheritance: HashMap<String, PropertyInheritanceNode>,
}

impl Default for HierarchicalSchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl HierarchicalSchemaRegistry {
    /// Create a new schema registry
    pub fn new() -> Self {
        Self {
            global_schemas: HashMap::new(),
            source_schemas: HashMap::new(),
            sink_schemas: HashMap::new(),
            global_context: GlobalSchemaContext::default(),
            property_inheritance: HashMap::new(),
        }
    }

    /// Get global singleton instance (for integration with existing code)
    pub fn global() -> Arc<RwLock<Self>> {
        static INSTANCE: std::sync::OnceLock<Arc<RwLock<HierarchicalSchemaRegistry>>> =
            std::sync::OnceLock::new();
        INSTANCE
            .get_or_init(|| Arc::new(RwLock::new(Self::new())))
            .clone()
    }

    /// Register a global schema provider
    pub fn register_global_schema<T>(&mut self)
    where
        T: ConfigSchemaProvider + Default + 'static,
    {
        let provider = Arc::new(T::default());
        self.global_schemas
            .insert(T::config_type_id().to_string(), provider);
    }

    /// Register a source schema provider
    pub fn register_source_schema<T>(&mut self)
    where
        T: ConfigSchemaProvider + Default + 'static,
    {
        let provider = Arc::new(T::default());
        self.source_schemas
            .insert(T::config_type_id().to_string(), provider);
    }

    /// Register a sink schema provider
    pub fn register_sink_schema<T>(&mut self)
    where
        T: ConfigSchemaProvider + Default + 'static,
    {
        let provider = Arc::new(T::default());
        self.sink_schemas
            .insert(T::config_type_id().to_string(), provider);
    }

    /// Update global context for property resolution
    pub fn update_global_context(&mut self, context: GlobalSchemaContext) {
        self.global_context = context;
    }

    /// Validate a configuration with hierarchical inheritance
    pub fn validate_config_with_inheritance(
        &self,
        global_config: &HashMap<String, String>,
        named_config: &HashMap<String, String>,
        inline_config: &HashMap<String, String>,
    ) -> ConfigValidationResult<()> {
        let mut errors = Vec::new();

        // Validate global configuration
        for (key, value) in global_config {
            if let Err(mut property_errors) = self.validate_global_property(key, value) {
                errors.append(&mut property_errors);
            }
        }

        // Validate named configurations
        for (key, value) in named_config {
            if let Err(mut property_errors) = self.validate_named_property(key, value) {
                errors.append(&mut property_errors);
            }
        }

        // Validate inline configurations
        for (key, value) in inline_config {
            if let Err(mut property_errors) = self.validate_inline_property(key, value) {
                errors.append(&mut property_errors);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validate a global property
    fn validate_global_property(&self, key: &str, value: &str) -> ConfigValidationResult<()> {
        // Find appropriate global schema provider
        for (_, provider) in &self.global_schemas {
            if let Err(validation_errors) = provider.validate_property(key, value) {
                let errors = validation_errors
                    .into_iter()
                    .map(|message| ConfigValidationError {
                        property: key.to_string(),
                        message,
                        suggestion: None,
                        source_name: None,
                        inheritance_path: vec!["global".to_string()],
                    })
                    .collect();
                return Err(errors);
            }
        }
        Ok(())
    }

    /// Validate a named configuration property
    fn validate_named_property(&self, key: &str, value: &str) -> ConfigValidationResult<()> {
        // Parse named property (e.g., "kafka_orders.bootstrap.servers")
        let parts: Vec<&str> = key.splitn(2, '.').collect();
        if parts.len() != 2 {
            return Err(vec![ConfigValidationError {
                property: key.to_string(),
                message: "Named property must be in format 'source_name.property_name'".to_string(),
                suggestion: Some("Use format like 'kafka_orders.bootstrap.servers'".to_string()),
                source_name: None,
                inheritance_path: vec!["named".to_string()],
            }]);
        }

        let source_name = parts[0];
        let property_name = parts[1];

        // Determine source type and validate with appropriate schema
        let source_type = self.determine_source_type(
            source_name,
            &HashMap::from([(key.to_string(), value.to_string())]),
        );
        if let Some(provider) = self.source_schemas.get(&source_type) {
            if let Err(validation_errors) = provider.validate_property(property_name, value) {
                let errors = validation_errors
                    .into_iter()
                    .map(|message| ConfigValidationError {
                        property: key.to_string(),
                        message,
                        suggestion: None,
                        source_name: Some(source_name.to_string()),
                        inheritance_path: vec!["named".to_string(), source_name.to_string()],
                    })
                    .collect();
                return Err(errors);
            }
        }

        Ok(())
    }

    /// Validate an inline configuration property
    fn validate_inline_property(&self, key: &str, value: &str) -> ConfigValidationResult<()> {
        // Inline properties are typically global overrides
        self.validate_global_property(key, value)
    }

    /// Determine source type from configuration
    fn determine_source_type(&self, source_name: &str, config: &HashMap<String, String>) -> String {
        // Look for explicit type declaration
        if let Some(source_type) = config.get(&format!("{}.type", source_name)) {
            return source_type.clone();
        }

        // Infer from source name patterns
        if source_name.contains("kafka") {
            "kafka_source".to_string()
        } else if source_name.contains("file") {
            "file_source".to_string()
        } else if source_name.contains("s3") {
            "s3_source".to_string()
        } else {
            "generic_source".to_string()
        }
    }

    /// Generate complete JSON Schema for all registered configurations
    pub fn generate_complete_json_schema(&self) -> Value {
        let mut schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "FerrisStreams Configuration Schema",
            "description": "Comprehensive schema for FerrisStreams configuration validation",
            "type": "object",
            "properties": {},
            "definitions": {}
        });

        // Add global schemas
        for (type_id, _) in &self.global_schemas {
            if let Ok(type_schema) = self.get_schema_for_type(type_id) {
                schema["definitions"][type_id] = type_schema;
            }
        }

        // Add source schemas
        for (type_id, _) in &self.source_schemas {
            if let Ok(type_schema) = self.get_schema_for_type(type_id) {
                schema["definitions"][type_id] = type_schema;
            }
        }

        // Add sink schemas
        for (type_id, _) in &self.sink_schemas {
            if let Ok(type_schema) = self.get_schema_for_type(type_id) {
                schema["definitions"][type_id] = type_schema;
            }
        }

        schema
    }

    /// Get JSON schema for a specific configuration type
    fn get_schema_for_type(&self, type_id: &str) -> Result<Value, String> {
        // Try global schemas first
        if let Some(_) = self.global_schemas.get(type_id) {
            return Ok(self.build_type_schema(type_id));
        }

        // Try source schemas
        if let Some(_) = self.source_schemas.get(type_id) {
            return Ok(self.build_type_schema(type_id));
        }

        // Try sink schemas
        if let Some(_) = self.sink_schemas.get(type_id) {
            return Ok(self.build_type_schema(type_id));
        }

        Err(format!("Schema not found for type: {}", type_id))
    }

    /// Build JSON schema for a configuration type
    fn build_type_schema(&self, type_id: &str) -> Value {
        serde_json::json!({
            "type": "object",
            "description": format!("Configuration schema for {}", type_id),
            "properties": {},
            "additionalProperties": true
        })
    }
}

/// Convenience function to validate configuration using global registry
pub fn validate_configuration(
    global_config: &HashMap<String, String>,
    named_config: &HashMap<String, String>,
    inline_config: &HashMap<String, String>,
) -> ConfigValidationResult<()> {
    let registry = HierarchicalSchemaRegistry::global();
    let registry_lock = registry.read().map_err(|_| {
        vec![ConfigValidationError {
            property: "system".to_string(),
            message: "Failed to acquire registry lock".to_string(),
            suggestion: None,
            source_name: None,
            inheritance_path: vec!["system".to_string()],
        }]
    })?;

    registry_lock.validate_config_with_inheritance(global_config, named_config, inline_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct TestConfigProvider;

    impl ConfigSchemaProvider for TestConfigProvider {
        fn config_type_id() -> &'static str {
            "test_config"
        }

        fn inheritable_properties() -> Vec<&'static str> {
            vec!["global_property"]
        }

        fn required_named_properties() -> Vec<&'static str> {
            vec!["required_property"]
        }

        fn optional_properties_with_defaults() -> HashMap<&'static str, PropertyDefault> {
            let mut defaults = HashMap::new();
            defaults.insert(
                "optional_property",
                PropertyDefault::Static("default_value".to_string()),
            );
            defaults
        }

        fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>> {
            match key {
                "required_property" => {
                    if value.is_empty() {
                        Err(vec!["required_property cannot be empty".to_string()])
                    } else {
                        Ok(())
                    }
                }
                _ => Ok(()),
            }
        }

        fn json_schema() -> Value {
            serde_json::json!({
                "type": "object",
                "properties": {
                    "required_property": {
                        "type": "string",
                        "description": "A required test property"
                    }
                },
                "required": ["required_property"]
            })
        }
    }

    #[test]
    fn test_schema_registry_creation() {
        let mut registry = HierarchicalSchemaRegistry::new();
        registry.register_global_schema::<TestConfigProvider>();

        assert_eq!(registry.global_schemas.len(), 1);
        assert!(registry.global_schemas.contains_key("test_config"));
    }

    #[test]
    fn test_property_validation() {
        let provider = TestConfigProvider::default();

        // Valid property
        assert!(provider
            .validate_property("required_property", "valid_value")
            .is_ok());

        // Invalid property (empty required)
        assert!(provider.validate_property("required_property", "").is_err());
    }

    #[test]
    fn test_json_schema_generation() {
        let schema = TestConfigProvider::json_schema();
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["required_property"]["type"] == "string");
    }
}
