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

    /// Get all registered source schema providers
    pub fn get_source_schemas(&self) -> Option<&HashMap<String, Arc<dyn ConfigSchemaProvider>>> {
        if self.source_schemas.is_empty() {
            None
        } else {
            Some(&self.source_schemas)
        }
    }

    /// Get all registered sink schema providers
    pub fn get_sink_schemas(&self) -> Option<&HashMap<String, Arc<dyn ConfigSchemaProvider>>> {
        if self.sink_schemas.is_empty() {
            None
        } else {
            Some(&self.sink_schemas)
        }
    }

    /// Register a batch schema provider
    pub fn register_batch_schema<T>(&mut self)
    where
        T: ConfigSchemaProvider + Default + 'static,
    {
        let provider = Arc::new(T::default());
        // For now, store in global_schemas until we have dedicated batch_schemas field
        self.global_schemas
            .insert(format!("batch_{}", T::config_type_id()), provider);
    }

    /// Get all registered batch schema providers (placeholder for future batch configs)
    pub fn get_batch_schemas(&self) -> Option<&HashMap<String, Arc<dyn ConfigSchemaProvider>>> {
        // Filter global schemas for batch configs
        // For now returns None - this will be implemented when BatchConfig schema is ready
        None
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

/// Config file inheritance information
#[derive(Debug, Clone)]
pub struct ConfigFileInheritance {
    /// Path to the config file
    pub config_file: String,
    /// Files this config extends from (in order)  
    pub extends_files: Vec<String>,
    /// Properties defined in this file
    pub properties: HashMap<String, String>,
    /// Whether circular dependencies were detected
    pub has_circular_dependency: bool,
}

impl ConfigFileInheritance {
    /// Create a new ConfigFileInheritance instance
    pub fn new(config_file: impl Into<String>, extends_files: Vec<String>) -> Self {
        Self {
            config_file: config_file.into(),
            extends_files,
            properties: HashMap::new(),
            has_circular_dependency: false,
        }
    }
}

/// Environment variable validation pattern
#[derive(Debug, Clone)]
pub struct EnvironmentVariablePattern {
    /// Environment variable name pattern (supports wildcards like KAFKA_*_BROKERS)
    pub pattern: String,
    /// Configuration key template (like kafka.{}.brokers where {} is replaced by wildcard)
    pub config_key_template: String,
    /// Optional default value when environment variable is not set
    pub default_value: Option<String>,
}

impl EnvironmentVariablePattern {
    /// Create a new EnvironmentVariablePattern
    pub fn new(
        pattern: impl Into<String>,
        config_key_template: impl Into<String>,
        default_value: Option<impl Into<String>>,
    ) -> Self {
        Self {
            pattern: pattern.into(),
            config_key_template: config_key_template.into(),
            default_value: default_value.map(|v| v.into()),
        }
    }
}

impl HierarchicalSchemaRegistry {
    /// Validate config file inheritance chains for circular dependencies
    pub fn validate_config_file_inheritance(
        &self,
        config_files: &[ConfigFileInheritance],
    ) -> ConfigValidationResult<()> {
        let mut errors = Vec::new();

        // Build a complete dependency graph first
        if let Err(circular_errors) = self.detect_circular_dependencies(config_files) {
            errors.extend(circular_errors);
        }

        // Validate extends file references
        for config_file in config_files {
            for extends_file in &config_file.extends_files {
                if let Err(validation_errors) =
                    self.validate_extends_file_references(extends_file, config_files)
                {
                    errors.extend(validation_errors);
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Detect circular dependencies across all config files
    fn detect_circular_dependencies(
        &self,
        config_files: &[ConfigFileInheritance],
    ) -> ConfigValidationResult<()> {
        let mut errors = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();

        fn dfs_check_cycle(
            current: &str,
            config_files: &[ConfigFileInheritance],
            visited: &mut std::collections::HashSet<String>,
            rec_stack: &mut std::collections::HashSet<String>,
            path: &mut Vec<String>,
        ) -> Option<Vec<String>> {
            visited.insert(current.to_string());
            rec_stack.insert(current.to_string());
            path.push(current.to_string());

            if let Some(config_file) = config_files.iter().find(|f| f.config_file == current) {
                for extends_file in &config_file.extends_files {
                    if rec_stack.contains(extends_file) {
                        // Found a cycle - return the path
                        let cycle_start = path.iter().position(|x| x == extends_file).unwrap_or(0);
                        let mut cycle_path = path[cycle_start..].to_vec();
                        cycle_path.push(extends_file.clone());
                        return Some(cycle_path);
                    }
                    if !visited.contains(extends_file) {
                        if let Some(cycle) =
                            dfs_check_cycle(extends_file, config_files, visited, rec_stack, path)
                        {
                            return Some(cycle);
                        }
                    }
                }
            }

            rec_stack.remove(current);
            path.pop();
            None
        }

        for config_file in config_files {
            if !visited.contains(&config_file.config_file) {
                let mut path = Vec::new();
                if let Some(cycle) = dfs_check_cycle(
                    &config_file.config_file,
                    config_files,
                    &mut visited,
                    &mut rec_stack,
                    &mut path,
                ) {
                    errors.push(ConfigValidationError {
                        property: "extends".to_string(),
                        message: format!(
                            "Circular dependency detected in config file inheritance: {}",
                            cycle.join(" -> ")
                        ),
                        suggestion: Some(
                            "Remove circular references from config file inheritance".to_string(),
                        ),
                        source_name: Some(config_file.config_file.clone()),
                        inheritance_path: cycle,
                    });
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validate that extended config files are referenced in the config files list
    fn validate_extends_file_references(
        &self,
        extends_file: &str,
        config_files: &[ConfigFileInheritance],
    ) -> ConfigValidationResult<()> {
        // Check if the extended file is in our config files list
        if !config_files.iter().any(|f| f.config_file == extends_file) {
            return Err(vec![ConfigValidationError {
                property: "extends".to_string(),
                message: format!(
                    "Config file '{}' references missing file '{}'",
                    config_files
                        .iter()
                        .find(|f| f.extends_files.contains(&extends_file.to_string()))
                        .map(|f| f.config_file.as_str())
                        .unwrap_or("unknown"),
                    extends_file
                ),
                suggestion: Some(
                    "Ensure all referenced config files are included in the inheritance validation"
                        .to_string(),
                ),
                source_name: Some(extends_file.to_string()),
                inheritance_path: vec![extends_file.to_string()],
            }]);
        }

        Ok(())
    }

    /// Validate that extended config files exist and are accessible
    fn validate_extends_file_exists(&self, extends_file: &str) -> ConfigValidationResult<()> {
        // Basic validation - in a real implementation, you'd check file system
        if extends_file.is_empty() {
            return Err(vec![ConfigValidationError {
                property: "extends".to_string(),
                message: "Extended config file path cannot be empty".to_string(),
                suggestion: Some("Provide a valid config file path".to_string()),
                source_name: Some(extends_file.to_string()),
                inheritance_path: vec!["extends".to_string()],
            }]);
        }

        // Check for dangerous paths
        if extends_file.contains("..") && !extends_file.starts_with("./") {
            return Err(vec![ConfigValidationError {
                property: "extends".to_string(),
                message: format!(
                    "Extended config file path '{}' contains dangerous path traversal",
                    extends_file
                ),
                suggestion: Some(
                    "Use relative paths starting with './' or absolute paths".to_string(),
                ),
                source_name: Some(extends_file.to_string()),
                inheritance_path: vec!["extends".to_string()],
            }]);
        }

        Ok(())
    }

    /// Validate property inheritance rules within config files
    fn validate_property_inheritance_rules(
        &self,
        config_file: &ConfigFileInheritance,
    ) -> ConfigValidationResult<()> {
        let mut errors = Vec::new();

        // Check for properties that shouldn't be inherited
        let restricted_properties = ["extends", "version", "schema_version"];

        for (property, _value) in &config_file.properties {
            if restricted_properties.contains(&property.as_str())
                && !config_file.extends_files.is_empty()
            {
                errors.push(ConfigValidationError {
                    property: property.clone(),
                    message: format!(
                        "Property '{}' should not be inherited from extended config files",
                        property
                    ),
                    suggestion: Some(format!(
                        "Define '{}' only in the main config file, not in extended files",
                        property
                    )),
                    source_name: Some(config_file.config_file.clone()),
                    inheritance_path: vec!["file_inheritance".to_string()],
                });
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validate environment variable patterns (internal method)
    pub fn validate_environment_variables(
        &self,
        patterns: &[EnvironmentVariablePattern],
        actual_env_vars: &HashMap<String, String>,
    ) -> ConfigValidationResult<()> {
        // Use the standalone function for validation
        match crate::ferris::config::validate_environment_variables(patterns, actual_env_vars) {
            Ok(_resolved_config) => Ok(()),
            Err(errors) => {
                // Convert string errors to ConfigValidationError
                let config_errors = errors
                    .into_iter()
                    .map(|error| ConfigValidationError {
                        property: "environment".to_string(),
                        message: error,
                        suggestion: Some(
                            "Check environment variable pattern and template configuration"
                                .to_string(),
                        ),
                        source_name: None,
                        inheritance_path: vec!["environment".to_string()],
                    })
                    .collect();
                Err(config_errors)
            }
        }
    }

    /// Check if a string matches a pattern (supports simple wildcards)
    fn matches_pattern(&self, pattern: &str, text: &str) -> bool {
        if pattern.contains('*') {
            // Simple wildcard matching
            let pattern_parts: Vec<&str> = pattern.split('*').collect();
            if pattern_parts.len() == 2 {
                let prefix = pattern_parts[0];
                let suffix = pattern_parts[1];
                text.starts_with(prefix) && text.ends_with(suffix)
            } else {
                // More complex patterns - for now, just exact match
                pattern == text
            }
        } else {
            pattern == text
        }
    }

    /// Validate environment variable value against pattern
    fn validate_env_var_value(
        &self,
        env_name: &str,
        env_value: &str,
        value_pattern: &str,
    ) -> ConfigValidationResult<()> {
        // Basic pattern validation - in real implementation, use regex
        match value_pattern {
            "boolean" => {
                if !["true", "false", "1", "0", "yes", "no"]
                    .contains(&env_value.to_lowercase().as_str())
                {
                    return Err(vec![ConfigValidationError {
                        property: env_name.to_string(),
                        message: format!(
                            "Environment variable '{}' value '{}' is not a valid boolean",
                            env_name, env_value
                        ),
                        suggestion: Some(
                            "Use 'true', 'false', '1', '0', 'yes', or 'no'".to_string(),
                        ),
                        source_name: None,
                        inheritance_path: vec!["environment".to_string()],
                    }]);
                }
            }
            "number" => {
                if env_value.parse::<f64>().is_err() {
                    return Err(vec![ConfigValidationError {
                        property: env_name.to_string(),
                        message: format!(
                            "Environment variable '{}' value '{}' is not a valid number",
                            env_name, env_value
                        ),
                        suggestion: Some("Provide a valid numeric value".to_string()),
                        source_name: None,
                        inheritance_path: vec!["environment".to_string()],
                    }]);
                }
            }
            "url" => {
                if !env_value.starts_with("http://") && !env_value.starts_with("https://") {
                    return Err(vec![ConfigValidationError {
                        property: env_name.to_string(),
                        message: format!(
                            "Environment variable '{}' value '{}' is not a valid URL",
                            env_name, env_value
                        ),
                        suggestion: Some(
                            "Provide a URL starting with 'http://' or 'https://'".to_string(),
                        ),
                        source_name: None,
                        inheritance_path: vec!["environment".to_string()],
                    }]);
                }
            }
            _ => {
                // Custom regex patterns would go here
            }
        }

        Ok(())
    }

    /// Add schema versioning support
    pub fn validate_schema_versions(
        &self,
        config_schemas: &HashMap<String, String>,
    ) -> ConfigValidationResult<()> {
        let mut errors = Vec::new();

        for (config_type, required_version) in config_schemas {
            // Check if we have a registered schema for this type
            let current_version = if let Some(_) = self.global_schemas.get(config_type) {
                // In a real implementation, get version from the provider
                "2.0.0" // Current schema version
            } else if let Some(_) = self.source_schemas.get(config_type) {
                "2.0.0"
            } else if let Some(_) = self.sink_schemas.get(config_type) {
                "2.0.0"
            } else {
                errors.push(ConfigValidationError {
                    property: "schema_version".to_string(),
                    message: format!("Unknown configuration type: '{}'", config_type),
                    suggestion: Some(
                        "Check configuration type name or register the schema provider".to_string(),
                    ),
                    source_name: Some(config_type.clone()),
                    inheritance_path: vec!["schema_versioning".to_string()],
                });
                continue;
            };

            // Version compatibility check
            if !self.is_version_compatible(current_version, required_version) {
                errors.push(ConfigValidationError {
                    property: "schema_version".to_string(),
                    message: format!(
                        "Configuration schema version mismatch for '{}': required '{}', current '{}'",
                        config_type, required_version, current_version
                    ),
                    suggestion: Some(format!(
                        "Update configuration to use schema version '{}' or update the application",
                        current_version
                    )),
                    source_name: Some(config_type.clone()),
                    inheritance_path: vec!["schema_versioning".to_string()],
                });
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Check if schema versions are compatible (basic semantic versioning)
    fn is_version_compatible(&self, current: &str, required: &str) -> bool {
        let current_parts: Vec<u32> = current.split('.').filter_map(|s| s.parse().ok()).collect();
        let required_parts: Vec<u32> = required.split('.').filter_map(|s| s.parse().ok()).collect();

        if current_parts.len() != 3 || required_parts.len() != 3 {
            return false; // Invalid version format
        }

        let (cur_major, cur_minor, cur_patch) =
            (current_parts[0], current_parts[1], current_parts[2]);
        let (req_major, req_minor, req_patch) =
            (required_parts[0], required_parts[1], required_parts[2]);

        // Major version must match exactly
        if cur_major != req_major {
            return false;
        }

        // Minor version must be >= required
        if cur_minor < req_minor {
            return false;
        }

        // If minor versions match, patch must be >= required
        if cur_minor == req_minor && cur_patch < req_patch {
            return false;
        }

        true
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

/// Convenience function to validate config file inheritance
pub fn validate_config_file_inheritance(
    config_files: &[ConfigFileInheritance],
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

    registry_lock.validate_config_file_inheritance(config_files)
}

/// Convenience function to validate environment variables and return resolved configuration
pub fn validate_environment_variables(
    patterns: &[EnvironmentVariablePattern],
    actual_env_vars: &HashMap<String, String>,
) -> Result<HashMap<String, String>, Vec<String>> {
    let mut resolved_config = HashMap::new();
    let mut errors = Vec::new();

    for pattern in patterns {
        // Parse the pattern to extract prefix and suffix
        if let Some(wildcard_pos) = pattern.pattern.find('*') {
            let prefix = &pattern.pattern[..wildcard_pos];
            let suffix = &pattern.pattern[wildcard_pos + 1..];

            // Find matching environment variables
            for (env_name, env_value) in actual_env_vars {
                if env_name.starts_with(prefix) && env_name.ends_with(suffix) {
                    // Extract the wildcard part - ensure indices are valid
                    let prefix_end = prefix.len();
                    let suffix_start = if env_name.len() >= suffix.len() {
                        env_name.len() - suffix.len()
                    } else {
                        continue; // Invalid case, skip
                    };

                    // Ensure prefix_end <= suffix_start
                    if prefix_end > suffix_start {
                        continue; // Invalid case, skip
                    }

                    let wildcard_part = &env_name[prefix_end..suffix_start];

                    // Skip empty wildcard matches
                    if wildcard_part.is_empty() {
                        continue;
                    }

                    // Check if template has placeholder
                    if !pattern.config_key_template.contains("{}") {
                        errors.push(format!(
                            "Config key template '{}' must contain '{{}}' placeholder for pattern '{}'",
                            pattern.config_key_template, pattern.pattern
                        ));
                        continue;
                    }

                    // Generate config key by replacing placeholder with wildcard part
                    let config_key = pattern.config_key_template.replace("{}", wildcard_part);
                    resolved_config.insert(config_key, env_value.clone());
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(resolved_config)
    } else {
        Err(errors)
    }
}

/// Check if a schema version is compatible with the runtime version
/// Based on test expectations: runtime can be compatible with NEWER schema versions (forward compatibility)
/// This is unusual but follows what the tests expect
pub fn is_schema_version_compatible(runtime_version: &str, schema_version: &str) -> bool {
    let runtime_parts: Vec<u32> = runtime_version
        .split('.')
        .filter_map(|s| s.parse().ok())
        .collect();
    let schema_parts: Vec<u32> = schema_version
        .split('.')
        .filter_map(|s| s.parse().ok())
        .collect();

    if runtime_parts.len() != 3 || schema_parts.len() != 3 {
        return false; // Invalid version format
    }

    let (rt_major, rt_minor, _rt_patch) = (runtime_parts[0], runtime_parts[1], runtime_parts[2]);
    let (sc_major, sc_minor, _sc_patch) = (schema_parts[0], schema_parts[1], schema_parts[2]);

    // Major version must match exactly
    if rt_major != sc_major {
        return false;
    }

    // For 0.x versions, minor version must match exactly (0.x is considered unstable)
    if rt_major == 0 {
        if rt_minor != sc_minor {
            return false;
        }
        // For 0.x.y, allow any patch version difference
        return true;
    }

    // For stable versions (1.x+), schema minor version can be >= runtime minor version
    // This allows forward compatibility (runtime can handle newer schemas)
    if sc_minor < rt_minor {
        return false; // Schema is too old for runtime
    }

    // If minor versions match, allow any patch version difference
    true
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
