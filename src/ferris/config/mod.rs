//! FerrisStreams Configuration Management
//!
//! This module provides the self-registering configuration schema system for FerrisStreams.
//! It includes hierarchical validation, property inheritance, and JSON schema generation
//! for comprehensive configuration management.

pub mod schema_registry;

// Re-export main types for convenience
pub use schema_registry::{
    validate_config_file_inheritance, validate_configuration, validate_environment_variables,
    ConfigFileInheritance, ConfigSchemaProvider, ConfigValidationError, ConfigValidationResult,
    EnvironmentVariablePattern, GlobalSchemaContext, HierarchicalSchemaRegistry, PropertyDefault,
    PropertyValidation,
};
