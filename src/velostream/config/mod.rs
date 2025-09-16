//! VeloStream Configuration Management
//!
//! This module provides the self-registering configuration schema system for VeloStream.
//! It includes hierarchical validation, property inheritance, and JSON schema generation
//! for comprehensive configuration management.

pub mod schema_registry;

// Re-export main types for convenience
pub use schema_registry::{
    validate_environment_variables, ConfigSchemaProvider, GlobalSchemaContext,
    HierarchicalSchemaRegistry, PropertyDefault, PropertyValidation,
};
