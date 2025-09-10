# Configuration Schema System - API Reference

## Core Types

### ConfigSchemaProvider

The main trait that all configuration providers must implement.

```rust
pub trait ConfigSchemaProvider: Send + Sync {
    // Required methods
    fn config_type_id() -> &'static str where Self: Sized;
    fn inheritable_properties() -> Vec<&'static str> where Self: Sized;
    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>>;
    fn get_property_default(&self, key: &str) -> PropertyDefault;
    fn validate_configuration(&self, config: &HashMap<String, String>) -> ConfigValidationResult<()>;
    fn json_schema(&self) -> serde_json::Value;
    fn schema_version() -> &'static str where Self: Sized;
    fn environment_defaults(&self) -> HashMap<String, String>;
    fn validate_property_types(&self, config: &HashMap<String, String>) -> ConfigValidationResult<()>;
    fn get_validation_context(&self) -> GlobalSchemaContext;
    fn dependency_properties(&self) -> Vec<PropertyValidation>;
}
```

#### Method Details

**`config_type_id() -> &'static str`**
- Returns unique identifier for this configuration type
- Used for registry lookup and validation calls
- Should be lowercase with underscores (e.g., "kafka_source", "file_sink")

**`inheritable_properties() -> Vec<&'static str>`**
- Lists properties that can be inherited from parent configurations
- Used in property hierarchy resolution
- Common inheritable properties: "timeout", "retry_count", "compression.type"

**`validate_property(key: &str, value: &str) -> Result<(), Vec<String>>`**
- Validates individual property key-value pairs
- Returns `Ok(())` if valid, `Err(Vec<String>)` with error messages if invalid
- Should provide specific, actionable error messages

**`get_property_default(key: &str) -> PropertyDefault`**
- Returns default value for a property
- Supports static defaults and environment-aware defaults
- Used when property is not explicitly set

**`validate_configuration(config: &HashMap<String, String>) -> ConfigValidationResult<()>`**
- Validates complete configuration
- Checks inter-property dependencies and business logic
- Called after individual property validation

**`json_schema() -> serde_json::Value`**
- Returns JSON Schema definition for this configuration
- Used for IDE integration and documentation generation
- Should follow JSON Schema Draft 7 specification

**`schema_version() -> &'static str`**
- Returns semantic version of this schema (e.g., "1.0.0")
- Used for compatibility checking
- Must follow semantic versioning rules

**`environment_defaults() -> HashMap<String, String>`**
- Returns environment-specific default values
- Different defaults for development vs production
- Overrides static defaults when environment is detected

**`validate_property_types(config: &HashMap<String, String>) -> ConfigValidationResult<()>`**
- Validates data types of all properties
- Checks string format, numeric ranges, boolean values
- Called before business logic validation

**`get_validation_context() -> GlobalSchemaContext`**
- Returns current validation environment context
- Used to determine which defaults and validations to apply
- Values: Development, Staging, Production

**`dependency_properties() -> Vec<PropertyValidation>`**
- Returns property dependencies and constraints
- Defines which properties require other properties
- Used for complex validation scenarios

### ConfigValidationError

Structured error type for validation failures.

```rust
pub struct ConfigValidationError {
    pub property: String,        // Property name that failed
    pub message: String,         // Human-readable error message
    pub suggestion: Option<String>, // Optional suggestion for fixing
    pub source_name: Option<String>, // Configuration source name
    pub inheritance_path: Vec<String>, // Property inheritance chain
}
```

### PropertyDefault

Enum for different types of property defaults.

```rust
pub enum PropertyDefault {
    None,                        // No default value
    Static(String),             // Fixed default value
    EnvironmentAware {          // Different defaults per environment
        development: String,
        production: String,
    },
    Computed(Box<dyn Fn() -> String + Send + Sync>), // Dynamic default
}
```

### PropertyValidation

Defines validation rules and dependencies for properties.

```rust
pub struct PropertyValidation {
    pub property: String,        // Property name
    pub required: bool,          // Whether property is required
    pub depends_on: Vec<String>, // Properties this one depends on
    pub mutually_exclusive: Vec<String>, // Properties that conflict with this one
    pub validation_rule: ValidationRule, // Specific validation logic
}
```

### HierarchicalSchemaRegistry

Central registry for managing configuration schemas.

```rust
pub struct HierarchicalSchemaRegistry {
    // Private fields - use methods to interact
}

impl HierarchicalSchemaRegistry {
    pub fn new() -> Self;
    pub fn register_source_schema<T: ConfigSchemaProvider + Default + 'static>(&mut self);
    pub fn register_sink_schema<T: ConfigSchemaProvider + Default + 'static>(&mut self);
    pub fn register_batch_schema<T: ConfigSchemaProvider + Default + 'static>(&mut self);
    pub fn validate_configuration(&self, config_type: &str, config: &HashMap<String, String>) -> ConfigValidationResult<()>;
    pub fn generate_combined_json_schema(&self) -> serde_json::Value;
    pub fn get_property_defaults(&self, config_type: &str) -> HashMap<String, PropertyDefault>;
    pub fn validate_config_file_inheritance(&self, config_files: &[ConfigFileInheritance]) -> ConfigValidationResult<()>;
    pub fn validate_environment_variables(&self, patterns: &[EnvironmentVariablePattern], env_vars: &HashMap<String, String>) -> ConfigValidationResult<()>;
}
```

## Advanced Types

### ConfigFileInheritance

Represents configuration file inheritance relationships.

```rust
pub struct ConfigFileInheritance {
    pub config_file: String,         // Path to config file
    pub extends_files: Vec<String>,  // Files this config extends
    pub properties: HashMap<String, String>, // Properties in this file
    pub has_circular_dependency: bool, // Circular dependency flag
}

impl ConfigFileInheritance {
    pub fn new(config_file: impl Into<String>, extends_files: Vec<String>) -> Self;
}
```

### EnvironmentVariablePattern

Defines patterns for mapping environment variables to configuration properties.

```rust
pub struct EnvironmentVariablePattern {
    pub pattern: String,              // Pattern with wildcards (e.g., "KAFKA_*_BROKERS")
    pub config_key_template: String, // Template for config keys (e.g., "kafka.{}.brokers")
    pub default_value: Option<String>, // Default value if env var not set
}

impl EnvironmentVariablePattern {
    pub fn new(
        pattern: impl Into<String>,
        config_key_template: impl Into<String>,
        default_value: Option<impl Into<String>>
    ) -> Self;
}
```

### GlobalSchemaContext

Environment context for validation.

```rust
pub enum GlobalSchemaContext {
    Development,    // Development environment
    Staging,       // Staging environment  
    Production,    // Production environment
}
```

## Global Functions

### Registry Functions

```rust
// Get the global registry instance
pub fn global_registry() -> &'static Arc<Mutex<HierarchicalSchemaRegistry>>;

// Initialize global registry with custom instance
pub fn initialize_global_registry(registry: HierarchicalSchemaRegistry);

// Register schema providers globally
pub fn register_global_source<T: ConfigSchemaProvider + Default + 'static>();
pub fn register_global_sink<T: ConfigSchemaProvider + Default + 'static>();
```

### Validation Functions

```rust
// Validate configuration against registered schema
pub fn validate_configuration(
    config_type: &str, 
    config: &HashMap<String, String>
) -> ConfigValidationResult<()>;

// Validate config file inheritance
pub fn validate_config_file_inheritance(
    config_files: &[ConfigFileInheritance]
) -> Result<(), Vec<String>>;

// Validate and resolve environment variables
pub fn validate_environment_variables(
    patterns: &[EnvironmentVariablePattern],
    env_vars: &HashMap<String, String>
) -> Result<HashMap<String, String>, Vec<String>>;

// Check schema version compatibility  
pub fn is_schema_version_compatible(
    runtime_version: &str, 
    schema_version: &str
) -> bool;
```

## Type Aliases

```rust
pub type ConfigValidationResult<T> = Result<T, Vec<ConfigValidationError>>;
```

## Common Validation Patterns

### URL Validation

```rust
fn validate_url(value: &str) -> Result<(), Vec<String>> {
    if value.starts_with("http://") || value.starts_with("https://") {
        Ok(())
    } else {
        Err(vec!["URL must start with http:// or https://".to_string()])
    }
}
```

### Port Validation

```rust
fn validate_port(value: &str) -> Result<(), Vec<String>> {
    match value.parse::<u16>() {
        Ok(port) if port > 0 => Ok(()),
        _ => Err(vec!["Port must be between 1 and 65535".to_string()])
    }
}
```

### Timeout Validation

```rust
fn validate_timeout(value: &str) -> Result<(), Vec<String>> {
    match value.parse::<u32>() {
        Ok(timeout) if timeout >= 1 && timeout <= 300 => Ok(()),
        Ok(_) => Err(vec!["Timeout must be between 1 and 300 seconds".to_string()]),
        Err(_) => Err(vec!["Timeout must be a valid number".to_string()])
    }
}
```

### Broker List Validation

```rust
fn validate_broker_list(value: &str) -> Result<(), Vec<String>> {
    if value.is_empty() {
        return Err(vec!["Broker list cannot be empty".to_string()]);
    }
    
    let brokers: Vec<&str> = value.split(',').collect();
    for broker in brokers {
        let parts: Vec<&str> = broker.trim().split(':').collect();
        if parts.len() != 2 {
            return Err(vec![format!("Invalid broker format: '{}'. Expected 'host:port'", broker)]);
        }
        
        if let Err(_) = parts[1].parse::<u16>() {
            return Err(vec![format!("Invalid port in broker: '{}'", broker)]);
        }
    }
    
    Ok(())
}
```

## Error Handling Best Practices

### Detailed Error Messages

```rust
fn validate_batch_size(value: &str) -> Result<(), Vec<String>> {
    match value.parse::<u32>() {
        Ok(size) if size >= 1 && size <= 100_000 => Ok(()),
        Ok(size) => Err(vec![
            format!("Batch size {} is out of range", size),
            "Batch size must be between 1 and 100,000".to_string(),
            "Consider using a smaller batch size for better memory usage".to_string()
        ]),
        Err(_) => Err(vec![
            format!("Invalid batch size: '{}'", value),
            "Batch size must be a positive integer".to_string(),
            "Example: batch.size=16384".to_string()
        ])
    }
}
```

### Structured Error Creation

```rust
fn create_validation_error(property: &str, message: &str, suggestion: Option<&str>) -> ConfigValidationError {
    ConfigValidationError {
        property: property.to_string(),
        message: message.to_string(),
        suggestion: suggestion.map(|s| s.to_string()),
        source_name: Some("my_component".to_string()),
        inheritance_path: vec![property.to_string()],
    }
}
```

## Integration Examples

### SQL Integration

```sql
CREATE STREAM user_events 
FROM KAFKA 'kafka://localhost:9092/events'
WITH (
    'group.id' = 'analytics-service',  -- Validated by KafkaDataSource
    'auto.offset.reset' = 'earliest',  -- Validated by KafkaDataSource
    'batch.size' = '16384'             -- Validated by BatchConfig
);
```

### Configuration File Integration

```yaml
# config.yaml - validated against combined JSON schema
datasources:
  kafka:
    brokers: "localhost:9092"    # Validated by KafkaDataSource
    topic: "user-events"         # Validated by KafkaDataSource
  
  file:  
    path: "./data/events.json"   # Validated by FileDataSource
    format: "json"               # Validated by FileDataSource
```

This API reference provides comprehensive documentation for all types and functions in the FerrisStreams Configuration Schema System.