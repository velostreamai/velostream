# Configuration Schema System - Quick Start Guide

## TL;DR - 5 Minute Setup

Want to add configuration validation to your component? Here's the minimal setup:

### 1. Implement the Trait (2 minutes)

```rust
use velostream::velo::config::{ConfigSchemaProvider, PropertyDefault};

impl ConfigSchemaProvider for YourComponent {
    fn config_type_id() -> &'static str { "your_component" }
    
    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>> {
        match key {
            "url" => validate_url(value),
            "timeout" => validate_positive_integer(value),
            _ => Ok(()) // Unknown properties are ignored
        }
    }
    
    fn get_property_default(&self, key: &str) -> PropertyDefault {
        match key {
            "timeout" => PropertyDefault::Static("30".to_string()),
            _ => PropertyDefault::None
        }
    }
    
    // Minimal implementations for remaining required methods:
    fn validate_configuration(&self, _: &std::collections::HashMap<String, String>) -> crate::velo::config::ConfigValidationResult<()> { Ok(()) }
    fn json_schema(&self) -> serde_json::Value { serde_json::json!({}) }
    fn schema_version() -> &'static str { "1.0.0" }
    fn environment_defaults(&self) -> std::collections::HashMap<String, String> { std::collections::HashMap::new() }
    fn validate_property_types(&self, _: &std::collections::HashMap<String, String>) -> crate::velo::config::ConfigValidationResult<()> { Ok(()) }
    fn get_validation_context(&self) -> crate::velo::config::GlobalSchemaContext { crate::velo::config::GlobalSchemaContext::Development }
    fn dependency_properties(&self) -> Vec<crate::velo::config::PropertyValidation> { vec![] }
    fn inheritable_properties() -> Vec<&'static str> { vec![] }
}
```

### 2. Register Your Schema (1 minute)

```rust
use velostream::velo::config::register_global_source;

// During application startup:
fn initialize_schemas() {
    let registry = velostream::velo::config::global_registry();
    let mut registry = registry.lock().unwrap();
    registry.register_source_schema::<YourComponent>();
}
```

### 3. Use Validation (2 minutes)

```rust
use velostream::velo::config::validate_configuration;
use std::collections::HashMap;

let mut config = HashMap::new();
config.insert("url".to_string(), "https://api.example.com".to_string());
config.insert("timeout".to_string(), "60".to_string());

match validate_configuration("your_component", &config) {
    Ok(()) => println!("✅ Configuration is valid!"),
    Err(errors) => {
        for error in errors {
            println!("❌ {}: {}", error.property, error.message);
        }
    }
}
```

## Common Validation Helpers

```rust
fn validate_url(value: &str) -> Result<(), Vec<String>> {
    if value.starts_with("http://") || value.starts_with("https://") {
        Ok(())
    } else {
        Err(vec!["URL must start with http:// or https://".to_string()])
    }
}

fn validate_positive_integer(value: &str) -> Result<(), Vec<String>> {
    match value.parse::<u32>() {
        Ok(n) if n > 0 => Ok(()),
        _ => Err(vec!["Must be a positive integer".to_string()])
    }
}

fn validate_port(value: &str) -> Result<(), Vec<String>> {
    match value.parse::<u16>() {
        Ok(port) if port > 0 => Ok(()),
        _ => Err(vec!["Port must be between 1 and 65535".to_string()])
    }
}
```

## Real-World Example: Database Configuration

```rust
pub struct DatabaseConfig {
    host: String,
    port: u16,
    database: String,
}

impl ConfigSchemaProvider for DatabaseConfig {
    fn config_type_id() -> &'static str { "database" }
    
    fn validate_property(&self, key: &str, value: &str) -> Result<(), Vec<String>> {
        match key {
            "host" => {
                if value.is_empty() {
                    Err(vec!["Host cannot be empty".to_string()])
                } else {
                    Ok(())
                }
            },
            "port" => validate_port(value),
            "database" => {
                if value.len() < 3 {
                    Err(vec!["Database name must be at least 3 characters".to_string()])
                } else {
                    Ok(())
                }
            },
            "connection_timeout" => validate_positive_integer(value),
            _ => Ok(())
        }
    }
    
    fn get_property_default(&self, key: &str) -> PropertyDefault {
        match key {
            "port" => PropertyDefault::Static("5432".to_string()),
            "connection_timeout" => PropertyDefault::EnvironmentAware {
                development: "10".to_string(),
                production: "30".to_string(),
            },
            _ => PropertyDefault::None
        }
    }
    
    fn validate_configuration(&self, config: &HashMap<String, String>) -> ConfigValidationResult<()> {
        let mut errors = Vec::new();
        
        // Ensure required properties are present
        for required in &["host", "database"] {
            if !config.contains_key(*required) {
                errors.push(ConfigValidationError {
                    property: required.to_string(),
                    message: format!("Required property '{}' is missing", required),
                    suggestion: Some(format!("Add '{}' to your configuration", required)),
                    source_name: Some("database".to_string()),
                    inheritance_path: vec![required.to_string()],
                });
            }
        }
        
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
    
    // ... implement remaining methods with sensible defaults
}
```

## IDE Integration (Bonus)

Generate JSON Schema for your IDE:

```rust
use velostream::velo::config::HierarchicalSchemaRegistry;

let registry = HierarchicalSchemaRegistry::new();
registry.register_source_schema::<DatabaseConfig>();

let schema = registry.generate_combined_json_schema();
std::fs::write("config.schema.json", serde_json::to_string_pretty(&schema)?)?;
```

Then in your `config.yaml`:

```yaml
# yaml-language-server: $schema=./config.schema.json

database:
  host: localhost    # IDE will validate this!
  port: 5432        # IDE will show errors for invalid values
  database: mydb    # IDE will provide autocompletion
```

## Testing Your Implementation

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_valid_config() {
        let db_config = DatabaseConfig::default();
        let mut config = HashMap::new();
        config.insert("host".to_string(), "localhost".to_string());
        config.insert("database".to_string(), "testdb".to_string());
        
        assert!(db_config.validate_configuration(&config).is_ok());
    }

    #[test]
    fn test_invalid_port() {
        let db_config = DatabaseConfig::default();
        let result = db_config.validate_property("port", "99999");
        
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("Port must be"));
    }
}
```

## What's Next?

- Read the [Full Documentation](./CONFIGURATION_SCHEMA_SYSTEM.md) for advanced features
- Check out existing implementations in `src/velo/datasource/kafka/` and `src/velo/datasource/file/`  
- Add environment variable patterns for dynamic configuration
- Implement config file inheritance for complex deployments

## Need Help?

- **Compilation errors**: Make sure all trait methods are implemented
- **Schema not found**: Verify registration is called during startup  
- **Validation not working**: Check that your `config_type_id()` matches the validation call
- **Performance issues**: Validation should be < 50ms; check for expensive operations in validation methods

That's it! You now have production-ready configuration validation in under 5 minutes. 🚀