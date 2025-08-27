//! SQL WITH Clause Configuration Parser
//!
//! Advanced parsing and validation of WITH clause configurations for
//! schema registry integration, caching strategies, and data source options.

use std::collections::HashMap;
use std::time::Duration;

use crate::ferris::schema::client::{MultiLevelCacheConfig, RegistryClientConfig};
use crate::ferris::sql::config::{ConfigError, DataSourceConfig};

/// SQL WITH clause configuration parser
pub struct WithClauseParser {
    /// Known configuration keys and their validation rules
    config_schema: HashMap<String, ConfigKeySchema>,
    /// Environment variable resolver
    env_resolver: EnvironmentResolver,
}

/// Configuration key schema for validation
#[derive(Debug, Clone)]
pub struct ConfigKeySchema {
    /// Key name (e.g., "schema.registry.url")
    pub key: String,
    /// Expected value type
    pub value_type: ConfigValueType,
    /// Whether the key is required
    pub required: bool,
    /// Default value if not specified
    pub default_value: Option<String>,
    /// Validation pattern (regex)
    pub validation_pattern: Option<String>,
    /// Description for documentation
    pub description: String,
    /// Allowed values for enum types
    pub allowed_values: Option<Vec<String>>,
}

/// Configuration value types supported in WITH clauses
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValueType {
    String,
    Integer,
    Float,
    Boolean,
    Duration,
    Url,
    FilePath,
    Enum(Vec<String>),
    List(Box<ConfigValueType>),
}

/// Parsed WITH clause configuration
#[derive(Debug, Clone)]
pub struct WithClauseConfig {
    /// Raw key-value pairs from WITH clause
    pub raw_config: HashMap<String, String>,
    /// Parsed and validated schema registry configuration
    pub schema_registry: Option<RegistryClientConfig>,
    /// Parsed multi-level cache configuration
    pub cache_config: Option<MultiLevelCacheConfig>,
    /// Data source configuration
    pub data_source: Option<DataSourceConfig>,
    /// Custom application-specific configurations
    pub custom_config: HashMap<String, ConfigValue>,
    /// Configuration parsing warnings
    pub warnings: Vec<String>,
}

/// Strongly-typed configuration value
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Duration(Duration),
    Url(String),
    FilePath(String),
    List(Vec<ConfigValue>),
}

/// Environment variable resolver for configuration interpolation
#[derive(Debug)]
pub struct EnvironmentResolver {
    /// Environment variables cache
    env_cache: HashMap<String, String>,
    /// Whether to use system environment variables
    use_system_env: bool,
}

/// Configuration parsing result
pub type WithClauseResult<T> = Result<T, WithClauseError>;

/// Errors that can occur during WITH clause parsing
#[derive(Debug, Clone)]
pub enum WithClauseError {
    /// Invalid syntax in WITH clause
    SyntaxError { message: String, position: usize },
    /// Unknown configuration key
    UnknownKey { key: String, suggestions: Vec<String> },
    /// Invalid value for configuration key
    InvalidValue { key: String, value: String, expected: ConfigValueType },
    /// Required configuration key missing
    MissingRequired { key: String },
    /// Configuration validation failed
    ValidationError { key: String, message: String },
    /// Environment variable not found
    EnvironmentError { variable: String },
    /// URL parsing error
    UrlError { url: String, reason: String },
    /// File path error
    PathError { path: String, reason: String },
}

impl WithClauseParser {
    /// Create a new WITH clause parser with default schema registry configurations
    pub fn new() -> Self {
        let mut parser = Self {
            config_schema: HashMap::new(),
            env_resolver: EnvironmentResolver::new(),
        };
        
        parser.register_default_schema();
        parser
    }

    /// Parse WITH clause from SQL string
    pub fn parse_with_clause(&self, with_clause: &str) -> WithClauseResult<WithClauseConfig> {
        // Parse raw key-value pairs
        let raw_config = self.parse_raw_config(with_clause)?;
        
        // Resolve environment variables
        let resolved_config = self.resolve_environment_variables(&raw_config)?;
        
        // Validate against schema
        let validated_config = self.validate_config(&resolved_config)?;
        
        // Build typed configuration objects
        let mut config = WithClauseConfig {
            raw_config: validated_config.clone(),
            schema_registry: None,
            cache_config: None,
            data_source: None,
            custom_config: HashMap::new(),
            warnings: Vec::new(),
        };
        
        // Parse schema registry configuration
        if self.has_schema_registry_config(&validated_config) {
            config.schema_registry = Some(self.parse_schema_registry_config(&validated_config)?);
        }
        
        // Parse cache configuration
        if self.has_cache_config(&validated_config) {
            config.cache_config = Some(self.parse_cache_config(&validated_config)?);
        }
        
        // Parse data source configuration
        if self.has_data_source_config(&validated_config) {
            config.data_source = Some(self.parse_data_source_config(&validated_config)?);
        }
        
        // Parse custom configurations
        config.custom_config = self.parse_custom_config(&validated_config)?;
        
        Ok(config)
    }

    /// Register configuration schema for a specific domain
    pub fn register_config_schema(&mut self, domain: &str, schemas: Vec<ConfigKeySchema>) {
        for schema in schemas {
            let full_key = if domain.is_empty() {
                schema.key.clone()
            } else {
                format!("{}.{}", domain, schema.key)
            };
            self.config_schema.insert(full_key, schema);
        }
    }

    /// Parse raw key-value pairs from WITH clause string
    fn parse_raw_config(&self, with_clause: &str) -> WithClauseResult<HashMap<String, String>> {
        let mut config = HashMap::new();
        let mut position = 0;
        
        // Remove WITH keyword if present
        let content = with_clause.trim_start_matches("WITH").trim();
        
        // Split by commas, handling quoted values
        let pairs = self.split_config_pairs(content)?;
        
        for pair in pairs {
            let (key, value) = self.parse_config_pair(&pair, position)?;
            config.insert(key, value);
            position += pair.len();
        }
        
        Ok(config)
    }

    /// Split configuration pairs handling quotes and nested structures
    fn split_config_pairs(&self, content: &str) -> WithClauseResult<Vec<String>> {
        let mut pairs = Vec::new();
        let mut current_pair = String::new();
        let mut in_quotes = false;
        let mut quote_char = '"';
        let mut escape_next = false;
        let mut paren_depth = 0;
        
        for (_pos, ch) in content.char_indices() {
            if escape_next {
                current_pair.push(ch);
                escape_next = false;
                continue;
            }
            
            match ch {
                '\\' if in_quotes => {
                    escape_next = true;
                    current_pair.push(ch);
                }
                '\'' | '"' if !in_quotes => {
                    in_quotes = true;
                    quote_char = ch;
                    current_pair.push(ch);
                }
                c if in_quotes && c == quote_char => {
                    in_quotes = false;
                    current_pair.push(ch);
                }
                '(' if !in_quotes => {
                    paren_depth += 1;
                    current_pair.push(ch);
                }
                ')' if !in_quotes => {
                    paren_depth -= 1;
                    current_pair.push(ch);
                }
                ',' if !in_quotes && paren_depth == 0 => {
                    if !current_pair.trim().is_empty() {
                        pairs.push(current_pair.trim().to_string());
                    }
                    current_pair.clear();
                }
                _ => {
                    current_pair.push(ch);
                }
            }
        }
        
        if !current_pair.trim().is_empty() {
            pairs.push(current_pair.trim().to_string());
        }
        
        if in_quotes {
            return Err(WithClauseError::SyntaxError {
                message: "Unterminated quoted string".to_string(),
                position: content.len(),
            });
        }
        
        Ok(pairs)
    }

    /// Parse individual configuration pair (key = value)
    fn parse_config_pair(&self, pair: &str, position: usize) -> WithClauseResult<(String, String)> {
        let parts: Vec<&str> = pair.splitn(2, '=').collect();
        
        if parts.len() != 2 {
            return Err(WithClauseError::SyntaxError {
                message: format!("Invalid configuration pair: '{}'. Expected format: 'key = value'", pair),
                position,
            });
        }
        
        let key = parts[0].trim().trim_matches('\'').trim_matches('"');
        let value = parts[1].trim();
        
        // Remove quotes from value if present
        let unquoted_value = if (value.starts_with('"') && value.ends_with('"')) ||
                               (value.starts_with('\'') && value.ends_with('\'')) {
            &value[1..value.len()-1]
        } else {
            value
        };
        
        Ok((key.to_string(), unquoted_value.to_string()))
    }

    /// Resolve environment variables in configuration values
    fn resolve_environment_variables(&self, config: &HashMap<String, String>) -> WithClauseResult<HashMap<String, String>> {
        let mut resolved = HashMap::new();
        
        for (key, value) in config {
            let resolved_value = self.env_resolver.resolve(value)?;
            resolved.insert(key.clone(), resolved_value);
        }
        
        Ok(resolved)
    }

    /// Validate configuration against registered schema
    fn validate_config(&self, config: &HashMap<String, String>) -> WithClauseResult<HashMap<String, String>> {
        let mut validated = HashMap::new();
        let mut warnings = Vec::new();
        
        // Check for unknown keys
        for key in config.keys() {
            if !self.config_schema.contains_key(key) {
                let suggestions = self.suggest_similar_keys(key);
                if suggestions.is_empty() {
                    warnings.push(format!("Unknown configuration key: '{}'", key));
                } else {
                    return Err(WithClauseError::UnknownKey {
                        key: key.clone(),
                        suggestions,
                    });
                }
            }
        }
        
        // Check required keys
        for (key, schema) in &self.config_schema {
            if schema.required && !config.contains_key(key) {
                return Err(WithClauseError::MissingRequired { key: key.clone() });
            }
        }
        
        // Validate values against types
        for (key, value) in config {
            if let Some(schema) = self.config_schema.get(key) {
                self.validate_value(key, value, &schema.value_type)?;
                validated.insert(key.clone(), value.clone());
            }
        }
        
        // Add default values
        for (key, schema) in &self.config_schema {
            if !validated.contains_key(key) {
                if let Some(default) = &schema.default_value {
                    validated.insert(key.clone(), default.clone());
                }
            }
        }
        
        Ok(validated)
    }

    /// Validate individual configuration value against expected type
    fn validate_value(&self, key: &str, value: &str, expected_type: &ConfigValueType) -> WithClauseResult<()> {
        match expected_type {
            ConfigValueType::String => Ok(()),
            ConfigValueType::Integer => {
                value.parse::<i64>().map_err(|_| WithClauseError::InvalidValue {
                    key: key.to_string(),
                    value: value.to_string(),
                    expected: expected_type.clone(),
                })?;
                Ok(())
            }
            ConfigValueType::Float => {
                value.parse::<f64>().map_err(|_| WithClauseError::InvalidValue {
                    key: key.to_string(),
                    value: value.to_string(),
                    expected: expected_type.clone(),
                })?;
                Ok(())
            }
            ConfigValueType::Boolean => {
                match value.to_lowercase().as_str() {
                    "true" | "false" | "yes" | "no" | "1" | "0" => Ok(()),
                    _ => Err(WithClauseError::InvalidValue {
                        key: key.to_string(),
                        value: value.to_string(),
                        expected: expected_type.clone(),
                    })
                }
            }
            ConfigValueType::Duration => {
                self.parse_duration(value).map_err(|_| WithClauseError::InvalidValue {
                    key: key.to_string(),
                    value: value.to_string(),
                    expected: expected_type.clone(),
                })?;
                Ok(())
            }
            ConfigValueType::Url => {
                if !value.starts_with("http://") && !value.starts_with("https://") {
                    return Err(WithClauseError::UrlError {
                        url: value.to_string(),
                        reason: "URL must start with http:// or https://".to_string(),
                    });
                }
                Ok(())
            }
            ConfigValueType::FilePath => {
                // Basic path validation
                if value.is_empty() {
                    return Err(WithClauseError::PathError {
                        path: value.to_string(),
                        reason: "Path cannot be empty".to_string(),
                    });
                }
                Ok(())
            }
            ConfigValueType::Enum(allowed_values) => {
                if allowed_values.contains(&value.to_string()) {
                    Ok(())
                } else {
                    Err(WithClauseError::InvalidValue {
                        key: key.to_string(),
                        value: value.to_string(),
                        expected: expected_type.clone(),
                    })
                }
            }
            ConfigValueType::List(_) => {
                // List validation would be more complex, simplified for now
                Ok(())
            }
        }
    }

    /// Parse duration string (e.g., "5s", "10m", "1h")
    fn parse_duration(&self, duration_str: &str) -> Result<Duration, std::num::ParseIntError> {
        let duration_str = duration_str.trim();
        
        if let Some(suffix) = duration_str.chars().last() {
            let (value_str, multiplier) = match suffix {
                's' => (&duration_str[..duration_str.len()-1], 1),
                'm' => (&duration_str[..duration_str.len()-1], 60),
                'h' => (&duration_str[..duration_str.len()-1], 3600),
                'd' => (&duration_str[..duration_str.len()-1], 86400),
                _ => (duration_str, 1), // Assume seconds if no suffix
            };
            
            let value: u64 = value_str.parse()?;
            Ok(Duration::from_secs(value * multiplier))
        } else {
            let value: u64 = duration_str.parse()?;
            Ok(Duration::from_secs(value))
        }
    }

    /// Suggest similar configuration keys for typos
    fn suggest_similar_keys(&self, key: &str) -> Vec<String> {
        let mut suggestions = Vec::new();
        
        for schema_key in self.config_schema.keys() {
            if self.levenshtein_distance(key, schema_key) <= 2 {
                suggestions.push(schema_key.clone());
            }
        }
        
        suggestions.sort();
        suggestions.truncate(3); // Limit to 3 suggestions
        suggestions
    }

    /// Calculate Levenshtein distance for string similarity
    fn levenshtein_distance(&self, a: &str, b: &str) -> usize {
        let a_chars: Vec<char> = a.chars().collect();
        let b_chars: Vec<char> = b.chars().collect();
        let a_len = a_chars.len();
        let b_len = b_chars.len();
        
        if a_len == 0 { return b_len; }
        if b_len == 0 { return a_len; }
        
        let mut matrix = vec![vec![0; b_len + 1]; a_len + 1];
        
        for i in 0..=a_len {
            matrix[i][0] = i;
        }
        for j in 0..=b_len {
            matrix[0][j] = j;
        }
        
        for i in 1..=a_len {
            for j in 1..=b_len {
                let cost = if a_chars[i-1] == b_chars[j-1] { 0 } else { 1 };
                matrix[i][j] = std::cmp::min(
                    std::cmp::min(
                        matrix[i-1][j] + 1,      // deletion
                        matrix[i][j-1] + 1       // insertion
                    ),
                    matrix[i-1][j-1] + cost      // substitution
                );
            }
        }
        
        matrix[a_len][b_len]
    }

    /// Check if configuration contains schema registry settings
    fn has_schema_registry_config(&self, config: &HashMap<String, String>) -> bool {
        config.keys().any(|k| k.starts_with("schema.registry."))
    }

    /// Check if configuration contains cache settings
    fn has_cache_config(&self, config: &HashMap<String, String>) -> bool {
        config.keys().any(|k| k.starts_with("cache."))
    }

    /// Check if configuration contains data source settings
    fn has_data_source_config(&self, config: &HashMap<String, String>) -> bool {
        config.keys().any(|k| k.starts_with("source.") || k.starts_with("sink."))
    }

    /// Parse schema registry configuration from validated config
    fn parse_schema_registry_config(&self, config: &HashMap<String, String>) -> WithClauseResult<RegistryClientConfig> {
        let registry_config = RegistryClientConfig::default();
        
        // Parse schema registry URL
        if let Some(_url) = config.get("schema.registry.url") {
            // URL validation already done in validate_config
            // Set registry URL in config (would need to add this field to RegistryClientConfig)
        }
        
        // Parse authentication settings
        if let Some(auth_type) = config.get("schema.registry.auth.type") {
            match auth_type.as_str() {
                "basic" => {
                    // Parse basic auth
                    if let (Some(_username), Some(_password)) = (
                        config.get("schema.registry.auth.username"),
                        config.get("schema.registry.auth.password")
                    ) {
                        // Set authentication in config
                    }
                }
                "oauth2" => {
                    // Parse OAuth2 settings
                    if let Some(_token) = config.get("schema.registry.auth.oauth2.token") {
                        // Set OAuth2 token
                    }
                }
                _ => {
                    return Err(WithClauseError::InvalidValue {
                        key: "schema.registry.auth.type".to_string(),
                        value: auth_type.clone(),
                        expected: ConfigValueType::Enum(vec![
                            "basic".to_string(),
                            "oauth2".to_string(),
                            "none".to_string(),
                        ]),
                    });
                }
            }
        }
        
        Ok(registry_config)
    }

    /// Parse cache configuration from validated config
    fn parse_cache_config(&self, config: &HashMap<String, String>) -> WithClauseResult<MultiLevelCacheConfig> {
        let mut cache_config = MultiLevelCacheConfig::default();
        
        // L1 cache settings
        if let Some(l1_max_entries) = config.get("cache.l1.max_entries") {
            cache_config.l1_max_entries = l1_max_entries.parse().unwrap(); // Already validated
        }
        
        if let Some(l1_max_memory) = config.get("cache.l1.max_memory_mb") {
            cache_config.l1_max_memory_mb = l1_max_memory.parse().unwrap();
        }
        
        if let Some(l1_ttl) = config.get("cache.l1.ttl") {
            cache_config.l1_ttl = self.parse_duration(l1_ttl).unwrap(); // Already validated
        }
        
        // L2 cache settings
        if let Some(l2_max_entries) = config.get("cache.l2.max_entries") {
            cache_config.l2_max_entries = l2_max_entries.parse().unwrap();
        }
        
        if let Some(l2_max_memory) = config.get("cache.l2.max_memory_mb") {
            cache_config.l2_max_memory_mb = l2_max_memory.parse().unwrap();
        }
        
        if let Some(l2_ttl) = config.get("cache.l2.ttl") {
            cache_config.l2_ttl = self.parse_duration(l2_ttl).unwrap();
        }
        
        // L3 cache settings
        if let Some(l3_max_entries) = config.get("cache.l3.max_entries") {
            cache_config.l3_max_entries = l3_max_entries.parse().unwrap();
        }
        
        if let Some(l3_path) = config.get("cache.l3.storage_path") {
            cache_config.l3_storage_path = l3_path.clone();
        }
        
        if let Some(l3_compression) = config.get("cache.l3.compression_enabled") {
            cache_config.l3_compression_enabled = self.parse_boolean(l3_compression);
        }
        
        // Adaptive settings
        if let Some(adaptive) = config.get("cache.adaptive_management") {
            cache_config.enable_adaptive_management = self.parse_boolean(adaptive);
        }
        
        if let Some(promotion_threshold) = config.get("cache.promotion_threshold") {
            cache_config.promotion_threshold = promotion_threshold.parse().unwrap();
        }
        
        if let Some(demotion_threshold) = config.get("cache.demotion_threshold") {
            cache_config.demotion_threshold = demotion_threshold.parse().unwrap();
        }
        
        Ok(cache_config)
    }

    /// Parse data source configuration
    fn parse_data_source_config(&self, _config: &HashMap<String, String>) -> WithClauseResult<DataSourceConfig> {
        // This would parse source/sink specific configurations
        // Implementation depends on DataSourceConfig structure
        Ok(DataSourceConfig::default())
    }

    /// Parse custom configuration values
    fn parse_custom_config(&self, config: &HashMap<String, String>) -> WithClauseResult<HashMap<String, ConfigValue>> {
        let mut custom = HashMap::new();
        
        for (key, value) in config {
            if !key.starts_with("schema.registry.") && 
               !key.starts_with("cache.") && 
               !key.starts_with("source.") && 
               !key.starts_with("sink.") {
                
                // Try to infer the type from the value
                let config_value = if let Ok(int_val) = value.parse::<i64>() {
                    ConfigValue::Integer(int_val)
                } else if let Ok(float_val) = value.parse::<f64>() {
                    ConfigValue::Float(float_val)
                } else if let Ok(bool_val) = self.try_parse_boolean(value) {
                    ConfigValue::Boolean(bool_val)
                } else if let Ok(duration_val) = self.parse_duration(value) {
                    ConfigValue::Duration(duration_val)
                } else if value.starts_with("http://") || value.starts_with("https://") {
                    ConfigValue::Url(value.clone())
                } else {
                    ConfigValue::String(value.clone())
                };
                
                custom.insert(key.clone(), config_value);
            }
        }
        
        Ok(custom)
    }

    /// Parse boolean from various string representations
    fn parse_boolean(&self, value: &str) -> bool {
        match value.to_lowercase().as_str() {
            "true" | "yes" | "1" | "on" | "enabled" => true,
            _ => false,
        }
    }

    /// Try to parse boolean (returns error if not a boolean)
    fn try_parse_boolean(&self, value: &str) -> Result<bool, ()> {
        match value.to_lowercase().as_str() {
            "true" | "yes" | "1" | "on" | "enabled" => Ok(true),
            "false" | "no" | "0" | "off" | "disabled" => Ok(false),
            _ => Err(()),
        }
    }

    /// Register default configuration schema for schema registry and caching
    fn register_default_schema(&mut self) {
        // Schema Registry configurations
        let schema_registry_configs = vec![
            ConfigKeySchema {
                key: "schema.registry.url".to_string(),
                value_type: ConfigValueType::Url,
                required: false,
                default_value: Some("http://localhost:8081".to_string()),
                validation_pattern: None,
                description: "Schema Registry base URL".to_string(),
                allowed_values: None,
            },
            ConfigKeySchema {
                key: "schema.registry.auth.type".to_string(),
                value_type: ConfigValueType::Enum(vec![
                    "none".to_string(),
                    "basic".to_string(),
                    "oauth2".to_string(),
                ]),
                required: false,
                default_value: Some("none".to_string()),
                validation_pattern: None,
                description: "Authentication type for Schema Registry".to_string(),
                allowed_values: Some(vec![
                    "none".to_string(),
                    "basic".to_string(),
                    "oauth2".to_string(),
                ]),
            },
            ConfigKeySchema {
                key: "schema.registry.auth.username".to_string(),
                value_type: ConfigValueType::String,
                required: false,
                default_value: None,
                validation_pattern: None,
                description: "Username for basic authentication".to_string(),
                allowed_values: None,
            },
            ConfigKeySchema {
                key: "schema.registry.auth.password".to_string(),
                value_type: ConfigValueType::String,
                required: false,
                default_value: None,
                validation_pattern: None,
                description: "Password for basic authentication".to_string(),
                allowed_values: None,
            },
        ];

        // Cache configurations
        let cache_configs = vec![
            ConfigKeySchema {
                key: "cache.l1.max_entries".to_string(),
                value_type: ConfigValueType::Integer,
                required: false,
                default_value: Some("100".to_string()),
                validation_pattern: None,
                description: "Maximum entries in L1 hot cache".to_string(),
                allowed_values: None,
            },
            ConfigKeySchema {
                key: "cache.l1.max_memory_mb".to_string(),
                value_type: ConfigValueType::Integer,
                required: false,
                default_value: Some("50".to_string()),
                validation_pattern: None,
                description: "Maximum memory for L1 cache in MB".to_string(),
                allowed_values: None,
            },
            ConfigKeySchema {
                key: "cache.l1.ttl".to_string(),
                value_type: ConfigValueType::Duration,
                required: false,
                default_value: Some("5m".to_string()),
                validation_pattern: None,
                description: "Time-to-live for L1 cache entries".to_string(),
                allowed_values: None,
            },
            ConfigKeySchema {
                key: "cache.adaptive_management".to_string(),
                value_type: ConfigValueType::Boolean,
                required: false,
                default_value: Some("true".to_string()),
                validation_pattern: None,
                description: "Enable adaptive cache management".to_string(),
                allowed_values: None,
            },
        ];

        self.register_config_schema("", schema_registry_configs);
        self.register_config_schema("", cache_configs);
    }
}

impl EnvironmentResolver {
    /// Create a new environment resolver
    pub fn new() -> Self {
        Self {
            env_cache: HashMap::new(),
            use_system_env: true,
        }
    }

    /// Resolve environment variables in a configuration value
    pub fn resolve(&self, value: &str) -> WithClauseResult<String> {
        let mut result = value.to_string();
        
        // Simple pattern matching for ${VARIABLE_NAME} or ${VARIABLE_NAME:-default_value}
        let mut start = 0;
        while let Some(var_start) = result[start..].find("${") {
            let abs_var_start = start + var_start;
            if let Some(var_end) = result[abs_var_start..].find('}') {
                let abs_var_end = abs_var_start + var_end;
                let var_spec = &result[abs_var_start + 2..abs_var_end];
                
                let (var_name, default_value) = if let Some(pos) = var_spec.find(":-") {
                    (&var_spec[..pos], Some(&var_spec[pos+2..]))
                } else {
                    (var_spec, None)
                };
                
                let resolved_value = if let Ok(env_value) = std::env::var(var_name) {
                    env_value
                } else if let Some(default) = default_value {
                    default.to_string()
                } else {
                    return Err(WithClauseError::EnvironmentError {
                        variable: var_name.to_string(),
                    });
                };
                
                result.replace_range(abs_var_start..abs_var_end + 1, &resolved_value);
                start = abs_var_start + resolved_value.len();
            } else {
                start = abs_var_start + 2;
            }
        }
        
        Ok(result)
    }
}

impl Default for WithClauseParser {
    fn default() -> Self {
        Self::new()
    }
}

// Error Display implementations
impl std::fmt::Display for WithClauseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WithClauseError::SyntaxError { message, position } => {
                write!(f, "Syntax error at position {}: {}", position, message)
            }
            WithClauseError::UnknownKey { key, suggestions } => {
                write!(f, "Unknown configuration key '{}'. Did you mean: {:?}?", key, suggestions)
            }
            WithClauseError::InvalidValue { key, value, expected } => {
                write!(f, "Invalid value '{}' for key '{}'. Expected: {:?}", value, key, expected)
            }
            WithClauseError::MissingRequired { key } => {
                write!(f, "Required configuration key '{}' is missing", key)
            }
            WithClauseError::ValidationError { key, message } => {
                write!(f, "Validation error for key '{}': {}", key, message)
            }
            WithClauseError::EnvironmentError { variable } => {
                write!(f, "Environment variable '{}' not found", variable)
            }
            WithClauseError::UrlError { url, reason } => {
                write!(f, "Invalid URL '{}': {}", url, reason)
            }
            WithClauseError::PathError { path, reason } => {
                write!(f, "Invalid path '{}': {}", path, reason)
            }
        }
    }
}

impl std::error::Error for WithClauseError {}

impl From<WithClauseError> for ConfigError {
    fn from(_error: WithClauseError) -> Self {
        // Convert to a ParseError - simplified for now
        use crate::ferris::sql::config::connection_string::ParseError as ConnParseError;
        ConfigError::ParseError(ConnParseError::MissingScheme)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_with_clause_parsing() {
        let parser = WithClauseParser::new();
        let with_clause = r#"'schema.registry.url' = 'http://localhost:8081', 'cache.l1.max_entries' = '200'"#;
        
        let result = parser.parse_with_clause(with_clause);
        assert!(result.is_ok());
        
        let config = result.unwrap();
        assert_eq!(config.raw_config.get("schema.registry.url"), Some(&"http://localhost:8081".to_string()));
        assert_eq!(config.raw_config.get("cache.l1.max_entries"), Some(&"200".to_string()));
    }

    #[test]
    fn test_environment_variable_resolution() {
        std::env::set_var("TEST_REGISTRY_URL", "http://test-registry:8081");
        
        let parser = WithClauseParser::new();
        let with_clause = r#"'schema.registry.url' = '${TEST_REGISTRY_URL}'"#;
        
        let result = parser.parse_with_clause(with_clause);
        assert!(result.is_ok());
        
        let config = result.unwrap();
        assert_eq!(config.raw_config.get("schema.registry.url"), Some(&"http://test-registry:8081".to_string()));
    }

    #[test]
    fn test_invalid_configuration_key() {
        let parser = WithClauseParser::new();
        let with_clause = r#"'invalid.key' = 'value'"#;
        
        let result = parser.parse_with_clause(with_clause);
        // Should succeed but generate warnings for unknown keys
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_validation() {
        let parser = WithClauseParser::new();
        let with_clause = r#"'cache.l1.max_entries' = 'not_a_number'"#;
        
        let result = parser.parse_with_clause(with_clause);
        assert!(result.is_err());
        
        match result.unwrap_err() {
            WithClauseError::InvalidValue { key, .. } => {
                assert_eq!(key, "cache.l1.max_entries");
            }
            _ => panic!("Expected InvalidValue error"),
        }
    }
}