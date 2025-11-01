//! Environment-based Configuration
//!
//! This module provides environment variable and file-based configuration support
//! for Velostream. It supports multiple configuration sources with priority
//! ordering and automatic type conversion.
//!
//! ## Features
//!
//! - **Environment Variables**: Load configuration from environment variables
//! - **Configuration Files**: Support for JSON, YAML, and TOML configuration files
//! - **Priority System**: Override configuration with environment-specific values
//! - **Template Expansion**: Variable substitution in configuration values
//! - **Type Conversion**: Automatic conversion of string values to appropriate types

use crate::velostream::sql::config::{ConfigError, DataSourceConfig};
use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs;
use std::path::Path;

/// Configuration source type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConfigSource {
    /// Configuration created in code
    Code,

    /// Configuration from URI parsing
    Uri(String),

    /// Configuration from environment variables
    Environment,

    /// Configuration from file
    File(String),

    /// Configuration from multiple sources (merged)
    Merged(Vec<ConfigSource>),

    /// Configuration from template
    Template(String),
}

/// Environment-based configuration loader
#[derive(Debug, Clone)]
pub struct EnvironmentConfig {
    /// Environment variable prefix (default: "VELO_")
    pub prefix: String,

    /// Configuration file paths to try
    pub config_files: Vec<String>,

    /// Whether to allow missing configuration files
    pub allow_missing_files: bool,

    /// Environment variable mappings
    pub env_mappings: HashMap<String, String>,

    /// Default values
    pub defaults: HashMap<String, String>,

    /// Template variables for expansion
    pub template_vars: HashMap<String, String>,
}

impl EnvironmentConfig {
    /// Create new environment configuration with defaults
    pub fn new() -> Self {
        Self {
            prefix: "VELO_".to_string(),
            config_files: vec![
                "./velostream.toml".to_string(),
                "./config/velostream.yaml".to_string(),
                "~/.config/velostream/config.json".to_string(),
                "/etc/velostream/config.yaml".to_string(),
            ],
            allow_missing_files: true,
            env_mappings: Self::default_env_mappings(),
            defaults: Self::default_values(),
            template_vars: Self::default_template_vars(),
        }
    }

    /// Create configuration with custom prefix
    pub fn with_prefix<S: Into<String>>(prefix: S) -> Self {
        let mut config = Self::new();
        config.prefix = prefix.into();
        config
    }

    /// Add configuration file path
    pub fn add_config_file<S: Into<String>>(&mut self, path: S) {
        self.config_files.insert(0, path.into());
    }

    /// Set environment variable mapping
    pub fn set_env_mapping<K: Into<String>, V: Into<String>>(&mut self, env_var: K, config_key: V) {
        self.env_mappings.insert(env_var.into(), config_key.into());
    }

    /// Set default value
    pub fn set_default<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) {
        self.defaults.insert(key.into(), value.into());
    }

    /// Set template variable
    pub fn set_template_var<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) {
        self.template_vars.insert(key.into(), value.into());
    }

    /// Load configuration for a specific scheme
    pub fn load_config(&self, scheme: &str) -> Result<DataSourceConfig, ConfigError> {
        let mut config = DataSourceConfig::new(scheme);
        config.source = ConfigSource::Environment;

        // 1. Apply defaults
        self.apply_defaults(&mut config);

        // 2. Load from configuration files
        self.load_from_files(&mut config, scheme)?;

        // 3. Override with environment variables
        self.load_from_environment(&mut config, scheme)?;

        // 4. Expand template variables
        self.expand_templates(&mut config)?;

        Ok(config)
    }

    /// Load all configurations from environment
    pub fn load_all_configs(&self) -> Result<HashMap<String, DataSourceConfig>, ConfigError> {
        let mut configs = HashMap::new();

        // Try to detect schemes from environment variables and files
        let detected_schemes = self.detect_schemes()?;

        for scheme in detected_schemes {
            let config = self.load_config(&scheme)?;
            configs.insert(scheme, config);
        }

        Ok(configs)
    }

    /// Apply default values
    fn apply_defaults(&self, config: &mut DataSourceConfig) {
        for (key, value) in &self.defaults {
            if key == "timeout_ms" {
                if let Ok(timeout) = value.parse::<u64>() {
                    config.timeout_ms = Some(timeout);
                }
            } else if key == "max_retries" {
                if let Ok(retries) = value.parse::<u32>() {
                    config.max_retries = Some(retries);
                }
            } else {
                config.set_parameter(key, value);
            }
        }
    }

    /// Load configuration from files
    fn load_from_files(
        &self,
        config: &mut DataSourceConfig,
        scheme: &str,
    ) -> Result<(), ConfigError> {
        for file_path in &self.config_files {
            let expanded_path = self.expand_path(file_path);

            if !Path::new(&expanded_path).exists() {
                if self.allow_missing_files {
                    continue;
                } else {
                    return Err(ConfigError::EnvironmentError(format!(
                        "Configuration file not found: {}",
                        expanded_path
                    )));
                }
            }

            match self.load_config_file(&expanded_path, config, scheme) {
                Ok(_) => {
                    config.source = ConfigSource::Merged(vec![
                        config.source.clone(),
                        ConfigSource::File(expanded_path.clone()),
                    ]);
                }
                Err(e) if self.allow_missing_files => {
                    eprintln!(
                        "Warning: Failed to load config file {}: {}",
                        expanded_path, e
                    );
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    /// Load specific configuration file
    fn load_config_file(
        &self,
        file_path: &str,
        config: &mut DataSourceConfig,
        scheme: &str,
    ) -> Result<(), ConfigError> {
        let content = fs::read_to_string(file_path).map_err(|e| {
            ConfigError::EnvironmentError(format!(
                "Failed to read config file {}: {}",
                file_path, e
            ))
        })?;

        let file_config = if file_path.ends_with(".json") {
            self.parse_json_config(&content, scheme)?
        } else if file_path.ends_with(".yaml") || file_path.ends_with(".yml") {
            self.parse_yaml_config(&content, scheme)?
        } else if file_path.ends_with(".toml") {
            self.parse_toml_config(&content, scheme)?
        } else {
            return Err(ConfigError::EnvironmentError(format!(
                "Unsupported config file format: {}",
                file_path
            )));
        };

        // Merge configurations
        self.merge_config(config, &file_config);

        Ok(())
    }

    /// Parse JSON configuration (simplified implementation)
    fn parse_json_config(
        &self,
        content: &str,
        scheme: &str,
    ) -> Result<HashMap<String, String>, ConfigError> {
        // This is a simplified JSON parser for basic key-value pairs
        // In a real implementation, you'd use serde_json
        let mut config = HashMap::new();

        for line in content.lines() {
            let line = line.trim();
            if line.starts_with('"') && line.contains(':') {
                if let Some(colon_pos) = line.find(':') {
                    let key_part = &line[..colon_pos];
                    let value_part = &line[colon_pos + 1..];

                    let key = key_part.trim_matches(|c| c == '"' || c == ' ');
                    let value = value_part.trim_matches(|c| c == '"' || c == ' ' || c == ',');

                    if key.starts_with(scheme) || Self::is_global_key(key) {
                        config.insert(key.to_string(), value.to_string());
                    }
                }
            }
        }

        Ok(config)
    }

    /// Parse YAML configuration (simplified implementation)
    fn parse_yaml_config(
        &self,
        content: &str,
        scheme: &str,
    ) -> Result<HashMap<String, String>, ConfigError> {
        // This is a simplified YAML parser for basic key-value pairs
        // In a real implementation, you'd use serde_yaml
        let mut config = HashMap::new();
        let mut in_scheme_section = false;

        for line in content.lines() {
            let line = line.trim();

            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if line == format!("{}:", scheme) {
                in_scheme_section = true;
                continue;
            }

            if line.ends_with(':') && !line.starts_with(' ') {
                in_scheme_section = line.trim_end_matches(':') == scheme;
                continue;
            }

            if line.contains(':') && (in_scheme_section || Self::is_global_key(line)) {
                if let Some(colon_pos) = line.find(':') {
                    let key = line[..colon_pos].trim();
                    let value = line[colon_pos + 1..].trim();

                    config.insert(key.to_string(), value.to_string());
                }
            }
        }

        Ok(config)
    }

    /// Parse TOML configuration (simplified implementation)
    fn parse_toml_config(
        &self,
        content: &str,
        scheme: &str,
    ) -> Result<HashMap<String, String>, ConfigError> {
        // This is a simplified TOML parser for basic key-value pairs
        // In a real implementation, you'd use toml crate
        let mut config = HashMap::new();
        let mut in_scheme_section = false;

        for line in content.lines() {
            let line = line.trim();

            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if line.starts_with('[') && line.ends_with(']') {
                let section = &line[1..line.len() - 1];
                in_scheme_section = section == scheme;
                continue;
            }

            if line.contains('=') && (in_scheme_section || Self::is_global_key(line)) {
                if let Some(eq_pos) = line.find('=') {
                    let key = line[..eq_pos].trim();
                    let value = line[eq_pos + 1..].trim().trim_matches('"');

                    config.insert(key.to_string(), value.to_string());
                }
            }
        }

        Ok(config)
    }

    /// Load configuration from environment variables
    fn load_from_environment(
        &self,
        config: &mut DataSourceConfig,
        scheme: &str,
    ) -> Result<(), ConfigError> {
        // Load direct environment mappings
        for (env_var, config_key) in &self.env_mappings {
            if let Ok(value) = env::var(env_var) {
                self.set_config_value(config, config_key, &value);
            }
        }

        // Load prefixed environment variables
        let scheme_prefix = format!("{}{}_{}", self.prefix, scheme.to_uppercase(), "");
        let global_prefix = self.prefix.to_string();

        for (key, value) in env::vars() {
            if let Some(config_key) = key.strip_prefix(&scheme_prefix) {
                self.set_config_value(config, &config_key.to_lowercase(), &value);
            } else if let Some(config_key) = key.strip_prefix(&global_prefix) {
                if Self::is_global_key(&config_key.to_lowercase()) {
                    self.set_config_value(config, &config_key.to_lowercase(), &value);
                }
            }
        }

        // Update source to reflect environment loading
        config.source =
            ConfigSource::Merged(vec![config.source.clone(), ConfigSource::Environment]);

        Ok(())
    }

    /// Expand template variables in configuration
    fn expand_templates(&self, config: &mut DataSourceConfig) -> Result<(), ConfigError> {
        // Expand template variables in parameters
        let mut expanded_params = HashMap::new();

        for (key, value) in &config.parameters {
            let expanded_value = self.expand_string(value)?;
            expanded_params.insert(key.clone(), expanded_value);
        }

        config.parameters = expanded_params;

        // Expand other string fields
        if let Some(host) = &config.host {
            config.host = Some(self.expand_string(host)?);
        }

        if let Some(path) = &config.path {
            config.path = Some(self.expand_string(path)?);
        }

        Ok(())
    }

    /// Expand template variables in a string
    pub fn expand_string(&self, s: &str) -> Result<String, ConfigError> {
        let mut result = s.to_string();

        // Replace ${VAR} and $VAR patterns
        for (var_name, var_value) in &self.template_vars {
            let patterns = [format!("${{{}}}", var_name), format!("${}", var_name)];

            for pattern in &patterns {
                result = result.replace(pattern, var_value);
            }
        }

        // Replace environment variables
        while result.contains("${") {
            if let Some(start) = result.find("${") {
                if let Some(end) = result[start..].find('}') {
                    let var_name = &result[start + 2..start + end];
                    let var_value =
                        env::var(var_name).unwrap_or_else(|_| format!("${{{}}}", var_name)); // Keep unexpanded if not found

                    result.replace_range(start..start + end + 1, &var_value);
                } else {
                    break; // Invalid syntax, stop processing
                }
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Set configuration value with type conversion
    pub fn set_config_value(&self, config: &mut DataSourceConfig, key: &str, value: &str) {
        match key {
            "host" => config.host = Some(value.to_string()),
            "port" => {
                if let Ok(port) = value.parse::<u16>() {
                    config.port = Some(port);
                }
            }
            "path" => config.path = Some(value.to_string()),
            "timeout_ms" => {
                if let Ok(timeout) = value.parse::<u64>() {
                    config.timeout_ms = Some(timeout);
                }
            }
            "max_retries" => {
                if let Ok(retries) = value.parse::<u32>() {
                    config.max_retries = Some(retries);
                }
            }
            _ => {
                config.set_parameter(key, value);
            }
        }
    }

    /// Merge two configurations
    fn merge_config(&self, target: &mut DataSourceConfig, source: &HashMap<String, String>) {
        for (key, value) in source {
            self.set_config_value(target, key, value);
        }
    }

    /// Detect schemes from environment and files
    fn detect_schemes(&self) -> Result<Vec<String>, ConfigError> {
        let mut schemes = std::collections::HashSet::new();

        // Detect from environment variables
        let prefix_len = self.prefix.len();
        for (key, _) in env::vars() {
            if key.starts_with(&self.prefix) && key.len() > prefix_len {
                let remainder = &key[prefix_len..];
                if let Some(underscore_pos) = remainder.find('_') {
                    let scheme = remainder[..underscore_pos].to_lowercase();
                    schemes.insert(scheme);
                }
            }
        }

        // Detect from configuration files
        for file_path in &self.config_files {
            let expanded_path = self.expand_path(file_path);
            if Path::new(&expanded_path).exists() {
                if let Ok(content) = fs::read_to_string(&expanded_path) {
                    schemes.extend(self.detect_schemes_in_content(&content));
                }
            }
        }

        // Add common schemes if none detected
        if schemes.is_empty() {
            schemes.extend(
                ["kafka", "s3", "file", "postgresql"]
                    .iter()
                    .map(|s| s.to_string()),
            );
        }

        Ok(schemes.into_iter().collect())
    }

    /// Detect schemes in configuration file content
    pub fn detect_schemes_in_content(&self, content: &str) -> Vec<String> {
        let mut schemes = Vec::new();
        let common_schemes = ["kafka", "s3", "file", "postgresql", "clickhouse", "http"];

        for scheme in &common_schemes {
            if content.contains(scheme) {
                schemes.push(scheme.to_string());
            }
        }

        schemes
    }

    /// Check if a key is a global configuration key
    pub fn is_global_key(key: &str) -> bool {
        matches!(
            key,
            "timeout_ms" | "max_retries" | "log_level" | "debug" | "profile"
        )
    }

    /// Expand file path (handle ~ and environment variables)
    pub fn expand_path(&self, path: &str) -> String {
        let mut expanded = path.to_string();

        // Expand home directory
        if expanded.starts_with("~/") {
            if let Ok(home) = env::var("HOME") {
                expanded = expanded.replacen("~/", &format!("{}/", home), 1);
            }
        }

        // Expand environment variables
        if let Ok(expanded_with_env) = self.expand_string(&expanded) {
            expanded = expanded_with_env;
        }

        expanded
    }

    /// Default environment variable mappings
    fn default_env_mappings() -> HashMap<String, String> {
        let mut mappings = HashMap::new();

        // Database URL patterns
        mappings.insert("DATABASE_URL".to_string(), "uri".to_string());
        mappings.insert("KAFKA_BROKERS".to_string(), "brokers".to_string());
        mappings.insert("AWS_REGION".to_string(), "region".to_string());
        mappings.insert("AWS_ACCESS_KEY_ID".to_string(), "access_key".to_string());
        mappings.insert(
            "AWS_SECRET_ACCESS_KEY".to_string(),
            "secret_key".to_string(),
        );

        mappings
    }

    /// Default configuration values
    fn default_values() -> HashMap<String, String> {
        let mut defaults = HashMap::new();

        defaults.insert("timeout_ms".to_string(), "30000".to_string());
        defaults.insert("max_retries".to_string(), "3".to_string());

        defaults
    }

    /// Default template variables
    fn default_template_vars() -> HashMap<String, String> {
        let mut vars = HashMap::new();

        // Common environment variables
        if let Ok(home) = env::var("HOME") {
            vars.insert("HOME".to_string(), home);
        }

        if let Ok(user) = env::var("USER") {
            vars.insert("USER".to_string(), user);
        }

        vars
    }
}

impl Default for EnvironmentConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigSource::Code => write!(f, "Code"),
            ConfigSource::Uri(uri) => write!(f, "URI({})", uri),
            ConfigSource::Environment => write!(f, "Environment"),
            ConfigSource::File(path) => write!(f, "File({})", path),
            ConfigSource::Merged(sources) => {
                let source_strs: Vec<String> = sources.iter().map(|s| s.to_string()).collect();
                write!(f, "Merged({})", source_strs.join(", "))
            }
            ConfigSource::Template(name) => write!(f, "Template({})", name),
        }
    }
}
