//! YAML Configuration Loader with Extends and Variable Substitution Support
//!
//! This module provides YAML configuration loading with inheritance support
//! through the `extends` keyword and environment variable substitution,
//! enabling DRY configuration management and 12-factor app style configuration.
//!
//! ## Features
//!
//! - **Inheritance**: Support for `extends: base_config.yaml` syntax
//! - **Variable Substitution**: Support for `${ENV_VAR:default}` syntax
//! - **Merge Strategy**: Base configs are merged with derived configs
//! - **Circular Detection**: Prevents infinite inheritance loops
//! - **Path Resolution**: Resolves relative paths from config file locations
//! - **Validation**: Validates merged configuration structure
//!
//! ## Variable Substitution
//!
//! String values can contain environment variable references:
//! - `${VAR_NAME}` - Replaced with env var value, empty string if not set
//! - `${VAR_NAME:default}` - Replaced with env var value, or default if not set
//!
//! ```yaml
//! datasource:
//!   consumer_config:
//!     # Explicit: uses env var VELOSTREAM_KAFKA_BROKERS, defaults to localhost:9092
//!     bootstrap.servers: "${VELOSTREAM_KAFKA_BROKERS:localhost:9092}"
//! ```
//!
//! ## Usage
//!
//! ```yaml
//! # common_kafka.yaml
//! datasource:
//!   type: kafka
//!   bootstrap_servers: "broker:9092"
//!   schema:
//!     format: avro
//!     registry_url: "http://schema-registry:8081"
//!
//! # market_data.yaml
//! extends: common_kafka.yaml
//! topic:
//!   name: "market_data"
//!   partitions: 12
//! schema:
//!   key.field: symbol
//! ```

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Substitute environment variables in a string value.
///
/// Supports two syntaxes:
/// - `${VAR_NAME}` - replaced with env var value, or empty string if not set
/// - `${VAR_NAME:default}` - replaced with env var value, or default if not set
///
/// Multiple substitutions in a single string are supported.
/// Substitute environment variables in a string value at runtime.
///
/// This function is public to allow deferred/lazy substitution - call it at the point
/// where you need the final value, not at config load time. This is important for
/// testcontainers and other scenarios where env vars are set after config loading.
pub fn substitute_env_vars(value: &str) -> String {
    // Match ${VAR_NAME} or ${VAR_NAME:default}
    // The regex captures: VAR_NAME and optionally :default
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}").unwrap();

    re.replace_all(value, |caps: &regex::Captures| {
        let var_name = &caps[1];
        let default = caps.get(2).map(|m| m.as_str()).unwrap_or("");

        match std::env::var(var_name) {
            Ok(val) => val,
            Err(_) => default.to_string(),
        }
    })
    .to_string()
}

/// Recursively substitute environment variables in all string values in a YAML structure
fn substitute_env_vars_recursive(value: &mut serde_yaml::Value) {
    match value {
        serde_yaml::Value::String(s) => {
            if s.contains("${") {
                *s = substitute_env_vars(s);
            }
        }
        serde_yaml::Value::Mapping(map) => {
            for (_, v) in map.iter_mut() {
                substitute_env_vars_recursive(v);
            }
        }
        serde_yaml::Value::Sequence(seq) => {
            for v in seq.iter_mut() {
                substitute_env_vars_recursive(v);
            }
        }
        _ => {} // Numbers, booleans, null - no substitution needed
    }
}

/// YAML configuration loader with extends support
pub struct YamlConfigLoader {
    /// Base directory for resolving relative paths
    base_dir: PathBuf,
    /// Cache of loaded configurations to prevent re-loading
    config_cache: HashMap<PathBuf, RawYamlConfig>,
    /// Track inheritance chain to detect circular dependencies
    loading_stack: HashSet<PathBuf>,
}

/// Raw YAML configuration before processing extends
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawYamlConfig {
    /// Optional inheritance from another config file
    extends: Option<String>,
    /// Rest of the configuration as generic value
    #[serde(flatten)]
    config: serde_yaml::Value,
}

/// Processed configuration after resolving extends
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedYamlConfig {
    /// Complete configuration with inheritance resolved
    pub config: serde_yaml::Value,
    /// Metadata about the resolution process
    pub metadata: ConfigResolutionMetadata,
}

/// Metadata about configuration resolution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResolutionMetadata {
    /// Original file that was loaded
    pub source_file: PathBuf,
    /// Chain of inherited files (base to derived)
    pub inheritance_chain: Vec<PathBuf>,
    /// Total number of merged configurations
    pub merge_count: usize,
    /// Whether any circular dependencies were detected
    pub has_circular_dependency: bool,
}

/// Errors that can occur during YAML config loading
#[derive(Error, Debug)]
pub enum YamlConfigError {
    #[error("File not found: {path}")]
    FileNotFound { path: PathBuf },

    #[error("YAML parsing error in {file}: {error}")]
    ParseError {
        file: PathBuf,
        error: serde_yaml::Error,
    },

    #[error("IO error reading {file}: {error}")]
    IoError {
        file: PathBuf,
        error: std::io::Error,
    },

    #[error("Circular dependency detected: {chain:?}")]
    CircularDependency { chain: Vec<PathBuf> },

    #[error("Invalid extends path '{path}' in {file}")]
    InvalidExtendsPath { path: String, file: PathBuf },

    #[error("Config merge error: {message}")]
    MergeError { message: String },
}

impl YamlConfigLoader {
    /// Create a new YAML config loader
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            config_cache: HashMap::new(),
            loading_stack: HashSet::new(),
        }
    }

    /// Load and resolve a YAML configuration file with extends support
    pub fn load_config<P: AsRef<Path>>(
        &mut self,
        file_path: P,
    ) -> Result<ResolvedYamlConfig, YamlConfigError> {
        let path = self.resolve_path(file_path.as_ref())?;

        // Check if file exists before proceeding
        if !path.exists() {
            return Err(YamlConfigError::FileNotFound { path: path.clone() });
        }

        self.load_config_internal(&path)
    }

    /// Internal recursive config loading with circular dependency detection
    fn load_config_internal(&mut self, path: &Path) -> Result<ResolvedYamlConfig, YamlConfigError> {
        let canonical_path = path.canonicalize().map_err(|e| YamlConfigError::IoError {
            file: path.to_path_buf(),
            error: e,
        })?;

        // Check for circular dependency
        if self.loading_stack.contains(&canonical_path) {
            let mut chain: Vec<PathBuf> = self.loading_stack.iter().cloned().collect();
            chain.push(canonical_path);
            return Err(YamlConfigError::CircularDependency { chain });
        }

        // Check cache first
        if let Some(cached_raw) = self.config_cache.get(&canonical_path).cloned() {
            return self.resolve_extends(&cached_raw, &canonical_path);
        }

        // Add to loading stack
        self.loading_stack.insert(canonical_path.clone());

        // Load raw configuration
        let raw_config = self.load_raw_config(&canonical_path)?;

        // Cache the raw config
        self.config_cache
            .insert(canonical_path.clone(), raw_config.clone());

        // Resolve extends
        let result = self.resolve_extends(&raw_config, &canonical_path);

        // Remove from loading stack
        self.loading_stack.remove(&canonical_path);

        result
    }

    /// Load raw YAML configuration without processing extends
    fn load_raw_config(&self, path: &Path) -> Result<RawYamlConfig, YamlConfigError> {
        let content = fs::read_to_string(path).map_err(|e| YamlConfigError::IoError {
            file: path.to_path_buf(),
            error: e,
        })?;

        serde_yaml::from_str(&content).map_err(|e| YamlConfigError::ParseError {
            file: path.to_path_buf(),
            error: e,
        })
    }

    /// Resolve extends inheritance chain
    fn resolve_extends(
        &mut self,
        raw_config: &RawYamlConfig,
        current_path: &Path,
    ) -> Result<ResolvedYamlConfig, YamlConfigError> {
        let mut inheritance_chain = Vec::new();
        let mut merged_config = raw_config.config.clone();
        let mut merge_count = 1;

        if let Some(extends_path) = &raw_config.extends {
            let base_path = self.resolve_extends_path(extends_path, current_path)?;
            let base_resolved = self.load_config_internal(&base_path)?;

            // Merge base config with current config
            merged_config = self.merge_configs(&base_resolved.config, &merged_config)?;

            // Combine inheritance chains
            inheritance_chain.extend(base_resolved.metadata.inheritance_chain);
            merge_count += base_resolved.metadata.merge_count;
        }

        inheritance_chain.push(current_path.to_path_buf());

        // NOTE: Environment variable substitution is now deferred to runtime
        // This is critical for testcontainers integration where env vars are set AFTER config loading
        // The substitute_env_vars() function is called when values are actually used
        // substitute_env_vars_recursive(&mut merged_config);

        Ok(ResolvedYamlConfig {
            config: merged_config,
            metadata: ConfigResolutionMetadata {
                source_file: current_path.to_path_buf(),
                inheritance_chain,
                merge_count,
                has_circular_dependency: false,
            },
        })
    }

    /// Resolve extends path relative to current config file
    fn resolve_extends_path(
        &self,
        extends_path: &str,
        current_path: &Path,
    ) -> Result<PathBuf, YamlConfigError> {
        let extends_path = Path::new(extends_path);

        if extends_path.is_absolute() {
            Ok(extends_path.to_path_buf())
        } else {
            // Relative to current config file's directory
            if let Some(parent) = current_path.parent() {
                Ok(parent.join(extends_path))
            } else {
                // Fallback to base directory
                Ok(self.base_dir.join(extends_path))
            }
        }
    }

    /// Resolve file path relative to base directory
    fn resolve_path(&self, path: &Path) -> Result<PathBuf, YamlConfigError> {
        if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            Ok(self.base_dir.join(path))
        }
    }

    /// Merge two YAML configurations (base config is overridden by derived config)
    fn merge_configs(
        &self,
        base: &serde_yaml::Value,
        derived: &serde_yaml::Value,
    ) -> Result<serde_yaml::Value, YamlConfigError> {
        match (base, derived) {
            // Both are mappings - merge recursively
            (serde_yaml::Value::Mapping(base_map), serde_yaml::Value::Mapping(derived_map)) => {
                let mut merged = base_map.clone();

                for (key, derived_value) in derived_map {
                    if let Some(base_value) = base_map.get(key) {
                        // Key exists in both - merge recursively
                        merged.insert(key.clone(), self.merge_configs(base_value, derived_value)?);
                    } else {
                        // Key only in derived - use derived value
                        merged.insert(key.clone(), derived_value.clone());
                    }
                }

                Ok(serde_yaml::Value::Mapping(merged))
            }

            // Both are sequences - derived overrides base completely
            (serde_yaml::Value::Sequence(_), serde_yaml::Value::Sequence(_)) => Ok(derived.clone()),

            // Any other case - derived overrides base
            _ => Ok(derived.clone()),
        }
    }

    /// Clear the config cache (useful for testing or reloading)
    pub fn clear_cache(&mut self) {
        self.config_cache.clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, usize) {
        (self.config_cache.len(), self.loading_stack.len())
    }
}

/// Convenience function to load a single config file
pub fn load_yaml_config<P: AsRef<Path>>(
    file_path: P,
) -> Result<ResolvedYamlConfig, YamlConfigError> {
    let path = file_path.as_ref();

    // If path is relative, use current working directory as base
    // If path is absolute, use its parent directory as base
    let (base_dir, config_path) = if path.is_absolute() {
        let base_dir = path.parent().unwrap_or_else(|| Path::new("/"));
        (base_dir, path)
    } else {
        // For relative paths, use current directory as base and keep path as-is
        (Path::new("."), path)
    };

    let mut loader = YamlConfigLoader::new(base_dir);
    loader.load_config(config_path)
}

/// Load a config file with a custom base directory for resolving relative paths
///
/// This is useful when config_file paths in SQL are relative to the SQL file's directory,
/// not the current working directory.
///
/// # Arguments
/// * `file_path` - The config file path (can be relative or absolute)
/// * `base_dir` - The base directory for resolving relative paths (typically the SQL file's directory)
///
/// # Example
/// ```ignore
/// // If SQL file is at /app/sql/app.sql and references '../configs/kafka.yaml'
/// // The base_dir should be /app/sql/ so the config resolves to /app/configs/kafka.yaml
/// let config = load_yaml_config_with_base("../configs/kafka.yaml", "/app/sql/")?;
/// ```
pub fn load_yaml_config_with_base<P: AsRef<Path>, B: AsRef<Path>>(
    file_path: P,
    base_dir: B,
) -> Result<ResolvedYamlConfig, YamlConfigError> {
    let path = file_path.as_ref();
    let base = base_dir.as_ref();

    // If path is absolute, ignore base_dir and use path's parent
    if path.is_absolute() {
        let parent = path.parent().unwrap_or_else(|| Path::new("/"));
        let mut loader = YamlConfigLoader::new(parent);
        loader.load_config(path)
    } else {
        // For relative paths, use the provided base directory
        // The loader's resolve_path will join base_dir + file_path
        let mut loader = YamlConfigLoader::new(base);
        loader.load_config(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_simple_config_loading() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("simple.yaml");

        fs::write(
            &config_path,
            r#"
name: "test"
value: 42
nested:
  key: "value"
"#,
        )
        .unwrap();

        let mut loader = YamlConfigLoader::new(temp_dir.path());
        let result = loader.load_config(&config_path).unwrap();

        assert_eq!(result.metadata.inheritance_chain.len(), 1);
        assert_eq!(result.metadata.merge_count, 1);
    }

    #[test]
    fn test_extends_inheritance() {
        let temp_dir = TempDir::new().unwrap();

        // Create base config
        let base_path = temp_dir.path().join("base.yaml");
        fs::write(
            &base_path,
            r#"
datasource:
  type: kafka
  host: "localhost"
  port: 9092
schema:
  format: avro
"#,
        )
        .unwrap();

        // Create derived config
        let derived_path = temp_dir.path().join("derived.yaml");
        fs::write(
            &derived_path,
            r#"
extends: base.yaml
topic:
  name: "test_topic"
  partitions: 4
schema:
  key.field: "id"
"#,
        )
        .unwrap();

        let mut loader = YamlConfigLoader::new(temp_dir.path());
        let result = loader.load_config(&derived_path).unwrap();

        assert_eq!(result.metadata.inheritance_chain.len(), 2);
        assert_eq!(result.metadata.merge_count, 2);

        // Verify merged configuration contains values from both files
        let config = &result.config;
        assert_eq!(config["datasource"]["type"], "kafka");
        assert_eq!(config["datasource"]["host"], "localhost");
        assert_eq!(config["topic"]["name"], "test_topic");
        assert_eq!(config["schema"]["format"], "avro");
        assert_eq!(config["schema"]["key.field"], "id");
    }

    #[test]
    fn test_circular_dependency_detection() {
        let temp_dir = TempDir::new().unwrap();

        let a_path = temp_dir.path().join("a.yaml");
        fs::write(
            &a_path,
            r#"
extends: b.yaml
value: "a"
"#,
        )
        .unwrap();

        let b_path = temp_dir.path().join("b.yaml");
        fs::write(
            &b_path,
            r#"
extends: a.yaml
value: "b"
"#,
        )
        .unwrap();

        let mut loader = YamlConfigLoader::new(temp_dir.path());
        let result = loader.load_config(&a_path);

        assert!(matches!(
            result,
            Err(YamlConfigError::CircularDependency { .. })
        ));
    }

    #[test]
    fn test_env_var_substitution_with_default() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("env_test.yaml");

        fs::write(
            &config_path,
            r#"
datasource:
  consumer_config:
    bootstrap.servers: "${YAML_TEST_BROKERS:localhost:9092}"
    group.id: "${YAML_TEST_GROUP_ID:default-group}"
"#,
        )
        .unwrap();

        let mut loader = YamlConfigLoader::new(temp_dir.path());
        let result = loader.load_config(&config_path).unwrap();

        // Config loading preserves env var placeholders (substitution is deferred for testcontainers)
        // Substitution happens when values are actually used via substitute_env_vars()
        let raw_servers = result.config["datasource"]["consumer_config"]["bootstrap.servers"]
            .as_str()
            .unwrap();
        let raw_group = result.config["datasource"]["consumer_config"]["group.id"]
            .as_str()
            .unwrap();

        // Apply deferred substitution
        assert_eq!(substitute_env_vars(raw_servers), "localhost:9092");
        assert_eq!(substitute_env_vars(raw_group), "default-group");
    }

    #[test]
    fn test_env_var_substitution_from_env() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("env_test2.yaml");

        fs::write(
            &config_path,
            r#"
datasource:
  consumer_config:
    bootstrap.servers: "${YAML_TEST_BROKERS_2:localhost:9092}"
"#,
        )
        .unwrap();

        // Set env var (unsafe in Rust 2024 due to thread safety)
        // SAFETY: This test runs in isolation and we clean up after
        unsafe {
            std::env::set_var("YAML_TEST_BROKERS_2", "testcontainer:32789");
        }

        let mut loader = YamlConfigLoader::new(temp_dir.path());
        let result = loader.load_config(&config_path).unwrap();

        // Config preserves placeholders; substitution is deferred
        let raw_value = result.config["datasource"]["consumer_config"]["bootstrap.servers"]
            .as_str()
            .unwrap();

        // With env var set, deferred substitution should use env value
        assert_eq!(substitute_env_vars(raw_value), "testcontainer:32789");

        // Cleanup
        unsafe {
            std::env::remove_var("YAML_TEST_BROKERS_2");
        }
    }

    #[test]
    fn test_env_var_substitution_without_default() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("env_test3.yaml");

        fs::write(
            &config_path,
            r#"
datasource:
  optional_field: "${YAML_TEST_OPTIONAL}"
"#,
        )
        .unwrap();

        let mut loader = YamlConfigLoader::new(temp_dir.path());
        let result = loader.load_config(&config_path).unwrap();

        // Config preserves placeholders; substitution is deferred
        let raw_value = result.config["datasource"]["optional_field"]
            .as_str()
            .unwrap();

        // Without env var and no default, deferred substitution yields empty string
        assert_eq!(substitute_env_vars(raw_value), "");
    }

    #[test]
    fn test_env_var_substitution_multiple_in_string() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("env_test4.yaml");

        fs::write(
            &config_path,
            r#"
connection_string: "kafka://${YAML_HOST:localhost}:${YAML_PORT:9092}/topic"
"#,
        )
        .unwrap();

        let mut loader = YamlConfigLoader::new(temp_dir.path());
        let result = loader.load_config(&config_path).unwrap();

        // Config preserves placeholders; substitution is deferred
        let raw_value = result.config["connection_string"].as_str().unwrap();

        // Deferred substitution handles multiple vars in one string
        assert_eq!(
            substitute_env_vars(raw_value),
            "kafka://localhost:9092/topic"
        );
    }

    #[test]
    fn test_substitute_env_vars_function() {
        // Direct function tests
        assert_eq!(
            substitute_env_vars("${NONEXISTENT_VAR:default_value}"),
            "default_value"
        );
        assert_eq!(
            substitute_env_vars("prefix_${NONEXISTENT_VAR:middle}_suffix"),
            "prefix_middle_suffix"
        );
        assert_eq!(substitute_env_vars("no_vars_here"), "no_vars_here");
    }
}
