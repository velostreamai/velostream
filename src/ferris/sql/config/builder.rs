//! Configuration Builder with Fluent API
//!
//! This module provides an intuitive builder pattern for constructing data source
//! configurations. It offers type-safe configuration building with validation
//! and intelligent defaults.
//!
//! ## Features
//!
//! - **Fluent API**: Chainable method calls for intuitive configuration
//! - **Type Safety**: Compile-time validation of configuration structure
//! - **Intelligent Defaults**: Automatic defaults based on scheme
//! - **Validation Integration**: Built-in validation during construction
//! - **Template Support**: Pre-configured templates for common setups

use crate::ferris::sql::config::validation::ConfigValidatorRegistry;
use crate::ferris::sql::config::{ConfigSource, DataSourceConfig, ValidationError};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

/// Configuration builder with fluent API
#[derive(Debug)]
pub struct DataSourceConfigBuilder {
    /// Current configuration being built
    config: DataSourceConfig,

    /// Validation registry
    validator: Option<ConfigValidatorRegistry>,

    /// Whether to validate during build
    validate_on_build: bool,

    /// Template being used (if any)
    template: Option<String>,
}

/// Configuration builder error
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigurationError {
    /// Missing required field
    MissingRequired(String),

    /// Invalid value
    InvalidValue {
        field: String,
        value: String,
        reason: String,
    },

    /// Validation failed during build
    ValidationFailed(ValidationError),

    /// Template not found
    TemplateNotFound(String),

    /// Configuration conflict
    ConflictError {
        field1: String,
        field2: String,
        reason: String,
    },
}

impl DataSourceConfigBuilder {
    /// Create new builder
    pub fn new() -> Self {
        Self {
            config: DataSourceConfig::default(),
            validator: None,
            validate_on_build: true,
            template: None,
        }
    }

    /// Create builder with scheme
    pub fn with_scheme<S: Into<String>>(scheme: S) -> Self {
        let mut builder = Self::new();
        builder.config.scheme = scheme.into();
        builder
    }

    /// Create builder from existing configuration
    pub fn from_config(config: DataSourceConfig) -> Self {
        Self {
            config,
            validator: None,
            validate_on_build: true,
            template: None,
        }
    }

    /// Create builder from URI
    pub fn from_uri<S: Into<String>>(uri: S) -> Result<Self, ConfigurationError> {
        let uri = uri.into();
        let config =
            DataSourceConfig::from_uri(&uri).map_err(|e| ConfigurationError::InvalidValue {
                field: "uri".to_string(),
                value: uri,
                reason: e.to_string(),
            })?;

        Ok(Self::from_config(config))
    }

    /// Use a pre-configured template
    pub fn from_template<S: Into<String>>(template_name: S) -> Result<Self, ConfigurationError> {
        let template_name = template_name.into();
        let template = ConfigTemplate::get(&template_name)
            .ok_or_else(|| ConfigurationError::TemplateNotFound(template_name.clone()))?;

        let mut builder = Self::from_config(template.config);
        builder.template = Some(template_name);
        Ok(builder)
    }

    // === Core Configuration Methods ===

    /// Set the URI scheme
    pub fn scheme<S: Into<String>>(mut self, scheme: S) -> Self {
        self.config.scheme = scheme.into();
        self
    }

    /// Set the host
    pub fn host<S: Into<String>>(mut self, host: S) -> Self {
        self.config.host = Some(host.into());
        self
    }

    /// Set the port
    pub fn port(mut self, port: u16) -> Self {
        self.config.port = Some(port);
        self
    }

    /// Set the path
    pub fn path<S: Into<String>>(mut self, path: S) -> Self {
        self.config.path = Some(path.into());
        self
    }

    /// Set a parameter
    pub fn parameter<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.config.set_parameter(key, value);
        self
    }

    /// Set multiple parameters
    pub fn parameters<K, V>(mut self, params: HashMap<K, V>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in params {
            self.config.set_parameter(k, v);
        }
        self
    }

    /// Set timeout in milliseconds
    pub fn timeout_ms(mut self, timeout: u64) -> Self {
        self.config.timeout_ms = Some(timeout);
        self
    }

    /// Set timeout using Duration
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout_ms = Some(timeout.as_millis() as u64);
        self
    }

    /// Set maximum retry attempts
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config.max_retries = Some(retries);
        self
    }

    /// Set configuration source metadata
    pub fn source(mut self, source: ConfigSource) -> Self {
        self.config.source = source;
        self
    }

    // === Convenience Methods ===

    /// Set host and port together
    pub fn host_port<S: Into<String>>(self, host: S, port: u16) -> Self {
        self.host(host).port(port)
    }

    /// Set boolean parameter
    pub fn bool_parameter<K: Into<String>>(self, key: K, value: bool) -> Self {
        self.parameter(key, if value { "true" } else { "false" })
    }

    /// Set integer parameter
    pub fn int_parameter<K: Into<String>>(self, key: K, value: i64) -> Self {
        self.parameter(key, value.to_string())
    }

    /// Set float parameter
    pub fn float_parameter<K: Into<String>>(self, key: K, value: f64) -> Self {
        self.parameter(key, value.to_string())
    }

    // === Scheme-specific Builder Methods ===

    /// Kafka-specific configuration
    pub fn kafka(self) -> KafkaConfigBuilder {
        KafkaConfigBuilder::new(self.scheme("kafka"))
    }

    /// S3-specific configuration
    pub fn s3(self) -> S3ConfigBuilder {
        S3ConfigBuilder::new(self.scheme("s3"))
    }

    /// File-specific configuration
    pub fn file(self) -> FileConfigBuilder {
        FileConfigBuilder::new(self.scheme("file"))
    }

    /// PostgreSQL-specific configuration
    pub fn postgresql(self) -> PostgreSQLConfigBuilder {
        PostgreSQLConfigBuilder::new(self.scheme("postgresql"))
    }

    /// Set topic (convenience method for Kafka compatibility)
    pub fn topic<S: Into<String>>(self, topic: S) -> Self {
        self.path(format!("/{}", topic.into().trim_start_matches('/')))
    }

    // === Validation Configuration ===

    /// Enable validation during build (default: true)
    pub fn validate_on_build(mut self, validate: bool) -> Self {
        self.validate_on_build = validate;
        self
    }

    /// Set custom validator
    pub fn validator(mut self, validator: ConfigValidatorRegistry) -> Self {
        self.validator = Some(validator);
        self
    }

    // === Build Methods ===

    /// Build the configuration with validation
    pub fn build(mut self) -> Result<DataSourceConfig, ConfigurationError> {
        if self.validate_on_build {
            if self.validator.is_none() {
                self.validator = Some(ConfigValidatorRegistry::new());
            }

            if let Some(ref mut validator) = self.validator {
                validator
                    .validate(&self.config)
                    .map_err(ConfigurationError::ValidationFailed)?;
            }
        }

        self.config.mark_validated();
        Ok(self.config)
    }

    /// Build without validation
    pub fn build_unchecked(mut self) -> DataSourceConfig {
        self.config.clear_validation();
        self.config
    }

    /// Try to build, returning warnings on success
    pub fn try_build(
        mut self,
    ) -> Result<
        (
            DataSourceConfig,
            Vec<crate::ferris::sql::config::validation::ValidationWarning>,
        ),
        ConfigurationError,
    > {
        if self.validator.is_none() {
            self.validator = Some(ConfigValidatorRegistry::new());
        }

        let warnings = if let Some(ref mut validator) = self.validator {
            match validator.validate(&self.config) {
                Ok(warnings) => warnings,
                Err(err) => return Err(ConfigurationError::ValidationFailed(err)),
            }
        } else {
            vec![]
        };

        self.config.mark_validated();
        Ok((self.config, warnings))
    }

    /// Get current configuration (without building)
    pub fn current(&self) -> &DataSourceConfig {
        &self.config
    }

    /// Clone current configuration (without building)
    pub fn clone_current(&self) -> DataSourceConfig {
        self.config.clone()
    }

    /// Create a new builder with the same configuration
    pub fn duplicate(&self) -> Self {
        Self {
            config: self.config.clone(),
            validator: None, // Reset validator
            validate_on_build: self.validate_on_build,
            template: self.template.clone(),
        }
    }
}

/// Kafka-specific configuration builder
#[derive(Debug)]
pub struct KafkaConfigBuilder {
    inner: DataSourceConfigBuilder,
}

impl KafkaConfigBuilder {
    fn new(builder: DataSourceConfigBuilder) -> Self {
        Self { inner: builder }
    }

    /// Set Kafka broker(s)
    pub fn brokers<S: Into<String>>(self, brokers: S) -> Self {
        let brokers_str = brokers.into();
        if let Some(first_broker) = brokers_str.split(',').next() {
            if let Some(colon_pos) = first_broker.find(':') {
                let host = first_broker[..colon_pos].to_string();
                if let Ok(port) = first_broker[colon_pos + 1..].parse::<u16>() {
                    return Self {
                        inner: self
                            .inner
                            .host(host)
                            .port(port)
                            .parameter("brokers", brokers_str),
                    };
                }
            }
        }
        Self {
            inner: self.inner.parameter("brokers", brokers_str),
        }
    }

    /// Set Kafka topic
    pub fn topic<S: Into<String>>(self, topic: S) -> Self {
        let topic = topic.into();
        Self {
            inner: self
                .inner
                .path(format!("/{}", topic.trim_start_matches('/'))),
        }
    }

    /// Set consumer group ID
    pub fn group_id<S: Into<String>>(self, group_id: S) -> Self {
        Self {
            inner: self.inner.parameter("group_id", group_id),
        }
    }

    /// Set auto commit behavior
    pub fn auto_commit(self, enabled: bool) -> Self {
        Self {
            inner: self.inner.bool_parameter("auto_commit", enabled),
        }
    }

    /// Set session timeout
    pub fn session_timeout(self, timeout: Duration) -> Self {
        Self {
            inner: self
                .inner
                .int_parameter("session_timeout_ms", timeout.as_millis() as i64),
        }
    }

    /// Set security protocol
    pub fn security_protocol<S: Into<String>>(self, protocol: S) -> Self {
        Self {
            inner: self.inner.parameter("security_protocol", protocol),
        }
    }

    /// Set a parameter (generic)
    pub fn parameter<K, V>(self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            inner: self.inner.parameter(key, value),
        }
    }

    /// Set host (fallback for generic builder compatibility)
    pub fn host<S: Into<String>>(self, host: S) -> Self {
        Self {
            inner: self.inner.host(host),
        }
    }

    /// Set port (fallback for generic builder compatibility)
    pub fn port(self, port: u16) -> Self {
        Self {
            inner: self.inner.port(port),
        }
    }

    /// Set timeout
    pub fn timeout(self, timeout: Duration) -> Self {
        Self {
            inner: self.inner.timeout(timeout),
        }
    }

    /// Set max retries
    pub fn max_retries(self, retries: u32) -> Self {
        Self {
            inner: self.inner.max_retries(retries),
        }
    }

    /// Convert back to generic builder
    pub fn into_builder(self) -> DataSourceConfigBuilder {
        self.inner
    }

    /// Build the configuration
    pub fn build(self) -> Result<DataSourceConfig, ConfigurationError> {
        self.inner.build()
    }

    /// Build the configuration without validation
    pub fn build_unchecked(self) -> DataSourceConfig {
        self.inner.build_unchecked()
    }
}

/// S3-specific configuration builder
#[derive(Debug)]
pub struct S3ConfigBuilder {
    inner: DataSourceConfigBuilder,
}

impl S3ConfigBuilder {
    fn new(builder: DataSourceConfigBuilder) -> Self {
        Self { inner: builder }
    }

    /// Set S3 bucket
    pub fn bucket<S: Into<String>>(self, bucket: S) -> Self {
        Self {
            inner: self.inner.host(bucket),
        }
    }

    /// Set S3 key prefix/path
    pub fn key<S: Into<String>>(self, key: S) -> Self {
        let key = key.into();
        Self {
            inner: self.inner.path(format!("/{}", key.trim_start_matches('/'))),
        }
    }

    /// Set AWS region
    pub fn region<S: Into<String>>(self, region: S) -> Self {
        Self {
            inner: self.inner.parameter("region", region),
        }
    }

    /// Set AWS credentials
    pub fn credentials<K: Into<String>, S: Into<String>>(
        self,
        access_key: K,
        secret_key: S,
    ) -> Self {
        Self {
            inner: self
                .inner
                .parameter("access_key", access_key)
                .parameter("secret_key", secret_key),
        }
    }

    /// Set AWS profile
    pub fn profile<S: Into<String>>(self, profile: S) -> Self {
        Self {
            inner: self.inner.parameter("profile", profile),
        }
    }

    /// Convert back to generic builder
    pub fn into_builder(self) -> DataSourceConfigBuilder {
        self.inner
    }

    /// Build the configuration
    pub fn build(self) -> Result<DataSourceConfig, ConfigurationError> {
        self.inner.build()
    }
}

/// File-specific configuration builder
#[derive(Debug)]
pub struct FileConfigBuilder {
    inner: DataSourceConfigBuilder,
}

impl FileConfigBuilder {
    fn new(builder: DataSourceConfigBuilder) -> Self {
        Self { inner: builder }
    }

    /// Set file path
    pub fn file_path<S: Into<String>>(self, path: S) -> Self {
        Self {
            inner: self.inner.path(path),
        }
    }

    /// Set file format
    pub fn format<S: Into<String>>(self, format: S) -> Self {
        Self {
            inner: self.inner.parameter("format", format),
        }
    }

    /// Enable file watching
    pub fn watch(self, enabled: bool) -> Self {
        Self {
            inner: self.inner.bool_parameter("watch", enabled),
        }
    }

    /// Set compression type
    pub fn compression<S: Into<String>>(self, compression: S) -> Self {
        Self {
            inner: self.inner.parameter("compression", compression),
        }
    }

    /// Convert back to generic builder
    pub fn into_builder(self) -> DataSourceConfigBuilder {
        self.inner
    }

    /// Build the configuration
    pub fn build(self) -> Result<DataSourceConfig, ConfigurationError> {
        self.inner.build()
    }
}

/// PostgreSQL-specific configuration builder
#[derive(Debug)]
pub struct PostgreSQLConfigBuilder {
    inner: DataSourceConfigBuilder,
}

impl PostgreSQLConfigBuilder {
    fn new(builder: DataSourceConfigBuilder) -> Self {
        Self { inner: builder }
    }

    /// Set database name
    pub fn database<S: Into<String>>(self, database: S) -> Self {
        let database = database.into();
        Self {
            inner: self
                .inner
                .path(format!("/{}", database.trim_start_matches('/'))),
        }
    }

    /// Set username and password
    pub fn credentials<U: Into<String>, P: Into<String>>(self, username: U, password: P) -> Self {
        Self {
            inner: self
                .inner
                .parameter("user", username)
                .parameter("password", password),
        }
    }

    /// Set SSL mode
    pub fn ssl_mode<S: Into<String>>(self, ssl_mode: S) -> Self {
        Self {
            inner: self.inner.parameter("sslmode", ssl_mode),
        }
    }

    /// Set connection pool size
    pub fn pool_size(self, size: u32) -> Self {
        Self {
            inner: self.inner.int_parameter("pool_size", size as i64),
        }
    }

    /// Convert back to generic builder
    pub fn into_builder(self) -> DataSourceConfigBuilder {
        self.inner
    }

    /// Build the configuration
    pub fn build(self) -> Result<DataSourceConfig, ConfigurationError> {
        self.inner.build()
    }
}

/// Configuration templates for common setups
#[derive(Debug, Clone)]
pub struct ConfigTemplate {
    /// Template name
    pub name: String,

    /// Template description
    pub description: String,

    /// Base configuration
    pub config: DataSourceConfig,
}

impl ConfigTemplate {
    /// Get all available templates
    pub fn all() -> HashMap<String, ConfigTemplate> {
        let mut templates = HashMap::new();

        // Local Kafka template
        templates.insert(
            "kafka-local".to_string(),
            ConfigTemplate {
                name: "kafka-local".to_string(),
                description: "Local Kafka development setup".to_string(),
                config: {
                    let mut config = DataSourceConfig::new("kafka");
                    config.host = Some("localhost".to_string());
                    config.port = Some(9092);
                    config.set_parameter("group_id", "ferrisstreams-dev");
                    config.set_parameter("auto_commit", "true");
                    config
                },
            },
        );

        // Production Kafka cluster template
        templates.insert(
            "kafka-cluster".to_string(),
            ConfigTemplate {
                name: "kafka-cluster".to_string(),
                description: "Production Kafka cluster setup".to_string(),
                config: {
                    let mut config = DataSourceConfig::new("kafka");
                    config.set_parameter("security_protocol", "SASL_SSL");
                    config.set_parameter("session_timeout_ms", "30000");
                    config.set_parameter("auto_commit", "false");
                    config
                },
            },
        );

        // Local PostgreSQL template
        templates.insert(
            "postgres-local".to_string(),
            ConfigTemplate {
                name: "postgres-local".to_string(),
                description: "Local PostgreSQL development setup".to_string(),
                config: {
                    let mut config = DataSourceConfig::new("postgresql");
                    config.host = Some("localhost".to_string());
                    config.port = Some(5432);
                    config.set_parameter("user", "postgres");
                    config.set_parameter("sslmode", "disable");
                    config
                },
            },
        );

        // S3 data lake template
        templates.insert(
            "s3-datalake".to_string(),
            ConfigTemplate {
                name: "s3-datalake".to_string(),
                description: "S3 data lake setup with Parquet files".to_string(),
                config: {
                    let mut config = DataSourceConfig::new("s3");
                    config.set_parameter("format", "parquet");
                    config.set_parameter("compression", "snappy");
                    config
                },
            },
        );

        templates
    }

    /// Get a specific template by name
    pub fn get(name: &str) -> Option<ConfigTemplate> {
        Self::all().get(name).cloned()
    }

    /// List all template names
    pub fn list_names() -> Vec<String> {
        Self::all().keys().cloned().collect()
    }
}

impl Default for DataSourceConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigurationError::MissingRequired(field) => {
                write!(f, "Missing required field: {}", field)
            }
            ConfigurationError::InvalidValue {
                field,
                value,
                reason,
            } => {
                write!(f, "Invalid value for '{}': '{}' ({})", field, value, reason)
            }
            ConfigurationError::ValidationFailed(err) => {
                write!(f, "Configuration validation failed: {}", err)
            }
            ConfigurationError::TemplateNotFound(template) => {
                write!(f, "Template '{}' not found", template)
            }
            ConfigurationError::ConflictError {
                field1,
                field2,
                reason,
            } => {
                write!(
                    f,
                    "Configuration conflict between '{}' and '{}': {}",
                    field1, field2, reason
                )
            }
        }
    }
}

impl std::error::Error for ConfigurationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigurationError::ValidationFailed(err) => Some(err),
            _ => None,
        }
    }
}

