//! Configuration Validation System
//!
//! This module provides comprehensive validation for data source configurations.
//! It includes scheme-specific validators, parameter validation, and cross-parameter
//! constraint checking.
//!
//! ## Features
//!
//! - **Scheme-specific Validation**: Tailored rules for each data source type
//! - **Parameter Validation**: Type checking and range validation for parameters
//! - **Cross-validation**: Check parameter combinations and conflicts
//! - **Custom Validators**: Pluggable validation system for custom rules
//! - **Detailed Error Reporting**: Clear error messages with suggestions

use crate::velostream::sql::config::DataSourceConfig;
use std::collections::HashMap;
use std::fmt;
use std::time::Instant;

/// Configuration validator trait
pub trait ConfigValidator {
    /// Validate a complete configuration
    fn validate(&self, config: &DataSourceConfig) -> ValidationResult;

    /// Validate a URI string before parsing
    fn validate_uri(&self, uri: &str) -> ValidationResult;

    /// Get supported schemes for this validator
    fn supported_schemes(&self) -> Vec<&'static str>;

    /// Get validator metadata
    fn metadata(&self) -> ValidatorMetadata;
}

/// Validation result
pub type ValidationResult = Result<Vec<ValidationWarning>, ValidationError>;

/// Validation warnings (non-fatal issues)
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationWarning {
    /// Warning code
    pub code: String,

    /// Human-readable message
    pub message: String,

    /// Parameter that triggered the warning
    pub parameter: Option<String>,

    /// Suggested action
    pub suggestion: Option<String>,
}

/// Validation errors (fatal issues)
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    /// Missing required parameter
    MissingRequired {
        parameter: String,
        scheme: String,
        description: String,
    },

    /// Invalid parameter value
    InvalidValue {
        parameter: String,
        value: String,
        expected: String,
        scheme: String,
    },

    /// Parameter out of valid range
    OutOfRange {
        parameter: String,
        value: String,
        min: Option<String>,
        max: Option<String>,
    },

    /// Conflicting parameters
    ConflictingParameters {
        parameter1: String,
        parameter2: String,
        reason: String,
    },

    /// Invalid parameter combination
    InvalidCombination {
        parameters: Vec<String>,
        reason: String,
    },

    /// Custom validation error
    Custom {
        code: String,
        message: String,
        parameter: Option<String>,
    },
}

/// Validator metadata
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatorMetadata {
    /// Validator name
    pub name: String,

    /// Supported schemes
    pub schemes: Vec<String>,

    /// Version
    pub version: String,

    /// Description
    pub description: String,
}

/// Comprehensive configuration validator registry
pub struct ConfigValidatorRegistry {
    /// Registered validators by scheme
    validators: HashMap<String, Box<dyn ConfigValidator>>,

    /// Validation statistics
    stats: ValidationStats,
}

/// Validation statistics
#[derive(Debug, Clone, Default)]
pub struct ValidationStats {
    /// Total validations performed
    pub total_validations: usize,

    /// Successful validations
    pub successful_validations: usize,

    /// Failed validations
    pub failed_validations: usize,

    /// Total validation time in microseconds
    pub total_validation_time_us: u64,

    /// Validation time by scheme
    pub validation_time_by_scheme: HashMap<String, u64>,

    /// Error counts by type
    pub error_counts: HashMap<String, usize>,
}

impl Default for ConfigValidatorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigValidatorRegistry {
    /// Create new registry with default validators
    pub fn new() -> Self {
        let mut registry = Self {
            validators: HashMap::new(),
            stats: ValidationStats::default(),
        };

        // Register default validators
        registry.register("kafka", Box::new(KafkaValidator::new()));
        registry.register("s3", Box::new(S3Validator::new()));
        registry.register("file", Box::new(FileValidator::new()));
        registry.register("postgresql", Box::new(PostgreSQLValidator::new()));
        registry.register("clickhouse", Box::new(ClickHouseValidator::new()));
        registry.register("http", Box::new(HttpValidator::new()));

        registry
    }

    /// Register a validator for a scheme
    pub fn register(&mut self, scheme: &str, validator: Box<dyn ConfigValidator>) {
        self.validators.insert(scheme.to_lowercase(), validator);
    }

    /// Validate a configuration
    pub fn validate(&mut self, config: &DataSourceConfig) -> ValidationResult {
        let start_time = Instant::now();
        self.stats.total_validations += 1;

        let result = if let Some(validator) = self.validators.get(&config.scheme.to_lowercase()) {
            validator.validate(config)
        } else {
            // Unknown scheme - perform basic validation
            self.validate_basic(config)
        };

        let validation_time = start_time.elapsed().as_micros() as u64;
        self.stats.total_validation_time_us += validation_time;

        let scheme = config.scheme.clone();
        *self
            .stats
            .validation_time_by_scheme
            .entry(scheme)
            .or_insert(0) += validation_time;

        match &result {
            Ok(_) => self.stats.successful_validations += 1,
            Err(err) => {
                self.stats.failed_validations += 1;
                let error_type = format!("{:?}", err)
                    .split('(')
                    .next()
                    .unwrap_or("Unknown")
                    .to_string();
                *self.stats.error_counts.entry(error_type).or_insert(0) += 1;
            }
        }

        result
    }

    /// Basic validation for unknown schemes
    fn validate_basic(&self, config: &DataSourceConfig) -> ValidationResult {
        let mut warnings = Vec::new();

        // Check for empty scheme
        if config.scheme.is_empty() {
            return Err(ValidationError::InvalidValue {
                parameter: "scheme".to_string(),
                value: "".to_string(),
                expected: "non-empty scheme (e.g., kafka, s3, file)".to_string(),
                scheme: config.scheme.clone(),
            });
        }

        // Warn about unknown scheme
        warnings.push(ValidationWarning {
            code: "UNKNOWN_SCHEME".to_string(),
            message: format!("Unknown scheme '{}' - validation is limited", config.scheme),
            parameter: Some("scheme".to_string()),
            suggestion: Some(
                "Consider using a known scheme (kafka, s3, file, postgresql, clickhouse, http)"
                    .to_string(),
            ),
        });

        Ok(warnings)
    }

    /// Get validation statistics
    pub fn stats(&self) -> &ValidationStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = ValidationStats::default();
    }

    /// List supported schemes
    pub fn supported_schemes(&self) -> Vec<String> {
        self.validators.keys().cloned().collect()
    }
}

/// Kafka configuration validator
#[derive(Debug)]
pub struct KafkaValidator {
    metadata: ValidatorMetadata,
}

impl Default for KafkaValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaValidator {
    pub fn new() -> Self {
        Self {
            metadata: ValidatorMetadata {
                name: "KafkaValidator".to_string(),
                schemes: vec!["kafka".to_string()],
                version: "1.0.0".to_string(),
                description: "Validates Kafka data source configurations".to_string(),
            },
        }
    }
}

impl ConfigValidator for KafkaValidator {
    fn validate(&self, config: &DataSourceConfig) -> ValidationResult {
        let mut warnings = Vec::new();

        // Check required parameters
        if config.host.is_none() {
            return Err(ValidationError::MissingRequired {
                parameter: "host".to_string(),
                scheme: "kafka".to_string(),
                description: "Kafka broker host is required".to_string(),
            });
        }

        if config.path.is_none() {
            return Err(ValidationError::MissingRequired {
                parameter: "path".to_string(),
                scheme: "kafka".to_string(),
                description: "Kafka topic name is required in path".to_string(),
            });
        }

        // Validate port range
        if let Some(port) = config.port {
            if port < 1024 {
                return Err(ValidationError::OutOfRange {
                    parameter: "port".to_string(),
                    value: port.to_string(),
                    min: Some("1024".to_string()),
                    max: Some("65535".to_string()),
                });
            }

            if port != 9092 && port != 9093 && port != 9094 {
                warnings.push(ValidationWarning {
                    code: "UNUSUAL_KAFKA_PORT".to_string(),
                    message: format!("Port {} is unusual for Kafka (typical: 9092-9094)", port),
                    parameter: Some("port".to_string()),
                    suggestion: Some("Consider using standard Kafka ports 9092-9094".to_string()),
                });
            }
        }

        // Validate consumer group
        if let Some(group_id) = config.get_parameter("group_id") {
            if group_id.is_empty() {
                return Err(ValidationError::InvalidValue {
                    parameter: "group_id".to_string(),
                    value: "".to_string(),
                    expected: "non-empty consumer group ID".to_string(),
                    scheme: "kafka".to_string(),
                });
            }
        }

        // Validate boolean parameters
        for param in config.parameters.keys() {
            if matches!(
                param.as_str(),
                "auto_commit" | "enable.auto.commit" | "enable_partition_eof"
            ) && config.get_bool_parameter(param).is_none()
            {
                warnings.push(ValidationWarning {
                    code: "INVALID_BOOLEAN".to_string(),
                    message: format!("Parameter '{}' should be a boolean (true/false)", param),
                    parameter: Some(param.clone()),
                    suggestion: Some("Use 'true' or 'false'".to_string()),
                });
            }
        }

        // Validate timeout values
        if let Some(timeout) = config.get_int_parameter("session.timeout.ms") {
            if !(6000..=300000).contains(&timeout) {
                warnings.push(ValidationWarning {
                    code: "TIMEOUT_OUT_OF_RANGE".to_string(),
                    message: format!(
                        "Session timeout {}ms is outside recommended range 6000-300000ms",
                        timeout
                    ),
                    parameter: Some("session.timeout.ms".to_string()),
                    suggestion: Some("Use timeout between 6000ms and 300000ms".to_string()),
                });
            }
        }

        Ok(warnings)
    }

    fn validate_uri(&self, uri: &str) -> ValidationResult {
        if !uri.starts_with("kafka://") {
            return Err(ValidationError::InvalidValue {
                parameter: "uri".to_string(),
                value: uri.to_string(),
                expected: "URI starting with 'kafka://'".to_string(),
                scheme: "kafka".to_string(),
            });
        }

        Ok(vec![])
    }

    fn supported_schemes(&self) -> Vec<&'static str> {
        vec!["kafka"]
    }

    fn metadata(&self) -> ValidatorMetadata {
        self.metadata.clone()
    }
}

/// S3 configuration validator
#[derive(Debug)]
pub struct S3Validator {
    metadata: ValidatorMetadata,
}

impl Default for S3Validator {
    fn default() -> Self {
        Self::new()
    }
}

impl S3Validator {
    pub fn new() -> Self {
        Self {
            metadata: ValidatorMetadata {
                name: "S3Validator".to_string(),
                schemes: vec!["s3".to_string()],
                version: "1.0.0".to_string(),
                description: "Validates S3 data source configurations".to_string(),
            },
        }
    }
}

impl ConfigValidator for S3Validator {
    fn validate(&self, config: &DataSourceConfig) -> ValidationResult {
        let mut warnings = Vec::new();

        // Check bucket name
        if config.host.is_none() {
            return Err(ValidationError::MissingRequired {
                parameter: "host".to_string(),
                scheme: "s3".to_string(),
                description: "S3 bucket name is required".to_string(),
            });
        }

        if let Some(bucket) = &config.host {
            // S3 bucket naming rules
            if bucket.len() < 3 || bucket.len() > 63 {
                return Err(ValidationError::OutOfRange {
                    parameter: "bucket".to_string(),
                    value: bucket.clone(),
                    min: Some("3 characters".to_string()),
                    max: Some("63 characters".to_string()),
                });
            }

            if bucket.contains("_") || bucket.contains("..") {
                return Err(ValidationError::InvalidValue {
                    parameter: "bucket".to_string(),
                    value: bucket.clone(),
                    expected: "valid S3 bucket name (no underscores or consecutive dots)"
                        .to_string(),
                    scheme: "s3".to_string(),
                });
            }
        }

        // Validate region
        if let Some(region) = config.get_parameter("region") {
            if !Self::is_valid_aws_region(region) {
                warnings.push(ValidationWarning {
                    code: "UNKNOWN_AWS_REGION".to_string(),
                    message: format!("Region '{}' is not a recognized AWS region", region),
                    parameter: Some("region".to_string()),
                    suggestion: Some(
                        "Use a valid AWS region like 'us-east-1', 'us-west-2', etc.".to_string(),
                    ),
                });
            }
        }

        // Validate access credentials
        let has_access_key = config.get_parameter("access_key").is_some();
        let has_secret_key = config.get_parameter("secret_key").is_some();

        if has_access_key != has_secret_key {
            return Err(ValidationError::InvalidCombination {
                parameters: vec!["access_key".to_string(), "secret_key".to_string()],
                reason: "Both access_key and secret_key must be provided together".to_string(),
            });
        }

        Ok(warnings)
    }

    fn validate_uri(&self, uri: &str) -> ValidationResult {
        if !uri.starts_with("s3://") {
            return Err(ValidationError::InvalidValue {
                parameter: "uri".to_string(),
                value: uri.to_string(),
                expected: "URI starting with 's3://'".to_string(),
                scheme: "s3".to_string(),
            });
        }

        Ok(vec![])
    }

    fn supported_schemes(&self) -> Vec<&'static str> {
        vec!["s3"]
    }

    fn metadata(&self) -> ValidatorMetadata {
        self.metadata.clone()
    }
}

impl S3Validator {
    fn is_valid_aws_region(region: &str) -> bool {
        matches!(
            region,
            "us-east-1"
                | "us-east-2"
                | "us-west-1"
                | "us-west-2"
                | "eu-west-1"
                | "eu-west-2"
                | "eu-west-3"
                | "eu-central-1"
                | "ap-southeast-1"
                | "ap-southeast-2"
                | "ap-northeast-1"
                | "ap-northeast-2"
                | "ca-central-1"
                | "sa-east-1"
                | "ap-south-1"
                | "eu-north-1"
                | "af-south-1"
                | "ap-east-1"
                | "ap-southeast-3"
                | "eu-south-1"
                | "me-south-1"
        )
    }
}

/// File system configuration validator
#[derive(Debug)]
pub struct FileValidator {
    metadata: ValidatorMetadata,
}

impl Default for FileValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl FileValidator {
    pub fn new() -> Self {
        Self {
            metadata: ValidatorMetadata {
                name: "FileValidator".to_string(),
                schemes: vec!["file".to_string()],
                version: "1.0.0".to_string(),
                description: "Validates file system data source configurations".to_string(),
            },
        }
    }
}

impl ConfigValidator for FileValidator {
    fn validate(&self, config: &DataSourceConfig) -> ValidationResult {
        let mut warnings = Vec::new();

        // Check file path
        if config.path.is_none() {
            return Err(ValidationError::MissingRequired {
                parameter: "path".to_string(),
                scheme: "file".to_string(),
                description: "File path is required".to_string(),
            });
        }

        // Validate format parameter
        if let Some(format) = config.get_parameter("format") {
            if !matches!(
                format.to_lowercase().as_str(),
                "json" | "csv" | "parquet" | "avro" | "txt"
            ) {
                warnings.push(ValidationWarning {
                    code: "UNKNOWN_FORMAT".to_string(),
                    message: format!("Format '{}' is not recognized", format),
                    parameter: Some("format".to_string()),
                    suggestion: Some(
                        "Use supported formats: json, csv, parquet, avro, txt".to_string(),
                    ),
                });
            }
        }

        Ok(warnings)
    }

    fn validate_uri(&self, uri: &str) -> ValidationResult {
        if !uri.starts_with("file://") {
            return Err(ValidationError::InvalidValue {
                parameter: "uri".to_string(),
                value: uri.to_string(),
                expected: "URI starting with 'file://'".to_string(),
                scheme: "file".to_string(),
            });
        }

        Ok(vec![])
    }

    fn supported_schemes(&self) -> Vec<&'static str> {
        vec!["file"]
    }

    fn metadata(&self) -> ValidatorMetadata {
        self.metadata.clone()
    }
}

// Additional validators for PostgreSQL, ClickHouse, and HTTP would follow similar patterns
// For brevity, I'll provide simplified implementations

macro_rules! simple_validator {
    ($name:ident, $scheme:literal, $description:literal) => {
        #[derive(Debug)]
        pub struct $name {
            metadata: ValidatorMetadata,
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    metadata: ValidatorMetadata {
                        name: stringify!($name).to_string(),
                        schemes: vec![$scheme.to_string()],
                        version: "1.0.0".to_string(),
                        description: $description.to_string(),
                    },
                }
            }
        }

        impl ConfigValidator for $name {
            fn validate(&self, config: &DataSourceConfig) -> ValidationResult {
                let warnings = Vec::new();

                if config.host.is_none() {
                    return Err(ValidationError::MissingRequired {
                        parameter: "host".to_string(),
                        scheme: $scheme.to_string(),
                        description: format!("{} host is required", $scheme),
                    });
                }

                Ok(warnings)
            }

            fn validate_uri(&self, uri: &str) -> ValidationResult {
                let expected = format!("{}://", $scheme);
                if !uri.starts_with(&expected) {
                    return Err(ValidationError::InvalidValue {
                        parameter: "uri".to_string(),
                        value: uri.to_string(),
                        expected: format!("URI starting with '{}'", expected),
                        scheme: $scheme.to_string(),
                    });
                }

                Ok(vec![])
            }

            fn supported_schemes(&self) -> Vec<&'static str> {
                vec![$scheme]
            }

            fn metadata(&self) -> ValidatorMetadata {
                self.metadata.clone()
            }
        }
    };
}

simple_validator!(
    PostgreSQLValidator,
    "postgresql",
    "Validates PostgreSQL database configurations"
);
simple_validator!(
    ClickHouseValidator,
    "clickhouse",
    "Validates ClickHouse database configurations"
);
simple_validator!(HttpValidator, "http", "Validates HTTP API configurations");

impl ValidationStats {
    /// Get success rate percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_validations == 0 {
            0.0
        } else {
            (self.successful_validations as f64 / self.total_validations as f64) * 100.0
        }
    }

    /// Get average validation time in microseconds
    pub fn avg_validation_time_us(&self) -> f64 {
        if self.total_validations == 0 {
            0.0
        } else {
            self.total_validation_time_us as f64 / self.total_validations as f64
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::MissingRequired {
                parameter,
                scheme,
                description,
            } => {
                write!(
                    f,
                    "[{}] Missing required parameter '{}': {}",
                    scheme, parameter, description
                )
            }
            ValidationError::InvalidValue {
                parameter,
                value,
                expected,
                scheme,
            } => {
                write!(
                    f,
                    "[{}] Invalid value for '{}': got '{}', expected {}",
                    scheme, parameter, value, expected
                )
            }
            ValidationError::OutOfRange {
                parameter,
                value,
                min,
                max,
            } => {
                let range = match (min, max) {
                    (Some(min), Some(max)) => format!("between {} and {}", min, max),
                    (Some(min), None) => format!("at least {}", min),
                    (None, Some(max)) => format!("at most {}", max),
                    (None, None) => "within valid range".to_string(),
                };
                write!(
                    f,
                    "Parameter '{}' value '{}' is out of range (expected {})",
                    parameter, value, range
                )
            }
            ValidationError::ConflictingParameters {
                parameter1,
                parameter2,
                reason,
            } => {
                write!(
                    f,
                    "Conflicting parameters '{}' and '{}': {}",
                    parameter1, parameter2, reason
                )
            }
            ValidationError::InvalidCombination { parameters, reason } => {
                write!(
                    f,
                    "Invalid parameter combination {:?}: {}",
                    parameters, reason
                )
            }
            ValidationError::Custom {
                code,
                message,
                parameter,
            } => {
                if let Some(param) = parameter {
                    write!(f, "[{}] {}: {}", code, param, message)
                } else {
                    write!(f, "[{}] {}", code, message)
                }
            }
        }
    }
}

impl std::error::Error for ValidationError {}

impl fmt::Debug for ConfigValidatorRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigValidatorRegistry")
            .field(
                "validators",
                &format!("{} validators", self.validators.len()),
            )
            .field("stats", &self.stats)
            .finish()
    }
}
