//! Schema definition and parsing
//!
//! Defines the schema format for test data generation including:
//! - Field definitions with types and constraints
//! - Value distributions (uniform, normal, log_normal)
//! - Derived field expressions
//! - Foreign key relationships

use super::error::{TestHarnessError, TestHarnessResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Schema definition for test data generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Schema name (used for identification)
    pub name: String,

    /// Optional description
    #[serde(default)]
    pub description: Option<String>,

    /// Field definitions
    pub fields: Vec<FieldDefinition>,

    /// Record count for generation (can be overridden)
    #[serde(default = "default_record_count")]
    pub record_count: usize,

    /// Field to use as Kafka message key (optional)
    /// When set, the value of this field will be used as the Kafka message key
    #[serde(default)]
    pub key_field: Option<String>,

    /// Source path where this schema was loaded from (not serialized)
    #[serde(skip)]
    pub source_path: Option<String>,
}

fn default_record_count() -> usize {
    1000
}

/// Definition of a single field in the schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    /// Field name
    pub name: String,

    /// Field type
    #[serde(rename = "type")]
    pub field_type: FieldType,

    /// Constraints for value generation
    #[serde(default)]
    pub constraints: FieldConstraints,

    /// Whether field can be null
    #[serde(default)]
    pub nullable: bool,

    /// Optional description
    #[serde(default)]
    pub description: Option<String>,
}

/// Supported field types
///
/// For simple types, use the type name directly:
/// ```yaml
/// type: string
/// type: integer
/// type: float
/// ```
///
/// For decimal with precision, use the nested format:
/// ```yaml
/// type:
///   decimal:
///     precision: 4
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", untagged)]
pub enum FieldType {
    /// Decimal with precision (e.g., financial values)
    /// Must be checked first in untagged deserialization
    DecimalType { decimal: DecimalConfig },
    /// String/text field
    Simple(SimpleFieldType),
}

/// Simple field types without additional configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SimpleFieldType {
    /// String/text field
    String,
    /// 64-bit integer
    Integer,
    /// 64-bit float
    Float,
    /// Boolean
    Boolean,
    /// Timestamp (ISO 8601)
    Timestamp,
    /// Date (YYYY-MM-DD)
    Date,
    /// UUID
    Uuid,
}

/// Configuration for decimal field type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecimalConfig {
    /// Number of decimal places
    pub precision: u8,
}

// Convenience constructors for FieldType
impl FieldType {
    /// Create a String field type
    pub fn string() -> Self {
        FieldType::Simple(SimpleFieldType::String)
    }

    /// Create an Integer field type
    pub fn integer() -> Self {
        FieldType::Simple(SimpleFieldType::Integer)
    }

    /// Create a Float field type
    pub fn float() -> Self {
        FieldType::Simple(SimpleFieldType::Float)
    }

    /// Create a Boolean field type
    pub fn boolean() -> Self {
        FieldType::Simple(SimpleFieldType::Boolean)
    }

    /// Create a Timestamp field type
    pub fn timestamp() -> Self {
        FieldType::Simple(SimpleFieldType::Timestamp)
    }

    /// Create a Date field type
    pub fn date() -> Self {
        FieldType::Simple(SimpleFieldType::Date)
    }

    /// Create a UUID field type
    pub fn uuid() -> Self {
        FieldType::Simple(SimpleFieldType::Uuid)
    }

    /// Create a Decimal field type with given precision
    pub fn decimal(precision: u8) -> Self {
        FieldType::DecimalType {
            decimal: DecimalConfig { precision },
        }
    }
}

/// Constraints for field value generation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FieldConstraints {
    /// Enum values (pick from list)
    #[serde(default)]
    pub enum_values: Option<EnumConstraint>,

    /// Numeric range (min/max)
    #[serde(default)]
    pub range: Option<RangeConstraint>,

    /// String pattern (regex-like)
    #[serde(default)]
    pub pattern: Option<String>,

    /// String length constraints
    #[serde(default)]
    pub length: Option<LengthConstraint>,

    /// Value distribution
    #[serde(default)]
    pub distribution: Option<Distribution>,

    /// Derived from other field(s)
    #[serde(default)]
    pub derived: Option<DerivedConstraint>,

    /// Foreign key reference
    #[serde(default)]
    pub references: Option<ReferenceConstraint>,

    /// Timestamp range (relative or absolute)
    #[serde(default)]
    pub timestamp_range: Option<TimestampRange>,
}

/// Enum constraint with optional weights
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumConstraint {
    /// Allowed values
    pub values: Vec<String>,

    /// Optional weights (must match values length)
    #[serde(default)]
    pub weights: Option<Vec<f64>>,
}

/// Numeric range constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeConstraint {
    /// Minimum value (inclusive)
    pub min: f64,

    /// Maximum value (inclusive)
    pub max: f64,
}

/// String length constraint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LengthConstraint {
    /// Minimum length
    #[serde(default)]
    pub min: Option<usize>,

    /// Maximum length
    #[serde(default)]
    pub max: Option<usize>,
}

/// Value distribution for generation
///
/// For simple uniform distribution, use the string value directly:
/// ```yaml
/// distribution: uniform
/// ```
///
/// For parameterized distributions, use the nested format:
/// ```yaml
/// distribution:
///   log_normal:
///     mean: 8.0
///     std_dev: 1.5
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", untagged)]
pub enum Distribution {
    /// Random walk / Geometric Brownian Motion for realistic price paths
    /// Each value depends on the previous value, creating realistic time series
    RandomWalk { random_walk: RandomWalkConfig },
    /// Normal distribution (must be first for untagged deserialization)
    Normal { normal: NormalConfig },
    /// Log-normal distribution
    LogNormal { log_normal: LogNormalConfig },
    /// Zipf distribution (for skewed data)
    Zipf { zipf: ZipfConfig },
    /// Uniform distribution (default) - simple string
    Uniform(SimpleDistribution),
}

/// Simple distribution types without parameters
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SimpleDistribution {
    Uniform,
}

/// Configuration for normal distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalConfig {
    pub mean: f64,
    pub std_dev: f64,
}

/// Configuration for log-normal distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogNormalConfig {
    pub mean: f64,
    pub std_dev: f64,
}

/// Configuration for Zipf distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZipfConfig {
    pub exponent: f64,
}

/// Configuration for random walk / Geometric Brownian Motion
///
/// Generates realistic price paths where each value depends on the previous.
/// Uses the GBM formula: S(t+1) = S(t) * (1 + drift + volatility * Z)
/// where Z ~ N(0,1)
///
/// Example usage in schema:
/// ```yaml
/// - name: price
///   type:
///     decimal:
///       precision: 4
///   constraints:
///     range:
///       min: 150.0    # Initial value
///       max: 500.0    # Optional ceiling (prevents runaway)
///     distribution:
///       random_walk:
///         drift: 0.0001       # Expected return per step (~0.01%)
///         volatility: 0.02    # Std dev per step (2%)
///         group_by: symbol    # Separate paths per symbol
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RandomWalkConfig {
    /// Drift parameter (mu) - expected return per step
    /// Typical values: 0.0001 to 0.001 for small positive drift
    #[serde(default)]
    pub drift: f64,

    /// Volatility parameter (sigma) - standard deviation per step
    /// Typical values: 0.01 to 0.05 (1% to 5%)
    #[serde(default = "default_volatility")]
    pub volatility: f64,

    /// Field to group by - each unique value gets its own price path
    /// e.g., "symbol" means AAPL, GOOGL, etc. each have independent paths
    #[serde(default)]
    pub group_by: Option<String>,

    /// Whether to allow the value to go below range.min (default: false)
    /// If false, values are floored at range.min
    #[serde(default)]
    pub allow_below_min: bool,

    /// Whether to allow the value to exceed range.max (default: false)
    /// If false, values are capped at range.max
    #[serde(default)]
    pub allow_above_max: bool,
}

fn default_volatility() -> f64 {
    0.02 // 2% default volatility
}

/// Derived field expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivedConstraint {
    /// Expression to evaluate (e.g., "price * quantity")
    pub expression: String,

    /// Fields this depends on
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// Foreign key reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceConstraint {
    /// Schema to reference
    pub schema: String,

    /// Field in referenced schema
    pub field: String,

    /// Optional: sample from file instead of schema
    #[serde(default)]
    pub file: Option<String>,
}

/// Timestamp range specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TimestampRange {
    /// Relative range (e.g., "-1h" to "now")
    Relative { start: String, end: String },
    /// Absolute range (ISO 8601)
    Absolute {
        start: String, // ISO 8601
        end: String,   // ISO 8601
    },
}

impl Schema {
    /// Load schema from YAML file
    pub fn from_file(path: impl AsRef<Path>) -> TestHarnessResult<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })?;

        let mut schema = Self::from_yaml(&content, path.display().to_string())?;
        schema.source_path = Some(path.display().to_string());
        Ok(schema)
    }

    /// Parse schema from YAML string
    pub fn from_yaml(yaml: &str, file_name: String) -> TestHarnessResult<Self> {
        serde_yaml::from_str(yaml).map_err(|e| TestHarnessError::SchemaParseError {
            message: e.to_string(),
            file: file_name,
        })
    }

    /// Set the source path where this schema was loaded from
    pub fn with_source_path(mut self, path: impl AsRef<Path>) -> Self {
        self.source_path = Some(path.as_ref().display().to_string());
        self
    }

    /// Validate the schema definition
    pub fn validate(&self) -> TestHarnessResult<()> {
        // Check for duplicate field names
        let mut seen = std::collections::HashSet::new();
        for field in &self.fields {
            if !seen.insert(&field.name) {
                return Err(TestHarnessError::SchemaParseError {
                    message: format!("Duplicate field name: {}", field.name),
                    file: self.name.clone(),
                });
            }
        }

        // Validate enum weights match values
        for field in &self.fields {
            if let Some(ref enum_constraint) = field.constraints.enum_values {
                if let Some(ref weights) = enum_constraint.weights {
                    if weights.len() != enum_constraint.values.len() {
                        return Err(TestHarnessError::SchemaParseError {
                            message: format!(
                                "Field '{}': weights count ({}) doesn't match values count ({})",
                                field.name,
                                weights.len(),
                                enum_constraint.values.len()
                            ),
                            file: self.name.clone(),
                        });
                    }
                }
            }
        }

        // Validate range constraints
        for field in &self.fields {
            if let Some(ref range) = field.constraints.range {
                if range.min > range.max {
                    return Err(TestHarnessError::SchemaParseError {
                        message: format!(
                            "Field '{}': min ({}) > max ({})",
                            field.name, range.min, range.max
                        ),
                        file: self.name.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Get field by name
    pub fn get_field(&self, name: &str) -> Option<&FieldDefinition> {
        self.fields.iter().find(|f| f.name == name)
    }
}

/// Collection of schemas indexed by name
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    schemas: HashMap<String, Schema>,
}

// =============================================================================
// SCHEMA GENERATION FROM DATA HINTS
// =============================================================================

use super::annotate::{DataHint, DataHintType, GlobalDataHints};

/// Generate a Schema from parsed data hints
///
/// This allows SQL files to embed data generation hints directly:
/// ```sql
/// -- @data.record_count: 1000
/// -- @data.symbol.type: string
/// -- @data.symbol: enum ["AAPL", "GOOGL", "MSFT"]
/// -- @data.price.type: decimal(4)
/// -- @data.price: range [100, 500], distribution: random_walk, volatility: 0.02
/// ```
pub fn generate_schema_from_hints(
    name: &str,
    global_hints: &GlobalDataHints,
    field_hints: &[DataHint],
) -> TestHarnessResult<Schema> {
    let mut fields = Vec::new();

    for hint in field_hints {
        let field_def = convert_hint_to_field(hint)?;
        fields.push(field_def);
    }

    // Validate that all fields have explicit types
    for field in &fields {
        if matches!(field.field_type, FieldType::Simple(SimpleFieldType::String))
            && field.constraints.enum_values.is_none()
            && field.constraints.pattern.is_none()
        {
            // Check if this was explicitly set or just defaulted
            // For now, we allow string as the fallback type
        }
    }

    let schema = Schema {
        name: name.to_string(),
        description: Some(format!("Auto-generated from @data hints in {}", name)),
        fields,
        record_count: global_hints.record_count.unwrap_or(1000),
        key_field: None,
        source_path: None,
    };

    schema.validate()?;
    Ok(schema)
}

/// Convert a single DataHint to a FieldDefinition
fn convert_hint_to_field(hint: &DataHint) -> TestHarnessResult<FieldDefinition> {
    // Determine field type
    let field_type = match &hint.field_type {
        Some(DataHintType::String) => FieldType::string(),
        Some(DataHintType::Integer) => FieldType::integer(),
        Some(DataHintType::Float) => FieldType::float(),
        Some(DataHintType::Decimal(precision)) => FieldType::decimal(*precision),
        Some(DataHintType::Timestamp) => FieldType::timestamp(),
        Some(DataHintType::Date) => FieldType::date(),
        Some(DataHintType::Uuid) => FieldType::uuid(),
        Some(DataHintType::Boolean) => FieldType::boolean(),
        None => {
            // Try to infer from constraints
            if hint.enum_values.is_some() {
                FieldType::string()
            } else if hint.range.is_some() {
                // Could be integer or float/decimal
                FieldType::float()
            } else if hint.sequential.is_some() {
                FieldType::timestamp()
            } else {
                return Err(TestHarnessError::SchemaParseError {
                    message: format!(
                        "Field '{}' requires explicit type via @data.{}.type: <type>",
                        hint.field_name, hint.field_name
                    ),
                    file: "SQL hints".to_string(),
                });
            }
        }
    };

    // Build constraints
    let mut constraints = FieldConstraints::default();

    // Enum values
    if let Some(ref values) = hint.enum_values {
        constraints.enum_values = Some(EnumConstraint {
            values: values.clone(),
            weights: hint.enum_weights.clone(),
        });
    }

    // Range
    if let Some((min, max)) = hint.range {
        constraints.range = Some(RangeConstraint { min, max });
    }

    // Pattern
    if let Some(ref pattern) = hint.pattern {
        constraints.pattern = Some(pattern.clone());
    }

    // Distribution
    if let Some(ref dist) = hint.distribution {
        constraints.distribution = Some(convert_distribution(
            dist,
            hint.volatility,
            hint.drift,
            hint.group_by.clone(),
        )?);
    }

    // Timestamp range from sequential flag
    if hint.sequential == Some(true) {
        constraints.timestamp_range = Some(TimestampRange::Relative {
            start: "-1h".to_string(),
            end: "now".to_string(),
        });
    }

    // References
    if let Some(ref reference) = hint.references {
        let parts: Vec<&str> = reference.splitn(2, '.').collect();
        if parts.len() == 2 {
            constraints.references = Some(ReferenceConstraint {
                schema: parts[0].to_string(),
                field: parts[1].to_string(),
                file: None,
            });
        }
    }

    Ok(FieldDefinition {
        name: hint.field_name.clone(),
        field_type,
        constraints,
        nullable: false,
        description: None,
    })
}

/// Convert distribution string to Distribution enum
fn convert_distribution(
    dist: &str,
    volatility: Option<f64>,
    drift: Option<f64>,
    group_by: Option<String>,
) -> TestHarnessResult<Distribution> {
    match dist.to_lowercase().as_str() {
        "uniform" => Ok(Distribution::Uniform(SimpleDistribution::Uniform)),
        "normal" => Ok(Distribution::Normal {
            normal: NormalConfig {
                mean: 0.0,
                std_dev: 1.0,
            },
        }),
        "log_normal" | "lognormal" => Ok(Distribution::LogNormal {
            log_normal: LogNormalConfig {
                mean: 0.0,
                std_dev: 1.0,
            },
        }),
        "zipf" => Ok(Distribution::Zipf {
            zipf: ZipfConfig { exponent: 1.0 },
        }),
        "random_walk" | "randomwalk" | "gbm" => Ok(Distribution::RandomWalk {
            random_walk: RandomWalkConfig {
                drift: drift.unwrap_or(0.0001),
                volatility: volatility.unwrap_or(0.02),
                group_by,
                allow_below_min: false,
                allow_above_max: false,
            },
        }),
        _ => Err(TestHarnessError::SchemaParseError {
            message: format!("Unknown distribution: {}", dist),
            file: "SQL hints".to_string(),
        }),
    }
}

/// Serialize a Schema to YAML string
pub fn schema_to_yaml(schema: &Schema) -> TestHarnessResult<String> {
    serde_yaml::to_string(schema).map_err(|e| TestHarnessError::SchemaParseError {
        message: format!("Failed to serialize schema: {}", e),
        file: schema.name.clone(),
    })
}

impl SchemaRegistry {
    /// Create empty registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Load schemas from directory
    pub fn load_from_dir(path: impl AsRef<Path>) -> TestHarnessResult<Self> {
        let mut registry = Self::new();
        let path = path.as_ref();

        if !path.exists() {
            return Ok(registry);
        }

        for entry in std::fs::read_dir(path).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })? {
            let entry = entry.map_err(|e| TestHarnessError::IoError {
                message: e.to_string(),
                path: path.display().to_string(),
            })?;

            let file_path = entry.path();
            if file_path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml")
            {
                if let Ok(schema) = Schema::from_file(&file_path) {
                    registry.register(schema);
                }
            }
        }

        Ok(registry)
    }

    /// Register a schema
    pub fn register(&mut self, schema: Schema) {
        self.schemas.insert(schema.name.clone(), schema);
    }

    /// Get schema by name
    pub fn get(&self, name: &str) -> Option<&Schema> {
        self.schemas.get(name)
    }

    /// List all schema names
    pub fn names(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| s.as_str()).collect()
    }
}
