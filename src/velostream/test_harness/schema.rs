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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    /// String/text field
    String,
    /// 64-bit integer
    Integer,
    /// 64-bit float
    Float,
    /// Decimal with precision (e.g., financial values)
    Decimal { precision: u8 },
    /// Boolean
    Boolean,
    /// Timestamp (ISO 8601)
    Timestamp,
    /// Date (YYYY-MM-DD)
    Date,
    /// UUID
    Uuid,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distribution {
    /// Uniform distribution (default)
    Uniform,
    /// Normal distribution
    Normal { mean: f64, std_dev: f64 },
    /// Log-normal distribution
    LogNormal { mean: f64, std_dev: f64 },
    /// Zipf distribution (for skewed data)
    Zipf { exponent: f64 },
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

        Self::from_yaml(&content, path.display().to_string())
    }

    /// Parse schema from YAML string
    pub fn from_yaml(yaml: &str, file_name: String) -> TestHarnessResult<Self> {
        serde_yaml::from_str(yaml).map_err(|e| TestHarnessError::SchemaParseError {
            message: e.to_string(),
            file: file_name,
        })
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
