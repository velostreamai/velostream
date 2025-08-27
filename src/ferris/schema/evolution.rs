//! Schema Evolution
//!
//! Handles backward and forward compatibility checking, schema migration,
//! and automatic record transformation between schema versions.

use crate::ferris::schema::{
    CompatibilityMode, FieldDefinition, Schema, SchemaError, SchemaResult,
};
use crate::ferris::sql::ast::DataType;
use crate::ferris::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::{HashMap, HashSet};

/// Schema evolution manager for handling compatibility and migrations
pub struct SchemaEvolution {
    /// Configuration for evolution behavior
    config: EvolutionConfig,
    /// Cache of compatibility checks
    compatibility_cache: HashMap<String, bool>,
}

/// Configuration for schema evolution behavior
#[derive(Debug, Clone)]
pub struct EvolutionConfig {
    /// Whether to allow field additions
    pub allow_field_additions: bool,
    /// Whether to allow field deletions
    pub allow_field_deletions: bool,
    /// Whether to allow field type changes
    pub allow_type_changes: bool,
    /// Whether to allow nullability changes
    pub allow_nullability_changes: bool,
    /// Default values for new fields
    pub default_values: HashMap<String, FieldValue>,
}

/// Represents the differences between two schemas
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaDiff {
    pub added_fields: Vec<FieldDefinition>,
    pub removed_fields: Vec<FieldDefinition>,
    pub modified_fields: Vec<FieldModification>,
    pub is_compatible: bool,
    pub compatibility_mode: CompatibilityMode,
}

/// Represents a modification to a field between schema versions
#[derive(Debug, Clone, PartialEq)]
pub struct FieldModification {
    pub field_name: String,
    pub old_field: FieldDefinition,
    pub new_field: FieldDefinition,
    pub change_type: FieldChangeType,
    pub is_compatible: bool,
}

/// Types of changes that can occur to a field
#[derive(Debug, Clone, PartialEq)]
pub enum FieldChangeType {
    /// Data type was changed
    TypeChange,
    /// Nullability was changed (nullable -> non-nullable or vice versa)
    NullabilityChange,
    /// Field description was updated
    DescriptionChange,
    /// Default value was changed
    DefaultValueChange,
    /// Multiple changes occurred
    MultipleChanges(Vec<FieldChangeType>),
}

/// Migration plan for transforming records from one schema to another
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    pub from_schema: Schema,
    pub to_schema: Schema,
    pub field_mappings: Vec<FieldMapping>,
    pub transformations: Vec<FieldTransformation>,
}

/// Mapping between fields in different schema versions
#[derive(Debug, Clone)]
pub enum FieldMapping {
    /// Field exists in both schemas with same name and compatible type
    Direct { field_name: String },
    /// Field was renamed
    Renamed { old_name: String, new_name: String },
    /// Field needs type conversion
    TypeConversion {
        field_name: String,
        converter: TypeConverter,
    },
    /// Field gets default value in target schema
    DefaultValue {
        field_name: String,
        value: FieldValue,
    },
    /// Field is dropped in target schema
    Dropped { field_name: String },
}

/// Represents a field transformation operation
#[derive(Debug, Clone)]
pub struct FieldTransformation {
    pub target_field: String,
    pub operation: TransformationOperation,
}

/// Types of transformation operations
#[derive(Debug, Clone)]
pub enum TransformationOperation {
    /// Copy field value directly
    Copy { source_field: String },
    /// Convert field type
    Convert {
        source_field: String,
        converter: TypeConverter,
    },
    /// Set constant value
    Constant { value: FieldValue },
    /// Calculate from multiple source fields
    Calculate {
        sources: Vec<String>,
        function: String,
    },
    /// Use default value
    Default,
}

/// Type conversion functions
#[derive(Debug, Clone)]
pub enum TypeConverter {
    StringToInteger,
    IntegerToString,
    FloatToInteger,
    IntegerToFloat,
    StringToBoolean,
    BooleanToString,
    TimestampToString,
    StringToTimestamp,
    Custom(String), // Custom conversion function name
}

impl SchemaEvolution {
    /// Create a new schema evolution manager with default configuration
    pub fn new() -> Self {
        Self {
            config: EvolutionConfig::default(),
            compatibility_cache: HashMap::new(),
        }
    }

    /// Create a new schema evolution manager with custom configuration
    pub fn with_config(config: EvolutionConfig) -> Self {
        Self {
            config,
            compatibility_cache: HashMap::new(),
        }
    }

    /// Check if one schema can evolve to another
    pub fn can_evolve(&self, from: &Schema, to: &Schema) -> bool {
        let diff = self.compute_diff(from, to);
        self.is_compatible_evolution(&diff, &from.metadata.compatibility)
    }

    /// Compute the differences between two schemas
    pub fn compute_diff(&self, from: &Schema, to: &Schema) -> SchemaDiff {
        let from_fields: HashMap<String, &FieldDefinition> = from
            .fields
            .iter()
            .map(|field| (field.name.clone(), field))
            .collect();

        let to_fields: HashMap<String, &FieldDefinition> = to
            .fields
            .iter()
            .map(|field| (field.name.clone(), field))
            .collect();

        let from_names: HashSet<String> = from_fields.keys().cloned().collect();
        let to_names: HashSet<String> = to_fields.keys().cloned().collect();

        // Find added fields
        let added_fields: Vec<FieldDefinition> = to_names
            .difference(&from_names)
            .filter_map(|name| to_fields.get(name).map(|&field| field.clone()))
            .collect();

        // Find removed fields
        let removed_fields: Vec<FieldDefinition> = from_names
            .difference(&to_names)
            .filter_map(|name| from_fields.get(name).map(|&field| field.clone()))
            .collect();

        // Find modified fields
        let modified_fields: Vec<FieldModification> = from_names
            .intersection(&to_names)
            .filter_map(|name| {
                let old_field = from_fields.get(name)?;
                let new_field = to_fields.get(name)?;

                if old_field != new_field {
                    Some(self.analyze_field_modification(old_field, new_field))
                } else {
                    None
                }
            })
            .collect();

        let compatibility_mode = to.metadata.compatibility.clone();
        let is_compatible = self.is_compatible_diff(
            &added_fields,
            &removed_fields,
            &modified_fields,
            &compatibility_mode,
        );

        SchemaDiff {
            added_fields,
            removed_fields,
            modified_fields,
            is_compatible,
            compatibility_mode,
        }
    }

    /// Create a migration plan for evolving from one schema to another
    pub fn create_migration_plan(&self, from: &Schema, to: &Schema) -> SchemaResult<MigrationPlan> {
        let diff = self.compute_diff(from, to);

        if !diff.is_compatible {
            return Err(SchemaError::Evolution {
                from: from.version.clone().unwrap_or("unknown".to_string()),
                to: to.version.clone().unwrap_or("unknown".to_string()),
                reason: "Schemas are not compatible for evolution".to_string(),
            });
        }

        let field_mappings = self.create_field_mappings(&diff, from, to);
        let transformations = self.create_transformations(&field_mappings, to);

        Ok(MigrationPlan {
            from_schema: from.clone(),
            to_schema: to.clone(),
            field_mappings,
            transformations,
        })
    }

    /// Transform a record from one schema version to another
    pub fn evolve_record(
        &self,
        record: StreamRecord,
        migration_plan: &MigrationPlan,
    ) -> SchemaResult<StreamRecord> {
        let mut new_fields = HashMap::new();

        for transformation in &migration_plan.transformations {
            let value = match &transformation.operation {
                TransformationOperation::Copy { source_field } => record
                    .fields
                    .get(source_field)
                    .cloned()
                    .unwrap_or(FieldValue::Null),
                TransformationOperation::Convert {
                    source_field,
                    converter,
                } => {
                    if let Some(source_value) = record.fields.get(source_field) {
                        self.convert_field_value(source_value, converter)?
                    } else {
                        FieldValue::Null
                    }
                }
                TransformationOperation::Constant { value } => value.clone(),
                TransformationOperation::Calculate {
                    sources: _,
                    function: _,
                } => {
                    // TODO: Implement calculated fields
                    FieldValue::Null
                }
                TransformationOperation::Default => {
                    self.get_default_value(&transformation.target_field)
                }
            };

            new_fields.insert(transformation.target_field.clone(), value);
        }

        Ok(StreamRecord {
            fields: new_fields,
            timestamp: record.timestamp,
            offset: record.offset,
            partition: record.partition,
            headers: record.headers,
        })
    }

    /// Check schema compatibility for a specific compatibility mode
    pub fn check_compatibility(
        &self,
        from: &Schema,
        to: &Schema,
        mode: &CompatibilityMode,
    ) -> SchemaResult<()> {
        let diff = self.compute_diff(from, to);

        match mode {
            CompatibilityMode::None => Ok(()),
            CompatibilityMode::Backward => self.check_backward_compatibility(&diff),
            CompatibilityMode::Forward => self.check_forward_compatibility(&diff),
            CompatibilityMode::Full => {
                self.check_backward_compatibility(&diff)?;
                self.check_forward_compatibility(&diff)
            }
            CompatibilityMode::Strict => {
                if from == to {
                    Ok(())
                } else {
                    Err(SchemaError::Incompatible {
                        reason: "Strict mode requires identical schemas".to_string(),
                    })
                }
            }
        }
    }

    // Private helper methods

    fn analyze_field_modification(
        &self,
        old_field: &FieldDefinition,
        new_field: &FieldDefinition,
    ) -> FieldModification {
        let mut changes = Vec::new();

        if old_field.data_type != new_field.data_type {
            changes.push(FieldChangeType::TypeChange);
        }

        if old_field.nullable != new_field.nullable {
            changes.push(FieldChangeType::NullabilityChange);
        }

        if old_field.description != new_field.description {
            changes.push(FieldChangeType::DescriptionChange);
        }

        if old_field.default_value != new_field.default_value {
            changes.push(FieldChangeType::DefaultValueChange);
        }

        let change_type = match changes.len() {
            0 => FieldChangeType::DescriptionChange, // Shouldn't happen
            1 => changes[0].clone(),
            _ => FieldChangeType::MultipleChanges(changes),
        };

        let is_compatible = self.is_field_change_compatible(&change_type, old_field, new_field);

        FieldModification {
            field_name: old_field.name.clone(),
            old_field: old_field.clone(),
            new_field: new_field.clone(),
            change_type,
            is_compatible,
        }
    }

    fn is_compatible_evolution(&self, diff: &SchemaDiff, mode: &CompatibilityMode) -> bool {
        match mode {
            CompatibilityMode::None => true,
            CompatibilityMode::Strict => {
                diff.added_fields.is_empty()
                    && diff.removed_fields.is_empty()
                    && diff.modified_fields.is_empty()
            }
            _ => diff.is_compatible,
        }
    }

    fn is_compatible_diff(
        &self,
        added: &[FieldDefinition],
        removed: &[FieldDefinition],
        modified: &[FieldModification],
        mode: &CompatibilityMode,
    ) -> bool {
        match mode {
            CompatibilityMode::None => true,
            CompatibilityMode::Backward => {
                // Backward compatible: can add optional fields, remove fields, modify compatible fields
                added
                    .iter()
                    .all(|field| field.nullable || field.default_value.is_some())
                    && modified.iter().all(|m| m.is_compatible)
            }
            CompatibilityMode::Forward => {
                // Forward compatible: can remove fields, modify compatible fields
                removed.is_empty() && modified.iter().all(|m| m.is_compatible)
            }
            CompatibilityMode::Full => {
                // Full compatible: can only add optional fields and modify compatible fields
                added
                    .iter()
                    .all(|field| field.nullable || field.default_value.is_some())
                    && removed.is_empty()
                    && modified.iter().all(|m| m.is_compatible)
            }
            CompatibilityMode::Strict => {
                added.is_empty() && removed.is_empty() && modified.is_empty()
            }
        }
    }

    fn is_field_change_compatible(
        &self,
        change_type: &FieldChangeType,
        old_field: &FieldDefinition,
        new_field: &FieldDefinition,
    ) -> bool {
        match change_type {
            FieldChangeType::TypeChange => {
                self.are_types_compatible(&old_field.data_type, &new_field.data_type)
            }
            FieldChangeType::NullabilityChange => {
                // Making field nullable is always compatible
                // Making field non-nullable is only compatible if default value exists
                !old_field.nullable && new_field.nullable
                    || old_field.nullable
                        && !new_field.nullable
                        && new_field.default_value.is_some()
            }
            FieldChangeType::DescriptionChange | FieldChangeType::DefaultValueChange => true,
            FieldChangeType::MultipleChanges(changes) => changes
                .iter()
                .all(|change| self.is_field_change_compatible(change, old_field, new_field)),
        }
    }

    fn are_types_compatible(&self, from: &DataType, to: &DataType) -> bool {
        match (from, to) {
            // Same types are always compatible
            (a, b) if a == b => true,
            // Number type promotions
            (DataType::Integer, DataType::Float) => true,
            // String conversions (if allowed)
            (_, DataType::String) if self.config.allow_type_changes => true,
            // Array and Map type compatibility (simplified)
            (DataType::Array(from_inner), DataType::Array(to_inner)) => {
                self.are_types_compatible(from_inner, to_inner)
            }
            _ => false,
        }
    }

    fn create_field_mappings(
        &self,
        _diff: &SchemaDiff,
        from: &Schema,
        to: &Schema,
    ) -> Vec<FieldMapping> {
        let mut mappings = Vec::new();

        // Create mappings for existing fields
        for to_field in &to.fields {
            if let Some(from_field) = from.get_field(&to_field.name) {
                if from_field.data_type == to_field.data_type {
                    mappings.push(FieldMapping::Direct {
                        field_name: to_field.name.clone(),
                    });
                } else if self.are_types_compatible(&from_field.data_type, &to_field.data_type) {
                    let converter =
                        self.get_type_converter(&from_field.data_type, &to_field.data_type);
                    mappings.push(FieldMapping::TypeConversion {
                        field_name: to_field.name.clone(),
                        converter,
                    });
                }
            } else {
                // New field, use default value
                mappings.push(FieldMapping::DefaultValue {
                    field_name: to_field.name.clone(),
                    value: self.get_default_value(&to_field.name),
                });
            }
        }

        mappings
    }

    fn create_transformations(
        &self,
        mappings: &[FieldMapping],
        to_schema: &Schema,
    ) -> Vec<FieldTransformation> {
        to_schema
            .fields
            .iter()
            .map(|field| {
                let operation = mappings
                    .iter()
                    .find(|mapping| self.mapping_targets_field(mapping, &field.name))
                    .map(|mapping| self.mapping_to_operation(mapping))
                    .unwrap_or(TransformationOperation::Default);

                FieldTransformation {
                    target_field: field.name.clone(),
                    operation,
                }
            })
            .collect()
    }

    fn mapping_targets_field(&self, mapping: &FieldMapping, field_name: &str) -> bool {
        match mapping {
            FieldMapping::Direct { field_name: name }
            | FieldMapping::TypeConversion {
                field_name: name, ..
            }
            | FieldMapping::DefaultValue {
                field_name: name, ..
            } => name == field_name,
            FieldMapping::Renamed { new_name, .. } => new_name == field_name,
            FieldMapping::Dropped { .. } => false,
        }
    }

    fn mapping_to_operation(&self, mapping: &FieldMapping) -> TransformationOperation {
        match mapping {
            FieldMapping::Direct { field_name } => TransformationOperation::Copy {
                source_field: field_name.clone(),
            },
            FieldMapping::TypeConversion {
                field_name,
                converter,
            } => TransformationOperation::Convert {
                source_field: field_name.clone(),
                converter: converter.clone(),
            },
            FieldMapping::DefaultValue { value, .. } => TransformationOperation::Constant {
                value: value.clone(),
            },
            FieldMapping::Renamed { old_name, .. } => TransformationOperation::Copy {
                source_field: old_name.clone(),
            },
            FieldMapping::Dropped { .. } => TransformationOperation::Default,
        }
    }

    fn get_type_converter(&self, from: &DataType, to: &DataType) -> TypeConverter {
        match (from, to) {
            (DataType::String, DataType::Integer) => TypeConverter::StringToInteger,
            (DataType::Integer, DataType::String) => TypeConverter::IntegerToString,
            (DataType::Float, DataType::Integer) => TypeConverter::FloatToInteger,
            (DataType::Integer, DataType::Float) => TypeConverter::IntegerToFloat,
            (DataType::String, DataType::Boolean) => TypeConverter::StringToBoolean,
            (DataType::Boolean, DataType::String) => TypeConverter::BooleanToString,
            (DataType::Timestamp, DataType::String) => TypeConverter::TimestampToString,
            (DataType::String, DataType::Timestamp) => TypeConverter::StringToTimestamp,
            _ => TypeConverter::Custom(format!("{:?}_to_{:?}", from, to)),
        }
    }

    fn convert_field_value(
        &self,
        value: &FieldValue,
        converter: &TypeConverter,
    ) -> SchemaResult<FieldValue> {
        match converter {
            TypeConverter::StringToInteger => {
                if let FieldValue::String(s) = value {
                    s.parse::<i64>()
                        .map(FieldValue::Integer)
                        .map_err(|_| SchemaError::Evolution {
                            from: "String".to_string(),
                            to: "Integer".to_string(),
                            reason: format!("Cannot convert '{}' to integer", s),
                        })
                } else {
                    Err(SchemaError::Evolution {
                        from: format!("{:?}", value),
                        to: "Integer".to_string(),
                        reason: "Expected string value".to_string(),
                    })
                }
            }
            TypeConverter::IntegerToString => {
                if let FieldValue::Integer(i) = value {
                    Ok(FieldValue::String(i.to_string()))
                } else {
                    Err(SchemaError::Evolution {
                        from: format!("{:?}", value),
                        to: "String".to_string(),
                        reason: "Expected integer value".to_string(),
                    })
                }
            }
            TypeConverter::IntegerToFloat => {
                if let FieldValue::Integer(i) = value {
                    Ok(FieldValue::Float(*i as f64))
                } else {
                    Err(SchemaError::Evolution {
                        from: format!("{:?}", value),
                        to: "Float".to_string(),
                        reason: "Expected integer value".to_string(),
                    })
                }
            }
            // TODO: Implement other converters
            _ => Ok(value.clone()),
        }
    }

    fn get_default_value(&self, field_name: &str) -> FieldValue {
        self.config
            .default_values
            .get(field_name)
            .cloned()
            .unwrap_or(FieldValue::Null)
    }

    fn check_backward_compatibility(&self, diff: &SchemaDiff) -> SchemaResult<()> {
        // Check that all new fields are optional
        for field in &diff.added_fields {
            if !field.nullable && field.default_value.is_none() {
                return Err(SchemaError::Incompatible {
                    reason: format!(
                        "Added field '{}' is required but has no default value",
                        field.name
                    ),
                });
            }
        }

        // Check that field modifications are compatible
        for modification in &diff.modified_fields {
            if !modification.is_compatible {
                return Err(SchemaError::Incompatible {
                    reason: format!(
                        "Field '{}' has incompatible changes",
                        modification.field_name
                    ),
                });
            }
        }

        Ok(())
    }

    fn check_forward_compatibility(&self, diff: &SchemaDiff) -> SchemaResult<()> {
        // Forward compatibility means old readers can read new data
        // Cannot remove fields
        if !diff.removed_fields.is_empty() {
            return Err(SchemaError::Incompatible {
                reason: format!(
                    "Removed {} fields, breaking forward compatibility",
                    diff.removed_fields.len()
                ),
            });
        }

        Ok(())
    }
}

impl Default for EvolutionConfig {
    fn default() -> Self {
        Self {
            allow_field_additions: true,
            allow_field_deletions: true,
            allow_type_changes: false,
            allow_nullability_changes: true,
            default_values: HashMap::new(),
        }
    }
}

impl Default for SchemaEvolution {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ferris::sql::ast::DataType;

    fn create_test_schema_v1() -> Schema {
        Schema {
            fields: vec![
                FieldDefinition::required("id".to_string(), DataType::Integer),
                FieldDefinition::required("name".to_string(), DataType::String),
            ],
            version: Some("1.0.0".to_string()),
            metadata: super::super::SchemaMetadata::new("test".to_string()),
        }
    }

    fn create_test_schema_v2() -> Schema {
        Schema {
            fields: vec![
                FieldDefinition::required("id".to_string(), DataType::Integer),
                FieldDefinition::required("name".to_string(), DataType::String),
                FieldDefinition::optional("email".to_string(), DataType::String),
            ],
            version: Some("2.0.0".to_string()),
            metadata: super::super::SchemaMetadata::new("test".to_string())
                .with_compatibility(CompatibilityMode::Backward),
        }
    }

    #[test]
    fn test_schema_evolution_compatibility() {
        let evolution = SchemaEvolution::new();
        let v1 = create_test_schema_v1();
        let v2 = create_test_schema_v2();

        assert!(evolution.can_evolve(&v1, &v2));
    }

    #[test]
    fn test_schema_diff_computation() {
        let evolution = SchemaEvolution::new();
        let v1 = create_test_schema_v1();
        let v2 = create_test_schema_v2();

        let diff = evolution.compute_diff(&v1, &v2);

        assert_eq!(diff.added_fields.len(), 1);
        assert_eq!(diff.added_fields[0].name, "email");
        assert_eq!(diff.removed_fields.len(), 0);
        assert_eq!(diff.modified_fields.len(), 0);
        assert!(diff.is_compatible);
    }

    #[test]
    fn test_migration_plan_creation() {
        let evolution = SchemaEvolution::new();
        let v1 = create_test_schema_v1();
        let v2 = create_test_schema_v2();

        let plan = evolution.create_migration_plan(&v1, &v2).unwrap();

        assert_eq!(plan.transformations.len(), 3); // id, name, email
        assert_eq!(plan.from_schema.version, Some("1.0.0".to_string()));
        assert_eq!(plan.to_schema.version, Some("2.0.0".to_string()));
    }
}
