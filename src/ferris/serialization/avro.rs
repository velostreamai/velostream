//! Avro serialization format implementation

use super::helpers::{
    avro_value_to_field_value, avro_value_to_field_value_with_schema, field_value_to_avro,
    field_value_to_avro_with_decimal_schema, field_value_to_internal, internal_to_field_value,
    DecimalSchemaInfo,
};
use super::{FieldValue, InternalValue, SerializationError, SerializationFormat};
use std::collections::HashMap;

/// Avro serialization implementation (feature-gated)
pub struct AvroFormat {
    writer_schema: apache_avro::Schema,
    reader_schema: apache_avro::Schema,
    decimal_fields: HashMap<String, DecimalSchemaInfo>,
}

impl AvroFormat {
    /// Create new Avro format with schema
    pub fn new(schema_json: &str) -> Result<Self, SerializationError> {
        let schema = apache_avro::Schema::parse_str(schema_json).map_err(|e| {
            SerializationError::FormatConversionFailed(format!("Invalid Avro schema: {}", e))
        })?;

        let decimal_fields = extract_decimal_fields(&schema)?;

        Ok(AvroFormat {
            writer_schema: schema.clone(),
            reader_schema: schema,
            decimal_fields,
        })
    }

    /// Create Avro format with separate reader and writer schemas (for schema evolution)
    pub fn with_schemas(
        writer_schema_json: &str,
        reader_schema_json: &str,
    ) -> Result<Self, SerializationError> {
        let writer_schema = apache_avro::Schema::parse_str(writer_schema_json).map_err(|e| {
            SerializationError::FormatConversionFailed(format!("Invalid writer schema: {}", e))
        })?;
        let reader_schema = apache_avro::Schema::parse_str(reader_schema_json).map_err(|e| {
            SerializationError::FormatConversionFailed(format!("Invalid reader schema: {}", e))
        })?;

        // Extract decimal fields from reader schema (used for deserialization)
        let decimal_fields = extract_decimal_fields(&reader_schema)?;

        Ok(AvroFormat {
            writer_schema,
            reader_schema,
            decimal_fields,
        })
    }

    /// Create a default Avro format with generic record schema
    pub fn default_format() -> Result<Self, SerializationError> {
        let schema_json = r#"
        {
            "type": "record",
            "name": "GenericRecord",
            "fields": [
                {"name": "data", "type": ["null", "string", "long", "double", "boolean", {"type": "map", "values": "string"}]}
            ]
        }
        "#;
        Self::new(schema_json)
    }
}

impl SerializationFormat for AvroFormat {
    fn serialize_record(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<Vec<u8>, SerializationError> {
        use apache_avro::Writer;

        // Convert record to Avro value (schema-aware for decimal fields)
        let avro_value = record_to_avro_value_with_schema(record, &self.decimal_fields)?;

        // Create writer and encode
        let mut writer = Writer::new(&self.writer_schema, Vec::new());
        writer.append(avro_value).map_err(|e| {
            SerializationError::SerializationFailed(format!("Avro serialization failed: {}", e))
        })?;

        writer.into_inner().map_err(|e| {
            SerializationError::SerializationFailed(format!(
                "Avro writer finalization failed: {}",
                e
            ))
        })
    }

    fn deserialize_record(
        &self,
        bytes: &[u8],
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        use apache_avro::Reader;

        let mut reader = Reader::with_schema(&self.reader_schema, bytes).map_err(|e| {
            SerializationError::DeserializationFailed(format!("Avro reader creation failed: {}", e))
        })?;

        // Read first record (assuming single record per message)
        if let Some(record_result) = reader.next() {
            let avro_value = record_result.map_err(|e| {
                SerializationError::DeserializationFailed(format!(
                    "Avro deserialization failed: {}",
                    e
                ))
            })?;

            return avro_value_to_record_with_schema(&avro_value, &self.decimal_fields);
        }

        Err(SerializationError::DeserializationFailed(
            "No records found in Avro data".to_string(),
        ))
    }

    fn to_execution_format(
        &self,
        record: &HashMap<String, FieldValue>,
    ) -> Result<HashMap<String, InternalValue>, SerializationError> {
        let mut execution_map = HashMap::new();

        for (key, field_value) in record {
            let internal_value = field_value_to_internal(field_value)?;
            execution_map.insert(key.clone(), internal_value);
        }

        Ok(execution_map)
    }

    fn from_execution_format(
        &self,
        data: &HashMap<String, InternalValue>,
    ) -> Result<HashMap<String, FieldValue>, SerializationError> {
        let mut record = HashMap::new();

        for (key, internal_value) in data {
            let field_value = internal_to_field_value(internal_value)?;
            record.insert(key.clone(), field_value);
        }

        Ok(record)
    }

    fn format_name(&self) -> &'static str {
        "Avro"
    }
}

// Helper functions

fn record_to_avro_value(
    record: &HashMap<String, FieldValue>,
) -> Result<apache_avro::types::Value, SerializationError> {
    use apache_avro::types::Value;

    let mut avro_fields = Vec::new();
    for (key, field_value) in record {
        let avro_value = match field_value {
            FieldValue::Null => {
                // For nullable fields in unions, wrap null in union with index 1
                // (assuming ["string", "null"] or similar patterns where null is typically second)
                Value::Union(1, Box::new(Value::Null))
            }
            _ => field_value_to_avro(field_value)?,
        };
        avro_fields.push((key.clone(), avro_value));
    }

    Ok(Value::Record(avro_fields))
}

fn record_to_avro_value_with_schema(
    record: &HashMap<String, FieldValue>,
    decimal_fields: &HashMap<String, DecimalSchemaInfo>,
) -> Result<apache_avro::types::Value, SerializationError> {
    use apache_avro::types::Value;

    let mut avro_fields = Vec::new();
    eprintln!("DEBUG: Serializing record with {} fields", record.len());
    eprintln!(
        "DEBUG: Decimal fields map has {} entries: {:?}",
        decimal_fields.len(),
        decimal_fields.keys().collect::<Vec<_>>()
    );

    for (key, field_value) in record {
        eprintln!(
            "DEBUG: Processing field '{}' with value type: {:?}",
            key,
            std::mem::discriminant(field_value)
        );
        let avro_value = match field_value {
            FieldValue::Null => {
                eprintln!("DEBUG: Field '{}' is null", key);
                // For nullable fields in unions, wrap null in union with index 1
                Value::Union(1, Box::new(Value::Null))
            }
            _ => {
                // Check if this field is a decimal field and get its schema info
                let decimal_info = decimal_fields.get(key);
                eprintln!("DEBUG: Field '{}' decimal_info = {:?}", key, decimal_info);
                field_value_to_avro_with_decimal_schema(field_value, decimal_info)?
            }
        };
        eprintln!("DEBUG: Converted field '{}' to Avro value", key);
        avro_fields.push((key.clone(), avro_value));
    }

    Ok(Value::Record(avro_fields))
}

fn avro_value_to_record(
    avro_value: &apache_avro::types::Value,
) -> Result<HashMap<String, FieldValue>, SerializationError> {
    match avro_value {
        apache_avro::types::Value::Record(fields) => {
            let mut record = HashMap::new();
            for (key, value) in fields {
                let field_value = avro_value_to_field_value(value)?;
                record.insert(key.clone(), field_value);
            }
            Ok(record)
        }
        _ => Err(SerializationError::DeserializationFailed(
            "Expected Avro record".to_string(),
        )),
    }
}

fn avro_value_to_record_with_schema(
    avro_value: &apache_avro::types::Value,
    decimal_fields: &HashMap<String, DecimalSchemaInfo>,
) -> Result<HashMap<String, FieldValue>, SerializationError> {
    match avro_value {
        apache_avro::types::Value::Record(fields) => {
            let mut record = HashMap::new();
            for (key, value) in fields {
                // Check if this field has decimal schema info
                let decimal_info = decimal_fields.get(key);
                let field_value = avro_value_to_field_value_with_schema(value, decimal_info)?;
                record.insert(key.clone(), field_value);
            }
            Ok(record)
        }
        _ => Err(SerializationError::DeserializationFailed(
            "Expected Avro record".to_string(),
        )),
    }
}

/// Extract decimal field information from Avro schema for schema-aware conversion
fn extract_decimal_fields(
    schema: &apache_avro::Schema,
) -> Result<HashMap<String, DecimalSchemaInfo>, SerializationError> {
    let mut decimal_fields = HashMap::new();

    match schema {
        apache_avro::Schema::Record(record_schema) => {
            eprintln!(
                "DEBUG: Processing record schema with {} fields",
                record_schema.fields.len()
            );
            for field in &record_schema.fields {
                eprintln!("DEBUG: Processing field: {}", field.name);
                eprintln!("DEBUG: Field schema: {:?}", field.schema);

                // Check custom attributes on the field itself (not just the schema)
                let field_attrs = &field.custom_attributes;
                if !field_attrs.is_empty() {
                    eprintln!(
                        "DEBUG: Field '{}' has custom attributes: {:?}",
                        field.name, field_attrs
                    );
                } else {
                    eprintln!("DEBUG: Field '{}' has no custom attributes", field.name);
                }

                if is_decimal_field_from_field(field) {
                    eprintln!("DEBUG: Field '{}' is a decimal field", field.name);
                    if let Some(decimal_info) = extract_decimal_info_from_field(field) {
                        eprintln!("DEBUG: Adding decimal field '{}' to map", field.name);
                        decimal_fields.insert(field.name.clone(), decimal_info);
                    }
                } else {
                    eprintln!("DEBUG: Field '{}' is NOT a decimal field", field.name);
                }
            }
        }
        _ => {
            // Not a record schema, no fields to extract
            eprintln!("DEBUG: Not a record schema, no fields to extract");
        }
    }

    eprintln!(
        "DEBUG: Final decimal_fields map contains {} entries",
        decimal_fields.len()
    );
    Ok(decimal_fields)
}

/// Check if a field schema represents a decimal field (using custom properties)
fn is_decimal_field(field_schema: &apache_avro::Schema) -> bool {
    match field_schema {
        apache_avro::Schema::Bytes => {
            // Check if this bytes field has custom decimal properties
            if let Some(attributes) = field_schema.custom_attributes() {
                eprintln!("DEBUG: Found bytes field with attributes: {:?}", attributes);
                let has_precision = attributes.contains_key("decimalPrecision");
                let has_scale = attributes.contains_key("decimalScale");
                eprintln!(
                    "DEBUG: Has decimalPrecision: {}, decimalScale: {}",
                    has_precision, has_scale
                );
                return has_precision && has_scale;
            } else {
                eprintln!("DEBUG: Bytes field has no custom attributes");
            }
        }
        apache_avro::Schema::Decimal(_) => {
            // Still support the old logicalType approach for compatibility
            eprintln!("DEBUG: Found Schema::Decimal field (legacy logicalType)");
            return true;
        }
        _ => {
            eprintln!("DEBUG: Field schema type: {:?}", field_schema);
        }
    }
    false
}

/// Extract decimal precision and scale from schema
fn extract_decimal_info(field_schema: &apache_avro::Schema) -> Option<DecimalSchemaInfo> {
    match field_schema {
        apache_avro::Schema::Decimal(decimal_schema) => {
            // Standard logicalType approach (gets converted to Decimal internally)
            let info = DecimalSchemaInfo {
                precision: decimal_schema.precision as u32,
                scale: decimal_schema.scale as u32,
                is_standard_logical_type: true, // This is the standard "logicalType": "decimal"
            };
            eprintln!("DEBUG: Extracted decimal info from Decimal schema - precision: {}, scale: {} (inner: {:?})", 
                     info.precision, info.scale, decimal_schema.inner);
            Some(info)
        }
        apache_avro::Schema::Bytes => {
            // Check for custom decimal properties (Flink-compatible approach)
            if let Some(attributes) = field_schema.custom_attributes() {
                // Try custom properties first
                if let (Some(precision_val), Some(scale_val)) = (
                    attributes.get("decimalPrecision"),
                    attributes.get("decimalScale"),
                ) {
                    let precision = precision_val.as_u64()? as u32;
                    let scale = scale_val.as_u64()? as u32;
                    eprintln!(
                        "DEBUG: Extracted from custom properties - precision: {}, scale: {}",
                        precision, scale
                    );
                    return Some(DecimalSchemaInfo {
                        precision,
                        scale,
                        is_standard_logical_type: false, // Custom properties approach
                    });
                }

                // Fall back to standard logicalType approach
                let logical_type = attributes.get("logicalType")?.as_str()?;
                if logical_type == "decimal" {
                    let precision = attributes.get("precision")?.as_u64()? as u32;
                    let scale = attributes.get("scale")?.as_u64()? as u32;
                    eprintln!(
                        "DEBUG: Extracted from logicalType - precision: {}, scale: {}",
                        precision, scale
                    );
                    return Some(DecimalSchemaInfo {
                        precision,
                        scale,
                        is_standard_logical_type: false, // Custom properties approach
                    });
                }
            }
            None
        }
        _ => None,
    }
}

/// Extract decimal info from an Avro record field (including field-level custom attributes)
fn extract_decimal_info_from_field(
    field: &apache_avro::schema::RecordField,
) -> Option<DecimalSchemaInfo> {
    // First try the schema-based extraction
    if let Some(info) = extract_decimal_info(&field.schema) {
        return Some(info);
    }

    // Then try field-level custom attributes
    let field_attrs = &field.custom_attributes;
    if !field_attrs.is_empty() {
        eprintln!(
            "DEBUG: Checking field-level custom attributes: {:?}",
            field_attrs
        );

        // Try custom decimal properties
        if let (Some(precision_val), Some(scale_val)) = (
            field_attrs.get("decimalPrecision"),
            field_attrs.get("decimalScale"),
        ) {
            if let (Some(precision), Some(scale)) = (precision_val.as_u64(), scale_val.as_u64()) {
                eprintln!("DEBUG: Extracted from field-level custom properties - precision: {}, scale: {}", precision, scale);
                return Some(DecimalSchemaInfo {
                    precision: precision as u32,
                    scale: scale as u32,
                    is_standard_logical_type: false, // Custom field-level properties
                });
            }
        }

        // Try logicalType approach at field level
        if let Some(logical_type) = field_attrs.get("logicalType") {
            if logical_type.as_str() == Some("decimal") {
                if let (Some(precision_val), Some(scale_val)) =
                    (field_attrs.get("precision"), field_attrs.get("scale"))
                {
                    if let (Some(precision), Some(scale)) =
                        (precision_val.as_u64(), scale_val.as_u64())
                    {
                        eprintln!("DEBUG: Extracted from field-level logicalType - precision: {}, scale: {}", precision, scale);
                        return Some(DecimalSchemaInfo {
                            precision: precision as u32,
                            scale: scale as u32,
                            is_standard_logical_type: true, // Field-level standard logicalType
                        });
                    }
                }
            }
        }
    }

    None
}

/// Check if a field represents a decimal field (using field-level custom attributes)
fn is_decimal_field_from_field(field: &apache_avro::schema::RecordField) -> bool {
    // First check the schema-based approach
    if is_decimal_field(&field.schema) {
        return true;
    }

    // Then check field-level custom attributes
    let field_attrs = &field.custom_attributes;
    if !field_attrs.is_empty() {
        eprintln!(
            "DEBUG: Checking field-level custom attributes for decimal: {:?}",
            field_attrs
        );

        // Check for bytes type with custom decimal properties
        if matches!(field.schema, apache_avro::Schema::Bytes) {
            let has_precision = field_attrs.contains_key("decimalPrecision");
            let has_scale = field_attrs.contains_key("decimalScale");
            eprintln!(
                "DEBUG: Bytes field - has decimalPrecision: {}, decimalScale: {}",
                has_precision, has_scale
            );
            if has_precision && has_scale {
                return true;
            }

            // Also check for logicalType at field level
            if let Some(logical_type) = field_attrs.get("logicalType") {
                if logical_type.as_str() == Some("decimal") {
                    eprintln!("DEBUG: Found logicalType=decimal at field level");
                    return true;
                }
            }
        }
    }

    false
}
