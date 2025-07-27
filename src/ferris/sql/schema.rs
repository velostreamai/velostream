use std::collections::HashMap;
use std::sync::Arc;
use crate::ferris::sql::ast::DataType;
use crate::ferris::sql::error::{SqlError, SqlResult};
use crate::ferris::kafka::Message;
use futures::Stream;
use serde_json::Value;

/// Schema definition for streaming data
#[derive(Debug, Clone)]
pub struct Schema {
    fields: Vec<Field>,
    field_map: HashMap<String, usize>,
    event_time_column: Option<String>,
    primary_key_columns: Vec<String>,
}

/// Field definition within a schema
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

/// Handle to a registered stream for SQL operations
#[derive(Debug, Clone)]
pub struct StreamHandle {
    pub name: String,
    pub schema: Schema,
    pub stream_type: StreamType,
}

/// Type of stream source
#[derive(Debug, Clone)]
pub enum StreamType {
    /// Kafka topic stream
    KafkaStream {
        topic: String,
        partition_count: Option<i32>,
    },
    /// KTable (materialized view)
    KTable {
        topic: String,
        compacted: bool,
    },
    /// Derived stream from query
    DerivedStream {
        source_query: String,
    },
}

/// Trait for stream sources that can be queried
pub trait StreamSource: Send + Sync {
    /// Get the schema of this stream
    fn schema(&self) -> &Schema;
    
    /// Get a stream of records
    fn record_stream(&self) -> Box<dyn Stream<Item = SqlResult<StreamRecord>> + Send + Unpin>;
    
    /// Get stream metadata
    fn metadata(&self) -> StreamMetadata;
}

/// A record from a stream with typed values
#[derive(Debug, Clone)]
pub struct StreamRecord {
    pub values: HashMap<String, Value>,
    pub timestamp: Option<i64>,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
}

/// Metadata about a stream
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    pub record_count: Option<u64>,
    pub last_updated: Option<i64>,
    pub partitions: Vec<PartitionMetadata>,
}

/// Metadata about a stream partition
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    pub partition_id: i32,
    pub low_watermark: i64,
    pub high_watermark: i64,
}

impl Schema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Schema {
            fields: Vec::new(),
            field_map: HashMap::new(),
            event_time_column: None,
            primary_key_columns: Vec::new(),
        }
    }
    
    /// Add a field to the schema
    pub fn add_field(mut self, name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        let name = name.into();
        let index = self.fields.len();
        
        self.fields.push(Field {
            name: name.clone(),
            data_type,
            nullable,
            metadata: HashMap::new(),
        });
        
        self.field_map.insert(name, index);
        self
    }
    
    /// Set the event time column
    pub fn set_event_time_column(mut self, column: impl Into<String>) -> Self {
        self.event_time_column = Some(column.into());
        self
    }
    
    /// Add a primary key column
    pub fn add_primary_key(mut self, column: impl Into<String>) -> Self {
        self.primary_key_columns.push(column.into());
        self
    }
    
    /// Get a field by name
    pub fn get_field(&self, name: &str) -> SqlResult<&Field> {
        self.field_map.get(name)
            .and_then(|&index| self.fields.get(index))
            .ok_or_else(|| SqlError::schema_error(
                format!("Column '{}' not found in schema", name),
                Some(name.to_string())
            ))
    }
    
    /// Get field index by name
    pub fn get_field_index(&self, name: &str) -> SqlResult<usize> {
        self.field_map.get(name)
            .copied()
            .ok_or_else(|| SqlError::schema_error(
                format!("Column '{}' not found in schema", name),
                Some(name.to_string())
            ))
    }
    
    /// Get all field names
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }
    
    /// Get the event time column if set
    pub fn event_time_column(&self) -> Option<&str> {
        self.event_time_column.as_deref()
    }
    
    /// Get primary key columns
    pub fn primary_key_columns(&self) -> &[String] {
        &self.primary_key_columns
    }
    
    /// Validate that a record matches this schema
    pub fn validate_record(&self, record: &HashMap<String, Value>) -> SqlResult<()> {
        // Check that all non-nullable fields are present
        for field in &self.fields {
            if !field.nullable && !record.contains_key(&field.name) {
                return Err(SqlError::schema_error(
                    format!("Required field '{}' is missing", field.name),
                    Some(field.name.clone())
                ));
            }
            
            // Check type compatibility if field is present
            if let Some(value) = record.get(&field.name) {
                self.validate_field_type(&field.name, &field.data_type, value)?;
            }
        }
        
        Ok(())
    }
    
    /// Validate that a value matches the expected data type
    fn validate_field_type(&self, field_name: &str, expected_type: &DataType, value: &Value) -> SqlResult<()> {
        let is_valid = match (expected_type, value) {
            (DataType::String, Value::String(_)) => true,
            (DataType::Integer, Value::Number(n)) => n.is_i64(),
            (DataType::Long, Value::Number(n)) => n.is_i64(),
            (DataType::Float, Value::Number(n)) => n.is_f64(),
            (DataType::Double, Value::Number(n)) => n.is_f64(),
            (DataType::Boolean, Value::Bool(_)) => true,
            (DataType::Timestamp, Value::Number(n)) => n.is_i64(),
            (DataType::Bytes, Value::String(_)) => true, // Base64 encoded
            (DataType::Array(_), Value::Array(_)) => true, // TODO: Validate element types
            (DataType::Map(_, _), Value::Object(_)) => true, // TODO: Validate key/value types
            (_, Value::Null) => true, // Null values are always valid
            _ => false,
        };
        
        if !is_valid {
            return Err(SqlError::type_error(
                format!("{:?}", expected_type),
                format!("{:?}", value),
                Some(field_name.to_string())
            ));
        }
        
        Ok(())
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl Field {
    /// Create a new field
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Field {
            name: name.into(),
            data_type,
            nullable,
            metadata: HashMap::new(),
        }
    }
    
    /// Add metadata to this field
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

impl StreamHandle {
    /// Create a new stream handle
    pub fn new(name: impl Into<String>, schema: Schema, stream_type: StreamType) -> Self {
        StreamHandle {
            name: name.into(),
            schema,
            stream_type,
        }
    }
    
    /// Check if this is a table (KTable)
    pub fn is_table(&self) -> bool {
        matches!(self.stream_type, StreamType::KTable { .. })
    }
    
    /// Check if this is a stream
    pub fn is_stream(&self) -> bool {
        matches!(self.stream_type, StreamType::KafkaStream { .. } | StreamType::DerivedStream { .. })
    }
}

impl StreamRecord {
    /// Create a new stream record
    pub fn new(values: HashMap<String, Value>) -> Self {
        StreamRecord {
            values,
            timestamp: None,
            partition: None,
            offset: None,
        }
    }
    
    /// Set timestamp
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
    
    /// Set partition info
    pub fn with_partition_info(mut self, partition: i32, offset: i64) -> Self {
        self.partition = Some(partition);
        self.offset = Some(offset);
        self
    }
    
    /// Get a value by field name
    pub fn get_value(&self, field_name: &str) -> Option<&Value> {
        self.values.get(field_name)
    }
    
    /// Get event time if available
    pub fn event_time(&self, schema: &Schema) -> Option<i64> {
        if let Some(time_col) = schema.event_time_column() {
            self.get_value(time_col)
                .and_then(|v| v.as_i64())
        } else {
            self.timestamp
        }
    }
}