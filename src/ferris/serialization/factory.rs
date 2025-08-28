//! Factory for creating serialization formats

use super::json::JsonFormat;
use super::{SerializationError, SerializationFormat};

#[cfg(feature = "avro")]
use super::avro::AvroFormat;

#[cfg(feature = "protobuf")]
use super::protobuf::ProtobufFormat;

/// Factory for creating serialization formats
///
/// This factory provides a centralized way to create serialization format instances
/// by name or with custom configuration. It handles feature detection and returns
/// appropriate errors for unsupported formats.
///
/// # Examples
///
/// ```rust
/// use ferrisstreams::ferris::serialization::SerializationFormatFactory;
///
/// // Create JSON format (always available)
/// let json = SerializationFormatFactory::create_format("json").unwrap();
///
/// // Get list of supported formats
/// let formats = SerializationFormatFactory::supported_formats();
/// println!("Available: {:?}", formats);
///
/// // Get default format
/// let default = SerializationFormatFactory::default_format();
/// ```
pub struct SerializationFormatFactory;

impl SerializationFormatFactory {
    /// Create a serialization format by name
    ///
    /// Creates a format instance using the default configuration for the specified
    /// format. For schema-based formats like Avro, this uses a generic schema.
    ///
    /// # Supported Format Names
    ///
    /// - `"json"` - Always available
    /// - `"avro"` - Requires `avro` feature, uses default schema
    /// - `"protobuf"` or `"proto"` - Requires `protobuf` feature
    ///
    /// # Arguments
    ///
    /// * `format_name` - Case-insensitive format name
    ///
    /// # Returns
    ///
    /// * `Ok(Box<dyn SerializationFormat>)` - Format instance
    /// * `Err(SerializationError::UnsupportedType)` - If format is unknown or feature not enabled
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ferrisstreams::ferris::serialization::SerializationFormatFactory;
    ///
    /// let json_format = SerializationFormatFactory::create_format("json")?;
    /// assert_eq!(json_format.format_name(), "JSON");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create_format(
        format_name: &str,
    ) -> Result<Box<dyn SerializationFormat>, SerializationError> {
        match format_name.to_lowercase().as_str() {
            "json" => Ok(Box::new(JsonFormat)),
            #[cfg(feature = "avro")]
            "avro" => {
                let avro_format = AvroFormat::default_format().map_err(|e| {
                    SerializationError::FormatConversionFailed(format!(
                        "Failed to create Avro format: {}",
                        e
                    ))
                })?;
                Ok(Box::new(avro_format))
            }
            #[cfg(feature = "protobuf")]
            "protobuf" | "proto" => {
                // Generic protobuf format - in practice you'd specify the message type
                Ok(Box::new(ProtobufFormat::<()>::new()))
            }
            _ => Err(SerializationError::UnsupportedType(format!(
                "Unknown format: {}",
                format_name
            ))),
        }
    }

    /// Create Avro format with custom schema
    #[cfg(feature = "avro")]
    pub fn create_avro_format(
        schema_json: &str,
    ) -> Result<Box<dyn SerializationFormat>, SerializationError> {
        let avro_format = AvroFormat::new(schema_json)?;
        Ok(Box::new(avro_format))
    }

    /// Create Avro format with schema evolution support
    #[cfg(feature = "avro")]
    pub fn create_avro_format_with_schemas(
        writer_schema_json: &str,
        reader_schema_json: &str,
    ) -> Result<Box<dyn SerializationFormat>, SerializationError> {
        let avro_format = AvroFormat::with_schemas(writer_schema_json, reader_schema_json)?;
        Ok(Box::new(avro_format))
    }

    /// Create Protobuf format with specific message type
    #[cfg(feature = "protobuf")]
    pub fn create_protobuf_format<T>() -> Box<dyn SerializationFormat>
    where
        T: prost::Message + Default + Clone + Send + Sync + 'static,
    {
        Box::new(ProtobufFormat::<T>::new())
    }

    /// Get list of supported formats
    pub fn supported_formats() -> Vec<&'static str> {
        let mut formats = vec!["json"];

        #[cfg(feature = "avro")]
        formats.push("avro");

        #[cfg(feature = "protobuf")]
        {
            formats.push("protobuf");
            formats.push("proto");
        }

        formats
    }

    /// Get default format (JSON)
    pub fn default_format() -> Box<dyn SerializationFormat> {
        Box::new(JsonFormat)
    }
}
