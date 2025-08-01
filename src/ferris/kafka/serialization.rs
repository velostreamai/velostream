use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[cfg(feature = "protobuf")]
use prost::Message;

#[cfg(feature = "avro")]
use apache_avro::{
    from_avro_datum, to_avro_datum, types::Value as AvroValue, Error as AvroError,
    Schema as AvroSchema,
};

/// Trait for objects that can be serialized to bytes for Kafka messages
pub trait KafkaSerialize {
    /// Serializes the object to bytes
    fn to_bytes(&self) -> Result<Vec<u8>, SerializationError>;
}

/// Trait for deserializing bytes from Kafka messages into objects
pub trait KafkaDeserialize<T> {
    /// Deserializes bytes into an object
    fn from_bytes(bytes: &[u8]) -> Result<T, SerializationError>;
}

/// Trait for serializers that can convert between objects and bytes
pub trait Serializer<T> {
    /// Serialize an object to bytes
    fn serialize(&self, value: &T) -> Result<Vec<u8>, SerializationError>;

    /// Deserialize bytes to an object
    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerializationError>;
}

/// Enumeration of possible serialization errors
#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Protocol Buffers error: {0}")]
    ProtoBuf(String),

    #[cfg(feature = "avro")]
    #[error("Avro serialization error: {0}")]
    Avro(#[from] AvroError),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Feature not enabled: {0}")]
    FeatureNotEnabled(String),
}

// JSON Serialization Helpers
//==========================

/// Serialize a struct to JSON bytes
pub fn to_json<T: Serialize>(value: &T) -> Result<Vec<u8>, SerializationError> {
    Ok(serde_json::to_vec(value)?)
}

/// Deserialize JSON bytes to a struct
pub fn from_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, SerializationError> {
    Ok(serde_json::from_slice(bytes)?)
}

// Implementation for any type that implements Serialize
impl<T: Serialize> KafkaSerialize for T {
    fn to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        to_json(self)
    }
}

/// JSON serializer implementation
pub struct JsonSerializer;

impl<T> Serializer<T> for JsonSerializer
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn serialize(&self, value: &T) -> Result<Vec<u8>, SerializationError> {
        to_json(value)
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerializationError> {
        from_json(bytes)
    }
}

// Avro Serialization Helpers
//==========================

#[cfg(feature = "avro")]
/// Serialize a value to Avro bytes using a schema
pub fn to_avro(value: &AvroValue, schema: &AvroSchema) -> Result<Vec<u8>, SerializationError> {
    Ok(to_avro_datum(schema, value.clone())?)
}

#[cfg(feature = "avro")]
/// Deserialize Avro bytes to a value using a schema
pub fn from_avro(bytes: &[u8], schema: &AvroSchema) -> Result<AvroValue, SerializationError> {
    let mut cursor = Cursor::new(bytes);
    Ok(from_avro_datum(schema, &mut cursor, None)?)
}

#[cfg(not(feature = "avro"))]
/// Placeholder when Avro feature is not enabled
pub fn to_avro(_value: &(), _schema: &()) -> Result<Vec<u8>, SerializationError> {
    Err(SerializationError::FeatureNotEnabled("avro".to_string()))
}

#[cfg(not(feature = "avro"))]
/// Placeholder when Avro feature is not enabled
pub fn from_avro(_bytes: &[u8], _schema: &()) -> Result<(), SerializationError> {
    Err(SerializationError::FeatureNotEnabled("avro".to_string()))
}

#[cfg(feature = "avro")]
/// Avro serializer implementation
pub struct AvroSerializer {
    schema: AvroSchema,
}

#[cfg(feature = "avro")]
impl AvroSerializer {
    pub fn new(schema: AvroSchema) -> Self {
        Self { schema }
    }
}

#[cfg(feature = "avro")]
impl Serializer<AvroValue> for AvroSerializer {
    fn serialize(&self, value: &AvroValue) -> Result<Vec<u8>, SerializationError> {
        to_avro(value, &self.schema)
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<AvroValue, SerializationError> {
        from_avro(bytes, &self.schema)
    }
}

// Protocol Buffers Serialization Helpers
//======================================

#[cfg(feature = "protobuf")]
/// Serialize a Protocol Buffers message to bytes
pub fn to_proto<T: Message>(message: &T) -> Result<Vec<u8>, SerializationError> {
    let mut buf = Vec::new();
    match message.encode(&mut buf) {
        Ok(_) => Ok(buf),
        Err(e) => Err(SerializationError::ProtoBuf(e.to_string())),
    }
}

#[cfg(feature = "protobuf")]
/// Deserialize bytes to a Protocol Buffers message
pub fn from_proto<T: Message + Default>(bytes: &[u8]) -> Result<T, SerializationError> {
    match T::decode(bytes) {
        Ok(message) => Ok(message),
        Err(e) => Err(SerializationError::ProtoBuf(e.to_string())),
    }
}

#[cfg(not(feature = "protobuf"))]
/// Placeholder when Protocol Buffers feature is not enabled
pub fn to_proto<T>(_message: &T) -> Result<Vec<u8>, SerializationError> {
    Err(SerializationError::FeatureNotEnabled(
        "protobuf".to_string(),
    ))
}

#[cfg(not(feature = "protobuf"))]
/// Placeholder when Protocol Buffers feature is not enabled
pub fn from_proto<T>(_bytes: &[u8]) -> Result<T, SerializationError> {
    Err(SerializationError::FeatureNotEnabled(
        "protobuf".to_string(),
    ))
}

#[cfg(feature = "protobuf")]
/// Protocol Buffers serializer implementation
pub struct ProtoSerializer<T: Message + Default>(std::marker::PhantomData<T>);

#[cfg(feature = "protobuf")]
impl<T: Message + Default> ProtoSerializer<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

#[cfg(feature = "protobuf")]
impl<T: Message + Default> Serializer<T> for ProtoSerializer<T> {
    fn serialize(&self, message: &T) -> Result<Vec<u8>, SerializationError> {
        to_proto(message)
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<T, SerializationError> {
        from_proto(bytes)
    }
}

// Raw Bytes Serialization (No-op)
//===================================

/// Raw bytes serializer that performs no serialization/deserialization
/// This is useful for sending raw byte arrays directly to Kafka
#[derive(Clone)]
pub struct BytesSerializer;

impl Serializer<Vec<u8>> for BytesSerializer {
    fn serialize(&self, value: &Vec<u8>) -> Result<Vec<u8>, SerializationError> {
        Ok(value.clone())
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<Vec<u8>, SerializationError> {
        Ok(bytes.to_vec())
    }
}

/// String serializer that converts strings to/from UTF-8 bytes
#[derive(Clone)]
pub struct StringSerializer;

impl Serializer<String> for StringSerializer {
    fn serialize(&self, value: &String) -> Result<Vec<u8>, SerializationError> {
        Ok(value.as_bytes().to_vec())
    }

    fn deserialize(&self, bytes: &[u8]) -> Result<String, SerializationError> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| SerializationError::Schema(format!("Invalid UTF-8: {}", e)))
    }
}
