use serde::{Deserialize, Serialize};

use prost::Message;

use apache_avro::{
    from_avro_datum, to_avro_datum, types::Value as AvroValue, Schema as AvroSchema,
};

use std::io::Cursor;

// Use the unified serialization error from the main serialization module
pub use crate::ferris::serialization::SerializationError;

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

/// Modern async serialization trait using Rust 2024 edition features
pub trait AsyncSerializer<T: Send + Sync> {
    /// Async serialize an object to bytes (useful for large objects or I/O-heavy serialization)
    fn serialize_async(
        &self,
        value: &T,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, SerializationError>> + Send
    where
        Self: std::marker::Sync,
    {
        async move {
            // Default implementation falls back to sync version
            self.serialize_sync(value)
        }
    }

    /// Async deserialize bytes to an object (useful for streaming deserialization)
    fn deserialize_async(
        &self,
        bytes: &[u8],
    ) -> impl std::future::Future<Output = Result<T, SerializationError>> + Send
    where
        Self: std::marker::Sync,
    {
        async move {
            // Default implementation falls back to sync version
            self.deserialize_sync(bytes)
        }
    }

    /// Synchronous serialize (provided for compatibility)
    fn serialize_sync(&self, value: &T) -> Result<Vec<u8>, SerializationError>;

    /// Synchronous deserialize (provided for compatibility)
    fn deserialize_sync(&self, bytes: &[u8]) -> Result<T, SerializationError>;

    /// Stream serialize large objects chunk by chunk
    fn serialize_stream(
        &self,
        value: &T,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, SerializationError>> + Send
    where
        Self: Sync,
    {
        self.serialize_async(value)
    }
}

// JSON Serialization Helpers
//==========================

/// Serialize a struct to JSON bytes
pub fn to_json<T: Serialize>(value: &T) -> Result<Vec<u8>, SerializationError> {
    serde_json::to_vec(value)
        .map_err(|e| SerializationError::json_error("Failed to serialize to JSON bytes", e))
}

/// Deserialize JSON bytes to a struct
pub fn from_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, SerializationError> {
    serde_json::from_slice(bytes)
        .map_err(|e| SerializationError::json_error("Failed to deserialize from JSON bytes", e))
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

/// Modern async implementation of JsonSerializer using Rust 2024 features
impl<T> AsyncSerializer<T> for JsonSerializer
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync,
{
    fn serialize_sync(&self, value: &T) -> Result<Vec<u8>, SerializationError> {
        to_json(value)
    }

    fn deserialize_sync(&self, bytes: &[u8]) -> Result<T, SerializationError> {
        from_json(bytes)
    }

    async fn serialize_async(&self, value: &T) -> Result<Vec<u8>, SerializationError>
    where
        Self: std::marker::Sync,
    {
        // For large JSON objects, we could spawn this on a thread pool
        // For now, just call the sync version
        tokio::task::yield_now().await; // Yield to allow other tasks to run
        self.serialize_sync(value)
    }

    async fn deserialize_async(&self, bytes: &[u8]) -> Result<T, SerializationError>
    where
        Self: std::marker::Sync,
    {
        // For large JSON parsing, we could spawn this on a thread pool
        // For now, just call the sync version
        tokio::task::yield_now().await; // Yield to allow other tasks to run
        self.deserialize_sync(bytes)
    }
}

// Avro Serialization Helpers
//==========================

/// Serialize a value to Avro bytes using a schema
pub fn to_avro(value: &AvroValue, schema: &AvroSchema) -> Result<Vec<u8>, SerializationError> {
    to_avro_datum(schema, value.clone())
        .map_err(|e| SerializationError::avro_error("Failed to serialize to Avro bytes", e))
}

/// Deserialize Avro bytes to a value using a schema
pub fn from_avro(bytes: &[u8], schema: &AvroSchema) -> Result<AvroValue, SerializationError> {
    let mut cursor = Cursor::new(bytes);
    from_avro_datum(schema, &mut cursor, None)
        .map_err(|e| SerializationError::avro_error("Failed to deserialize from Avro bytes", e))
}

/// Avro serializer implementation
pub struct AvroSerializer {
    schema: AvroSchema,
}

impl AvroSerializer {
    pub fn new(schema: AvroSchema) -> Self {
        Self { schema }
    }
}

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

/// Serialize a Protocol Buffers message to bytes
pub fn to_proto<T: Message>(message: &T) -> Result<Vec<u8>, SerializationError> {
    let mut buf = Vec::new();
    match message.encode(&mut buf) {
        Ok(_) => Ok(buf),
        Err(e) => Err(SerializationError::protobuf_error(
            "Failed to encode protobuf message",
            e,
        )),
    }
}

/// Deserialize bytes to a Protocol Buffers message
pub fn from_proto<T: Message + Default>(bytes: &[u8]) -> Result<T, SerializationError> {
    match T::decode(bytes) {
        Ok(message) => Ok(message),
        Err(e) => Err(SerializationError::protobuf_error(
            "Failed to decode protobuf message",
            e,
        )),
    }
}

/// Protocol Buffers serializer implementation
pub struct ProtoSerializer<T: Message + Default>(std::marker::PhantomData<T>);

impl<T: Message + Default> Default for ProtoSerializer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Message + Default> ProtoSerializer<T> {
    pub fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

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
            .map_err(|e| SerializationError::SchemaError(format!("Invalid UTF-8: {}", e)))
    }
}
