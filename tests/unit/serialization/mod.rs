//! Serialization format tests
//!
//! Tests for all supported serialization formats including JSON, Avro, and Protobuf

pub mod common_test_data;
pub mod json_serialization_tests;

#[cfg(feature = "avro")]
pub mod avro_serialization_tests;

#[cfg(feature = "avro")]
pub mod avro_scaled_integer_test;

#[cfg(feature = "protobuf")]
pub mod protobuf_serialization_tests;

pub mod serialization_factory_tests;
