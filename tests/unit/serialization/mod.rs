//! Serialization format tests
//!
//! Tests for all supported serialization formats including JSON, Avro, and Protobuf
//! All formats are runtime configurable without feature gates.

pub mod avro_scaled_integer_test;
pub mod avro_serialization_tests;
pub mod codec_integration_tests;
pub mod common_test_data;
pub mod enhanced_error_test;
pub mod json_serialization_tests;
pub mod protobuf_serialization_tests;
