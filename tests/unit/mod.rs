// Unit Tests - Fast tests with no external dependencies
// These tests should run quickly and not require Kafka or other external services

pub mod config_validation_test;
pub mod headers_edge_cases_test;
pub mod message_metadata_test;
// Shared test utilities and messages
pub mod test_messages;
// Legacy core test modules (remaining)
mod builder_pattern_test;
pub mod common;
mod error_context_test;
mod error_handling_test;
mod serialization_unit_test;
pub mod test_utils;
