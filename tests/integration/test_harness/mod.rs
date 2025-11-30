//! Test Harness Integration Tests
//!
//! Organized into logical modules:
//! - `infra_test`: Testcontainers Kafka infrastructure tests
//! - `assertion_test`: Assertion framework tests
//! - `generator_test`: Schema-driven data generation tests
//! - `execution_test`: QueryExecutor and E2E SQL execution tests
//! - `config_test`: Configuration and spec parsing tests

pub mod assertion_test;
pub mod config_test;
pub mod execution_test;
pub mod generator_test;
pub mod infra_test;
