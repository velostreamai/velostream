//! SQL Application Test Harness
//!
//! A comprehensive testing framework for Velostream SQL applications that provides:
//! - Testcontainers-based Kafka infrastructure
//! - Schema-driven test data generation
//! - Sequential query execution with sink capture
//! - Template-based assertions with rich diagnostics
//! - AI-powered failure analysis (optional)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     Test Harness Flow                           │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  1. Parse SQL file & test spec                                  │
//! │  2. Start testcontainers (Kafka)                                │
//! │  3. Override configs (bootstrap.servers, topics, paths)         │
//! │  4. For each query:                                             │
//! │     a. Generate input data from schemas                         │
//! │     b. Publish to source topics                                 │
//! │     c. Execute query via StreamJobServer                        │
//! │     d. Capture sink output                                      │
//! │     e. Run assertions                                           │
//! │  5. Generate report                                             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```bash
//! # Run tests
//! velo-test run app.sql --spec test_spec.yaml
//!
//! # Validate SQL syntax only
//! velo-test validate app.sql
//!
//! # Generate test spec template
//! velo-test init app.sql --output test_spec.yaml
//!
//! # Infer schemas from SQL and data files
//! velo-test infer-schema app.sql --data-dir data/ --output schemas/
//!
//! # Stress testing
//! velo-test stress app.sql --records 100000 --duration 60
//! ```

pub mod ai;
pub mod assertions;
pub mod capture;
pub mod cli;
pub mod config_override;
pub mod error;
pub mod executor;
pub mod generator;
pub mod inference;
pub mod infra;
pub mod report;
pub mod schema;
pub mod spec;
pub mod spec_generator;
pub mod stress;

// Re-export main types for convenience
pub use config_override::{ConfigOverrideBuilder, ConfigOverrides};
pub use error::TestHarnessError;
pub use inference::SchemaInferencer;
pub use infra::{SharedTestInfra, TestHarnessInfra};
pub use schema::Schema;
pub use spec::TestSpec;
pub use spec_generator::SpecGenerator;
pub use stress::{MemoryTracker, StressConfig, StressMetrics, StressRunner};
