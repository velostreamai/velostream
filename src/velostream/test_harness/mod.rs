//! SQL Application Test Harness
//!
//! A comprehensive testing framework for Velostream SQL applications that provides:
//! - Testcontainers-based Kafka infrastructure
//! - Schema-driven test data generation
//! - Sequential query execution with sink capture
//! - Statement-by-statement debugging with breakpoints
//! - Template-based assertions with rich diagnostics
//! - Temporal and statistical assertions
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
//!
//! # Step-by-step execution
//! velo-test run app.sql --step
//!
//! # Interactive debugging with breakpoints
//! velo-test debug app.sql --breakpoint query_name
//! ```
//!
//! # Statement-by-Statement Debugging
//!
//! The test harness supports interactive debugging for complex SQL pipelines:
//!
//! ```rust,ignore
//! use velostream::test_harness::{StatementExecutor, ExecutionMode, DebugSession};
//!
//! // Create executor in step mode
//! let mut executor = StatementExecutor::new(infra, timeout)
//!     .with_mode(ExecutionMode::Step);
//!
//! // Add breakpoints
//! executor.add_breakpoint("aggregation_query");
//!
//! // Load and execute
//! executor.load_sql(&sql_file)?;
//! while let Some(result) = executor.step_next().await? {
//!     println!("Executed: {} - {}", result.name, result.success);
//! }
//! ```

// Allow dead_code and unused_imports for public API items used by the velo-test binary
// These exports are consumed by src/bin/velo-test.rs, not by the library itself
#![allow(dead_code)]
#![allow(unused_imports)]

pub mod ai;
pub mod annotate;
pub mod assertions;
pub mod capture;
pub mod cli;
pub mod config_override;
pub mod dlq;
pub mod error;
pub mod executor;
pub mod fault_injection;
pub mod file_io;
pub mod generator;
pub mod health;
pub mod inference;
pub mod infra;
pub mod log_capture;
pub mod report;
pub mod scaffold;
pub mod schema;
pub mod spec;
pub mod spec_generator;
pub mod statement_executor;
pub mod stress;
pub mod table_state;
pub mod utils;

// Re-export main types for convenience - used by velo-test binary
pub use annotate::{
    AnnotateConfig, Annotator, DataHint, DataHintParser, DataHintType, DetectedMetric,
    GlobalDataHints, MetricType, QueryAnalysis, QueryType, SqlAnalysis,
};
pub use capture::{
    CaptureConfig, CaptureFormat, SinkCapture, json_to_field_values, json_type_name,
};
pub use config_override::{ConfigOverrideBuilder, ConfigOverrides};
pub use dlq::{CapturedDlqOutput, DlqCapture, DlqConfig, DlqRecord, DlqStatistics, ErrorType};
pub use error::TestHarnessError;
pub use fault_injection::{FaultInjectionConfig, FaultInjector, MalformationType};
pub use file_io::{FileSinkFactory, FileSourceFactory};
pub use generator::{SchemaDataGenerator, parse_time_spec};
pub use health::{
    CheckResult, CheckStatus, ConsumerGroupInfo, ContainerInfo, HealthCheckType, HealthChecker,
    HealthConfig, HealthReport, HealthSummary, ProcessInfo, TopicHealthInfo,
};
pub use inference::SchemaInferencer;
pub use infra::{SharedTestInfra, TestHarnessInfra, create_kafka_config};
pub use log_capture::{
    CapturedLogEntry, clear as clear_log_buffer, entries_since, errors_since,
    init_capturing_logger, init_capturing_logger_with_level, is_capture_enabled,
    recent_entries as recent_log_entries, set_capture_enabled, stats as log_stats, warnings_since,
};
pub use scaffold::{DetectedStructure, ScaffoldConfig, ScaffoldStyle, Scaffolder};
pub use schema::{Schema, generate_schema_from_hints, schema_to_yaml};
pub use spec::{
    FileFormat, OutputConfig, SinkOutputConfig, SinkType, SourceType, TestSpec,
    TimeSimulationConfig, TopicNamingConfig,
};
pub use spec_generator::SpecGenerator;
pub use statement_executor::{
    CommandResult, ConsumerInfo, ConsumerPosition, ConsumerState, DataSinkInfo, DataSinkType,
    DataSourceInfo, DataSourceType, DebugCommand, DebugSession, ExecutionMode, JobInfo, JobState,
    JobStats, JobType, ParsedStatement, PartitionInfo, SessionState, StatementExecutor,
    StatementResult, StatementType, TopicInfo,
};
pub use stress::{MemoryTracker, StressConfig, StressMetrics, StressRunner};
pub use table_state::{TableSnapshot, TableState, TableStateConfig, TableStateManager};
