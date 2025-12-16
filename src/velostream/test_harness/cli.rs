//! CLI helpers for velo-test binary
//!
//! Provides utilities for CLI argument processing and command execution.

use super::assertions::AssertionRunner;
use super::error::{TestHarnessError, TestHarnessResult};
use super::executor::{CapturedOutput, ExecutionResult, QueryExecutor};
use super::infra::TestHarnessInfra;
use super::report::{OutputFormat, ReportGenerator, TestReport};
use super::schema::SchemaRegistry;
use super::spec::TestSpec;
use std::path::Path;
use std::time::Duration;

/// Configuration for test run
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// SQL file path
    pub sql_file: std::path::PathBuf,

    /// Test specification file
    pub spec_file: Option<std::path::PathBuf>,

    /// Schema directory
    pub schema_dir: Option<std::path::PathBuf>,

    /// Specific query to run
    pub query_filter: Option<String>,

    /// Output format
    pub output_format: OutputFormat,

    /// Timeout per query in milliseconds
    pub timeout_ms: u64,

    /// Enable AI analysis
    pub ai_enabled: bool,
}

/// Run test suite with configuration
pub async fn run_tests(config: &RunConfig) -> TestHarnessResult<TestReport> {
    // Load test specification
    let spec = if let Some(ref spec_path) = config.spec_file {
        TestSpec::from_file(spec_path)?
    } else {
        // Generate minimal spec from SQL file
        generate_minimal_spec(&config.sql_file)?
    };

    spec.validate()?;

    // Load schemas (will be used when schema integration is completed)
    let _schemas = if let Some(ref schema_dir) = config.schema_dir {
        SchemaRegistry::load_from_dir(schema_dir)?
    } else {
        SchemaRegistry::new()
    };

    // Initialize infrastructure
    let mut infra = TestHarnessInfra::new();
    infra.start().await?;

    // Create executor
    let mut executor =
        QueryExecutor::new(infra).with_timeout(Duration::from_millis(config.timeout_ms));

    // Create report generator
    let mut report_gen = ReportGenerator::new(&spec.application, executor.infra().run_id());

    // Get queries to run
    let queries: Vec<_> = if let Some(ref filter) = config.query_filter {
        spec.queries.iter().filter(|q| q.name == *filter).collect()
    } else {
        spec.execution_order()
    };

    if queries.is_empty() {
        return Err(TestHarnessError::ConfigError {
            message: if config.query_filter.is_some() {
                format!(
                    "No query matching filter '{}' found",
                    config.query_filter.as_ref().unwrap()
                )
            } else {
                "No queries defined in test spec".to_string()
            },
        });
    }

    // Create assertion runner
    let assertion_runner = AssertionRunner::new();

    // Execute queries with assertions
    for query_test in queries {
        log::info!("Executing query: {}", query_test.name);

        match executor.execute_query(query_test).await {
            Ok(exec_result) => {
                // Run assertions on captured output
                let mut assertion_results = Vec::new();

                for output in &exec_result.outputs {
                    // Get assertions for this specific sink (includes fallback to top-level)
                    let sink_assertions: Vec<_> = query_test
                        .assertions_for_sink(&output.sink_name)
                        .into_iter()
                        .cloned()
                        .collect();
                    let results = assertion_runner.run_assertions(output, &sink_assertions);
                    assertion_results.extend(results);
                }

                // If no outputs captured, create empty output for assertions
                let all_assertions: Vec<_> =
                    query_test.all_assertions().into_iter().cloned().collect();
                if exec_result.outputs.is_empty() && !all_assertions.is_empty() {
                    let empty_output = CapturedOutput {
                        query_name: query_test.name.clone(),
                        sink_name: format!("{}_output", query_test.name),
                        topic: None,
                        records: Vec::new(),
                        execution_time_ms: 0,
                        warnings: Vec::new(),
                        memory_peak_bytes: None,
                        memory_growth_bytes: None,
                    };
                    let results = assertion_runner.run_assertions(&empty_output, &all_assertions);
                    assertion_results.extend(results);
                }

                report_gen.add_query_result(&exec_result, &assertion_results);
            }
            Err(e) => {
                log::error!("Query '{}' failed: {}", query_test.name, e);
                // Create error result
                let error_result = ExecutionResult {
                    query_name: query_test.name.clone(),
                    success: false,
                    error: Some(e.to_string()),
                    outputs: Vec::new(),
                    execution_time_ms: 0,
                };
                report_gen.add_query_result(&error_result, &[]);
            }
        }
    }

    // Cleanup infrastructure
    if let Err(e) = executor.stop().await {
        log::warn!("Failed to stop executor: {}", e);
    }

    // Generate report
    let report = report_gen.generate();

    Ok(report)
}

/// Generate minimal test specification from SQL file
fn generate_minimal_spec(sql_file: &Path) -> TestHarnessResult<TestSpec> {
    use crate::velostream::sql::validator::SqlValidator;

    let content = std::fs::read_to_string(sql_file).map_err(|e| TestHarnessError::IoError {
        message: e.to_string(),
        path: sql_file.display().to_string(),
    })?;

    let validator = SqlValidator::new();
    let result = validator.validate_sql_content(&content);

    // Extract application name from file
    let app_name = sql_file
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Create query tests from parsed queries
    let queries: Vec<_> = result
        .query_results
        .iter()
        .filter_map(|q| {
            extract_stream_name(&q.query_text).map(|name| super::spec::QueryTest {
                name,
                description: None,
                skip: false,
                inputs: Vec::new(),
                output: None,
                outputs: Vec::new(),
                assertions: vec![super::spec::AssertionConfig::RecordCount(
                    super::spec::RecordCountAssertion {
                        equals: None,
                        greater_than: Some(0),
                        less_than: None,
                        between: None,
                        expression: None,
                    },
                )],
                timeout_ms: None,
            })
        })
        .collect();

    Ok(TestSpec {
        application: app_name,
        description: Some("Auto-generated test spec".to_string()),
        default_timeout_ms: 30000,
        default_records: 1000,
        topic_naming: None,
        config: std::collections::HashMap::new(),
        queries,
    })
}

/// Extract stream name from CREATE STREAM statement
fn extract_stream_name(query: &str) -> Option<String> {
    let query_upper = query.to_uppercase();

    if let Some(pos) = query_upper.find("CREATE STREAM") {
        let after = &query[pos + "CREATE STREAM".len()..];
        let trimmed = after.trim_start();

        let name: String = trimmed
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();

        if !name.is_empty() {
            return Some(name);
        }
    }

    None
}

/// Validate SQL file without execution
pub fn validate_sql(sql_file: &Path, verbose: bool) -> TestHarnessResult<bool> {
    use crate::velostream::sql::validator::SqlValidator;

    let validator = SqlValidator::new();
    let result = validator.validate_application_file(sql_file);

    if verbose {
        println!(
            "Application: {}",
            result.application_name.as_deref().unwrap_or("unknown")
        );
        let invalid = result.total_queries.saturating_sub(result.valid_queries);
        println!(
            "Total Queries: {}, Valid: {}, Invalid: {}",
            result.total_queries, result.valid_queries, invalid
        );
    }

    Ok(result.is_valid)
}

/// Print help for output formats
pub fn print_format_help() {
    println!("Available output formats:");
    println!("  text   - Human-readable console output (default)");
    println!("  json   - Machine-readable JSON format");
    println!("  junit  - JUnit XML format for CI/CD integration");
}
