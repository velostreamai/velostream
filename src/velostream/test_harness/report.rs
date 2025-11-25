//! Test report generation
//!
//! Generates test reports in multiple formats:
//! - Text (human-readable console output)
//! - JSON (machine-readable)
//! - JUnit XML (CI/CD integration)

use super::assertions::AssertionResult;
use super::executor::ExecutionResult;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

/// Complete test run report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestReport {
    /// Application name
    pub application: String,

    /// Run ID
    pub run_id: String,

    /// Start time (ISO 8601)
    pub start_time: String,

    /// End time (ISO 8601)
    pub end_time: String,

    /// Total duration in milliseconds
    pub duration_ms: u64,

    /// Summary statistics
    pub summary: TestSummary,

    /// Per-query results
    pub queries: Vec<QueryReport>,
}

/// Summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestSummary {
    /// Total queries executed
    pub total: usize,

    /// Queries that passed
    pub passed: usize,

    /// Queries that failed
    pub failed: usize,

    /// Queries that were skipped
    pub skipped: usize,

    /// Queries that had errors
    pub errors: usize,

    /// Total assertions run
    pub total_assertions: usize,

    /// Assertions that passed
    pub passed_assertions: usize,

    /// Assertions that failed
    pub failed_assertions: usize,
}

/// Report for a single query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryReport {
    /// Query name
    pub name: String,

    /// Query status
    pub status: QueryStatus,

    /// Execution time in milliseconds
    pub duration_ms: u64,

    /// Error message if failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Record counts
    pub record_counts: RecordCounts,

    /// Assertion results
    pub assertions: Vec<AssertionReport>,
}

/// Query execution status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Passed,
    Failed,
    Error,
    Skipped,
}

/// Record count summary
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecordCounts {
    /// Input record counts by source
    pub inputs: std::collections::HashMap<String, usize>,

    /// Output record counts by sink
    pub outputs: std::collections::HashMap<String, usize>,
}

/// Report for a single assertion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionReport {
    /// Assertion type
    #[serde(rename = "type")]
    pub assertion_type: String,

    /// Whether it passed
    pub passed: bool,

    /// Human-readable message
    pub message: String,

    /// Expected value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,

    /// Actual value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
}

impl From<&AssertionResult> for AssertionReport {
    fn from(result: &AssertionResult) -> Self {
        Self {
            assertion_type: result.assertion_type.clone(),
            passed: result.passed,
            message: result.message.clone(),
            expected: result.expected.clone(),
            actual: result.actual.clone(),
        }
    }
}

/// Report generator
pub struct ReportGenerator {
    /// Application name
    application: String,

    /// Run ID
    run_id: String,

    /// Query reports
    queries: Vec<QueryReport>,

    /// Start time
    start_time: chrono::DateTime<chrono::Utc>,
}

impl ReportGenerator {
    /// Create new report generator
    pub fn new(application: &str, run_id: &str) -> Self {
        Self {
            application: application.to_string(),
            run_id: run_id.to_string(),
            queries: Vec::new(),
            start_time: chrono::Utc::now(),
        }
    }

    /// Add query result
    pub fn add_query_result(
        &mut self,
        execution: &ExecutionResult,
        assertions: &[AssertionResult],
    ) {
        let status = if !execution.success {
            QueryStatus::Error
        } else if assertions.iter().all(|a| a.passed) {
            QueryStatus::Passed
        } else {
            QueryStatus::Failed
        };

        let record_counts = RecordCounts {
            inputs: std::collections::HashMap::new(), // TODO: Track input counts
            outputs: execution
                .outputs
                .iter()
                .map(|o| (o.sink_name.clone(), o.records.len()))
                .collect(),
        };

        let query_report = QueryReport {
            name: execution.query_name.clone(),
            status,
            duration_ms: execution.execution_time_ms,
            error: execution.error.clone(),
            record_counts,
            assertions: assertions.iter().map(AssertionReport::from).collect(),
        };

        self.queries.push(query_report);
    }

    /// Generate final report
    pub fn generate(&self) -> TestReport {
        let end_time = chrono::Utc::now();
        let duration = end_time - self.start_time;

        let summary = self.calculate_summary();

        TestReport {
            application: self.application.clone(),
            run_id: self.run_id.clone(),
            start_time: self.start_time.to_rfc3339(),
            end_time: end_time.to_rfc3339(),
            duration_ms: duration.num_milliseconds() as u64,
            summary,
            queries: self.queries.clone(),
        }
    }

    /// Calculate summary statistics
    fn calculate_summary(&self) -> TestSummary {
        let total = self.queries.len();
        let passed = self
            .queries
            .iter()
            .filter(|q| q.status == QueryStatus::Passed)
            .count();
        let failed = self
            .queries
            .iter()
            .filter(|q| q.status == QueryStatus::Failed)
            .count();
        let skipped = self
            .queries
            .iter()
            .filter(|q| q.status == QueryStatus::Skipped)
            .count();
        let errors = self
            .queries
            .iter()
            .filter(|q| q.status == QueryStatus::Error)
            .count();

        let total_assertions: usize = self.queries.iter().map(|q| q.assertions.len()).sum();
        let passed_assertions: usize = self
            .queries
            .iter()
            .flat_map(|q| &q.assertions)
            .filter(|a| a.passed)
            .count();

        TestSummary {
            total,
            passed,
            failed,
            skipped,
            errors,
            total_assertions,
            passed_assertions,
            failed_assertions: total_assertions - passed_assertions,
        }
    }
}

/// Output format for reports
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutputFormat {
    Text,
    Json,
    Junit,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(OutputFormat::Text),
            "json" => Ok(OutputFormat::Json),
            "junit" | "xml" => Ok(OutputFormat::Junit),
            _ => Err(format!("Unknown output format: {}", s)),
        }
    }
}

/// Write report to output
pub fn write_report(
    report: &TestReport,
    format: OutputFormat,
    writer: &mut dyn Write,
) -> std::io::Result<()> {
    match format {
        OutputFormat::Text => write_text_report(report, writer),
        OutputFormat::Json => write_json_report(report, writer),
        OutputFormat::Junit => write_junit_report(report, writer),
    }
}

/// Write text format report
fn write_text_report(report: &TestReport, writer: &mut dyn Write) -> std::io::Result<()> {
    writeln!(writer, "\n🧪 Velostream SQL Test Report")?;
    writeln!(writer, "════════════════════════════════════════")?;
    writeln!(writer, "Application: {}", report.application)?;
    writeln!(writer, "Run ID: {}", report.run_id)?;
    writeln!(writer, "Duration: {}ms", report.duration_ms)?;
    writeln!(writer)?;

    // Summary
    writeln!(writer, "📊 Summary")?;
    writeln!(writer, "─────────────────────────────────────────")?;
    writeln!(
        writer,
        "Queries: {} total, {} passed, {} failed, {} errors, {} skipped",
        report.summary.total,
        report.summary.passed,
        report.summary.failed,
        report.summary.errors,
        report.summary.skipped
    )?;
    writeln!(
        writer,
        "Assertions: {} total, {} passed, {} failed",
        report.summary.total_assertions,
        report.summary.passed_assertions,
        report.summary.failed_assertions
    )?;
    writeln!(writer)?;

    // Per-query results
    writeln!(writer, "📋 Query Results")?;
    writeln!(writer, "─────────────────────────────────────────")?;

    for query in &report.queries {
        let status_icon = match query.status {
            QueryStatus::Passed => "✅",
            QueryStatus::Failed => "❌",
            QueryStatus::Error => "💥",
            QueryStatus::Skipped => "⏭️",
        };

        writeln!(
            writer,
            "\n{} {} ({}ms)",
            status_icon, query.name, query.duration_ms
        )?;

        if let Some(ref error) = query.error {
            writeln!(writer, "   ERROR: {}", error)?;
        }

        // Show record counts
        if !query.record_counts.outputs.is_empty() {
            let output_counts: Vec<_> = query
                .record_counts
                .outputs
                .iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect();
            writeln!(writer, "   Records: {}", output_counts.join(", "))?;
        }

        // Show assertions
        for assertion in &query.assertions {
            let icon = if assertion.passed { "✓" } else { "✗" };
            writeln!(writer, "   {} {}", icon, assertion.message)?;

            if !assertion.passed {
                if let (Some(expected), Some(actual)) = (&assertion.expected, &assertion.actual) {
                    writeln!(writer, "      Expected: {}", expected)?;
                    writeln!(writer, "      Actual:   {}", actual)?;
                }
            }
        }
    }

    // Final status
    writeln!(writer)?;
    if report.summary.failed == 0 && report.summary.errors == 0 {
        writeln!(writer, "🎉 ALL TESTS PASSED!")?;
    } else {
        writeln!(
            writer,
            "❌ {} failures, {} errors",
            report.summary.failed, report.summary.errors
        )?;
    }

    Ok(())
}

/// Write JSON format report
fn write_json_report(report: &TestReport, writer: &mut dyn Write) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(report)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    writeln!(writer, "{}", json)
}

/// Write JUnit XML format report
fn write_junit_report(report: &TestReport, writer: &mut dyn Write) -> std::io::Result<()> {
    writeln!(writer, r#"<?xml version="1.0" encoding="UTF-8"?>"#)?;
    writeln!(
        writer,
        r#"<testsuites name="{}" tests="{}" failures="{}" errors="{}" time="{:.3}">"#,
        report.application,
        report.summary.total,
        report.summary.failed,
        report.summary.errors,
        report.duration_ms as f64 / 1000.0
    )?;

    writeln!(
        writer,
        r#"  <testsuite name="{}" tests="{}" failures="{}" errors="{}" skipped="{}" time="{:.3}">"#,
        report.application,
        report.queries.len(),
        report.summary.failed,
        report.summary.errors,
        report.summary.skipped,
        report.duration_ms as f64 / 1000.0
    )?;

    for query in &report.queries {
        writeln!(
            writer,
            r#"    <testcase name="{}" classname="{}" time="{:.3}">"#,
            escape_xml(&query.name),
            escape_xml(&report.application),
            query.duration_ms as f64 / 1000.0
        )?;

        match query.status {
            QueryStatus::Failed => {
                for assertion in &query.assertions {
                    if !assertion.passed {
                        writeln!(
                            writer,
                            r#"      <failure type="{}" message="{}">"#,
                            escape_xml(&assertion.assertion_type),
                            escape_xml(&assertion.message)
                        )?;
                        if let (Some(expected), Some(actual)) =
                            (&assertion.expected, &assertion.actual)
                        {
                            writeln!(writer, "Expected: {}", escape_xml(expected))?;
                            writeln!(writer, "Actual: {}", escape_xml(actual))?;
                        }
                        writeln!(writer, "      </failure>")?;
                    }
                }
            }
            QueryStatus::Error => {
                if let Some(ref error) = query.error {
                    writeln!(
                        writer,
                        r#"      <error message="{}">{}</error>"#,
                        escape_xml(error),
                        escape_xml(error)
                    )?;
                }
            }
            QueryStatus::Skipped => {
                writeln!(writer, "      <skipped/>")?;
            }
            QueryStatus::Passed => {}
        }

        writeln!(writer, "    </testcase>")?;
    }

    writeln!(writer, "  </testsuite>")?;
    writeln!(writer, "</testsuites>")?;

    Ok(())
}

/// Escape XML special characters
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
