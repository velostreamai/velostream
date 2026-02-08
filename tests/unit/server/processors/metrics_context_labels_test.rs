//! Tests for auto-injection of `job` and `query` context labels
//! into remote-write SQL-native metrics.
//!
//! Verifies that:
//! - `extract_query_name` correctly extracts stream/table names
//! - `ProcessorMetricsHelper` stores and reports `app_name`
//! - `ObservabilityWrapperBuilder` threads `app_name` through to the helper

use velostream::velostream::server::processors::metrics_helper::{
    ProcessorMetricsHelper, extract_job_name, extract_query_name,
};
use velostream::velostream::server::processors::observability_wrapper::ObservabilityWrapper;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[test]
fn test_extract_query_name_create_stream() {
    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("CREATE STREAM trading_positions AS SELECT * FROM input")
        .expect("parse failed");

    let name = extract_query_name(&query);
    assert_eq!(name, Some("trading_positions".to_string()));
}

#[test]
fn test_extract_query_name_create_table() {
    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("CREATE TABLE merchant_analytics AS SELECT * FROM input")
        .expect("parse failed");

    let name = extract_query_name(&query);
    assert_eq!(name, Some("merchant_analytics".to_string()));
}

#[test]
fn test_extract_query_name_select_returns_none() {
    let parser = StreamingSqlParser::new();
    let query = parser.parse("SELECT * FROM input").expect("parse failed");

    let name = extract_query_name(&query);
    assert_eq!(name, None);
}

#[test]
fn test_extract_job_name_is_consistent_with_query_name() {
    let parser = StreamingSqlParser::new();
    let query = parser
        .parse("CREATE STREAM my_stream AS SELECT * FROM input")
        .expect("parse failed");

    // Both should return the same stream name
    assert_eq!(extract_job_name(&query), "my_stream");
    assert_eq!(extract_query_name(&query), Some("my_stream".to_string()));
}

#[test]
fn test_metrics_helper_app_name_default_none() {
    let helper = ProcessorMetricsHelper::new();
    assert_eq!(helper.app_name(), None);
}

#[test]
fn test_metrics_helper_set_app_name() {
    let mut helper = ProcessorMetricsHelper::new();
    helper.set_app_name("app_risk".to_string());
    assert_eq!(helper.app_name(), Some("app_risk"));
}

#[test]
fn test_observability_wrapper_builder_with_app_name() {
    let wrapper = ObservabilityWrapper::builder()
        .with_app_name(Some("app_trading".to_string()))
        .build();

    // The app_name should be accessible through the metrics_helper
    assert_eq!(wrapper.metrics_helper().app_name(), Some("app_trading"));
}

#[test]
fn test_observability_wrapper_builder_without_app_name() {
    let wrapper = ObservabilityWrapper::builder().build();

    // No app_name set
    assert_eq!(wrapper.metrics_helper().app_name(), None);
}

#[test]
fn test_observability_wrapper_builder_app_name_none_passthrough() {
    let wrapper = ObservabilityWrapper::builder().with_app_name(None).build();

    // Explicitly passing None should leave it unset
    assert_eq!(wrapper.metrics_helper().app_name(), None);
}

#[test]
fn test_observability_wrapper_with_observability_no_app_name() {
    // The convenience constructor should not set app_name
    let wrapper = ObservabilityWrapper::with_observability(None);
    assert_eq!(wrapper.metrics_helper().app_name(), None);
}
