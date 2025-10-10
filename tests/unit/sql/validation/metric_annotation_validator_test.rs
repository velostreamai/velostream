//! Tests for SQL validator @metric annotation validation (FR-073)

use velostream::velostream::sql::validator::SqlValidator;
use std::path::PathBuf;

#[test]
fn test_valid_metric_annotations() {
    let validator = SqlValidator::new();

    // Create test SQL with valid @metric annotations
    let test_sql = r#"
-- @metric: test_counter_total
-- @metric_type: counter
-- @metric_help: "Test counter metric"
-- @metric_labels: symbol, status
CREATE STREAM test_stream AS
SELECT * FROM source;
"#;

    // Write to temporary file
    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_metric_validation.sql");
    std::fs::write(&test_file, test_sql).expect("Failed to write test file");

    // Validate
    let result = validator.validate_application_file(&test_file);

    // Should be valid
    assert!(result.is_valid, "Expected valid SQL with metric annotations");

    // Should have recommendation about found annotations
    let metric_recommendation = result.recommendations.iter()
        .find(|r| r.contains("@metric annotation"));
    assert!(metric_recommendation.is_some(), "Expected recommendation about @metric annotations");
    assert!(metric_recommendation.unwrap().contains("1"), "Expected 1 annotation found");

    // Cleanup
    std::fs::remove_file(&test_file).ok();
}

#[test]
fn test_multiple_valid_metric_annotations() {
    let validator = SqlValidator::new();

    // Create test SQL with multiple valid @metric annotations
    let test_sql = r#"
-- @metric: test_counter_total
-- @metric_type: counter
-- @metric_help: "Test counter metric"
-- @metric_labels: symbol
CREATE STREAM stream1 AS SELECT * FROM source1;

-- @metric: test_gauge_value
-- @metric_type: gauge
-- @metric_help: "Test gauge metric"
-- @metric_field: value
CREATE STREAM stream2 AS SELECT * FROM source2;

-- @metric: test_histogram_distribution
-- @metric_type: histogram
-- @metric_help: "Test histogram metric"
-- @metric_field: duration
-- @metric_buckets: 0.1, 0.5, 1.0, 2.0, 5.0
CREATE STREAM stream3 AS SELECT * FROM source3;
"#;

    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_multiple_metrics.sql");
    std::fs::write(&test_file, test_sql).expect("Failed to write test file");

    let result = validator.validate_application_file(&test_file);

    assert!(result.is_valid, "Expected valid SQL with multiple metric annotations");

    let metric_recommendation = result.recommendations.iter()
        .find(|r| r.contains("@metric annotation"));
    assert!(metric_recommendation.is_some(), "Expected recommendation about @metric annotations");
    assert!(metric_recommendation.unwrap().contains("3"), "Expected 3 annotations found");

    std::fs::remove_file(&test_file).ok();
}

#[test]
fn test_invalid_metric_annotation_missing_field() {
    let validator = SqlValidator::new();

    // Gauge without @metric_field should fail
    let test_sql = r#"
-- @metric: test_gauge_value
-- @metric_type: gauge
-- @metric_help: "Test gauge metric without field"
CREATE STREAM test_stream AS SELECT * FROM source;
"#;

    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_invalid_metric.sql");
    std::fs::write(&test_file, test_sql).expect("Failed to write test file");

    let result = validator.validate_application_file(&test_file);

    // Should be invalid
    assert!(!result.is_valid, "Expected invalid SQL with incomplete gauge annotation");

    // Should have error about missing field
    let field_error = result.global_errors.iter()
        .find(|e| e.contains("@metric_field"));
    assert!(field_error.is_some(), "Expected error about missing @metric_field");

    std::fs::remove_file(&test_file).ok();
}

#[test]
fn test_invalid_metric_type() {
    let validator = SqlValidator::new();

    // Invalid metric type
    let test_sql = r#"
-- @metric: test_invalid_type
-- @metric_type: invalid_type
-- @metric_help: "Test with invalid type"
CREATE STREAM test_stream AS SELECT * FROM source;
"#;

    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_invalid_type.sql");
    std::fs::write(&test_file, test_sql).expect("Failed to write test file");

    let result = validator.validate_application_file(&test_file);

    // Should be invalid
    assert!(!result.is_valid, "Expected invalid SQL with invalid metric type");

    // Should have error about invalid type
    let type_error = result.global_errors.iter()
        .find(|e| e.contains("metric type") || e.contains("counter") || e.contains("gauge") || e.contains("histogram"));
    assert!(type_error.is_some(), "Expected error about invalid metric type");

    std::fs::remove_file(&test_file).ok();
}

#[test]
fn test_histogram_without_field() {
    let validator = SqlValidator::new();

    // Histogram without @metric_field should fail
    let test_sql = r#"
-- @metric: test_histogram_distribution
-- @metric_type: histogram
-- @metric_help: "Test histogram without field"
-- @metric_buckets: 0.1, 0.5, 1.0
CREATE STREAM test_stream AS SELECT * FROM source;
"#;

    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_histogram_no_field.sql");
    std::fs::write(&test_file, test_sql).expect("Failed to write test file");

    let result = validator.validate_application_file(&test_file);

    assert!(!result.is_valid, "Expected invalid SQL with histogram missing field");

    let field_error = result.global_errors.iter()
        .find(|e| e.contains("@metric_field"));
    assert!(field_error.is_some(), "Expected error about missing @metric_field for histogram");

    std::fs::remove_file(&test_file).ok();
}

#[test]
fn test_no_annotations_passes_validation() {
    let validator = SqlValidator::new();

    // SQL without any @metric annotations should still be valid
    let test_sql = r#"
CREATE STREAM test_stream AS
SELECT * FROM source;
"#;

    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_no_annotations.sql");
    std::fs::write(&test_file, test_sql).expect("Failed to write test file");

    let result = validator.validate_application_file(&test_file);

    // Should be valid (annotations are optional)
    assert!(result.is_valid, "Expected valid SQL without annotations");

    // Should not have any annotation recommendations
    let metric_recommendation = result.recommendations.iter()
        .find(|r| r.contains("@metric annotation"));
    assert!(metric_recommendation.is_none(), "Should not have annotation recommendations when none present");

    std::fs::remove_file(&test_file).ok();
}

#[test]
fn test_counter_with_labels_and_condition() {
    let validator = SqlValidator::new();

    // Counter with labels and condition
    let test_sql = r#"
-- @metric: test_alerts_total
-- @metric_type: counter
-- @metric_help: "Test alerts by severity"
-- @metric_labels: symbol, severity
-- @metric_condition: severity IN ('HIGH', 'CRITICAL')
CREATE STREAM test_stream AS SELECT * FROM source;
"#;

    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_counter_with_condition.sql");
    std::fs::write(&test_file, test_sql).expect("Failed to write test file");

    let result = validator.validate_application_file(&test_file);

    assert!(result.is_valid, "Expected valid SQL with counter having labels and condition");

    let metric_recommendation = result.recommendations.iter()
        .find(|r| r.contains("@metric annotation"));
    assert!(metric_recommendation.is_some(), "Expected recommendation about @metric annotations");

    std::fs::remove_file(&test_file).ok();
}
