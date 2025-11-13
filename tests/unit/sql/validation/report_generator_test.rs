//! Unit tests for ValidationReportGenerator

use std::path::Path;
use velostream::velostream::sql::execution::processors::BatchValidationTarget;
use velostream::velostream::sql::validation::ApplicationMetadata;
use velostream::velostream::sql::validation::report_generator::ValidationReportGenerator;
use velostream::velostream::sql::validation::result_types::QueryValidationResult;

#[test]
fn test_report_generator_creation() {
    let generator = ValidationReportGenerator::new();
    // Verify the generator was created successfully
}

#[test]
fn test_report_generator_minimal() {
    let generator = ValidationReportGenerator::new_minimal();
    // Verify the minimal generator was created successfully
}

#[test]
fn test_report_generator_with_performance_tips() {
    let generator = ValidationReportGenerator::new().with_performance_tips(false);
    // Verify the generator was configured successfully
}

#[test]
fn test_report_generator_with_configuration_summary() {
    let generator = ValidationReportGenerator::new().with_configuration_summary(false);
    // Verify the generator was configured successfully
}

#[test]
fn test_generate_application_result_empty() {
    let generator = ValidationReportGenerator::new();
    let path = Path::new("test.sql");
    let metadata = ApplicationMetadata {
        name: "Test Application".to_string(),
        description: "Test description".to_string(),
        version: "1.0.0".to_string(),
    };
    let query_results = vec![];

    let result = generator.generate_application_result(path, metadata, query_results);

    assert_eq!(result.application_name, "Test Application");
    assert_eq!(result.description, "Test description".to_string());
    assert_eq!(result.version, "1.0.0".to_string());
    assert_eq!(result.query_results.len(), 0);
}

#[test]
fn test_generate_application_result_with_queries() {
    let generator = ValidationReportGenerator::new();
    let path = Path::new("test.sql");
    let metadata = ApplicationMetadata {
        name: "Test Application".to_string(),
        description: "Test SQL application".to_string(),
        version: "2.0.0".to_string(),
    };

    let query1 = QueryValidationResult::new("SELECT * FROM stream1".to_string());
    let mut query2 = QueryValidationResult::new("SELECT count(*) FROM stream2".to_string());
    query2.add_warning("Performance warning".to_string());

    let query_results = vec![query1, query2];

    let result = generator.generate_application_result(path, metadata, query_results);

    assert_eq!(result.application_name, "Test Application");
    assert_eq!(result.query_results.len(), 2);
    assert_eq!(result.query_results[0].query_text, "SELECT * FROM stream1");
    assert_eq!(
        result.query_results[1].query_text,
        "SELECT count(*) FROM stream2"
    );
}

#[test]
fn test_generate_application_result_with_invalid_queries() {
    let generator = ValidationReportGenerator::new();
    let path = Path::new("invalid.sql");
    let metadata = ApplicationMetadata {
        name: "Invalid Application".to_string(),
        description: String::new(),
        version: String::new(),
    };

    let mut query1 = QueryValidationResult::new("INVALID SQL".to_string());
    query1.is_valid = false;
    query1.add_configuration_error("Syntax error".to_string());

    let query_results = vec![query1];

    let result = generator.generate_application_result(path, metadata, query_results);

    assert_eq!(result.application_name, "Invalid Application");
    assert_eq!(result.description, String::new());
    assert_eq!(result.version, String::new());
    assert_eq!(result.query_results.len(), 1);
    assert!(!result.query_results[0].is_valid);
}

#[test]
fn test_generate_application_result_builder_pattern() {
    let generator = ValidationReportGenerator::new()
        .with_performance_tips(true)
        .with_configuration_summary(true);

    let path = Path::new("builder_test.sql");
    let metadata = ApplicationMetadata {
        name: "Builder Test".to_string(),
        description: "Testing builder pattern".to_string(),
        version: "1.0.0".to_string(),
    };

    let query = QueryValidationResult::new("SELECT 1".to_string());
    let query_results = vec![query];

    let result = generator.generate_application_result(path, metadata, query_results);

    assert_eq!(result.application_name, "Builder Test");
    assert_eq!(result.query_results.len(), 1);
}

#[test]
fn test_generate_application_result_minimal_config() {
    let generator = ValidationReportGenerator::new_minimal();

    let path = Path::new("minimal.sql");
    let metadata = ApplicationMetadata {
        name: "Minimal Test".to_string(),
        description: "Minimal configuration test".to_string(),
        version: "0.1.0".to_string(),
    };

    let query = QueryValidationResult::new("SELECT * FROM test".to_string());
    let query_results = vec![query];

    let result = generator.generate_application_result(path, metadata, query_results);

    assert_eq!(result.application_name, "Minimal Test");
    assert_eq!(result.query_results.len(), 1);
}

#[test]
fn test_generator_reusability() {
    let generator = ValidationReportGenerator::new();

    // First generation
    let path1 = Path::new("test1.sql");
    let metadata1 = ApplicationMetadata {
        name: "Test 1".to_string(),
        description: String::new(),
        version: String::new(),
    };
    let query1 = QueryValidationResult::new("SELECT * FROM table1".to_string());
    let result1 = generator.generate_application_result(path1, metadata1, vec![query1]);

    // Second generation
    let path2 = Path::new("test2.sql");
    let metadata2 = ApplicationMetadata {
        name: "Test 2".to_string(),
        description: String::new(),
        version: String::new(),
    };
    let query2 = QueryValidationResult::new("SELECT * FROM table2".to_string());
    let result2 = generator.generate_application_result(path2, metadata2, vec![query2]);

    // Results should be independent
    assert_ne!(result1.application_name, result2.application_name);
    assert_ne!(
        result1.query_results[0].query_text,
        result2.query_results[0].query_text
    );
}
