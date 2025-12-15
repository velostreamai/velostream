//! Unit tests for AI mock infrastructure
//!
//! Tests the MockAiProvider for testing without the real Claude API.

use velostream::velostream::test_harness::ai::{
    AiAnalysis, AiProvider, AnalysisContext, MockAiProvider, MockCall,
};
use velostream::velostream::test_harness::schema::{
    FieldConstraints, FieldDefinition, FieldType, Schema, SimpleFieldType,
};
use velostream::velostream::test_harness::spec::TestSpec;

#[tokio::test]
async fn test_mock_provider_available_by_default() {
    let mock = MockAiProvider::new();
    assert!(mock.is_available());
}

#[tokio::test]
async fn test_mock_provider_unavailable() {
    let mock = MockAiProvider::unavailable();
    assert!(!mock.is_available());
}

#[tokio::test]
async fn test_mock_provider_with_analysis() {
    let expected_analysis = AiAnalysis {
        summary: "Test failure due to schema mismatch".to_string(),
        root_cause: Some("The input field was integer but expected string".to_string()),
        suggestions: vec![
            "Fix the schema definition".to_string(),
            "Convert the field type".to_string(),
        ],
        confidence: 0.92,
    };

    let mock = MockAiProvider::new().with_analysis(expected_analysis.clone());

    let context = AnalysisContext {
        query_sql: "SELECT * FROM test".to_string(),
        query_name: "test_query".to_string(),
        input_samples: vec![],
        output_samples: vec![],
        assertion: None,
    };

    let result = mock.analyze_failure(&context).await.unwrap();
    assert_eq!(result.summary, expected_analysis.summary);
    assert_eq!(result.root_cause, expected_analysis.root_cause);
    assert_eq!(result.suggestions, expected_analysis.suggestions);
    assert!((result.confidence - expected_analysis.confidence).abs() < 0.001);
}

#[tokio::test]
async fn test_mock_provider_records_calls() {
    let mock = MockAiProvider::new().with_analysis(AiAnalysis {
        summary: "Test".to_string(),
        root_cause: None,
        suggestions: vec![],
        confidence: 0.5,
    });

    let context = AnalysisContext {
        query_sql: "SELECT id FROM users".to_string(),
        query_name: "user_query".to_string(),
        input_samples: vec![],
        output_samples: vec![],
        assertion: None,
    };

    let _ = mock.analyze_failure(&context).await;

    let calls = mock.get_calls().await;
    assert_eq!(calls.len(), 1);

    match &calls[0] {
        MockCall::AnalyzeFailure { query_name } => {
            assert_eq!(query_name, "user_query");
        }
        _ => panic!("Expected AnalyzeFailure call"),
    }
}

#[tokio::test]
async fn test_mock_provider_unavailable_returns_error() {
    let mock = MockAiProvider::unavailable();

    let context = AnalysisContext {
        query_sql: "SELECT * FROM test".to_string(),
        query_name: "test".to_string(),
        input_samples: vec![],
        output_samples: vec![],
        assertion: None,
    };

    let result = mock.analyze_failure(&context).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("not available"));
}

#[tokio::test]
async fn test_mock_provider_no_response_configured() {
    let mock = MockAiProvider::new();

    let context = AnalysisContext {
        query_sql: "SELECT * FROM test".to_string(),
        query_name: "test".to_string(),
        input_samples: vec![],
        output_samples: vec![],
        assertion: None,
    };

    let result = mock.analyze_failure(&context).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("No mock"));
}

#[tokio::test]
async fn test_mock_provider_with_schema() {
    let schema = Schema {
        name: "test_schema".to_string(),
        description: Some("Test schema".to_string()),
        record_count: 100,
        key_field: None,
        fields: vec![FieldDefinition {
            name: "id".to_string(),
            field_type: FieldType::Simple(SimpleFieldType::Integer),
            nullable: false,
            description: None,
            constraints: FieldConstraints::default(),
        }],
        source_path: None,
    };

    let mock = MockAiProvider::new().with_schema(schema);

    let result = mock.infer_schema("SELECT id FROM test", &[]).await;
    assert!(result.is_ok());
    let schema = result.unwrap();
    assert_eq!(schema.name, "test_schema");
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0].name, "id");
}

#[tokio::test]
async fn test_mock_provider_clear_calls() {
    let mock = MockAiProvider::new().with_analysis(AiAnalysis {
        summary: "Test".to_string(),
        root_cause: None,
        suggestions: vec![],
        confidence: 0.5,
    });

    let context = AnalysisContext {
        query_sql: "SELECT * FROM test".to_string(),
        query_name: "test".to_string(),
        input_samples: vec![],
        output_samples: vec![],
        assertion: None,
    };

    let _ = mock.analyze_failure(&context).await;
    assert_eq!(mock.get_calls().await.len(), 1);

    mock.clear_calls().await;
    assert_eq!(mock.get_calls().await.len(), 0);
}

#[tokio::test]
async fn test_mock_provider_default_impl() {
    let mock = MockAiProvider::default();
    assert!(mock.is_available());
}

#[tokio::test]
async fn test_mock_provider_with_test_spec() {
    let spec = TestSpec {
        application: "test_app".to_string(),
        description: Some("Test application".to_string()),
        default_timeout_ms: 30000,
        default_records: 1000,
        topic_naming: None,
        config: Default::default(),
        queries: vec![],
    };

    let mock = MockAiProvider::new().with_test_spec(spec);

    let result = mock
        .generate_test_spec("SELECT * FROM test", "test_app")
        .await;
    assert!(result.is_ok());
    let spec = result.unwrap();
    assert_eq!(spec.application, "test_app");
}

#[tokio::test]
async fn test_mock_provider_records_generate_test_spec_call() {
    let spec = TestSpec {
        application: "my_app".to_string(),
        description: None,
        default_timeout_ms: 30000,
        default_records: 1000,
        topic_naming: None,
        config: Default::default(),
        queries: vec![],
    };

    let mock = MockAiProvider::new().with_test_spec(spec);

    let _ = mock
        .generate_test_spec("SELECT id FROM users", "my_app")
        .await;

    let calls = mock.get_calls().await;
    assert_eq!(calls.len(), 1);

    match &calls[0] {
        MockCall::GenerateTestSpec { sql, app_name } => {
            assert_eq!(sql, "SELECT id FROM users");
            assert_eq!(app_name, "my_app");
        }
        _ => panic!("Expected GenerateTestSpec call"),
    }
}

#[tokio::test]
async fn test_mock_provider_records_infer_schema_call() {
    let schema = Schema {
        name: "test".to_string(),
        description: None,
        record_count: 1000,
        key_field: None,
        fields: vec![],
        source_path: None,
    };

    let mock = MockAiProvider::new().with_schema(schema);

    let _ = mock.infer_schema("SELECT * FROM orders", &[]).await;

    let calls = mock.get_calls().await;
    assert_eq!(calls.len(), 1);

    match &calls[0] {
        MockCall::InferSchema { sql, sample_count } => {
            assert_eq!(sql, "SELECT * FROM orders");
            assert_eq!(*sample_count, 0);
        }
        _ => panic!("Expected InferSchema call"),
    }
}
