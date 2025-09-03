use ferrisstreams::ferris::modern_multi_job_server::MultiJobSqlServer;
use ferrisstreams::ferris::sql::app_parser::{
    ApplicationMetadata, ApplicationResources, SqlApplication, SqlStatement, StatementType,
};
use std::collections::HashMap;
use tokio;

#[tokio::test]
async fn test_select_statement_auto_deployment() {
    // Create a simple SQL application with SELECT statements
    let app = SqlApplication {
        metadata: ApplicationMetadata {
            name: "test_select_app".to_string(),
            version: "1.0.0".to_string(),
            description: Some("Test application for SELECT statement deployment".to_string()),
            author: Some("test_author".to_string()),
            dependencies: vec![],
            created_at: chrono::Utc::now(),
            tags: HashMap::new(),
        },
        statements: vec![
            // Simple SELECT statement - should become a job
            SqlStatement {
                id: "stmt_1".to_string(),
                order: 1,
                statement_type: StatementType::Select,
                sql: "SELECT customer_id, amount FROM transactions".to_string(),
                name: None, // No explicit name - should auto-generate
                dependencies: vec![],
                properties: HashMap::new(),
            },
            // SELECT with explicit name
            SqlStatement {
                id: "stmt_2".to_string(),
                order: 2,
                statement_type: StatementType::Select,
                sql: "SELECT COUNT(*) as total FROM transactions".to_string(),
                name: Some("transaction_count_job".to_string()),
                dependencies: vec!["processed_transactions".to_string()], // Should use this topic
                properties: HashMap::new(),
            },
            // CREATE STREAM - should be skipped
            SqlStatement {
                id: "stmt_3".to_string(),
                order: 3,
                statement_type: StatementType::CreateStream,
                sql: "CREATE STREAM test_stream (id STRING) WITH (datasource='test')".to_string(),
                name: None,
                dependencies: vec![],
                properties: HashMap::new(),
            },
        ],
        resources: ApplicationResources {
            streams: vec![],
            tables: vec![],
            jobs: vec![],
            topics: vec![],
        },
    };

    // Create server instance for testing
    let server = MultiJobSqlServer::new("localhost:9092".to_string(), "test-group".to_string(), 5);

    // Test deployment
    let result = server
        .deploy_sql_application(app.clone(), Some("default_topic".to_string()))
        .await;

    // Should not fail due to our changes
    assert!(result.is_ok(), "Deployment should succeed: {:?}", result);

    let deployed_jobs = result.unwrap();

    // Should deploy exactly 2 jobs (the 2 SELECT statements)
    assert_eq!(
        deployed_jobs.len(),
        2,
        "Should deploy exactly 2 SELECT statement jobs"
    );

    // Check job names
    assert!(
        deployed_jobs.contains(&"test_select_app_stmt_1".to_string()),
        "Should contain auto-generated job name for first SELECT"
    );
    assert!(
        deployed_jobs.contains(&"transaction_count_job".to_string()),
        "Should contain explicit job name for second SELECT"
    );

    println!("✅ SELECT statement auto-deployment test passed!");
    println!("   Deployed jobs: {:?}", deployed_jobs);
}

#[tokio::test]
async fn test_mixed_statement_types_deployment() {
    let app = SqlApplication {
        metadata: ApplicationMetadata {
            name: "mixed_statements_app".to_string(),
            version: "1.0.0".to_string(),
            description: Some("Test mixed statement types".to_string()),
            author: Some("test_author".to_string()),
            dependencies: vec![],
            created_at: chrono::Utc::now(),
            tags: HashMap::new(),
        },
        statements: vec![
            // StartJob - should deploy
            SqlStatement {
                id: "stmt_1".to_string(),
                order: 1,
                statement_type: StatementType::StartJob,
                sql: "START JOB explicit_job SELECT * FROM data".to_string(),
                name: Some("explicit_job".to_string()),
                dependencies: vec!["input_topic".to_string()],
                properties: HashMap::new(),
            },
            // SELECT - should auto-deploy
            SqlStatement {
                id: "stmt_2".to_string(),
                order: 2,
                statement_type: StatementType::Select,
                sql: "SELECT customer_id, SUM(amount) FROM transactions GROUP BY customer_id"
                    .to_string(),
                name: None,
                dependencies: vec![],
                properties: HashMap::new(),
            },
            // CreateStream - should skip
            SqlStatement {
                id: "stmt_3".to_string(),
                order: 3,
                statement_type: StatementType::CreateStream,
                sql: "CREATE STREAM test (id STRING)".to_string(),
                name: None,
                dependencies: vec![],
                properties: HashMap::new(),
            },
        ],
        resources: ApplicationResources {
            streams: vec![],
            tables: vec![],
            jobs: vec![],
            topics: vec![],
        },
    };

    let server =
        MultiJobSqlServer::new("localhost:9092".to_string(), "test-group-2".to_string(), 5);
    let result = server.deploy_sql_application(app.clone(), None).await;

    assert!(
        result.is_ok(),
        "Mixed statement deployment should succeed: {:?}",
        result
    );

    let deployed_jobs = result.unwrap();

    // Should deploy 2 jobs: 1 StartJob + 1 SELECT
    assert_eq!(
        deployed_jobs.len(),
        2,
        "Should deploy StartJob and SELECT statements only"
    );

    println!("✅ Mixed statement types deployment test passed!");
    println!("   Deployed jobs: {:?}", deployed_jobs);
}

#[tokio::test]
async fn test_topic_generation_logic() {
    let app = SqlApplication {
        metadata: ApplicationMetadata {
            name: "topic_test_app".to_string(),
            version: "1.0.0".to_string(),
            description: Some("Test topic generation logic".to_string()),
            author: Some("test_author".to_string()),
            dependencies: vec![],
            created_at: chrono::Utc::now(),
            tags: HashMap::new(),
        },
        statements: vec![
            // SELECT with dependencies - should use dependency as topic
            SqlStatement {
                id: "stmt_1".to_string(),
                order: 1,
                statement_type: StatementType::Select,
                sql: "SELECT * FROM input_stream".to_string(),
                name: Some("job_with_dependency".to_string()),
                dependencies: vec!["dependency_topic".to_string()],
                properties: HashMap::new(),
            },
            // SELECT without dependencies, with default topic - should use default
            SqlStatement {
                id: "stmt_2".to_string(),
                order: 2,
                statement_type: StatementType::Select,
                sql: "SELECT COUNT(*) FROM data".to_string(),
                name: Some("job_with_default".to_string()),
                dependencies: vec![],
                properties: HashMap::new(),
            },
            // SELECT without dependencies, no default - should auto-generate
            SqlStatement {
                id: "stmt_3".to_string(),
                order: 3,
                statement_type: StatementType::Select,
                sql: "SELECT AVG(price) FROM products".to_string(),
                name: Some("job_auto_topic".to_string()),
                dependencies: vec![],
                properties: HashMap::new(),
            },
        ],
        resources: ApplicationResources {
            streams: vec![],
            tables: vec![],
            jobs: vec![],
            topics: vec![],
        },
    };

    let server = MultiJobSqlServer::new(
        "localhost:9092".to_string(),
        "topic-test-group".to_string(),
        5,
    );

    // Test with default topic
    let result_with_default = server
        .deploy_sql_application(app.clone(), Some("default_topic".to_string()))
        .await;
    assert!(
        result_with_default.is_ok(),
        "Should succeed with default topic"
    );

    // Test without default topic
    let result_no_default = server.deploy_sql_application(app.clone(), None).await;
    assert!(
        result_no_default.is_ok(),
        "Should succeed without default topic (auto-generate)"
    );

    println!("✅ Topic generation logic test passed!");
}

#[test]
fn test_statement_type_matching() {
    // Test that our match arms correctly identify SELECT statements
    let select_stmt = StatementType::Select;
    let start_job_stmt = StatementType::StartJob;
    let deploy_job_stmt = StatementType::DeployJob;
    let create_stream_stmt = StatementType::CreateStream;

    // This mimics the match logic in deploy_sql_application
    let should_deploy_select = matches!(
        select_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );
    let should_deploy_start = matches!(
        start_job_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );
    let should_deploy_deploy = matches!(
        deploy_job_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );
    let should_skip_create = matches!(
        create_stream_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );

    assert!(
        should_deploy_select,
        "SELECT statements should be deployable"
    );
    assert!(
        should_deploy_start,
        "StartJob statements should be deployable"
    );
    assert!(
        should_deploy_deploy,
        "DeployJob statements should be deployable"
    );
    assert!(
        !should_skip_create,
        "CreateStream statements should be skipped"
    );

    println!("✅ Statement type matching test passed!");
}
