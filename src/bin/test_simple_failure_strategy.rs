//! Simple test for failure strategy configuration parsing

use ferrisstreams::ferris::sql::config::with_clause_parser::WithClauseParser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Testing Failure Strategy Configuration ===");

    let parser = WithClauseParser::new();

    let with_clause = r#"
        'sink.bootstrap.servers' = 'localhost:9092',
        'sink.topic' = 'test-topic',
        'sink.failure_strategy' = 'LogAndContinue'
    "#;

    let config = parser.parse_with_clause(with_clause)?;

    // Check that failure strategy is parsed
    assert_eq!(
        config.raw_config.get("sink.failure_strategy").unwrap(),
        "LogAndContinue"
    );
    println!(
        "✅ sink.failure_strategy = {}",
        config.raw_config.get("sink.failure_strategy").unwrap()
    );

    // Test all failure strategy variants
    let strategies = vec![
        "LogAndContinue",
        "SendToDLQ",
        "FailBatch",
        "RetryWithBackoff",
    ];

    for strategy in strategies {
        let test_clause = format!(
            r#"
            'sink.bootstrap.servers' = 'localhost:9092',
            'sink.topic' = 'test',
            'sink.failure_strategy' = '{}'
        "#,
            strategy
        );

        let test_config = parser.parse_with_clause(&test_clause)?;
        assert_eq!(
            test_config.raw_config.get("sink.failure_strategy").unwrap(),
            strategy
        );
        println!("✅ {} parsed successfully", strategy);
    }

    println!("\n🎉 All failure strategy tests passed!");
    Ok(())
}
