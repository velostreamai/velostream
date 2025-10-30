use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pause_job_basic() {
        let parser = StreamingSqlParser::new();

        let query = "PAUSE JOB order_monitor";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse PAUSE JOB: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::PauseJob { name } => {
                assert_eq!(name, "order_monitor");
            }
            _ => panic!("Expected PauseJob"),
        }
    }

    #[test]
    fn test_resume_job_basic() {
        let parser = StreamingSqlParser::new();

        let query = "RESUME JOB order_monitor";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse RESUME JOB: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::ResumeJob { name } => {
                assert_eq!(name, "order_monitor");
            }
            _ => panic!("Expected ResumeJob"),
        }
    }

    #[test]
    fn test_deploy_job_basic() {
        let parser = StreamingSqlParser::new();

        let query =
            "DEPLOY JOB order_monitor VERSION '1.2.0' AS SELECT * FROM orders WHERE amount > 100";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse DEPLOY JOB: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::DeployJob {
                name,
                version,
                query,
                properties,
                strategy,
            } => {
                assert_eq!(name, "order_monitor");
                assert_eq!(version, "1.2.0");
                assert!(properties.is_empty());
                assert_eq!(strategy, DeploymentStrategy::BlueGreen); // Default strategy

                // Check the underlying query
                match *query {
                    StreamingQuery::Select { .. } => {} // Expected
                    _ => panic!("Expected Select query inside DEPLOY JOB"),
                }
            }
            _ => panic!("Expected DeployJob"),
        }
    }

    #[test]
    fn test_deploy_job_with_strategy_blue_green() {
        let parser = StreamingSqlParser::new();

        let query = "DEPLOY JOB analytics VERSION '2.0.0' AS SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id STRATEGY BLUE_GREEN";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse DEPLOY JOB with BLUE_GREEN: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::DeployJob {
                name,
                version,
                strategy,
                ..
            } => {
                assert_eq!(name, "analytics");
                assert_eq!(version, "2.0.0");
                assert_eq!(strategy, DeploymentStrategy::BlueGreen);
            }
            _ => panic!("Expected DeployJob"),
        }
    }

    #[test]
    fn test_deploy_job_with_strategy_canary() {
        let parser = StreamingSqlParser::new();

        let query =
            "DEPLOY JOB analytics VERSION '2.1.0' AS SELECT * FROM orders STRATEGY CANARY(25)";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse DEPLOY JOB with CANARY: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::DeployJob {
                name,
                version,
                strategy,
                ..
            } => {
                assert_eq!(name, "analytics");
                assert_eq!(version, "2.1.0");
                assert_eq!(strategy, DeploymentStrategy::Canary { percentage: 25 });
            }
            _ => panic!("Expected DeployJob"),
        }
    }

    #[test]
    fn test_deploy_job_with_strategy_rolling() {
        let parser = StreamingSqlParser::new();

        let query = "DEPLOY JOB processor VERSION '1.5.2' AS CREATE STREAM processed_orders AS SELECT * FROM orders WHERE order_status = 'validated' STRATEGY ROLLING";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse DEPLOY JOB with ROLLING: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::DeployJob {
                name,
                version,
                strategy,
                ..
            } => {
                assert_eq!(name, "processor");
                assert_eq!(version, "1.5.2");
                assert_eq!(strategy, DeploymentStrategy::Rolling);
            }
            _ => panic!("Expected DeployJob"),
        }
    }

    #[test]
    fn test_deploy_job_with_strategy_replace() {
        let parser = StreamingSqlParser::new();

        let query = "DEPLOY JOB quick_fix VERSION '1.0.1' AS SELECT * FROM alerts WHERE severity = 'critical' STRATEGY REPLACE";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse DEPLOY JOB with REPLACE: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::DeployJob {
                name,
                version,
                strategy,
                ..
            } => {
                assert_eq!(name, "quick_fix");
                assert_eq!(version, "1.0.1");
                assert_eq!(strategy, DeploymentStrategy::Replace);
            }
            _ => panic!("Expected DeployJob"),
        }
    }

    #[test]
    fn test_deploy_job_with_properties() {
        let parser = StreamingSqlParser::new();

        let query = "DEPLOY JOB enriched_orders VERSION '3.0.0' AS SELECT * FROM orders WHERE amount > 500 WITH ('replicas' = '3', 'memory.limit' = '2Gi')";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse DEPLOY JOB with properties: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::DeployJob {
                name,
                version,
                properties,
                ..
            } => {
                assert_eq!(name, "enriched_orders");
                assert_eq!(version, "3.0.0");
                assert_eq!(properties.len(), 2);
                assert_eq!(properties.get("replicas"), Some(&"3".to_string()));
                assert_eq!(properties.get("memory.limit"), Some(&"2Gi".to_string()));
            }
            _ => panic!("Expected DeployJob"),
        }
    }

    #[test]
    fn test_rollback_job_basic() {
        let parser = StreamingSqlParser::new();

        let query = "ROLLBACK JOB order_monitor";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse ROLLBACK JOB: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::RollbackJob {
                name,
                target_version,
            } => {
                assert_eq!(name, "order_monitor");
                assert!(target_version.is_none()); // No specific version specified
            }
            _ => panic!("Expected RollbackJob"),
        }
    }

    #[test]
    fn test_rollback_job_with_version() {
        let parser = StreamingSqlParser::new();

        let query = "ROLLBACK JOB analytics VERSION '1.5.0'";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse ROLLBACK JOB with version: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::RollbackJob {
                name,
                target_version,
            } => {
                assert_eq!(name, "analytics");
                assert_eq!(target_version, Some("1.5.0".to_string()));
            }
            _ => panic!("Expected RollbackJob"),
        }
    }

    #[test]
    fn test_case_insensitive_lifecycle_commands() {
        let parser = StreamingSqlParser::new();

        let test_cases = vec![
            ("pause query test", "PauseJob"),
            ("PAUSE JOB test", "PauseJob"),
            ("Pause Query test", "PauseJob"),
            ("resume query test", "ResumeJob"),
            ("RESUME JOB test", "ResumeJob"),
            ("Resume Query test", "ResumeJob"),
        ];

        for (query_str, expected_type) in test_cases {
            let result = parser.parse(query_str);
            assert!(
                result.is_ok(),
                "Failed to parse case-insensitive {}: {}",
                expected_type,
                query_str
            );

            match result.unwrap() {
                StreamingQuery::PauseJob { name } if expected_type == "PauseJob" => {
                    assert_eq!(name, "test");
                }
                StreamingQuery::ResumeJob { name } if expected_type == "ResumeJob" => {
                    assert_eq!(name, "test");
                }
                _ => panic!("Expected {} for: {}", expected_type, query_str),
            }
        }
    }

    #[test]
    fn test_invalid_canary_percentage() {
        let parser = StreamingSqlParser::new();

        let query = "DEPLOY JOB test VERSION '1.0.0' AS SELECT * FROM orders STRATEGY CANARY(150)";
        let result = parser.parse(query);

        // This should parse successfully but validation would catch invalid percentage
        assert!(
            result.is_ok(),
            "Parser should accept any number, validation handles range"
        );
    }

    #[test]
    fn test_missing_query_keyword_in_pause() {
        let parser = StreamingSqlParser::new();

        let query = "PAUSE test";
        let result = parser.parse(query);
        assert!(result.is_err(), "Should fail when QUERY keyword is missing");
    }

    #[test]
    fn test_missing_version_in_deploy() {
        let parser = StreamingSqlParser::new();

        let query = "DEPLOY JOB test AS SELECT * FROM orders";
        let result = parser.parse(query);
        assert!(result.is_err(), "Should fail when VERSION is missing");
    }

    #[test]
    fn test_deploy_job_complex_underlying_query() {
        let parser = StreamingSqlParser::new();

        let query = "DEPLOY JOB complex_analytics VERSION '2.5.0' AS SELECT customer_id, COUNT(*) as order_count, AVG(amount) as avg_amount FROM orders WHERE created_at > '2024-01-01' GROUP BY customer_id HAVING COUNT(*) > 10 ORDER BY avg_amount DESC LIMIT 100";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse DEPLOY JOB with complex SELECT: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::DeployJob {
                name,
                version,
                query,
                ..
            } => {
                assert_eq!(name, "complex_analytics");
                assert_eq!(version, "2.5.0");

                // Verify the underlying query is properly parsed
                match *query {
                    StreamingQuery::Select {
                        fields,
                        where_clause,
                        group_by,
                        having,
                        order_by,
                        limit,
                        ..
                    } => {
                        assert_eq!(fields.len(), 3); // customer_id, COUNT(*), AVG(amount)
                        assert!(where_clause.is_some());
                        assert!(group_by.is_some());
                        assert!(having.is_some());
                        assert!(order_by.is_some());
                        assert_eq!(limit, Some(100));
                    }
                    _ => panic!("Expected Select query inside DEPLOY JOB"),
                }
            }
            _ => panic!("Expected DeployJob"),
        }
    }

    #[test]
    fn test_get_columns_for_lifecycle_commands() {
        let parser = StreamingSqlParser::new();

        // Test that lifecycle commands that don't reference columns return empty vec
        let lifecycle_queries = vec!["PAUSE JOB test", "RESUME JOB test", "ROLLBACK JOB test"];

        for query_str in lifecycle_queries {
            let result = parser.parse(query_str);
            assert!(result.is_ok());

            let columns = result.unwrap().get_columns();
            assert!(
                columns.is_empty(),
                "Lifecycle command should not reference columns: {}",
                query_str
            );
        }

        // Test that DEPLOY JOB gets columns from underlying query
        let deploy_query = "DEPLOY JOB test VERSION '1.0.0' AS SELECT customer_id, amount FROM orders WHERE order_status = 'active'";
        let result = parser.parse(deploy_query);
        assert!(result.is_ok());

        let columns = result.unwrap().get_columns();
        assert!(columns.contains(&"customer_id".to_string()));
        assert!(columns.contains(&"amount".to_string()));
        assert!(columns.contains(&"order_status".to_string()));
    }

    #[test]
    fn test_has_window_for_lifecycle_commands() {
        let parser = StreamingSqlParser::new();

        // Test commands that don't have windows
        let no_window_queries = vec!["PAUSE JOB test", "RESUME JOB test", "ROLLBACK JOB test"];

        for query_str in no_window_queries {
            let result = parser.parse(query_str);
            assert!(result.is_ok());
            assert!(
                !result.unwrap().has_window(),
                "Command should not have window: {}",
                query_str
            );
        }

        // Test DEPLOY JOB with windowed underlying query
        let windowed_deploy = "DEPLOY JOB test VERSION '1.0.0' AS SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id WINDOW TUMBLING(5m)";
        let result = parser.parse(windowed_deploy);
        assert!(result.is_ok());
        assert!(
            result.unwrap().has_window(),
            "DEPLOY JOB should inherit window from underlying query"
        );
    }
}
