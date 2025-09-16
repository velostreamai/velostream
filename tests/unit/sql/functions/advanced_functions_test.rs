use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_command() {
        let parser = StreamingSqlParser::new();

        let query = "DESCRIBE orders";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse DESCRIBE: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(
                    resource_type,
                    ShowResourceType::Describe {
                        name: "orders".to_string()
                    }
                );
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query with Describe resource type"),
        }
    }

    #[test]
    fn test_advanced_aggregation_functions() {
        let parser = StreamingSqlParser::new();

        let functions = vec![
            "MIN(amount)",
            "MAX(amount)",
            "FIRST_VALUE(customer_id)",
            "LAST_VALUE(order_date)",
            "APPROX_COUNT_DISTINCT(customer_id)",
        ];

        for func in functions {
            let query = format!("SELECT {} FROM orders", func);
            let result = parser.parse(&query);
            assert!(
                result.is_ok(),
                "Failed to parse query with {}: {:?}",
                func,
                result.err()
            );

            match result.unwrap() {
                StreamingQuery::Select { fields, .. } => {
                    assert_eq!(fields.len(), 1);
                    match &fields[0] {
                        SelectField::Expression { expr, .. } => match expr {
                            Expr::Function { name, args } => {
                                assert!(!name.is_empty());
                                assert_eq!(args.len(), 1);
                            }
                            _ => panic!("Expected function expression"),
                        },
                        _ => panic!("Expected expression field"),
                    }
                }
                _ => panic!("Expected Select query"),
            }
        }
    }

    #[test]
    fn test_timestamp_function() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT customer_id, TIMESTAMP() as event_time FROM orders";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse TIMESTAMP() function: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 2);

                // Check the TIMESTAMP() function
                match &fields[1] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("event_time".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "TIMESTAMP");
                                assert!(args.is_empty());
                            }
                            _ => panic!("Expected TIMESTAMP function"),
                        }
                    }
                    _ => panic!("Expected expression field for TIMESTAMP"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_cast_function() {
        let parser = StreamingSqlParser::new();

        let test_cases = vec![
            ("CAST(amount, 'INTEGER')", "INTEGER"),
            ("CAST(price, 'FLOAT')", "FLOAT"),
            ("CAST(customer_id, 'STRING')", "STRING"),
            ("CAST(is_active, 'BOOLEAN')", "BOOLEAN"),
        ];

        for (cast_expr, expected_type) in test_cases {
            let query = format!("SELECT {} FROM orders", cast_expr);
            let result = parser.parse(&query);
            assert!(
                result.is_ok(),
                "Failed to parse CAST function {}: {:?}",
                cast_expr,
                result.err()
            );

            match result.unwrap() {
                StreamingQuery::Select { fields, .. } => {
                    match &fields[0] {
                        SelectField::Expression { expr, .. } => {
                            match expr {
                                Expr::Function { name, args } => {
                                    assert_eq!(name.to_uppercase(), "CAST");
                                    assert_eq!(args.len(), 2);

                                    // Check the target type argument
                                    match &args[1] {
                                        Expr::Literal(LiteralValue::String(type_str)) => {
                                            assert_eq!(type_str, expected_type);
                                        }
                                        _ => panic!("Expected string literal for CAST target type"),
                                    }
                                }
                                _ => panic!("Expected CAST function"),
                            }
                        }
                        _ => panic!("Expected expression field"),
                    }
                }
                _ => panic!("Expected Select query"),
            }
        }
    }

    #[test]
    fn test_split_function() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT SPLIT(full_name, ' ') as first_name FROM customers";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse SPLIT function: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("first_name".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "SPLIT");
                                assert_eq!(args.len(), 2);

                                // Check the delimiter argument
                                match &args[1] {
                                    Expr::Literal(LiteralValue::String(delimiter)) => {
                                        assert_eq!(delimiter, " ");
                                    }
                                    _ => panic!("Expected string literal for SPLIT delimiter"),
                                }
                            }
                            _ => panic!("Expected SPLIT function"),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_join_function() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT JOIN(', ', first_name, last_name) as full_name FROM customers";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse JOIN function: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("full_name".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "JOIN");
                                assert_eq!(args.len(), 3); // delimiter + 2 values

                                // Check the delimiter argument
                                match &args[0] {
                                    Expr::Literal(LiteralValue::String(delimiter)) => {
                                        assert_eq!(delimiter, ", ");
                                    }
                                    _ => panic!("Expected string literal for JOIN delimiter"),
                                }
                            }
                            _ => panic!("Expected JOIN function"),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_complex_query_with_multiple_functions() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT 
            customer_id,
            CAST(amount, 'FLOAT') as amount_float,
            TIMESTAMP() as event_time,
            FIRST_VALUE(product_name) as first_product,
            APPROX_COUNT_DISTINCT(category) as unique_categories,
            SPLIT(address, ', ') as city,
            JOIN(' - ', order_id, customer_id) as order_key
        FROM orders 
        WHERE amount > 100 
        GROUP BY customer_id 
        ORDER BY amount DESC 
        LIMIT 50";

        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse complex query with multiple functions: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select {
                fields,
                where_clause,
                group_by,
                order_by,
                limit,
                ..
            } => {
                assert_eq!(fields.len(), 7);
                assert!(where_clause.is_some());
                assert!(group_by.is_some());
                assert!(order_by.is_some());
                assert_eq!(limit, Some(50));

                // Verify the functions are parsed correctly
                let function_names: Vec<String> = fields
                    .iter()
                    .filter_map(|field| match field {
                        SelectField::Expression { expr, .. } => match expr {
                            Expr::Function { name, .. } => Some(name.to_uppercase()),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect();

                assert!(function_names.contains(&"CAST".to_string()));
                assert!(function_names.contains(&"TIMESTAMP".to_string()));
                assert!(function_names.contains(&"FIRST_VALUE".to_string()));
                assert!(function_names.contains(&"APPROX_COUNT_DISTINCT".to_string()));
                assert!(function_names.contains(&"SPLIT".to_string()));
                assert!(function_names.contains(&"JOIN".to_string()));
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_show_enhanced_commands() {
        let parser = StreamingSqlParser::new();

        let test_cases = vec![
            ("SHOW STATUS", ShowResourceType::JobStatus { name: None }),
            (
                "SHOW STATUS analytics",
                ShowResourceType::JobStatus {
                    name: Some("analytics".to_string()),
                },
            ),
            (
                "SHOW VERSIONS order_monitor",
                ShowResourceType::JobVersions {
                    name: "order_monitor".to_string(),
                },
            ),
            ("SHOW METRICS", ShowResourceType::JobMetrics { name: None }),
            (
                "SHOW METRICS analytics",
                ShowResourceType::JobMetrics {
                    name: Some("analytics".to_string()),
                },
            ),
        ];

        for (query_str, expected_resource_type) in test_cases {
            let result = parser.parse(query_str);
            assert!(
                result.is_ok(),
                "Failed to parse {}: {:?}",
                query_str,
                result.err()
            );

            match result.unwrap() {
                StreamingQuery::Show { resource_type, .. } => {
                    assert_eq!(resource_type, expected_resource_type);
                }
                _ => panic!("Expected Show query for: {}", query_str),
            }
        }
    }

    #[test]
    fn test_nested_function_calls() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT CAST(SPLIT(metadata, ':'), 'INTEGER') as port FROM servers";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse nested function calls: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("port".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "CAST");
                                assert_eq!(args.len(), 2);

                                // Check that the first argument is a SPLIT function
                                match &args[0] {
                                    Expr::Function {
                                        name: inner_name,
                                        args: inner_args,
                                    } => {
                                        assert_eq!(inner_name.to_uppercase(), "SPLIT");
                                        assert_eq!(inner_args.len(), 2);
                                    }
                                    _ => {
                                        panic!("Expected SPLIT function as first argument to CAST")
                                    }
                                }
                            }
                            _ => panic!("Expected CAST function"),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_case_insensitive_function_names() {
        let parser = StreamingSqlParser::new();

        let test_cases = vec![
            "select timestamp() from orders",
            "SELECT TIMESTAMP() FROM orders",
            "Select Timestamp() From orders",
            "sElEcT tImEsTaMp() fRoM orders",
        ];

        for query_str in test_cases {
            let result = parser.parse(query_str);
            assert!(
                result.is_ok(),
                "Failed to parse case-insensitive function: {}",
                query_str
            );

            match result.unwrap() {
                StreamingQuery::Select { fields, .. } => match &fields[0] {
                    SelectField::Expression { expr, .. } => match expr {
                        Expr::Function { name, .. } => {
                            assert_eq!(name.to_uppercase(), "TIMESTAMP");
                        }
                        _ => panic!("Expected function expression"),
                    },
                    _ => panic!("Expected expression field"),
                },
                _ => panic!("Expected Select query"),
            }
        }
    }

    #[test]
    fn test_function_error_cases() {
        let parser = StreamingSqlParser::new();

        // These should parse successfully (validation happens at execution time)
        let valid_cases = vec![
            "SELECT CAST(amount) FROM orders", // Missing type argument
            "SELECT SPLIT(name) FROM orders",  // Missing delimiter
            "SELECT JOIN() FROM orders",       // No arguments
        ];

        for query_str in valid_cases {
            let result = parser.parse(query_str);
            // Parser should succeed, execution engine will catch argument errors
            assert!(
                result.is_ok(),
                "Parser should accept function call: {}",
                query_str
            );
        }
    }

    #[test]
    fn test_show_jobs_compatibility() {
        let parser = StreamingSqlParser::new();

        // Test that SHOW JOBS still works (backward compatibility)
        let query = "SHOW JOBS";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse SHOW JOBS: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Show { resource_type, .. } => {
                assert_eq!(resource_type, ShowResourceType::Jobs);
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_deployment_with_advanced_functions() {
        let parser = StreamingSqlParser::new();

        let query = "DEPLOY JOB enrichment VERSION '2.0.0' AS 
            SELECT 
                customer_id,
                CAST(amount, 'FLOAT') as amount_float,
                SPLIT(address, ', ') as city,
                TIMESTAMP() as processed_at
            FROM orders 
            WHERE amount > CAST('100', 'INTEGER')
            STRATEGY CANARY(10)";

        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse deployment with advanced functions: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::DeployJob {
                name,
                version,
                query,
                strategy,
                ..
            } => {
                assert_eq!(name, "enrichment");
                assert_eq!(version, "2.0.0");
                assert_eq!(strategy, DeploymentStrategy::Canary { percentage: 10 });

                // Verify the underlying query contains functions
                match *query {
                    StreamingQuery::Select { fields, .. } => {
                        assert_eq!(fields.len(), 4);

                        // Check that functions are present
                        let has_functions = fields.iter().any(|field| match field {
                            SelectField::Expression { expr, .. } => {
                                matches!(expr, Expr::Function { .. })
                            }
                            _ => false,
                        });
                        assert!(
                            has_functions,
                            "Expected to find function calls in deployed query"
                        );
                    }
                    _ => panic!("Expected Select query inside deployment"),
                }
            }
            _ => panic!("Expected DeployJob"),
        }
    }
}
