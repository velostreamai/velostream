use velostream::velostream::sql::ast::*;
use velostream::velostream::sql::execution::types::system_columns;
use velostream::velostream::sql::parser::StreamingSqlParser;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substring_function_basic() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT SUBSTRING(message, 1, 5) as msg_prefix FROM events";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse SUBSTRING function: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("msg_prefix".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "SUBSTRING");
                                assert_eq!(args.len(), 3);

                                // Check arguments
                                match &args[1] {
                                    Expr::Literal(LiteralValue::Integer(start)) => {
                                        assert_eq!(*start, 1);
                                    }
                                    _ => panic!("Expected integer literal for start position"),
                                }

                                match &args[2] {
                                    Expr::Literal(LiteralValue::Integer(length)) => {
                                        assert_eq!(*length, 5);
                                    }
                                    _ => panic!("Expected integer literal for length"),
                                }
                            }
                            _ => panic!("Expected SUBSTRING function"),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_substring_function_without_length() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT SUBSTRING(full_name, 6) as last_name FROM users";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse SUBSTRING without length: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("last_name".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "SUBSTRING");
                                assert_eq!(args.len(), 2); // Only string and start position
                            }
                            _ => panic!("Expected SUBSTRING function"),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_json_extract_function() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT JSON_EXTRACT(payload, '$.user.name') as user_name FROM events";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse JSON_EXTRACT function: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("user_name".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "JSON_EXTRACT");
                                assert_eq!(args.len(), 2);

                                // Check path argument
                                match &args[1] {
                                    Expr::Literal(LiteralValue::String(path)) => {
                                        assert_eq!(path, "$.user.name");
                                    }
                                    _ => panic!("Expected string literal for JSON path"),
                                }
                            }
                            _ => panic!("Expected JSON_EXTRACT function"),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_json_value_function() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT JSON_VALUE(metadata, 'order.total') as order_total FROM transactions";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse JSON_VALUE function: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("order_total".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "JSON_VALUE");
                                assert_eq!(args.len(), 2);

                                // Check path argument
                                match &args[1] {
                                    Expr::Literal(LiteralValue::String(path)) => {
                                        assert_eq!(path, "order.total");
                                    }
                                    _ => panic!("Expected string literal for JSON path"),
                                }
                            }
                            _ => panic!("Expected JSON_VALUE function"),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_json_extract_with_array_access() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT JSON_EXTRACT(data, '$.items[0].id') as first_item_id FROM collections";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse JSON_EXTRACT with array access: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("first_item_id".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "JSON_EXTRACT");
                                assert_eq!(args.len(), 2);

                                // Check path argument with array access
                                match &args[1] {
                                    Expr::Literal(LiteralValue::String(path)) => {
                                        assert_eq!(path, "$.items[0].id");
                                    }
                                    _ => panic!("Expected string literal for JSON path"),
                                }
                            }
                            _ => panic!("Expected JSON_EXTRACT function"),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_complex_query_with_string_and_json_functions() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT
            event_id,
            SUBSTRING(message, 1, 50) as short_message,
            JSON_EXTRACT(payload, '$.user.id') as user_id,
            JSON_VALUE(payload, '$.timestamp') as event_time,
            CAST(JSON_VALUE(payload, '$.amount') AS FLOAT) as amount_float
        FROM kafka_events
        WHERE JSON_VALUE(payload, '$.type') = 'purchase'
        ORDER BY event_time DESC
        LIMIT 100";

        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse complex query with string/JSON functions: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select {
                fields,
                where_clause,
                order_by,
                limit,
                ..
            } => {
                assert_eq!(fields.len(), 5);
                assert!(where_clause.is_some());
                assert!(order_by.is_some());
                assert_eq!(limit, Some(100));

                // Verify the functions are parsed correctly
                let function_names: Vec<String> = fields
                    .iter()
                    .filter_map(|field| match field {
                        SelectField::Expression {
                            expr: Expr::Function { name, .. },
                            ..
                        } => Some(name.to_uppercase()),
                        _ => None,
                    })
                    .collect();

                assert!(function_names.contains(&"SUBSTRING".to_string()));
                assert!(function_names.contains(&"JSON_EXTRACT".to_string()));
                assert!(function_names.contains(&"JSON_VALUE".to_string()));
                assert!(function_names.contains(&"CAST".to_string()));
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_nested_string_json_functions() {
        let parser = StreamingSqlParser::new();

        let query = "SELECT SUBSTRING(JSON_VALUE(data, '$.description'), 1, 20) as short_desc FROM products";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse nested string/JSON functions: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select { fields, .. } => {
                match &fields[0] {
                    SelectField::Expression { expr, alias } => {
                        assert_eq!(alias, &Some("short_desc".to_string()));
                        match expr {
                            Expr::Function { name, args } => {
                                assert_eq!(name.to_uppercase(), "SUBSTRING");
                                assert_eq!(args.len(), 3);

                                // Check that the first argument is a JSON_VALUE function
                                match &args[0] {
                                    Expr::Function {
                                        name: inner_name,
                                        args: inner_args,
                                    } => {
                                        assert_eq!(inner_name.to_uppercase(), "JSON_VALUE");
                                        assert_eq!(inner_args.len(), 2);
                                    }
                                    _ => panic!(
                                        "Expected JSON_VALUE function as first argument to SUBSTRING"
                                    ),
                                }
                            }
                            _ => panic!("Expected SUBSTRING function"),
                        }
                    }
                    _ => panic!("Expected expression field"),
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_job_deployment_with_json_processing() {
        let parser = StreamingSqlParser::new();

        let query = "DEPLOY JOB json_processor VERSION '1.0.0' AS
            SELECT
                event_id,
                JSON_EXTRACT(payload, '$.customer.id') as customer_id,
                CAST(JSON_VALUE(payload, '$.order.amount') AS FLOAT) as order_amount,
                SUBSTRING(JSON_VALUE(payload, '$.customer.email'), 1,
                         CAST(JSON_VALUE(payload, '$.customer.email_length') AS INTEGER)) as email_prefix
            FROM order_events
            WHERE JSON_VALUE(payload, '$.order.status') = 'completed'
            STRATEGY CANARY(20)";

        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse job deployment with JSON processing: {:?}",
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
                assert_eq!(name, "json_processor");
                assert_eq!(version, "1.0.0");
                assert_eq!(strategy, DeploymentStrategy::Canary { percentage: 20 });

                // Verify the underlying query contains JSON functions
                match *query {
                    StreamingQuery::Select { fields, .. } => {
                        assert_eq!(fields.len(), 4);

                        // Check that functions are present
                        let has_json_functions = fields.iter().any(|field| match field {
                            SelectField::Expression { expr, .. } => {
                                matches!(expr, Expr::Function { name, .. } if
                                        name.to_uppercase() == "JSON_EXTRACT" ||
                                        name.to_uppercase() == "JSON_VALUE")
                            }
                            _ => false,
                        });
                        assert!(
                            has_json_functions,
                            "Expected to find JSON function calls in deployed query"
                        );
                    }
                    _ => panic!("Expected Select query inside deployment"),
                }
            }
            _ => panic!("Expected DeployJob"),
        }
    }

    #[test]
    fn test_case_insensitive_string_json_functions() {
        let parser = StreamingSqlParser::new();

        let test_cases = vec![
            "select substring(msg, 1, 5) from events",
            "SELECT SUBSTRING(msg, 1, 5) FROM events",
            "Select Substring(msg, 1, 5) From events",
            "select json_extract(data, '$.id') from events",
            "SELECT JSON_EXTRACT(data, '$.id') FROM events",
            "Select Json_Extract(data, '$.id') From events",
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
                            let upper_name = name.to_uppercase();
                            assert!(
                                upper_name == "SUBSTRING" || upper_name == "JSON_EXTRACT",
                                "Expected SUBSTRING or JSON_EXTRACT, got: {}",
                                upper_name
                            );
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
    fn test_function_argument_errors() {
        let parser = StreamingSqlParser::new();

        // These should parse successfully (validation happens at execution time)
        let cases_that_should_parse = vec![
            "SELECT SUBSTRING(msg) FROM events", // Missing required arguments
            "SELECT JSON_EXTRACT(data) FROM events", // Missing path
            "SELECT JSON_VALUE() FROM events",   // No arguments
        ];

        for query_str in cases_that_should_parse {
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
    fn test_kafka_payload_processing_example() {
        let parser = StreamingSqlParser::new();

        // Real-world example for processing Kafka messages with nested JSON
        let query = "SELECT
            _timestamp as kafka_timestamp,
            _partition as kafka_partition,
            JSON_VALUE(value, '$.eventType') as event_type,
            JSON_EXTRACT(value, '$.user') as user_data,
            CAST(JSON_VALUE(value, '$.user.id') AS INTEGER) as user_id,
            SUBSTRING(JSON_VALUE(value, '$.user.email'), 1, 50) as email_truncated,
            JSON_VALUE(value, '$.order.items[0].name') as first_item_name,
            CAST(JSON_VALUE(value, '$.order.total') AS FLOAT) as order_total
        FROM kafka_topic_orders
        WHERE JSON_VALUE(value, '$.eventType') IN ('ORDER_CREATED', 'ORDER_UPDATED')
        AND CAST(JSON_VALUE(value, '$.order.total') AS FLOAT) > 100.0";

        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse Kafka payload processing query: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Select {
                fields,
                where_clause,
                ..
            } => {
                assert_eq!(fields.len(), 8);
                assert!(where_clause.is_some());

                // Verify we have the expected system columns and JSON functions
                let has_system_columns = fields.iter().any(|field| match field {
                    SelectField::AliasedColumn { column, .. } => {
                        column == system_columns::TIMESTAMP || column == system_columns::PARTITION
                    }
                    SelectField::Column(column) => {
                        column == system_columns::TIMESTAMP || column == system_columns::PARTITION
                    }
                    SelectField::Expression { expr, .. } => match expr {
                        Expr::Column(column) => {
                            column == system_columns::TIMESTAMP
                                || column == system_columns::PARTITION
                        }
                        _ => false,
                    },
                    _ => false,
                });
                assert!(
                    has_system_columns,
                    "Expected system columns for Kafka metadata"
                );

                let has_json_functions = fields.iter().any(|field| match field {
                    SelectField::Expression { expr, .. } => {
                        matches!(expr, Expr::Function { name, .. } if
                                name.to_uppercase() == "JSON_VALUE" ||
                                name.to_uppercase() == "JSON_EXTRACT")
                    }
                    _ => false,
                });
                assert!(has_json_functions, "Expected JSON processing functions");
            }
            _ => panic!("Expected Select query"),
        }
    }
}
