use ferrisstreams::ferris::sql::ast::*;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_streams() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW STREAMS";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse SHOW STREAMS: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(resource_type, ShowResourceType::Streams);
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_list_streams() {
        let parser = StreamingSqlParser::new();

        let query = "LIST STREAMS";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse LIST STREAMS: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(resource_type, ShowResourceType::Streams);
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_show_tables() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW TABLES";
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse SHOW TABLES");

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(resource_type, ShowResourceType::Tables);
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_show_topics() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW TOPICS";
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse SHOW TOPICS");

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(resource_type, ShowResourceType::Topics);
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_show_functions() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW FUNCTIONS";
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse SHOW FUNCTIONS");

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(resource_type, ShowResourceType::Functions);
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_show_queries() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW JOBS";
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse SHOW JOBS");

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(resource_type, ShowResourceType::Jobs);
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_show_schema() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW SCHEMA orders";
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse SHOW SCHEMA");

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(
                    resource_type,
                    ShowResourceType::Schema {
                        name: "orders".to_string()
                    }
                );
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_show_properties() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW PROPERTIES STREAM orders";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse SHOW PROPERTIES: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(
                    resource_type,
                    ShowResourceType::Properties {
                        resource_type: "STREAM".to_string(),
                        name: "orders".to_string()
                    }
                );
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_show_partitions() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW PARTITIONS orders";
        let result = parser.parse(query);
        assert!(result.is_ok(), "Failed to parse SHOW PARTITIONS");

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(
                    resource_type,
                    ShowResourceType::Partitions {
                        name: "orders".to_string()
                    }
                );
                assert!(pattern.is_none());
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_show_streams_with_like_pattern() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW STREAMS LIKE 'order%'";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse SHOW STREAMS with LIKE pattern: {:?}",
            result.err()
        );

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(resource_type, ShowResourceType::Streams);
                assert_eq!(pattern, Some("order%".to_string()));
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_show_tables_with_like_pattern() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW TABLES LIKE 'user_*'";
        let result = parser.parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse SHOW TABLES with LIKE pattern"
        );

        match result.unwrap() {
            StreamingQuery::Show {
                resource_type,
                pattern,
            } => {
                assert_eq!(resource_type, ShowResourceType::Tables);
                assert_eq!(pattern, Some("user_*".to_string()));
            }
            _ => panic!("Expected Show query"),
        }
    }

    #[test]
    fn test_case_insensitive_show_commands() {
        let parser = StreamingSqlParser::new();

        let queries = vec![
            "show streams",
            "SHOW STREAMS",
            "Show Streams",
            "sHoW sTrEaMs",
        ];

        for query in queries {
            let result = parser.parse(query);
            assert!(
                result.is_ok(),
                "Failed to parse case-insensitive query: {}",
                query
            );

            match result.unwrap() {
                StreamingQuery::Show { resource_type, .. } => {
                    assert_eq!(resource_type, ShowResourceType::Streams);
                }
                _ => panic!("Expected Show query for: {}", query),
            }
        }
    }

    #[test]
    fn test_invalid_show_command() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW INVALID";
        let result = parser.parse(query);
        assert!(result.is_err(), "Should fail to parse invalid SHOW command");
    }

    #[test]
    fn test_show_schema_missing_name() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW SCHEMA";
        let result = parser.parse(query);
        assert!(
            result.is_err(),
            "Should fail when SHOW SCHEMA is missing resource name"
        );
    }

    #[test]
    fn test_show_properties_missing_args() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW PROPERTIES";
        let result = parser.parse(query);
        assert!(
            result.is_err(),
            "Should fail when SHOW PROPERTIES is missing arguments"
        );
    }

    #[test]
    fn test_show_partitions_missing_name() {
        let parser = StreamingSqlParser::new();

        let query = "SHOW PARTITIONS";
        let result = parser.parse(query);
        assert!(
            result.is_err(),
            "Should fail when SHOW PARTITIONS is missing resource name"
        );
    }
}
