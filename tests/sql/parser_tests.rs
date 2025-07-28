use ferrisstreams::ferris::sql::ast::*;
use ferrisstreams::ferris::sql::parser::StreamingSqlParser;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_all() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT * FROM orders");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { fields, from, where_clause, window, limit } => {
                assert_eq!(fields.len(), 1);
                assert!(matches!(fields[0], SelectField::Wildcard));
                assert!(matches!(from, StreamSource::Stream(_)));
                assert!(where_clause.is_none());
                assert!(window.is_none());
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_select_specific_columns() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT customer_id, amount, timestamp FROM orders");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { fields, from, .. } => {
                assert_eq!(fields.len(), 3);
                assert!(matches!(from, StreamSource::Stream(_)));
                
                for field in &fields {
                    assert!(matches!(field, SelectField::Expression { .. }));
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_select_with_alias() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT customer_id AS cid, amount AS total FROM orders");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 2);
                
                if let SelectField::Expression { alias, .. } = &fields[0] {
                    assert_eq!(alias.as_ref().unwrap(), "cid");
                }
                
                if let SelectField::Expression { alias, .. } = &fields[1] {
                    assert_eq!(alias.as_ref().unwrap(), "total");
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_tumbling_window() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT COUNT(*) FROM orders WINDOW TUMBLING(5m)");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { window, limit: None, .. } => {
                assert!(window.is_some());
                let window_spec = window.unwrap();
                
                assert!(matches!(window_spec, WindowSpec::Tumbling { .. }));
                if let WindowSpec::Tumbling { size, .. } = window_spec {
                    assert_eq!(size.as_secs(), 300); // 5 minutes
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_sliding_window() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT AVG(amount) FROM orders WINDOW SLIDING(10m, 5m)");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { window, limit: None, .. } => {
                assert!(window.is_some());
                let window_spec = window.unwrap();
                
                assert!(matches!(window_spec, WindowSpec::Sliding { .. }));
                if let WindowSpec::Sliding { size, advance, .. } = window_spec {
                    assert_eq!(size.as_secs(), 600); // 10 minutes
                    assert_eq!(advance.as_secs(), 300); // 5 minutes
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_session_window() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT SUM(amount) FROM orders WINDOW SESSION(30s)");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { window, limit: None, .. } => {
                assert!(window.is_some());
                let window_spec = window.unwrap();
                
                assert!(matches!(window_spec, WindowSpec::Session { .. }));
                if let WindowSpec::Session { gap, .. } = window_spec {
                    assert_eq!(gap.as_secs(), 30);
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_invalid_sql() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("INVALID SQL QUERY");
        
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_from_clause() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT *");
        
        assert!(result.is_err());
    }

    #[test]
    fn test_string_literals() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT 'hello world', \"another string\" FROM orders");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 2);
                
                for field in &fields {
                    if let SelectField::Expression { expr, .. } = field {
                        assert!(matches!(expr, Expr::Literal(LiteralValue::String(_))));
                    }
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_numeric_literals() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT 42, 3.14, 0 FROM orders");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 3);
                
                // Check first field (integer)
                if let SelectField::Expression { expr, .. } = &fields[0] {
                    assert!(matches!(expr, Expr::Literal(LiteralValue::Integer(42))));
                }
                
                // Check second field (float)
                if let SelectField::Expression { expr, .. } = &fields[1] {
                    assert!(matches!(expr, Expr::Literal(LiteralValue::Float(_))));
                }
                
                // Check third field (integer zero)
                if let SelectField::Expression { expr, .. } = &fields[2] {
                    assert!(matches!(expr, Expr::Literal(LiteralValue::Integer(0))));
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_column_references() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT customer_id FROM orders");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 1);
                
                if let SelectField::Expression { expr, .. } = &fields[0] {
                    assert!(matches!(expr, Expr::Column(_)));
                    if let Expr::Column(name) = expr {
                        assert_eq!(name, "customer_id");
                    }
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_qualified_column_references() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT orders.customer_id FROM orders");
        
        assert!(result.is_ok());
        let query = result.unwrap();
        
        match query {
            StreamingQuery::Select { fields, .. } => {
                assert_eq!(fields.len(), 1);
                
                if let SelectField::Expression { expr, .. } = &fields[0] {
                    assert!(matches!(expr, Expr::Column(_)));
                    if let Expr::Column(name) = expr {
                        assert_eq!(name, "orders.customer_id");
                    }
                }
            }
            _ => panic!("Expected Select query"),
        }
    }

    #[test]
    fn test_duration_parsing() {
        let parser = StreamingSqlParser::new();
        
        // Test seconds
        let result = parser.parse("SELECT * FROM orders WINDOW TUMBLING(30s)");
        assert!(result.is_ok());
        
        // Test minutes  
        let result = parser.parse("SELECT * FROM orders WINDOW TUMBLING(5m)");
        assert!(result.is_ok());
        
        // Test hours
        let result = parser.parse("SELECT * FROM orders WINDOW TUMBLING(2h)");
        assert!(result.is_ok());
        
        // Test plain number (defaults to seconds)
        let result = parser.parse("SELECT * FROM orders WINDOW TUMBLING(60)");
        assert!(result.is_ok());
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let parser = StreamingSqlParser::new();
        
        let queries = vec![
            "select * from orders",
            "SELECT * FROM orders", 
            "Select * From orders",
            "sElEcT * fRoM orders",
        ];
        
        for query in queries {
            let result = parser.parse(query);
            assert!(result.is_ok(), "Failed to parse: {}", query);
        }
    }

    #[test]
    fn test_whitespace_handling() {
        let parser = StreamingSqlParser::new();
        
        let queries = vec![
            "SELECT * FROM orders",
            "SELECT  *  FROM  orders",
            "  SELECT * FROM orders  ",
            "\tSELECT\t*\tFROM\torders\t",
            "\nSELECT\n*\nFROM\norders\n",
        ];
        
        for query in queries {
            let result = parser.parse(query);
            assert!(result.is_ok(), "Failed to parse: {}", query);
        }
    }
}