#[cfg(test)]
mod debug_tests {
    use ferrisstreams::ferris::sql::parser::StreamingSqlParser;

    #[test]
    fn debug_simple_query() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT * FROM orders");
        
        match &result {
            Ok(query) => println!("SUCCESS: {:?}", query),
            Err(err) => println!("ERROR: {:?}", err),
        }
        
        assert!(result.is_ok());
    }

    #[test]
    fn debug_window_query() {
        let parser = StreamingSqlParser::new();
        let result = parser.parse("SELECT COUNT(*) FROM orders WINDOW TUMBLING(5m)");
        
        match &result {
            Ok(query) => println!("SUCCESS: {:?}", query),
            Err(err) => println!("ERROR: {:?}", err),
        }
        
        // Don't assert for now, just debug
    }
}