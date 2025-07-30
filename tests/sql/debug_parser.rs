use ferrisstreams::ferris::sql::parser::StreamingSqlParser;

fn main() {
    let parser = StreamingSqlParser::new();
    let result = parser.parse("SELECT COUNT(*) FROM orders WINDOW TUMBLING(5m)");
    
    match result {
        Ok(query) => println!("SUCCESS: {:?}", query),
        Err(err) => println!("ERROR: {:?}", err),
    }
}