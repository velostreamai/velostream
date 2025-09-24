use velostream::velostream::sql::parser::StreamingSqlParser;

fn main() {
    let parser = StreamingSqlParser::new();

    let query = "SELECT 1 FROM t2 WHERE t2.name LIKE '%pattern'";
    println!("Testing LIKE query: {}", query);

    match parser.parse(query) {
        Ok(parsed_query) => {
            println!("Parsed successfully: {:#?}", parsed_query);
        }
        Err(e) => {
            println!("Parse error: {}", e);
        }
    }
}