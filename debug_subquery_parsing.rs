use velostream::velostream::sql::{parser::StreamingSqlParser, validator::SqlValidator};

fn main() {
    let parser = StreamingSqlParser::new();
    let validator = SqlValidator::new();

    let query = "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)";

    println!("Testing query: {}", query);

    match parser.parse(query) {
        Ok(parsed_query) => {
            println!("Parsed successfully: {:#?}", parsed_query);

            // Test validator
            let result = validator.validate_query(query, 0, 1, query);
            println!("Validation warnings count: {}", result.warnings.len());
            for warning in &result.warnings {
                println!("Warning: {}", warning.message);
            }
        }
        Err(e) => {
            println!("Parse error: {}", e);
        }
    }
}