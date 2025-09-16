// Simple test binary for UNION operator parsing
use velostream::velostream::sql::{ast::StreamingQuery, parser::StreamingSqlParser};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing UNION operator parsing...");

    let parser = StreamingSqlParser::new();

    // Test 1: Basic UNION parsing
    println!("\n1. Testing basic UNION parsing:");
    let sql1 = "SELECT name FROM customers UNION SELECT name FROM suppliers";
    match parser.parse(sql1) {
        Ok(StreamingQuery::Union { left, right, all }) => {
            println!("✓ Successfully parsed UNION query");
            println!("  - All flag: {} (should be false for UNION)", all);
            match left.as_ref() {
                StreamingQuery::Select { .. } => println!("  - Left side: SELECT query"),
                _ => println!("  - Left side: Other query type"),
            }
            match right.as_ref() {
                StreamingQuery::Select { .. } => println!("  - Right side: SELECT query"),
                _ => println!("  - Right side: Other query type"),
            }
        }
        Ok(_) => println!("✗ Parsed successfully but not as UNION query"),
        Err(e) => println!("✗ Failed to parse: {:?}", e),
    }

    // Test 2: UNION ALL parsing
    println!("\n2. Testing UNION ALL parsing:");
    let sql2 = "SELECT id FROM orders UNION ALL SELECT id FROM returns";
    match parser.parse(sql2) {
        Ok(StreamingQuery::Union { left, right, all }) => {
            println!("✓ Successfully parsed UNION ALL query");
            println!("  - All flag: {} (should be true for UNION ALL)", all);
        }
        Ok(_) => println!("✗ Parsed successfully but not as UNION query"),
        Err(e) => println!("✗ Failed to parse: {:?}", e),
    }

    // Test 3: Multiple UNION chaining (should be left-associative)
    println!("\n3. Testing chained UNION parsing:");
    let sql3 = "SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT c FROM t3";
    match parser.parse(sql3) {
        Ok(query) => {
            println!("✓ Successfully parsed chained UNION");
            match query {
                StreamingQuery::Union { left, .. } => match left.as_ref() {
                    StreamingQuery::Union { .. } => {
                        println!("  - Left side is also UNION (left-associative)")
                    }
                    _ => println!("  - Left side is not UNION"),
                },
                _ => println!("  - Not a UNION query"),
            }
        }
        Err(e) => println!("✗ Failed to parse chained UNION: {:?}", e),
    }

    // Test 4: Complex UNION with WHERE clauses
    println!("\n4. Testing UNION with WHERE clauses:");
    let sql4 = "SELECT name FROM customers WHERE age > 30 UNION ALL SELECT name FROM suppliers WHERE rating > 4";
    match parser.parse(sql4) {
        Ok(StreamingQuery::Union { all, .. }) => {
            println!("✓ Successfully parsed UNION with WHERE clauses");
            println!("  - All flag: {} (should be true for UNION ALL)", all);
        }
        Ok(_) => println!("✗ Parsed successfully but not as UNION query"),
        Err(e) => println!("✗ Failed to parse: {:?}", e),
    }

    // Test 5: Error cases
    println!("\n5. Testing error cases:");

    // Missing right side
    let sql_error1 = "SELECT name FROM customers UNION";
    match parser.parse(sql_error1) {
        Ok(_) => println!("✗ Should have failed: missing right side"),
        Err(_) => println!("✓ Correctly failed: missing right side"),
    }

    // Invalid ALL placement
    let sql_error2 = "SELECT name FROM customers ALL UNION SELECT name FROM suppliers";
    match parser.parse(sql_error2) {
        Ok(_) => println!("✗ Should have failed: invalid ALL placement"),
        Err(_) => println!("✓ Correctly failed: invalid ALL placement"),
    }

    println!("\n✅ UNION operator parsing tests completed!");

    // Show some examples of what UNION does conceptually
    println!("\n📝 UNION operator semantics:");
    println!("  - UNION combines rows from multiple SELECT queries");
    println!("  - UNION removes duplicate rows (like SQL DISTINCT)");
    println!("  - UNION ALL preserves all rows including duplicates");
    println!("  - Both sides must have compatible column types and counts");

    Ok(())
}
