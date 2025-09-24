use velostream::velostream::sql::validator::SqlValidator;

fn main() {
    let validator = SqlValidator::new();

    let query = "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.name LIKE '%pattern')";
    println!("Testing query: {}", query);

    let result = validator.validate_query(query, 0, 1, query);

    println!("Generated {} warnings:", result.warnings.len());
    for (i, warning) in result.warnings.iter().enumerate() {
        println!("  {}: {}", i + 1, warning.message);
    }

    println!();
    println!("Looking for pattern: 'LIKE patterns starting with %'");
    let performance_warning = result
        .warnings
        .iter()
        .find(|w| w.message.contains("LIKE patterns starting with %"));

    if performance_warning.is_some() {
        println!("✅ Found LIKE performance warning!");
    } else {
        println!("❌ LIKE performance warning NOT found");
    }
}