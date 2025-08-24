use ferrisstreams::ferris::sql::execution::types::FieldValue;

fn main() {
    println!("Testing Financial Precision Implementation");
    println!("==========================================");

    // Create financial values
    let price1 = FieldValue::from_financial_f64(123.4567, 4);
    let price2 = FieldValue::from_financial_f64(456.7890, 4);

    println!("Price 1: {}", price1.to_display_string());
    println!("Price 2: {}", price2.to_display_string());

    // Test arithmetic operations
    match price1.add(&price2) {
        Ok(sum) => println!("Sum: {}", sum.to_display_string()),
        Err(e) => println!("Error in addition: {:?}", e),
    }

    match price1.multiply(&FieldValue::Integer(100)) {
        Ok(product) => println!("Price1 * 100: {}", product.to_display_string()),
        Err(e) => println!("Error in multiplication: {:?}", e),
    }

    // Test conversion back to f64
    match price1.to_financial_f64() {
        Some(f) => println!("Price1 as f64: {}", f),
        None => println!("Could not convert to f64"),
    }

    // Test casting
    match price1.cast_to("FLOAT") {
        Ok(float_val) => println!("Price1 cast to float: {}", float_val.to_display_string()),
        Err(e) => println!("Error casting to float: {:?}", e),
    }

    println!("\nTesting precision vs f64:");

    // Demonstrate the precision difference
    let f64_val = 123.4567_f64;
    let scaled_val = FieldValue::from_financial_f64(123.4567, 4);

    println!("Original f64: {:.10}", f64_val);
    println!("ScaledInteger: {}", scaled_val.to_display_string());

    // Test that scaled integer preserves exact values
    let amount1 = FieldValue::from_financial_f64(10.01, 4);
    let amount2 = FieldValue::from_financial_f64(10.02, 4);
    let amount3 = FieldValue::from_financial_f64(20.03, 4);

    match amount1.add(&amount2) {
        Ok(sum) => {
            println!("$10.01 + $10.02 = ${}", sum.to_display_string());
            match sum == amount3 {
                true => println!("✓ Exact precision maintained!"),
                false => println!("✗ Precision lost"),
            }
        }
        Err(e) => println!("Error: {:?}", e),
    }
}
