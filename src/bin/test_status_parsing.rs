use velostream::velostream::sql::parser::StreamingSqlParser;

fn main() {
    let parser = StreamingSqlParser::new();

    println!("🧪 Testing STATUS keyword parsing fixes...\n");

    // Test 1: status as field name (should work now)
    println!("1️⃣ Testing 'status' as field name...");
    let sql_with_status_field = "SELECT id, name, status FROM users WHERE status = 'active'";
    match parser.parse(sql_with_status_field) {
        Ok(_) => println!("✅ 'status' as field name: PASSED"),
        Err(e) => println!("❌ 'status' as field name: FAILED - {}", e),
    }

    // Test 2: PARTITION BY status (the original issue)
    println!("\n2️⃣ Testing 'PARTITION BY status'...");
    let sql_with_partition = "SELECT status, COUNT(*) OVER (PARTITION BY status) as count FROM users";
    match parser.parse(sql_with_partition) {
        Ok(_) => println!("✅ 'PARTITION BY status': PASSED"),
        Err(e) => println!("❌ 'PARTITION BY status': FAILED - {}", e),
    }

    // Test 3: SHOW STATUS command (should still work)
    println!("\n3️⃣ Testing 'SHOW STATUS' command...");
    let sql_show_status = "SHOW STATUS";
    match parser.parse(sql_show_status) {
        Ok(_) => println!("✅ 'SHOW STATUS': PASSED"),
        Err(e) => println!("❌ 'SHOW STATUS': FAILED - {}", e),
    }

    // Test 4: SHOW STATUS job_name command (should still work)
    println!("\n4️⃣ Testing 'SHOW STATUS job_name' command...");
    let sql_show_status_job = "SHOW STATUS my_job";
    match parser.parse(sql_show_status_job) {
        Ok(_) => println!("✅ 'SHOW STATUS job_name': PASSED"),
        Err(e) => println!("❌ 'SHOW STATUS job_name': FAILED - {}", e),
    }

    // Test 5: Complex query with status field (original failing case)
    println!("\n5️⃣ Testing original failing case...");
    let sql_complex = r#"
        SELECT
            id,
            name,
            status,
            created_at,
            COUNT(*) OVER (PARTITION BY status) as status_count
        FROM integration_source
        WHERE status IN ('active', 'pending')
    "#;
    match parser.parse(sql_complex) {
        Ok(_) => println!("✅ Complex query with status: PASSED"),
        Err(e) => println!("❌ Complex query with status: FAILED - {}", e),
    }

    // Test 6: metrics as field name
    println!("\n6️⃣ Testing 'metrics' as field name...");
    let sql_metrics_field = "SELECT id, name, metrics FROM performance WHERE metrics > 100";
    match parser.parse(sql_metrics_field) {
        Ok(_) => println!("✅ 'metrics' as field name: PASSED"),
        Err(e) => println!("❌ 'metrics' as field name: FAILED - {}", e),
    }

    // Test 7: properties as field name
    println!("\n7️⃣ Testing 'properties' as field name...");
    let sql_properties_field = "SELECT id, name, properties FROM config WHERE properties IS NOT NULL";
    match parser.parse(sql_properties_field) {
        Ok(_) => println!("✅ 'properties' as field name: PASSED"),
        Err(e) => println!("❌ 'properties' as field name: FAILED - {}", e),
    }

    // Test 8: SHOW METRICS command (should still work)
    println!("\n8️⃣ Testing 'SHOW METRICS' command...");
    let sql_show_metrics = "SHOW METRICS";
    match parser.parse(sql_show_metrics) {
        Ok(_) => println!("✅ 'SHOW METRICS': PASSED"),
        Err(e) => println!("❌ 'SHOW METRICS': FAILED - {}", e),
    }

    // Test 9: SHOW PROPERTIES command (should still work)
    println!("\n9️⃣ Testing 'SHOW PROPERTIES' command...");
    let sql_show_properties = "SHOW PROPERTIES STREAM my_stream";
    match parser.parse(sql_show_properties) {
        Ok(_) => println!("✅ 'SHOW PROPERTIES': PASSED"),
        Err(e) => println!("❌ 'SHOW PROPERTIES': FAILED - {}", e),
    }

    // Test 10: Complex query with all fixed fields
    println!("\n🔟 Testing complex query with all fixed fields...");
    let sql_all_fields = r#"
        SELECT
            id,
            name,
            status,
            metrics,
            properties,
            COUNT(*) OVER (PARTITION BY status) as status_count,
            AVG(metrics) OVER (PARTITION BY properties) as avg_metrics
        FROM data_source
        WHERE status = 'active' AND metrics > 0 AND properties IS NOT NULL
    "#;
    match parser.parse(sql_all_fields) {
        Ok(_) => println!("✅ Complex query with all fixed fields: PASSED"),
        Err(e) => println!("❌ Complex query with all fixed fields: FAILED - {}", e),
    }

    println!("\n🎉 All tests completed!");
}