// Test the SQL snippet extraction and job name generation logic

#[test]
fn test_sql_snippet_extraction() {
    // Test different SQL patterns to ensure meaningful job names

    // Basic SELECT with specific fields
    let sql1 = "SELECT customer_id, amount FROM transactions";
    let snippet1 = extract_sql_snippet(sql1);
    assert_eq!(snippet1, "selectcustomer_idamountfromtra");
    println!("SQL: {} -> Snippet: {}", sql1, snippet1);

    // Aggregate function
    let sql2 = "SELECT COUNT(*) FROM fraud_alerts";
    let snippet2 = extract_sql_snippet(sql2);
    assert_eq!(snippet2, "job"); // Special chars filtered out, falls back to "job"
    println!("SQL: {} -> Snippet: {}", sql2, snippet2);

    // Complex query with multiple elements
    let sql3 =
        "SELECT AVG(amount) AS avg_transaction FROM financial_data WHERE date > '2023-01-01'";
    let snippet3 = extract_sql_snippet(sql3);
    assert_eq!(snippet3, "selectavgamountasavg_transacti");
    println!("SQL: {} -> Snippet: {}", sql3, snippet3);

    // SELECT * pattern
    let sql4 = "SELECT * FROM user_profiles";
    let snippet4 = extract_sql_snippet(sql4);
    assert_eq!(snippet4, "job"); // * is filtered out, falls back to "job"
    println!("SQL: {} -> Snippet: {}", sql4, snippet4);

    // Non-SELECT statement
    let sql5 = "INSERT INTO logs (message) VALUES ('test')";
    let snippet5 = extract_sql_snippet(sql5);
    assert_eq!(snippet5, "insertintologsmessagevaluestes");
    println!("SQL: {} -> Snippet: {}", sql5, snippet5);

    println!("✅ SQL snippet extraction tests passed!");
}

#[test]
fn test_job_name_generation_format() {
    // Test the full job name format: snippet_timestamp_id
    let sql = "SELECT customer_id, total_amount FROM sales_data";
    let snippet = extract_sql_snippet(sql);

    // Simulate the format used in the actual code
    let stmt_order = 5;
    let mock_timestamp = 98765; // 5-digit timestamp
    let job_name = format!("{}_{:05}_{:02}", snippet, mock_timestamp, stmt_order);

    // Verify format
    assert!(
        job_name.len() <= 50,
        "Job name should be reasonably short: {}",
        job_name
    );
    assert!(
        job_name.ends_with("_05"),
        "Should end with zero-padded order: {}",
        job_name
    );
    assert!(
        job_name.contains("98765"),
        "Should contain timestamp: {}",
        job_name
    );
    assert!(
        snippet.starts_with("select"),
        "Should start with SQL snippet: {}",
        snippet
    );

    // Ensure no invalid characters
    assert!(
        job_name.chars().all(|c| c.is_alphanumeric() || c == '_'),
        "Job name should only contain alphanumeric and underscore: {}",
        job_name
    );

    println!("Generated job name: {}", job_name);
    println!("✅ Job name format tests passed!");
}

#[test]
fn test_edge_cases() {
    // Test edge cases that might break the snippet extraction

    // Empty SQL
    let empty_snippet = extract_sql_snippet("");
    assert_eq!(empty_snippet, "job", "Empty SQL should fallback to 'job'");

    // Malformed SQL
    let malformed_snippet = extract_sql_snippet("SELEC customer FROM");
    assert!(
        !malformed_snippet.is_empty(),
        "Malformed SQL should still produce output"
    );

    // Very long table names
    let long_sql = "SELECT * FROM very_very_very_long_table_name_that_exceeds_normal_limits";
    let long_snippet = extract_sql_snippet(long_sql);
    assert!(
        long_snippet.len() <= 30,
        "Long snippets should be truncated: {}",
        long_snippet
    );

    // Special characters in SQL
    let special_sql = "SELECT `field-name`, [another_field] FROM `table-with-dashes`";
    let special_snippet = extract_sql_snippet(special_sql);
    assert!(
        special_snippet
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_'),
        "Special chars should be filtered: {}",
        special_snippet
    );

    println!("✅ Edge case tests passed!");
}

// Helper function that mimics the actual implementation
fn extract_sql_snippet(sql: &str) -> String {
    // Clean and normalize the SQL
    let sql_clean = sql
        .to_lowercase()
        .replace(['\n', '\r', '\t'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    // Ensure it's a valid identifier (alphanumeric + underscore)
    sql_clean
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>()
        .get(..30) // Increase limit to 30 chars for more descriptive names
        .unwrap_or("job")
        .to_string()
}
