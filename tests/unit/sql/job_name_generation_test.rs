// Test the SQL snippet extraction and job name generation logic

#[test]
fn test_sql_snippet_extraction() {
    // Test different SQL patterns to ensure meaningful job names

    // Basic SELECT with specific fields
    let sql1 = "SELECT customer_id, amount FROM transactions";
    let snippet1 = extract_sql_snippet(sql1);
    assert!(snippet1.starts_with("sel_"));
    assert!(snippet1.contains("customer"));
    assert!(snippet1.contains("transaction"));
    println!("SQL: {} -> Snippet: {}", sql1, snippet1);

    // Aggregate function
    let sql2 = "SELECT COUNT(*) FROM fraud_alerts";
    let snippet2 = extract_sql_snippet(sql2);
    assert!(snippet2.starts_with("sel_"));
    assert!(snippet2.contains("count"));
    assert!(snippet2.contains("fraud_alert"));
    println!("SQL: {} -> Snippet: {}", sql2, snippet2);

    // Complex query with multiple elements
    let sql3 =
        "SELECT AVG(amount) AS avg_transaction FROM financial_data WHERE date > '2023-01-01'";
    let snippet3 = extract_sql_snippet(sql3);
    assert!(snippet3.starts_with("sel_"));
    assert!(snippet3.contains("avg"));
    println!("SQL: {} -> Snippet: {}", sql3, snippet3);

    // SELECT * pattern
    let sql4 = "SELECT * FROM user_profiles";
    let snippet4 = extract_sql_snippet(sql4);
    assert!(snippet4.starts_with("sel_"));
    assert!(snippet4.contains("all"));
    assert!(snippet4.contains("user_profile"));
    println!("SQL: {} -> Snippet: {}", sql4, snippet4);

    // Non-SELECT statement
    let sql5 = "INSERT INTO logs (message) VALUES ('test')";
    let snippet5 = extract_sql_snippet(sql5);
    assert!(snippet5.starts_with("insert"));
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
        job_name.len() <= 35,
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
        job_name.starts_with("sel_"),
        "Should start with SQL snippet: {}",
        job_name
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
        long_snippet.len() <= 20,
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

    // Extract key components
    let mut parts = Vec::new();

    // Extract query type (SELECT, INSERT, etc.)
    if sql_clean.starts_with("select") {
        parts.push("sel");

        // Try to extract meaningful fields or functions
        if let Some(select_part) = sql_clean.strip_prefix("select ") {
            if let Some(from_pos) = select_part.find(" from ") {
                let select_fields = &select_part[..from_pos];

                // Look for aggregate functions
                if select_fields.contains("count(") {
                    parts.push("count");
                } else if select_fields.contains("sum(") {
                    parts.push("sum");
                } else if select_fields.contains("avg(") {
                    parts.push("avg");
                } else if select_fields.contains("max(") {
                    parts.push("max");
                } else if select_fields.contains("min(") {
                    parts.push("min");
                } else if select_fields.contains("*") {
                    parts.push("all");
                } else {
                    // Extract first significant field name
                    let first_field = select_fields
                        .split(',')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .split_whitespace()
                        .next()
                        .unwrap_or("data");
                    parts.push(&first_field[..first_field.len().min(8)]);
                }
            }

            // Extract table name
            if let Some(from_part) =
                select_part.strip_prefix(&select_part[..select_part.find(" from ").unwrap_or(0)])
            {
                if let Some(table_start) = from_part.strip_prefix(" from ") {
                    let table_name = table_start.split_whitespace().next().unwrap_or("table");
                    parts.push(&table_name[..table_name.len().min(12)]);
                }
            }
        }
    } else {
        // For non-SELECT statements, use first word + generic identifier
        let first_word = sql_clean.split_whitespace().next().unwrap_or("query");
        parts.push(&first_word[..first_word.len().min(6)]);
        parts.push("stmt");
    }

    // Join parts with underscores, ensuring valid identifier
    let result = parts.join("_");

    // Ensure it's a valid identifier (alphanumeric + underscore)
    result
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>()
        .get(..20) // Limit to 20 chars
        .unwrap_or("job")
        .to_string()
}
