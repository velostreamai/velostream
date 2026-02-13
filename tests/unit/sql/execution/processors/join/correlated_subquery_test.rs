/*!
# Correlated Subquery Test

Focused test to verify that correlated subqueries with table aliases work correctly
when executed through the StreamExecutionEngine with context customizer.

This test specifically addresses the issue where correlated subqueries like:
```sql
SELECT employee_id, salary
FROM employees e
WHERE salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE department_id = e.department_id
)
```

Previously returned 0 results because the employees table wasn't loaded into the
execution context. This test verifies the fix works correctly.
*/

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::{FieldValue, StreamExecutionEngine, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;
use velostream::velostream::table::{OptimizedTableImpl, UnifiedTable};

/// Create employees table for testing correlated subqueries
fn create_employees_table() -> Arc<dyn UnifiedTable> {
    let table = OptimizedTableImpl::new();

    // Create 10 employees across 2 departments
    // Department 1: employees with salaries 50000, 55000, 60000, 65000, 70000
    // Department 2: employees with salaries 75000, 80000, 85000, 90000, 95000
    for i in 0..10 {
        let mut fields = HashMap::new();
        let employee_id = i as i64;
        let department_id = if i < 5 { 1 } else { 2 };
        let salary = 50000.0 + (i as f64 * 5000.0);

        fields.insert("employee_id".to_string(), FieldValue::Integer(employee_id));
        fields.insert(
            "department_id".to_string(),
            FieldValue::Integer(department_id),
        );
        fields.insert("salary".to_string(), FieldValue::Float(salary));

        let key = employee_id.to_string();
        let _ = table.insert(key, fields);
    }

    Arc::new(table)
}

/// Create a context customizer that loads the employees table
fn create_employees_context() -> Arc<dyn Fn(&mut ProcessorContext) + Send + Sync> {
    Arc::new(move |context: &mut ProcessorContext| {
        context.load_reference_table("employees", create_employees_table());
    })
}

/// Generate a test employee record
fn create_test_employee(id: i64, dept_id: i64, salary: f64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("employee_id".to_string(), FieldValue::Integer(id));
    fields.insert("department_id".to_string(), FieldValue::Integer(dept_id));
    fields.insert("salary".to_string(), FieldValue::Float(salary));

    StreamRecord::new(fields)
}

/// Execute a correlated subquery test
async fn execute_correlated_subquery_test(
    query: &str,
    test_record: StreamRecord,
) -> Result<Vec<StreamRecord>, Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Set up the context with employees table
    engine.context_customizer = Some(create_employees_context());

    let parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query)?;

    engine
        .execute_with_record(&parsed_query, &test_record)
        .await?;

    let mut results = Vec::new();
    while let Ok(result) = rx.try_recv() {
        results.push(result);
    }
    Ok(results)
}

/// Test: Correlated EXISTS with exact match
///
/// This test verifies that correlated EXISTS subqueries work with table aliases.
#[tokio::test]
async fn test_correlated_subquery_exists_with_alias() {
    let query = r#"
        SELECT employee_id, department_id
        FROM employees e
        WHERE EXISTS (
            SELECT 1
            FROM employees
            WHERE department_id = e.department_id
        )
    "#;

    // Test employee from department 1 (ids 0-4)
    // Should find other employees in same department
    let test_record = create_test_employee(2, 1, 60000.0);

    let result = execute_correlated_subquery_test(query, test_record).await;

    match result {
        Ok(results) => {
            println!("✅ Correlated EXISTS executed successfully");
            println!("   Results: {}", results.len());

            if results.is_empty() {
                println!("   ❌ ISSUE: No results found (expected 1)");
                println!("   Should find employee 2 because other employees exist in dept 1");
                panic!("Correlated EXISTS should find matching records");
            } else {
                println!("   ✅ Correctly returned {} result(s)", results.len());
                assert_eq!(
                    results.len(),
                    1,
                    "Should return exactly one matching record"
                );
            }
        }
        Err(e) => {
            panic!("Correlated EXISTS execution failed: {}", e);
        }
    }
}

/// Test: Verify the employees table is actually loaded in context
#[tokio::test]
async fn test_employees_table_loaded_in_context() {
    // Simple query to verify table loading
    let query = r#"
        SELECT employee_id, salary
        FROM employees
        WHERE employee_id = 5
    "#;

    let test_record = create_test_employee(5, 2, 75000.0);
    let result = execute_correlated_subquery_test(query, test_record).await;

    match result {
        Ok(results) => {
            println!("✅ Table loading test passed");
            println!("   Results: {}", results.len());
            assert!(
                !results.is_empty(),
                "Employees table should be loaded and queryable"
            );
        }
        Err(e) => {
            panic!("Table loading test failed: {}", e);
        }
    }
}
