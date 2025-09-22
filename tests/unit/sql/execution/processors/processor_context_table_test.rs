/*!
# ProcessorContext Table Management Tests

Tests for the Table management functionality added to ProcessorContext for subquery execution.
This validates the state_tables field and associated methods for loading and accessing reference tables.
*/

use std::collections::HashMap;
use std::sync::Arc;
use velostream::velostream::sql::error::SqlError;
use velostream::velostream::sql::execution::processors::context::ProcessorContext;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::sql::{SqlDataSource, SqlQueryable};

/// Mock Table data source for testing ProcessorContext table management
struct MockTableDataSource {
    data: HashMap<String, FieldValue>,
    name: String,
}

impl MockTableDataSource {
    fn new(name: &str) -> Self {
        let mut data = HashMap::new();

        // Add some test data
        if name == "users" {
            data.insert(
                "user1".to_string(),
                FieldValue::Struct({
                    let mut user = HashMap::new();
                    user.insert("id".to_string(), FieldValue::String("user1".to_string()));
                    user.insert("name".to_string(), FieldValue::String("Alice".to_string()));
                    user.insert("active".to_string(), FieldValue::Boolean(true));
                    user.insert("score".to_string(), FieldValue::Float(95.0));
                    user
                }),
            );

            data.insert(
                "user2".to_string(),
                FieldValue::Struct({
                    let mut user = HashMap::new();
                    user.insert("id".to_string(), FieldValue::String("user2".to_string()));
                    user.insert("name".to_string(), FieldValue::String("Bob".to_string()));
                    user.insert("active".to_string(), FieldValue::Boolean(false));
                    user.insert("score".to_string(), FieldValue::Float(78.5));
                    user
                }),
            );
        } else if name == "config" {
            data.insert(
                "setting1".to_string(),
                FieldValue::Struct({
                    let mut config = HashMap::new();
                    config.insert(
                        "key".to_string(),
                        FieldValue::String("max_connections".to_string()),
                    );
                    config.insert("value".to_string(), FieldValue::String("100".to_string()));
                    config.insert("enabled".to_string(), FieldValue::Boolean(true));
                    config
                }),
            );
        }

        Self {
            data,
            name: name.to_string(),
        }
    }
}

impl SqlDataSource for MockTableDataSource {
    fn get_all_records(&self) -> Result<HashMap<String, FieldValue>, SqlError> {
        Ok(self.data.clone())
    }

    fn get_record(&self, key: &str) -> Result<Option<FieldValue>, SqlError> {
        Ok(self.data.get(key).cloned())
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn record_count(&self) -> usize {
        self.data.len()
    }
}

// SqlQueryable is automatically implemented via blanket implementation

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processor_context_creation_with_empty_tables() {
        let context = ProcessorContext::new("test-query");

        // Should start with no tables
        assert_eq!(context.list_tables().len(), 0);
        assert!(!context.has_table("users"));
        assert!(!context.has_table("config"));
    }

    #[test]
    fn test_load_reference_table() {
        let mut context = ProcessorContext::new("test-query");
        let user_table = Arc::new(MockTableDataSource::new("users"));

        // Load the table
        context.load_reference_table("users", user_table);

        // Verify it's available
        assert!(context.has_table("users"));
        assert_eq!(context.list_tables().len(), 1);
        assert!(context.list_tables().contains(&"users".to_string()));
    }

    #[test]
    fn test_get_table_success() {
        let mut context = ProcessorContext::new("test-query");
        let user_table = Arc::new(MockTableDataSource::new("users"));

        context.load_reference_table("users", user_table);

        // Get the table
        let result = context.get_table("users");
        assert!(result.is_ok());

        let table = result.unwrap();

        // Test that it's functional by checking record count
        let records = table.sql_filter("true").unwrap();
        assert_eq!(records.len(), 2); // Should have user1 and user2
    }

    #[test]
    fn test_get_table_not_found() {
        let context = ProcessorContext::new("test-query");

        // Try to get a table that doesn't exist
        let result = context.get_table("nonexistent");
        assert!(result.is_err());

        if let Err(SqlError::ExecutionError { message, .. }) = result {
            assert!(message.contains("Reference table 'nonexistent' not found"));
        } else {
            panic!("Expected SqlError::ExecutionError");
        }
    }

    #[test]
    fn test_load_multiple_reference_tables() {
        let mut context = ProcessorContext::new("test-query");

        let user_table = Arc::new(MockTableDataSource::new("users"));
        let config_table = Arc::new(MockTableDataSource::new("config"));

        context.load_reference_table("users", user_table);
        context.load_reference_table("config", config_table);

        // Verify both tables are available
        assert!(context.has_table("users"));
        assert!(context.has_table("config"));
        assert_eq!(context.list_tables().len(), 2);

        // Test accessing both tables
        let users = context.get_table("users").unwrap();
        let config = context.get_table("config").unwrap();

        let user_records = users.sql_filter("true").unwrap();
        let config_records = config.sql_filter("true").unwrap();

        assert_eq!(user_records.len(), 2);
        assert_eq!(config_records.len(), 1);
    }

    #[test]
    fn test_load_reference_tables_batch() {
        let mut context = ProcessorContext::new("test-query");

        let mut tables: HashMap<String, Arc<dyn SqlQueryable + Send + Sync>> = HashMap::new();
        tables.insert(
            "users".to_string(),
            Arc::new(MockTableDataSource::new("users")),
        );
        tables.insert(
            "config".to_string(),
            Arc::new(MockTableDataSource::new("config")),
        );

        context.load_reference_tables(tables);

        // Verify both tables are loaded
        assert_eq!(context.list_tables().len(), 2);
        assert!(context.has_table("users"));
        assert!(context.has_table("config"));
    }

    #[test]
    fn test_remove_table() {
        let mut context = ProcessorContext::new("test-query");
        let user_table = Arc::new(MockTableDataSource::new("users"));

        context.load_reference_table("users", user_table);
        assert!(context.has_table("users"));

        // Remove the table
        let removed = context.remove_table("users");
        assert!(removed.is_some());
        assert!(!context.has_table("users"));
        assert_eq!(context.list_tables().len(), 0);

        // Try to remove a non-existent table
        let removed_again = context.remove_table("users");
        assert!(removed_again.is_none());
    }

    #[test]
    fn test_clear_tables() {
        let mut context = ProcessorContext::new("test-query");

        let user_table = Arc::new(MockTableDataSource::new("users"));
        let config_table = Arc::new(MockTableDataSource::new("config"));

        context.load_reference_table("users", user_table);
        context.load_reference_table("config", config_table);

        assert_eq!(context.list_tables().len(), 2);

        // Clear all tables
        context.clear_tables();

        assert_eq!(context.list_tables().len(), 0);
        assert!(!context.has_table("users"));
        assert!(!context.has_table("config"));
    }

    #[test]
    fn test_table_sql_operations_through_context() {
        let mut context = ProcessorContext::new("test-query");
        let user_table = Arc::new(MockTableDataSource::new("users"));

        context.load_reference_table("users", user_table);

        let table = context.get_table("users").unwrap();

        // Test EXISTS query
        let has_active_users = table.sql_exists("active = true").unwrap();
        assert!(has_active_users);

        let has_inactive_users = table.sql_exists("active = false").unwrap();
        assert!(has_inactive_users);

        // Test FILTER query
        let active_users = table.sql_filter("active = true").unwrap();
        assert_eq!(active_users.len(), 1);

        // Test column extraction
        let user_names = table.sql_column_values("name", "active = true").unwrap();
        assert_eq!(user_names.len(), 1);
        assert_eq!(user_names[0], FieldValue::String("Alice".to_string()));

        // Test scalar query
        let active_user_name = table.sql_scalar("name", "active = true").unwrap();
        assert_eq!(active_user_name, FieldValue::String("Alice".to_string()));
    }

    #[test]
    fn test_table_replacement() {
        let mut context = ProcessorContext::new("test-query");

        // Load initial table
        let user_table_v1 = Arc::new(MockTableDataSource::new("users"));
        context.load_reference_table("users", user_table_v1);

        let table_v1 = context.get_table("users").unwrap();
        let records_v1 = table_v1.sql_filter("true").unwrap();
        assert_eq!(records_v1.len(), 2);

        // Replace with a new table (same name)
        let user_table_v2 = Arc::new(MockTableDataSource::new("config")); // Different data
        context.load_reference_table("users", user_table_v2);

        // Should now access the new table
        let table_v2 = context.get_table("users").unwrap();
        let records_v2 = table_v2.sql_filter("true").unwrap();
        assert_eq!(records_v2.len(), 1); // Config table has 1 record

        // Verify we still have only one table named "users"
        assert_eq!(context.list_tables().len(), 1);
    }

    #[test]
    fn test_concurrent_table_access() {
        let mut context = ProcessorContext::new("test-query");
        let user_table = Arc::new(MockTableDataSource::new("users"));

        context.load_reference_table("users", user_table);

        // Get multiple references to the same table
        let table1 = context.get_table("users").unwrap();
        let table2 = context.get_table("users").unwrap();

        // Both should work independently
        let records1 = table1.sql_filter("active = true").unwrap();
        let records2 = table2.sql_filter("active = false").unwrap();

        assert_eq!(records1.len(), 1);
        assert_eq!(records2.len(), 1);
    }
}
