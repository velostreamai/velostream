/*!
# Test Data Sources

This module provides utility functions to create test data sources for subquery execution
in unit tests and integration tests. This replaces the previous approach of having the
engine create test data directly.
*/

use crate::ferris::sql::execution::{FieldValue, StreamRecord};
use std::collections::HashMap;

/// Create comprehensive test data sources for subquery testing
///
/// This creates the standard set of tables that are expected by most subquery tests:
/// - `config`: Configuration records with active/inactive status
/// - `active_configs`: Subset of active configurations  
/// - `products`: Sample product catalog data
pub fn create_test_data_sources() -> HashMap<String, Vec<StreamRecord>> {
    let mut data_sources = HashMap::new();

    // Create 'config' table data - used by many subquery tests
    let config_records = create_config_table_data();
    data_sources.insert("config".to_string(), config_records);

    // Create 'active_configs' table data
    let active_configs = create_active_configs_table_data();
    data_sources.insert("active_configs".to_string(), active_configs);

    // Create 'products' table data for additional test scenarios
    let products_records = create_products_table_data();
    data_sources.insert("products".to_string(), products_records);

    // Create 'users' table data for JOIN testing
    let users_records = create_users_table_data();
    data_sources.insert("users".to_string(), users_records);

    // Create 'blocks' table data for NOT EXISTS testing (intentionally empty)
    let blocks_records = create_blocks_table_data();
    data_sources.insert("blocks".to_string(), blocks_records);

    data_sources
}

/// Create config table test data
fn create_config_table_data() -> Vec<StreamRecord> {
    let mut config_records = Vec::new();

    // Config record 1: active configuration
    let mut config_fields_1 = HashMap::new();
    config_fields_1.insert("id".to_string(), FieldValue::Integer(1));
    config_fields_1.insert("active".to_string(), FieldValue::Boolean(true));
    config_fields_1.insert("enabled".to_string(), FieldValue::Boolean(true));
    config_fields_1.insert("valid_id".to_string(), FieldValue::Integer(42));
    config_fields_1.insert(
        "valid_name".to_string(),
        FieldValue::String("test_record".to_string()),
    );
    config_fields_1.insert(
        "config_name".to_string(),
        FieldValue::String("production".to_string()),
    );
    config_fields_1.insert("max_value".to_string(), FieldValue::Integer(100));

    config_records.push(StreamRecord {
        fields: config_fields_1,
        headers: HashMap::new(),
        timestamp: 1640995200000,
        offset: 1,
        partition: 0,
    });

    // Config record 2: inactive configuration
    let mut config_fields_2 = HashMap::new();
    config_fields_2.insert("id".to_string(), FieldValue::Integer(2));
    config_fields_2.insert("active".to_string(), FieldValue::Boolean(false));
    config_fields_2.insert("enabled".to_string(), FieldValue::Boolean(false));
    config_fields_2.insert("blocked_id".to_string(), FieldValue::Integer(999));
    config_fields_2.insert(
        "config_name".to_string(),
        FieldValue::String("staging".to_string()),
    );
    config_fields_2.insert("max_value".to_string(), FieldValue::Integer(200));

    config_records.push(StreamRecord {
        fields: config_fields_2,
        headers: HashMap::new(),
        timestamp: 1640995200001,
        offset: 2,
        partition: 0,
    });

    config_records
}

/// Create active_configs table test data
fn create_active_configs_table_data() -> Vec<StreamRecord> {
    let mut active_config_fields = HashMap::new();
    active_config_fields.insert(
        "config_name".to_string(),
        FieldValue::String("production".to_string()),
    );
    active_config_fields.insert(
        "status".to_string(),
        FieldValue::String("active".to_string()),
    );

    vec![StreamRecord {
        fields: active_config_fields,
        headers: HashMap::new(),
        timestamp: 1640995200000,
        offset: 1,
        partition: 0,
    }]
}

/// Create products table test data
fn create_products_table_data() -> Vec<StreamRecord> {
    let mut products_records = Vec::new();

    // Product 1: Laptop
    let mut product_fields_1 = HashMap::new();
    product_fields_1.insert("id".to_string(), FieldValue::Integer(1));
    product_fields_1.insert("name".to_string(), FieldValue::String("laptop".to_string()));
    product_fields_1.insert("price".to_string(), FieldValue::Float(999.99));
    product_fields_1.insert(
        "category".to_string(),
        FieldValue::String("electronics".to_string()),
    );

    products_records.push(StreamRecord {
        fields: product_fields_1,
        headers: HashMap::new(),
        timestamp: 1640995200000,
        offset: 1,
        partition: 0,
    });

    // Product 2: Phone
    let mut product_fields_2 = HashMap::new();
    product_fields_2.insert("id".to_string(), FieldValue::Integer(2));
    product_fields_2.insert("name".to_string(), FieldValue::String("phone".to_string()));
    product_fields_2.insert("price".to_string(), FieldValue::Float(599.99));
    product_fields_2.insert(
        "category".to_string(),
        FieldValue::String("electronics".to_string()),
    );

    products_records.push(StreamRecord {
        fields: product_fields_2,
        headers: HashMap::new(),
        timestamp: 1640995200001,
        offset: 2,
        partition: 0,
    });

    products_records
}

/// Create users table test data for JOIN operations
fn create_users_table_data() -> Vec<StreamRecord> {
    let mut users_records = Vec::new();

    // User 1
    let mut user_fields_1 = HashMap::new();
    user_fields_1.insert("id".to_string(), FieldValue::Integer(100));
    user_fields_1.insert("user_id".to_string(), FieldValue::Integer(100)); // For compatibility
    user_fields_1.insert(
        "name".to_string(),
        FieldValue::String("Test User".to_string()),
    );
    user_fields_1.insert(
        "email".to_string(),
        FieldValue::String("user@example.com".to_string()),
    );
    user_fields_1.insert(
        "status".to_string(),
        FieldValue::String("active".to_string()),
    );

    users_records.push(StreamRecord {
        fields: user_fields_1,
        headers: HashMap::new(),
        timestamp: 1640995200000,
        offset: 1,
        partition: 0,
    });

    // User 2
    let mut user_fields_2 = HashMap::new();
    user_fields_2.insert("id".to_string(), FieldValue::Integer(101));
    user_fields_2.insert("user_id".to_string(), FieldValue::Integer(101)); // For compatibility
    user_fields_2.insert(
        "name".to_string(),
        FieldValue::String("Another User".to_string()),
    );
    user_fields_2.insert(
        "email".to_string(),
        FieldValue::String("user2@example.com".to_string()),
    );
    user_fields_2.insert(
        "status".to_string(),
        FieldValue::String("active".to_string()),
    );

    users_records.push(StreamRecord {
        fields: user_fields_2,
        headers: HashMap::new(),
        timestamp: 1640995200001,
        offset: 2,
        partition: 0,
    });

    users_records
}

/// Create blocks table test data for NOT EXISTS testing
/// This table is intentionally empty to make NOT EXISTS return true
fn create_blocks_table_data() -> Vec<StreamRecord> {
    // Return empty vector - this will make NOT EXISTS (SELECT ... FROM blocks) return true
    // because there are no records that match the condition
    Vec::new()
}

/// Create a minimal data source set for basic testing
pub fn create_minimal_test_data_sources() -> HashMap<String, Vec<StreamRecord>> {
    let mut data_sources = HashMap::new();

    // Just the config table for basic subquery tests
    let config_records = create_config_table_data();
    data_sources.insert("config".to_string(), config_records);

    data_sources
}

/// Create custom data source for specific test scenarios
///
/// # Arguments
/// * `table_name` - Name of the table/stream
/// * `records` - Vector of StreamRecord data
///
/// # Returns
/// HashMap with single data source entry
pub fn create_custom_data_source(
    table_name: &str,
    records: Vec<StreamRecord>,
) -> HashMap<String, Vec<StreamRecord>> {
    let mut data_sources = HashMap::new();
    data_sources.insert(table_name.to_string(), records);
    data_sources
}
