//! Tests for aggregation state management

use ferrisstreams::ferris::sql::ast::Expr;
use ferrisstreams::ferris::sql::execution::aggregation::GroupByStateManager;
use ferrisstreams::ferris::sql::execution::types::{FieldValue, StreamRecord};
use std::collections::HashMap;

fn create_test_record(fields: Vec<(&str, FieldValue)>) -> StreamRecord {
    let mut field_map = HashMap::new();
    for (key, value) in fields {
        field_map.insert(key.to_string(), value);
    }

    StreamRecord {
        fields: field_map,
        timestamp: 0,
        offset: 0,
        partition: 0,
        headers: HashMap::new(),
    }
}

#[test]
fn test_field_value_to_group_key() {
    assert_eq!(
        GroupByStateManager::field_value_to_group_key(&FieldValue::String("test".to_string())),
        "test"
    );
    assert_eq!(
        GroupByStateManager::field_value_to_group_key(&FieldValue::Integer(42)),
        "42"
    );
    assert_eq!(
        GroupByStateManager::field_value_to_group_key(&FieldValue::Float(std::f64::consts::PI)),
        std::f64::consts::PI.to_string()
    );
    assert_eq!(
        GroupByStateManager::field_value_to_group_key(&FieldValue::Boolean(true)),
        "true"
    );
    assert_eq!(
        GroupByStateManager::field_value_to_group_key(&FieldValue::Null),
        "NULL"
    );
}

#[test]
fn test_generate_group_key_simple() {
    let record = create_test_record(vec![
        ("category", FieldValue::String("electronics".to_string())),
        ("priority", FieldValue::Integer(1)),
    ]);

    let expressions = vec![
        Expr::Column("category".to_string()),
        Expr::Column("priority".to_string()),
    ];

    let result = GroupByStateManager::generate_group_key(&expressions, &record);
    assert!(result.is_ok());
    let key = result.unwrap();
    assert_eq!(key, vec!["electronics".to_string(), "1".to_string()]);
}

#[test]
fn test_record_matches_group_key() {
    let record = create_test_record(vec![(
        "category",
        FieldValue::String("electronics".to_string()),
    )]);

    let expressions = vec![Expr::Column("category".to_string())];
    let target_key = vec!["electronics".to_string()];

    let result = GroupByStateManager::record_matches_group_key(&expressions, &record, &target_key);
    assert!(result.is_ok());
    assert!(result.unwrap());

    let wrong_key = vec!["books".to_string()];
    let result2 = GroupByStateManager::record_matches_group_key(&expressions, &record, &wrong_key);
    assert!(result2.is_ok());
    assert!(!result2.unwrap());
}

#[test]
fn test_extract_group_keys() {
    let records = vec![
        create_test_record(vec![(
            "category",
            FieldValue::String("electronics".to_string()),
        )]),
        create_test_record(vec![("category", FieldValue::String("books".to_string()))]),
        create_test_record(vec![(
            "category",
            FieldValue::String("electronics".to_string()),
        )]),
    ];

    let expressions = vec![Expr::Column("category".to_string())];

    let result = GroupByStateManager::extract_group_keys(&expressions, &records);
    assert!(result.is_ok());
    let keys = result.unwrap();

    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&vec!["electronics".to_string()]));
    assert!(keys.contains(&vec!["books".to_string()]));
}
