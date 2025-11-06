//! Tests for aggregation state management

use std::collections::HashMap;
use velostream::velostream::sql::ast::Expr;
use velostream::velostream::sql::execution::aggregation::state::GroupByStateManager;
use velostream::velostream::sql::execution::internal::GroupKey;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

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
        event_time: None,
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

    // Phase 4B: generate_group_key now returns GroupKey instead of Vec<String>
    let result = GroupByStateManager::generate_group_key(&expressions, &record);
    assert!(result.is_ok());
    let key = result.unwrap();

    // Verify the key contains the expected values
    assert_eq!(key.values().len(), 2);
    assert_eq!(
        key.values()[0],
        FieldValue::String("electronics".to_string())
    );
    assert_eq!(key.values()[1], FieldValue::Integer(1));
}

#[test]
fn test_record_matches_group_key() {
    let record = create_test_record(vec![(
        "category",
        FieldValue::String("electronics".to_string()),
    )]);

    let expressions = vec![Expr::Column("category".to_string())];

    // Phase 4B: Create GroupKey instead of Vec<String>
    let target_key = GroupKey::new(vec![FieldValue::String("electronics".to_string())]);

    let result = GroupByStateManager::record_matches_group_key(&expressions, &record, &target_key);
    assert!(result.is_ok());
    assert!(result.unwrap());

    // Phase 4B: Create wrong key as GroupKey
    let wrong_key = GroupKey::new(vec![FieldValue::String("books".to_string())]);
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

    // Phase 4B: extract_group_keys now returns Vec<GroupKey> instead of Vec<Vec<String>>
    let result = GroupByStateManager::extract_group_keys(&expressions, &records);
    assert!(result.is_ok());
    let keys = result.unwrap();

    assert_eq!(keys.len(), 2);

    // Verify the keys contain the expected values
    let electronics_key = GroupKey::new(vec![FieldValue::String("electronics".to_string())]);
    let books_key = GroupKey::new(vec![FieldValue::String("books".to_string())]);

    assert!(keys.contains(&electronics_key));
    assert!(keys.contains(&books_key));
}
