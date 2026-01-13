use std::collections::HashMap;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::unified_table::{
    CachedPredicate, TableResult, UnifiedTable, parse_key_lookup, parse_where_clause_cached,
};

#[test]
fn test_key_lookup_parsing() {
    assert_eq!(
        parse_key_lookup("key = 'test123'"),
        Some("test123".to_string())
    );

    assert_eq!(
        parse_key_lookup("_key = \"test456\""),
        Some("test456".to_string())
    );

    // Should not match complex queries
    assert_eq!(parse_key_lookup("key = 'test' AND field = 'value'"), None);

    // Should not match non-key fields
    assert_eq!(parse_key_lookup("name = 'test'"), None);
}

#[tokio::test]
async fn test_unified_table_interface() {
    // This test would validate the unified interface works correctly
    // Implementation would be added when concrete implementations exist
}

// ============================================================================
// Tests for IN operator parsing (parse_where_clause_cached)
// ============================================================================

#[test]
fn test_parse_in_operator_string_list() {
    let result = parse_where_clause_cached("tier IN ('gold', 'platinum')").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "tier");
            assert_eq!(values, vec!["gold", "platinum"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_integer_list() {
    let result = parse_where_clause_cached("status IN (1, 2, 3)").unwrap();
    match result {
        CachedPredicate::FieldInIntegerList { field, values } => {
            assert_eq!(field, "status");
            assert_eq!(values, vec![1, 2, 3]);
        }
        other => panic!("Expected FieldInIntegerList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_single_value() {
    let result = parse_where_clause_cached("category IN ('electronics')").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "category");
            assert_eq!(values, vec!["electronics"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_with_table_prefix() {
    // Should strip table prefix from field name
    let result = parse_where_clause_cached("t.tier IN ('gold', 'silver')").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "tier");
            assert_eq!(values, vec!["gold", "silver"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_case_insensitive() {
    // IN should be case-insensitive
    let result = parse_where_clause_cached("status in ('active', 'pending')").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "status");
            assert_eq!(values, vec!["active", "pending"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_double_quotes() {
    let result = parse_where_clause_cached("name IN (\"Alice\", \"Bob\")").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "name");
            assert_eq!(values, vec!["Alice", "Bob"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_with_spaces() {
    let result = parse_where_clause_cached("tier  IN  ( 'gold' , 'platinum' )").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "tier");
            assert_eq!(values, vec!["gold", "platinum"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_comma_in_quoted_value() {
    // Commas inside quotes should not split
    let result = parse_where_clause_cached("desc IN ('hello, world', 'foo')").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "desc");
            assert_eq!(values, vec!["hello, world", "foo"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

// ============================================================================
// Tests for AlwaysTrue/AlwaysFalse patterns
// ============================================================================

#[test]
fn test_parse_always_true_patterns() {
    // All these should return AlwaysTrue
    let patterns = ["true", "1=1", "1 = 1"];
    for pattern in patterns {
        let result = parse_where_clause_cached(pattern).unwrap();
        assert!(
            matches!(result, CachedPredicate::AlwaysTrue),
            "Expected AlwaysTrue for '{}', got {:?}",
            pattern,
            result
        );
    }
}

#[test]
fn test_parse_always_false_patterns() {
    // All these should return AlwaysFalse
    let patterns = ["false", "1=0", "1 = 0"];
    for pattern in patterns {
        let result = parse_where_clause_cached(pattern).unwrap();
        assert!(
            matches!(result, CachedPredicate::AlwaysFalse),
            "Expected AlwaysFalse for '{}', got {:?}",
            pattern,
            result
        );
    }
}

// ============================================================================
// Tests for CachedPredicate::evaluate with FieldInStringList
// ============================================================================

#[test]
fn test_field_in_string_list_evaluate_string_field() {
    let predicate = CachedPredicate::FieldInStringList {
        field: "tier".to_string(),
        values: vec!["gold".to_string(), "platinum".to_string()],
    };

    let mut record = HashMap::new();

    // Match: tier = "gold"
    record.insert("tier".to_string(), FieldValue::String("gold".to_string()));
    assert!(predicate.evaluate("key1", &record));

    // Match: tier = "platinum"
    record.insert(
        "tier".to_string(),
        FieldValue::String("platinum".to_string()),
    );
    assert!(predicate.evaluate("key2", &record));

    // No match: tier = "silver"
    record.insert("tier".to_string(), FieldValue::String("silver".to_string()));
    assert!(!predicate.evaluate("key3", &record));
}

#[test]
fn test_field_in_string_list_evaluate_integer_field() {
    // String list with numeric strings should match integer fields
    let predicate = CachedPredicate::FieldInStringList {
        field: "status".to_string(),
        values: vec!["1".to_string(), "2".to_string(), "3".to_string()],
    };

    let mut record = HashMap::new();

    // Match: status = 1 (integer)
    record.insert("status".to_string(), FieldValue::Integer(1));
    assert!(predicate.evaluate("key1", &record));

    // Match: status = 2 (integer)
    record.insert("status".to_string(), FieldValue::Integer(2));
    assert!(predicate.evaluate("key2", &record));

    // No match: status = 5 (integer)
    record.insert("status".to_string(), FieldValue::Integer(5));
    assert!(!predicate.evaluate("key3", &record));
}

#[test]
fn test_field_in_string_list_evaluate_float_field() {
    let predicate = CachedPredicate::FieldInStringList {
        field: "price".to_string(),
        values: vec!["10.5".to_string(), "20.5".to_string()],
    };

    let mut record = HashMap::new();

    // Match: price = 10.5 (float)
    record.insert("price".to_string(), FieldValue::Float(10.5));
    assert!(predicate.evaluate("key1", &record));

    // No match: price = 15.5 (float)
    record.insert("price".to_string(), FieldValue::Float(15.5));
    assert!(!predicate.evaluate("key2", &record));
}

#[test]
fn test_field_in_string_list_evaluate_missing_field() {
    let predicate = CachedPredicate::FieldInStringList {
        field: "tier".to_string(),
        values: vec!["gold".to_string()],
    };

    let record: HashMap<String, FieldValue> = HashMap::new();
    // Missing field should not match
    assert!(!predicate.evaluate("key1", &record));
}

#[test]
fn test_field_in_string_list_evaluate_null_field() {
    let predicate = CachedPredicate::FieldInStringList {
        field: "tier".to_string(),
        values: vec!["gold".to_string()],
    };

    let mut record = HashMap::new();
    record.insert("tier".to_string(), FieldValue::Null);
    // Null field should not match
    assert!(!predicate.evaluate("key1", &record));
}

// ============================================================================
// Tests for CachedPredicate::evaluate with FieldInIntegerList
// ============================================================================

#[test]
fn test_field_in_integer_list_evaluate_integer_field() {
    let predicate = CachedPredicate::FieldInIntegerList {
        field: "status".to_string(),
        values: vec![1, 2, 3],
    };

    let mut record = HashMap::new();

    // Match: status = 1
    record.insert("status".to_string(), FieldValue::Integer(1));
    assert!(predicate.evaluate("key1", &record));

    // Match: status = 3
    record.insert("status".to_string(), FieldValue::Integer(3));
    assert!(predicate.evaluate("key2", &record));

    // No match: status = 5
    record.insert("status".to_string(), FieldValue::Integer(5));
    assert!(!predicate.evaluate("key3", &record));
}

#[test]
fn test_field_in_integer_list_evaluate_string_field() {
    // Integer list should match string fields containing numeric values
    let predicate = CachedPredicate::FieldInIntegerList {
        field: "code".to_string(),
        values: vec![100, 200, 300],
    };

    let mut record = HashMap::new();

    // Match: code = "100" (string)
    record.insert("code".to_string(), FieldValue::String("100".to_string()));
    assert!(predicate.evaluate("key1", &record));

    // No match: code = "150" (string)
    record.insert("code".to_string(), FieldValue::String("150".to_string()));
    assert!(!predicate.evaluate("key2", &record));

    // No match: code = "abc" (non-numeric string)
    record.insert("code".to_string(), FieldValue::String("abc".to_string()));
    assert!(!predicate.evaluate("key3", &record));
}

#[test]
fn test_field_in_integer_list_evaluate_float_field() {
    // Float field should be truncated to integer for comparison
    let predicate = CachedPredicate::FieldInIntegerList {
        field: "id".to_string(),
        values: vec![1, 2, 3],
    };

    let mut record = HashMap::new();

    // Match: id = 1.9 -> truncates to 1
    record.insert("id".to_string(), FieldValue::Float(1.9));
    assert!(predicate.evaluate("key1", &record));

    // Match: id = 2.0
    record.insert("id".to_string(), FieldValue::Float(2.0));
    assert!(predicate.evaluate("key2", &record));

    // No match: id = 4.5 -> truncates to 4
    record.insert("id".to_string(), FieldValue::Float(4.5));
    assert!(!predicate.evaluate("key3", &record));
}

// ============================================================================
// Tests for edge cases and error handling
// ============================================================================

#[test]
fn test_parse_in_operator_empty_list() {
    // Empty IN list should fall back to AlwaysTrue (safe default)
    let result = parse_where_clause_cached("status IN ()").unwrap();
    assert!(
        matches!(result, CachedPredicate::AlwaysTrue),
        "Expected AlwaysTrue for empty IN list, got {:?}",
        result
    );
}

#[test]
fn test_parse_in_operator_negative_integers() {
    let result = parse_where_clause_cached("balance IN (-100, -50, 0, 50)").unwrap();
    match result {
        CachedPredicate::FieldInIntegerList { field, values } => {
            assert_eq!(field, "balance");
            assert_eq!(values, vec![-100, -50, 0, 50]);
        }
        other => panic!("Expected FieldInIntegerList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_empty_string_value() {
    // Empty string in list should be preserved
    let result = parse_where_clause_cached("status IN ('', 'active')").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "status");
            assert_eq!(values, vec!["", "active"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_unclosed_quote_handled() {
    // Unclosed quote - should gracefully handle (currently includes trailing chars)
    // This tests the current behavior - unclosed quotes result in values including the quote
    let result = parse_where_clause_cached("status IN ('gold, 'silver')").unwrap();
    // Current behavior: parses as single malformed value, falls back to string list
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "status");
            // Values will be malformed but parsing shouldn't panic
            assert!(!values.is_empty());
        }
        CachedPredicate::AlwaysTrue => {
            // Also acceptable - unrecognized pattern
        }
        other => panic!("Expected FieldInStringList or AlwaysTrue, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_not_in_falls_back() {
    // NOT IN is not supported in CachedPredicate - should fall back to AlwaysTrue
    // (handled by full SQL evaluator instead)
    let result = parse_where_clause_cached("status NOT IN ('inactive', 'deleted')").unwrap();
    assert!(
        matches!(result, CachedPredicate::AlwaysTrue),
        "Expected AlwaysTrue for NOT IN (unsupported), got {:?}",
        result
    );
}

#[test]
fn test_parse_in_operator_special_characters() {
    // Special SQL characters in values
    let result = parse_where_clause_cached("query IN ('SELECT * FROM', 'DROP TABLE')").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "query");
            assert_eq!(values, vec!["SELECT * FROM", "DROP TABLE"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_unicode_values() {
    let result = parse_where_clause_cached("name IN ('日本語', '中文', 'العربية')").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "name");
            assert_eq!(values, vec!["日本語", "中文", "العربية"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_in_operator_whitespace_only_values() {
    let result = parse_where_clause_cached("status IN ('  ', 'valid')").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "status");
            assert_eq!(values, vec!["  ", "valid"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_field_in_integer_list_empty_list() {
    // Edge case: empty list should match nothing
    let predicate = CachedPredicate::FieldInIntegerList {
        field: "id".to_string(),
        values: vec![],
    };

    let mut record = HashMap::new();
    record.insert("id".to_string(), FieldValue::Integer(1));
    assert!(!predicate.evaluate("key1", &record));
}

#[test]
fn test_field_in_string_list_empty_list() {
    let predicate = CachedPredicate::FieldInStringList {
        field: "status".to_string(),
        values: vec![],
    };

    let mut record = HashMap::new();
    record.insert("status".to_string(), FieldValue::String("active".to_string()));
    assert!(!predicate.evaluate("key1", &record));
}

// ============================================================================
// Tests for mixed scenarios
// ============================================================================

#[test]
fn test_parse_in_operator_mixed_values_treated_as_string() {
    // Mixed quoted and unquoted values where unquoted is not integer
    let result = parse_where_clause_cached("status IN ('active', pending)").unwrap();
    match result {
        CachedPredicate::FieldInStringList { field, values } => {
            assert_eq!(field, "status");
            assert_eq!(values, vec!["active", "pending"]);
        }
        other => panic!("Expected FieldInStringList, got {:?}", other),
    }
}

#[test]
fn test_parse_equality_still_works() {
    // Ensure IN operator parsing doesn't break simple equality
    let result = parse_where_clause_cached("status = 'active'").unwrap();
    match result {
        CachedPredicate::FieldEqualsString { field, value } => {
            assert_eq!(field, "status");
            assert_eq!(value, "active");
        }
        other => panic!("Expected FieldEqualsString, got {:?}", other),
    }
}

#[test]
fn test_parse_numeric_comparison_still_works() {
    // Ensure IN operator parsing doesn't break numeric comparisons
    let result = parse_where_clause_cached("amount > 100").unwrap();
    match result {
        CachedPredicate::FieldGreaterThan { field, value } => {
            assert_eq!(field, "amount");
            assert_eq!(value, 100.0);
        }
        other => panic!("Expected FieldGreaterThan, got {:?}", other),
    }
}

#[test]
fn test_unrecognized_pattern_returns_always_true() {
    // Complex/unrecognized patterns should return AlwaysTrue (safe default)
    // Note: parse_simple_equality is very permissive, so we need patterns
    // that don't contain "=" or comparison operators

    // BETWEEN pattern - no simple equality or comparison parser handles this
    let result = parse_where_clause_cached("x BETWEEN 1 AND 10").unwrap();
    assert!(
        matches!(result, CachedPredicate::AlwaysTrue),
        "Expected AlwaysTrue for BETWEEN expression, got {:?}",
        result
    );

    // LIKE pattern - no parser handles this
    let result = parse_where_clause_cached("name LIKE '%test%'").unwrap();
    assert!(
        matches!(result, CachedPredicate::AlwaysTrue),
        "Expected AlwaysTrue for LIKE expression, got {:?}",
        result
    );

    // IS NULL pattern - no parser handles this
    let result = parse_where_clause_cached("value IS NULL").unwrap();
    assert!(
        matches!(result, CachedPredicate::AlwaysTrue),
        "Expected AlwaysTrue for IS NULL expression, got {:?}",
        result
    );
}
