use velostream::velostream::sql::error::SqlError;
use velostream::velostream::sql::parser::annotations::{
    PartitionAnnotations, parse_partition_annotations,
};

/// Test parsing partition annotations from empty comment list
#[test]
fn test_parse_partition_annotations_empty() {
    let comments = vec![];
    let annotations = parse_partition_annotations(&comments).unwrap();
    assert_eq!(annotations.sticky_partition_id, None);
    assert_eq!(annotations.partition_count, None);
}

/// Test parsing sticky partition ID annotation
#[test]
fn test_parse_partition_annotations_sticky_partition_id() {
    let comments = vec!["-- @sticky-partition-id: 2".to_string()];
    let annotations = parse_partition_annotations(&comments).unwrap();
    assert_eq!(annotations.sticky_partition_id, Some(2));
    assert_eq!(annotations.partition_count, None);
}

/// Test parsing partition count annotation
#[test]
fn test_parse_partition_annotations_partition_count() {
    let comments = vec!["-- @partition-count: 8".to_string()];
    let annotations = parse_partition_annotations(&comments).unwrap();
    assert_eq!(annotations.sticky_partition_id, None);
    assert_eq!(annotations.partition_count, Some(8));
}

/// Test parsing both sticky partition ID and partition count annotations
#[test]
fn test_parse_partition_annotations_both() {
    let comments = vec![
        "-- @sticky-partition-id: 3".to_string(),
        "-- @partition-count: 16".to_string(),
    ];
    let annotations = parse_partition_annotations(&comments).unwrap();
    assert_eq!(annotations.sticky_partition_id, Some(3));
    assert_eq!(annotations.partition_count, Some(16));
}

/// Test parsing annotations mixed with regular comments
#[test]
fn test_parse_partition_annotations_with_other_comments() {
    let comments = vec![
        "-- This is a regular comment".to_string(),
        "-- @sticky-partition-id: 1".to_string(),
        "-- Another comment".to_string(),
        "-- @partition-count: 4".to_string(),
    ];
    let annotations = parse_partition_annotations(&comments).unwrap();
    assert_eq!(annotations.sticky_partition_id, Some(1));
    assert_eq!(annotations.partition_count, Some(4));
}

/// Test that invalid sticky partition ID raises error
#[test]
fn test_parse_partition_annotations_invalid_sticky_partition_id() {
    let comments = vec!["-- @sticky-partition-id: invalid".to_string()];
    let result = parse_partition_annotations(&comments);
    assert!(result.is_err());
    if let Err(SqlError::ParseError { message, .. }) = result {
        assert!(message.contains("Invalid sticky-partition-id"));
    }
}

/// Test that invalid partition count raises error
#[test]
fn test_parse_partition_annotations_invalid_partition_count() {
    let comments = vec!["-- @partition-count: not_a_number".to_string()];
    let result = parse_partition_annotations(&comments);
    assert!(result.is_err());
    if let Err(SqlError::ParseError { message, .. }) = result {
        assert!(message.contains("Invalid partition-count"));
    }
}

/// Test that zero partition count is rejected
#[test]
fn test_parse_partition_annotations_zero_partition_count() {
    let comments = vec!["-- @partition-count: 0".to_string()];
    let result = parse_partition_annotations(&comments);
    assert!(result.is_err());
    if let Err(SqlError::ParseError { message, .. }) = result {
        assert!(message.contains("partition-count must be at least 1"));
    }
}

/// Test parsing with large partition values
#[test]
fn test_parse_partition_annotations_large_values() {
    let comments = vec![
        "-- @sticky-partition-id: 999".to_string(),
        "-- @partition-count: 1024".to_string(),
    ];
    let annotations = parse_partition_annotations(&comments).unwrap();
    assert_eq!(annotations.sticky_partition_id, Some(999));
    assert_eq!(annotations.partition_count, Some(1024));
}

/// Test default PartitionAnnotations returns None for both fields
#[test]
fn test_partition_annotations_default() {
    let annotations = PartitionAnnotations::default();
    assert_eq!(annotations.sticky_partition_id, None);
    assert_eq!(annotations.partition_count, None);
}
