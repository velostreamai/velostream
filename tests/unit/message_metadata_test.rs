use crate::unit::common::*;

#[test]
fn test_message_timestamp_timezone_handling() {
    let headers = Headers::new().insert("source", "test");

    // Test various timestamp scenarios
    let test_cases = vec![
        (Some(0), Some("1970-01-01 00:00:00.000")), // Unix epoch
        (Some(1633046400000), Some("2021-10-01 00:00:00.000")), // UTC midnight
        (Some(1633046400001), Some("2021-10-01 00:00:00.001")), // With milliseconds
        (Some(1640995200000), Some("2022-01-01 00:00:00.000")), // New Year UTC
        (None, None),                               // No timestamp
    ];

    for (timestamp, expected) in test_cases {
        let message = Message::new(
            Some("key".to_string()),
            "value".to_string(),
            headers.clone(),
            0,
            42,
            timestamp,
        );

        assert_eq!(message.timestamp(), timestamp);

        if let Some(expected_str) = expected {
            assert_eq!(message.timestamp_string(), Some(expected_str.to_string()));
        } else {
            assert_eq!(message.timestamp_string(), None);
        }
    }
}

#[test]
fn test_message_partition_offset_validation() {
    let headers = Headers::new();
    let timestamp = Some(1633046400000); // UTC midnight

    // Test boundary values for partition and offset
    let test_cases = vec![
        (-1, -1),             // Special values (often used for "any partition" or "unknown")
        (0, 0),               // Minimum valid values
        (0, 1),               // First message in partition
        (100, 999),           // Reasonable values
        (i32::MAX, i64::MAX), // Maximum values
    ];

    for (partition, offset) in test_cases {
        let message = Message::new(
            Some("key".to_string()),
            "value".to_string(),
            headers.clone(),
            partition,
            offset,
            timestamp,
        );

        assert_eq!(message.partition(), partition);
        assert_eq!(message.offset(), offset);

        // Test is_first() method
        if offset == 0 {
            assert!(message.is_first());
        } else {
            assert!(!message.is_first());
        }

        // Test metadata string contains partition and offset info
        let metadata = message.metadata_string();
        assert!(metadata.contains(&partition.to_string()));
        assert!(metadata.contains(&offset.to_string()));
    }
}

#[test]
fn test_message_metadata_consistency() {
    let headers = Headers::new()
        .insert("correlation-id", "test-123")
        .insert("source", "metadata-test");

    let message = Message::new(
        Some("test-key".to_string()),
        "test-value".to_string(),
        headers,
        5,
        1000,
        Some(1633046400000),
    );

    // Test that all metadata fields are consistently accessible
    assert_eq!(message.key(), Some(&"test-key".to_string()));
    assert_eq!(message.value(), &"test-value".to_string());
    assert_eq!(message.partition(), 5);
    assert_eq!(message.offset(), 1000);
    assert_eq!(message.timestamp(), Some(1633046400000));

    // Test headers are preserved
    assert_eq!(message.headers().get("correlation-id"), Some("test-123"));
    assert_eq!(message.headers().get("source"), Some("metadata-test"));

    // Test metadata string includes all information
    let metadata = message.metadata_string();
    assert!(metadata.contains("test-key"));
    assert!(metadata.contains("5")); // partition
    assert!(metadata.contains("1000")); // offset
    assert!(metadata.contains("2021")); // timestamp year
}

#[test]
fn test_message_key_value_edge_cases() {
    let headers = Headers::new();
    let timestamp = Some(1633046400000);

    // Test various key/value combinations
    let test_cases = vec![
        (None, ""),                      // No key, empty value
        (Some("".to_string()), "value"), // Empty key, normal value
        (Some("key".to_string()), ""),   // Normal key, empty value
        (Some("🦀".to_string()), "🚀"),  // Unicode key and value
        (
            Some("key\nwith\nnewlines".to_string()),
            "value\nwith\nnewlines",
        ), // Newlines
    ];

    for (key, value) in test_cases {
        let message = Message::new(
            key.clone(),
            value.to_string(),
            headers.clone(),
            0,
            0,
            timestamp,
        );

        assert_eq!(message.key(), key.as_ref());
        assert_eq!(message.value(), &value.to_string());

        // Test metadata string handles edge cases gracefully
        let metadata = message.metadata_string();
        assert!(!metadata.is_empty());
    }
}

#[test]
fn test_timestamp_edge_cases() {
    let headers = Headers::new();

    // Test edge case timestamps
    let edge_timestamps = vec![
        Some(0),               // Unix epoch
        Some(1),               // Almost epoch
        Some(253402300799000), // Year 9999
        None,                  // No timestamp
    ];

    for timestamp in edge_timestamps {
        let message = Message::new(
            Some("timestamp-test".to_string()),
            "value".to_string(),
            headers.clone(),
            0,
            0,
            timestamp,
        );

        assert_eq!(message.timestamp(), timestamp);

        // Timestamp string should either be valid or None
        let timestamp_str = message.timestamp_string();
        if timestamp.is_none() {
            assert_eq!(timestamp_str, None);
        } else {
            // Should not panic or return empty string
            if let Some(str_val) = timestamp_str {
                assert!(!str_val.is_empty());
            }
        }
    }
}
