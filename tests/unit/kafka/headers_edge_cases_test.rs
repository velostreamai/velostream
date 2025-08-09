use crate::unit::common::*;

#[test]
fn test_headers_with_binary_data() {
    let binary_data = vec![0u8, 1u8, 2u8, 0x7F];
    let null_bytes = b"before\0after".to_vec();
    let mixed = b"text\0binary\x7F".to_vec();

    let headers = Headers::new()
        .insert("binary", String::from_utf8_lossy(&binary_data))
        .insert("null-bytes", String::from_utf8_lossy(&null_bytes))
        .insert("mixed", String::from_utf8_lossy(&mixed));

    // Test that binary data is preserved
    assert_eq!(
        headers.get("binary").unwrap(),
        String::from_utf8_lossy(&binary_data)
    );
    assert_eq!(
        headers.get("null-bytes").unwrap(),
        String::from_utf8_lossy(&null_bytes)
    );
    assert_eq!(
        headers.get("mixed").unwrap(),
        String::from_utf8_lossy(&mixed)
    );

    // Test iteration over binary headers
    let mut found_binary = false;
    for (key, value) in headers.iter() {
        if key == "binary" {
            found_binary = true;
            assert_eq!(
                value,
                &Some(String::from_utf8_lossy(&binary_data).to_string())
            );
        }
    }
    assert!(found_binary);
}

#[test]
fn test_headers_size_limits() {
    // Test various header sizes
    let small_value = "small".to_string();
    let medium_value = "x".repeat(1024); // 1KB
    let large_value = "x".repeat(64 * 1024); // 64KB

    let headers = Headers::new()
        .insert("small", &small_value)
        .insert("medium", &medium_value)
        .insert("large", &large_value);

    // Verify all values are stored correctly
    assert_eq!(
        headers.get("small").map(ToString::to_string),
        Some(small_value.clone())
    );
    assert_eq!(
        headers.get("medium").map(ToString::to_string),
        Some(medium_value.clone())
    );
    assert_eq!(
        headers.get("large").map(ToString::to_string),
        Some(large_value.clone())
    );

    // Verify iteration works with large values
    for (key, value) in headers.iter() {
        match key.as_str() {
            "small" => assert_eq!(
                value.as_ref().map(|s| s.to_string()),
                Some(small_value.clone())
            ),
            "medium" => assert_eq!(value.as_ref().map(|s| s.len()), Some(1024)),
            "large" => assert_eq!(value.as_ref().map(|s| s.len()), Some(64 * 1024)),
            _ => panic!("Unexpected key: {}", key),
        }
    }
}

#[test]
fn test_headers_special_characters() {
    let headers = Headers::new()
        .insert("unicode-emoji", "🦀🚀✨") // Emoji
        .insert("unicode-text", "Hello, 世界!") // Mixed languages
        .insert("control-chars", "\t\n\r") // Control characters
        .insert("quotes", "\"single\" 'double'") // Quote characters
        .insert("symbols", "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./") // Special symbols
        .insert("spaces", "  leading and trailing  ") // Leading/trailing spaces
        .insert("newlines", "line1\nline2\nline3"); // Multiline

    // Test Unicode handling
    assert_eq!(headers.get("unicode-emoji"), Some("🦀🚀✨"));
    assert_eq!(headers.get("unicode-text"), Some("Hello, 世界!"));

    // Test control characters
    assert_eq!(headers.get("control-chars"), Some("\t\n\r"));

    // Test special symbols
    assert_eq!(
        headers.get("symbols"),
        Some("!@#$%^&*()_+-={}[]|\\:;\"'<>?,./")
    );

    // Test whitespace preservation
    assert_eq!(headers.get("spaces"), Some("  leading and trailing  "));

    // Test multiline values
    assert_eq!(headers.get("newlines"), Some("line1\nline2\nline3"));
}

#[test]
fn test_headers_edge_case_keys() {
    let headers = Headers::new()
        .insert("", "empty-key") // Empty key
        .insert(" ", "space-key") // Space key
        .insert("\t", "tab-key") // Tab key
        .insert("🔑", "emoji-key") // Emoji key
        .insert("key with spaces", "spaces-in-key") // Spaces in key
        .insert("key.with.dots", "dotted-key") // Dots in key
        .insert("key-with-dashes", "dashed-key") // Dashes in key
        .insert("key_with_underscores", "underscore-key") // Underscores
        .insert("UPPERCASE", "upper-key") // Uppercase
        .insert("MixedCase", "mixed-key"); // Mixed case

    // Test that all keys are preserved
    assert_eq!(headers.get(""), Some("empty-key"));
    assert_eq!(headers.get(" "), Some("space-key"));
    assert_eq!(headers.get("\t"), Some("tab-key"));
    assert_eq!(headers.get("🔑"), Some("emoji-key"));
    assert_eq!(headers.get("key with spaces"), Some("spaces-in-key"));
    assert_eq!(headers.get("key.with.dots"), Some("dotted-key"));
    assert_eq!(headers.get("key-with-dashes"), Some("dashed-key"));
    assert_eq!(headers.get("key_with_underscores"), Some("underscore-key"));
    assert_eq!(headers.get("UPPERCASE"), Some("upper-key"));
    assert_eq!(headers.get("MixedCase"), Some("mixed-key"));
}

#[test]
fn test_headers_null_values() {
    let headers = Headers::new()
        .insert_null("null-header")
        .insert("regular-header", "regular-value")
        .insert_null("another-null");

    // Test null value handling
    assert_eq!(headers.get_optional("null-header"), Some(&None));
    assert_eq!(
        headers.get_optional("regular-header"),
        Some(&Some("regular-value".to_string()))
    );
    assert_eq!(headers.get_optional("another-null"), Some(&None));

    // Test that get() returns None for null headers
    assert_eq!(headers.get("null-header"), None);
    assert_eq!(headers.get("regular-header"), Some("regular-value"));

    // Test contains_key for null headers
    assert!(headers.contains_key("null-header"));
    assert!(headers.contains_key("regular-header"));
    assert!(headers.contains_key("another-null"));
}

#[test]
fn test_headers_capacity_and_performance() {
    // Test with pre-allocated capacity
    let mut headers_with_capacity = Headers::with_capacity(100);

    // Add many headers to test capacity handling
    for i in 0..50 {
        headers_with_capacity =
            headers_with_capacity.insert(&format!("key{}", i), &format!("value{}", i));
    }

    assert_eq!(headers_with_capacity.len(), 50);

    // Test that all headers are accessible
    for i in 0..50 {
        assert_eq!(
            headers_with_capacity.get(&format!("key{}", i)),
            Some(format!("value{}", i).as_str())
        );
    }
}

#[test]
fn test_headers_overwrite_behavior() {
    let headers = Headers::new()
        .insert("key", "value1")
        .insert("key", "value2") // Should overwrite
        .insert("key", "value3"); // Should overwrite again

    // Test that the last value wins
    assert_eq!(headers.get("key"), Some("value3"));
    assert_eq!(headers.len(), 1); // Should only have one entry
}

#[test]
fn test_headers_case_sensitivity() {
    let headers = Headers::new()
        .insert("Key", "uppercase")
        .insert("key", "lowercase")
        .insert("KEY", "allcaps")
        .insert("kEy", "mixed");

    // Headers should be case-sensitive - each should be separate
    assert_eq!(headers.get("Key"), Some("uppercase"));
    assert_eq!(headers.get("key"), Some("lowercase"));
    assert_eq!(headers.get("KEY"), Some("allcaps"));
    assert_eq!(headers.get("kEy"), Some("mixed"));
    assert_eq!(headers.len(), 4);
}
