use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use velostream::velostream::datasource::config::SourceConfig;
use velostream::velostream::datasource::file::data_source::FileDataSource;
use velostream::velostream::datasource::traits::DataSource;
use velostream::velostream::table::retry_utils::{
    format_file_missing_error, parse_duration, wait_for_file_to_exist, wait_for_pattern_match,
};

#[test]
fn test_parse_duration_file_specific() {
    // Test file-specific duration parsing scenarios
    assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
    assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
    assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
    assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
    assert_eq!(parse_duration("0"), Some(Duration::from_secs(0)));

    // Test invalid formats
    assert_eq!(parse_duration("invalid"), None);
    assert_eq!(parse_duration(""), None);
    assert_eq!(parse_duration("-5s"), None);
}

#[tokio::test]
async fn test_wait_for_file_no_timeout() {
    // Test immediate failure when file doesn't exist and no timeout is configured
    let result = wait_for_file_to_exist(
        "/non/existent/file/test.txt",
        Duration::from_secs(0),
        Duration::from_secs(1),
    )
    .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err();
    assert!(error_message.contains("file.wait.timeout"));
    assert!(error_message.contains("watch"));
}

#[tokio::test]
async fn test_wait_for_file_with_timeout() {
    // Test timeout behavior when file doesn't exist
    let start = std::time::Instant::now();
    let result = wait_for_file_to_exist(
        "/non/existent/file/timeout_test.txt",
        Duration::from_secs(2),
        Duration::from_millis(500),
    )
    .await;

    let elapsed = start.elapsed();

    // Should timeout after ~2 seconds
    assert!(result.is_err());
    assert!(elapsed >= Duration::from_secs(2));
    assert!(elapsed < Duration::from_secs(4)); // Give some buffer for timing

    let error_message = result.unwrap_err();
    assert!(error_message.contains("Timeout waiting"));
    assert!(error_message.contains("timeout_test.txt"));
}

#[tokio::test]
async fn test_wait_for_file_existing_file() {
    // Create a temporary file for testing
    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_file_retry_existing.txt");
    fs::write(&test_file, "test content").expect("Failed to create test file");

    // Test that existing file is found immediately
    let start = std::time::Instant::now();
    let result = wait_for_file_to_exist(
        test_file.to_str().unwrap(),
        Duration::from_secs(10),
        Duration::from_secs(1),
    )
    .await;

    let elapsed = start.elapsed();

    // Should succeed quickly
    assert!(result.is_ok());
    assert!(elapsed < Duration::from_secs(1));

    // Clean up
    fs::remove_file(&test_file).ok();
}

#[tokio::test]
async fn test_wait_for_file_created_during_wait() {
    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("test_file_retry_delayed.txt");

    // Ensure file doesn't exist initially
    fs::remove_file(&test_file).ok();

    let file_path = test_file.to_str().unwrap().to_string();

    // Start waiting for file with a spawn task
    let wait_task = tokio::spawn(async move {
        wait_for_file_to_exist(
            &file_path,
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .await
    });

    // Create the file after a short delay
    tokio::spawn(async move {
        sleep(Duration::from_millis(300)).await;
        fs::write(&test_file, "delayed content").expect("Failed to create delayed test file");
    });

    let result = wait_task.await.expect("Task failed");

    // Should succeed because file was created during wait
    assert!(result.is_ok());

    // Clean up
    fs::remove_file(&test_file).ok();
}

#[tokio::test]
async fn test_wait_for_pattern_match_no_files() {
    // Test pattern matching when no files match
    let result = wait_for_pattern_match(
        "/non/existent/path/*.txt",
        Duration::from_secs(1),
        Duration::from_millis(200),
    )
    .await;

    assert!(result.is_err());
    let error_message = result.unwrap_err();
    assert!(error_message.contains("Timeout waiting"));
    assert!(error_message.contains("*.txt"));
}

#[tokio::test]
async fn test_wait_for_pattern_match_existing_files() {
    let temp_dir = std::env::temp_dir();
    let test_file1 = temp_dir.join("pattern_test_1.log");
    let test_file2 = temp_dir.join("pattern_test_2.log");

    // Create test files
    fs::write(&test_file1, "log content 1").expect("Failed to create test file 1");
    fs::write(&test_file2, "log content 2").expect("Failed to create test file 2");

    // Test pattern matching
    let pattern = format!("{}/*.log", temp_dir.to_str().unwrap());
    let result =
        wait_for_pattern_match(&pattern, Duration::from_secs(5), Duration::from_millis(100)).await;

    assert!(result.is_ok());
    let files = result.unwrap();
    assert!(files.len() >= 2); // Should find at least our 2 test files

    // Clean up
    fs::remove_file(&test_file1).ok();
    fs::remove_file(&test_file2).ok();
}

#[test]
fn test_format_file_missing_error() {
    let error_msg = format_file_missing_error("/path/to/missing/file.csv");

    assert!(error_msg.contains("/path/to/missing/file.csv"));
    assert!(error_msg.contains("file.wait.timeout"));
    assert!(error_msg.contains("watch"));
    assert!(error_msg.contains("Check the file path"));
}

#[tokio::test]
async fn test_file_data_source_with_retry_properties() {
    let mut properties = HashMap::new();
    properties.insert(
        "path".to_string(),
        "/non/existent/test/file.csv".to_string(),
    );
    properties.insert("format".to_string(), "csv".to_string());
    properties.insert("file.wait.timeout".to_string(), "1s".to_string());
    properties.insert("file.retry.interval".to_string(), "200ms".to_string());

    let mut file_source = FileDataSource::from_properties(&properties);

    // Test that the properties are stored and used
    let start = std::time::Instant::now();
    let result = file_source.self_initialize().await;
    let elapsed = start.elapsed();

    // Should fail after ~1 second due to timeout
    assert!(result.is_err());
    assert!(elapsed >= Duration::from_millis(800)); // Some tolerance
    assert!(elapsed < Duration::from_secs(3)); // Should not wait too long
}

#[tokio::test]
async fn test_file_data_source_backward_compatibility() {
    // Test that existing behavior (no retry properties) still works
    let mut properties = HashMap::new();
    properties.insert(
        "path".to_string(),
        "/non/existent/backward/compat.csv".to_string(),
    );
    properties.insert("format".to_string(), "csv".to_string());
    // No retry properties

    let mut file_source = FileDataSource::from_properties(&properties);

    let start = std::time::Instant::now();
    let result = file_source.self_initialize().await;
    let elapsed = start.elapsed();

    // Should fail immediately (no retry)
    assert!(result.is_err());
    assert!(elapsed < Duration::from_millis(500)); // Should be very fast
}

#[tokio::test]
async fn test_file_data_source_with_existing_file() {
    // Create a temporary CSV file
    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("existing_test.csv");
    fs::write(&test_file, "id,name,value\n1,test,100\n").expect("Failed to create test CSV");

    let mut properties = HashMap::new();
    properties.insert("path".to_string(), test_file.to_str().unwrap().to_string());
    properties.insert("format".to_string(), "csv".to_string());
    properties.insert("file.wait.timeout".to_string(), "5s".to_string());

    let mut file_source = FileDataSource::from_properties(&properties);

    // Should succeed quickly since file exists
    let start = std::time::Instant::now();
    let result = file_source.self_initialize().await;
    let elapsed = start.elapsed();

    assert!(result.is_ok());
    assert!(elapsed < Duration::from_secs(1)); // Should be immediate

    // Clean up
    fs::remove_file(&test_file).ok();
}

#[test]
fn test_property_defaults() {
    // Test various property value scenarios
    let test_cases = vec![
        ("", None),
        ("invalid", None),
        ("-10s", None),
        ("0", Some(Duration::from_secs(0))),
        ("500ms", Some(Duration::from_millis(500))),
        ("30s", Some(Duration::from_secs(30))),
        ("5m", Some(Duration::from_secs(300))),
        ("2h", Some(Duration::from_secs(7200))),
    ];

    for (input, expected) in test_cases {
        assert_eq!(
            parse_duration(input),
            expected,
            "Failed for input: '{}'",
            input
        );
    }
}
