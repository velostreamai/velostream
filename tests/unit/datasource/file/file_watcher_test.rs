//! Tests for file watching functionality
//!
//! Covers: creation/default, watch file/directory/glob/nonexistent,
//! change detection, new file detection, polling interval, rate limiting,
//! timeout, stop/cleanup, multiple files, and Drop behavior.

use std::fs;
use tempfile::TempDir;
use tokio::time::{Duration, sleep};
use velostream::velostream::datasource::file::watcher::FileWatcher;

#[tokio::test]
async fn test_file_watcher_creation() {
    let watcher = FileWatcher::new();
    assert_eq!(watcher.watched_paths().len(), 0);
    assert_eq!(watcher.polling_interval(), Duration::from_secs(1));
}

#[tokio::test]
async fn test_file_watcher_default() {
    let watcher = FileWatcher::default();
    assert_eq!(watcher.watched_paths().len(), 0);
}

#[tokio::test]
async fn test_watch_single_file() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    fs::write(&file_path, "initial content").unwrap();

    let mut watcher = FileWatcher::new();
    let result = watcher.watch(file_path.to_str().unwrap(), Some(100)).await;

    assert!(result.is_ok());
    assert_eq!(watcher.watched_paths().len(), 1);
    assert_eq!(watcher.polling_interval(), Duration::from_millis(100));
}

#[tokio::test]
async fn test_watch_directory() {
    let temp_dir = TempDir::new().unwrap();

    let mut watcher = FileWatcher::new();
    let result = watcher.watch(temp_dir.path().to_str().unwrap(), None).await;

    assert!(result.is_ok());
    assert_eq!(watcher.watched_paths().len(), 1);
    assert_eq!(watcher.polling_interval(), Duration::from_secs(1)); // Default
}

#[tokio::test]
async fn test_watch_glob_pattern() {
    let temp_dir = TempDir::new().unwrap();
    let pattern = format!("{}/*.txt", temp_dir.path().display());

    let mut watcher = FileWatcher::new();
    let result = watcher.watch(&pattern, Some(500)).await;

    assert!(result.is_ok());
    assert_eq!(watcher.polling_interval(), Duration::from_millis(500));
    // Should watch the parent directory
    assert_eq!(watcher.watched_paths().len(), 1);
}

#[tokio::test]
async fn test_watch_nonexistent_file() {
    let mut watcher = FileWatcher::new();
    let result = watcher.watch("/nonexistent/path/file.txt", None).await;

    assert!(result.is_err());
    assert_eq!(watcher.watched_paths().len(), 0);
}

#[tokio::test]
async fn test_detect_file_modifications() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    fs::write(&file_path, "initial content").unwrap();

    let mut watcher = FileWatcher::new();
    watcher
        .watch(file_path.to_str().unwrap(), Some(10))
        .await
        .unwrap();

    // Initial check should not detect changes
    let changes = watcher.check_for_changes().await.unwrap();
    assert!(!changes);

    // Modify the file
    sleep(Duration::from_millis(20)).await;
    fs::write(&file_path, "modified content").unwrap();

    // Should detect changes after modification
    sleep(Duration::from_millis(20)).await;
    let changes = watcher.check_for_changes().await.unwrap();
    assert!(changes);
}

#[tokio::test]
async fn test_detect_new_files_in_directory() {
    let temp_dir = TempDir::new().unwrap();

    let mut watcher = FileWatcher::new();
    watcher
        .watch(temp_dir.path().to_str().unwrap(), Some(10))
        .await
        .unwrap();

    // Initial check
    let changes = watcher.check_for_changes().await.unwrap();
    assert!(!changes);

    // Create a new file
    sleep(Duration::from_millis(20)).await;
    let new_file = temp_dir.path().join("new_file.txt");
    fs::write(&new_file, "new file content").unwrap();

    // Should detect the new file
    sleep(Duration::from_millis(20)).await;
    let _changes = watcher.check_for_changes().await.unwrap();

    // The new file should be added to watched paths
    assert!(watcher.watched_paths().len() > 1);
}

#[tokio::test]
async fn test_set_polling_interval() {
    let mut watcher = FileWatcher::new();
    assert_eq!(watcher.polling_interval(), Duration::from_secs(1));

    watcher.set_polling_interval(Duration::from_millis(250));
    assert_eq!(watcher.polling_interval(), Duration::from_millis(250));
}

#[tokio::test]
async fn test_watcher_stop() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    fs::write(&file_path, "content").unwrap();

    let mut watcher = FileWatcher::new();
    watcher
        .watch(file_path.to_str().unwrap(), None)
        .await
        .unwrap();

    assert_eq!(watcher.watched_paths().len(), 1);

    watcher.stop();
    assert_eq!(watcher.watched_paths().len(), 0);
}

#[tokio::test]
async fn test_watcher_drop_cleanup() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    fs::write(&file_path, "content").unwrap();

    let mut watcher = FileWatcher::new();
    watcher
        .watch(file_path.to_str().unwrap(), None)
        .await
        .unwrap();
    assert_eq!(watcher.watched_paths().len(), 1);

    // Drop should trigger cleanup via Drop trait without panicking
    drop(watcher);
}

#[tokio::test]
async fn test_multiple_file_watching() {
    let temp_dir = TempDir::new().unwrap();

    let file1 = temp_dir.path().join("file1.txt");
    let file2 = temp_dir.path().join("file2.txt");
    fs::write(&file1, "content1").unwrap();
    fs::write(&file2, "content2").unwrap();

    let mut watcher = FileWatcher::new();
    watcher
        .watch(file1.to_str().unwrap(), Some(10))
        .await
        .unwrap();
    watcher
        .watch(file2.to_str().unwrap(), Some(10))
        .await
        .unwrap();

    assert_eq!(watcher.watched_paths().len(), 2);

    // Modify one file
    sleep(Duration::from_millis(20)).await;
    fs::write(&file1, "modified content1").unwrap();

    sleep(Duration::from_millis(20)).await;
    let changes = watcher.check_for_changes().await.unwrap();
    assert!(changes);
}

#[tokio::test]
async fn test_rate_limiting_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    fs::write(&file_path, "content").unwrap();

    let mut watcher = FileWatcher::new();
    watcher
        .watch(file_path.to_str().unwrap(), Some(200))
        .await
        .unwrap(); // 200ms interval

    // Multiple rapid calls should be rate-limited
    let start = tokio::time::Instant::now();
    let _result1 = watcher.check_for_changes().await.unwrap();
    let result2 = watcher.check_for_changes().await.unwrap();
    let result3 = watcher.check_for_changes().await.unwrap();
    let elapsed = start.elapsed();

    // Subsequent calls should return quickly due to rate limiting
    assert!(!result2);
    assert!(!result3);
    assert!(elapsed < Duration::from_millis(100));
}

#[tokio::test]
async fn test_wait_for_changes_with_timeout() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    fs::write(&file_path, "initial content").unwrap();

    let mut watcher = FileWatcher::new();
    watcher
        .watch(file_path.to_str().unwrap(), Some(10))
        .await
        .unwrap();

    // Test timeout without changes
    let start = tokio::time::Instant::now();
    let changes = watcher
        .wait_for_changes(Some(Duration::from_millis(100)))
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert!(!changes);
    assert!(elapsed >= Duration::from_millis(90));
    assert!(elapsed <= Duration::from_millis(300));
}

#[tokio::test]
async fn test_wait_for_changes_with_modification() {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    fs::write(&file_path, "initial content").unwrap();

    let mut watcher = FileWatcher::new();
    watcher
        .watch(file_path.to_str().unwrap(), Some(10))
        .await
        .unwrap();

    // Modify file in background
    let file_path_clone = file_path.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(50)).await;
        fs::write(&file_path_clone, "modified content").unwrap();
    });

    // Should detect changes before timeout
    let start = tokio::time::Instant::now();
    let changes = watcher
        .wait_for_changes(Some(Duration::from_millis(500)))
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert!(changes);
    assert!(elapsed < Duration::from_millis(300));
}

#[tokio::test]
async fn test_watch_pattern_parent_directory() {
    let temp_dir = TempDir::new().unwrap();
    let subdir = temp_dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();

    let pattern = format!("{}/subdir/*.txt", temp_dir.path().display());

    let mut watcher = FileWatcher::new();
    let result = watcher.watch(&pattern, None).await;

    assert!(result.is_ok());
    assert!(!watcher.watched_paths().is_empty());
}

#[tokio::test]
async fn test_empty_directory_watching() {
    let temp_dir = TempDir::new().unwrap();
    let empty_dir = temp_dir.path().join("empty");
    fs::create_dir(&empty_dir).unwrap();

    let mut watcher = FileWatcher::new();
    let result = watcher.watch(empty_dir.to_str().unwrap(), None).await;

    assert!(result.is_ok());
    assert_eq!(watcher.watched_paths().len(), 1);

    // Should not detect changes in empty directory
    let changes = watcher.check_for_changes().await.unwrap();
    assert!(!changes);
}
