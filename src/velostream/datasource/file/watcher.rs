//! File Watching Implementation

use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::time::{Instant, sleep};

use super::error::FileDataSourceError;

/// File watcher for monitoring file changes and new files
pub struct FileWatcher {
    watched_paths: Vec<PathBuf>,
    last_modified: Vec<Option<SystemTime>>,
    polling_interval: Duration,
    last_check: Option<Instant>,
}

impl FileWatcher {
    /// Create a new file watcher
    pub fn new() -> Self {
        Self {
            watched_paths: Vec::new(),
            last_modified: Vec::new(),
            polling_interval: Duration::from_secs(1),
            last_check: None,
        }
    }

    /// Start watching a file or directory pattern
    pub async fn watch(
        &mut self,
        path: &str,
        polling_interval_ms: Option<u64>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let path_obj = Path::new(path);

        if let Some(interval) = polling_interval_ms {
            self.polling_interval = Duration::from_millis(interval);
        }

        // Handle different path types
        if path.contains('*') || path.contains('?') {
            // Glob pattern - watch the parent directory
            if let Some(parent) = path_obj.parent() {
                self.add_watch_path(parent.to_path_buf()).await?;
            }
        } else if path_obj.is_file() {
            // Single file
            self.add_watch_path(path_obj.to_path_buf()).await?;
        } else if path_obj.is_dir() {
            // Directory
            self.add_watch_path(path_obj.to_path_buf()).await?;
        } else {
            return Err(Box::new(FileDataSourceError::FileNotFound(
                path.to_string(),
            )));
        }

        self.last_check = Some(Instant::now());

        Ok(())
    }

    /// Add a path to watch list
    async fn add_watch_path(&mut self, path: PathBuf) -> Result<(), Box<dyn Error + Send + Sync>> {
        let metadata = path
            .metadata()
            .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;

        let modified_time = metadata.modified().ok();

        self.watched_paths.push(path);
        self.last_modified.push(modified_time);

        Ok(())
    }

    /// Check if any watched files have changed
    pub async fn check_for_changes(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let now = Instant::now();

        // Rate limit checks based on polling interval
        if let Some(last) = self.last_check {
            if now.duration_since(last) < self.polling_interval {
                return Ok(false);
            }
        }

        let mut changes_detected = false;

        for (i, path) in self.watched_paths.iter().enumerate() {
            if let Ok(metadata) = path.metadata() {
                let current_modified = metadata.modified().ok();

                if current_modified != self.last_modified[i] {
                    self.last_modified[i] = current_modified;
                    changes_detected = true;
                }
            }
        }

        // Check for new files in watched directories
        changes_detected |= self.check_for_new_files().await?;

        self.last_check = Some(now);

        Ok(changes_detected)
    }

    /// Check for new files in watched directories
    async fn check_for_new_files(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let mut new_files_found = false;

        for path in &self.watched_paths.clone() {
            if path.is_dir() {
                // Scan directory for new files
                let entries = std::fs::read_dir(path)
                    .map_err(|e| FileDataSourceError::IoError(e.to_string()))?;

                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    if entry_path.is_file() && !self.watched_paths.contains(&entry_path) {
                        // New file found
                        self.add_watch_path(entry_path).await?;
                        new_files_found = true;
                    }
                }
            }
        }

        Ok(new_files_found)
    }

    /// Wait for file changes with timeout
    pub async fn wait_for_changes(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let start = Instant::now();
        let timeout_duration = timeout.unwrap_or(Duration::from_secs(30));

        loop {
            if self.check_for_changes().await? {
                return Ok(true);
            }

            if start.elapsed() >= timeout_duration {
                return Ok(false);
            }

            // Sleep for a short interval before checking again
            sleep(self.polling_interval.min(Duration::from_millis(100))).await;
        }
    }

    /// Get list of watched paths
    pub fn watched_paths(&self) -> &[PathBuf] {
        &self.watched_paths
    }

    /// Get current polling interval
    pub fn polling_interval(&self) -> Duration {
        self.polling_interval
    }

    /// Set polling interval
    pub fn set_polling_interval(&mut self, interval: Duration) {
        self.polling_interval = interval;
    }

    /// Stop watching (cleanup)
    pub fn stop(&mut self) {
        self.watched_paths.clear();
        self.last_modified.clear();
        self.last_check = None;
    }
}

impl Default for FileWatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FileWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_file_watcher_creation() {
        let watcher = FileWatcher::new();
        assert_eq!(watcher.watched_paths.len(), 0);
        assert_eq!(watcher.polling_interval, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_watch_single_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        fs::write(&file_path, "test content").unwrap();

        let mut watcher = FileWatcher::new();
        let result = watcher.watch(file_path.to_str().unwrap(), Some(100)).await;

        assert!(result.is_ok());
        assert_eq!(watcher.watched_paths.len(), 1);
        assert_eq!(watcher.polling_interval, Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_detect_file_changes() {
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
        sleep(Duration::from_millis(20)).await; // Ensure different timestamp
        fs::write(&file_path, "modified content").unwrap();

        // Should detect changes now
        sleep(Duration::from_millis(20)).await; // Wait for polling interval
        let changes = watcher.check_for_changes().await.unwrap();
        assert!(changes);
    }
}
