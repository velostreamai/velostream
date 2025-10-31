//! Progress Monitoring for Stream-Table Load Coordination
//!
//! Provides real-time visibility into table loading progress, including loading rates,
//! ETA estimation, and health monitoring for production deployments.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, broadcast};

/// Real-time progress tracking for table loading operations
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableLoadProgress {
    /// Name of the table being loaded
    pub table_name: String,

    /// Total number of records expected (if known)
    pub total_records_expected: Option<usize>,

    /// Number of records loaded so far
    pub records_loaded: usize,

    /// Total bytes processed during loading
    pub bytes_processed: u64,

    /// When the loading operation started
    pub started_at: DateTime<Utc>,

    /// Estimated completion time (if calculable)
    pub estimated_completion: Option<DateTime<Utc>>,

    /// Current loading rate in records per second
    pub loading_rate: f64,

    /// Current loading rate in bytes per second
    pub bytes_per_second: f64,

    /// Current loading status
    pub status: TableLoadStatus,

    /// Optional error message if loading failed
    pub error_message: Option<String>,

    /// Progress percentage (0.0 to 100.0) if total is known
    pub progress_percentage: Option<f64>,
}

/// Status of table loading operation
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableLoadStatus {
    /// Table loading is starting up
    Initializing,

    /// Table is actively loading data
    Loading,

    /// Table loading completed successfully
    Completed,

    /// Table loading failed with error
    Failed,

    /// Table loading was cancelled
    Cancelled,

    /// Table is waiting for dependencies
    WaitingForDependencies,
}

/// Internal progress tracker with atomic counters for high-performance updates
#[derive(Debug)]
pub struct TableProgressTracker {
    /// Table name for identification
    table_name: String,

    /// Total records expected (if known)
    total_records_expected: Option<usize>,

    /// Atomic counter for records loaded
    records_loaded: AtomicUsize,

    /// Atomic counter for bytes processed
    bytes_processed: AtomicU64,

    /// When loading started
    started_at: Instant,

    /// Current status
    status: Arc<RwLock<TableLoadStatus>>,

    /// Error message if any
    error_message: Arc<RwLock<Option<String>>>,

    /// Broadcast channel for real-time updates
    progress_sender: broadcast::Sender<TableLoadProgress>,
}

impl TableProgressTracker {
    /// Create a new progress tracker for a table
    pub fn new(table_name: String, total_records_expected: Option<usize>) -> Self {
        let (progress_sender, _) = broadcast::channel(1000); // Buffer 1000 progress updates

        Self {
            table_name,
            total_records_expected,
            records_loaded: AtomicUsize::new(0),
            bytes_processed: AtomicU64::new(0),
            started_at: Instant::now(),
            status: Arc::new(RwLock::new(TableLoadStatus::Initializing)),
            error_message: Arc::new(RwLock::new(None)),
            progress_sender,
        }
    }

    /// Increment the record count by the specified amount
    pub async fn add_records(&self, count: usize, bytes: u64) {
        self.records_loaded.fetch_add(count, Ordering::Relaxed);
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);

        // Send progress update
        let progress = self.get_current_progress().await;
        let _ = self.progress_sender.send(progress); // Ignore send errors (no receivers)
    }

    /// Update the loading status
    pub async fn set_status(&self, status: TableLoadStatus) {
        let mut current_status = self.status.write().await;
        *current_status = status;

        // Send progress update
        let progress = self.get_current_progress().await;
        let _ = self.progress_sender.send(progress);
    }

    /// Set an error message and mark as failed
    pub async fn set_error(&self, error: String) {
        let mut error_msg = self.error_message.write().await;
        *error_msg = Some(error);

        let mut status = self.status.write().await;
        *status = TableLoadStatus::Failed;

        // Send progress update
        let progress = self.get_current_progress().await;
        let _ = self.progress_sender.send(progress);
    }

    /// Mark loading as completed
    pub async fn set_completed(&self) {
        let mut status = self.status.write().await;
        *status = TableLoadStatus::Completed;

        // Send final progress update
        let progress = self.get_current_progress().await;
        let _ = self.progress_sender.send(progress);
    }

    /// Get current progress snapshot
    pub async fn get_current_progress(&self) -> TableLoadProgress {
        let records_loaded = self.records_loaded.load(Ordering::Relaxed);
        let bytes_processed = self.bytes_processed.load(Ordering::Relaxed);
        let elapsed = self.started_at.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        // Calculate loading rates
        let loading_rate = if elapsed_secs > 0.0 {
            records_loaded as f64 / elapsed_secs
        } else {
            0.0
        };

        let bytes_per_second = if elapsed_secs > 0.0 {
            bytes_processed as f64 / elapsed_secs
        } else {
            0.0
        };

        // Calculate progress percentage and ETA
        let (progress_percentage, estimated_completion) =
            if let Some(total) = self.total_records_expected {
                let percentage = if total > 0 {
                    Some((records_loaded as f64 / total as f64 * 100.0).min(100.0))
                } else {
                    Some(100.0)
                };

                let eta = if loading_rate > 0.0 && records_loaded < total {
                    let remaining_records = total - records_loaded;
                    let eta_seconds = remaining_records as f64 / loading_rate;
                    Some(Utc::now() + chrono::Duration::seconds(eta_seconds as i64))
                } else {
                    None
                };

                (percentage, eta)
            } else {
                (None, None)
            };

        let status = self.status.read().await.clone();
        let error_message = self.error_message.read().await.clone();

        TableLoadProgress {
            table_name: self.table_name.clone(),
            total_records_expected: self.total_records_expected,
            records_loaded,
            bytes_processed,
            started_at: Utc::now() - chrono::Duration::from_std(elapsed).unwrap_or_default(),
            estimated_completion,
            loading_rate,
            bytes_per_second,
            status,
            error_message,
            progress_percentage,
        }
    }

    /// Subscribe to real-time progress updates
    pub fn subscribe(&self) -> broadcast::Receiver<TableLoadProgress> {
        self.progress_sender.subscribe()
    }

    /// Get a snapshot of the current progress without async
    pub fn get_current_progress_sync(&self) -> TableLoadProgress {
        let records_loaded = self.records_loaded.load(Ordering::Relaxed);
        let bytes_processed = self.bytes_processed.load(Ordering::Relaxed);
        let elapsed = self.started_at.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();

        // Calculate loading rates
        let loading_rate = if elapsed_secs > 0.0 {
            records_loaded as f64 / elapsed_secs
        } else {
            0.0
        };

        let bytes_per_second = if elapsed_secs > 0.0 {
            bytes_processed as f64 / elapsed_secs
        } else {
            0.0
        };

        // Calculate progress percentage and ETA
        let (progress_percentage, estimated_completion) =
            if let Some(total) = self.total_records_expected {
                let percentage = if total > 0 {
                    Some((records_loaded as f64 / total as f64 * 100.0).min(100.0))
                } else {
                    Some(100.0)
                };

                let eta = if loading_rate > 0.0 && records_loaded < total {
                    let remaining_records = total - records_loaded;
                    let eta_seconds = remaining_records as f64 / loading_rate;
                    Some(Utc::now() + chrono::Duration::seconds(eta_seconds as i64))
                } else {
                    None
                };

                (percentage, eta)
            } else {
                (None, None)
            };

        // For sync access, we can't read the RwLock, so use default values
        let status = TableLoadStatus::Loading; // Default assumption
        let error_message = None; // Can't access async lock

        TableLoadProgress {
            table_name: self.table_name.clone(),
            total_records_expected: self.total_records_expected,
            records_loaded,
            bytes_processed,
            started_at: Utc::now() - chrono::Duration::from_std(elapsed).unwrap_or_default(),
            estimated_completion,
            loading_rate,
            bytes_per_second,
            status,
            error_message,
            progress_percentage,
        }
    }
}

/// Progress monitoring manager for all table loading operations
#[derive(Debug)]
pub struct ProgressMonitor {
    /// Active progress trackers by table name
    trackers: Arc<RwLock<HashMap<String, Arc<TableProgressTracker>>>>,

    /// Global progress broadcast channel
    global_progress_sender: broadcast::Sender<HashMap<String, TableLoadProgress>>,
}

impl ProgressMonitor {
    /// Create a new progress monitor
    pub fn new() -> Self {
        let (global_progress_sender, _) = broadcast::channel(100);

        Self {
            trackers: Arc::new(RwLock::new(HashMap::new())),
            global_progress_sender,
        }
    }

    /// Start tracking progress for a new table
    pub async fn start_tracking(
        &self,
        table_name: String,
        total_records_expected: Option<usize>,
    ) -> Arc<TableProgressTracker> {
        let tracker = Arc::new(TableProgressTracker::new(
            table_name.clone(),
            total_records_expected,
        ));

        let mut trackers = self.trackers.write().await;
        trackers.insert(table_name, tracker.clone());

        tracker
    }

    /// Stop tracking a table (when loading completes or fails)
    pub async fn stop_tracking(&self, table_name: &str) {
        let mut trackers = self.trackers.write().await;
        trackers.remove(table_name);
    }

    /// Get progress for a specific table
    pub async fn get_table_progress(&self, table_name: &str) -> Option<TableLoadProgress> {
        let trackers = self.trackers.read().await;
        if let Some(tracker) = trackers.get(table_name) {
            Some(tracker.get_current_progress().await)
        } else {
            None
        }
    }

    /// Get progress for all actively loading tables
    pub async fn get_all_progress(&self) -> HashMap<String, TableLoadProgress> {
        // Clone the tracker references to avoid holding the lock while calling async methods
        let tracker_refs: Vec<(String, Arc<TableProgressTracker>)> = {
            let trackers = self.trackers.read().await;
            trackers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        let mut progress_map = HashMap::new();
        for (table_name, tracker) in tracker_refs {
            let progress = tracker.get_current_progress().await;
            progress_map.insert(table_name, progress);
        }

        progress_map
    }

    /// Subscribe to global progress updates for all tables
    pub fn subscribe_to_global_progress(
        &self,
    ) -> broadcast::Receiver<HashMap<String, TableLoadProgress>> {
        self.global_progress_sender.subscribe()
    }

    /// Subscribe to progress updates for a specific table
    pub async fn subscribe_to_table_progress(
        &self,
        table_name: &str,
    ) -> Option<broadcast::Receiver<TableLoadProgress>> {
        let trackers = self.trackers.read().await;
        trackers.get(table_name).map(|tracker| tracker.subscribe())
    }

    /// Get loading summary statistics
    pub async fn get_loading_summary(&self) -> LoadingSummary {
        // Clone the tracker references to avoid holding the lock while calling async methods
        let tracker_refs: Vec<Arc<TableProgressTracker>> = {
            let trackers = self.trackers.read().await;
            let refs: Vec<Arc<TableProgressTracker>> = trackers.values().cloned().collect();
            refs
        };

        let mut summary = LoadingSummary::default();
        summary.total_tables = tracker_refs.len();

        for tracker in tracker_refs {
            let progress = tracker.get_current_progress().await;

            match progress.status {
                TableLoadStatus::Initializing => summary.initializing += 1,
                TableLoadStatus::Loading => summary.loading += 1,
                TableLoadStatus::Completed => summary.completed += 1,
                TableLoadStatus::Failed => summary.failed += 1,
                TableLoadStatus::Cancelled => summary.cancelled += 1,
                TableLoadStatus::WaitingForDependencies => summary.waiting_for_dependencies += 1,
            }

            summary.total_records_loaded += progress.records_loaded;
            summary.total_bytes_processed += progress.bytes_processed;
        }

        summary
    }

    /// Send global progress update
    async fn broadcast_global_progress(&self) {
        let progress = self.get_all_progress().await;
        let _ = self.global_progress_sender.send(progress);
    }
}

impl Default for ProgressMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary statistics for all loading operations
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct LoadingSummary {
    pub total_tables: usize,
    pub initializing: usize,
    pub loading: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub waiting_for_dependencies: usize,
    pub total_records_loaded: usize,
    pub total_bytes_processed: u64,
}

impl LoadingSummary {
    /// Calculate overall progress percentage
    pub fn overall_progress_percentage(&self) -> f64 {
        if self.total_tables == 0 {
            100.0
        } else {
            (self.completed as f64 / self.total_tables as f64) * 100.0
        }
    }

    /// Check if all tables are done loading (completed, failed, or cancelled)
    pub fn is_all_done(&self) -> bool {
        self.loading + self.initializing + self.waiting_for_dependencies == 0
    }

    /// Check if any tables have errors
    pub fn has_errors(&self) -> bool {
        self.failed > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration as TokioDuration, sleep};

    #[test]
    fn test_table_progress_tracker() {
        let tracker = TableProgressTracker::new("test_table".to_string(), Some(1000));

        // Test initial state using sync version
        let progress = tracker.get_current_progress_sync();
        assert_eq!(progress.table_name, "test_table");
        assert_eq!(progress.total_records_expected, Some(1000));
        assert_eq!(progress.records_loaded, 0);
        assert_eq!(progress.bytes_processed, 0);

        // Test atomic operations
        use std::sync::atomic::Ordering;
        tracker.records_loaded.store(100, Ordering::Relaxed);
        tracker.bytes_processed.store(1024, Ordering::Relaxed);

        let progress = tracker.get_current_progress_sync();
        assert_eq!(progress.records_loaded, 100);
        assert_eq!(progress.bytes_processed, 1024);
        assert!(progress.loading_rate >= 0.0);

        // Check progress percentage
        if let Some(percentage) = progress.progress_percentage {
            assert!((percentage - 10.0).abs() < 0.1); // Should be ~10%
        }
    }

    #[test]
    fn test_progress_monitor() {
        let monitor = ProgressMonitor::new();

        // Test basic monitor creation and structure
        let trackers_guard = futures::executor::block_on(monitor.trackers.read());
        assert_eq!(trackers_guard.len(), 0);

        // Test that we can create trackers directly
        let tracker1 =
            std::sync::Arc::new(TableProgressTracker::new("table1".to_string(), Some(500)));
        let tracker2 = std::sync::Arc::new(TableProgressTracker::new("table2".to_string(), None));

        // Test tracker functionality directly without async operations
        use std::sync::atomic::Ordering;
        tracker1.records_loaded.store(50, Ordering::Relaxed);
        tracker1.bytes_processed.store(512, Ordering::Relaxed);
        tracker2.records_loaded.store(25, Ordering::Relaxed);
        tracker2.bytes_processed.store(256, Ordering::Relaxed);

        let progress1 = tracker1.get_current_progress_sync();
        let progress2 = tracker2.get_current_progress_sync();

        assert_eq!(progress1.records_loaded, 50);
        assert_eq!(progress1.bytes_processed, 512);
        assert_eq!(progress2.records_loaded, 25);
        assert_eq!(progress2.bytes_processed, 256);
    }

    #[tokio::test]
    async fn test_progress_subscription() {
        let tracker = TableProgressTracker::new("test_table".to_string(), Some(100));
        let mut receiver = tracker.subscribe();

        // Add records and verify we receive updates
        tracker.add_records(10, 100).await;

        // We should receive a progress update
        let progress = receiver.recv().await.unwrap();
        assert_eq!(progress.records_loaded, 10);
        assert_eq!(progress.bytes_processed, 100);
    }

    #[tokio::test]
    async fn test_loading_rate_calculation() {
        let tracker = TableProgressTracker::new("test_table".to_string(), Some(1000));

        // Add records and wait a bit to get a meaningful rate
        tracker.add_records(100, 1000).await;
        sleep(TokioDuration::from_millis(100)).await;
        tracker.add_records(100, 1000).await;

        let progress = tracker.get_current_progress().await;
        assert!(progress.loading_rate > 0.0);
        assert!(progress.bytes_per_second > 0.0);

        // Rate should be reasonable (not impossibly high)
        assert!(progress.loading_rate < 100_000.0); // Less than 100K records/sec
    }

    #[tokio::test]
    async fn test_eta_calculation() {
        let tracker = TableProgressTracker::new("test_table".to_string(), Some(1000));

        // Add some records and wait for rate calculation
        tracker.add_records(100, 1000).await;
        sleep(TokioDuration::from_millis(50)).await;

        let progress = tracker.get_current_progress().await;

        // Should have ETA if we have a loading rate and records remaining
        if progress.loading_rate > 0.0 && progress.records_loaded < 1000 {
            assert!(progress.estimated_completion.is_some());
        }
    }
}
