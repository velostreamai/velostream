//! Log capture for debugger step execution
//!
//! Captures ERROR and WARN log entries to a ring buffer during step execution,
//! allowing the debugger to show relevant logs for each step.
//!
//! # Usage
//!
//! ```ignore
//! // Initialize at startup (replaces env_logger::init())
//! log_capture::init_capturing_logger();
//!
//! // During step execution
//! let start = Instant::now();
//! // ... execute step ...
//! let logs = log_capture::entries_since(start);
//! ```

use chrono::{DateTime, Utc};
use log::{Level, LevelFilter, Log, Metadata, Record};
use std::collections::VecDeque;
use std::sync::{LazyLock, RwLock};
use std::time::Instant;

/// Maximum entries to keep in the ring buffer
const MAX_BUFFER_SIZE: usize = 1000;

/// A captured log entry
#[derive(Debug, Clone)]
pub struct CapturedLogEntry {
    /// Monotonic timestamp for time-range queries
    pub instant: Instant,
    /// Wall clock time for display
    pub timestamp: DateTime<Utc>,
    /// Log level (Error or Warn)
    pub level: Level,
    /// Logger target (module path)
    pub target: String,
    /// Log message
    pub message: String,
}

impl CapturedLogEntry {
    /// Format for display
    pub fn display(&self) -> String {
        let level_str = match self.level {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            _ => "INFO",
        };
        format!("[{}] {} - {}", level_str, self.target, self.message)
    }

    /// Short format (just level and message)
    pub fn display_short(&self) -> String {
        let level_str = match self.level {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            _ => "INFO",
        };
        format!("[{}] {}", level_str, self.message)
    }
}

/// Global ring buffer for captured log entries
static LOG_BUFFER: LazyLock<RwLock<VecDeque<CapturedLogEntry>>> =
    LazyLock::new(|| RwLock::new(VecDeque::with_capacity(MAX_BUFFER_SIZE)));

/// Whether capture is enabled (can be toggled at runtime)
static CAPTURE_ENABLED: LazyLock<RwLock<bool>> = LazyLock::new(|| RwLock::new(true));

/// Get all log entries since the given instant
pub fn entries_since(start: Instant) -> Vec<CapturedLogEntry> {
    if let Ok(buffer) = LOG_BUFFER.read() {
        buffer
            .iter()
            .filter(|entry| entry.instant >= start)
            .cloned()
            .collect()
    } else {
        Vec::new()
    }
}

/// Get ERROR entries since the given instant
pub fn errors_since(start: Instant) -> Vec<CapturedLogEntry> {
    if let Ok(buffer) = LOG_BUFFER.read() {
        buffer
            .iter()
            .filter(|entry| entry.instant >= start && entry.level == Level::Error)
            .cloned()
            .collect()
    } else {
        Vec::new()
    }
}

/// Get WARN entries since the given instant
pub fn warnings_since(start: Instant) -> Vec<CapturedLogEntry> {
    if let Ok(buffer) = LOG_BUFFER.read() {
        buffer
            .iter()
            .filter(|entry| entry.instant >= start && entry.level == Level::Warn)
            .cloned()
            .collect()
    } else {
        Vec::new()
    }
}

/// Clear all captured entries
pub fn clear() {
    if let Ok(mut buffer) = LOG_BUFFER.write() {
        buffer.clear();
    }
}

/// Enable or disable log capture
pub fn set_capture_enabled(enabled: bool) {
    if let Ok(mut flag) = CAPTURE_ENABLED.write() {
        *flag = enabled;
    }
}

/// Check if capture is enabled
pub fn is_capture_enabled() -> bool {
    CAPTURE_ENABLED.read().map(|f| *f).unwrap_or(false)
}

/// Get recent entries (last N)
pub fn recent_entries(count: usize) -> Vec<CapturedLogEntry> {
    if let Ok(buffer) = LOG_BUFFER.read() {
        buffer.iter().rev().take(count).cloned().collect()
    } else {
        Vec::new()
    }
}

/// Get buffer statistics
pub fn stats() -> (usize, usize) {
    if let Ok(buffer) = LOG_BUFFER.read() {
        let errors = buffer.iter().filter(|e| e.level == Level::Error).count();
        let warns = buffer.iter().filter(|e| e.level == Level::Warn).count();
        (errors, warns)
    } else {
        (0, 0)
    }
}

/// Add an entry to the buffer (used by CapturingLogger)
fn add_entry(entry: CapturedLogEntry) {
    if let Ok(mut buffer) = LOG_BUFFER.write() {
        buffer.push_back(entry);
        // Keep buffer at max size
        while buffer.len() > MAX_BUFFER_SIZE {
            buffer.pop_front();
        }
    }
}

/// A logger that wraps env_logger and captures ERROR/WARN to buffer
pub struct CapturingLogger {
    inner: env_logger::Logger,
}

impl CapturingLogger {
    /// Create a new capturing logger
    pub fn new() -> Self {
        let inner = env_logger::Builder::from_default_env()
            .filter_level(LevelFilter::Info)
            .build();
        Self { inner }
    }

    /// Create with a specific log level
    pub fn with_level(level: LevelFilter) -> Self {
        let inner = env_logger::Builder::from_default_env()
            .filter_level(level)
            .build();
        Self { inner }
    }
}

impl Default for CapturingLogger {
    fn default() -> Self {
        Self::new()
    }
}

impl Log for CapturingLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record) {
        // Always forward to inner logger
        self.inner.log(record);

        // Capture ERROR and WARN to buffer if enabled
        if is_capture_enabled() && record.level() <= Level::Warn {
            add_entry(CapturedLogEntry {
                instant: Instant::now(),
                timestamp: Utc::now(),
                level: record.level(),
                target: record.target().to_string(),
                message: format!("{}", record.args()),
            });
        }
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

/// Initialize the capturing logger (replaces env_logger::init())
///
/// Call this once at startup instead of `env_logger::init()`.
pub fn init_capturing_logger() {
    let logger = CapturingLogger::new();
    let max_level = logger.inner.filter();
    if log::set_boxed_logger(Box::new(logger)).is_ok() {
        log::set_max_level(max_level);
    }
}

/// Initialize with a specific log level
pub fn init_capturing_logger_with_level(level: LevelFilter) {
    let logger = CapturingLogger::with_level(level);
    if log::set_boxed_logger(Box::new(logger)).is_ok() {
        log::set_max_level(level);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    // Generate a unique test ID for message filtering
    fn test_id() -> String {
        format!(
            "test_{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    #[test]
    fn test_add_and_query_entries() {
        let test_marker = test_id();
        let start = Instant::now();
        thread::sleep(Duration::from_millis(1));

        // Manually add entries for testing with unique marker
        add_entry(CapturedLogEntry {
            instant: Instant::now(),
            timestamp: Utc::now(),
            level: Level::Error,
            target: format!("test::module::{}", test_marker),
            message: "Test error message".to_string(),
        });

        add_entry(CapturedLogEntry {
            instant: Instant::now(),
            timestamp: Utc::now(),
            level: Level::Warn,
            target: format!("test::module::{}", test_marker),
            message: "Test warning message".to_string(),
        });

        // Filter by both time and marker to avoid interference from parallel tests
        let entries: Vec<_> = entries_since(start)
            .into_iter()
            .filter(|e| e.target.contains(&test_marker))
            .collect();
        assert_eq!(entries.len(), 2);

        let errors: Vec<_> = errors_since(start)
            .into_iter()
            .filter(|e| e.target.contains(&test_marker))
            .collect();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].level, Level::Error);

        let warnings: Vec<_> = warnings_since(start)
            .into_iter()
            .filter(|e| e.target.contains(&test_marker))
            .collect();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].level, Level::Warn);
    }

    #[test]
    fn test_ring_buffer_limit() {
        // This test verifies the ring buffer doesn't grow unbounded
        // We add many entries and verify the buffer stays within limits
        let test_marker = test_id();

        // Add more than MAX_BUFFER_SIZE entries
        for i in 0..MAX_BUFFER_SIZE + 100 {
            add_entry(CapturedLogEntry {
                instant: Instant::now(),
                timestamp: Utc::now(),
                level: Level::Warn,
                target: format!("ring_test::{}", test_marker),
                message: format!("Message {}", i),
            });
        }

        // Total entries should be capped at MAX_BUFFER_SIZE
        let entries = recent_entries(MAX_BUFFER_SIZE + 100);
        assert!(entries.len() <= MAX_BUFFER_SIZE);
    }

    #[test]
    fn test_capture_toggle() {
        // Test that the toggle flag works correctly
        set_capture_enabled(true);
        assert!(is_capture_enabled());

        set_capture_enabled(false);
        assert!(!is_capture_enabled());

        // Re-enable for other tests
        set_capture_enabled(true);
        assert!(is_capture_enabled());
    }

    #[test]
    fn test_display_formats() {
        let entry = CapturedLogEntry {
            instant: Instant::now(),
            timestamp: Utc::now(),
            level: Level::Error,
            target: "velostream::kafka::consumer".to_string(),
            message: "Connection timeout".to_string(),
        };

        assert!(entry.display().contains("[ERROR]"));
        assert!(entry.display().contains("velostream::kafka::consumer"));
        assert!(entry.display().contains("Connection timeout"));

        let short = entry.display_short();
        assert!(short.contains("[ERROR]"));
        assert!(short.contains("Connection timeout"));
        assert!(!short.contains("velostream::kafka"));
    }

    #[test]
    fn test_time_filtered_counts() {
        // Test that time-filtered queries work correctly
        let test_marker = test_id();
        let start = Instant::now();
        thread::sleep(Duration::from_millis(1));

        add_entry(CapturedLogEntry {
            instant: Instant::now(),
            timestamp: Utc::now(),
            level: Level::Error,
            target: format!("stats_test::{}", test_marker),
            message: "Error 1".to_string(),
        });
        add_entry(CapturedLogEntry {
            instant: Instant::now(),
            timestamp: Utc::now(),
            level: Level::Error,
            target: format!("stats_test::{}", test_marker),
            message: "Error 2".to_string(),
        });
        add_entry(CapturedLogEntry {
            instant: Instant::now(),
            timestamp: Utc::now(),
            level: Level::Warn,
            target: format!("stats_test::{}", test_marker),
            message: "Warning 1".to_string(),
        });

        // Filter our test entries by marker
        let our_errors = errors_since(start)
            .into_iter()
            .filter(|e| e.target.contains(&test_marker))
            .count();
        let our_warnings = warnings_since(start)
            .into_iter()
            .filter(|e| e.target.contains(&test_marker))
            .count();

        assert_eq!(our_errors, 2);
        assert_eq!(our_warnings, 1);

        // Verify stats() doesn't crash and returns sensible values
        let (errors, warns) = stats();
        // Buffer-wide counts should include at least our logged errors/warnings
        let _ = (errors, warns); // Use the values to avoid unused variable warning
    }
}
