/*!
# Query Tracker

Real-time query execution tracking for performance monitoring.
Provides lifecycle management and metrics collection during query execution.
*/

use super::{metrics::MemoryMetrics, query_performance::QueryPerformance};
use std::time::Instant;

/// Performance tracking token for query lifecycle
#[derive(Debug)]
pub struct QueryTracker {
    /// Query performance data
    pub performance: QueryPerformance,
    /// Current processor being tracked
    current_processor: Option<String>,
    /// Processor start time
    processor_start: Option<Instant>,
}

impl QueryTracker {
    /// Create new query tracker
    pub fn new(query_id: String, query_text: String) -> Self {
        Self {
            performance: QueryPerformance::new(query_id, query_text),
            current_processor: None,
            processor_start: None,
        }
    }

    /// Start tracking a specific processor
    pub fn start_processor(&mut self, processor_name: &str) {
        // Finish previous processor if any
        self.finish_current_processor();

        self.current_processor = Some(processor_name.to_string());
        self.processor_start = Some(Instant::now());
    }

    /// Finish tracking current processor
    pub fn finish_current_processor(&mut self) {
        if let (Some(processor), Some(start_time)) =
            (&self.current_processor, &self.processor_start)
        {
            let duration = start_time.elapsed();
            self.performance
                .processor_times
                .insert(processor.clone(), duration);
            self.current_processor = None;
            self.processor_start = None;
        }
    }

    /// Update memory metrics
    pub fn update_memory_metrics(&mut self, metrics: MemoryMetrics) {
        self.performance.memory_metrics = metrics;
    }

    /// Increment record count
    pub fn add_records_processed(&mut self, count: u64) {
        self.performance.records_processed += count;
    }

    /// Increment byte count
    pub fn add_bytes_processed(&mut self, bytes: u64) {
        self.performance.bytes_processed += bytes;
    }

    /// Get current processor being tracked
    pub fn current_processor(&self) -> Option<&str> {
        self.current_processor.as_deref()
    }

    /// Get elapsed time for current processor
    pub fn current_processor_elapsed(&self) -> Option<std::time::Duration> {
        self.processor_start.map(|start| start.elapsed())
    }

    /// Get total elapsed time since query started
    pub fn total_elapsed(&self) -> std::time::Duration {
        self.performance.started_at.elapsed()
    }

    /// Check if tracker is currently tracking a processor
    pub fn is_tracking_processor(&self) -> bool {
        self.current_processor.is_some()
    }

    /// Finish query tracking and return performance data
    pub fn finish(mut self) -> QueryPerformance {
        // Finish any current processor
        self.finish_current_processor();

        // Calculate total execution time
        self.performance.completed_at = Instant::now();
        self.performance.execution_time = self
            .performance
            .completed_at
            .duration_since(self.performance.started_at);

        self.performance
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_query_tracker_creation() {
        let tracker = QueryTracker::new("q1".to_string(), "SELECT * FROM test".to_string());
        assert_eq!(tracker.performance.query_id, "q1");
        assert_eq!(tracker.performance.query_text, "SELECT * FROM test");
        assert!(!tracker.is_tracking_processor());
    }

    #[test]
    fn test_processor_tracking() {
        let mut tracker = QueryTracker::new("q1".to_string(), "SELECT * FROM test".to_string());

        // Start tracking processor
        tracker.start_processor("SelectProcessor");
        assert!(tracker.is_tracking_processor());
        assert_eq!(tracker.current_processor(), Some("SelectProcessor"));

        // Small delay to ensure time passes
        thread::sleep(Duration::from_millis(1));

        // Finish processor
        tracker.finish_current_processor();
        assert!(!tracker.is_tracking_processor());
        assert!(tracker
            .performance
            .processor_times
            .contains_key("SelectProcessor"));
    }

    #[test]
    fn test_multiple_processors() {
        let mut tracker = QueryTracker::new("q1".to_string(), "SELECT * FROM test".to_string());

        // Track first processor
        tracker.start_processor("SelectProcessor");
        thread::sleep(Duration::from_millis(1));

        // Track second processor (should finish first automatically)
        tracker.start_processor("FilterProcessor");
        thread::sleep(Duration::from_millis(1));

        tracker.finish_current_processor();

        // Should have both processors tracked
        assert!(tracker
            .performance
            .processor_times
            .contains_key("SelectProcessor"));
        assert!(tracker
            .performance
            .processor_times
            .contains_key("FilterProcessor"));
    }

    #[test]
    fn test_metrics_updates() {
        let mut tracker = QueryTracker::new("q1".to_string(), "SELECT * FROM test".to_string());

        tracker.add_records_processed(100);
        tracker.add_bytes_processed(1000);

        let mut memory_metrics = MemoryMetrics::default();
        memory_metrics.allocated_bytes = 2048;
        tracker.update_memory_metrics(memory_metrics);

        assert_eq!(tracker.performance.records_processed, 100);
        assert_eq!(tracker.performance.bytes_processed, 1000);
        assert_eq!(tracker.performance.memory_metrics.allocated_bytes, 2048);
    }

    #[test]
    fn test_finish_tracking() {
        let mut tracker = QueryTracker::new("q1".to_string(), "SELECT * FROM test".to_string());

        tracker.start_processor("SelectProcessor");
        thread::sleep(Duration::from_millis(2));

        let performance = tracker.finish();

        assert!(performance.execution_time > Duration::from_millis(1));
        assert!(performance.processor_times.contains_key("SelectProcessor"));
    }

    #[test]
    fn test_elapsed_time_tracking() {
        let mut tracker = QueryTracker::new("q1".to_string(), "SELECT * FROM test".to_string());

        thread::sleep(Duration::from_millis(1));

        tracker.start_processor("SelectProcessor");
        thread::sleep(Duration::from_millis(1));

        assert!(tracker.total_elapsed() > Duration::from_millis(1));
        assert!(tracker.current_processor_elapsed().unwrap() > Duration::from_nanos(1));
    }
}
