/*!
# Watermark Management for Event-Time Processing

Watermarks are critical for streaming systems to handle out-of-order events and determine
when it's safe to emit results for time-based windows. This module provides a flexible,
optional watermark system that integrates with existing FerrisStreams architecture.

## Key Concepts

**Watermark**: A timestamp indicating that no more events with timestamps less than or
equal to this value will arrive. This allows the system to safely close time windows
and emit results.

**Event-Time vs Processing-Time**:
- Event-time: When the event actually occurred (from `event_time` field)  
- Processing-time: When FerrisStreams processed the record (from `timestamp` field)

## Design Philosophy

This watermark system follows Phase 1B requirements:
- **Optional Component**: Only activated when explicitly configured
- **Backward Compatible**: Existing code works without watermarks
- **Progressive Enhancement**: Can be enabled via StreamingConfig
- **Industry Standard**: Follows Apache Flink/Beam watermark semantics
*/

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::ferris::sql::execution::types::StreamRecord;

/// Manages watermarks for event-time processing across multiple sources
/// 
/// The WatermarkManager tracks event-time progress per source and generates
/// system-wide watermarks for safe window processing. This is an optional
/// component that only activates when event-time processing is enabled.
#[derive(Debug)]
pub struct WatermarkManager {
    /// Per-source watermark tracking
    source_watermarks: Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
    
    /// Global watermark (minimum of all sources)
    global_watermark: Arc<RwLock<Option<DateTime<Utc>>>>,
    
    /// Maximum allowed lateness for out-of-order events
    allowed_lateness: Duration,
    
    /// Strategy for generating watermarks
    strategy: WatermarkStrategy,
    
    /// Whether watermark management is enabled
    enabled: bool,
}

/// Strategy for generating watermarks from event streams
#[derive(Clone)]
pub enum WatermarkStrategy {
    /// Generate watermarks periodically based on observed event times
    /// with a fixed lag to account for typical out-of-order arrival
    BoundedOutOfOrderness {
        /// Maximum expected delay for out-of-order events
        max_out_of_orderness: Duration,
        
        /// How often to generate watermark updates
        watermark_interval: Duration,
    },
    
    /// Generate watermarks based on punctuated events in the stream
    /// (e.g., special marker records that indicate event-time progress)
    Punctuated,
    
    /// Ascending timestamps - events arrive in event-time order
    /// (optimized case with minimal latency)
    Ascending,
    
    /// Custom watermark generation logic
    Custom(Arc<dyn CustomWatermarkGenerator + Send + Sync>),
}

impl std::fmt::Debug for WatermarkStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BoundedOutOfOrderness { max_out_of_orderness, watermark_interval } => {
                f.debug_struct("BoundedOutOfOrderness")
                    .field("max_out_of_orderness", max_out_of_orderness)
                    .field("watermark_interval", watermark_interval)
                    .finish()
            }
            Self::Punctuated => write!(f, "Punctuated"),
            Self::Ascending => write!(f, "Ascending"),
            Self::Custom(_) => write!(f, "Custom"),
        }
    }
}

/// Trait for implementing custom watermark generation logic
pub trait CustomWatermarkGenerator {
    /// Generate a watermark for a source based on the current record
    fn generate_watermark(
        &self,
        source_id: &str,
        record: &StreamRecord,
        current_watermark: Option<DateTime<Utc>>,
    ) -> Option<DateTime<Utc>>;
}

/// Strategy for handling late data (events arriving after watermark)
#[derive(Clone)]
pub enum LateDataStrategy {
    /// Drop late records silently
    Drop,
    
    /// Include late records in the next window
    IncludeInNextWindow,
    
    /// Send late records to a dead letter queue for manual handling
    DeadLetterQueue { queue_name: String },
    
    /// Update previous window results if still possible
    /// (requires stateful window storage)
    UpdatePreviousWindow { grace_period: Duration },
    
    /// Custom late data handling
    Custom(Arc<dyn LateDataHandler + Send + Sync>),
}

impl std::fmt::Debug for LateDataStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Drop => write!(f, "Drop"),
            Self::IncludeInNextWindow => write!(f, "IncludeInNextWindow"),
            Self::DeadLetterQueue { queue_name } => {
                f.debug_struct("DeadLetterQueue")
                    .field("queue_name", queue_name)
                    .finish()
            }
            Self::UpdatePreviousWindow { grace_period } => {
                f.debug_struct("UpdatePreviousWindow")
                    .field("grace_period", grace_period)
                    .finish()
            }
            Self::Custom(_) => write!(f, "Custom"),
        }
    }
}

/// Trait for implementing custom late data handling
pub trait LateDataHandler {
    /// Handle a late record that arrived after the watermark
    fn handle_late_record(
        &self,
        record: StreamRecord,
        watermark: DateTime<Utc>,
        lateness: Duration,
    ) -> LateDataAction;
}

/// Action to take for a late record
#[derive(Debug)]
pub enum LateDataAction {
    /// Process the record normally (include in current window)
    Process,
    
    /// Drop the record
    Drop,
    
    /// Send to dead letter queue
    DeadLetter,
    
    /// Update a previous window with this record
    UpdatePrevious { window_end: DateTime<Utc> },
}

/// Watermark event emitted when watermarks advance
#[derive(Debug, Clone)]
pub struct WatermarkEvent {
    /// Source that triggered the watermark update
    pub source_id: String,
    
    /// New watermark timestamp
    pub watermark: DateTime<Utc>,
    
    /// Previous watermark (if any)
    pub previous_watermark: Option<DateTime<Utc>>,
    
    /// When this watermark was generated
    pub generated_at: DateTime<Utc>,
}

impl Default for WatermarkManager {
    fn default() -> Self {
        Self::new(WatermarkStrategy::BoundedOutOfOrderness {
            max_out_of_orderness: Duration::from_secs(5),
            watermark_interval: Duration::from_secs(1),
        })
    }
}

impl WatermarkManager {
    /// Create a new WatermarkManager with the specified strategy
    pub fn new(strategy: WatermarkStrategy) -> Self {
        Self {
            source_watermarks: Arc::new(RwLock::new(HashMap::new())),
            global_watermark: Arc::new(RwLock::new(None)),
            allowed_lateness: Duration::from_secs(60), // Default 1 minute
            strategy,
            enabled: false, // Disabled by default for backward compatibility
        }
    }
    
    /// Create a new WatermarkManager with custom allowed lateness
    pub fn with_allowed_lateness(strategy: WatermarkStrategy, allowed_lateness: Duration) -> Self {
        Self {
            source_watermarks: Arc::new(RwLock::new(HashMap::new())),
            global_watermark: Arc::new(RwLock::new(None)),
            allowed_lateness,
            strategy,
            enabled: false,
        }
    }
    
    /// Enable watermark management (required for event-time processing)
    pub fn enable(&mut self) -> &mut Self {
        self.enabled = true;
        self
    }
    
    /// Set the maximum allowed lateness for late data
    pub fn set_allowed_lateness(mut self, lateness: Duration) -> Self {
        self.allowed_lateness = lateness;
        self
    }
    
    /// Check if watermark management is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
    
    /// Update watermark for a specific source based on a new record
    pub fn update_watermark(
        &self,
        source_id: &str,
        record: &StreamRecord,
    ) -> Option<WatermarkEvent> {
        if !self.enabled {
            return None;
        }
        
        // Extract event time from record, fallback to processing time
        let event_time = record.event_time.unwrap_or_else(|| {
            DateTime::from_timestamp_millis(record.timestamp)
                .unwrap_or_else(|| Utc::now())
        });
        
        let new_watermark = self.generate_watermark_for_source(source_id, record, event_time);
        
        if let Some(watermark) = new_watermark {
            let previous = self.set_source_watermark(source_id, watermark);
            self.update_global_watermark();
            
            return Some(WatermarkEvent {
                source_id: source_id.to_string(),
                watermark,
                previous_watermark: previous,
                generated_at: Utc::now(),
            });
        }
        
        None
    }
    
    /// Get the current global watermark (minimum across all sources)
    pub fn get_global_watermark(&self) -> Option<DateTime<Utc>> {
        self.global_watermark.read().ok().and_then(|w| *w)
    }
    
    /// Get watermark for a specific source
    pub fn get_source_watermark(&self, source_id: &str) -> Option<DateTime<Utc>> {
        self.source_watermarks
            .read()
            .ok()?
            .get(source_id)
            .copied()
    }
    
    /// Check if a record is late based on current watermark
    pub fn is_late(&self, record: &StreamRecord) -> bool {
        if !self.enabled {
            return false; // No late data when watermarks are disabled
        }
        
        if let Some(global_watermark) = self.get_global_watermark() {
            let event_time = record.event_time.unwrap_or_else(|| {
                DateTime::from_timestamp_millis(record.timestamp)
                    .unwrap_or_else(|| Utc::now())
            });
            
            event_time < global_watermark
        } else {
            false // No watermark yet, so nothing is late
        }
    }
    
    /// Calculate how late a record is
    pub fn calculate_lateness(&self, record: &StreamRecord) -> Option<Duration> {
        if !self.enabled {
            return None;
        }
        
        let global_watermark = self.get_global_watermark()?;
        let event_time = record.event_time.unwrap_or_else(|| {
            DateTime::from_timestamp_millis(record.timestamp)
                .unwrap_or_else(|| Utc::now())
        });
        
        if event_time < global_watermark {
            Some((global_watermark - event_time).to_std().unwrap_or(Duration::ZERO))
        } else {
            None
        }
    }
    
    /// Get all sources being tracked
    pub fn get_tracked_sources(&self) -> Vec<String> {
        self.source_watermarks
            .read()
            .map(|sources| sources.keys().cloned().collect())
            .unwrap_or_default()
    }
    
    /// Get the allowed lateness duration
    pub fn get_allowed_lateness(&self) -> Duration {
        self.allowed_lateness
    }
    
    /// Check if a record is within the allowed lateness threshold
    pub fn is_within_allowed_lateness(&self, record: &StreamRecord) -> bool {
        if !self.enabled {
            return true; // No watermarks = no late data
        }
        
        let lateness = self.calculate_lateness(record);
        match lateness {
            Some(duration) => duration <= self.allowed_lateness,
            None => true, // Not late if no lateness calculated
        }
    }
    
    /// Determine the action to take for a late record
    pub fn determine_late_data_action(
        &self,
        record: &StreamRecord,
        strategy: &LateDataStrategy,
    ) -> LateDataAction {
        if !self.is_late(record) {
            return LateDataAction::Process;
        }
        
        match strategy {
            LateDataStrategy::Drop => LateDataAction::Drop,
            
            LateDataStrategy::IncludeInNextWindow => LateDataAction::Process,
            
            LateDataStrategy::DeadLetterQueue { .. } => LateDataAction::DeadLetter,
            
            LateDataStrategy::UpdatePreviousWindow { grace_period } => {
                let lateness = self.calculate_lateness(record)
                    .unwrap_or(Duration::ZERO);
                    
                if lateness <= *grace_period {
                    // Calculate which window this record should have belonged to
                    let event_time = record.event_time.unwrap_or_else(|| {
                        DateTime::from_timestamp_millis(record.timestamp)
                            .unwrap_or_else(|| Utc::now())
                    });
                    LateDataAction::UpdatePrevious { window_end: event_time }
                } else {
                    LateDataAction::Drop // Too late even for grace period
                }
            }
            
            LateDataStrategy::Custom(handler) => {
                let lateness = self.calculate_lateness(record)
                    .unwrap_or(Duration::ZERO);
                let watermark = self.get_global_watermark()
                    .unwrap_or_else(|| Utc::now());
                    
                handler.handle_late_record(record.clone(), watermark, lateness)
            }
        }
    }
    
    // Private helper methods
    
    fn generate_watermark_for_source(
        &self,
        source_id: &str,
        record: &StreamRecord,
        event_time: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        match &self.strategy {
            WatermarkStrategy::BoundedOutOfOrderness { max_out_of_orderness, .. } => {
                // Generate watermark by subtracting max out-of-orderness from event time
                Some(event_time - chrono::Duration::from_std(*max_out_of_orderness).ok()?)
            }
            
            WatermarkStrategy::Ascending => {
                // For ascending timestamps, watermark can be very close to event time
                Some(event_time - chrono::Duration::milliseconds(1))
            }
            
            WatermarkStrategy::Punctuated => {
                // Look for punctuation markers in the record
                // This would typically check record headers or special fields
                if record.headers.contains_key("watermark") {
                    Some(event_time)
                } else {
                    None
                }
            }
            
            WatermarkStrategy::Custom(generator) => {
                let current_watermark = self.get_source_watermark(source_id);
                generator.generate_watermark(source_id, record, current_watermark)
            }
        }
    }
    
    fn set_source_watermark(
        &self,
        source_id: &str,
        new_watermark: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        if let Ok(mut sources) = self.source_watermarks.write() {
            let previous = sources.get(source_id).copied();
            
            // Only update if new watermark is later than current
            if let Some(current) = previous {
                if new_watermark > current {
                    sources.insert(source_id.to_string(), new_watermark);
                }
            } else {
                sources.insert(source_id.to_string(), new_watermark);
            }
            
            previous
        } else {
            None
        }
    }
    
    fn update_global_watermark(&self) {
        if let (Ok(sources), Ok(mut global)) = (
            self.source_watermarks.read(),
            self.global_watermark.write(),
        ) {
            // Global watermark is the minimum of all source watermarks
            let min_watermark = sources.values().min().copied();
            *global = min_watermark;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_record_with_event_time(event_time: DateTime<Utc>) -> StreamRecord {
        StreamRecord {
            fields: HashMap::new(),
            headers: HashMap::new(),
            timestamp: Utc::now().timestamp_millis(),
            offset: 0,
            partition: 0,
            event_time: Some(event_time),
        }
    }

    #[test]
    fn test_watermark_manager_disabled_by_default() {
        let manager = WatermarkManager::default();
        assert!(!manager.is_enabled());
        
        let record = create_test_record_with_event_time(Utc::now());
        assert!(manager.update_watermark("test_source", &record).is_none());
    }

    #[test]
    fn test_watermark_manager_enabled() {
        let mut manager = WatermarkManager::default();
        manager.enable();
        assert!(manager.is_enabled());
        
        let event_time = Utc::now() - chrono::Duration::seconds(10);
        let record = create_test_record_with_event_time(event_time);
        
        let event = manager.update_watermark("test_source", &record);
        assert!(event.is_some());
        
        let watermark = manager.get_source_watermark("test_source");
        assert!(watermark.is_some());
    }

    #[test]
    fn test_late_data_detection() {
        let mut manager = WatermarkManager::default();
        manager.enable();
        
        // Add a record to establish watermark
        let early_time = Utc::now() - chrono::Duration::seconds(30);
        let early_record = create_test_record_with_event_time(early_time);
        manager.update_watermark("source1", &early_record);
        
        // Create a late record
        let late_time = Utc::now() - chrono::Duration::seconds(60);
        let late_record = create_test_record_with_event_time(late_time);
        
        assert!(manager.is_late(&late_record));
        
        let lateness = manager.calculate_lateness(&late_record);
        assert!(lateness.is_some());
        assert!(lateness.unwrap() > Duration::from_secs(25));
    }

    #[test]
    fn test_multiple_sources_global_watermark() {
        let mut manager = WatermarkManager::default();
        manager.enable();
        
        let time1 = Utc::now() - chrono::Duration::seconds(10);
        let time2 = Utc::now() - chrono::Duration::seconds(20); // Earlier time
        
        let record1 = create_test_record_with_event_time(time1);
        let record2 = create_test_record_with_event_time(time2);
        
        manager.update_watermark("fast_source", &record1);
        manager.update_watermark("slow_source", &record2);
        
        // Global watermark should be the minimum (from slow_source)
        let global = manager.get_global_watermark().unwrap();
        let source2_watermark = manager.get_source_watermark("slow_source").unwrap();
        
        // Global should be based on the slower source
        assert!(global <= source2_watermark);
    }
}