//! Stream-Stream Join Execution
//!
//! This module implements stream-stream joins with temporal semantics.
//! It provides windowed join state management, concurrent source coordination,
//! and interval-based join processing.
//!
//! ## Architecture
//!
//! Stream-stream joins require:
//! 1. **Concurrent source reading** - Both streams are read simultaneously
//! 2. **Windowed state stores** - Buffer records from each side with time-based expiry
//! 3. **Join coordination** - Match records by key within time constraints
//! 4. **Watermark tracking** - Expire old state as time progresses
//!
//! ## Join Types Supported
//!
//! - **Interval Join**: Records match if their timestamps fall within a specified interval
//!   ```sql
//!   SELECT * FROM orders o JOIN shipments s
//!   ON o.order_id = s.order_id
//!   AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '24' HOUR
//!   ```
//!
//! - **Window Join**: Records match if they fall within the same time window
//!   ```sql
//!   SELECT * FROM orders o JOIN shipments s
//!   ON o.order_id = s.order_id
//!   WINDOW TUMBLING(INTERVAL '1' HOUR)
//!   ```
//!
//! ## Example
//!
//! ```rust,no_run
//! use velostream::velostream::sql::execution::join::{
//!     JoinStateStore, JoinConfig, JoinCoordinator, JoinSide,
//! };
//! use std::time::Duration;
//!
//! // Create state stores for each side
//! let retention = Duration::from_secs(86400); // 24 hours
//! let mut left_store = JoinStateStore::new(retention);
//! let mut right_store = JoinStateStore::new(retention);
//!
//! // Configure the join
//! let config = JoinConfig::interval(
//!     "orders",
//!     "shipments",
//!     vec![("order_id".to_string(), "order_id".to_string())],
//!     Duration::ZERO,              // lower bound
//!     Duration::from_secs(86400),  // upper bound (24 hours)
//! );
//! ```

mod coordinator;
mod key_extractor;
mod state_store;
mod watermark;

pub use coordinator::{
    JoinConfig, JoinCoordinator, JoinCoordinatorConfig, JoinCoordinatorStats, JoinSide, JoinType,
    MemoryPressure, MissingEventTimeBehavior,
};
pub use key_extractor::{JoinKeyExtractor, JoinKeyExtractorPair};
pub use state_store::{
    EvictionPolicy, JoinBufferEntry, JoinStateStats, JoinStateStore, JoinStateStoreConfig,
};
pub use watermark::{JoinWatermarkTracker, WatermarkConfig, WatermarkStats};
