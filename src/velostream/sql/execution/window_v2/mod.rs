//! Window Processing V2 - Trait-Based Architecture
//!
//! This module implements a refactored window processing system using trait-based
//! strategy patterns for improved performance and maintainability.
//!
//! ## Architecture Overview
//!
//! - **Traits**: WindowStrategy, EmissionStrategy, GroupByStrategy
//! - **Zero-Copy**: Arc<StreamRecord> for shared ownership
//! - **Pluggable**: Different strategies for different window types
//! - **Performance**: 3-5x improvement target (50-75K rec/sec)
//!
//! ## Phase 2A Goals
//!
//! - Eliminate clone() overhead via Arc<StreamRecord>
//! - Trait-based window strategies (TUMBLING, SLIDING, SESSION, ROWS)
//! - Ring buffer for efficient SLIDING windows
//! - Backward compatible with existing window.rs
//!
//! ## Feature Flags
//!
//! This module is feature-flagged to allow gradual migration:
//! - `#[cfg(feature = "window-v2")]` - Enable new architecture
//! - Default: Uses existing window.rs implementation

pub mod adapter;
pub mod emission;
pub mod strategies;
pub mod traits;
pub mod types;

// Re-export key types and traits
pub use adapter::WindowAdapter;
pub use traits::{EmissionStrategy, WindowStrategy};
pub use types::SharedRecord;
