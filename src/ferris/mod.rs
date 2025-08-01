pub mod error;
pub mod kafka;
pub mod sql;

// Re-export modern error types for convenience
pub use error::{FerrisError, FerrisResult};
