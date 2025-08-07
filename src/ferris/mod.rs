pub mod error;
pub mod kafka;
pub mod multi_job_server;
pub mod sql;

// Re-export modern error types for convenience
pub use error::{FerrisError, FerrisResult};

// Re-export multi-job server types
pub use multi_job_server::{JobMetrics, JobStatus, JobSummary, MultiJobSqlServer, RunningJob};
