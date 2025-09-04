pub mod datasource;
pub mod error;
pub mod kafka;
pub mod schema;
pub mod serialization;
pub mod server;
pub mod sql;

// Legacy support - TODO: Remove after migration
pub mod modern_multi_job_server;

// Re-export server types
pub use server::{
    JobProcessingConfig, SimpleJobProcessor, StreamJobServer, TransactionalJobProcessor,
};

// Legacy re-exports for backward compatibility - TODO: Remove after migration
pub use modern_multi_job_server::{
    JobMetrics, JobStatus, JobSummary, MultiJobSqlServer, RunningJob,
};
