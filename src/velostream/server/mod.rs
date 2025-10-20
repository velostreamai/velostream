//! StreamJobServer - Concurrent streaming SQL job execution
//!
//! This module provides a production-ready streaming SQL engine that can execute
//! multiple concurrent SQL jobs with full isolation. Each job runs in its own
//! dedicated streaming engine with separate memory pools, state management, and
//! resource allocation.
//!
//! ## Architecture
//!
//! - **StreamJobServer**: Main server orchestrating multiple streaming jobs
//! - **Job Processors**: Different execution strategies (Simple, Transactional)  
//! - **Resource Isolation**: Per-job memory limits, consumer groups, and state
//! - **Configuration**: Flexible job configuration with failure strategies
//!
//! ## Usage
//!
//! ```rust,no_run
//! use velostream::velostream::server::stream_job_server::StreamJobServer;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let server = StreamJobServer::new("localhost:9092".to_string(), "myapp".to_string(), 10);
//! server.deploy_job("job1".to_string(), "1.0".to_string(),
//!                  "SELECT * FROM events".to_string(), "events".to_string()).await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod dependency_graph;
pub mod graceful_degradation;
pub mod health_dashboard;
pub mod job_manager;
pub mod metrics;
pub mod observability_config_extractor;
pub mod parallel_loader;
pub mod processors;
pub mod progress_monitoring;
pub mod progress_streaming;
pub mod stream_job_server;
pub mod table_registry;

// Re-exports for convenience
