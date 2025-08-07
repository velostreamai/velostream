//! Multi-Job SQL Server Core Library
//!
//! This module contains the core MultiJobSqlServer implementation that can be
//! used both as a library and in the binary application.

use crate::ferris::sql::{
    SqlApplicationParser, SqlError, StreamExecutionEngine, StreamingSqlParser,
};
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;

/// Multi-Job SQL Server that can run multiple SQL jobs concurrently
#[derive(Clone)]
pub struct MultiJobSqlServer {
    jobs: Arc<RwLock<HashMap<String, RunningJob>>>,
    brokers: String,
    base_group_id: String,
    max_jobs: usize,
    job_counter: Arc<Mutex<u64>>,
}

/// Represents a running SQL job with all its metadata and execution context
#[derive(Debug)]
pub struct RunningJob {
    pub name: String,
    pub version: String,
    pub query: String,
    pub topic: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub execution_handle: JoinHandle<()>,
    pub shutdown_sender: mpsc::UnboundedSender<()>,
    pub metrics: JobMetrics,
}

/// Job execution status
#[derive(Clone, Debug, PartialEq)]
pub enum JobStatus {
    Starting,
    Running,
    Stopped,
    Failed(String),
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Starting => write!(f, "Starting"),
            JobStatus::Running => write!(f, "Running"),
            JobStatus::Stopped => write!(f, "Stopped"),
            JobStatus::Failed(err) => write!(f, "Failed: {}", err),
        }
    }
}

/// Job performance metrics
#[derive(Debug, Clone)]
pub struct JobMetrics {
    pub records_processed: u64,
    pub records_per_second: f64,
    pub errors_count: u64,
    pub last_processed: Option<chrono::DateTime<chrono::Utc>>,
}

impl Default for JobMetrics {
    fn default() -> Self {
        Self {
            records_processed: 0,
            records_per_second: 0.0,
            errors_count: 0,
            last_processed: None,
        }
    }
}

/// Summary information about a job for API responses
#[derive(Debug, Clone)]
pub struct JobSummary {
    pub name: String,
    pub version: String,
    pub status: JobStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub metrics: JobMetrics,
}

impl MultiJobSqlServer {
    /// Create a new MultiJobSqlServer instance
    pub fn new(brokers: String, base_group_id: String, max_jobs: usize) -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            brokers,
            base_group_id,
            max_jobs,
            job_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Deploy a new SQL job
    pub async fn deploy_job(
        &self,
        name: String,
        version: String,
        query: String,
        topic: String,
    ) -> Result<(), SqlError> {
        // Input validation
        if name.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Job name cannot be empty".to_string(),
                query: None,
            });
        }

        if version.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Job version cannot be empty".to_string(),
                query: None,
            });
        }

        if query.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Job query cannot be empty".to_string(),
                query: Some(query),
            });
        }

        if topic.is_empty() {
            return Err(SqlError::ExecutionError {
                message: "Job topic cannot be empty".to_string(),
                query: None,
            });
        }

        info!(
            "Deploying job '{}' version '{}' on topic '{}': {}",
            name, version, topic, query
        );

        // Check if we're at max capacity
        let jobs = self.jobs.read().await;
        if jobs.len() >= self.max_jobs {
            return Err(SqlError::ExecutionError {
                message: format!("Maximum jobs limit reached ({})", self.max_jobs),
                query: None,
            });
        }

        // Check if job already exists
        if jobs.contains_key(&name) {
            return Err(SqlError::ExecutionError {
                message: format!("Job '{}' already exists", name),
                query: None,
            });
        }
        drop(jobs);

        // Parse and validate the query
        let parser = StreamingSqlParser::new();
        let _parsed_query = parser.parse(&query)?;

        // Generate unique consumer group ID
        let mut counter = self.job_counter.lock().await;
        *counter += 1;
        let group_id = format!("{}-job-{}-{}", self.base_group_id, name, *counter);
        drop(counter);

        // Create shutdown channel
        let (shutdown_sender, mut shutdown_receiver) = mpsc::unbounded_channel();

        // Create job execution task (mock implementation for testing)
        let job_name_clone = name.clone();
        let query_clone = query.clone();
        let execution_handle = tokio::spawn(async move {
            info!("Starting job execution for '{}'", job_name_clone);

            // Mock job execution - in real implementation this would:
            // 1. Create Kafka consumer with group_id
            // 2. Set up stream execution engine
            // 3. Process messages continuously
            // 4. Update metrics

            loop {
                tokio::select! {
                    _ = shutdown_receiver.recv() => {
                        info!("Job '{}' received shutdown signal", job_name_clone);
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // Simulate processing work
                        continue;
                    }
                }
            }

            info!("Job '{}' execution completed", job_name_clone);
        });

        // Create and store the running job
        let now = chrono::Utc::now();
        let running_job = RunningJob {
            name: name.clone(),
            version,
            query,
            topic,
            status: JobStatus::Starting,
            created_at: now,
            updated_at: now,
            execution_handle,
            shutdown_sender,
            metrics: JobMetrics::default(),
        };

        // Insert the job
        let mut jobs = self.jobs.write().await;
        jobs.insert(name.clone(), running_job);
        drop(jobs);

        // Update status to running after a brief delay (simulate startup)
        let jobs_clone = self.jobs.clone();
        let name_clone = name.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut jobs = jobs_clone.write().await;
            if let Some(job) = jobs.get_mut(&name_clone) {
                job.status = JobStatus::Running;
                job.updated_at = chrono::Utc::now();
            }
        });

        info!("Successfully deployed job '{}'", name);
        Ok(())
    }

    /// Stop a running job
    pub async fn stop_job(&self, name: &str) -> Result<(), SqlError> {
        info!("Stopping job '{}'", name);

        let mut jobs = self.jobs.write().await;

        if let Some(mut job) = jobs.remove(name) {
            // Send shutdown signal
            if job.shutdown_sender.send(()).is_err() {
                warn!("Failed to send shutdown signal to job '{}'", name);
            }

            // Abort the execution task
            job.execution_handle.abort();

            // Update status
            job.status = JobStatus::Stopped;
            job.updated_at = chrono::Utc::now();

            info!("Successfully stopped job '{}'", name);
            Ok(())
        } else {
            Err(SqlError::ExecutionError {
                message: format!("Job '{}' not found", name),
                query: None,
            })
        }
    }

    // Note: pause_job functionality has been removed as it was not properly implemented
    // Future implementation should include proper job control channels and execution loop modifications

    /// List all jobs
    pub async fn list_jobs(&self) -> Vec<JobSummary> {
        let jobs = self.jobs.read().await;

        jobs.values()
            .map(|job| JobSummary {
                name: job.name.clone(),
                version: job.version.clone(),
                status: job.status.clone(),
                created_at: job.created_at,
                updated_at: job.updated_at,
                metrics: job.metrics.clone(),
            })
            .collect()
    }

    /// Get status of a specific job
    pub async fn get_job_status(&self, name: &str) -> Option<JobSummary> {
        let jobs = self.jobs.read().await;

        jobs.get(name).map(|job| JobSummary {
            name: job.name.clone(),
            version: job.version.clone(),
            status: job.status.clone(),
            created_at: job.created_at,
            updated_at: job.updated_at,
            metrics: job.metrics.clone(),
        })
    }

    /// Deploy a complete SQL application with multiple jobs
    pub async fn deploy_sql_application(
        &self,
        sql_content: &str,
        default_topic: Option<String>,
    ) -> Result<usize, SqlError> {
        info!("Deploying SQL application");

        let mut parser = SqlApplicationParser::new();
        let app = parser.parse_application(sql_content)?;

        info!(
            "Parsed SQL application '{}' version '{}' with {} jobs",
            app.metadata.name,
            app.metadata.version,
            app.resources.jobs.len()
        );

        let mut deployed_count = 0;
        let mut failed_jobs = Vec::new();

        for job_name in &app.resources.jobs {
            // In a real implementation, we would extract the actual SQL query for each job
            // For now, we'll use a placeholder query
            let mock_query = format!(
                "SELECT * FROM {}",
                default_topic.as_deref().unwrap_or("default_topic")
            );
            let topic = default_topic
                .as_deref()
                .unwrap_or("default_topic")
                .to_string();

            match self
                .deploy_job(
                    job_name.clone(),
                    app.metadata.version.clone(),
                    mock_query,
                    topic,
                )
                .await
            {
                Ok(_) => {
                    deployed_count += 1;
                    info!("Successfully deployed job '{}'", job_name);
                }
                Err(e) => {
                    error!("Failed to deploy job '{}': {}", job_name, e);
                    failed_jobs.push(job_name.clone());
                }
            }
        }

        if !failed_jobs.is_empty() {
            return Err(SqlError::ExecutionError {
                message: format!(
                    "Failed to deploy {} jobs: {:?}",
                    failed_jobs.len(),
                    failed_jobs
                ),
                query: None,
            });
        }

        info!(
            "Successfully deployed SQL application with {} jobs",
            deployed_count
        );

        Ok(deployed_count)
    }

    /// Get current job count
    pub async fn job_count(&self) -> usize {
        let jobs = self.jobs.read().await;
        jobs.len()
    }

    /// Get maximum jobs limit
    pub fn max_jobs(&self) -> usize {
        self.max_jobs
    }

    /// Get brokers configuration
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// Get base group ID
    pub fn base_group_id(&self) -> &str {
        &self.base_group_id
    }
}
