//! Progress Streaming System for Real-Time Table Loading Updates
//!
//! Provides streaming APIs for real-time progress monitoring, including WebSocket
//! connections, Server-Sent Events (SSE), and programmatic streaming interfaces.

use crate::velostream::server::progress_monitoring::{
    LoadingSummary, ProgressMonitor, TableLoadProgress,
};
use crate::velostream::server::table_registry::TableRegistry;
use futures::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{Instant, interval};

/// Progress streaming configuration
#[derive(Debug, Clone)]
pub struct ProgressStreamingConfig {
    /// Interval for sending periodic progress updates
    pub update_interval: Duration,

    /// Maximum number of concurrent streaming connections
    pub max_connections: usize,

    /// Buffer size for progress update channels
    pub buffer_size: usize,

    /// Whether to include detailed progress information
    pub include_detailed_progress: bool,

    /// Whether to include ETA calculations
    pub include_eta: bool,
}

impl Default for ProgressStreamingConfig {
    fn default() -> Self {
        Self {
            update_interval: Duration::from_millis(1000), // 1 second updates
            max_connections: 100,
            buffer_size: 1000,
            include_detailed_progress: true,
            include_eta: true,
        }
    }
}

/// Progress event types for streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ProgressEvent {
    /// Initial snapshot of all table progress
    InitialSnapshot {
        tables: HashMap<String, TableLoadProgress>,
        summary: LoadingSummary,
        timestamp: chrono::DateTime<chrono::Utc>,
    },

    /// Update for a specific table
    TableUpdate {
        table_name: String,
        progress: TableLoadProgress,
        timestamp: chrono::DateTime<chrono::Utc>,
    },

    /// Summary update with overall statistics
    SummaryUpdate {
        summary: LoadingSummary,
        timestamp: chrono::DateTime<chrono::Utc>,
    },

    /// Table loading completed
    TableCompleted {
        table_name: String,
        final_progress: TableLoadProgress,
        timestamp: chrono::DateTime<chrono::Utc>,
    },

    /// Table loading failed
    TableFailed {
        table_name: String,
        error_message: String,
        final_progress: Option<TableLoadProgress>,
        timestamp: chrono::DateTime<chrono::Utc>,
    },

    /// Keep-alive message to maintain connection
    KeepAlive {
        timestamp: chrono::DateTime<chrono::Utc>,
    },
}

/// Progress streaming server for managing multiple client connections
pub struct ProgressStreamingServer {
    /// Table registry for progress monitoring
    table_registry: Arc<TableRegistry>,

    /// Streaming configuration
    config: ProgressStreamingConfig,

    /// Broadcast channel for distributing progress events
    event_sender: broadcast::Sender<ProgressEvent>,

    /// Connection counter for managing max connections
    connection_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl ProgressStreamingServer {
    /// Create a new progress streaming server
    pub fn new(table_registry: Arc<TableRegistry>, config: ProgressStreamingConfig) -> Self {
        let (event_sender, _) = broadcast::channel(config.buffer_size);

        Self {
            table_registry,
            config,
            event_sender,
            connection_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Start the background progress monitoring task
    pub async fn start_monitoring(&self) -> tokio::task::JoinHandle<()> {
        let table_registry = Arc::clone(&self.table_registry);
        let event_sender = self.event_sender.clone();
        let update_interval = self.config.update_interval;
        let include_eta = self.config.include_eta;

        tokio::spawn(async move {
            let mut interval = interval(update_interval);
            let mut last_summary = LoadingSummary::default();

            loop {
                interval.tick().await;

                // Get current progress for all tables
                let current_progress = table_registry.get_loading_progress().await;
                let current_summary = table_registry.get_loading_summary().await;

                // Send individual table updates
                for (table_name, progress) in &current_progress {
                    let event = ProgressEvent::TableUpdate {
                        table_name: table_name.clone(),
                        progress: progress.clone(),
                        timestamp: chrono::Utc::now(),
                    };

                    let _ = event_sender.send(event);
                }

                // Send summary update if it changed significantly
                if Self::summary_changed_significantly(&last_summary, &current_summary) {
                    let event = ProgressEvent::SummaryUpdate {
                        summary: current_summary.clone(),
                        timestamp: chrono::Utc::now(),
                    };

                    let _ = event_sender.send(event);
                    last_summary = current_summary;
                }

                // Send keep-alive if no progress updates
                if current_progress.is_empty() {
                    let event = ProgressEvent::KeepAlive {
                        timestamp: chrono::Utc::now(),
                    };

                    let _ = event_sender.send(event);
                }
            }
        })
    }

    /// Create a new streaming connection for progress updates
    pub async fn create_stream(&self) -> Result<ProgressStream, ProgressStreamingError> {
        // Check connection limit
        let current_connections = self
            .connection_count
            .load(std::sync::atomic::Ordering::Relaxed);
        if current_connections >= self.config.max_connections {
            return Err(ProgressStreamingError::TooManyConnections {
                max_connections: self.config.max_connections,
                current_connections,
            });
        }

        // Increment connection counter
        self.connection_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Create receiver for progress events
        let event_receiver = self.event_sender.subscribe();

        // Send initial snapshot
        let initial_progress = self.table_registry.get_loading_progress().await;
        let initial_summary = self.table_registry.get_loading_summary().await;

        let initial_event = ProgressEvent::InitialSnapshot {
            tables: initial_progress,
            summary: initial_summary,
            timestamp: chrono::Utc::now(),
        };

        // Create the stream
        let stream = ProgressStream::new(
            event_receiver,
            initial_event,
            Arc::clone(&self.connection_count),
        );

        Ok(stream)
    }

    /// Create a filtered stream for specific tables only
    pub async fn create_filtered_stream(
        &self,
        table_names: Vec<String>,
    ) -> Result<ProgressStream, ProgressStreamingError> {
        let mut stream = self.create_stream().await?;
        stream.set_table_filter(table_names);
        Ok(stream)
    }

    /// Broadcast a custom progress event
    pub async fn broadcast_event(
        &self,
        event: ProgressEvent,
    ) -> Result<(), ProgressStreamingError> {
        self.event_sender
            .send(event)
            .map_err(|_| ProgressStreamingError::BroadcastFailed)?;
        Ok(())
    }

    /// Get current connection statistics
    pub fn get_connection_stats(&self) -> ConnectionStats {
        ConnectionStats {
            active_connections: self
                .connection_count
                .load(std::sync::atomic::Ordering::Relaxed),
            max_connections: self.config.max_connections,
            utilization_percentage: (self
                .connection_count
                .load(std::sync::atomic::Ordering::Relaxed)
                as f64
                / self.config.max_connections as f64
                * 100.0)
                .min(100.0),
        }
    }

    /// Check if summary changed significantly enough to warrant an update
    fn summary_changed_significantly(old: &LoadingSummary, new: &LoadingSummary) -> bool {
        // Consider it significant if:
        // - Number of tables changed
        // - Any status counts changed
        // - Total records loaded increased by more than 1000
        old.total_tables != new.total_tables
            || old.loading != new.loading
            || old.completed != new.completed
            || old.failed != new.failed
            || old.cancelled != new.cancelled
            || (new
                .total_records_loaded
                .saturating_sub(old.total_records_loaded)
                > 1000)
    }
}

/// Individual progress stream for a client connection
pub struct ProgressStream {
    /// Receiver for progress events
    event_receiver: broadcast::Receiver<ProgressEvent>,

    /// Optional filter for specific tables
    table_filter: Option<Vec<String>>,

    /// Connection counter reference for cleanup
    connection_count: Arc<std::sync::atomic::AtomicUsize>,

    /// Initial event to send first
    initial_event: Option<ProgressEvent>,
}

impl ProgressStream {
    fn new(
        event_receiver: broadcast::Receiver<ProgressEvent>,
        initial_event: ProgressEvent,
        connection_count: Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self {
            event_receiver,
            table_filter: None,
            connection_count,
            initial_event: Some(initial_event),
        }
    }

    /// Set a filter to only receive updates for specific tables
    pub fn set_table_filter(&mut self, table_names: Vec<String>) {
        self.table_filter = Some(table_names);
    }

    /// Convert to a tokio Stream for use with async frameworks
    pub fn into_stream(
        mut self,
    ) -> impl Stream<Item = Result<ProgressEvent, ProgressStreamingError>> {
        async_stream::stream! {
            // Send initial event first
            if let Some(initial) = self.initial_event.take() {
                yield Ok(initial);
            }

            // Then stream ongoing events
            loop {
                match self.event_receiver.recv().await {
                    Ok(event) => {
                        // Apply table filter if set
                        if let Some(filter) = self.table_filter.as_ref() {
                            match &event {
                                ProgressEvent::TableUpdate { table_name, .. } |
                                ProgressEvent::TableCompleted { table_name, .. } |
                                ProgressEvent::TableFailed { table_name, .. } => {
                                    if !filter.contains(table_name) {
                                        continue; // Skip this event
                                    }
                                }
                                _ => {} // Always include summary updates and keep-alives
                            }
                        }

                        yield Ok(event);
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Client fell behind, continue with next messages
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Channel closed, end stream
                        break;
                    }
                }
            }
        }
    }

    /// Collect the next N events (useful for testing)
    pub async fn collect_events(&mut self, count: usize) -> Vec<ProgressEvent> {
        let mut events = Vec::new();

        // Include initial event if available
        if let Some(initial) = self.initial_event.take() {
            events.push(initial);
            if count == 1 {
                return events;
            }
        }

        // Collect additional events
        for _ in 0..(count - events.len()) {
            match self.event_receiver.recv().await {
                Ok(event) => events.push(event),
                Err(_) => break,
            }
        }

        events
    }
}

impl Drop for ProgressStream {
    fn drop(&mut self) {
        // Decrement connection counter when stream is dropped
        self.connection_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Connection statistics for the streaming server
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionStats {
    pub active_connections: usize,
    pub max_connections: usize,
    pub utilization_percentage: f64,
}

/// Errors that can occur in the progress streaming system
#[derive(Debug, thiserror::Error)]
pub enum ProgressStreamingError {
    #[error("Too many connections: {current_connections}/{max_connections}")]
    TooManyConnections {
        max_connections: usize,
        current_connections: usize,
    },

    #[error("Failed to broadcast progress event")]
    BroadcastFailed,

    #[error("Stream receiver error: {0}")]
    ReceiverError(#[from] broadcast::error::RecvError),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::velostream::server::table_registry::TableRegistry;
    use tokio::time::{Duration as TokioDuration, sleep};

    #[tokio::test]
    async fn test_progress_streaming_server() {
        let registry = Arc::new(TableRegistry::new());
        let config = ProgressStreamingConfig {
            update_interval: Duration::from_millis(100),
            max_connections: 5,
            ..Default::default()
        };

        let server = ProgressStreamingServer::new(registry.clone(), config);

        // Start monitoring task
        let _monitoring_task = server.start_monitoring().await;

        // Create a stream
        let stream = server.create_stream().await.unwrap();
        let stats = server.get_connection_stats();

        assert_eq!(stats.active_connections, 1);
        assert_eq!(stats.max_connections, 5);
    }

    #[tokio::test]
    async fn test_connection_limits() {
        let registry = Arc::new(TableRegistry::new());
        let config = ProgressStreamingConfig {
            max_connections: 2,
            ..Default::default()
        };

        let server = ProgressStreamingServer::new(registry, config);

        // Create maximum allowed connections
        let _stream1 = server.create_stream().await.unwrap();
        let _stream2 = server.create_stream().await.unwrap();

        // Third connection should fail
        let result = server.create_stream().await;
        assert!(matches!(
            result,
            Err(ProgressStreamingError::TooManyConnections { .. })
        ));
    }

    #[tokio::test]
    async fn test_progress_stream_filtering() {
        let registry = Arc::new(TableRegistry::new());
        let config = ProgressStreamingConfig::default();
        let server = ProgressStreamingServer::new(registry, config);

        // Create filtered stream for specific tables
        let stream = server
            .create_filtered_stream(vec!["table1".to_string(), "table2".to_string()])
            .await
            .unwrap();

        // The stream should have the filter set
        assert!(stream.table_filter.is_some());
        assert_eq!(stream.table_filter.as_ref().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_event_serialization() {
        let progress = TableLoadProgress {
            table_name: "test_table".to_string(),
            total_records_expected: Some(1000),
            records_loaded: 500,
            bytes_processed: 1024000,
            started_at: chrono::Utc::now(),
            estimated_completion: None,
            loading_rate: 100.0,
            bytes_per_second: 2048.0,
            status: crate::velostream::server::progress_monitoring::TableLoadStatus::Loading,
            error_message: None,
            progress_percentage: Some(50.0),
        };

        let event = ProgressEvent::TableUpdate {
            table_name: "test_table".to_string(),
            progress,
            timestamp: chrono::Utc::now(),
        };

        // Test JSON serialization
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("TableUpdate"));
        assert!(json.contains("test_table"));

        // Test deserialization
        let deserialized: ProgressEvent = serde_json::from_str(&json).unwrap();
        match deserialized {
            ProgressEvent::TableUpdate { table_name, .. } => {
                assert_eq!(table_name, "test_table");
            }
            _ => panic!("Wrong event type"),
        }
    }
}
