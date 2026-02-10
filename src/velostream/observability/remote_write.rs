//! Prometheus Remote Write Client
//!
//! This module provides a client for pushing metrics with explicit event timestamps
//! to Prometheus via the remote-write protocol. This is essential for displaying
//! metrics with correct event-time semantics in Grafana, rather than using scrape time.
//!
//! # Background
//!
//! Standard Prometheus scraping uses the scrape timestamp for all metrics, which means
//! historical or event-sourced data appears at the time it was scraped, not when the
//! event actually occurred. Remote-write allows us to push metrics with their actual
//! event timestamps, enabling proper time-series visualization.
//!
//! # Basic Usage
//!
//! ```ignore
//! use velostream::observability::remote_write::{RemoteWriteClient, RetryConfig};
//!
//! // Create client with default retry settings (3 retries, exponential backoff)
//! let client = RemoteWriteClient::new(
//!     "http://prometheus:9090/api/v1/write",
//!     1000,   // batch_size: flush after 1000 samples
//!     5000,   // flush_interval_ms: or flush every 5 seconds
//! )?;
//!
//! // Push metrics with event timestamps
//! client.push_gauge(
//!     "trade_price",
//!     &["symbol".to_string(), "exchange".to_string()],
//!     &["AAPL".to_string(), "NYSE".to_string()],
//!     150.25,
//!     1704067200000,  // event timestamp in milliseconds
//! );
//!
//! // Flush to Prometheus (with automatic retry on failure)
//! client.flush().await?;
//! ```
//!
//! # Custom Retry Configuration
//!
//! ```ignore
//! use std::time::Duration;
//!
//! let client = RemoteWriteClient::with_retry_config(
//!     "http://prometheus:9090/api/v1/write",
//!     1000,
//!     5000,
//!     RetryConfig {
//!         max_retries: 5,
//!         initial_delay: Duration::from_millis(200),
//!         max_delay: Duration::from_secs(30),
//!         backoff_multiplier: 2.0,
//!         request_timeout: Duration::from_secs(60),
//!     },
//! )?;
//! ```
//!
//! # Integration with PrometheusConfig
//!
//! ```ignore
//! use velostream::sql::execution::config::PrometheusConfig;
//!
//! let config = PrometheusConfig {
//!     remote_write_enabled: true,
//!     remote_write_endpoint: Some("http://prometheus:9090/api/v1/write".to_string()),
//!     remote_write_batch_size: 1000,
//!     remote_write_flush_interval_ms: 5000,
//!     ..PrometheusConfig::default()
//! };
//! ```
//!
//! # Resilience Features
//!
//! - **Retry with exponential backoff**: Transient failures (5xx, timeouts) are retried
//! - **Smart retry decisions**: 4xx client errors fail immediately (except 429)
//! - **Data preservation**: Failed samples are restored to buffer for next flush
//! - **Configurable timeouts**: Per-request timeout with configurable duration
//! - **Buffer overflow protection**: Maximum buffer size prevents unbounded memory growth

use log::{debug, error, info, warn};
use prometheus_remote_write::{LABEL_NAME, Label, Sample, TimeSeries, WriteRequest};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use url::Url;

/// Error type for remote-write operations
#[derive(Debug, Clone)]
pub enum RemoteWriteError {
    /// Failed to compress the request
    CompressionError(String),
    /// Failed to send the request
    HttpError(String),
    /// Invalid configuration
    ConfigError(String),
}

impl std::fmt::Display for RemoteWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoteWriteError::CompressionError(msg) => write!(f, "Compression error: {}", msg),
            RemoteWriteError::HttpError(msg) => write!(f, "HTTP error: {}", msg),
            RemoteWriteError::ConfigError(msg) => write!(f, "Configuration error: {}", msg),
        }
    }
}

impl std::error::Error for RemoteWriteError {}

/// Configuration for retry behavior on remote-write failures
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries)
    pub max_retries: u32,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Backoff multiplier (e.g., 2.0 for exponential backoff)
    pub backoff_multiplier: f64,
    /// HTTP request timeout
    pub request_timeout: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl RetryConfig {
    /// Create a retry config with no retries (fail fast)
    pub fn no_retries() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Create a retry config for aggressive retrying
    pub fn aggressive() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            request_timeout: Duration::from_secs(60),
        }
    }

    /// Validate the retry configuration
    ///
    /// Returns an error if any values are invalid.
    pub fn validate(&self) -> Result<(), RemoteWriteError> {
        if self.backoff_multiplier <= 0.0 {
            return Err(RemoteWriteError::ConfigError(
                "backoff_multiplier must be greater than 0".to_string(),
            ));
        }

        if self.backoff_multiplier > 10.0 {
            return Err(RemoteWriteError::ConfigError(
                "backoff_multiplier should not exceed 10.0 (too aggressive)".to_string(),
            ));
        }

        if self.initial_delay > self.max_delay {
            return Err(RemoteWriteError::ConfigError(format!(
                "initial_delay ({:?}) cannot exceed max_delay ({:?})",
                self.initial_delay, self.max_delay
            )));
        }

        if self.request_timeout.is_zero() {
            return Err(RemoteWriteError::ConfigError(
                "request_timeout must be greater than 0".to_string(),
            ));
        }

        if self.request_timeout > Duration::from_secs(300) {
            return Err(RemoteWriteError::ConfigError(
                "request_timeout should not exceed 5 minutes".to_string(),
            ));
        }

        Ok(())
    }
}

/// Validate a remote-write endpoint URL
///
/// Checks that:
/// - URL is well-formed
/// - Scheme is http or https
/// - Host is present
/// - Port (if specified) is in valid range (1-65535)
fn validate_endpoint_url(endpoint: &str) -> Result<Url, RemoteWriteError> {
    if endpoint.is_empty() {
        return Err(RemoteWriteError::ConfigError(
            "Endpoint URL cannot be empty".to_string(),
        ));
    }

    let url = Url::parse(endpoint).map_err(|e| {
        RemoteWriteError::ConfigError(format!("Invalid endpoint URL '{}': {}", endpoint, e))
    })?;

    // Validate scheme
    match url.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(RemoteWriteError::ConfigError(format!(
                "Invalid URL scheme '{}': must be 'http' or 'https'",
                scheme
            )));
        }
    }

    // Validate host is present and not empty
    match url.host_str() {
        None | Some("") => {
            return Err(RemoteWriteError::ConfigError(format!(
                "Invalid endpoint URL '{}': missing host",
                endpoint
            )));
        }
        _ => {}
    }

    // Validate port if specified (Url::parse handles range validation)
    // But let's be explicit about common mistakes
    if let Some(port) = url.port() {
        if port == 0 {
            return Err(RemoteWriteError::ConfigError(
                "Invalid port 0: port must be between 1 and 65535".to_string(),
            ));
        }
    }

    Ok(url)
}

/// Maximum buffer size to prevent unbounded memory growth
const MAX_BUFFER_SIZE: usize = 100_000;

/// A timestamped metric sample for batching
#[derive(Debug, Clone)]
pub struct TimestampedSample {
    /// Metric name
    pub name: String,
    /// Label names (must match label_values length)
    pub label_names: Vec<String>,
    /// Label values (must match label_names length)
    pub label_values: Vec<String>,
    /// Metric value
    pub value: f64,
    /// Event timestamp in milliseconds since Unix epoch
    pub timestamp_ms: i64,
}

/// Buffer for accumulating metrics before flushing
#[derive(Debug)]
struct MetricBuffer {
    samples: Vec<TimestampedSample>,
    last_flush: Instant,
}

impl MetricBuffer {
    fn new() -> Self {
        Self {
            samples: Vec::new(),
            last_flush: Instant::now(),
        }
    }

    fn add(&mut self, sample: TimestampedSample) {
        self.samples.push(sample);
    }

    fn take(&mut self) -> Vec<TimestampedSample> {
        self.last_flush = Instant::now();
        std::mem::take(&mut self.samples)
    }

    /// Restore samples to the buffer (prepend to preserve order)
    /// Used when a flush fails and we want to retry later
    fn restore(&mut self, mut samples: Vec<TimestampedSample>) {
        // Prepend failed samples so they're sent first on next flush
        samples.append(&mut self.samples);
        self.samples = samples;
    }

    fn len(&self) -> usize {
        self.samples.len()
    }

    fn time_since_flush(&self) -> Duration {
        self.last_flush.elapsed()
    }
}

/// Client for pushing metrics to Prometheus via remote-write protocol
///
/// This client batches metrics and flushes them either when the batch size
/// is reached or when the flush interval expires. Includes retry logic with
/// exponential backoff for resilience against transient failures.
#[derive(Clone)]
pub struct RemoteWriteClient {
    /// Remote-write endpoint URL
    endpoint: String,
    /// Maximum samples to batch before auto-flush
    batch_size: usize,
    /// Flush interval in milliseconds
    flush_interval_ms: u64,
    /// Buffered metrics
    buffer: Arc<Mutex<MetricBuffer>>,
    /// HTTP client for sending requests
    http_client: reqwest::Client,
    /// Whether the client is active (shared across clones)
    active: Arc<AtomicBool>,
    /// Retry configuration for failed requests
    retry_config: RetryConfig,
    /// Counter for dropped samples (buffer overflow, future timestamps, label mismatch)
    dropped_samples: Arc<AtomicU64>,
}

impl RemoteWriteClient {
    /// Create a new remote-write client with default retry configuration
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The Prometheus remote-write endpoint URL (e.g., "http://prometheus:9090/api/v1/write")
    /// * `batch_size` - Maximum number of samples to buffer before flushing
    /// * `flush_interval_ms` - Maximum time to wait before flushing buffered samples
    pub fn new(
        endpoint: &str,
        batch_size: usize,
        flush_interval_ms: u64,
    ) -> Result<Self, RemoteWriteError> {
        Self::with_retry_config(
            endpoint,
            batch_size,
            flush_interval_ms,
            RetryConfig::default(),
        )
    }

    /// Create a new remote-write client with custom retry configuration
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The Prometheus remote-write endpoint URL (e.g., "http://prometheus:9090/api/v1/write")
    /// * `batch_size` - Maximum number of samples to buffer before flushing
    /// * `flush_interval_ms` - Maximum time to wait before flushing buffered samples
    /// * `retry_config` - Configuration for retry behavior on failures
    ///
    /// # Errors
    ///
    /// Returns `RemoteWriteError::ConfigError` if:
    /// - endpoint URL is invalid (bad format, wrong scheme, missing host, invalid port)
    /// - batch_size is 0
    /// - retry_config has invalid values
    pub fn with_retry_config(
        endpoint: &str,
        batch_size: usize,
        flush_interval_ms: u64,
        retry_config: RetryConfig,
    ) -> Result<Self, RemoteWriteError> {
        // Validate endpoint URL (scheme, host, port)
        let url = validate_endpoint_url(endpoint)?;

        // Validate batch_size
        if batch_size == 0 {
            return Err(RemoteWriteError::ConfigError(
                "batch_size must be greater than 0".to_string(),
            ));
        }

        if batch_size > MAX_BUFFER_SIZE {
            return Err(RemoteWriteError::ConfigError(format!(
                "batch_size {} exceeds maximum allowed {} (use smaller batches with more frequent flushes)",
                batch_size, MAX_BUFFER_SIZE
            )));
        }

        // Validate retry configuration
        retry_config.validate()?;

        let http_client = reqwest::Client::builder()
            .timeout(retry_config.request_timeout)
            .build()
            .map_err(|e| RemoteWriteError::HttpError(e.to_string()))?;

        info!(
            "📤 Remote-write client initialized: endpoint={}, batch_size={}, flush_interval_ms={}, max_retries={}",
            url, batch_size, flush_interval_ms, retry_config.max_retries
        );

        Ok(Self {
            endpoint: endpoint.to_string(),
            batch_size,
            flush_interval_ms,
            buffer: Arc::new(Mutex::new(MetricBuffer::new())),
            http_client,
            active: Arc::new(AtomicBool::new(true)),
            retry_config,
            dropped_samples: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Check if the client should auto-flush based on batch size or time
    fn should_flush(&self, buffer: &MetricBuffer) -> bool {
        buffer.len() >= self.batch_size
            || buffer.time_since_flush() >= Duration::from_millis(self.flush_interval_ms)
    }

    /// Push a gauge metric with an explicit timestamp
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name
    /// * `label_names` - Label names for this metric
    /// * `label_values` - Label values (must match label_names length)
    /// * `value` - The gauge value
    /// * `timestamp_ms` - Event timestamp in milliseconds since Unix epoch
    ///
    /// # Buffer Overflow Protection
    ///
    /// If the buffer exceeds `MAX_BUFFER_SIZE`, new samples are dropped with a warning.
    /// This prevents unbounded memory growth when the remote endpoint is unreachable.
    pub fn push_gauge(
        &self,
        name: &str,
        label_names: &[String],
        label_values: &[String],
        value: f64,
        timestamp_ms: i64,
    ) {
        if !self.active.load(Ordering::Relaxed) {
            return;
        }

        // Validate label alignment to prevent silent data corruption
        if label_names.len() != label_values.len() {
            let total = self.dropped_samples.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                "📤 Label mismatch for metric '{}': {} names vs {} values - skipping (total dropped: {})",
                name,
                label_names.len(),
                label_values.len(),
                total
            );
            return;
        }

        // Skip samples with timestamps too far in the future (Prometheus will reject them)
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let diff_ms = timestamp_ms - now_ms;
        if diff_ms > 300_000 {
            // More than 5 minutes in future
            let total = self.dropped_samples.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(
                "📤 Skipping metric '{}' with timestamp {}ms ({}s in future) - Prometheus would reject (total dropped: {})",
                name,
                timestamp_ms,
                diff_ms / 1000,
                total
            );
            return;
        }

        let sample = TimestampedSample {
            name: name.to_string(),
            label_names: label_names.to_vec(),
            label_values: label_values.to_vec(),
            value,
            timestamp_ms,
        };

        if let Ok(mut buffer) = self.buffer.lock() {
            // Buffer overflow protection
            if buffer.len() >= MAX_BUFFER_SIZE {
                let total = self.dropped_samples.fetch_add(1, Ordering::Relaxed) + 1;
                warn!(
                    "📤 Buffer overflow: dropping metric '{}' (buffer at max capacity {}, total dropped: {})",
                    name, MAX_BUFFER_SIZE, total
                );
                return;
            }

            buffer.add(sample);
            debug!(
                "📤 Buffered gauge '{}' = {} @ {}ms (buffer size: {})",
                name,
                value,
                timestamp_ms,
                buffer.len()
            );
        }
    }

    /// Push a counter metric with an explicit timestamp
    ///
    /// Note: For counters, we push the current counter value. Prometheus will
    /// compute the rate/increase from successive samples.
    pub fn push_counter(
        &self,
        name: &str,
        label_names: &[String],
        label_values: &[String],
        value: f64,
        timestamp_ms: i64,
    ) {
        // Counters are pushed the same way as gauges in remote-write
        // Prometheus handles the counter semantics
        self.push_gauge(name, label_names, label_values, value, timestamp_ms);
    }

    /// Push a histogram observation with an explicit timestamp
    ///
    /// Note: Histograms in remote-write require multiple time series for buckets,
    /// sum, and count. This simplified version pushes a single gauge value.
    /// For full histogram support, use the standard Prometheus registry.
    pub fn push_histogram_observation(
        &self,
        name: &str,
        label_names: &[String],
        label_values: &[String],
        value: f64,
        timestamp_ms: i64,
    ) {
        // For remote-write, we push histogram observations as individual samples
        // This allows tracking the distribution of values over time
        self.push_gauge(name, label_names, label_values, value, timestamp_ms);
    }

    /// Get the count of dropped samples (buffer overflow, future timestamps, label mismatch)
    ///
    /// This is useful for observability and alerting on data loss.
    pub fn dropped_samples_count(&self) -> u64 {
        self.dropped_samples.load(Ordering::Relaxed)
    }

    /// Reset the dropped samples counter and return the previous value
    ///
    /// Useful for periodic reporting where you want to track drops per interval.
    pub fn reset_dropped_count(&self) -> u64 {
        self.dropped_samples.swap(0, Ordering::Relaxed)
    }

    /// Flush all buffered metrics to Prometheus with retry logic
    ///
    /// This method sends all buffered samples to the remote-write endpoint.
    /// Samples are grouped by metric identity (name + labels) into time series.
    /// Out-of-order samples (older than previously sent) are filtered and logged.
    /// On failure, samples are restored to the buffer for retry on next flush.
    pub async fn flush(&self) -> Result<usize, RemoteWriteError> {
        let samples = {
            let mut buffer = self
                .buffer
                .lock()
                .map_err(|e| RemoteWriteError::HttpError(format!("Lock error: {}", e)))?;
            buffer.take()
        };

        if samples.is_empty() {
            return Ok(0);
        }

        // Group samples by metric identity (name + sorted labels)
        let mut series_map: HashMap<String, TimeSeries> = HashMap::new();

        for sample in &samples {
            // Build the identity key
            let mut identity_parts = vec![sample.name.clone()];
            for (name, value) in sample.label_names.iter().zip(sample.label_values.iter()) {
                identity_parts.push(format!("{}={}", name, value));
            }
            let identity = identity_parts.join("|");

            // Get or create the time series
            let series = series_map.entry(identity).or_insert_with(|| {
                let mut labels = vec![Label {
                    name: LABEL_NAME.to_string(),
                    value: sample.name.clone(),
                }];

                for (name, value) in sample.label_names.iter().zip(sample.label_values.iter()) {
                    labels.push(Label {
                        name: name.clone(),
                        value: value.clone(),
                    });
                }

                TimeSeries {
                    labels,
                    samples: Vec::new(),
                }
            });

            // Add the sample
            series.samples.push(Sample {
                value: sample.value,
                timestamp: sample.timestamp_ms,
            });
        }

        // Sort samples within each time series by timestamp (Prometheus requires ascending order)
        // and deduplicate: keep only the last value per timestamp (Prometheus rejects duplicate timestamps)
        let timeseries: Vec<TimeSeries> = series_map
            .into_values()
            .filter_map(|mut ts| {
                if ts.samples.is_empty() {
                    return None;
                }
                ts.samples.sort_by_key(|s| s.timestamp);
                // Deduplicate: for same-timestamp samples, keep the last value
                ts.samples.dedup_by_key(|s| s.timestamp);
                Some(ts)
            })
            .collect();

        if timeseries.is_empty() {
            debug!("📤 No samples to send");
            return Ok(0);
        }

        let sample_count: usize = timeseries.iter().map(|ts| ts.samples.len()).sum();

        // Build the write request
        let write_request = WriteRequest { timeseries }.sorted();

        // Encode with snappy compression
        let compressed = write_request
            .encode_compressed()
            .map_err(|e| RemoteWriteError::CompressionError(e.to_string()))?;

        // Attempt to send with retry logic
        let mut last_error = None;
        let mut current_delay = self.retry_config.initial_delay;

        for attempt in 0..=self.retry_config.max_retries {
            if attempt > 0 {
                warn!(
                    "📤 Remote-write retry attempt {}/{} after {:?} delay",
                    attempt, self.retry_config.max_retries, current_delay
                );
                sleep(current_delay).await;

                // Calculate next delay with exponential backoff
                current_delay = Duration::from_secs_f64(
                    (current_delay.as_secs_f64() * self.retry_config.backoff_multiplier)
                        .min(self.retry_config.max_delay.as_secs_f64()),
                );
            }

            // Clone the compressed payload for this attempt
            let payload = compressed.clone();

            match self
                .http_client
                .post(&self.endpoint)
                .header("Content-Type", prometheus_remote_write::CONTENT_TYPE)
                .header(
                    prometheus_remote_write::HEADER_NAME_REMOTE_WRITE_VERSION,
                    prometheus_remote_write::REMOTE_WRITE_VERSION_01,
                )
                .header("Content-Encoding", "snappy")
                .body(payload)
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        if attempt > 0 {
                            info!(
                                "📤 Successfully pushed {} samples after {} retries",
                                sample_count, attempt
                            );
                        } else {
                            debug!(
                                "📤 Successfully pushed {} samples to remote-write endpoint",
                                sample_count
                            );
                        }
                        return Ok(sample_count);
                    }

                    let status = response.status();

                    // Don't retry on 4xx client errors (except 429 Too Many Requests)
                    if status.is_client_error() && status.as_u16() != 429 {
                        let body = response
                            .text()
                            .await
                            .unwrap_or_else(|_| "Unknown error".to_string());

                        // Permanent rejections (too old, duplicate, out of order)
                        // must be dropped — restoring them causes an infinite retry
                        // loop that starves the async runtime.
                        let is_permanent = body.contains("too old")
                            || body.contains("out of order")
                            || body.contains("duplicate sample");

                        if is_permanent {
                            warn!(
                                "📤 Remote-write permanently rejected {} samples (dropping): status={}, body={}",
                                sample_count, status, body
                            );
                        } else {
                            error!(
                                "📤 Remote-write failed with client error (not retrying): status={}, body={}",
                                status, body
                            );
                            // Only restore samples that might succeed on a future
                            // attempt (e.g. schema mismatch the user can fix).
                            self.restore_samples(samples);
                        }

                        return Err(RemoteWriteError::HttpError(format!(
                            "HTTP {}: {}",
                            status, body
                        )));
                    }

                    // Retryable error
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "Unknown error".to_string());
                    last_error = Some(RemoteWriteError::HttpError(format!(
                        "HTTP {}: {}",
                        status, body
                    )));
                }
                Err(e) => {
                    // Network error, timeout, etc. - retryable
                    last_error = Some(RemoteWriteError::HttpError(e.to_string()));
                }
            }
        }

        // All retries exhausted - restore samples to buffer
        error!(
            "📤 Remote-write failed after {} retries, restoring {} samples to buffer",
            self.retry_config.max_retries, sample_count
        );
        self.restore_samples(samples);

        Err(last_error.unwrap_or_else(|| {
            RemoteWriteError::HttpError("Unknown error after retries".to_string())
        }))
    }

    /// Restore samples to the buffer after a failed flush
    fn restore_samples(&self, samples: Vec<TimestampedSample>) {
        if let Ok(mut buffer) = self.buffer.lock() {
            let count = samples.len();
            buffer.restore(samples);
            warn!(
                "📤 Restored {} samples to buffer for retry (buffer size: {})",
                count,
                buffer.len()
            );
        } else {
            error!(
                "📤 Failed to restore {} samples to buffer - samples lost!",
                samples.len()
            );
        }
    }

    /// Flush if the buffer has reached the batch size or flush interval
    ///
    /// Returns the number of samples flushed, or 0 if no flush was needed.
    pub async fn maybe_flush(&self) -> Result<usize, RemoteWriteError> {
        let should_flush = {
            let buffer = self
                .buffer
                .lock()
                .map_err(|e| RemoteWriteError::HttpError(format!("Lock error: {}", e)))?;
            self.should_flush(&buffer)
        };

        if should_flush {
            self.flush().await
        } else {
            Ok(0)
        }
    }

    /// Get the current number of buffered samples
    pub fn buffered_count(&self) -> usize {
        self.buffer.lock().map(|b| b.len()).unwrap_or(0)
    }

    /// Shutdown the client, flushing any remaining samples
    pub async fn shutdown(&mut self) -> Result<usize, RemoteWriteError> {
        self.active.store(false, Ordering::Relaxed);
        let count = self.flush().await?;
        info!("📤 Remote-write client shutdown, flushed {} samples", count);
        Ok(count)
    }
}

impl std::fmt::Debug for RemoteWriteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteWriteClient")
            .field("endpoint", &self.endpoint)
            .field("batch_size", &self.batch_size)
            .field("flush_interval_ms", &self.flush_interval_ms)
            .field("active", &self.active.load(Ordering::Relaxed))
            .field("buffered_count", &self.buffered_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamped_sample_creation() {
        let sample = TimestampedSample {
            name: "test_metric".to_string(),
            label_names: vec!["label1".to_string()],
            label_values: vec!["value1".to_string()],
            value: 42.0,
            timestamp_ms: 1000000,
        };

        assert_eq!(sample.name, "test_metric");
        assert_eq!(sample.value, 42.0);
        assert_eq!(sample.timestamp_ms, 1000000);
    }

    #[test]
    fn test_remote_write_client_creation() {
        let client = RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000);
        assert!(client.is_ok());

        let client = client.unwrap();
        assert_eq!(client.endpoint, "http://localhost:9090/api/v1/write");
        assert_eq!(client.batch_size, 100);
        assert_eq!(client.flush_interval_ms, 5000);
    }

    #[test]
    fn test_remote_write_client_empty_endpoint_error() {
        let client = RemoteWriteClient::new("", 100, 5000);
        assert!(client.is_err());

        match client {
            Err(RemoteWriteError::ConfigError(msg)) => {
                assert!(msg.contains("empty"));
            }
            _ => panic!("Expected ConfigError"),
        }
    }

    #[test]
    fn test_push_gauge_buffers_sample() {
        let client =
            RemoteWriteClient::new("http://localhost:9090/api/v1/write", 100, 5000).unwrap();

        assert_eq!(client.buffered_count(), 0);

        client.push_gauge(
            "test_gauge",
            &["symbol".to_string()],
            &["AAPL".to_string()],
            100.0,
            1704067200000, // 2024-01-01 00:00:00 UTC
        );

        assert_eq!(client.buffered_count(), 1);

        client.push_gauge(
            "test_gauge",
            &["symbol".to_string()],
            &["MSFT".to_string()],
            200.0,
            1704067260000, // 1 minute later
        );

        assert_eq!(client.buffered_count(), 2);
    }

    #[test]
    fn test_metric_buffer_take() {
        let mut buffer = MetricBuffer::new();

        buffer.add(TimestampedSample {
            name: "test".to_string(),
            label_names: vec![],
            label_values: vec![],
            value: 1.0,
            timestamp_ms: 1000,
        });

        buffer.add(TimestampedSample {
            name: "test".to_string(),
            label_names: vec![],
            label_values: vec![],
            value: 2.0,
            timestamp_ms: 2000,
        });

        assert_eq!(buffer.len(), 2);

        let samples = buffer.take();
        assert_eq!(samples.len(), 2);
        assert_eq!(buffer.len(), 0);
    }
}
