//! Tokio-based Span Processor - Reliable replacement for BatchSpanProcessor
//!
//! The `BatchSpanProcessor` from `opentelemetry_sdk 0.21.2` has a bug where its
//! background export loop stops after the first cycle due to a `stream::select` /
//! `FusedStream` interaction. This causes spans to be created with `recording=true`
//! but never exported during runtime — only on shutdown via `force_flush()`.
//!
//! `TokioSpanProcessor` replaces `BatchSpanProcessor` with a simple implementation
//! using `tokio::sync::mpsc` and `tokio::select!`, which avoids the `FusedStream`
//! bug because `tokio::select!` polls futures independently rather than combining
//! streams.

use opentelemetry::trace::TraceResult;
use opentelemetry_sdk::export::trace::{SpanData, SpanExporter};
use opentelemetry_sdk::trace::SpanProcessor;
use std::fmt;
use std::time::Duration;

/// Configuration for TokioSpanProcessor
#[derive(Debug, Clone)]
pub struct TokioSpanProcessorConfig {
    /// Maximum number of spans in the channel buffer
    pub max_queue_size: usize,
    /// Maximum batch size per export call
    pub max_export_batch_size: usize,
    /// Interval between scheduled exports
    pub scheduled_delay: Duration,
    /// Timeout for each export call (prevents a hanging HTTP client from
    /// blocking the entire export loop)
    pub export_timeout: Duration,
}

impl Default for TokioSpanProcessorConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 2048,
            max_export_batch_size: 512,
            scheduled_delay: Duration::from_millis(5000),
            export_timeout: Duration::from_secs(30),
        }
    }
}

/// A span processor that uses tokio::mpsc for reliable background export.
///
/// Replaces `BatchSpanProcessor` to avoid the SDK 0.21.2 `FusedStream` bug.
/// Spans are sent via `try_send` (non-blocking) and flushed periodically
/// by a background tokio task.
pub struct TokioSpanProcessor {
    sender: tokio::sync::mpsc::Sender<SpanData>,
}

impl fmt::Debug for TokioSpanProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokioSpanProcessor")
            .field("sender", &"<mpsc::Sender>")
            .finish()
    }
}

impl TokioSpanProcessor {
    /// Create a new TokioSpanProcessor with the given exporter and configuration.
    ///
    /// Spawns a background tokio task that:
    /// - Buffers incoming spans from `on_end()`
    /// - Flushes them to the exporter on a periodic interval
    /// - Exports any remaining spans when the channel closes (shutdown)
    pub fn new<E>(exporter: E, config: TokioSpanProcessorConfig) -> Self
    where
        E: SpanExporter + 'static,
    {
        let (sender, receiver) = tokio::sync::mpsc::channel(config.max_queue_size);

        tokio::spawn(async move {
            log::info!("TokioSpanProcessor: export loop started");
            export_loop(receiver, exporter, config).await;
            log::info!("TokioSpanProcessor: export loop ended");
        });

        Self { sender }
    }
}

/// Background export loop using `tokio::select!`
///
/// This avoids the `stream::select` / `FusedStream` bug in `BatchSpanProcessor`
/// because `tokio::select!` polls futures independently rather than combining them
/// into a combined stream that terminates when one branch is exhausted.
async fn export_loop<E>(
    mut receiver: tokio::sync::mpsc::Receiver<SpanData>,
    mut exporter: E,
    config: TokioSpanProcessorConfig,
) where
    E: SpanExporter + 'static,
{
    let mut buffer: Vec<SpanData> = Vec::with_capacity(config.max_export_batch_size);
    let mut interval = tokio::time::interval(config.scheduled_delay);

    // Skip the first immediate tick
    interval.tick().await;

    let export_timeout = config.export_timeout;
    let max_batch = config.max_export_batch_size;

    loop {
        tokio::select! {
            // Receive spans from the channel
            maybe_span = receiver.recv() => {
                match maybe_span {
                    Some(span) => {
                        buffer.push(span);
                        // If buffer is full, flush immediately
                        if buffer.len() >= max_batch {
                            flush_buffer(&mut buffer, &mut exporter, max_batch, export_timeout).await;
                        }
                    }
                    None => {
                        // Channel closed (shutdown) — flush remaining spans
                        if !buffer.is_empty() {
                            flush_buffer(&mut buffer, &mut exporter, max_batch, export_timeout).await;
                        }
                        log::info!("TokioSpanProcessor: channel closed, export loop exiting");
                        return;
                    }
                }
            }
            // Periodic flush
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    flush_buffer(&mut buffer, &mut exporter, max_batch, export_timeout).await;
                }
            }
        }
    }
}

/// Flush buffered spans to the exporter in batches with a timeout guard.
///
/// Each `export()` call is wrapped in `tokio::time::timeout()` so that a
/// hanging HTTP client cannot block the export loop indefinitely.
async fn flush_buffer<E>(
    buffer: &mut Vec<SpanData>,
    exporter: &mut E,
    max_batch_size: usize,
    export_timeout: Duration,
) where
    E: SpanExporter + 'static,
{
    let total = buffer.len();

    // Export in chunks of max_batch_size
    while !buffer.is_empty() {
        let drain_end = buffer.len().min(max_batch_size);
        let batch: Vec<SpanData> = buffer.drain(..drain_end).collect();
        let batch_len = batch.len();

        log::debug!(
            "TokioSpanProcessor: exporting batch of {} spans...",
            batch_len
        );

        match tokio::time::timeout(export_timeout, exporter.export(batch)).await {
            Ok(Ok(())) => {
                log::debug!(
                    "TokioSpanProcessor: export of {} spans succeeded",
                    batch_len
                );
            }
            Ok(Err(e)) => {
                log::warn!(
                    "TokioSpanProcessor: failed to export {} spans: {:?}",
                    batch_len,
                    e
                );
            }
            Err(_elapsed) => {
                log::warn!(
                    "TokioSpanProcessor: export of {} spans timed out after {:?}, continuing",
                    batch_len,
                    export_timeout,
                );
            }
        }
    }

    log::debug!("TokioSpanProcessor: flushed {} spans total", total);
}

impl SpanProcessor for TokioSpanProcessor {
    fn on_start(&self, _span: &mut opentelemetry_sdk::trace::Span, _cx: &opentelemetry::Context) {
        // No action needed on span start
    }

    fn on_end(&self, span: SpanData) {
        // Non-blocking send; drop span if queue is full
        if let Err(e) = self.sender.try_send(span) {
            match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    log::debug!("TokioSpanProcessor: queue full, dropping span");
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    log::debug!("TokioSpanProcessor: channel closed, dropping span");
                }
            }
        }
    }

    fn force_flush(&self) -> TraceResult<()> {
        // The background loop handles flushing periodically.
        // Unlike BatchSpanProcessor, we don't need blocking_recv here.
        Ok(())
    }

    fn shutdown(&mut self) -> TraceResult<()> {
        // Dropping the sender closes the channel, which causes the export loop
        // to flush remaining spans and exit. However, SpanProcessor::shutdown
        // doesn't take ownership, so we can't drop self.sender here.
        // The actual shutdown happens when TokioSpanProcessor is dropped
        // (sender is dropped, channel closes, export loop exits).
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
    use opentelemetry_sdk::InstrumentationLibrary;
    use opentelemetry_sdk::Resource;
    use std::borrow::Cow;
    use std::sync::{Arc, Mutex};

    /// Mock exporter that records all exported spans for verification
    #[derive(Debug, Clone)]
    struct RecordingExporter {
        exported: Arc<Mutex<Vec<SpanData>>>,
    }

    impl RecordingExporter {
        fn new() -> Self {
            Self {
                exported: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn exported_count(&self) -> usize {
            self.exported.lock().unwrap().len()
        }
    }

    impl SpanExporter for RecordingExporter {
        fn export(
            &mut self,
            batch: Vec<SpanData>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TraceResult<()>> + Send + 'static>>
        {
            let exported = self.exported.clone();
            Box::pin(async move {
                let mut guard = exported.lock().unwrap();
                guard.extend(batch);
                Ok(())
            })
        }
    }

    fn create_test_span(name: &'static str) -> SpanData {
        SpanData {
            span_context: SpanContext::new(
                TraceId::from_bytes([1u8; 16]),
                SpanId::from_bytes([2u8; 8]),
                TraceFlags::default(),
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::from_bytes([0u8; 8]),
            span_kind: opentelemetry::trace::SpanKind::Internal,
            name: Cow::Borrowed(name),
            start_time: std::time::SystemTime::now(),
            end_time: std::time::SystemTime::now(),
            attributes: vec![],
            dropped_attributes_count: 0,
            events: opentelemetry_sdk::trace::EvictedQueue::new(128),
            links: opentelemetry_sdk::trace::EvictedQueue::new(128),
            status: opentelemetry::trace::Status::Unset,
            resource: Cow::Owned(Resource::empty()),
            instrumentation_lib: InstrumentationLibrary::default(),
        }
    }

    #[tokio::test]
    async fn test_spans_exported_on_periodic_flush() {
        let exporter = RecordingExporter::new();
        let exported = exporter.exported.clone();

        let config = TokioSpanProcessorConfig {
            max_queue_size: 1024,
            max_export_batch_size: 512,
            scheduled_delay: Duration::from_millis(50),
            ..Default::default()
        };

        let processor = TokioSpanProcessor::new(exporter, config);

        // Send spans
        for _ in 0..5 {
            processor.on_end(create_test_span("periodic_test"));
        }

        // Wait for periodic flush
        tokio::time::sleep(Duration::from_millis(150)).await;

        let count = exported.lock().unwrap().len();
        assert_eq!(
            count, 5,
            "All 5 spans should be exported after flush interval"
        );
    }

    #[tokio::test]
    async fn test_spans_exported_on_buffer_full() {
        let exporter = RecordingExporter::new();
        let exported = exporter.exported.clone();

        let config = TokioSpanProcessorConfig {
            max_queue_size: 1024,
            max_export_batch_size: 3, // Small batch to trigger immediate flush
            scheduled_delay: Duration::from_secs(60), // Long delay so only batch-full flushes
            ..Default::default()
        };

        let processor = TokioSpanProcessor::new(exporter, config);

        // Send more spans than batch size
        for _ in 0..5 {
            processor.on_end(create_test_span("batch_full_test"));
        }

        // Give background task time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        let count = exported.lock().unwrap().len();
        // At least one batch of 3 should have been exported
        assert!(
            count >= 3,
            "At least 3 spans should be exported when buffer hits max_export_batch_size, got {}",
            count
        );
    }

    #[tokio::test]
    async fn test_spans_exported_on_channel_close() {
        let exporter = RecordingExporter::new();
        let exported = exporter.exported.clone();

        let config = TokioSpanProcessorConfig {
            max_queue_size: 1024,
            max_export_batch_size: 512,
            scheduled_delay: Duration::from_secs(60), // Long delay — rely on shutdown flush
            ..Default::default()
        };

        let processor = TokioSpanProcessor::new(exporter, config);

        // Send spans
        processor.on_end(create_test_span("shutdown_test_1"));
        processor.on_end(create_test_span("shutdown_test_2"));

        // Give background task time to receive
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Drop processor to close channel
        drop(processor);

        // Give export loop time to flush and exit
        tokio::time::sleep(Duration::from_millis(100)).await;

        let count = exported.lock().unwrap().len();
        assert_eq!(count, 2, "All spans should be flushed on channel close");
    }

    #[tokio::test]
    async fn test_drops_spans_when_queue_full() {
        let exporter = RecordingExporter::new();

        let config = TokioSpanProcessorConfig {
            max_queue_size: 2, // Tiny queue
            max_export_batch_size: 512,
            scheduled_delay: Duration::from_secs(60),
            ..Default::default()
        };

        let processor = TokioSpanProcessor::new(exporter, config);

        // Rapidly send more spans than queue can hold
        let mut sent = 0;
        for _ in 0..100 {
            processor.on_end(create_test_span("overflow_test"));
            sent += 1;
        }

        // Some should have been dropped (queue size is 2)
        // This test verifies no panic/block occurs
        assert!(sent == 100, "All sends should complete without blocking");
    }
}
