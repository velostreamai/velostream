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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

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

impl TokioSpanProcessorConfig {
    /// Validate configuration invariants.
    ///
    /// Returns an error description if any values would cause incorrect behavior.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_queue_size == 0 {
            return Err("max_queue_size must be > 0".to_string());
        }
        if self.max_export_batch_size == 0 {
            return Err(
                "max_export_batch_size must be > 0 (zero causes infinite loop)".to_string(),
            );
        }
        if self.scheduled_delay.is_zero() {
            return Err("scheduled_delay must be > 0 (zero causes spin loop)".to_string());
        }
        if self.export_timeout.is_zero() {
            return Err("export_timeout must be > 0".to_string());
        }
        Ok(())
    }
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

/// Message sent through the control channel for flush/shutdown coordination
enum ControlMessage {
    /// Request a flush; sender receives acknowledgment when complete
    Flush(oneshot::Sender<()>),
    /// Request shutdown; sender receives acknowledgment when drain is complete
    Shutdown(oneshot::Sender<()>),
}

/// A span processor that uses tokio::mpsc for reliable background export.
///
/// Replaces `BatchSpanProcessor` to avoid the SDK 0.21.2 `FusedStream` bug.
/// Spans are sent via `try_send` (non-blocking) and flushed periodically
/// by a background tokio task.
pub struct TokioSpanProcessor {
    sender: tokio::sync::mpsc::Sender<SpanData>,
    control_tx: tokio::sync::mpsc::Sender<ControlMessage>,
    dropped_count: Arc<AtomicUsize>,
    /// Approximate number of spans pending in the queue + export buffer.
    /// Incremented on successful `try_send`, decremented after export.
    pending_count: Arc<AtomicUsize>,
    /// Maximum queue capacity, used for computing queue pressure ratio.
    max_queue_size: usize,
}

impl fmt::Debug for TokioSpanProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokioSpanProcessor")
            .field("dropped_count", &self.dropped_count.load(Ordering::Relaxed))
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
    ///
    /// # Panics
    /// Panics if config validation fails (e.g., `max_export_batch_size == 0`).
    pub fn new<E>(exporter: E, config: TokioSpanProcessorConfig) -> Self
    where
        E: SpanExporter + 'static,
    {
        if let Err(msg) = config.validate() {
            panic!("TokioSpanProcessorConfig validation failed: {}", msg);
        }

        let max_queue_size = config.max_queue_size;
        let (sender, receiver) = tokio::sync::mpsc::channel(max_queue_size);
        let (control_tx, control_rx) = tokio::sync::mpsc::channel(4);
        let dropped_count = Arc::new(AtomicUsize::new(0));
        let pending_count = Arc::new(AtomicUsize::new(0));
        let pending_count_export = pending_count.clone();

        tokio::spawn(async move {
            log::info!("TokioSpanProcessor: export loop started");
            export_loop(receiver, control_rx, exporter, config, pending_count_export).await;
            log::info!("TokioSpanProcessor: export loop ended");
        });

        Self {
            sender,
            control_tx,
            dropped_count,
            pending_count,
            max_queue_size,
        }
    }

    /// Get the number of spans dropped due to queue full or channel closed.
    pub fn dropped_count(&self) -> usize {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// Get the approximate number of spans currently pending (queued + in-flight).
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// Get the maximum queue capacity.
    pub fn queue_capacity(&self) -> usize {
        self.max_queue_size
    }

    /// Queue pressure as a ratio from 0.0 (empty) to 1.0+ (full/overflowing).
    ///
    /// Values above 0.8 indicate the queue is nearing capacity and adaptive
    /// sampling should reduce the trace rate to avoid span drops.
    pub fn queue_pressure(&self) -> f64 {
        self.pending_count.load(Ordering::Relaxed) as f64 / self.max_queue_size.max(1) as f64
    }

    /// Get a clone of the internal pending count handle.
    ///
    /// This allows external code (e.g., TelemetryProvider) to monitor queue
    /// pressure after the processor has been moved into a TracerProvider.
    pub fn pending_count_handle(&self) -> Arc<AtomicUsize> {
        self.pending_count.clone()
    }
}

/// Background export loop using `tokio::select!`
///
/// This avoids the `stream::select` / `FusedStream` bug in `BatchSpanProcessor`
/// because `tokio::select!` polls futures independently rather than combining them
/// into a combined stream that terminates when one branch is exhausted.
async fn export_loop<E>(
    mut receiver: tokio::sync::mpsc::Receiver<SpanData>,
    mut control_rx: tokio::sync::mpsc::Receiver<ControlMessage>,
    mut exporter: E,
    config: TokioSpanProcessorConfig,
    pending_count: Arc<AtomicUsize>,
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
                            flush_buffer(&mut buffer, &mut exporter, max_batch, export_timeout, &pending_count).await;
                        }
                    }
                    None => {
                        // Channel closed (shutdown) — flush remaining spans
                        if !buffer.is_empty() {
                            flush_buffer(&mut buffer, &mut exporter, max_batch, export_timeout, &pending_count).await;
                        }
                        log::info!("TokioSpanProcessor: channel closed, export loop exiting");
                        return;
                    }
                }
            }
            // Control messages (flush/shutdown)
            maybe_ctrl = control_rx.recv() => {
                match maybe_ctrl {
                    Some(ControlMessage::Flush(ack)) => {
                        if !buffer.is_empty() {
                            flush_buffer(&mut buffer, &mut exporter, max_batch, export_timeout, &pending_count).await;
                        }
                        let _ = ack.send(());
                    }
                    Some(ControlMessage::Shutdown(ack)) => {
                        // Drain any remaining spans from the data channel
                        receiver.close();
                        while let Ok(span) = receiver.try_recv() {
                            buffer.push(span);
                        }
                        if !buffer.is_empty() {
                            flush_buffer(&mut buffer, &mut exporter, max_batch, export_timeout, &pending_count).await;
                        }
                        let _ = ack.send(());
                        log::info!("TokioSpanProcessor: shutdown complete, export loop exiting");
                        return;
                    }
                    None => {
                        // Control channel closed unexpectedly
                        return;
                    }
                }
            }
            // Periodic flush
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    flush_buffer(&mut buffer, &mut exporter, max_batch, export_timeout, &pending_count).await;
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
    pending_count: &AtomicUsize,
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
        // Decrement pending count after export (regardless of success/failure)
        pending_count.fetch_sub(batch_len, Ordering::Relaxed);
    }

    log::debug!("TokioSpanProcessor: flushed {} spans total", total);
}

impl SpanProcessor for TokioSpanProcessor {
    fn on_start(&self, _span: &mut opentelemetry_sdk::trace::Span, _cx: &opentelemetry::Context) {
        // No action needed on span start
    }

    fn on_end(&self, span: SpanData) {
        // Non-blocking send; drop span if queue is full
        match self.sender.try_send(span) {
            Ok(()) => {
                self.pending_count.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.dropped_count.fetch_add(1, Ordering::Relaxed);
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        log::debug!("TokioSpanProcessor: queue full, dropping span");
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        log::warn!(
                            "TokioSpanProcessor: channel closed, dropping span (export loop may have exited)"
                        );
                    }
                }
            }
        }
    }

    fn force_flush(&self) -> TraceResult<()> {
        // Send a flush request and block until the export loop acknowledges it.
        // Use a oneshot channel for synchronization.
        let (tx, rx) = oneshot::channel();
        if self.control_tx.try_send(ControlMessage::Flush(tx)).is_ok() {
            // Block on the response. This is called from a sync context
            // (SpanProcessor trait is not async), so we must block.
            // The export loop will acknowledge after flushing.
            let _ = std::thread::spawn(move || {
                let rt = tokio::runtime::Handle::try_current();
                if let Ok(handle) = rt {
                    handle.block_on(async {
                        let _ = rx.await;
                    });
                }
            })
            .join();
        }
        Ok(())
    }

    fn shutdown(&mut self) -> TraceResult<()> {
        let (tx, rx) = oneshot::channel();
        if self
            .control_tx
            .try_send(ControlMessage::Shutdown(tx))
            .is_ok()
        {
            let _ = std::thread::spawn(move || {
                let rt = tokio::runtime::Handle::try_current();
                if let Ok(handle) = rt {
                    handle.block_on(async {
                        let _ = rx.await;
                    });
                }
            })
            .join();
        }
        Ok(())
    }
}
