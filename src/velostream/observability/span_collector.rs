/// Test-time span collection for verification without exporting
///
/// This module provides a simple span processor that collects all created spans
/// into an in-memory collection for testing and verification purposes.
use opentelemetry_sdk::export::trace::SpanData;
use opentelemetry_sdk::trace::{Span, SpanProcessor};
use std::sync::{Arc, Mutex};

/// A span processor that collects spans into an in-memory vector for testing
pub struct CollectingSpanProcessor {
    spans: Arc<Mutex<Vec<SpanData>>>,
}

/// Trait object wrapper for span processor
pub type BoxedSpanProcessor = Box<dyn SpanProcessor>;

impl Clone for CollectingSpanProcessor {
    fn clone(&self) -> Self {
        Self {
            spans: Arc::clone(&self.spans),
        }
    }
}

impl std::fmt::Debug for CollectingSpanProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectingSpanProcessor").finish()
    }
}

impl CollectingSpanProcessor {
    /// Create a new span collector
    pub fn new() -> Self {
        Self {
            spans: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get all collected spans
    pub fn spans(&self) -> Vec<SpanData> {
        self.spans.lock().expect("Lock poisoned").clone()
    }

    /// Get the number of collected spans
    pub fn span_count(&self) -> usize {
        self.spans.lock().expect("Lock poisoned").len()
    }

    /// Clear all collected spans
    pub fn clear(&self) {
        self.spans.lock().expect("Lock poisoned").clear();
    }
}

impl Default for CollectingSpanProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl SpanProcessor for CollectingSpanProcessor {
    fn on_start(&self, _span: &mut Span, _cx: &opentelemetry::Context) {
        // No action needed on span start
    }

    fn on_end(&self, span: SpanData) {
        let span_name = span.name.clone();
        let mut spans = self.spans.lock().expect("Lock poisoned");
        log::debug!("📝 Collecting span: {}", span_name);
        spans.push(span);
        log::debug!("📊 Total collected spans: {}", spans.len());
    }

    fn force_flush(&self) -> opentelemetry::trace::TraceResult<()> {
        log::debug!("🔄 Flushing span collector");
        Ok(())
    }

    fn shutdown(&mut self) -> opentelemetry::trace::TraceResult<()> {
        log::info!("🛑 Shutting down span collector");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_collector_creation() {
        let collector = CollectingSpanProcessor::new();
        assert_eq!(collector.span_count(), 0);
    }

    #[test]
    fn test_span_collector_clear() {
        let collector = CollectingSpanProcessor::new();
        collector.clear();
        assert_eq!(collector.span_count(), 0);
    }
}
