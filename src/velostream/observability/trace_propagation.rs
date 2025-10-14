//! OpenTelemetry trace context propagation via Kafka headers
//!
//! This module implements W3C Trace Context propagation for distributed tracing
//! across Kafka message boundaries.
//!
//! References:
//! - W3C Trace Context: https://www.w3.org/TR/trace-context/
//! - OpenTelemetry Propagation: https://opentelemetry.io/docs/specs/otel/context/api-propagators/

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use std::collections::HashMap;
use std::str::FromStr;

/// W3C Trace Context header name for trace parent
const TRACEPARENT_HEADER: &str = "traceparent";

/// W3C Trace Context header name for trace state
const TRACESTATE_HEADER: &str = "tracestate";

/// Extract OpenTelemetry span context from Kafka message headers
///
/// Implements W3C Trace Context extraction from `traceparent` header.
/// Format: `00-{trace_id}-{span_id}-{trace_flags}`
///
/// # Arguments
/// * `headers` - Kafka message headers as HashMap<String, Vec<u8>>
///
/// # Returns
/// * `Some(SpanContext)` if valid trace context found in headers
/// * `None` if no trace context or invalid format
pub fn extract_trace_context(headers: &HashMap<String, Vec<u8>>) -> Option<SpanContext> {
    // Look for traceparent header (case-insensitive)
    let traceparent_value = headers
        .iter()
        .find(|(k, _)| k.to_lowercase() == TRACEPARENT_HEADER)
        .and_then(|(_, v)| String::from_utf8(v.clone()).ok())?;

    log::debug!(
        "🔍 Found traceparent header: {}",
        traceparent_value
    );

    // Parse W3C traceparent format: 00-{trace_id}-{span_id}-{trace_flags}
    let parts: Vec<&str> = traceparent_value.split('-').collect();
    if parts.len() != 4 {
        log::warn!(
            "⚠️  Invalid traceparent format (expected 4 parts): {}",
            traceparent_value
        );
        return None;
    }

    // Validate version (00)
    if parts[0] != "00" {
        log::warn!("⚠️  Unsupported trace context version: {}", parts[0]);
        return None;
    }

    // Parse trace ID (32 hex chars)
    let trace_id = TraceId::from_hex(parts[1]).ok()?;
    if !trace_id.is_valid() {
        log::warn!("⚠️  Invalid trace ID: {}", parts[1]);
        return None;
    }

    // Parse span ID (16 hex chars)
    let span_id = SpanId::from_hex(parts[2]).ok()?;
    if !span_id.is_valid() {
        log::warn!("⚠️  Invalid span ID: {}", parts[2]);
        return None;
    }

    // Parse trace flags (2 hex chars)
    let trace_flags = u8::from_str_radix(parts[3], 16).ok()?;
    let flags = TraceFlags::new(trace_flags);

    // Extract tracestate if present
    let trace_state = headers
        .iter()
        .find(|(k, _)| k.to_lowercase() == TRACESTATE_HEADER)
        .and_then(|(_, v)| String::from_utf8(v.clone()).ok())
        .and_then(|s| TraceState::from_str(&s).ok())
        .unwrap_or_else(TraceState::default);

    log::info!(
        "✅ Extracted trace context: trace_id={}, span_id={}, sampled={}",
        trace_id,
        span_id,
        flags.is_sampled()
    );

    Some(SpanContext::new(
        trace_id,
        span_id,
        flags,
        true, // is_remote: true (this context came from upstream)
        trace_state,
    ))
}

/// Inject OpenTelemetry span context into Kafka message headers
///
/// Implements W3C Trace Context injection into `traceparent` header.
/// Format: `00-{trace_id}-{span_id}-{trace_flags}`
///
/// # Arguments
/// * `span_context` - The span context to inject
/// * `headers` - Mutable reference to Kafka message headers
pub fn inject_trace_context(
    span_context: &SpanContext,
    headers: &mut HashMap<String, Vec<u8>>,
) {
    if !span_context.is_valid() {
        log::debug!("⚠️  Cannot inject invalid span context");
        return;
    }

    // Format W3C traceparent: 00-{trace_id}-{span_id}-{trace_flags}
    let traceparent = format!(
        "00-{}-{}-{:02x}",
        span_context.trace_id(),
        span_context.span_id(),
        span_context.trace_flags().to_u8()
    );

    log::debug!(
        "🔍 Injecting traceparent header: {}",
        traceparent
    );

    // Insert traceparent header
    headers.insert(
        TRACEPARENT_HEADER.to_string(),
        traceparent.into_bytes(),
    );

    // Insert tracestate header if present
    if !span_context.trace_state().is_empty() {
        let tracestate = span_context.trace_state().header();
        headers.insert(
            TRACESTATE_HEADER.to_string(),
            tracestate.into_bytes(),
        );
        log::debug!(
            "🔍 Injecting tracestate header: {}",
            tracestate
        );
    }

    log::info!(
        "✅ Injected trace context: trace_id={}, span_id={}",
        span_context.trace_id(),
        span_context.span_id()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_valid_trace_context() {
        let mut headers = HashMap::new();
        headers.insert(
            "traceparent".to_string(),
            b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_vec(),
        );

        let context = extract_trace_context(&headers);
        assert!(context.is_some());

        let ctx = context.unwrap();
        assert_eq!(
            format!("{}", ctx.trace_id()),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(format!("{}", ctx.span_id()), "00f067aa0ba902b7");
        assert!(ctx.trace_flags().is_sampled());
    }

    #[test]
    fn test_extract_missing_trace_context() {
        let headers = HashMap::new();
        let context = extract_trace_context(&headers);
        assert!(context.is_none());
    }

    #[test]
    fn test_extract_invalid_format() {
        let mut headers = HashMap::new();
        headers.insert(
            "traceparent".to_string(),
            b"invalid-format".to_vec(),
        );

        let context = extract_trace_context(&headers);
        assert!(context.is_none());
    }

    #[test]
    fn test_inject_trace_context() {
        let trace_id = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
        let span_id = SpanId::from_hex("00f067aa0ba902b7").unwrap();
        let flags = TraceFlags::new(1); // Sampled
        let trace_state = TraceState::default();

        let span_context = SpanContext::new(trace_id, span_id, flags, false, trace_state);

        let mut headers = HashMap::new();
        inject_trace_context(&span_context, &mut headers);

        assert!(headers.contains_key("traceparent"));

        let traceparent = String::from_utf8(headers["traceparent"].clone()).unwrap();
        assert_eq!(
            traceparent,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }

    #[test]
    fn test_roundtrip_trace_context() {
        let trace_id = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
        let span_id = SpanId::from_hex("00f067aa0ba902b7").unwrap();
        let flags = TraceFlags::new(1);
        let trace_state = TraceState::default();

        let original_context = SpanContext::new(trace_id, span_id, flags, false, trace_state);

        // Inject
        let mut headers = HashMap::new();
        inject_trace_context(&original_context, &mut headers);

        // Extract
        let extracted_context = extract_trace_context(&headers).unwrap();

        // Verify roundtrip
        assert_eq!(extracted_context.trace_id(), original_context.trace_id());
        assert_eq!(extracted_context.span_id(), original_context.span_id());
        assert_eq!(
            extracted_context.trace_flags().to_u8(),
            original_context.trace_flags().to_u8()
        );
    }
}
