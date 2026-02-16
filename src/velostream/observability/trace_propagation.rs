//! OpenTelemetry trace context propagation via Kafka headers
//!
//! This module implements W3C Trace Context propagation for distributed tracing
//! across Kafka message boundaries.
//!
//! References:
//! - W3C Trace Context: https://www.w3.org/TR/trace-context/
//! - OpenTelemetry Propagation: https://opentelemetry.io/docs/specs/otel/context/api-propagators/

use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
use rand::Rng;
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
/// * `headers` - Kafka message headers as HashMap<String, String>
///
/// # Returns
/// * `Some(SpanContext)` if valid trace context found in headers
/// * `None` if no trace context or invalid format
pub fn extract_trace_context(headers: &HashMap<String, String>) -> Option<SpanContext> {
    // Look for traceparent header (case-insensitive, allocation-free comparison)
    let traceparent_value = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(TRACEPARENT_HEADER))
        .map(|(_, v)| v.as_str())?;

    log::debug!("🔍 Found traceparent header: {}", traceparent_value);

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

    // Parse span ID (16 hex chars)
    let span_id = SpanId::from_hex(parts[2]).ok()?;

    // Parse trace flags (2 hex chars)
    let trace_flags = u8::from_str_radix(parts[3], 16).ok()?;
    let flags = TraceFlags::new(trace_flags);

    // Extract tracestate if present (allocation-free comparison)
    let trace_state = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(TRACESTATE_HEADER))
        .and_then(|(_, v)| TraceState::from_str(v.as_str()).ok())
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
pub fn inject_trace_context(span_context: &SpanContext, headers: &mut HashMap<String, String>) {
    // Format W3C traceparent: 00-{trace_id}-{span_id}-{trace_flags}
    let traceparent = format!(
        "00-{}-{}-{:02x}",
        span_context.trace_id(),
        span_context.span_id(),
        span_context.trace_flags().to_u8()
    );

    log::debug!("🔍 Injecting traceparent header: {}", traceparent);

    // Insert traceparent header
    headers.insert(TRACEPARENT_HEADER.to_string(), traceparent);

    // Insert tracestate header if present (OpenTelemetry 0.21 has header() method)
    let tracestate = span_context.trace_state().header();
    if !tracestate.is_empty() {
        headers.insert(TRACESTATE_HEADER.to_string(), tracestate.to_string());
        log::debug!("🔍 Injecting tracestate header: {}", tracestate);
    }

    log::debug!(
        "✅ Injected trace context: trace_id={}, span_id={}",
        span_context.trace_id(),
        span_context.span_id()
    );
}

/// Check if the traceparent flag byte indicates this record is sampled.
///
/// Returns `Some(true)` if the sampled flag (bit 0) is set, `Some(false)` if not,
/// or `None` if no traceparent header is present.
pub fn is_sampled(headers: &HashMap<String, String>) -> Option<bool> {
    let traceparent_value = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(TRACEPARENT_HEADER))
        .map(|(_, v)| v.as_str())?;

    let parts: Vec<&str> = traceparent_value.split('-').collect();
    if parts.len() != 4 {
        return None;
    }

    let trace_flags = u8::from_str_radix(parts[3], 16).ok()?;
    Some(trace_flags & 0x01 == 1)
}

/// Determine whether a record should be sampled for tracing.
///
/// Decision logic:
/// 1. If traceparent exists with sampled flag (01) -> true (continue sampled chain)
/// 2. If traceparent exists without sampled flag (00) -> false (continue unsampled chain)
/// 3. If no traceparent -> probabilistic: roll dice against `sampling_ratio`
pub fn should_sample_record(headers: &HashMap<String, String>, sampling_ratio: f64) -> bool {
    match is_sampled(headers) {
        Some(sampled) => sampled,
        None => {
            // No traceparent: probabilistic head-based sampling
            if sampling_ratio >= 1.0 {
                true
            } else if sampling_ratio <= 0.0 {
                false
            } else {
                rand::thread_rng().r#gen::<f64>() < sampling_ratio
            }
        }
    }
}

/// Make a sampling decision for a record and extract trace context if sampled.
///
/// This performs a single-pass lookup of the traceparent header, avoiding the
/// double-scan that would occur with separate `has_traceparent` + `extract_trace_context` calls.
///
/// Decision logic:
/// 1. Traceparent present with flag=01 → `Sampled(Some(ctx))` — continue sampled chain
/// 2. Traceparent present with flag=00 → `NotSampled` — continue unsampled chain
/// 3. Traceparent present but invalid → `NotSampled` — treat as unsampled
/// 4. No traceparent + dice roll passes → `Sampled(None)` — new root span
/// 5. No traceparent + dice roll fails → `NotSampledNew` — must inject flag=00
pub fn extract_trace_context_if_sampled(
    headers: &HashMap<String, String>,
    sampling_ratio: f64,
) -> SamplingDecision {
    // Single-pass: find the traceparent value directly
    let traceparent_value = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(TRACEPARENT_HEADER))
        .map(|(_, v)| v.as_str());

    match traceparent_value {
        Some(value) => {
            // Traceparent exists — parse inline (avoids second header scan via extract_trace_context)
            let parts: Vec<&str> = value.split('-').collect();
            if parts.len() != 4 || parts[0] != "00" {
                return SamplingDecision::NotSampled;
            }

            let trace_flags = match u8::from_str_radix(parts[3], 16) {
                Ok(f) => f,
                Err(_) => return SamplingDecision::NotSampled,
            };

            if trace_flags & 0x01 == 0 {
                // Flag=00: not sampled, no need to parse trace_id/span_id
                return SamplingDecision::NotSampled;
            }

            // Flag=01: sampled — parse full context for parent linking
            let trace_id = match TraceId::from_hex(parts[1]) {
                Ok(id) => id,
                Err(_) => return SamplingDecision::NotSampled,
            };
            let span_id = match SpanId::from_hex(parts[2]) {
                Ok(id) => id,
                Err(_) => return SamplingDecision::NotSampled,
            };

            let flags = TraceFlags::new(trace_flags);
            let trace_state = headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(TRACESTATE_HEADER))
                .and_then(|(_, v)| TraceState::from_str(v.as_str()).ok())
                .unwrap_or_else(TraceState::default);

            let ctx = SpanContext::new(trace_id, span_id, flags, true, trace_state);
            SamplingDecision::Sampled(Some(ctx))
        }
        None => {
            // No traceparent: probabilistic head-based sampling
            if should_sample_record(headers, sampling_ratio) {
                SamplingDecision::Sampled(None) // New root span
            } else {
                SamplingDecision::NotSampledNew // Must inject flag=00 to prevent downstream re-roll
            }
        }
    }
}

/// Result of a per-record sampling decision.
#[derive(Debug)]
pub enum SamplingDecision {
    /// Record should be sampled. Contains upstream SpanContext if present.
    Sampled(Option<SpanContext>),
    /// Record should NOT be sampled — upstream traceparent already had flag=00.
    /// Headers carry the decision naturally through the SQL engine; no injection needed.
    NotSampled,
    /// Record should NOT be sampled — no traceparent existed, dice roll failed.
    /// Processors MUST inject a traceparent with flag=00 into outputs to prevent
    /// downstream processors from re-rolling the dice.
    NotSampledNew,
}

/// Inject a "not sampled" traceparent into record headers.
///
/// Generates a random trace-id and span-id with flag=00. This tells downstream
/// processors that the sampling decision was already made and they should NOT
/// re-roll the dice.
///
/// Call this for `SamplingDecision::NotSampledNew` records.
pub fn inject_not_sampled_context(headers: &mut HashMap<String, String>) {
    let mut rng = rand::thread_rng();
    let trace_id = TraceId::from(rng.r#gen::<u128>());
    let span_id = SpanId::from(rng.r#gen::<u64>());
    let traceparent = format!("00-{}-{}-00", trace_id, span_id);

    headers.insert(TRACEPARENT_HEADER.to_string(), traceparent);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_valid_trace_context() {
        let mut headers = HashMap::new();
        headers.insert(
            "traceparent".to_string(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
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
        headers.insert("traceparent".to_string(), "invalid-format".to_string());

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

        let traceparent = &headers["traceparent"];
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
