// Tests for per-record sampling decision logic and not-sampled propagation.
//
// Verifies:
// - `is_sampled()` parses traceparent flags correctly
// - `should_sample_record()` respects existing flags and sampling ratios
// - `extract_trace_context_if_sampled()` returns all 3 SamplingDecision variants
// - `inject_not_sampled_context()` generates valid flag=00 traceparent
// - Full chain: injected flag=00 prevents downstream re-rolling

use std::collections::HashMap;
use velostream::velostream::observability::trace_propagation::{self, SamplingDecision};

// ─── is_sampled() ────────────────────────────────────────────

#[test]
fn test_is_sampled_with_sampled_flag() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
    );
    assert_eq!(trace_propagation::is_sampled(&headers), Some(true));
}

#[test]
fn test_is_sampled_with_not_sampled_flag() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".to_string(),
    );
    assert_eq!(trace_propagation::is_sampled(&headers), Some(false));
}

#[test]
fn test_is_sampled_no_traceparent() {
    let headers = HashMap::new();
    assert_eq!(trace_propagation::is_sampled(&headers), None);
}

#[test]
fn test_is_sampled_invalid_traceparent() {
    let mut headers = HashMap::new();
    headers.insert("traceparent".to_string(), "garbage".to_string());
    assert_eq!(trace_propagation::is_sampled(&headers), None);
}

// ─── should_sample_record() ──────────────────────────────────

#[test]
fn test_should_sample_respects_sampled_flag_regardless_of_ratio() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
    );
    // Even with ratio 0.0, an already-sampled record continues sampled
    assert!(trace_propagation::should_sample_record(&headers, 0.0));
}

#[test]
fn test_should_sample_respects_not_sampled_flag_regardless_of_ratio() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".to_string(),
    );
    // Even with ratio 1.0, a not-sampled record stays not-sampled
    assert!(!trace_propagation::should_sample_record(&headers, 1.0));
}

#[test]
fn test_should_sample_ratio_zero_no_traceparent() {
    let headers = HashMap::new();
    assert!(!trace_propagation::should_sample_record(&headers, 0.0));
}

#[test]
fn test_should_sample_ratio_one_no_traceparent() {
    let headers = HashMap::new();
    assert!(trace_propagation::should_sample_record(&headers, 1.0));
}

// ─── extract_trace_context_if_sampled() ──────────────────────

#[test]
fn test_extract_if_sampled_returns_sampled_with_context() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
    );
    let decision = trace_propagation::extract_trace_context_if_sampled(&headers, 0.0);
    match decision {
        SamplingDecision::Sampled(Some(ctx)) => {
            assert!(ctx.trace_flags().is_sampled());
            assert_eq!(
                format!("{}", ctx.trace_id()),
                "4bf92f3577b34da6a3ce929d0e0e4736"
            );
        }
        other => panic!("Expected Sampled(Some), got {:?}", other),
    }
}

#[test]
fn test_extract_if_sampled_returns_not_sampled_for_flag_00() {
    let mut headers = HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".to_string(),
    );
    let decision = trace_propagation::extract_trace_context_if_sampled(&headers, 1.0);
    assert!(
        matches!(decision, SamplingDecision::NotSampled),
        "Expected NotSampled, got {:?}",
        decision
    );
}

#[test]
fn test_extract_if_sampled_returns_sampled_none_for_new_root() {
    // No traceparent + ratio 1.0 → Sampled(None) — new root span
    let headers = HashMap::new();
    let decision = trace_propagation::extract_trace_context_if_sampled(&headers, 1.0);
    match decision {
        SamplingDecision::Sampled(None) => {} // correct
        other => panic!("Expected Sampled(None), got {:?}", other),
    }
}

#[test]
fn test_extract_if_sampled_returns_not_sampled_new_for_dice_fail() {
    // No traceparent + ratio 0.0 → NotSampledNew — must inject flag=00
    let headers = HashMap::new();
    let decision = trace_propagation::extract_trace_context_if_sampled(&headers, 0.0);
    assert!(
        matches!(decision, SamplingDecision::NotSampledNew),
        "Expected NotSampledNew, got {:?}",
        decision
    );
}

// ─── inject_not_sampled_context() ────────────────────────────

#[test]
fn test_inject_not_sampled_produces_valid_traceparent() {
    let mut headers = HashMap::new();
    trace_propagation::inject_not_sampled_context(&mut headers);

    let traceparent = headers.get("traceparent").expect("Should have traceparent");

    // Format: 00-{32 hex}-{16 hex}-00
    let parts: Vec<&str> = traceparent.split('-').collect();
    assert_eq!(parts.len(), 4, "traceparent should have 4 parts");
    assert_eq!(parts[0], "00", "Version should be 00");
    assert_eq!(parts[1].len(), 32, "Trace ID should be 32 hex chars");
    assert_eq!(parts[2].len(), 16, "Span ID should be 16 hex chars");
    assert_eq!(parts[3], "00", "Flags should be 00 (not sampled)");
}

#[test]
fn test_inject_not_sampled_generates_unique_ids() {
    let mut headers1 = HashMap::new();
    let mut headers2 = HashMap::new();

    trace_propagation::inject_not_sampled_context(&mut headers1);
    trace_propagation::inject_not_sampled_context(&mut headers2);

    let tp1 = headers1.get("traceparent").unwrap();
    let tp2 = headers2.get("traceparent").unwrap();

    assert_ne!(tp1, tp2, "Injected traceparents should be unique");
}

// ─── Full chain: not-sampled propagation ─────────────────────

#[test]
fn test_injected_not_sampled_prevents_downstream_reroll() {
    // Simulate: first processor sees no traceparent, dice fails → injects flag=00
    let mut headers = HashMap::new();
    let decision = trace_propagation::extract_trace_context_if_sampled(&headers, 0.0);
    assert!(matches!(decision, SamplingDecision::NotSampledNew));

    // Inject the not-sampled context (as processors do for NotSampledNew)
    trace_propagation::inject_not_sampled_context(&mut headers);

    // Now simulate downstream processor receiving these headers.
    // Even with ratio=1.0, it should NOT sample because flag=00 is present.
    let downstream_decision = trace_propagation::extract_trace_context_if_sampled(&headers, 1.0);

    assert!(
        matches!(downstream_decision, SamplingDecision::NotSampled),
        "Downstream should see NotSampled (flag=00 from upstream), got {:?}",
        downstream_decision
    );
}

#[test]
fn test_sampled_chain_propagates_through_injection() {
    // Simulate: first processor sees no traceparent, dice passes → Sampled(None)
    let headers = HashMap::new();
    let decision = trace_propagation::extract_trace_context_if_sampled(&headers, 1.0);
    assert!(matches!(decision, SamplingDecision::Sampled(None)));

    // Processor creates a span and injects context with flag=01.
    // Simulate by manually injecting a sampled traceparent.
    let mut downstream_headers = HashMap::new();
    downstream_headers.insert(
        "traceparent".to_string(),
        "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01".to_string(),
    );

    // Downstream sees the sampled flag and continues the chain
    let downstream_decision =
        trace_propagation::extract_trace_context_if_sampled(&downstream_headers, 0.0);

    match downstream_decision {
        SamplingDecision::Sampled(Some(ctx)) => {
            assert!(ctx.trace_flags().is_sampled());
        }
        other => panic!("Downstream should continue sampled chain, got {:?}", other),
    }
}
