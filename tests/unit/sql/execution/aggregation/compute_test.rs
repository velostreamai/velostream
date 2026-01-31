//! Tests for the shared compute module (Welford's algorithm and pure functions)

use velostream::velostream::sql::execution::aggregation::compute::*;
use velostream::velostream::sql::execution::types::FieldValue;

#[test]
fn test_welford_matches_naive() {
    let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
    let mut state = WelfordState::new();
    for &v in &values {
        state.update(v);
    }

    // Two-pass naive
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var_pop = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
    let var_samp =
        values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64;

    assert!((compute_avg_from_welford(&state).unwrap() - mean).abs() < 1e-10);
    assert!((compute_variance_from_welford(&state, false).unwrap() - var_pop).abs() < 1e-10);
    assert!((compute_variance_from_welford(&state, true).unwrap() - var_samp).abs() < 1e-10);
    assert!((compute_stddev_from_welford(&state, false).unwrap() - var_pop.sqrt()).abs() < 1e-10);
    assert!((compute_stddev_from_welford(&state, true).unwrap() - var_samp.sqrt()).abs() < 1e-10);
}

#[test]
fn test_welford_single_value() {
    let mut state = WelfordState::new();
    state.update(42.0);

    assert_eq!(state.count, 1);
    assert!((state.mean - 42.0).abs() < 1e-10);
    assert!((state.m2 - 0.0).abs() < 1e-10);

    assert_eq!(compute_avg_from_welford(&state), Some(42.0));
    // Sample variance needs n>=2
    assert_eq!(compute_variance_from_welford(&state, true), None);
    // Population variance of single value = 0
    assert_eq!(compute_variance_from_welford(&state, false), Some(0.0));
}

#[test]
fn test_welford_empty() {
    let state = WelfordState::new();
    assert_eq!(compute_avg_from_welford(&state), None);
    assert_eq!(compute_variance_from_welford(&state, true), None);
    assert_eq!(compute_variance_from_welford(&state, false), None);
    assert_eq!(compute_stddev_from_welford(&state, true), None);
    assert_eq!(compute_stddev_from_welford(&state, false), None);
}

#[test]
fn test_variance_from_values_sample_vs_pop() {
    let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
    let var_samp = compute_variance_from_values(&values, true).unwrap();
    let var_pop = compute_variance_from_values(&values, false).unwrap();

    // Population variance = 200.0, sample variance = 250.0
    assert!((var_pop - 200.0).abs() < 1e-10);
    assert!((var_samp - 250.0).abs() < 1e-10);
}

#[test]
fn test_stddev_from_values() {
    let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
    let sd_samp = compute_stddev_from_values(&values, true).unwrap();
    let sd_pop = compute_stddev_from_values(&values, false).unwrap();

    assert!((sd_pop - 200.0_f64.sqrt()).abs() < 1e-10);
    assert!((sd_samp - 250.0_f64.sqrt()).abs() < 1e-10);
}

#[test]
fn test_median_odd_even_empty() {
    assert_eq!(compute_median_from_values(&[]), None);
    assert_eq!(compute_median_from_values(&[5.0]), Some(5.0));
    assert_eq!(compute_median_from_values(&[1.0, 3.0, 5.0]), Some(3.0));
    assert_eq!(compute_median_from_values(&[1.0, 2.0, 3.0, 4.0]), Some(2.5));
}

#[test]
fn test_field_value_to_f64_all_variants() {
    assert_eq!(field_value_to_f64(&FieldValue::Integer(42)), Some(42.0));
    assert_eq!(field_value_to_f64(&FieldValue::Float(2.72)), Some(2.72));
    assert_eq!(
        field_value_to_f64(&FieldValue::ScaledInteger(12345, 2)),
        Some(123.45)
    );
    assert_eq!(
        field_value_to_f64(&FieldValue::String("hello".to_string())),
        None
    );
    assert_eq!(field_value_to_f64(&FieldValue::Boolean(true)), None);
    assert_eq!(field_value_to_f64(&FieldValue::Null), None);
}

#[test]
fn test_compute_sum_result_integer_vs_float() {
    // All integer inputs, whole sum -> Integer
    assert_eq!(
        compute_sum_result(42.0, true, true),
        FieldValue::Integer(42)
    );
    // Mixed inputs -> Float
    assert_eq!(
        compute_sum_result(42.5, false, true),
        FieldValue::Float(42.5)
    );
    // No values -> Null
    assert_eq!(compute_sum_result(0.0, true, false), FieldValue::Null);
    // All integer but fractional sum -> Float
    assert_eq!(
        compute_sum_result(42.5, true, true),
        FieldValue::Float(42.5)
    );
}
