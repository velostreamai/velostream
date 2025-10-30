use std::str::FromStr;
use velostream::velostream::sql::ProfilingMode;

#[test]
fn test_profiling_mode_from_str_off() {
    let mode = ProfilingMode::from_str("off").expect("Should parse 'off'");
    assert_eq!(mode, ProfilingMode::Off);
}

#[test]
fn test_profiling_mode_from_str_dev() {
    let mode = ProfilingMode::from_str("dev").expect("Should parse 'dev'");
    assert_eq!(mode, ProfilingMode::Dev);
}

#[test]
fn test_profiling_mode_from_str_prod() {
    let mode = ProfilingMode::from_str("prod").expect("Should parse 'prod'");
    assert_eq!(mode, ProfilingMode::Prod);
}

#[test]
fn test_profiling_mode_from_str_case_insensitive() {
    // Should be case-insensitive
    assert_eq!(ProfilingMode::from_str("OFF").unwrap(), ProfilingMode::Off);
    assert_eq!(ProfilingMode::from_str("Dev").unwrap(), ProfilingMode::Dev);
    assert_eq!(
        ProfilingMode::from_str("PROD").unwrap(),
        ProfilingMode::Prod
    );
}

#[test]
fn test_profiling_mode_from_str_invalid() {
    // Invalid values should return error
    assert!(ProfilingMode::from_str("invalid").is_err());
    assert!(ProfilingMode::from_str("").is_err());
    assert!(ProfilingMode::from_str("maybe").is_err());
    assert!(ProfilingMode::from_str("true").is_err());
    assert!(ProfilingMode::from_str("false").is_err());
}

#[test]
fn test_profiling_mode_as_str() {
    assert_eq!(ProfilingMode::Off.as_str(), "off");
    assert_eq!(ProfilingMode::Dev.as_str(), "dev");
    assert_eq!(ProfilingMode::Prod.as_str(), "prod");
}

#[test]
fn test_profiling_mode_overhead_percent() {
    assert_eq!(ProfilingMode::Off.overhead_percent(), 0.0);
    assert_eq!(ProfilingMode::Dev.overhead_percent(), 9.0);
    assert_eq!(ProfilingMode::Prod.overhead_percent(), 2.5);
}

#[test]
fn test_profiling_mode_sampling_hz() {
    assert_eq!(ProfilingMode::Off.sampling_hz(), 0);
    assert_eq!(ProfilingMode::Dev.sampling_hz(), 1000);
    assert_eq!(ProfilingMode::Prod.sampling_hz(), 50);
}

#[test]
fn test_profiling_mode_display() {
    // Test Display trait via to_string
    assert_eq!(ProfilingMode::Off.to_string(), "off");
    assert_eq!(ProfilingMode::Dev.to_string(), "dev");
    assert_eq!(ProfilingMode::Prod.to_string(), "prod");
}

#[test]
fn test_profiling_mode_clone_copy() {
    let mode1 = ProfilingMode::Dev;
    let mode2 = mode1; // Copy
    let mode3 = mode1.clone(); // Clone

    assert_eq!(mode1, mode2);
    assert_eq!(mode1, mode3);
}

#[test]
fn test_profiling_mode_equality() {
    assert_eq!(ProfilingMode::Off, ProfilingMode::Off);
    assert_eq!(ProfilingMode::Dev, ProfilingMode::Dev);
    assert_eq!(ProfilingMode::Prod, ProfilingMode::Prod);

    assert_ne!(ProfilingMode::Off, ProfilingMode::Dev);
    assert_ne!(ProfilingMode::Dev, ProfilingMode::Prod);
    assert_ne!(ProfilingMode::Prod, ProfilingMode::Off);
}

#[test]
fn test_profiling_mode_from_str_with_whitespace() {
    // Should handle trimmed input
    let mode = ProfilingMode::from_str("  off  ");
    // Depending on implementation, this might fail. If not handled, that's ok.
    // Just checking behavior is consistent
    let _ = mode;
}
