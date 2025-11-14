/// Benchmark Validation Module
///
/// Provides utilities for validating server metrics and sampled records
/// during performance benchmark tests. Ensures that benchmarks not only
/// measure performance but also validate correctness.
///
/// Supports Prometheus metrics export for observability
use std::sync::atomic::{AtomicU64, Ordering};
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Record validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub fn add_error(&mut self, error: String) {
        self.is_valid = false;
        self.errors.push(error);
    }

    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// Metrics validation structure
#[derive(Debug)]
pub struct MetricsValidation {
    pub records_processed: usize,
    pub batches_processed: usize,
    pub errors: usize,
    pub avg_batch_size: usize,
    pub is_valid: bool,
    pub validation_errors: Vec<String>,
}

impl MetricsValidation {
    /// Validate server metrics
    pub fn validate_metrics(
        records_processed: usize,
        batches_processed: usize,
        errors: usize,
    ) -> Self {
        let mut validation_errors = Vec::new();

        if records_processed == 0 {
            validation_errors.push("Records processed must be > 0".to_string());
        }

        if batches_processed == 0 {
            validation_errors.push("Batches processed must be > 0".to_string());
        }

        if errors != 0 {
            validation_errors.push(format!("Expected 0 errors, got {}", errors));
        }

        let avg_batch_size = if batches_processed > 0 {
            records_processed / batches_processed
        } else {
            0
        };

        if avg_batch_size == 0 && records_processed > 0 {
            validation_errors.push("Average batch size must be positive".to_string());
        }

        let is_valid = validation_errors.is_empty();

        Self {
            records_processed,
            batches_processed,
            errors,
            avg_batch_size,
            is_valid,
            validation_errors,
        }
    }

    pub fn print_results(&self) {
        println!("📈 Metrics Validation:");
        println!("  Records processed: {}", self.records_processed);
        println!("  Batches processed: {}", self.batches_processed);
        println!("  Average batch size: {}", self.avg_batch_size);
        println!("  Errors: {}", self.errors);

        if self.is_valid {
            println!("  ✅ All metrics valid");
        } else {
            println!(
                "  ❌ {} validation errors found:",
                self.validation_errors.len()
            );
            for error in &self.validation_errors {
                println!("    - {}", error);
            }
        }
    }
}

/// Validate sampled records for correctness
pub fn validate_records(samples: &[StreamRecord]) -> ValidationResult {
    let mut result = ValidationResult::new();

    if samples.is_empty() {
        result.add_warning("No samples collected for validation".to_string());
        return result;
    }

    for (idx, record) in samples.iter().enumerate() {
        // Validate that record has fields
        if record.fields.is_empty() {
            result.add_error(format!("Sample {}: Record has no fields", idx));
            continue;
        }

        // Check for NaN or Inf values in floats
        for (key, value) in &record.fields {
            if let FieldValue::Float(f) = value {
                if !f.is_finite() {
                    result.add_error(format!(
                        "Sample {}: Field '{}' has invalid value: {}",
                        idx, key, f
                    ));
                }
                if *f < 0.0 {
                    result.add_warning(format!(
                        "Sample {}: Field '{}' is negative: {}",
                        idx, key, f
                    ));
                }
            }

            // Check for empty strings
            if let FieldValue::String(s) = value {
                if s.is_empty() {
                    result.add_error(format!("Sample {}: Field '{}' is empty string", idx, key));
                }
            }

            // Check for invalid integers
            if let FieldValue::Integer(i) = value {
                if *i < 0 {
                    result.add_warning(format!(
                        "Sample {}: Field '{}' is negative integer: {}",
                        idx, key, i
                    ));
                }
            }
        }
    }

    result
}

/// Print validation results in a standard format
pub fn print_validation_results(
    samples: &[StreamRecord],
    validation: &ValidationResult,
    sample_rate: usize,
) {
    println!("\n🎯 Record Sampling (1 in {}):", sample_rate);
    println!("  Sampled records: {}", samples.len());

    if validation.is_valid {
        println!("  ✅ All sampled records valid");

        if !samples.is_empty() {
            println!("\n  📋 First sampled record fields:");
            let first_sample = &samples[0];
            for (key, value) in &first_sample.fields {
                match value {
                    FieldValue::String(s) => println!("    {}: \"{}\"", key, s),
                    FieldValue::Integer(i) => println!("    {}: {}", key, i),
                    FieldValue::Float(f) => println!("    {}: {:.2}", key, f),
                    FieldValue::Boolean(b) => println!("    {}: {}", key, b),
                    _ => println!("    {}: {:?}", key, value),
                }
            }
        }
    } else {
        println!("  ❌ {} validation errors found:", validation.errors.len());
        for error in &validation.errors {
            println!("    - {}", error);
        }
    }

    if validation.has_warnings() {
        println!("\n  ⚠️  {} warnings:", validation.warnings.len());
        for warning in validation.warnings.iter().take(5) {
            println!("    - {}", warning);
        }
        if validation.warnings.len() > 5 {
            println!("    ... and {} more", validation.warnings.len() - 5);
        }
    }
}

/// Prometheus metrics for benchmark validation
///
/// Provides Prometheus-compatible metrics for monitoring validation during profiling tests
#[derive(Debug)]
pub struct ValidationMetrics {
    validation_errors_total: AtomicU64,
    validation_warnings_total: AtomicU64,
    records_sampled_total: AtomicU64,
    validation_passes: AtomicU64,
    validation_failures: AtomicU64,
}

impl ValidationMetrics {
    /// Create new validation metrics
    pub fn new() -> Self {
        Self {
            validation_errors_total: AtomicU64::new(0),
            validation_warnings_total: AtomicU64::new(0),
            records_sampled_total: AtomicU64::new(0),
            validation_passes: AtomicU64::new(0),
            validation_failures: AtomicU64::new(0),
        }
    }

    /// Record validation result metrics
    pub fn record_validation(&self, validation: &ValidationResult, sampled_count: usize) {
        // Update error and warning counters
        self.validation_errors_total
            .fetch_add(validation.errors.len() as u64, Ordering::SeqCst);
        self.validation_warnings_total
            .fetch_add(validation.warnings.len() as u64, Ordering::SeqCst);

        // Update sampled records counter
        self.records_sampled_total
            .fetch_add(sampled_count as u64, Ordering::SeqCst);

        // Update pass/fail counters
        if validation.is_valid {
            self.validation_passes.fetch_add(1, Ordering::SeqCst);
        } else {
            self.validation_failures.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Export metrics in Prometheus text format
    pub fn export_prometheus_format(&self) -> String {
        let mut output = String::new();
        output.push_str("# HELP validation_errors_total Total validation errors detected\n");
        output.push_str("# TYPE validation_errors_total counter\n");
        output.push_str(&format!(
            "validation_errors_total {}\n",
            self.validation_errors_total.load(Ordering::SeqCst)
        ));

        output.push_str("# HELP validation_warnings_total Total validation warnings detected\n");
        output.push_str("# TYPE validation_warnings_total counter\n");
        output.push_str(&format!(
            "validation_warnings_total {}\n",
            self.validation_warnings_total.load(Ordering::SeqCst)
        ));

        output.push_str("# HELP records_sampled_total Total records sampled for validation\n");
        output.push_str("# TYPE records_sampled_total counter\n");
        output.push_str(&format!(
            "records_sampled_total {}\n",
            self.records_sampled_total.load(Ordering::SeqCst)
        ));

        output.push_str("# HELP validation_passes_total Total validation passes\n");
        output.push_str("# TYPE validation_passes_total counter\n");
        output.push_str(&format!(
            "validation_passes_total {}\n",
            self.validation_passes.load(Ordering::SeqCst)
        ));

        output.push_str("# HELP validation_failures_total Total validation failures\n");
        output.push_str("# TYPE validation_failures_total counter\n");
        output.push_str(&format!(
            "validation_failures_total {}\n",
            self.validation_failures.load(Ordering::SeqCst)
        ));

        output
    }

    /// Print metrics summary
    pub fn print_summary(&self) {
        println!("\n📊 Validation Metrics Summary:");
        println!(
            "  Errors detected: {}",
            self.validation_errors_total.load(Ordering::SeqCst)
        );
        println!(
            "  Warnings detected: {}",
            self.validation_warnings_total.load(Ordering::SeqCst)
        );
        println!(
            "  Records sampled: {}",
            self.records_sampled_total.load(Ordering::SeqCst)
        );
        println!(
            "  Validation passes: {}",
            self.validation_passes.load(Ordering::SeqCst)
        );
        println!(
            "  Validation failures: {}",
            self.validation_failures.load(Ordering::SeqCst)
        );
    }
}

impl Default for ValidationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_validation_valid() {
        let metrics = MetricsValidation::validate_metrics(1000, 10, 0);
        assert!(metrics.is_valid);
        assert_eq!(metrics.avg_batch_size, 100);
    }

    #[test]
    fn test_metrics_validation_invalid() {
        let metrics = MetricsValidation::validate_metrics(0, 10, 5);
        assert!(!metrics.is_valid);
        assert!(metrics.validation_errors.len() >= 2);
    }

    #[test]
    fn test_record_validation() {
        let mut record = StreamRecord::new(std::collections::HashMap::new());
        record
            .fields
            .insert("price".to_string(), FieldValue::Float(100.0));
        record
            .fields
            .insert("name".to_string(), FieldValue::String("test".to_string()));

        let result = validate_records(&[record]);
        assert!(result.is_valid);
    }

    #[test]
    fn test_record_validation_nan() {
        let mut record = StreamRecord::new(std::collections::HashMap::new());
        record
            .fields
            .insert("price".to_string(), FieldValue::Float(f64::NAN));

        let result = validate_records(&[record]);
        assert!(!result.is_valid);
        assert!(!result.errors.is_empty());
    }
}
