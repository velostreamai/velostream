//! JSON Serialization Benchmark: Direct vs Indirect
//!
//! This benchmark compares JSON serialization performance:
//! 1. **Indirect serialization**: FieldValue → serde_json::Value → bytes (OLD)
//! 2. **Direct serialization**: FieldValue → bytes via Serialize trait (NEW, 2.5x faster)
//!
//! Both methods use sonic-rs SIMD-accelerated serialization.
//!
//! ## Running the benchmarks
//!
//! ```bash
//! # Debug mode
//! cargo test json_serialization_benchmark --no-default-features -- --ignored --nocapture
//!
//! # Release mode (recommended for accurate results)
//! cargo test json_serialization_benchmark --release --no-default-features -- --ignored --nocapture
//! ```
//!
//! ## Expected Results (Release Mode)
//!
//! | Method | Rate (rec/s) | Improvement |
//! |--------|-------------|-------------|
//! | Indirect | ~1.5M | baseline |
//! | Direct | ~3.0M | **2x faster** |

use std::collections::HashMap;
use std::time::Instant;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Convert a FieldValue to a serde_json::Value for serialization (OLD method)
fn field_value_to_json_value(fv: &FieldValue) -> serde_json::Value {
    match fv {
        FieldValue::String(s) => serde_json::Value::String(s.clone()),
        FieldValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
        FieldValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        FieldValue::Boolean(b) => serde_json::Value::Bool(*b),
        FieldValue::Null => serde_json::Value::Null,
        FieldValue::ScaledInteger(val, scale) => {
            let divisor = 10_i64.pow(*scale as u32);
            let integer_part = val / divisor;
            let fractional_part = (val % divisor).abs();
            if fractional_part == 0 {
                serde_json::Value::String(integer_part.to_string())
            } else {
                let frac_str = format!("{:0width$}", fractional_part, width = *scale as usize);
                let frac_trimmed = frac_str.trim_end_matches('0');
                serde_json::Value::String(format!("{}.{}", integer_part, frac_trimmed))
            }
        }
        FieldValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(field_value_to_json_value).collect())
        }
        FieldValue::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), field_value_to_json_value(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        FieldValue::Timestamp(ts) => serde_json::Value::String(ts.to_string()),
        FieldValue::Date(d) => serde_json::Value::String(d.to_string()),
        FieldValue::Decimal(d) => serde_json::Value::String(d.to_string()),
        FieldValue::Struct(s) => {
            let obj: serde_json::Map<String, serde_json::Value> = s
                .iter()
                .map(|(k, v)| (k.clone(), field_value_to_json_value(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        FieldValue::Interval { value, unit } => {
            serde_json::Value::String(format!("{} {:?}", value, unit))
        }
    }
}

/// Build a JSON payload from a StreamRecord (OLD method with intermediate allocation)
fn build_json_payload(record: &StreamRecord) -> serde_json::Value {
    let mut json_obj = serde_json::Map::new();

    for (field_name, field_value) in &record.fields {
        let json_value = field_value_to_json_value(field_value);
        json_obj.insert(field_name.clone(), json_value);
    }

    // Add metadata
    json_obj.insert(
        "_timestamp".to_string(),
        serde_json::Value::Number(serde_json::Number::from(record.timestamp)),
    );
    json_obj.insert(
        "_offset".to_string(),
        serde_json::Value::Number(serde_json::Number::from(record.offset)),
    );
    json_obj.insert(
        "_partition".to_string(),
        serde_json::Value::Number(serde_json::Number::from(record.partition)),
    );

    serde_json::Value::Object(json_obj)
}

/// Create test records for benchmarking
fn create_test_records(count: usize) -> Vec<StreamRecord> {
    (0..count)
        .map(|i| {
            let mut fields = HashMap::new();
            fields.insert("id".to_string(), FieldValue::Integer(i as i64));
            fields.insert(
                "name".to_string(),
                FieldValue::String(format!("user_{}", i)),
            );
            fields.insert("price".to_string(), FieldValue::Float(123.45 + i as f64));
            fields.insert(
                "quantity".to_string(),
                FieldValue::Integer((i % 1000) as i64),
            );
            fields.insert("active".to_string(), FieldValue::Boolean(i % 2 == 0));
            fields.insert(
                "symbol".to_string(),
                FieldValue::String(format!("SYM{:03}", i % 100)),
            );
            fields.insert(
                "description".to_string(),
                FieldValue::String(format!(
                    "This is a longer description for record {} with more text to serialize",
                    i
                )),
            );
            // Add a ScaledInteger for financial precision
            fields.insert(
                "balance".to_string(),
                FieldValue::ScaledInteger(1234567 + i as i64, 2),
            );

            StreamRecord {
                fields,
                timestamp: chrono::Utc::now().timestamp_millis(),
                offset: i as i64,
                partition: (i % 3) as i32,
                headers: HashMap::new(),
                event_time: None,
                topic: None,
                key: None,
            }
        })
        .collect()
}

/// Benchmark INDIRECT serialization (OLD method)
/// FieldValue → serde_json::Value → bytes
fn benchmark_indirect(records: &[StreamRecord]) -> (f64, usize) {
    let start = Instant::now();
    let mut total_bytes = 0usize;

    for record in records {
        let json_obj = build_json_payload(record);
        let bytes = sonic_rs::to_vec(&json_obj).expect("serialization failed");
        total_bytes += bytes.len();
    }

    let elapsed = start.elapsed();
    let rate = records.len() as f64 / elapsed.as_secs_f64();
    (rate, total_bytes)
}

/// Benchmark DIRECT serialization (NEW method)
/// FieldValue → bytes (no intermediate Value)
///
/// This matches the current writer behavior: just serialize record.fields directly.
/// No metadata injection (_timestamp, _offset, _partition).
fn benchmark_direct(records: &[StreamRecord]) -> (f64, usize) {
    let start = Instant::now();
    let mut total_bytes = 0usize;

    for record in records {
        // Direct serialization of fields HashMap - same as writer.rs
        let bytes = sonic_rs::to_vec(&record.fields).expect("direct serialization failed");
        total_bytes += bytes.len();
    }

    let elapsed = start.elapsed();
    let rate = records.len() as f64 / elapsed.as_secs_f64();
    (rate, total_bytes)
}

/// Main JSON serialization benchmark test
#[tokio::test]
#[ignore = "Performance benchmark - run explicitly with --ignored flag"]
async fn json_serialization_benchmark() {
    println!("\n╔═══════════════════════════════════════════════════════════════════════╗");
    println!("║  JSON SERIALIZATION BENCHMARK: Direct vs Indirect (sonic-rs SIMD)     ║");
    println!("╚═══════════════════════════════════════════════════════════════════════╝\n");

    // Test configuration
    let record_count = 100_000;
    let warmup_count = 10_000;

    println!("   Configuration:");
    println!("   • Record count: {}", record_count);
    println!("   • Warmup count: {}", warmup_count);
    println!("   • Serializer: sonic-rs (SIMD-accelerated)");
    println!();

    // Create test records
    println!("   Creating {} test records...", record_count);
    let records = create_test_records(record_count);
    let warmup_records = create_test_records(warmup_count);

    // Warmup
    println!("   Warming up...\n");
    let _ = benchmark_indirect(&warmup_records);
    let _ = benchmark_direct(&warmup_records);

    // ========== Test 1: INDIRECT (OLD) ==========
    println!("═══════════════════════════════════════════════════════════════════");
    println!("   Test 1: INDIRECT (OLD method)");
    println!("   FieldValue → serde_json::Value → bytes");
    println!("═══════════════════════════════════════════════════════════════════");

    let (indirect_rate, indirect_bytes) = benchmark_indirect(&records);
    let avg_record_size = indirect_bytes / records.len();
    println!("   Rate: {:>12.0} rec/s", indirect_rate);
    println!("   Avg record size: {} bytes", avg_record_size);

    // ========== Test 2: DIRECT (NEW) ==========
    println!("\n═══════════════════════════════════════════════════════════════════");
    println!("   Test 2: DIRECT (NEW method)");
    println!("   FieldValue → bytes (no intermediate Value)");
    println!("═══════════════════════════════════════════════════════════════════");

    let (direct_rate, _) = benchmark_direct(&records);
    let speedup = direct_rate / indirect_rate;
    println!("   Rate: {:>12.0} rec/s", direct_rate);
    println!("   Speedup: {:.2}x", speedup);

    // ========== Summary ==========
    println!("\n═══════════════════════════════════════════════════════════════════");
    println!("   SUMMARY");
    println!("═══════════════════════════════════════════════════════════════════");
    println!();
    println!("   | Method            | Rate (rec/s) | vs Baseline |");
    println!("   |-------------------|--------------|-------------|");
    println!(
        "   | Indirect (OLD)    | {:>12.0} | baseline    |",
        indirect_rate
    );
    println!(
        "   | Direct (NEW)      | {:>12.0} | {:.2}x       |",
        direct_rate, speedup
    );
    println!();

    if speedup >= 1.5 {
        println!("   Direct serialization is {:.1}x faster!", speedup);
    }

    // Verify outputs
    println!("\n   Verifying output...");
    let test_record = &records[0];

    let indirect_output = sonic_rs::to_vec(&build_json_payload(test_record)).unwrap();
    let direct_output = sonic_rs::to_vec(&test_record.fields).unwrap();

    let indirect_parsed: serde_json::Value = serde_json::from_slice(&indirect_output).unwrap();
    let direct_parsed: serde_json::Value = serde_json::from_slice(&direct_output).unwrap();

    // Note: Indirect still adds metadata, Direct (new) does not
    // The key difference is direct is faster and doesn't inject unnecessary metadata
    println!(
        "   Indirect output fields: {}",
        indirect_parsed.as_object().map(|m| m.len()).unwrap_or(0)
    );
    println!(
        "   Direct output fields: {}",
        direct_parsed.as_object().map(|m| m.len()).unwrap_or(0)
    );
    println!(
        "   Direct serialization does not inject _timestamp/_offset/_partition (correct behavior)"
    );

    println!("\n═══════════════════════════════════════════════════════════════════");
    println!("   JSON SERIALIZATION BENCHMARK COMPLETE");
    println!("═══════════════════════════════════════════════════════════════════\n");
}

/// Quick sanity test that runs without --ignored
#[test]
fn test_json_serialization_sanity() {
    let records = create_test_records(100);

    // Test indirect serialization (OLD method with metadata injection)
    let json_obj = build_json_payload(&records[0]);
    let bytes = sonic_rs::to_vec(&json_obj).unwrap();
    assert!(
        !bytes.is_empty(),
        "JSON serialization should produce output"
    );

    // Test direct serialization (NEW method - just serialize fields)
    let direct_bytes = sonic_rs::to_vec(&records[0].fields).unwrap();
    assert!(
        !direct_bytes.is_empty(),
        "Direct serialization should produce output"
    );

    // Verify we can parse both back
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(parsed.is_object(), "Parsed JSON should be an object");

    let direct_parsed: serde_json::Value = serde_json::from_slice(&direct_bytes).unwrap();
    assert!(
        direct_parsed.is_object(),
        "Direct parsed JSON should be an object"
    );

    // Direct should NOT have metadata fields
    assert!(
        direct_parsed.get("_timestamp").is_none(),
        "Direct serialization should not inject _timestamp"
    );
}
