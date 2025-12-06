//! JSON Deserialization Performance Benchmark
//!
//! Compares deserialization approaches:
//! 1. **Old**: bytes → serde_json::Value → json_to_field_value → HashMap (slow)
//! 2. **New**: bytes → sonic_rs::from_slice → HashMap<String, FieldValue> directly (fast)
//!
//! Run with:
//! ```bash
//! cargo test json_deserialization_benchmark --no-default-features -- --ignored --nocapture
//! cargo test json_deserialization_benchmark --release --no-default-features -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::time::Instant;
use velostream::velostream::serialization::json_codec::JsonCodec;
use velostream::velostream::kafka::serialization::Serde;
use velostream::velostream::sql::execution::types::FieldValue;

/// Create test JSON payloads
fn create_test_payloads(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let json = format!(
                r#"{{"id":{},"name":"user_{}","price":123.45,"quantity":{},"active":{},"symbol":"SYM{:03}","description":"This is a longer description for record {} with more text","balance":"12345.67"}}"#,
                i, i, i % 1000, i % 2 == 0, i % 100, i
            );
            json.into_bytes()
        })
        .collect()
}

/// Benchmark: NEW - Direct JsonCodec.deserialize() using FieldValue's Deserialize impl
fn benchmark_direct_deserialize(payloads: &[Vec<u8>]) -> (f64, usize) {
    let codec = JsonCodec::new();
    let start = Instant::now();
    let mut total_fields = 0usize;

    for payload in payloads {
        let result = codec.deserialize(payload).expect("deserialize failed");
        total_fields += result.len();
    }

    let elapsed = start.elapsed();
    let rate = payloads.len() as f64 / elapsed.as_secs_f64();
    (rate, total_fields)
}

/// Benchmark: OLD - Indirect via serde_json::Value then conversion
fn benchmark_indirect_deserialize(payloads: &[Vec<u8>]) -> (f64, usize) {
    let start = Instant::now();
    let mut total_fields = 0usize;

    for payload in payloads {
        // OLD: Parse to serde_json::Value first
        let value: serde_json::Value = sonic_rs::from_slice(payload).expect("parse failed");

        // OLD: Then convert Value to HashMap<String, FieldValue>
        let mut fields = HashMap::new();
        if let serde_json::Value::Object(obj) = value {
            for (key, val) in obj {
                let field_value = convert_json_value_to_field_value(&val);
                fields.insert(key, field_value);
            }
        }
        total_fields += fields.len();
    }

    let elapsed = start.elapsed();
    let rate = payloads.len() as f64 / elapsed.as_secs_f64();
    (rate, total_fields)
}

/// Convert serde_json::Value to FieldValue (OLD method)
fn convert_json_value_to_field_value(val: &serde_json::Value) -> FieldValue {
    match val {
        serde_json::Value::String(s) => {
            // Try to detect decimal strings
            if let Some(decimal_pos) = s.find('.') {
                let before = &s[..decimal_pos];
                let after = &s[decimal_pos + 1..];
                if !before.is_empty()
                    && before.chars().all(|c| c.is_ascii_digit() || c == '-')
                    && !after.is_empty()
                    && after.chars().all(|c| c.is_ascii_digit())
                {
                    let scale = after.len() as u8;
                    if let Ok(value) = format!("{}{}", before, after).parse::<i64>() {
                        return FieldValue::ScaledInteger(value, scale);
                    }
                }
            }
            FieldValue::String(s.clone())
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FieldValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                FieldValue::Float(f)
            } else {
                FieldValue::String(n.to_string())
            }
        }
        serde_json::Value::Bool(b) => FieldValue::Boolean(*b),
        serde_json::Value::Null => FieldValue::Null,
        serde_json::Value::Array(arr) => {
            FieldValue::Array(arr.iter().map(convert_json_value_to_field_value).collect())
        }
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), convert_json_value_to_field_value(v));
            }
            FieldValue::Map(map)
        }
    }
}

/// Benchmark: Just sonic_rs parsing (no FieldValue conversion)
fn benchmark_sonic_parse_only(payloads: &[Vec<u8>]) -> (f64, usize) {
    let start = Instant::now();
    let mut total_fields = 0usize;

    for payload in payloads {
        let value: serde_json::Value = sonic_rs::from_slice(payload).expect("parse failed");
        if let serde_json::Value::Object(obj) = value {
            total_fields += obj.len();
        }
    }

    let elapsed = start.elapsed();
    let rate = payloads.len() as f64 / elapsed.as_secs_f64();
    (rate, total_fields)
}

#[tokio::test]
#[ignore = "Performance benchmark - run explicitly with --ignored flag"]
async fn json_deserialization_benchmark() {
    println!("\n╔═══════════════════════════════════════════════════════════════════════╗");
    println!("║           JSON DESERIALIZATION BENCHMARK: Direct vs Indirect           ║");
    println!("╚═══════════════════════════════════════════════════════════════════════╝\n");

    let record_count = 100_000;
    let warmup_count = 10_000;

    println!("   Configuration:");
    println!("   • Record count: {}", record_count);
    println!("   • Warmup count: {}", warmup_count);
    println!();

    // Create test payloads
    println!("   Creating {} test payloads...", record_count);
    let payloads = create_test_payloads(record_count);
    let warmup_payloads = create_test_payloads(warmup_count);

    let avg_payload_size = payloads.iter().map(|p| p.len()).sum::<usize>() / payloads.len();
    println!("   Average payload size: {} bytes\n", avg_payload_size);

    // Warmup
    println!("   Warming up...\n");
    let _ = benchmark_indirect_deserialize(&warmup_payloads);
    let _ = benchmark_direct_deserialize(&warmup_payloads);
    let _ = benchmark_sonic_parse_only(&warmup_payloads);

    // ========== Test 1: INDIRECT (OLD) ==========
    println!("═══════════════════════════════════════════════════════════════════");
    println!("   Test 1: INDIRECT (OLD method)");
    println!("   bytes → serde_json::Value → convert to FieldValue → HashMap");
    println!("═══════════════════════════════════════════════════════════════════");

    let (indirect_rate, _) = benchmark_indirect_deserialize(&payloads);
    println!("   Rate: {:>12.0} rec/s", indirect_rate);

    // ========== Test 2: DIRECT (NEW) ==========
    println!("\n═══════════════════════════════════════════════════════════════════");
    println!("   Test 2: DIRECT (NEW method) ⭐");
    println!("   bytes → sonic_rs → HashMap<String, FieldValue> directly");
    println!("═══════════════════════════════════════════════════════════════════");

    let (direct_rate, _) = benchmark_direct_deserialize(&payloads);
    let speedup = direct_rate / indirect_rate;
    println!("   Rate: {:>12.0} rec/s", direct_rate);
    println!("   Speedup vs indirect: {:.2}x", speedup);

    // ========== Test 3: Parse only (ceiling) ==========
    println!("\n═══════════════════════════════════════════════════════════════════");
    println!("   Test 3: sonic_rs parse only (theoretical ceiling)");
    println!("   bytes → serde_json::Value");
    println!("═══════════════════════════════════════════════════════════════════");

    let (parse_rate, _) = benchmark_sonic_parse_only(&payloads);
    let efficiency = (direct_rate / parse_rate) * 100.0;
    println!("   Rate: {:>12.0} rec/s", parse_rate);
    println!("   Direct efficiency: {:.0}% of parse ceiling", efficiency);

    // ========== Summary ==========
    println!("\n═══════════════════════════════════════════════════════════════════");
    println!("   SUMMARY");
    println!("═══════════════════════════════════════════════════════════════════");
    println!();
    println!("   | Method            | Rate (rec/s) | vs Indirect |");
    println!("   |-------------------|--------------|-------------|");
    println!("   | Indirect (OLD)    | {:>12.0} | baseline    |", indirect_rate);
    println!("   | Direct (NEW) ⭐   | {:>12.0} | {:.2}x       |", direct_rate, speedup);
    println!("   | Parse only        | {:>12.0} | {:.2}x       |", parse_rate, parse_rate / indirect_rate);
    println!();

    if speedup >= 1.5 {
        println!("   🚀 Direct deserialization is {:.1}x faster!", speedup);
    }

    let throughput = (direct_rate * avg_payload_size as f64) / 1_000_000.0;
    println!("   Throughput: {:.1} MB/s", throughput);

    println!("\n═══════════════════════════════════════════════════════════════════");
    println!("   JSON DESERIALIZATION BENCHMARK COMPLETE");
    println!("═══════════════════════════════════════════════════════════════════\n");
}

/// Quick sanity test
#[test]
fn test_json_deserialization_sanity() {
    let payloads = create_test_payloads(10);
    let codec = JsonCodec::new();

    for payload in &payloads {
        let result = codec.deserialize(payload).expect("deserialize should work");
        assert!(result.contains_key("id"), "should have id field");
        assert!(result.contains_key("name"), "should have name field");
        assert!(result.contains_key("price"), "should have price field");

        // Verify types
        match result.get("id") {
            Some(FieldValue::Integer(_)) => {}
            other => panic!("id should be Integer, got {:?}", other),
        }
        match result.get("active") {
            Some(FieldValue::Boolean(_)) => {}
            other => panic!("active should be Boolean, got {:?}", other),
        }
        // balance is a decimal string, should be ScaledInteger
        match result.get("balance") {
            Some(FieldValue::ScaledInteger(_, _)) => {}
            other => panic!("balance should be ScaledInteger, got {:?}", other),
        }
    }
}
