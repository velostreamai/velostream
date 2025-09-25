use std::collections::HashMap;
use velostream::velostream::table::ctas::CtasExecutor;

#[test]
fn test_should_use_compact_table_explicit_config() {
    let executor = CtasExecutor::new("localhost:9092".to_string(), "test".to_string());

    // Test explicit compact model
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "compact".to_string());
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for table_model = 'compact'");

    // Test explicit normal model
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "normal".to_string());
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table for table_model = 'normal'");

    // Test explicit standard model (alternative name)
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "standard".to_string());
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table for table_model = 'standard'");

    // Test case insensitive
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "COMPACT".to_string());
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for table_model = 'COMPACT' (case insensitive)");

    // Test invalid model (should default to normal)
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "invalid".to_string());
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table for invalid table_model");
}

#[test]
fn test_should_use_compact_table_high_volume_data() {
    let executor = CtasExecutor::new("localhost:9092".to_string(), "test".to_string());

    // Test massive dataset (> 1B records/day)
    let mut properties = HashMap::new();
    properties.insert("expected.records.per.day".to_string(), "5000000000".to_string()); // 5B records
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for massive dataset");

    // Test large dataset (> 100M records/day)
    let mut properties = HashMap::new();
    properties.insert("expected.records.per.day".to_string(), "500000000".to_string()); // 500M records
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for large dataset");

    // Test medium-volume dataset (< 100M records/day)
    let mut properties = HashMap::new();
    properties.insert("expected.records.per.day".to_string(), "50000000".to_string()); // 50M records
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table for medium-volume dataset");

    // Test invalid format
    let mut properties = HashMap::new();
    properties.insert("expected.records.per.day".to_string(), "invalid".to_string());
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table for invalid volume format");
}

#[test]
fn test_should_use_compact_table_cdc_workloads() {
    let executor = CtasExecutor::new("localhost:9092".to_string(), "test".to_string());

    // Test CDC enabled
    let mut properties = HashMap::new();
    properties.insert("cdc.enabled".to_string(), "true".to_string());
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for CDC workload");

    // Test EMIT CHANGES mode
    let mut properties = HashMap::new();
    properties.insert("emit.mode".to_string(), "changes".to_string());
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for EMIT CHANGES");

    // Test EMIT CHANGES mode (case insensitive)
    let mut properties = HashMap::new();
    properties.insert("emit.mode".to_string(), "CHANGES".to_string());
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for EMIT CHANGES (uppercase)");

    // Test EMIT FINAL mode (should use standard Table)
    let mut properties = HashMap::new();
    properties.insert("emit.mode".to_string(), "final".to_string());
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table for EMIT FINAL");
}

#[test]
fn test_should_use_compact_table_analytics_workloads() {
    let executor = CtasExecutor::new("localhost:9092".to_string(), "test".to_string());

    // Test analytics workload
    let mut properties = HashMap::new();
    properties.insert("workload.type".to_string(), "analytics".to_string());
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for analytics workload");

    // Test aggregation workload
    let mut properties = HashMap::new();
    properties.insert("workload.type".to_string(), "real-time aggregation".to_string());
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for aggregation workload");

    // Test simple workload (should use standard Table)
    let mut properties = HashMap::new();
    properties.insert("workload.type".to_string(), "simple filtering".to_string());
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table for simple workload");
}

#[test]
fn test_should_use_compact_table_memory_optimization() {
    let executor = CtasExecutor::new("localhost:9092".to_string(), "test".to_string());

    // Test explicit memory optimization
    let mut properties = HashMap::new();
    properties.insert("optimize.memory".to_string(), "true".to_string());
    assert!(executor.should_use_compact_table(&properties), "Should use CompactTable for explicit memory optimization");

    // Test memory optimization disabled
    let mut properties = HashMap::new();
    properties.insert("optimize.memory".to_string(), "false".to_string());
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table when memory optimization disabled");
}

#[test]
fn test_should_use_compact_table_multiple_criteria() {
    let executor = CtasExecutor::new("localhost:9092".to_string(), "test".to_string());

    // Test multiple criteria that suggest CompactTable
    let mut properties = HashMap::new();
    properties.insert("performance.mode".to_string(), "analytics".to_string());
    properties.insert("expected.records.per.day".to_string(), "10000000".to_string()); // 10M records
    properties.insert("cdc.enabled".to_string(), "true".to_string());
    properties.insert("optimize.memory".to_string(), "true".to_string());

    assert!(executor.should_use_compact_table(&properties),
        "Should use CompactTable when multiple criteria suggest high-performance needs");

    // Test conflicting criteria (performance mode overrides)
    let mut properties = HashMap::new();
    properties.insert("performance.mode".to_string(), "low_latency".to_string()); // Suggests standard Table
    properties.insert("expected.records.per.day".to_string(), "10000000".to_string()); // Suggests CompactTable

    // Performance mode should take precedence
    assert!(!executor.should_use_compact_table(&properties),
        "Performance mode should override other criteria");
}

#[test]
fn test_should_use_compact_table_defaults() {
    let executor = CtasExecutor::new("localhost:9092".to_string(), "test".to_string());

    // Test empty properties (should use standard Table)
    let properties = HashMap::new();
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table by default");

    // Test irrelevant properties (should use standard Table)
    let mut properties = HashMap::new();
    properties.insert("some.other.property".to_string(), "value".to_string());
    properties.insert("retention".to_string(), "7 days".to_string());
    assert!(!executor.should_use_compact_table(&properties), "Should use standard Table for irrelevant properties");
}

#[test]
fn test_compact_table_scenarios_documentation() {
    // This test documents the scenarios where CompactTable provides benefits
    println!("🔍 Why Not Use CompactTable All The Time?");
    println!("");
    println!("✅ **CompactTable Benefits:**");
    println!("   • 90% memory reduction vs HashMap storage");
    println!("   • String interning reduces memory overhead");
    println!("   • Schema-based compact storage");
    println!("   • Better cache locality for large datasets");
    println!("   • Optimized for millions of records");
    println!("");
    println!("❌ **CompactTable Trade-offs:**");
    println!("   • ~10-15% CPU overhead for FieldValue conversion");
    println!("   • Extra latency for field access (microseconds)");
    println!("   • Schema inference complexity");
    println!("   • String interning overhead for small datasets");
    println!("   • Less efficient for frequent random access");
    println!("   • Optimized for stable schemas (schema changes costly)");
    println!("");
    println!("📊 **When to Use Each:**");
    println!("");
    println!("🚀 **USE CompactTable for:**");
    println!("   • High-volume streams (>1B records/day)");
    println!("   • Massive analytics and aggregation workloads");
    println!("   • CDC with billions of updates per day");
    println!("   • Memory-constrained environments with huge tables");
    println!("   • Large-scale financial data (>100M trades/day)");
    println!("   • IoT sensor data at massive scale");
    println!("   • Global user behavior analytics");
    println!("");
    println!("⚡ **USE Standard Table for:**");
    println!("   • Low-latency requirements (<1ms response)");
    println!("   • Small-to-medium datasets (<100M records)");
    println!("   • Simple filtering operations");
    println!("   • Frequent random key lookups");
    println!("   • Schema changes during development");
    println!("   • Real-time trading (sub-millisecond latency)");
    println!("");
    println!("💡 **Rule of Thumb:**");
    println!("   Memory savings > CPU cost → Use CompactTable");
    println!("   Latency critical → Use Standard Table");
}