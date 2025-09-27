use std::collections::HashMap;
use velostream::velostream::table::ctas::CtasExecutor;

#[test]
fn test_should_use_compact_table_explicit_config() {
    let executor = CtasExecutor::new("localhost:9092".to_string(), "test".to_string());

    // Test explicit compact model
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "compact".to_string());
    assert!(
        executor.should_use_compact_table(&properties),
        "Should use CompactTable for table_model = 'compact'"
    );

    // Test explicit normal model
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "normal".to_string());
    assert!(
        !executor.should_use_compact_table(&properties),
        "Should use standard Table for table_model = 'normal'"
    );

    // Test explicit standard model (alternative name)
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "standard".to_string());
    assert!(
        !executor.should_use_compact_table(&properties),
        "Should use standard Table for table_model = 'standard'"
    );

    // Test case insensitive
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "COMPACT".to_string());
    assert!(
        executor.should_use_compact_table(&properties),
        "Should use CompactTable for table_model = 'COMPACT' (case insensitive)"
    );

    // Test invalid model (should default to normal)
    let mut properties = HashMap::new();
    properties.insert("table_model".to_string(), "invalid".to_string());
    assert!(
        !executor.should_use_compact_table(&properties),
        "Should use standard Table for invalid table_model"
    );
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
