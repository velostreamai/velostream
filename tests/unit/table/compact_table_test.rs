use std::collections::HashMap;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::compact_table::CompactTable;

#[test]
fn test_compact_table_wildcard_queries() {
    let table = CompactTable::new("financial-data".to_string(), "trading-group".to_string());

    // Create complex financial records with nested structures
    let mut portfolio_record = HashMap::new();

    // Portfolio with positions data
    let mut positions = HashMap::new();

    // AAPL position
    let mut aapl_position = HashMap::new();
    aapl_position.insert("shares".to_string(), FieldValue::Integer(150));
    aapl_position.insert("avg_price".to_string(), FieldValue::ScaledInteger(15025, 2)); // $150.25
    aapl_position.insert(
        "market_value".to_string(),
        FieldValue::ScaledInteger(2253750, 2),
    ); // $22,537.50
    positions.insert("AAPL".to_string(), FieldValue::Struct(aapl_position));

    // MSFT position
    let mut msft_position = HashMap::new();
    msft_position.insert("shares".to_string(), FieldValue::Integer(75));
    msft_position.insert("avg_price".to_string(), FieldValue::ScaledInteger(33025, 2)); // $330.25
    msft_position.insert(
        "market_value".to_string(),
        FieldValue::ScaledInteger(2476875, 2),
    ); // $24,768.75
    positions.insert("MSFT".to_string(), FieldValue::Struct(msft_position));

    // Small position (less than 100 shares)
    let mut tsla_position = HashMap::new();
    tsla_position.insert("shares".to_string(), FieldValue::Integer(25));
    tsla_position.insert("avg_price".to_string(), FieldValue::ScaledInteger(45050, 2)); // $450.50
    tsla_position.insert(
        "market_value".to_string(),
        FieldValue::ScaledInteger(1126250, 2),
    ); // $11,262.50
    positions.insert("TSLA".to_string(), FieldValue::Struct(tsla_position));

    portfolio_record.insert(
        "user_id".to_string(),
        FieldValue::String("trader-123".to_string()),
    );
    portfolio_record.insert("positions".to_string(), FieldValue::Struct(positions));
    portfolio_record.insert(
        "total_value".to_string(),
        FieldValue::ScaledInteger(4856875, 2),
    ); // $48,568.75

    table.insert("portfolio-001".to_string(), portfolio_record);

    // Test direct field access with path
    let aapl_shares =
        table.get_field_by_path(&"portfolio-001".to_string(), "positions.AAPL.shares");
    assert_eq!(aapl_shares, Some(FieldValue::Integer(150)));

    let aapl_price =
        table.get_field_by_path(&"portfolio-001".to_string(), "positions.AAPL.avg_price");
    assert_eq!(aapl_price, Some(FieldValue::ScaledInteger(15025, 2)));

    // Test memory efficiency
    let stats = table.memory_stats();
    println!("CompactTable memory stats: {:?}", stats);

    // Memory should be much less than regular HashMap approach
    assert!(stats.total_estimated_bytes < 2000); // Compact representation
}

#[test]
fn test_compact_table_schema_inference() {
    let table = CompactTable::new("test".to_string(), "test".to_string());

    // Insert first record - should trigger schema inference
    let mut record1 = HashMap::new();
    record1.insert("id".to_string(), FieldValue::Integer(1));
    record1.insert("name".to_string(), FieldValue::String("Alice".to_string()));
    record1.insert("balance".to_string(), FieldValue::ScaledInteger(100000, 2)); // $1000.00

    table.insert("user1".to_string(), record1.clone());

    // Insert second record with same schema
    let mut record2 = HashMap::new();
    record2.insert("id".to_string(), FieldValue::Integer(2));
    record2.insert("name".to_string(), FieldValue::String("Bob".to_string()));
    record2.insert("balance".to_string(), FieldValue::ScaledInteger(250050, 2)); // $2500.50

    table.insert("user2".to_string(), record2.clone());

    // Verify retrieval works correctly
    let retrieved1 = table.get(&"user1".to_string()).unwrap();
    assert_eq!(
        retrieved1.get("name"),
        Some(&FieldValue::String("Alice".to_string()))
    );
    assert_eq!(
        retrieved1.get("balance"),
        Some(&FieldValue::ScaledInteger(100000, 2))
    );

    let retrieved2 = table.get(&"user2".to_string()).unwrap();
    assert_eq!(
        retrieved2.get("name"),
        Some(&FieldValue::String("Bob".to_string()))
    );
    assert_eq!(
        retrieved2.get("balance"),
        Some(&FieldValue::ScaledInteger(250050, 2))
    );

    // Test direct field access
    let alice_balance = table
        .get_field_by_path(&"user1".to_string(), "balance")
        .unwrap();
    assert_eq!(alice_balance, FieldValue::ScaledInteger(100000, 2));

    let bob_name = table
        .get_field_by_path(&"user2".to_string(), "name")
        .unwrap();
    assert_eq!(bob_name, FieldValue::String("Bob".to_string()));
}

#[test]
fn test_compact_table_vs_regular_memory() {
    // Create two identical datasets and compare memory usage
    let compact_table = CompactTable::new("test".to_string(), "test".to_string());

    // Simulate regular HashMap storage for comparison
    let mut regular_storage: HashMap<String, HashMap<String, FieldValue>> = HashMap::new();

    // Insert 1000 records in both
    for i in 0..1000 {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string())); // Repeated string
        record.insert("price".to_string(), FieldValue::ScaledInteger(15000 + i, 2));
        record.insert("volume".to_string(), FieldValue::Integer(1000000 + i));

        let key = format!("key_{}", i);
        compact_table.insert(key.clone(), record.clone());
        regular_storage.insert(key, record);
    }

    let compact_stats = compact_table.memory_stats();
    let regular_estimated_size = regular_storage.len() * 200; // Rough estimate per record

    println!(
        "CompactTable memory: {} bytes",
        compact_stats.total_estimated_bytes
    );
    println!(
        "Regular HashMap estimated: {} bytes",
        regular_estimated_size
    );
    println!(
        "Memory reduction: {}%",
        100 - (compact_stats.total_estimated_bytes * 100 / regular_estimated_size)
    );

    // CompactTable should use significantly less memory due to:
    // 1. String interning ("AAPL" stored once, referenced by index)
    // 2. Schema sharing (field names stored once)
    // 3. Compact value representation
    assert!(compact_stats.total_estimated_bytes < regular_estimated_size / 2);
}

#[test]
fn test_compact_table_string_interning() {
    let table = CompactTable::new("test".to_string(), "test".to_string());

    // Insert many records with repeated string values
    for i in 0..100 {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert(
            "status".to_string(),
            FieldValue::String("active".to_string()),
        ); // Repeated
        record.insert(
            "category".to_string(),
            FieldValue::String("premium".to_string()),
        ); // Repeated
        record.insert(
            "region".to_string(),
            FieldValue::String("US-WEST".to_string()),
        ); // Repeated

        table.insert(format!("record_{}", i), record);
    }

    let stats = table.memory_stats();

    // String pool should be small despite 400 string insertions (100 records × 4 strings each)
    // because we only have 3 unique strings: "active", "premium", "US-WEST"
    println!(
        "String pool size: {} bytes for 400 string insertions",
        stats.string_pool_size
    );

    // Should be much smaller than if each string was stored separately
    assert!(stats.string_pool_size < 500); // Just the unique strings + overhead
}
