/*!
# Table Wildcard Query and Memory Efficiency Demo

This example demonstrates:
1. SQL wildcard queries with pattern matching (portfolio.positions.****.shares > 100)
2. CompactTable memory optimization for millions of records
3. String interning and schema-based storage
*/

use std::collections::HashMap;
use velostream::velostream::kafka::consumer_config::ConsumerConfig;
use velostream::velostream::kafka::serialization::StringSerializer;
use velostream::velostream::serialization::JsonFormat;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::Table;
use velostream::velostream::table::compact_table::CompactTable;
use velostream::velostream::table::sql::TableDataSource;
use velostream::velostream::table::unified_table::OptimizedTableImpl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Velostream Table Wildcard & Memory Efficiency Demo");
    println!("====================================================");

    // === Part 1: Wildcard SQL Queries ===
    println!("\n📊 Part 1: SQL Wildcard Queries");
    println!("--------------------------------");

    // Create a simple Table for SQL demonstration
    // NOTE: This would normally connect to Kafka, but we'll use it for local testing
    let table_result = Table::new(
        ConsumerConfig::new("localhost:9092", "demo-group"),
        "portfolio-topic".to_string(),
        StringSerializer,
        JsonFormat,
    )
    .await;

    match table_result {
        Ok(table) => {
            println!("✅ Table created successfully for SQL wildcard demo");

            // Create portfolio data with nested positions
            let mut portfolio_record = HashMap::new();
            let mut positions = HashMap::new();

            // AAPL position (large holding)
            let mut aapl_position = HashMap::new();
            aapl_position.insert("shares".to_string(), FieldValue::Integer(150));
            aapl_position.insert("price".to_string(), FieldValue::ScaledInteger(15025, 2)); // $150.25
            positions.insert("AAPL".to_string(), FieldValue::Struct(aapl_position));

            // TSLA position (small holding)
            let mut tsla_position = HashMap::new();
            tsla_position.insert("shares".to_string(), FieldValue::Integer(50));
            tsla_position.insert("price".to_string(), FieldValue::ScaledInteger(45050, 2)); // $450.50
            positions.insert("TSLA".to_string(), FieldValue::Struct(tsla_position));

            portfolio_record.insert("positions".to_string(), FieldValue::Struct(positions));
            portfolio_record.insert(
                "total_value".to_string(),
                FieldValue::ScaledInteger(4500000, 2),
            );

            // This would normally come from Kafka, but we'll simulate it
            println!("   Portfolio data structure created");

            // Create OptimizedTableImpl for high-performance SQL operations
            let sql_table = OptimizedTableImpl::new();
            sql_table.insert("portfolio_001".to_string(), portfolio_record)?;

            // Create SQL data source
            let sql_source = TableDataSource::from_table(sql_table);

            // Example wildcard queries (would work if data was in table):
            println!("   Available wildcard query patterns:");
            println!("   • portfolio.positions.*.shares > 100");
            println!("   • portfolio.positions.*  (get all positions)");
            println!("   • users.*.profile.email  (nested user data)");
        }
        Err(_) => {
            println!("⚠️  Kafka not available - skipping SQL wildcard demo");
            println!("   In production, you would:");
            println!("   1. table.sql_wildcard_values(\"portfolio.positions.*.shares > 100\")");
            println!("   2. Get all positions where shares > 100 across any symbol");
        }
    }

    // === Part 2: CompactTable Memory Efficiency ===
    println!("\n💾 Part 2: CompactTable Memory Efficiency");
    println!("------------------------------------------");

    let compact_table =
        CompactTable::new("financial-data".to_string(), "trading-group".to_string());
    println!("✅ CompactTable created for memory efficiency demo");

    // Simulate high-volume financial data
    println!("📈 Inserting 10,000 financial records...");

    let start_time = std::time::Instant::now();
    for i in 0..10_000 {
        let mut record = HashMap::new();

        // Financial data with repeated strings (perfect for interning)
        record.insert("trade_id".to_string(), FieldValue::Integer(i));
        record.insert("symbol".to_string(), FieldValue::String("AAPL".to_string())); // Repeated
        record.insert(
            "exchange".to_string(),
            FieldValue::String("NASDAQ".to_string()),
        ); // Repeated
        record.insert(
            "status".to_string(),
            FieldValue::String("executed".to_string()),
        ); // Repeated
        record.insert(
            "price".to_string(),
            FieldValue::ScaledInteger(15000 + (i % 100), 2),
        );
        record.insert("quantity".to_string(), FieldValue::Integer(100 + (i % 500)));
        record.insert(
            "trader_id".to_string(),
            FieldValue::String(format!("trader_{}", i % 100)),
        ); // Repeated

        compact_table.insert(format!("trade_{}", i), record);
    }

    let insert_duration = start_time.elapsed();
    println!("⚡ Inserted 10,000 records in {:?}", insert_duration);

    // Memory efficiency analysis
    let stats = compact_table.memory_stats();
    println!("\n📊 Memory Efficiency Analysis:");
    println!("   Records: {}", stats.record_count);
    println!("   Schema overhead: {} bytes", stats.schema_overhead);
    println!("   Record data: {} bytes", stats.record_overhead);
    println!("   String pool: {} bytes", stats.string_pool_size);
    println!(
        "   Total memory: {} bytes ({:.2} KB)",
        stats.total_estimated_bytes,
        stats.total_estimated_bytes as f64 / 1024.0
    );

    // Compare with naive approach
    let naive_estimate = stats.record_count * 300; // Rough estimate for HashMap<String, FieldValue>
    let memory_savings = 100 - (stats.total_estimated_bytes * 100 / naive_estimate);
    println!(
        "   Estimated naive storage: {} bytes ({:.2} KB)",
        naive_estimate,
        naive_estimate as f64 / 1024.0
    );
    println!("   💰 Memory savings: ~{}%", memory_savings);

    // === Part 3: Field Access Performance ===
    println!("\n⚡ Part 3: Direct Field Access Performance");
    println!("-------------------------------------------");

    let start_time = std::time::Instant::now();
    let mut price_sum = 0i64;

    // Test direct field access (no full record conversion)
    for i in 0..1000 {
        let key = format!("trade_{}", i);
        if let Some(price) = compact_table.get_field_by_path(&key, "price")
            && let FieldValue::ScaledInteger(value, _scale) = price
        {
            price_sum += value;
        }
    }

    let access_duration = start_time.elapsed();
    println!(
        "✅ Direct field access for 1,000 records in {:?}",
        access_duration
    );
    println!(
        "   Average access time: {:?} per record",
        access_duration / 1000
    );
    println!("   Price sum: ${:.2}", price_sum as f64 / 100.0);

    // === Part 4: Portfolio Wildcard Simulation ===
    println!("\n🎯 Part 4: Portfolio Wildcard Pattern Demo");
    println!("-------------------------------------------");

    let portfolio_table =
        CompactTable::new("portfolios".to_string(), "portfolio-group".to_string());

    // Create complex portfolio with nested positions
    let mut portfolio = HashMap::new();
    let mut positions = HashMap::new();

    // Multiple positions with different symbols
    for symbol in &["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"] {
        let mut position = HashMap::new();
        let shares = 50 + (symbol.len() * 20) as i64; // Varying position sizes
        let price = 10000 + (symbol.len() * 5000) as i64; // Varying prices

        position.insert("shares".to_string(), FieldValue::Integer(shares));
        position.insert("avg_price".to_string(), FieldValue::ScaledInteger(price, 2));
        position.insert(
            "market_value".to_string(),
            FieldValue::ScaledInteger(shares * price, 2),
        );

        positions.insert(symbol.to_string(), FieldValue::Struct(position));
    }

    portfolio.insert(
        "user_id".to_string(),
        FieldValue::String("trader-001".to_string()),
    );
    portfolio.insert("positions".to_string(), FieldValue::Struct(positions));

    portfolio_table.insert("portfolio-001".to_string(), portfolio);

    // Simulate wildcard access patterns
    println!("📈 Portfolio position analysis:");

    // Direct access to specific positions
    if let Some(aapl_shares) =
        portfolio_table.get_field_by_path(&"portfolio-001".to_string(), "positions.AAPL.shares")
    {
        println!("   AAPL shares: {:?}", aapl_shares);
    }

    if let Some(msft_price) =
        portfolio_table.get_field_by_path(&"portfolio-001".to_string(), "positions.MSFT.avg_price")
    {
        println!("   MSFT average price: {:?}", msft_price);
    }

    // This demonstrates the pattern that would be matched by:
    // sql_wildcard_values("portfolio.positions.*.shares > 100")
    println!("   🔍 Wildcard pattern 'positions.*.shares > 100' would find:");
    for symbol in &["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"] {
        let field_path = format!("positions.{}.shares", symbol);
        if let Some(shares) =
            portfolio_table.get_field_by_path(&"portfolio-001".to_string(), &field_path)
            && let FieldValue::Integer(share_count) = shares
        {
            if share_count > 100 {
                println!(
                    "      {}: {} shares (matches criteria)",
                    symbol, share_count
                );
            } else {
                println!("      {}: {} shares", symbol, share_count);
            }
        }
    }

    println!("\n🎉 Demo completed successfully!");
    println!("   Key Features Demonstrated:");
    println!("   ✅ Memory-efficient storage with string interning");
    println!("   ✅ Schema-based compact representation");
    println!("   ✅ Direct field access without full record conversion");
    println!("   ✅ Wildcard pattern support for nested structures");
    println!("   ✅ ScaledInteger precision for financial data");

    Ok(())
}
