/*!
# Schema Inference Demo - CTAS and CSAS

This example demonstrates schema inference options for CTAS and CSAS:

1. **Full Inference (SELECT *)** - Infer column names AND types from source
2. **Partial Inference (SELECT cols)** - Infer types only, specify column names
3. **No Inference (Explicit Types)** - Specify both column names and types explicitly

## Usage

```bash
cargo run --example schema_inference_demo --no-default-features
```

## Key Concepts

- **Schema Inference**: Automatically detect column types from Avro, Protobuf, or JSON sources
- **Explicit Schema**: Define exact types for validation and documentation
- **Type Safety**: Choose the right level of type enforcement for your use case
*/

use tokio::time::{Duration, sleep};
use velostream::velostream::server::stream_job_server::StreamJobServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    println!("🔍 Schema Inference Demo - CTAS and CSAS");
    println!("==========================================\n");

    // Create StreamJobServer
    let server = StreamJobServer::new("localhost:9092".to_string(), "schema-demo".to_string(), 10);

    println!("📊 Created StreamJobServer\n");

    // ========================================================================
    // Schema Inference Levels
    // ========================================================================
    println!("📚 Understanding Schema Inference Levels");
    println!("========================================\n");

    println!("**Level 1: Full Inference** (SELECT *)");
    println!("   CREATE TABLE orders AS SELECT * FROM source");
    println!("   → Infers: Column names + Column types");
    println!("   → Use when: Source schema is complete and trustworthy\n");

    println!("**Level 2: Partial Inference** (SELECT specific columns)");
    println!("   CREATE TABLE orders AS SELECT order_id, amount FROM source");
    println!("   → Infers: Column types only (names are explicit)");
    println!("   → Use when: You want to select subset of columns\n");

    println!("**Level 3: No Inference** (Explicit schema)");
    println!("   CREATE TABLE orders (order_id BIGINT, amount DECIMAL(10,2)) AS SELECT ...");
    println!("   → Infers: Nothing (fully explicit)");
    println!("   → Use when: Type safety is critical\n");

    // ========================================================================
    // Example 1: CTAS with Full Schema Inference (SELECT *)
    // ========================================================================
    println!("📝 Example 1: CTAS with Full Schema Inference (SELECT *)");
    println!("---------------------------------------------------------");

    let full_inference_ctas = r#"
        CREATE TABLE orders_full_inference AS
        SELECT *                -- All columns and types inferred from source!
        FROM orders_stream
        EMIT CHANGES;
    "#;

    println!("SQL:\n{}\n", full_inference_ctas);
    println!("✅ Benefits:");
    println!("   • Maximum simplicity - just 'SELECT *'");
    println!("   • All columns automatically included");
    println!("   • Types inferred from Avro/Protobuf schema");
    println!("   • Perfect for pass-through or materialization\n");

    println!("⚠️  Considerations:");
    println!("   • Schema changes in source affect your table");
    println!("   • May include unwanted columns");
    println!("   • Less explicit about what data is being used\n");

    match server
        .deploy_job(
            "orders_full_inference".to_string(),
            "v1".to_string(),
            full_inference_ctas.to_string(),
            "orders_stream".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Full inference table deployed\n"),
        Err(e) => println!("⚠️  Expected error (Kafka not running): {}\n", e),
    }

    // ========================================================================
    // Example 2: CTAS with Partial Schema Inference (SELECT specific columns)
    // ========================================================================
    println!("📝 Example 2: CTAS with Partial Schema Inference");
    println!("--------------------------------------------------");

    let partial_inference_ctas = r#"
        CREATE TABLE orders_partial_inference AS
        SELECT
            order_id,           -- Type inferred from source
            customer_id,        -- Type inferred from source
            amount,             -- Type inferred from source
            status,             -- Type inferred from source
            created_at          -- Type inferred from source
        FROM orders_stream
        EMIT CHANGES;
    "#;

    println!("SQL:\n{}\n", partial_inference_ctas);
    println!("✅ Benefits:");
    println!("   • Choose exactly which columns to include");
    println!("   • Types still inferred (no type management)");
    println!("   • More explicit than SELECT *");
    println!("   • Can add computed columns\n");

    println!("⚠️  Considerations:");
    println!("   • Must update query when adding new columns");
    println!("   • Still relies on source for type information\n");

    match server
        .deploy_job(
            "orders_partial_inference".to_string(),
            "v1".to_string(),
            partial_inference_ctas.to_string(),
            "orders_stream".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Partial inference table deployed\n"),
        Err(e) => println!("⚠️  Expected error (Kafka not running): {}\n", e),
    }

    // ========================================================================
    // Example 3: CTAS with Explicit Schema Definition (No Inference)
    // ========================================================================
    println!("📝 Example 3: CTAS with Explicit Schema Definition (No Inference)");
    println!("---------------------------------------------------");

    let explicit_ctas = r#"
        CREATE TABLE orders_explicit (
            order_id BIGINT,
            customer_id BIGINT,
            amount DECIMAL(10, 2),
            status VARCHAR(50),
            created_at TIMESTAMP
        ) AS
        SELECT order_id, customer_id, amount, status, created_at
        FROM orders_stream
        EMIT CHANGES;
    "#;

    println!("SQL:\n{}\n", explicit_ctas);
    println!("✅ Benefits:");
    println!("   • Explicit type enforcement (DECIMAL for financial precision)");
    println!("   • Clear schema contract for downstream consumers");
    println!("   • Better documentation and maintainability");
    println!("   • Type validation at ingestion time\n");

    println!("⚠️  Considerations:");
    println!("   • More verbose - requires type management");
    println!("   • Schema changes require code updates");
    println!("   • Slightly more development time\n");

    match server
        .deploy_job(
            "orders_explicit".to_string(),
            "v1".to_string(),
            explicit_ctas.to_string(),
            "orders_stream".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Explicit schema table deployed\n"),
        Err(e) => println!("⚠️  Expected error (Kafka not running): {}\n", e),
    }

    // ========================================================================
    // Example 4: CSAS with Partial Schema Inference
    // ========================================================================
    println!("📝 Example 4: CSAS with Partial Schema Inference");
    println!("--------------------------------------------------");

    let partial_csas = r#"
        CREATE STREAM high_value_orders AS
        SELECT
            order_id,
            customer_id,
            amount,
            status
        FROM orders_stream
        WHERE amount > 1000
        EMIT CHANGES;
    "#;

    println!("SQL:\n{}\n", partial_csas);
    println!("Use Case: Fast stream filtering without type overhead");
    println!("Schema: Types inferred from source (order_id, customer_id, amount, status)");
    println!("Memory: Minimal - stateless transformation");
    println!("Output: Kafka topic 'high_value_orders' with inferred types\n");

    match server
        .deploy_job(
            "high_value_orders_partial".to_string(),
            "v1".to_string(),
            partial_csas.to_string(),
            "orders_stream".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Partial inference stream deployed\n"),
        Err(e) => println!("⚠️  Expected error: {}\n", e),
    }

    // ========================================================================
    // Example 5: CSAS with Explicit Schema Definition (No Inference)
    // ========================================================================
    println!("📝 Example 5: CSAS with Explicit Schema Definition (No Inference)");
    println!("------------------------------------------------------------------");

    let explicit_csas = r#"
        CREATE STREAM fraud_alerts (
            transaction_id BIGINT,
            customer_id BIGINT,
            amount DECIMAL(10, 2),
            alert_type VARCHAR(100),
            alert_time TIMESTAMP
        ) AS
        SELECT
            transaction_id,
            customer_id,
            amount,
            'High value transaction' as alert_type,
            CURRENT_TIMESTAMP as alert_time
        FROM transactions_stream
        WHERE amount > 10000
        EMIT CHANGES;
    "#;

    println!("SQL:\n{}\n", explicit_csas);
    println!("Use Case: Schema contract for critical alerting system");
    println!("Type Safety: DECIMAL ensures exact financial amounts");
    println!("Documentation: Clear schema for downstream alert consumers\n");

    match server
        .deploy_job(
            "fraud_alerts_explicit".to_string(),
            "v1".to_string(),
            explicit_csas.to_string(),
            "transactions_stream".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Explicit schema stream deployed\n"),
        Err(e) => println!("⚠️  Expected error: {}\n", e),
    }

    // ========================================================================
    // Example 6: Hybrid Approach - Inference with Transformation
    // ========================================================================
    println!("📝 Example 6: Hybrid Approach - Inference + Transformation");
    println!("-----------------------------------------------------------");

    let hybrid_ctas = r#"
        CREATE TABLE customer_metrics AS
        SELECT
            customer_id,                    -- Inferred from source
            COUNT(*) as order_count,        -- Inferred as BIGINT
            SUM(amount) as total_spent,     -- Inferred from amount type
            AVG(amount) as avg_order_value, -- Inferred as DOUBLE
            MAX(created_at) as last_order   -- Inferred as TIMESTAMP
        FROM orders_stream
        GROUP BY customer_id
        EMIT CHANGES;
    "#;

    println!("SQL:\n{}\n", hybrid_ctas);
    println!("✅ Smart Approach:");
    println!("   • Base columns inferred from source");
    println!("   • Aggregations get appropriate types automatically");
    println!("   • Best of both worlds - simple yet type-safe\n");

    match server
        .deploy_job(
            "customer_metrics".to_string(),
            "v1".to_string(),
            hybrid_ctas.to_string(),
            "orders_stream".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Hybrid approach table deployed\n"),
        Err(e) => println!("⚠️  Expected error: {}\n", e),
    }

    // ========================================================================
    // Decision Guide: When to Use Each Approach
    // ========================================================================
    println!("📋 Decision Guide: Implicit vs Explicit Schema");
    println!("===============================================\n");

    println!("✅ Use IMPLICIT Schema Inference When:");
    println!("   • Source has strong typing (Avro, Protobuf with schema registry)");
    println!("   • Rapid prototyping and development");
    println!("   • Schema may evolve frequently");
    println!("   • Internal processing pipelines");
    println!("   • Trust the source schema quality\n");

    println!("✅ Use EXPLICIT Schema Definition When:");
    println!("   • Financial data requiring DECIMAL precision");
    println!("   • Creating public APIs or shared tables");
    println!("   • Regulatory compliance requires schema documentation");
    println!("   • Type validation is critical");
    println!("   • Schema contract is important for consumers");
    println!("   • Working with weakly-typed sources (JSON)\n");

    println!("✅ Hybrid Approach (Best of Both):");
    println!("   • Infer base column types from source");
    println!("   • Explicitly define computed/transformed columns");
    println!("   • Use explicit types for financial/critical fields only\n");

    // ========================================================================
    // Source Type Recommendations
    // ========================================================================
    println!("📊 Recommendations by Source Type");
    println!("==================================\n");

    println!("**Avro with Schema Registry**:");
    println!("   → ✅ Implicit inference (schemas are strongly typed)");
    println!("   → Example: CREATE TABLE users AS SELECT * FROM avro_source\n");

    println!("**Protobuf**:");
    println!("   → ✅ Implicit inference (protocol buffers are strongly typed)");
    println!("   → Example: CREATE TABLE events AS SELECT * FROM protobuf_source\n");

    println!("**JSON**:");
    println!("   → ⚠️  Consider explicit for critical fields");
    println!("   → Example: CREATE TABLE orders (amount DECIMAL(10,2), ...) AS SELECT ...\n");

    println!("**CSV**:");
    println!("   → ⚠️  Explicit recommended (CSV has no type information)");
    println!("   → Example: CREATE TABLE data (id BIGINT, value DECIMAL, ...) AS SELECT ...\n");

    // ========================================================================
    // Type Inference Examples
    // ========================================================================
    println!("🔬 Type Inference Behavior");
    println!("==========================\n");

    println!("When using schema inference, Velostream infers types as follows:\n");

    println!("**Numeric Types**:");
    println!("   • Integer values → BIGINT");
    println!("   • Decimal values → DOUBLE (use explicit DECIMAL for financial data!)");
    println!("   • Avro 'long' → BIGINT");
    println!("   • Avro 'decimal' → DECIMAL with precision from schema\n");

    println!("**String Types**:");
    println!("   • Text fields → STRING");
    println!("   • Avro 'string' → STRING");
    println!("   • Bounded strings → VARCHAR with inferred length\n");

    println!("**Temporal Types**:");
    println!("   • Timestamp fields → TIMESTAMP");
    println!("   • Date fields → DATE");
    println!("   • Avro logical types preserved\n");

    println!("**Aggregation Functions**:");
    println!("   • COUNT(*) → BIGINT");
    println!("   • SUM(integer) → BIGINT");
    println!("   • SUM(decimal) → DECIMAL");
    println!("   • AVG(any) → DOUBLE");
    println!("   • MAX/MIN → Same as input type\n");

    // ========================================================================
    // Summary
    // ========================================================================
    println!("🎯 Summary");
    println!("==========\n");

    println!("1. **Implicit Schema Inference** = Fast, flexible, great for Avro/Protobuf");
    println!("2. **Explicit Schema Definition** = Type-safe, documented, great for APIs");
    println!("3. **Hybrid Approach** = Practical balance for most applications");
    println!("4. **Financial Data** = Always use explicit DECIMAL types");
    println!("5. **Public Tables** = Document schema explicitly");
    println!("6. **Internal Pipelines** = Inference is often sufficient\n");

    println!("💡 Pro Tip:");
    println!("Start with implicit inference for rapid development,");
    println!("then add explicit types for critical fields as requirements emerge.\n");

    // Keep the program running briefly
    println!("⏳ Demo complete. Exiting in 3 seconds...");
    sleep(Duration::from_secs(3)).await;

    Ok(())
}
