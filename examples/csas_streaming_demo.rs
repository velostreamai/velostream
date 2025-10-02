/*!
# CSAS (CREATE STREAM AS SELECT) Streaming Demo

This example demonstrates CSAS stream-to-stream transformations:

1. Real-time filtering and alerting
2. Stream enrichment with table JOINs
3. Format transformations
4. Windowed aggregations to streams
5. Fan-out patterns (one stream → many streams)

## Usage

```bash
cargo run --example csas_streaming_demo --no-default-features
```

This demo shows how CSAS differs from CTAS:
- CSAS: Creates streams (no queryable state, low memory)
- CTAS: Creates tables (queryable state, higher memory)

## Prerequisites

- Kafka running on localhost:9092
- Topics: orders, customers, transactions

## What This Demo Shows

- ✅ CSAS for real-time alerting (fraud detection)
- ✅ CSAS for stream filtering (high-value orders)
- ✅ CSAS with table JOIN (order enrichment)
- ✅ CSAS with windowed aggregations (metrics)
- ✅ CSAS fan-out pattern (regional streams)
*/

use tokio::time::{sleep, Duration};
use velostream::velostream::server::stream_job_server::StreamJobServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    println!("🌊 Starting CSAS Streaming Demo");
    println!("================================\n");

    // Create StreamJobServer
    let server = StreamJobServer::new("localhost:9092".to_string(), "csas-demo".to_string(), 10);

    println!("📊 Created StreamJobServer for CSAS operations\n");

    // ========================================================================
    // Example 1: CSAS for Real-Time Fraud Alerts
    // ========================================================================
    println!("🚨 Example 1: Real-Time Fraud Detection Stream");
    println!("-----------------------------------------------");

    let fraud_alerts_csas = r#"
        CREATE STREAM fraud_alerts AS
        SELECT
            transaction_id,
            customer_id,
            amount,
            merchant,
            'Suspicious high-value transaction' as alert_type,
            CURRENT_TIMESTAMP as alert_time,
            HEADER('source_ip') as source_ip
        FROM transactions
        WHERE amount > 10000
           OR (amount > 1000 AND merchant LIKE '%foreign%')
        EMIT CHANGES;
    "#;

    println!("SQL:\n{}\n", fraud_alerts_csas);
    println!("Purpose: Filter transactions and forward alerts to fraud_alerts topic");
    println!("Memory: Minimal - only processes current batch");
    println!("Output: Kafka topic 'fraud_alerts' with suspicious transactions\n");

    match server
        .deploy_job(
            "fraud_alerts".to_string(),
            "v1".to_string(),
            fraud_alerts_csas.to_string(),
            "transactions".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Fraud alerts stream deployed successfully"),
        Err(e) => println!("⚠️  Expected error (Kafka not running): {}", e),
    }

    // ========================================================================
    // Example 2: CSAS for Stream Filtering (High-Value Orders)
    // ========================================================================
    println!("\n💰 Example 2: High-Value Order Filtering");
    println!("----------------------------------------");

    let high_value_orders_csas = r#"
        CREATE STREAM high_value_orders AS
        SELECT
            order_id,
            customer_id,
            amount,
            status,
            product_category,
            timestamp
        FROM orders
        WHERE amount > 5000
          AND status = 'pending'
        EMIT CHANGES;
    "#;

    println!("SQL:\n{}\n", high_value_orders_csas);
    println!("Purpose: Forward only high-value pending orders");
    println!("Use Case: Priority processing, VIP customer service");
    println!("Output: Kafka topic 'high_value_orders'\n");

    match server
        .deploy_job(
            "high_value_orders".to_string(),
            "v1".to_string(),
            high_value_orders_csas.to_string(),
            "orders".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ High-value orders stream deployed"),
        Err(e) => println!("⚠️  Expected error: {}", e),
    }

    // ========================================================================
    // Example 3: CSAS with Table JOIN (Order Enrichment)
    // ========================================================================
    println!("\n🔗 Example 3: Stream Enrichment with Table JOIN");
    println!("-----------------------------------------------");

    // First create a CTAS table for customer data
    let customers_table_ctas = r#"
        CREATE TABLE customers AS
        SELECT
            customer_id,
            name,
            tier,
            risk_score,
            lifetime_value
        FROM customers_stream
        EMIT CHANGES;
    "#;

    println!("Step 1: Create customers lookup table (CTAS)");
    println!("SQL:\n{}\n", customers_table_ctas);

    match server
        .deploy_job(
            "customers".to_string(),
            "v1".to_string(),
            customers_table_ctas.to_string(),
            "customers_stream".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Created customers table"),
        Err(e) => println!("⚠️  Expected error: {}", e),
    }

    // Now create CSAS that joins with the table
    let enriched_orders_csas = r#"
        CREATE STREAM enriched_orders AS
        SELECT
            o.order_id,
            o.amount,
            o.status,
            c.name as customer_name,
            c.tier as customer_tier,
            c.risk_score,
            c.lifetime_value,
            o.timestamp,
            CASE
                WHEN c.tier = 'GOLD' AND o.amount > 1000 THEN 'VIP_HIGH_VALUE'
                WHEN c.risk_score > 80 THEN 'HIGH_RISK'
                WHEN c.lifetime_value > 100000 THEN 'TOP_CUSTOMER'
                ELSE 'STANDARD'
            END as order_category
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.status IN ('pending', 'processing')
        EMIT CHANGES;
    "#;

    println!("\nStep 2: Create enriched orders stream (CSAS)");
    println!("SQL:\n{}\n", enriched_orders_csas);
    println!("Purpose: Enrich orders with customer data and forward");
    println!("Pattern: Table (CTAS) for lookups + Stream (CSAS) for forwarding");
    println!("Output: Enriched orders with customer details\n");

    match server
        .deploy_job(
            "enriched_orders".to_string(),
            "v1".to_string(),
            enriched_orders_csas.to_string(),
            "orders".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Enriched orders stream deployed"),
        Err(e) => println!("⚠️  Expected error: {}", e),
    }

    // ========================================================================
    // Example 4: CSAS with Windowed Aggregations
    // ========================================================================
    println!("\n📊 Example 4: Windowed Aggregation Stream");
    println!("----------------------------------------");

    let metrics_stream_csas = r#"
        CREATE STREAM order_metrics_stream AS
        SELECT
            product_category,
            COUNT(*) as order_count,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_order_value,
            MAX(amount) as max_order,
            TUMBLE_START(timestamp, INTERVAL '5' MINUTES) as window_start,
            TUMBLE_END(timestamp, INTERVAL '5' MINUTES) as window_end
        FROM orders
        GROUP BY
            product_category,
            TUMBLE(timestamp, INTERVAL '5' MINUTES)
        EMIT FINAL;
    "#;

    println!("SQL:\n{}\n", metrics_stream_csas);
    println!("Purpose: Aggregate 5-minute windows and forward results");
    println!("EMIT FINAL: Emits complete window results (not incremental updates)");
    println!("Use Case: Metrics forwarding to monitoring systems");
    println!("Output: 5-minute aggregated metrics per category\n");

    match server
        .deploy_job(
            "order_metrics_stream".to_string(),
            "v1".to_string(),
            metrics_stream_csas.to_string(),
            "orders".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Metrics stream deployed"),
        Err(e) => println!("⚠️  Expected error: {}", e),
    }

    // ========================================================================
    // Example 5: CSAS Fan-Out Pattern (Regional Streams)
    // ========================================================================
    println!("\n🌍 Example 5: Fan-Out Pattern (Regional Routing)");
    println!("-----------------------------------------------");

    let us_orders_csas = r#"
        CREATE STREAM us_orders AS
        SELECT
            order_id,
            customer_id,
            amount,
            'US' as region,
            timestamp
        FROM orders
        WHERE country = 'US'
        EMIT CHANGES;
    "#;

    let eu_orders_csas = r#"
        CREATE STREAM eu_orders AS
        SELECT
            order_id,
            customer_id,
            amount,
            'EU' as region,
            timestamp
        FROM orders
        WHERE country IN ('UK', 'DE', 'FR', 'IT', 'ES')
        EMIT CHANGES;
    "#;

    let asia_orders_csas = r#"
        CREATE STREAM asia_orders AS
        SELECT
            order_id,
            customer_id,
            amount,
            'ASIA' as region,
            timestamp
        FROM orders
        WHERE country IN ('JP', 'CN', 'KR', 'IN')
        EMIT CHANGES;
    "#;

    println!("SQL (US):\n{}\n", us_orders_csas);
    println!("SQL (EU):\n{}\n", eu_orders_csas);
    println!("SQL (ASIA):\n{}\n", asia_orders_csas);
    println!("Purpose: Split single orders stream into regional streams");
    println!("Pattern: Fan-out (1 input → 3 outputs)");
    println!("Use Case: Regional processing, data residency, localization\n");

    for (name, csas) in [
        ("us_orders", us_orders_csas),
        ("eu_orders", eu_orders_csas),
        ("asia_orders", asia_orders_csas),
    ] {
        match server
            .deploy_job(
                name.to_string(),
                "v1".to_string(),
                csas.to_string(),
                "orders".to_string(),
            )
            .await
        {
            Ok(_) => println!("✅ {} stream deployed", name),
            Err(e) => println!("⚠️  Expected error for {}: {}", name, e),
        }
    }

    // ========================================================================
    // Example 6: CSAS Format Transformation
    // ========================================================================
    println!("\n🔄 Example 6: Format Transformation (JSON → Avro)");
    println!("--------------------------------------------------");

    let avro_orders_csas = r#"
        CREATE STREAM orders_avro INTO orders_avro_topic AS
        SELECT
            order_id,
            customer_id,
            amount,
            status,
            timestamp
        FROM orders_json_stream
        WITH (
            'sink.format' = 'avro',
            'sink.schema.registry.url' = 'http://schema-registry:8081',
            'sink.topic' = 'orders-avro'
        )
        EMIT CHANGES;
    "#;

    println!("SQL:\n{}\n", avro_orders_csas);
    println!("Purpose: Convert JSON stream to Avro format");
    println!("Use Case: Format standardization, schema enforcement");
    println!("Output: Avro-encoded orders in orders_avro_topic\n");

    match server
        .deploy_job(
            "orders_avro".to_string(),
            "v1".to_string(),
            avro_orders_csas.to_string(),
            "orders_json_stream".to_string(),
        )
        .await
    {
        Ok(_) => println!("✅ Avro transformation stream deployed"),
        Err(e) => println!("⚠️  Expected error: {}", e),
    }

    // ========================================================================
    // Summary: CSAS vs CTAS
    // ========================================================================
    println!("\n📋 Summary: When to Use CSAS vs CTAS");
    println!("=====================================\n");

    println!("✅ Use CSAS (CREATE STREAM AS SELECT) when:");
    println!("   • You need to FORWARD/TRANSFORM data (not query it)");
    println!("   • You need real-time alerting or notifications");
    println!("   • You want minimal memory footprint");
    println!("   • You're building stream-to-stream ETL");
    println!("   • You need format transformations (JSON → Avro)");
    println!("   • You're implementing fan-out patterns\n");

    println!("✅ Use CTAS (CREATE TABLE AS SELECT) when:");
    println!("   • You need to QUERY data with SQL SELECT");
    println!("   • You need fast lookups by key (O(1) access)");
    println!("   • You're building real-time dashboards");
    println!("   • You need to JOIN with other queries");
    println!("   • Dataset is manageable (< 10M records or use CompactTable)\n");

    println!("✅ Use BOTH when:");
    println!("   • CTAS for lookup tables (customers, products)");
    println!("   • CSAS for enriching streams with table JOINs");
    println!("   • CTAS for analytics, CSAS for alerting\n");

    // ========================================================================
    // Memory Comparison
    // ========================================================================
    println!("📊 Memory Comparison:");
    println!("--------------------");
    println!("CSAS: ~10MB (only current batch)");
    println!("CTAS (normal): ~100MB - 1GB (depends on data size)");
    println!("CTAS (compact): ~10MB - 100MB (90% reduction)\n");

    println!("🎯 Key Insight:");
    println!("---------------");
    println!("If you don't need SQL queries → Use CSAS");
    println!("If you need SQL queries → Use CTAS");
    println!("For complex pipelines → Use both together\n");

    // Keep the program running briefly
    println!("⏳ Demo complete. Exiting in 3 seconds...");
    sleep(Duration::from_secs(3)).await;

    Ok(())
}
