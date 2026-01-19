//! Stream-Stream Join Metrics Demo
//!
//! Demonstrates how to expose join metrics through the unified MetricsProvider.
//! Shows the complete flow: JoinJobProcessor -> JoinJobStats -> MetricsProvider
//!
//! ## Metrics Exposed
//!
//! | Metric | Description |
//! |--------|-------------|
//! | `velo_join_left_records_total` | Records from left source |
//! | `velo_join_right_records_total` | Records from right source |
//! | `velo_join_matches_total` | Join matches emitted |
//! | `velo_join_interned_keys` | Unique keys interned (Arc<str> sharing) |
//! | `velo_join_interning_memory_saved_bytes` | Memory saved by interning |
//! | `velo_join_left_evictions_total` | Records evicted from left store |
//! | `velo_join_memory_pressure` | 0=Normal, 1=Warning, 2=Critical |
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example join_metrics_demo
//! ```

use std::collections::HashMap;

use chrono::Utc;
use velostream::velostream::observability::metrics::MetricsProvider;
use velostream::velostream::sql::execution::config::PrometheusConfig;
use velostream::velostream::sql::execution::join::{
    JoinConfig, JoinCoordinator, JoinCoordinatorConfig, JoinCoordinatorStats, JoinSide,
    JoinStateStoreConfig, MemoryPressure,
};
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};

/// Create a sample order record
fn create_order(order_id: &str, customer_id: i64, amount: f64, event_time_ms: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "order_id".to_string(),
        FieldValue::String(order_id.to_string()),
    );
    fields.insert("customer_id".to_string(), FieldValue::Integer(customer_id));
    fields.insert("amount".to_string(), FieldValue::Float(amount));
    fields.insert("event_time".to_string(), FieldValue::Integer(event_time_ms));

    StreamRecord {
        timestamp: event_time_ms,
        offset: 0,
        partition: 0,
        fields,
        headers: HashMap::new(),
        event_time: Some(
            chrono::DateTime::from_timestamp_millis(event_time_ms).unwrap_or_else(Utc::now),
        ),
        topic: None,
        key: None,
    }
}

/// Create a sample shipment record
fn create_shipment(
    shipment_id: &str,
    order_id: &str,
    carrier: &str,
    event_time_ms: i64,
) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert(
        "shipment_id".to_string(),
        FieldValue::String(shipment_id.to_string()),
    );
    fields.insert(
        "order_id".to_string(),
        FieldValue::String(order_id.to_string()),
    );
    fields.insert(
        "carrier".to_string(),
        FieldValue::String(carrier.to_string()),
    );
    fields.insert("event_time".to_string(), FieldValue::Integer(event_time_ms));

    StreamRecord {
        timestamp: event_time_ms,
        offset: 0,
        partition: 0,
        fields,
        headers: HashMap::new(),
        event_time: Some(
            chrono::DateTime::from_timestamp_millis(event_time_ms).unwrap_or_else(Utc::now),
        ),
        topic: None,
        key: None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║         Stream-Stream Join Metrics Demo                     ║");
    println!("║  Shows unified Prometheus metrics for join operations       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // =========================================================================
    // STEP 1: Create MetricsProvider (unified Prometheus registry)
    // =========================================================================
    println!("📊 Step 1: Creating MetricsProvider...");
    let prometheus_config = PrometheusConfig::default();
    let metrics_provider = MetricsProvider::new(prometheus_config).await?;
    println!("   ✓ MetricsProvider initialized with unified registry\n");

    // =========================================================================
    // STEP 2: Create JoinCoordinator with memory limits
    // =========================================================================
    println!("🔗 Step 2: Creating JoinCoordinator...");

    // Configure join with 1-hour interval
    // Join keys: order_id on both sides
    let join_keys = vec![("order_id".to_string(), "order_id".to_string())];
    let join_config = JoinConfig::interval_ms(
        "orders",    // left source
        "shipments", // right source
        join_keys,
        0,         // lower bound (shipment can come at same time as order)
        3_600_000, // upper bound (shipment within 1 hour of order)
    );

    // Configure state store with limits (to demonstrate eviction metrics)
    let store_config = JoinStateStoreConfig::with_all_limits(
        10_000,      // max 10K records per side
        100_000_000, // 100MB memory limit
        3_600_000,   // 1 hour TTL
    );

    // Create coordinator config with store limits
    let coordinator_config =
        JoinCoordinatorConfig::new(join_config).with_store_config(store_config);

    let mut coordinator = JoinCoordinator::with_config(coordinator_config);
    println!("   ✓ JoinCoordinator created with memory limits\n");

    // =========================================================================
    // STEP 3: Process some records through the join
    // =========================================================================
    println!("📥 Step 3: Processing records...");

    let base_time = Utc::now().timestamp_millis();

    // Process orders (left side)
    let orders = vec![
        create_order("ORD-001", 100, 150.00, base_time),
        create_order("ORD-002", 101, 250.00, base_time + 1000),
        create_order("ORD-003", 102, 350.00, base_time + 2000),
        create_order("ORD-004", 100, 450.00, base_time + 3000), // Same customer
        create_order("ORD-005", 103, 550.00, base_time + 4000),
    ];

    for order in &orders {
        let _ = coordinator.process(JoinSide::Left, order.clone());
    }
    println!("   ✓ Processed {} orders", orders.len());

    // Process shipments (right side) - some will match
    let shipments = vec![
        create_shipment("SHIP-001", "ORD-001", "FedEx", base_time + 5000),
        create_shipment("SHIP-002", "ORD-002", "UPS", base_time + 6000),
        create_shipment("SHIP-003", "ORD-003", "DHL", base_time + 7000),
        // ORD-004 and ORD-005 have no shipments yet
    ];

    let mut total_matches = 0;
    for shipment in &shipments {
        let matches = coordinator.process(JoinSide::Right, shipment.clone())?;
        total_matches += matches.len();
    }
    println!(
        "   ✓ Processed {} shipments, {} matches\n",
        shipments.len(),
        total_matches
    );

    // =========================================================================
    // STEP 4: Get coordinator stats and push to MetricsProvider
    // =========================================================================
    println!("📈 Step 4: Pushing stats to MetricsProvider...");

    let stats: JoinCoordinatorStats = coordinator.stats().clone();

    // This is the key integration point - push join stats to unified metrics
    metrics_provider.update_join_metrics("orders_shipments_join", &stats);

    // Optionally update memory pressure
    metrics_provider.update_join_memory_pressure("orders_shipments_join", MemoryPressure::Normal);

    println!("   ✓ Stats pushed to unified Prometheus registry\n");

    // =========================================================================
    // STEP 5: Display stats and export Prometheus metrics
    // =========================================================================
    println!("📊 Step 5: Join Statistics:");
    println!(
        "   ├── Left records processed:  {}",
        stats.left_records_processed
    );
    println!(
        "   ├── Right records processed: {}",
        stats.right_records_processed
    );
    println!("   ├── Matches emitted:         {}", stats.matches_emitted);
    println!("   ├── Left store size:         {}", stats.left_store_size);
    println!("   ├── Right store size:        {}", stats.right_store_size);
    println!(
        "   ├── Interned keys:           {}",
        stats.interned_key_count
    );
    println!(
        "   ├── Memory saved (bytes):    {}",
        stats.interning_memory_saved
    );
    println!("   ├── Left evictions:          {}", stats.left_evictions);
    println!("   └── Right evictions:         {}", stats.right_evictions);

    println!("\n📋 Prometheus Metrics Output:");
    println!("   (These would be available at http://localhost:9090/metrics)\n");

    let metrics_text = metrics_provider.get_metrics_text()?;

    // Filter and display only join metrics
    for line in metrics_text.lines() {
        if line.starts_with("velo_join_")
            || line.starts_with("# HELP velo_join")
            || line.starts_with("# TYPE velo_join")
        {
            println!("   {}", line);
        }
    }

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║  Demo Complete!                                             ║");
    println!("║                                                              ║");
    println!("║  Key Integration Points:                                     ║");
    println!("║  1. MetricsProvider::new() - unified Prometheus registry     ║");
    println!("║  2. update_join_metrics() - push JoinCoordinatorStats        ║");
    println!("║  3. get_metrics_text() - export for Prometheus scraping      ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    Ok(())
}
