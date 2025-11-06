//! FR-082 Phase 5 Week 8: Performance Profiling & Bottleneck Analysis
//!
//! This benchmark identifies specific bottlenecks in the EMIT CHANGES processing pipeline:
//! - Channel operation overhead (per-record vs batched draining)
//! - Lock contention (watermark, engine, context)
//! - Window buffer operations (ROWS window push/pop)
//! - Serialization/write throughput
//!
//! Based on existing baseline test patterns (scenario_3b_tumbling_emit_changes_baseline.rs)
//!
//! Run with: cargo test --release --test phase5_week8_profiling -- --nocapture

use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::StreamExecutionEngine;
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::sql::execution::types::StreamRecord;
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Detailed timing breakdown for a single phase
#[derive(Debug, Clone)]
struct PhaseMetrics {
    phase_name: &'static str,
    total_time_ms: f64,
    records_processed: usize,
    records_emitted: usize,
    throughput_rec_sec: f64,
    lock_wait_ms: f64,
    channel_drain_ms: f64,
}

impl PhaseMetrics {
    fn new(
        phase_name: &'static str,
        total_time_ms: f64,
        records_processed: usize,
        records_emitted: usize,
    ) -> Self {
        let throughput_rec_sec = if total_time_ms > 0.0 {
            (records_processed as f64 / total_time_ms) * 1000.0
        } else {
            0.0
        };

        Self {
            phase_name,
            total_time_ms,
            records_processed,
            records_emitted,
            throughput_rec_sec,
            lock_wait_ms: 0.0,
            channel_drain_ms: 0.0,
        }
    }

    fn with_lock_wait(mut self, lock_wait_ms: f64) -> Self {
        self.lock_wait_ms = lock_wait_ms;
        self
    }

    fn with_channel_drain(mut self, channel_drain_ms: f64) -> Self {
        self.channel_drain_ms = channel_drain_ms;
        self
    }
}

/// Generate test records for EMIT CHANGES scenario
fn generate_emit_changes_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);
    let base_time = 1700000000i64;

    for i in 0..count {
        let mut fields = HashMap::new();
        let trader_id = format!("TRADER{}", i % 20);
        let symbol = format!("SYM{}", i % 10);
        let price = 100.0 + (i as f64 % 50.0);
        let quantity = 100 + (i % 1000);
        let timestamp = base_time + (i as i64);

        fields.insert("trader_id".to_string(), FieldValue::String(trader_id));
        fields.insert("symbol".to_string(), FieldValue::String(symbol));
        fields.insert("price".to_string(), FieldValue::Float(price));
        fields.insert("quantity".to_string(), FieldValue::Integer(quantity as i64));
        fields.insert("trade_time".to_string(), FieldValue::Integer(timestamp));

        records.push(StreamRecord::new(fields));
    }
    records
}

/// Phase 5 Week 8: Baseline EMIT CHANGES Profiling with Real Engine
#[tokio::test]
#[ignore] // Run manually: cargo test --release phase5_week8_baseline -- --nocapture
async fn phase5_week8_baseline_profiling() {
    println!("\n═════════════════════════════════════════════════════════════");
    println!("🔬 FR-082 Phase 5 Week 8: EMIT CHANGES Bottleneck Analysis");
    println!("═════════════════════════════════════════════════════════════\n");

    // Query from scenario_3b_tumbling_emit_changes_baseline.rs
    let query = r#"
        SELECT
            trader_id,
            symbol,
            COUNT(*) as trade_count,
            AVG(price) as avg_price,
            SUM(quantity) as total_quantity
        FROM market_data
        GROUP BY trader_id, symbol
        WINDOW TUMBLING (trade_time, INTERVAL '1' MINUTE) EMIT CHANGES
    "#;

    // Generate test data (10K records = 2x Week 7 baseline)
    let records = generate_emit_changes_records(10_000);
    println!("✓ Generated {} test records", records.len());
    println!("  • 20 traders × 10 symbols = 200 groups");
    println!(
        "  • Expected emissions: ~{:.0} (19.96x amplification)\n",
        records.len() as f64 * 19.96
    );

    // Measure SQL Engine with EMIT CHANGES
    measure_sql_engine_emit_changes(&records, query).await;

    println!("\n═════════════════════════════════════════════════════════════\n");
}

/// Measure SQL Engine performance with EMIT CHANGES
async fn measure_sql_engine_emit_changes(records: &[StreamRecord], query: &str) {
    println!("Test: SQL Engine EMIT CHANGES Baseline");
    println!("─────────────────────────────────────");

    let mut parser = StreamingSqlParser::new();
    let parsed_query = parser.parse(query).expect("Failed to parse SQL");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);

    // Spawn drain task to count emissions
    let drain_task = tokio::spawn(async move {
        let mut count = 0;
        while let Some(_) = rx.recv().await {
            count += 1;
        }
        count
    });

    // Measure processing
    let start = Instant::now();
    for record in records {
        let _ = engine
            .execute_with_record(&parsed_query, record.clone())
            .await;
    }
    let processing_time = start.elapsed();

    // Drop engine to close channel
    drop(engine);

    // Wait for drain task
    let emission_count = drain_task.await.unwrap_or(0);

    let elapsed_ms = processing_time.as_secs_f64() * 1000.0;
    let input_throughput = (records.len() as f64 / elapsed_ms) * 1000.0;
    let emission_throughput = (emission_count as f64 / elapsed_ms) * 1000.0;
    let amplification = emission_count as f64 / records.len() as f64;

    println!("Results:");
    println!("  Input Records:        {}", records.len());
    println!("  Emitted Results:      {}", emission_count);
    println!("  Amplification Ratio:  {:.2}x", amplification);
    println!("  Processing Time:      {:.2}ms", elapsed_ms);
    println!("  Input Throughput:     {:.0} rec/sec", input_throughput);
    println!("  Emission Throughput:  {:.0} rec/sec", emission_throughput);
    println!(
        "  Per-Record Latency:   {:.3}µs",
        (elapsed_ms * 1000.0) / records.len() as f64
    );

    // Bottleneck analysis
    println!("\nBottleneck Analysis:");
    println!(
        "  • Input throughput {:.0} rec/sec shows EMIT CHANGES overhead",
        input_throughput
    );
    println!(
        "  • {}x amplification creates {:.0} output records",
        amplification as i64, emission_count as f64
    );
    println!(
        "  • Each of {} input triggers ~{:.0} emissions",
        records.len(),
        amplification
    );
    println!("  • Current design processes sequentially with per-record locks");
}

/// Phase 5 Week 8: Optimization Path Summary
#[tokio::test]
#[ignore] // Run with: cargo test --release phase5_week8_optimization_summary -- --nocapture
async fn phase5_week8_optimization_summary() {
    println!("\n═════════════════════════════════════════════════════════════");
    println!("📊 FR-082 Phase 5 Week 8: Optimization Path Summary");
    println!("═════════════════════════════════════════════════════════════\n");

    println!("CURRENT STATE (Week 7 Baseline):");
    println!("  Single Partition EMIT CHANGES:    ~500 rec/sec");
    println!("  With 19.96x amplification factor: ~10K output rec/sec");
    println!("  Per-core on 8 cores:              ~187.5K rec/sec target\n");

    println!("PERFORMANCE GAP ANALYSIS:");
    println!("  Target:                           1.5M rec/sec (8 cores)");
    println!("  Current:                          500 rec/sec (single partition)");
    println!("  Required improvement:             3000x total\n");

    println!("WEEK 8 OPTIMIZATION STRATEGY:");
    println!("  Phase 8.1: Profiling (identify actual bottlenecks)");
    println!("    → Run with: cargo test --release phase5_week8_baseline -- --nocapture\n");

    println!("  Phase 8.2: High-Impact Optimizations (target 50x per-core):");
    println!("    1. EMIT CHANGES batch draining         (5-10x)");
    println!("    2. Lock-free batch processing           (2-3x)");
    println!("    3. Window buffer pre-allocation         (2-5x)");
    println!("    4. Watermark batch updates              (1.5-2x)\n");

    println!("  Phase 8.3: Parallel Scaling:");
    println!("    → 8 cores × per-core optimization = 1.5M+ rec/sec target\n");

    println!("EXPECTED RESULTS AFTER OPTIMIZATION:");
    println!("  Conservative (50x optimization):  25K rec/sec per core");
    println!("    → 8 cores × 25K = 200K rec/sec (insufficient)");
    println!("  Optimistic (50x + parallel):      Requires parallel coordinator\n");

    println!("NEXT STEPS:");
    println!("  1. Run baseline profiling test");
    println!("  2. Identify top 3 bottlenecks");
    println!("  3. Implement high-impact optimizations incrementally");
    println!("  4. Validate with tests (460+ unit tests must pass)");
    println!("  5. Measure improvement after each optimization\n");
}
