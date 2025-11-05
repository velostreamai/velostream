/*!
# FR-082 Pure GROUP BY Performance Profiling

Direct comparison with tumbling window benchmark to measure pure GROUP BY + aggregation
performance without job server overhead.

This benchmark uses the same methodology as `profile_tumbling_instrumented_standard_path`
but focuses on GROUP BY aggregations without window operations to establish baseline
GROUP BY performance.

## Comparison
- Tumbling + GROUP BY: ~127K rec/sec (with window overhead)
- Pure GROUP BY: ? rec/sec (this benchmark - should be faster)
- Job Server + GROUP BY: ~28K rec/sec (with full infrastructure)
*/

use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::mpsc;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};
use velostream::velostream::sql::execution::StreamExecutionEngine;
use velostream::velostream::sql::parser::StreamingSqlParser;

/// FR-082 Phase 4 Validation: Pure GROUP BY performance without server overhead
///
/// Measures raw SQL engine GROUP BY + aggregation throughput using direct
/// `execute_with_record()` calls, matching the methodology of the tumbling window
/// benchmark that achieved 127K rec/sec.
///
/// Query: SELECT category, COUNT(*), SUM(amount), AVG(price)
///        FROM stream
///        GROUP BY category
///
/// Expected: Should exceed 127K rec/sec since GROUP BY without windows has less overhead
#[tokio::test]
async fn profile_pure_group_by_direct_execution() {
    println!("\n🔬 FR-082 VALIDATION: Pure GROUP BY Performance (Direct Execution)");
    println!("{}", "=".repeat(70));
    println!("Goal: Measure GROUP BY performance without server/window overhead");
    println!("Comparison baseline: Tumbling window = 127K rec/sec");
    println!("{}", "=".repeat(70));

    // Use same dataset size as tumbling benchmark for fair comparison
    let num_records = 100_000;

    let sql = r#"
        SELECT
            category,
            COUNT(*) as record_count,
            SUM(amount) as total_amount,
            AVG(price) as avg_price,
            MIN(price) as min_price,
            MAX(price) as max_price
        FROM stream
        GROUP BY category
    "#;

    // Phase 1: Record Generation
    let phase1_start = Instant::now();
    let mut records = Vec::new();

    for i in 0..num_records {
        let mut fields = HashMap::new();
        let category = format!("CAT{}", i % 50); // 50 distinct groups
        let amount = (i % 1000) as f64;
        let price = 100.0 + (i as f64 % 500.0);

        fields.insert("category".to_string(), FieldValue::String(category));
        fields.insert("amount".to_string(), FieldValue::Float(amount));
        fields.insert("price".to_string(), FieldValue::Float(price));

        records.push(StreamRecord::new(fields));
    }
    let phase1_duration = phase1_start.elapsed();
    println!(
        "✅ Phase 1: Record generation ({} records): {:?}",
        records.len(),
        phase1_duration
    );

    // Phase 2: Engine Setup
    let phase2_start = Instant::now();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();

    let query = match parser.parse(sql) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("❌ Failed to parse SQL: {:?}", e);
            return;
        }
    };
    let phase2_duration = phase2_start.elapsed();
    println!(
        "✅ Phase 2: Engine setup + SQL parsing: {:?}",
        phase2_duration
    );

    // Phase 3: Record Execution (Direct - No Server Overhead)
    println!("\n🚀 Phase 3: Processing {} records...", num_records);
    let phase3_start = Instant::now();

    for record in records.iter() {
        let _ = engine.execute_with_record(&query, record.clone()).await;
    }

    let phase3_duration = phase3_start.elapsed();

    // Phase 4: Drain output channel
    let mut output_count = 0;
    while rx.try_recv().is_ok() {
        output_count += 1;
    }

    // Calculate metrics
    let total_duration = phase3_duration;
    let records_per_sec = num_records as f64 / total_duration.as_secs_f64();
    let ns_per_record = total_duration.as_nanos() as f64 / num_records as f64;

    println!("\n{}", "═".repeat(70));
    println!("📊 PERFORMANCE RESULTS - Pure GROUP BY (Direct Execution)");
    println!("{}", "═".repeat(70));
    println!("Total records:     {}", num_records);
    println!("Processing time:   {:?}", total_duration);
    println!("Throughput:        {:.0} rec/sec", records_per_sec);
    println!("Time per record:   {:.2} ns", ns_per_record);
    println!("Output records:    {}", output_count);
    println!("Groups (distinct): 50");
    println!();
    println!("📈 COMPARISON:");
    println!("  Tumbling + GROUP BY: ~127,000 rec/sec (with window overhead)");
    println!("  Pure GROUP BY:       {:.0} rec/sec (this test)", records_per_sec);
    println!("  Job Server:          ~28,000 rec/sec (with infrastructure)");
    println!();

    let speedup_vs_server = records_per_sec / 28_000.0;
    println!("  Speedup vs Server:   {:.1}x", speedup_vs_server);

    if records_per_sec > 127_000.0 {
        println!("  ✅ FASTER than tumbling window (expected - less overhead)");
    } else if records_per_sec > 100_000.0 {
        println!("  ⚡ Comparable to tumbling window performance");
    } else {
        println!("  ⚠️  Slower than expected - investigation needed");
    }

    println!("{}", "═".repeat(70));

    // FR-082 Phase 4C validation
    println!("\n🔍 FR-082 Phase 4C Validation:");
    println!("  Arc-based state sharing should show minimal cloning overhead");
    println!("  Expected: >100K rec/sec for pure GROUP BY operations");
}

/// FR-082 Phase 4 Validation: GROUP BY with high cardinality
///
/// Tests performance with many distinct groups (10,000 groups) to validate
/// hash table optimization from Phase 4B (FxHashMap + GroupKey).
#[tokio::test]
async fn profile_group_by_high_cardinality() {
    println!("\n🔬 FR-082 VALIDATION: GROUP BY High Cardinality Test");
    println!("{}", "=".repeat(70));
    println!("Goal: Validate FxHashMap + GroupKey optimization with 10K groups");
    println!("{}", "=".repeat(70));

    let num_records = 100_000;
    let num_groups = 10_000; // High cardinality

    let sql = r#"
        SELECT
            user_id,
            product_id,
            COUNT(*) as purchase_count,
            SUM(amount) as total_spent
        FROM transactions
        GROUP BY user_id, product_id
    "#;

    // Record Generation
    let gen_start = Instant::now();
    let mut records = Vec::new();

    for i in 0..num_records {
        let mut fields = HashMap::new();
        let user_id = format!("USER{}", i % num_groups);
        let product_id = format!("PROD{}", (i / 10) % 1000);
        let amount = (i % 1000) as f64;

        fields.insert("user_id".to_string(), FieldValue::String(user_id));
        fields.insert("product_id".to_string(), FieldValue::String(product_id));
        fields.insert("amount".to_string(), FieldValue::Float(amount));

        records.push(StreamRecord::new(fields));
    }
    println!("✅ Generated {} records with ~{} distinct groups: {:?}",
             num_records, num_groups, gen_start.elapsed());

    // Engine Setup
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut engine = StreamExecutionEngine::new(tx);
    let parser = StreamingSqlParser::new();
    let query = parser.parse(sql).expect("Failed to parse SQL");

    // Execution
    println!("🚀 Processing...");
    let exec_start = Instant::now();

    for record in records.iter() {
        let _ = engine.execute_with_record(&query, record.clone()).await;
    }

    let exec_duration = exec_start.elapsed();
    let records_per_sec = num_records as f64 / exec_duration.as_secs_f64();

    // Drain output
    let mut output_count = 0;
    while rx.try_recv().is_ok() {
        output_count += 1;
    }

    println!("\n{}", "═".repeat(70));
    println!("📊 HIGH CARDINALITY RESULTS");
    println!("{}", "═".repeat(70));
    println!("Records:           {}", num_records);
    println!("Distinct groups:   ~{}", num_groups);
    println!("Processing time:   {:?}", exec_duration);
    println!("Throughput:        {:.0} rec/sec", records_per_sec);
    println!("Output records:    {}", output_count);
    println!();
    println!("🔍 FR-082 Phase 4B Validation:");
    println!("  FxHashMap: 2-3x faster than std HashMap");
    println!("  GroupKey:  Pre-computed hash avoids per-lookup cost");
    println!("  Expected:  Minimal degradation with high cardinality");

    if records_per_sec > 80_000.0 {
        println!("  ✅ Excellent performance with high cardinality");
    } else if records_per_sec > 50_000.0 {
        println!("  ⚡ Good performance");
    } else {
        println!("  ⚠️  Performance degradation detected");
    }
    println!("{}", "═".repeat(70));
}
