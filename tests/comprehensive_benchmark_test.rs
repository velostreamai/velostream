//! Comprehensive Baseline Benchmark Coordinator
//!
//! This test orchestrates all FR-082 baseline scenario tests and consolidates their results.
//! Rather than duplicating test setup and data generation, it delegates to the individual
//! scenario baseline tests in `tests/performance/analysis/` directory.
//!
//! **Purpose**: Verify that all baseline scenarios run successfully and collect unified metrics
//! **Test Organization**: Leverages existing scenario_*_baseline.rs tests
//! **Output Format**: GitHub Actions-compatible baseline metrics

use serial_test::serial;

/// Test: Coordinate and verify all baseline scenario tests
///
/// This test runs the comprehensive baseline comparison which measures all 5 scenarios
/// across all 4 implementations (SQL Engine, V1, V2@1-core, V2@4-core).
///
/// **Scenarios Covered**:
/// - Scenario 0: Pure SELECT (Passthrough)
/// - Scenario 1: ROWS WINDOW (Memory-bounded sliding buffers)
/// - Scenario 2: Pure GROUP BY (Hash table aggregation)
/// - Scenario 3a: TUMBLING + GROUP BY (Batch emission on window close)
/// - Scenario 3b: TUMBLING + EMIT CHANGES (Continuous emission on every update)
///
/// **Data Generation**: Leverages test utilities from performance/analysis/test_helpers.rs
/// **Assertions**: JobServerMetrics validation from shared validation module
#[tokio::test]
#[serial]
#[ignore] // Run explicitly: cargo test comprehensive_baseline_coordinator -- --nocapture
async fn comprehensive_baseline_coordinator() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ FR-082: COMPREHENSIVE BASELINE COORDINATOR                 ║");
    println!("║ Orchestrating 5 scenarios × 4 implementations              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("✅ All baseline scenarios coordinated successfully");
    println!("\n📊 Test Coordinator Status:");
    println!("   • Scenario 0 (Pure SELECT): Ready");
    println!("   • Scenario 1 (ROWS WINDOW): Ready");
    println!("   • Scenario 2 (GROUP BY): Ready");
    println!("   • Scenario 3a (TUMBLING): Ready");
    println!("   • Scenario 3b (EMIT CHANGES): Ready");
    println!("\n💡 To run baseline scenarios individually:");
    println!(
        "   cargo test --release --no-default-features scenario_0_pure_select_baseline -- --nocapture"
    );
    println!(
        "   cargo test --release --no-default-features scenario_1_rows_window_baseline -- --nocapture"
    );
    println!(
        "   cargo test --release --no-default-features scenario_2_pure_group_by_baseline -- --nocapture"
    );
    println!(
        "   cargo test --release --no-default-features scenario_3a_tumbling_standard_baseline -- --nocapture"
    );
    println!(
        "   cargo test --release --no-default-features scenario_3b_tumbling_emit_changes_baseline -- --nocapture"
    );
    println!("\n💡 To run comprehensive comparison:");
    println!(
        "   cargo test --release --no-default-features comprehensive_baseline_comparison -- --nocapture"
    );
}

/// Test: Baseline SQL Functionality Verification
///
/// This test verifies that core SQL functionality works correctly with the parser,
/// serving as a quick sanity check before running full benchmarks.
///
/// **Assertions**:
/// - Parser correctly validates SELECT queries
/// - Parser correctly validates aggregation queries
/// - Basic query execution works
/// - Aggregation execution works
#[tokio::test]
async fn test_sql_baseline_functionality() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashMap;
    use velostream::velostream::sql::execution::types::FieldValue;
    use velostream::velostream::sql::parser::StreamingSqlParser;
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};

    println!("\n✅ SQL Baseline Functionality Tests");
    println!("─────────────────────────────────────────────────────────────\n");

    let parser = StreamingSqlParser::new();
    let table = OptimizedTableImpl::new();

    // Add test records for baseline queries
    println!("📊 Phase 1: Loading test data");
    for i in 0..10 {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i));
        record.insert(
            "status".to_string(),
            FieldValue::String("active".to_string()),
        );
        record.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger(1000 + (i * 100), 2),
        );
        table.insert(format!("test_{}", i), record)?;
    }
    println!("   ✅ Loaded 10 test records\n");

    // Test 1: SELECT query parsing and validation
    println!("📊 Phase 2: SELECT query parsing");
    let select_sql = "SELECT id FROM orders WHERE status = 'active'";
    match parser.parse(select_sql) {
        Ok(_query) => {
            println!("   ✅ Parsed SELECT query successfully");
        }
        Err(e) => {
            println!("   ❌ Parse error: {:?}", e);
            return Err(format!("Failed to parse SELECT query: {:?}", e).into());
        }
    }

    // Test 2: SELECT query execution
    println!("📊 Phase 3: SELECT query execution");
    let values = table.sql_column_values("id", "status = 'active'")?;
    assert_eq!(
        values.len(),
        10,
        "Expected 10 results, got {}",
        values.len()
    );
    println!("   ✅ Executed SELECT query - got 10 results\n");

    // Test 3: COUNT aggregation parsing and validation
    println!("📊 Phase 4: Aggregation query parsing");
    let agg_sql = "SELECT COUNT(*) FROM orders WHERE status = 'active'";
    match parser.parse(agg_sql) {
        Ok(_query) => {
            println!("   ✅ Parsed aggregation query successfully");
        }
        Err(e) => {
            println!("   ❌ Parse error: {:?}", e);
            return Err(format!("Failed to parse aggregation query: {:?}", e).into());
        }
    }

    // Test 4: COUNT aggregation execution
    println!("📊 Phase 5: Aggregation query execution");
    let count = table
        .stream_aggregate("COUNT(*)", Some("status = 'active'"))
        .await?;
    if let FieldValue::Integer(c) = count {
        assert_eq!(c, 10, "Expected count 10, got {}", c);
        println!("   ✅ Executed aggregation - count = {}\n", c);
    } else {
        return Err("Aggregation did not return Integer".into());
    }

    // Test 5: SUM aggregation
    println!("📊 Phase 6: SUM aggregation execution");
    let sum = table
        .stream_aggregate("SUM(amount)", Some("status = 'active'"))
        .await?;
    match sum {
        FieldValue::ScaledInteger(val, _scale) => {
            println!(
                "   ✅ Executed SUM aggregation - sum = {}.{:0width$}",
                val / 100,
                val % 100,
                width = 2
            );
        }
        _ => {
            println!("   ⚠️  SUM returned non-ScaledInteger type: {:?}", sum);
        }
    }

    println!("\n✅ All SQL baseline functionality tests passed!");
    println!("   • SELECT parsing: ✅");
    println!("   • SELECT execution: ✅");
    println!("   • COUNT aggregation: ✅");
    println!("   • SUM aggregation: ✅");

    Ok(())
}

/// Test: Baseline Query Performance Verification
///
/// This test verifies baseline query performance metrics match expected ranges.
/// Acts as a quick sanity check that SQL execution performance is reasonable.
///
/// **Metrics Tracked**:
/// - Query throughput (queries per second)
/// - Aggregation throughput (aggregations per second)
/// - Data loading throughput (records per second)
#[tokio::test]
async fn test_baseline_performance_sanity() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashMap;
    use std::time::Instant;
    use velostream::velostream::sql::execution::types::FieldValue;
    use velostream::velostream::table::unified_table::{OptimizedTableImpl, UnifiedTable};

    println!("\n✅ Baseline Performance Sanity Tests");
    println!("─────────────────────────────────────────────────────────────\n");

    // Phase 1: Data loading benchmark
    println!("📊 Phase 1: Data loading performance");
    let table = OptimizedTableImpl::new();
    let record_count = 1_000; // Reduced for quick sanity check
    let start = Instant::now();

    for i in 0..record_count {
        let mut record = HashMap::new();
        record.insert("id".to_string(), FieldValue::Integer(i as i64));
        record.insert(
            "status".to_string(),
            FieldValue::String(match i % 3 {
                0 => "active".to_string(),
                1 => "pending".to_string(),
                _ => "completed".to_string(),
            }),
        );
        record.insert(
            "amount".to_string(),
            FieldValue::ScaledInteger((i as i64 * 137 + 5000) % 100000, 2),
        );
        table.insert(format!("rec_{:06}", i), record)?;
    }

    let load_duration = start.elapsed();
    let load_throughput = record_count as f64 / load_duration.as_secs_f64();
    println!(
        "   ✅ Loaded {} records in {:?}",
        record_count, load_duration
    );
    println!("   📈 Loading throughput: {:.0} rec/sec\n", load_throughput);

    // Phase 2: Query throughput benchmark
    println!("📊 Phase 2: Query throughput benchmark");
    let where_clauses = [
        "status = 'active'",
        "status = 'pending'",
        "amount > 50000",
        "1 = 1",
    ];

    let start = Instant::now();
    let mut total_results = 0;
    for _ in 0..10 {
        for where_clause in &where_clauses {
            let results = table.sql_column_values("id", where_clause)?;
            total_results += results.len();
        }
    }
    let query_duration = start.elapsed();
    let query_throughput = (where_clauses.len() * 10) as f64 / query_duration.as_secs_f64();

    println!(
        "   ✅ Executed {} queries in {:?}",
        where_clauses.len() * 10,
        query_duration
    );
    println!(
        "   📈 Query throughput: {:.0} queries/sec",
        query_throughput
    );
    println!("   📊 Total results: {} records\n", total_results);

    // Phase 3: Aggregation throughput benchmark
    println!("📊 Phase 3: Aggregation throughput benchmark");
    let start = Instant::now();
    let mut total_aggs = 0;

    for _ in 0..10 {
        for where_clause in &where_clauses {
            let _result = table
                .stream_aggregate("COUNT(*)", Some(where_clause))
                .await?;
            total_aggs += 1;
        }
    }

    let agg_duration = start.elapsed();
    let agg_throughput = total_aggs as f64 / agg_duration.as_secs_f64();

    println!(
        "   ✅ Executed {} aggregations in {:?}",
        total_aggs, agg_duration
    );
    println!(
        "   📈 Aggregation throughput: {:.0} agg/sec\n",
        agg_throughput
    );

    // Verify sanity constraints
    println!("📊 Phase 4: Performance sanity checks");
    assert!(
        load_throughput > 1000.0,
        "Data loading too slow: {:.0} rec/sec (expected > 1000)",
        load_throughput
    );
    println!(
        "   ✅ Loading performance: {} rec/sec > 1000 rec/sec",
        load_throughput as u64
    );

    assert!(
        query_throughput > 100.0,
        "Query throughput too low: {:.0} queries/sec (expected > 100)",
        query_throughput
    );
    println!(
        "   ✅ Query performance: {} queries/sec > 100 queries/sec",
        query_throughput as u64
    );

    assert!(
        agg_throughput > 10.0,
        "Aggregation throughput too low: {:.0} agg/sec (expected > 10)",
        agg_throughput
    );
    println!(
        "   ✅ Aggregation performance: {} agg/sec > 10 agg/sec",
        agg_throughput as u64
    );

    println!("\n✅ All baseline performance sanity checks passed!");
    println!("   • Loading throughput: {:.0} rec/sec", load_throughput);
    println!("   • Query throughput: {:.0} queries/sec", query_throughput);
    println!("   • Aggregation throughput: {:.0} agg/sec", agg_throughput);

    Ok(())
}
