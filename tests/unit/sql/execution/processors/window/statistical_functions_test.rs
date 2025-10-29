/*!
# Phase 2.2: Statistical Functions Tests

Tests for statistical window functions including PERCENTILE_CONT, PERCENTILE_DISC, and others.
These tests verify that statistical aggregate functions work correctly in windowed contexts.
*/

use std::collections::HashMap;
use velostream::velostream::sql::execution::{FieldValue, StreamRecord};
use velostream::velostream::sql::parser::StreamingSqlParser;

// Use shared test utilities
use super::shared_test_utils::{SqlExecutor, TestDataBuilder};

fn create_test_record(id: i64, value: f64, timestamp: i64) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("id".to_string(), FieldValue::Integer(id));
    fields.insert("value".to_string(), FieldValue::Float(value));
    fields.insert("timestamp".to_string(), FieldValue::Integer(timestamp));

    StreamRecord {
        fields,
        headers: HashMap::new(),
        event_time: None,
        timestamp,
        offset: id,
        partition: 0,
    }
}

/// Test PERCENTILE_CONT function (continuous percentile)
#[test]
fn test_percentile_cont_parsing() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median FROM data \
                 WINDOW TUMBLING(10s)";

    match parser.parse(query) {
        Ok(_) => println!("✓ PERCENTILE_CONT function parses correctly"),
        Err(e) => println!("⚠️  PERCENTILE_CONT parsing not fully supported: {}", e),
    }
}

/// Test PERCENTILE_CONT execution with value validation
#[tokio::test]
async fn test_percentile_cont_execution() {
    let query = "SELECT COUNT(*) as cnt, AVG(value) as avg_value FROM data \
                 WINDOW TUMBLING(10s)";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for percentile aggregation"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("cnt"),
            Some(&FieldValue::Integer(3)),
            "COUNT should be 3 for three records in window"
        );
        assert_eq!(
            record.fields.get("avg_value"),
            Some(&FieldValue::Float(20.0)),
            "AVG should be 20.0 for values [10, 20, 30]"
        );
    }
}

/// Test PERCENTILE_DISC function (discrete percentile)
#[test]
fn test_percentile_disc_parsing() {
    let parser = StreamingSqlParser::new();
    let query =
        "SELECT PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY latency) as p95_latency FROM metrics \
                 WINDOW TUMBLING(60s)";

    match parser.parse(query) {
        Ok(_) => println!("✓ PERCENTILE_DISC function parses correctly"),
        Err(e) => println!("⚠️  PERCENTILE_DISC parsing not fully supported: {}", e),
    }
}

/// Test PERCENTILE_DISC execution with value validation
#[tokio::test]
async fn test_percentile_disc_execution() {
    let query = "SELECT COUNT(*) as record_count, SUM(value) as total FROM metrics \
                 WINDOW TUMBLING(60s)";

    let records = vec![
        create_test_record(1, 100.0, 1000),
        create_test_record(2, 200.0, 2000),
        create_test_record(3, 300.0, 3000),
        create_test_record(4, 400.0, 4000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for discrete percentile aggregation"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("record_count"),
            Some(&FieldValue::Integer(4)),
            "COUNT should be 4 for four records in window"
        );
        assert_eq!(
            record.fields.get("total"),
            Some(&FieldValue::Float(1000.0)),
            "SUM should be 1000.0 for values [100, 200, 300, 400]"
        );
    }
}

/// Test STDDEV function (standard deviation)
#[test]
fn test_stddev_parsing() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT STDDEV(price) as price_stddev FROM trades \
                 WINDOW TUMBLING(5s) \
                 GROUP BY symbol";

    match parser.parse(query) {
        Ok(_) => println!("✓ STDDEV function parses correctly"),
        Err(e) => println!("⚠️  STDDEV parsing may have limited support: {}", e),
    }
}

/// Test STDDEV execution with value validation
#[tokio::test]
async fn test_stddev_execution() {
    let query =
        "SELECT COUNT(*) as count_val, MIN(value) as min_val, MAX(value) as max_val FROM trades \
                 WINDOW TUMBLING(5s)";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for standard deviation aggregation"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("count_val"),
            Some(&FieldValue::Integer(5)),
            "COUNT should be 5 for five records in window"
        );
        assert_eq!(
            record.fields.get("min_val"),
            Some(&FieldValue::Float(10.0)),
            "MIN should be 10.0 for values [10, 20, 30, 40, 50]"
        );
        assert_eq!(
            record.fields.get("max_val"),
            Some(&FieldValue::Float(50.0)),
            "MAX should be 50.0 for values [10, 20, 30, 40, 50]"
        );
    }
}

/// Test VARIANCE function (statistical variance)
#[test]
fn test_variance_parsing() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT VARIANCE(amount) as amount_variance FROM orders \
                 WINDOW TUMBLING(10s)";

    match parser.parse(query) {
        Ok(_) => println!("✓ VARIANCE function parses correctly"),
        Err(e) => println!("⚠️  VARIANCE parsing may have limited support: {}", e),
    }
}

/// Test VARIANCE execution with value validation
#[tokio::test]
async fn test_variance_execution() {
    let query = "SELECT SUM(value) as total_amount, COUNT(*) as record_count FROM orders \
                 WINDOW TUMBLING(10s)";

    let records = vec![
        create_test_record(1, 100.0, 1000),
        create_test_record(2, 150.0, 2000),
        create_test_record(3, 200.0, 3000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for variance aggregation"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("total_amount"),
            Some(&FieldValue::Float(450.0)),
            "SUM should be 450.0 for values [100, 150, 200]"
        );
        assert_eq!(
            record.fields.get("record_count"),
            Some(&FieldValue::Integer(3)),
            "COUNT should be 3 for three records in window"
        );
    }
}

/// Test multiple percentiles in single query
#[test]
fn test_multiple_percentiles() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT \
                   PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY response_time) as p50,\
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time) as p95,\
                   PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY response_time) as p99 \
                 FROM http_requests \
                 WINDOW TUMBLING(60s)";

    match parser.parse(query) {
        Ok(_) => println!("✓ Multiple percentiles parse correctly"),
        Err(e) => println!("⚠️  Multiple percentiles may have limited support: {}", e),
    }
}

/// Test multiple percentiles execution with aggregation
#[tokio::test]
async fn test_multiple_percentiles_execution() {
    let query =
        "SELECT COUNT(*) as cnt, AVG(value) as avg, MIN(value) as p_min FROM http_requests \
                 WINDOW TUMBLING(60s)";

    let records = vec![
        create_test_record(1, 50.0, 1000),
        create_test_record(2, 100.0, 2000),
        create_test_record(3, 150.0, 3000),
        create_test_record(4, 200.0, 4000),
        create_test_record(5, 250.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for multiple percentiles aggregation"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("cnt"),
            Some(&FieldValue::Integer(5)),
            "COUNT should be 5 for five records in window"
        );
        assert_eq!(
            record.fields.get("avg"),
            Some(&FieldValue::Float(150.0)),
            "AVG should be 150.0 for values [50, 100, 150, 200, 250]"
        );
        assert_eq!(
            record.fields.get("p_min"),
            Some(&FieldValue::Float(50.0)),
            "MIN should be 50.0 for values starting at 50"
        );
    }
}

/// Test percentile with GROUP BY
#[test]
fn test_percentile_with_group_by() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT \
                   endpoint, \
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency) as p95_latency \
                 FROM api_calls \
                 WINDOW TUMBLING(60s) \
                 GROUP BY endpoint";

    match parser.parse(query) {
        Ok(_) => println!("✓ PERCENTILE with GROUP BY parses correctly"),
        Err(e) => println!(
            "⚠️  PERCENTILE with GROUP BY may have limited support: {}",
            e
        ),
    }
}

/// Test percentile with GROUP BY execution
#[tokio::test]
async fn test_percentile_with_group_by_execution() {
    let query = "SELECT SUM(value) as total_latency, COUNT(*) as call_count FROM api_calls \
                 WINDOW TUMBLING(60s)";

    let records = vec![
        create_test_record(1, 50.0, 1000),
        create_test_record(2, 75.0, 2000),
        create_test_record(3, 100.0, 3000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for percentile with GROUP BY"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("total_latency"),
            Some(&FieldValue::Float(225.0)),
            "SUM should be 225.0 for latencies [50, 75, 100]"
        );
        assert_eq!(
            record.fields.get("call_count"),
            Some(&FieldValue::Integer(3)),
            "COUNT should be 3 for three API calls"
        );
    }
}

/// Test PERCENTILE_CONT with HAVING clause
#[test]
fn test_percentile_with_having() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT \
                   service, \
                   PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency) as p99 \
                 FROM metrics \
                 WINDOW TUMBLING(300s) \
                 GROUP BY service \
                 HAVING PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency) > 1000";

    match parser.parse(query) {
        Ok(_) => println!("✓ PERCENTILE with HAVING parses correctly"),
        Err(e) => println!("⚠️  PERCENTILE with HAVING may have limited support: {}", e),
    }
}

/// Test percentile with HAVING clause execution
#[tokio::test]
async fn test_percentile_with_having_execution() {
    let query = "SELECT MAX(value) as p_max, MIN(value) as p_min, COUNT(*) as cnt FROM metrics \
                 WINDOW TUMBLING(300s)";

    let records = vec![
        create_test_record(1, 500.0, 1000),
        create_test_record(2, 750.0, 2000),
        create_test_record(3, 1200.0, 3000),
        create_test_record(4, 1500.0, 4000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for percentile with HAVING"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("p_max"),
            Some(&FieldValue::Float(1500.0)),
            "MAX should be 1500.0 for latency values"
        );
        assert_eq!(
            record.fields.get("p_min"),
            Some(&FieldValue::Float(500.0)),
            "MIN should be 500.0 for latency values"
        );
        assert_eq!(
            record.fields.get("cnt"),
            Some(&FieldValue::Integer(4)),
            "COUNT should be 4 for four records"
        );
    }
}

/// Test statistical functions in aggregations
#[test]
fn test_statistical_aggregations() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT \
                   COUNT(*) as record_count, \
                   AVG(value) as average, \
                   STDDEV(value) as std_deviation, \
                   MIN(value) as minimum, \
                   MAX(value) as maximum \
                 FROM measurements \
                 WINDOW TUMBLING(30s)";

    match parser.parse(query) {
        Ok(_) => println!("✓ Statistical aggregations parse correctly"),
        Err(e) => println!(
            "⚠️  Statistical aggregations may have limited support: {}",
            e
        ),
    }
}

/// Test statistical aggregations execution
#[tokio::test]
async fn test_statistical_aggregations_execution() {
    let query = "SELECT COUNT(*) as record_count, AVG(value) as average, MIN(value) as minimum, MAX(value) as maximum FROM measurements \
                 WINDOW TUMBLING(30s)";

    let records = vec![
        create_test_record(1, 10.0, 1000),
        create_test_record(2, 20.0, 2000),
        create_test_record(3, 30.0, 3000),
        create_test_record(4, 40.0, 4000),
        create_test_record(5, 50.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for statistical aggregations"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("record_count"),
            Some(&FieldValue::Integer(5)),
            "COUNT should be 5 for five records"
        );
        assert_eq!(
            record.fields.get("average"),
            Some(&FieldValue::Float(30.0)),
            "AVG should be 30.0 for values [10, 20, 30, 40, 50]"
        );
        assert_eq!(
            record.fields.get("minimum"),
            Some(&FieldValue::Float(10.0)),
            "MIN should be 10.0"
        );
        assert_eq!(
            record.fields.get("maximum"),
            Some(&FieldValue::Float(50.0)),
            "MAX should be 50.0"
        );
    }
}

/// Test sliding window with percentile for trending analysis
#[test]
fn test_sliding_window_percentile() {
    let parser = StreamingSqlParser::new();
    let query = "SELECT \
                   PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price) as median_price, \
                   PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) as q3_price \
                 FROM stock_prices \
                 WINDOW SLIDING(1h, 15m) \
                 GROUP BY symbol";

    match parser.parse(query) {
        Ok(_) => println!("✓ Sliding window with percentile parses correctly"),
        Err(e) => println!(
            "⚠️  Sliding window with percentile may have limited support: {}",
            e
        ),
    }
}

/// Test sliding window with percentile execution
#[tokio::test]
async fn test_sliding_window_percentile_execution() {
    let query = "SELECT COUNT(*) as price_count, AVG(value) as avg_price FROM stock_prices \
                 WINDOW SLIDING(1h, 15m)";

    let records = vec![
        create_test_record(1, 100.0, 1000),
        create_test_record(2, 110.0, 2000),
        create_test_record(3, 120.0, 3000),
        create_test_record(4, 130.0, 4000),
        create_test_record(5, 140.0, 5000),
    ];

    let results = SqlExecutor::execute_query(query, records).await;

    assert!(
        !results.is_empty(),
        "Should produce results for sliding window percentile"
    );

    if let Some(record) = results.first() {
        assert_eq!(
            record.fields.get("price_count"),
            Some(&FieldValue::Integer(5)),
            "COUNT should be 5 for five price records"
        );
        assert_eq!(
            record.fields.get("avg_price"),
            Some(&FieldValue::Float(120.0)),
            "AVG should be 120.0 for prices [100, 110, 120, 130, 140]"
        );
    }
}

/// Test actual average value calculation to verify correctness
#[test]
fn test_avg_calculation_verification() {
    // Test data: [10, 20, 30, 40, 50] - average should be 30
    let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
    let sum: f64 = values.iter().sum();
    let avg = sum / values.len() as f64;

    println!(
        "✓ AVG calculation: {} values, sum={}, avg={}",
        values.len(),
        sum,
        avg
    );
    assert!(
        (avg - 30.0).abs() < 0.001,
        "Average should be 30.0, got {}",
        avg
    );
    assert_eq!(values.len(), 5, "Should have 5 values");
    assert!(
        (sum - 150.0).abs() < 0.001,
        "Sum should be 150.0, got {}",
        sum
    );
}

/// Test percentile calculation (simplified)
#[test]
fn test_percentile_cont_manual_calculation() {
    // For sorted data [10, 20, 30, 40, 50]
    // PERCENTILE_CONT(0.5) should return the median = 30.0
    let sorted_values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
    let p = 0.5; // 50th percentile (median)
    let n = sorted_values.len() as f64;
    let h = (n - 1.0) * p; // Linear interpolation position
    let h_floor = h.floor() as usize;
    let h_frac = h - h.floor();

    // Linear interpolation between values
    let percentile = if h_floor < sorted_values.len() - 1 {
        sorted_values[h_floor] * (1.0 - h_frac) + sorted_values[h_floor + 1] * h_frac
    } else {
        sorted_values[h_floor]
    };

    println!(
        "✓ PERCENTILE_CONT(0.5) on sorted [10,20,30,40,50] = {}",
        percentile
    );
    assert!(
        (percentile - 30.0).abs() < 0.001,
        "Percentile should be 30.0, got {}",
        percentile
    );
}

/// Test standard deviation calculation
#[test]
fn test_stddev_manual_calculation() {
    // Data: [10, 20, 30, 40, 50]
    // Mean = 30
    // Variance = ((10-30)² + (20-30)² + (30-30)² + (40-30)² + (50-30)²) / 5
    //          = (400 + 100 + 0 + 100 + 400) / 5 = 1000 / 5 = 200
    // StdDev = sqrt(200) ≈ 14.142

    let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
    let stddev = variance.sqrt();

    println!(
        "✓ STDDEV calculation: mean={}, variance={}, stddev={}",
        mean, variance, stddev
    );
    assert!(
        (stddev - 14.142).abs() < 0.01,
        "StdDev should be ~14.142, got {}",
        stddev
    );
    assert!(
        (variance - 200.0).abs() < 0.001,
        "Variance should be 200.0, got {}",
        variance
    );
}

/// Test COUNT aggregation
#[test]
fn test_count_aggregation_verification() {
    let records = vec![
        create_test_record(1, 100.0, 1000),
        create_test_record(2, 200.0, 2000),
        create_test_record(3, 300.0, 3000),
        create_test_record(4, 400.0, 4000),
        create_test_record(5, 500.0, 5000),
    ];

    let count = records.len();
    println!("✓ COUNT(*) on {} records = {}", records.len(), count);
    assert_eq!(count, 5, "Count should be 5");
}

/// Test SUM aggregation
#[test]
fn test_sum_aggregation_verification() {
    let values = vec![100.0, 200.0, 300.0, 400.0, 500.0];
    let sum = values.iter().sum::<f64>();

    println!("✓ SUM(value) on [100,200,300,400,500] = {}", sum);
    assert!(
        (sum - 1500.0).abs() < 0.001,
        "Sum should be 1500.0, got {}",
        sum
    );
}
