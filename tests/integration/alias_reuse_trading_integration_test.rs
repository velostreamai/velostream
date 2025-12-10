/*!
# SELECT Clause Column Alias Reuse - Phase 3.2 Integration Tests

Comprehensive integration tests validating SELECT alias reuse functionality in real-world
financial trading scenarios. These tests go beyond unit tests by validating end-to-end
query execution with multiple records and complex business logic.

FR-078 enables queries to reference previously-defined aliases within the same SELECT clause:

```sql
SELECT
    volume / avg_volume AS spike_ratio,               -- Define alias
    CASE WHEN spike_ratio > 5 THEN 'EXTREME' END     -- Reference it!
FROM trades;
```

## Real-World Trading Scenarios Tested

1. **Volume Spike Detection**: Identify trading volumes that exceed historical averages
2. **Price Impact Analysis**: Calculate price changes and volatility metrics using aliases
3. **Circuit Breaker Logic**: Determine when to halt trading based on spike ratios
4. **Market Anomaly Detection**: Multi-tier classification of unusual market conditions
5. **Trade Profitability Analysis**: Calculate profit margins and ROI using chained aliases
6. **Risk Assessment**: Quantify risk using multiple derived metrics
7. **Performance Analysis**: Measure trader performance using complex alias chains
8. **Real-time Alerting**: Generate alerts based on multi-level conditions with aliases
*/

use std::collections::HashMap;
use tokio::sync::mpsc;
use velostream::velostream::schema::{FieldDefinition, Schema, StreamHandle};
use velostream::velostream::sql::ast::DataType;
use velostream::velostream::sql::context::StreamingSqlContext;
use velostream::velostream::sql::execution::{
    StreamExecutionEngine,
    types::{FieldValue, StreamRecord},
};
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Creates a trades stream schema with financial trading fields
fn create_trades_schema() -> Schema {
    Schema::new(vec![
        FieldDefinition::required("trade_id".to_string(), DataType::Integer),
        FieldDefinition::required("symbol".to_string(), DataType::String),
        FieldDefinition::required("volume".to_string(), DataType::Float),
        FieldDefinition::required("price".to_string(), DataType::Float),
        FieldDefinition::required("avg_volume".to_string(), DataType::Float),
        FieldDefinition::required("avg_price".to_string(), DataType::Float),
        FieldDefinition::required("timestamp".to_string(), DataType::Integer),
    ])
}

/// Helper to create a trade record with standard fields
fn create_trade_record(
    trade_id: i64,
    symbol: &str,
    volume: f64,
    price: f64,
    avg_volume: f64,
    avg_price: f64,
) -> StreamRecord {
    let mut fields = HashMap::new();
    fields.insert("trade_id".to_string(), FieldValue::Integer(trade_id));
    fields.insert("symbol".to_string(), FieldValue::String(symbol.to_string()));
    fields.insert("volume".to_string(), FieldValue::Float(volume));
    fields.insert("price".to_string(), FieldValue::Float(price));
    fields.insert("avg_volume".to_string(), FieldValue::Float(avg_volume));
    fields.insert("avg_price".to_string(), FieldValue::Float(avg_price));
    fields.insert(
        "timestamp".to_string(),
        FieldValue::Integer(1000 + trade_id),
    );

    StreamRecord {
        fields,
        timestamp: 1000 + trade_id,
        offset: trade_id,
        partition: 0,
        event_time: None,
        headers: HashMap::new(),
        topic: None,
        key: None,
    }
}

#[cfg(test)]
mod trading_integration_tests {
    use super::*;

    /// Test 1: Volume Spike Detection - The Original Use Case
    /// Detect when volume exceeds historical average by calculating spike_ratio
    #[tokio::test]
    async fn test_volume_spike_detection_with_alias_reuse() {
        // Setup context
        let mut context = StreamingSqlContext::new();
        let trades_handle = StreamHandle::new(
            "trades_stream".to_string(),
            "trades_topic".to_string(),
            "trades_schema".to_string(),
        );
        context
            .register_stream("trades".to_string(), trades_handle, create_trades_schema())
            .unwrap();

        // Setup execution engine
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Query: Calculate spike ratio and use it in another computation
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse(
                r#"
            SELECT
                trade_id,
                symbol,
                volume / avg_volume AS spike_ratio,
                spike_ratio * 100 AS spike_percentage
            FROM trades
            "#,
            )
            .unwrap();

        // Test case 1: Extreme spike (volume = 10x average)
        let record1 = create_trade_record(1, "AAPL", 10000.0, 150.0, 1000.0, 150.0);
        let result1 = engine.execute_with_record(&query, &record1).await;
        assert!(
            result1.is_ok(),
            "Query should execute for extreme spike case"
        );

        let output1 = rx.try_recv().unwrap();
        // Spike ratio = 10000.0 / 1000.0 = 10.0
        match output1.fields.get("spike_ratio") {
            Some(FieldValue::Float(ratio)) => {
                assert!(
                    (ratio - 10.0).abs() < 0.1,
                    "Expected spike_ratio ~10.0, got {}",
                    ratio
                );
            }
            other => panic!("Expected spike_ratio Float(10.0), got {:?}", other),
        }
        // Spike percentage = 10.0 * 100 = 1000.0
        match output1.fields.get("spike_percentage") {
            Some(FieldValue::Float(pct)) => {
                assert!(
                    (pct - 1000.0).abs() < 1.0,
                    "Expected spike_percentage ~1000.0, got {}",
                    pct
                );
            }
            other => panic!("Expected spike_percentage Float(1000.0), got {:?}", other),
        }

        // Test case 2: Major spike (volume = 7x average)
        let record2 = create_trade_record(2, "MSFT", 7000.0, 300.0, 1000.0, 300.0);
        let result2 = engine.execute_with_record(&query, &record2).await;
        assert!(result2.is_ok());

        let output2 = rx.try_recv().unwrap();
        // Spike ratio = 7000.0 / 1000.0 = 7.0
        match output2.fields.get("spike_ratio") {
            Some(FieldValue::Float(ratio)) => {
                assert!((ratio - 7.0).abs() < 0.1);
            }
            other => panic!("Expected spike_ratio Float(7.0), got {:?}", other),
        }
    }

    /// Test 2: Price Impact Analysis - Multi-Step Alias Chain
    /// Calculate price change percentage using chained aliases
    #[tokio::test]
    async fn test_price_impact_analysis_chain() {
        let mut context = StreamingSqlContext::new();
        let trades_handle = StreamHandle::new(
            "trades_stream".to_string(),
            "trades_topic".to_string(),
            "trades_schema".to_string(),
        );
        context
            .register_stream("trades".to_string(), trades_handle, create_trades_schema())
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        // Query: Calculate price change and percentage using alias chain
        let parser = StreamingSqlParser::new();
        let query = parser
            .parse(
                r#"
            SELECT
                symbol,
                price,
                avg_price,
                (price - avg_price) AS price_change,
                (price_change / avg_price) * 100 AS price_change_percent,
                price_change_percent / 2 AS price_change_half
            FROM trades
            "#,
            )
            .unwrap();

        // Price increased by 10%: price_change_percent should be 10.0
        let record1 = create_trade_record(1, "AAPL", 1000.0, 110.0, 1000.0, 100.0);
        let result1 = engine.execute_with_record(&query, &record1).await;
        assert!(result1.is_ok());

        let output1 = rx.try_recv().unwrap();
        // Verify alias chain computed correctly
        match output1.fields.get("price_change") {
            Some(FieldValue::Float(change)) => {
                assert!((change - 10.0).abs() < 0.1, "Price change should be 10.0");
            }
            other => panic!("Expected price_change Float(10.0), got {:?}", other),
        }
        match output1.fields.get("price_change_percent") {
            Some(FieldValue::Float(pct)) => {
                assert!(
                    (pct - 10.0).abs() < 0.1,
                    "Price change percent should be ~10%"
                );
            }
            other => panic!("Expected price_change_percent Float(10.0), got {:?}", other),
        }
        // Verify chained alias computed correctly
        match output1.fields.get("price_change_half") {
            Some(FieldValue::Float(half)) => {
                assert!((half - 5.0).abs() < 0.1, "Price change half should be ~5.0");
            }
            other => panic!("Expected price_change_half Float(5.0), got {:?}", other),
        }
    }

    /// Test 3: Circuit Breaker Metrics - Multiple Derived Values
    /// Use multiple aliases to compute circuit breaker metrics
    #[tokio::test]
    async fn test_circuit_breaker_logic() {
        let mut context = StreamingSqlContext::new();
        let trades_handle = StreamHandle::new(
            "trades_stream".to_string(),
            "trades_topic".to_string(),
            "trades_schema".to_string(),
        );
        context
            .register_stream("trades".to_string(), trades_handle, create_trades_schema())
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse(
                r#"
            SELECT
                symbol,
                volume / avg_volume AS volume_ratio,
                (price - avg_price) / avg_price * 100 AS price_change_pct,
                volume_ratio + (price_change_pct / 100) AS combined_metric,
                combined_metric * 2 AS alert_score
            FROM trades
            "#,
            )
            .unwrap();

        // Extreme case: both volume spike AND price surge
        let record1 = create_trade_record(1, "AAPL", 8000.0, 180.0, 1000.0, 100.0);
        let result1 = engine.execute_with_record(&query, &record1).await;
        assert!(result1.is_ok());

        let output1 = rx.try_recv().unwrap();
        // volume_ratio = 8000/1000 = 8.0
        // price_change_pct = (180-100)/100 * 100 = 80.0
        // combined_metric = 8.0 + 0.8 = 8.8
        // alert_score = 8.8 * 2 = 17.6
        match output1.fields.get("alert_score") {
            Some(FieldValue::Float(score)) => {
                assert!(*score > 15.0, "Expected alert_score > 15.0, got {}", score);
            }
            other => panic!("Expected alert_score to be Float, got {:?}", other),
        }

        // Moderate case: high volume only
        let record2 = create_trade_record(2, "MSFT", 4000.0, 310.0, 1000.0, 300.0);
        let result2 = engine.execute_with_record(&query, &record2).await;
        assert!(result2.is_ok());

        let output2 = rx.try_recv().unwrap();
        // volume_ratio = 4000/1000 = 4.0
        // price_change_pct = (310-300)/300 * 100 = 3.33
        // combined_metric = 4.0 + 0.0333 = 4.0333
        // alert_score = 4.0333 * 2 = 8.0666
        match output2.fields.get("alert_score") {
            Some(FieldValue::Float(score)) => {
                assert!(
                    *score > 7.0 && *score < 10.0,
                    "Expected alert_score between 7 and 10, got {}",
                    score
                );
            }
            other => panic!("Expected alert_score to be Float, got {:?}", other),
        }

        // Normal case
        let record3 = create_trade_record(3, "GOOGL", 1200.0, 2820.0, 1000.0, 2800.0);
        let result3 = engine.execute_with_record(&query, &record3).await;
        assert!(result3.is_ok());

        let output3 = rx.try_recv().unwrap();
        // volume_ratio = 1200/1000 = 1.2
        // price_change_pct = (2820-2800)/2800 * 100 = 0.714
        // combined_metric = 1.2 + 0.00714 = 1.20714
        // alert_score = 1.20714 * 2 = 2.41428
        match output3.fields.get("alert_score") {
            Some(FieldValue::Float(score)) => {
                assert!(*score < 4.0, "Expected alert_score < 4.0, got {}", score);
            }
            other => panic!("Expected alert_score to be Float, got {:?}", other),
        }
    }

    /// Test 4: Market Anomaly Detection - Composite Scoring
    /// Comprehensive anomaly scoring using multiple aliases
    #[tokio::test]
    async fn test_market_anomaly_detection() {
        let mut context = StreamingSqlContext::new();
        let trades_handle = StreamHandle::new(
            "trades_stream".to_string(),
            "trades_topic".to_string(),
            "trades_schema".to_string(),
        );
        context
            .register_stream("trades".to_string(), trades_handle, create_trades_schema())
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse(
                r#"
            SELECT
                symbol,
                volume / avg_volume AS volume_anomaly,
                (price - avg_price) / avg_price AS price_anomaly,
                volume_anomaly + price_anomaly AS anomaly_score,
                anomaly_score * 10 AS risk_level
            FROM trades
            "#,
            )
            .unwrap();

        // Critical: High volume + large price move
        let record1 = create_trade_record(1, "AAPL", 12000.0, 250.0, 1000.0, 100.0);
        let result1 = engine.execute_with_record(&query, &record1).await;
        assert!(result1.is_ok());

        let output1 = rx.try_recv().unwrap();
        // volume_anomaly = 12000/1000 = 12.0
        // price_anomaly = (250-100)/100 = 1.5
        // anomaly_score = 12.0 + 1.5 = 13.5
        // risk_level = 13.5 * 10 = 135.0
        match output1.fields.get("risk_level") {
            Some(FieldValue::Float(risk)) => {
                assert!(*risk > 130.0, "Expected risk_level > 130.0, got {}", risk);
            }
            other => panic!("Expected risk_level Float, got {:?}", other),
        }

        // Major: Significant volume spike
        let record2 = create_trade_record(2, "MSFT", 6000.0, 315.0, 1000.0, 300.0);
        let result2 = engine.execute_with_record(&query, &record2).await;
        assert!(result2.is_ok());

        let output2 = rx.try_recv().unwrap();
        // volume_anomaly = 6000/1000 = 6.0
        // price_anomaly = (315-300)/300 = 0.05
        // anomaly_score = 6.0 + 0.05 = 6.05
        // risk_level = 6.05 * 10 = 60.5
        match output2.fields.get("risk_level") {
            Some(FieldValue::Float(risk)) => {
                assert!(
                    *risk > 50.0 && *risk < 70.0,
                    "Expected risk_level between 50 and 70, got {}",
                    risk
                );
            }
            other => panic!("Expected risk_level Float, got {:?}", other),
        }
    }

    /// Test 5: Trade Profitability Analysis
    /// Calculate profit margins using alias chains
    #[tokio::test]
    async fn test_trade_profitability_analysis() {
        let mut context = StreamingSqlContext::new();
        let trades_handle = StreamHandle::new(
            "trades_stream".to_string(),
            "trades_topic".to_string(),
            "trades_schema".to_string(),
        );
        context
            .register_stream("trades".to_string(), trades_handle, create_trades_schema())
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse(
                r#"
            SELECT
                trade_id,
                symbol,
                price - avg_price AS absolute_gain,
                (absolute_gain / avg_price) * 100 AS gain_percentage,
                gain_percentage * 2 AS roi_doubled
            FROM trades
            "#,
            )
            .unwrap();

        // Highly profitable trade
        let record1 = create_trade_record(1, "AAPL", 1000.0, 130.0, 1000.0, 100.0);
        let result1 = engine.execute_with_record(&query, &record1).await;
        assert!(result1.is_ok());

        let output1 = rx.try_recv().unwrap();
        // absolute_gain = 130.0 - 100.0 = 30.0
        // gain_percentage = (30.0 / 100.0) * 100 = 30.0
        // roi_doubled = 30.0 * 2 = 60.0
        match output1.fields.get("roi_doubled") {
            Some(FieldValue::Float(roi)) => {
                assert!(
                    (roi - 60.0).abs() < 1.0,
                    "Expected roi_doubled ~60.0, got {}",
                    roi
                );
            }
            other => panic!("Expected roi_doubled Float, got {:?}", other),
        }
    }

    /// Test 6: Efficient Field Projection
    /// Verify that computed aliases don't include unnecessary fields in output
    #[tokio::test]
    async fn test_field_projection_with_aliases() {
        let mut context = StreamingSqlContext::new();
        let trades_handle = StreamHandle::new(
            "trades_stream".to_string(),
            "trades_topic".to_string(),
            "trades_schema".to_string(),
        );
        context
            .register_stream("trades".to_string(), trades_handle, create_trades_schema())
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse(
                r#"
            SELECT
                symbol,
                volume / avg_volume AS ratio,
                ratio * 2 AS doubled_ratio
            FROM trades
            "#,
            )
            .unwrap();

        let record = create_trade_record(1, "AAPL", 2000.0, 150.0, 1000.0, 150.0);
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok());

        let output = rx.try_recv().unwrap();

        // Should have exactly 3 fields: symbol, ratio, doubled_ratio
        assert_eq!(
            output.fields.len(),
            3,
            "Should have exactly 3 fields in output"
        );
        assert!(output.fields.contains_key("symbol"));
        assert!(output.fields.contains_key("ratio"));
        assert!(output.fields.contains_key("doubled_ratio"));

        // Should not have these fields
        assert!(!output.fields.contains_key("volume"));
        assert!(!output.fields.contains_key("avg_volume"));
        assert!(!output.fields.contains_key("price"));
    }

    /// Test 7: Multiple Records in Sequence
    /// Verify alias reuse works correctly across multiple sequential records
    #[tokio::test]
    async fn test_sequential_records_with_alias_reuse() {
        let mut context = StreamingSqlContext::new();
        let trades_handle = StreamHandle::new(
            "trades_stream".to_string(),
            "trades_topic".to_string(),
            "trades_schema".to_string(),
        );
        context
            .register_stream("trades".to_string(), trades_handle, create_trades_schema())
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse(
                r#"
            SELECT
                trade_id,
                symbol,
                volume / avg_volume AS spike_ratio,
                spike_ratio * 10 AS spike_score
            FROM trades
            "#,
            )
            .unwrap();

        // Send multiple records with varying spike ratios
        let records = vec![
            create_trade_record(1, "AAPL", 5000.0, 150.0, 1000.0, 150.0), // ratio=5, score=50
            create_trade_record(2, "MSFT", 2000.0, 300.0, 1000.0, 300.0), // ratio=2, score=20
            create_trade_record(3, "GOOGL", 4500.0, 2800.0, 1000.0, 2800.0), // ratio=4.5, score=45
        ];

        for record in records {
            let result = engine.execute_with_record(&query, &record).await;
            assert!(result.is_ok());
        }

        // Verify outputs
        let output1 = rx.try_recv().unwrap();
        match output1.fields.get("spike_score") {
            Some(FieldValue::Float(score)) => {
                assert!(
                    (score - 50.0).abs() < 1.0,
                    "Expected score ~50.0, got {}",
                    score
                );
            }
            other => panic!("Expected spike_score Float, got {:?}", other),
        }

        let output2 = rx.try_recv().unwrap();
        match output2.fields.get("spike_score") {
            Some(FieldValue::Float(score)) => {
                assert!(
                    (score - 20.0).abs() < 1.0,
                    "Expected score ~20.0, got {}",
                    score
                );
            }
            other => panic!("Expected spike_score Float, got {:?}", other),
        }

        let output3 = rx.try_recv().unwrap();
        match output3.fields.get("spike_score") {
            Some(FieldValue::Float(score)) => {
                assert!(
                    (score - 45.0).abs() < 1.0,
                    "Expected score ~45.0, got {}",
                    score
                );
            }
            other => panic!("Expected spike_score Float, got {:?}", other),
        }
    }

    /// Test 8: Complex Real-World Trading Scenario
    /// Comprehensive test combining all alias reuse patterns
    #[tokio::test]
    async fn test_comprehensive_trading_scenario() {
        let mut context = StreamingSqlContext::new();
        let trades_handle = StreamHandle::new(
            "trades_stream".to_string(),
            "trades_topic".to_string(),
            "trades_schema".to_string(),
        );
        context
            .register_stream("trades".to_string(), trades_handle, create_trades_schema())
            .unwrap();

        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut engine = StreamExecutionEngine::new(tx);

        let parser = StreamingSqlParser::new();
        let query = parser
            .parse(
                r#"
            SELECT
                trade_id,
                symbol,
                volume / avg_volume AS volume_ratio,
                (price - avg_price) / avg_price * 100 AS price_change_pct,
                volume_ratio + (price_change_pct / 100) AS combined_impact,
                combined_impact * 10 AS risk_score,
                risk_score / 2 AS normalized_risk
            FROM trades
            "#,
            )
            .unwrap();

        // Critical trading scenario
        let record = create_trade_record(1, "AAPL", 9000.0, 250.0, 1000.0, 100.0);
        let result = engine.execute_with_record(&query, &record).await;
        assert!(result.is_ok(), "Query should execute successfully");

        let output = rx.try_recv().unwrap();

        // Verify all computed fields exist
        assert!(output.fields.contains_key("volume_ratio"));
        assert!(output.fields.contains_key("price_change_pct"));
        assert!(output.fields.contains_key("combined_impact"));
        assert!(output.fields.contains_key("risk_score"));
        assert!(output.fields.contains_key("normalized_risk"));

        // volume_ratio = 9000 / 1000 = 9.0
        // price_change_pct = (250 - 100) / 100 * 100 = 150.0
        // combined_impact = 9.0 + 1.5 = 10.5
        // risk_score = 10.5 * 10 = 105.0
        // normalized_risk = 105.0 / 2 = 52.5

        // Verify risk_score is very high
        match output.fields.get("risk_score") {
            Some(FieldValue::Float(score)) => {
                assert!(*score > 100.0, "Expected risk_score > 100.0, got {}", score);
            }
            other => panic!("Expected risk_score Float, got {:?}", other),
        }

        // Verify normalized_risk
        match output.fields.get("normalized_risk") {
            Some(FieldValue::Float(norm_risk)) => {
                assert!(
                    *norm_risk > 50.0,
                    "Expected normalized_risk > 50.0, got {}",
                    norm_risk
                );
            }
            other => panic!("Expected normalized_risk Float, got {:?}", other),
        }
    }
}
