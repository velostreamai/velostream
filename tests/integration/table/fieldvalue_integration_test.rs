/*!
# FieldValue Integration Tests

Comprehensive integration tests for KTable SQL functionality with actual FieldValue data.
Tests the complete pipeline from JSON → FieldValue → SQL operations.
*/

use std::collections::HashMap;
use serde_json::{json, Value as JsonValue};
use velostream::velostream::sql::execution::types::FieldValue;
use velostream::velostream::table::sql::{SqlDataSource, SqlQueryable};
use velostream::velostream::sql::error::SqlError;

/// Integration test data source with realistic financial data
struct FinancialDataSource {
    records: HashMap<String, FieldValue>,
}

impl FinancialDataSource {
    fn new() -> Self {
        let mut records = HashMap::new();

        // User portfolio data with complex nested structures
        let portfolio_data = json!({
            "user_id": "user_001",
            "account_balance": 125000.50,
            "currency": "USD",
            "tier": "premium",
            "active": true,
            "created_at": "2024-01-15T10:30:00Z",
            "positions": {
                "AAPL": {
                    "shares": 150,
                    "avg_price": 185.25,
                    "market_value": 27787.50,
                    "sector": "Technology"
                },
                "MSFT": {
                    "shares": 75,
                    "avg_price": 380.40,
                    "market_value": 28530.00,
                    "sector": "Technology"
                },
                "TSLA": {
                    "shares": 25,
                    "avg_price": 220.80,
                    "market_value": 5520.00,
                    "sector": "Automotive"
                }
            },
            "risk_metrics": {
                "value_at_risk": 12500.75,
                "sharpe_ratio": 1.45,
                "beta": 1.12,
                "volatility": 0.18
            },
            "transaction_history": [
                {
                    "id": "txn_001",
                    "type": "BUY",
                    "symbol": "AAPL",
                    "quantity": 50,
                    "price": 180.25,
                    "timestamp": "2024-01-10T09:30:00Z"
                },
                {
                    "id": "txn_002",
                    "type": "BUY",
                    "symbol": "MSFT",
                    "quantity": 25,
                    "price": 375.60,
                    "timestamp": "2024-01-12T14:15:00Z"
                },
                {
                    "id": "txn_003",
                    "type": "SELL",
                    "symbol": "AAPL",
                    "quantity": 10,
                    "price": 190.50,
                    "timestamp": "2024-01-14T11:45:00Z"
                }
            ]
        });

        let portfolio_field = Self::json_to_field_value(&portfolio_data);
        records.insert("portfolio-001".to_string(), portfolio_field);

        // Market data with real-time pricing
        let market_data = json!({
            "symbol": "AAPL",
            "current_price": 187.45,
            "day_change": 2.20,
            "day_change_percent": 1.19,
            "volume": 52874561,
            "market_cap": 2847532000000i64,
            "pe_ratio": 29.87,
            "dividend_yield": 0.52,
            "sector": "Technology",
            "exchange": "NASDAQ",
            "last_updated": "2024-01-15T16:00:00Z",
            "technical_indicators": {
                "rsi": 67.8,
                "macd": 1.25,
                "bollinger_upper": 192.50,
                "bollinger_lower": 180.75,
                "sma_20": 185.33,
                "sma_50": 182.87,
                "sma_200": 175.42
            }
        });

        let market_field = Self::json_to_field_value(&market_data);
        records.insert("market-AAPL".to_string(), market_field);

        // Risk configuration data
        let risk_config = json!({
            "risk_limits": {
                "max_position_size": 100000.00,
                "max_sector_exposure": 0.40,
                "max_single_stock": 0.15,
                "stop_loss_threshold": 0.10
            },
            "compliance": {
                "margin_requirement": 0.25,
                "day_trading_limit": 4,
                "pattern_day_trader": false,
                "accredited_investor": true
            },
            "alerts": [
                {
                    "type": "POSITION_LIMIT",
                    "threshold": 95000.00,
                    "enabled": true
                },
                {
                    "type": "SECTOR_CONCENTRATION",
                    "threshold": 0.35,
                    "enabled": true
                }
            ]
        });

        let risk_field = Self::json_to_field_value(&risk_config);
        records.insert("risk-config-001".to_string(), risk_field);

        Self { records }
    }

    /// Convert JSON to FieldValue with comprehensive type support
    fn json_to_field_value(value: &JsonValue) -> FieldValue {
        match value {
            JsonValue::Null => FieldValue::Null,
            JsonValue::Bool(b) => FieldValue::Boolean(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    FieldValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    FieldValue::Float(f)
                } else {
                    FieldValue::Null
                }
            }
            JsonValue::String(s) => {
                // Try to parse as timestamp
                if s.ends_with('Z') && s.contains('T') {
                    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                        return FieldValue::Timestamp(dt.naive_utc());
                    }
                }
                FieldValue::String(s.clone())
            }
            JsonValue::Array(arr) => {
                let field_values: Vec<FieldValue> = arr.iter()
                    .map(|v| Self::json_to_field_value(v))
                    .collect();
                FieldValue::Array(field_values)
            }
            JsonValue::Object(obj) => {
                let mut field_map = HashMap::new();
                for (key, val) in obj {
                    field_map.insert(key.clone(), Self::json_to_field_value(val));
                }
                FieldValue::Struct(field_map)
            }
        }
    }

    /// Helper method to get field by path - similar to the real KTable functionality
    fn get_field_by_path(&self, record_key: &str, field_path: &str) -> Option<FieldValue> {
        let record = self.records.get(record_key)?;
        Self::extract_field_from_path(record, field_path)
    }

    /// Extract field from FieldValue using dot notation path
    fn extract_field_from_path(field_value: &FieldValue, path: &str) -> Option<FieldValue> {
        if !path.contains('.') && !path.contains('[') {
            // Simple field access
            if let FieldValue::Struct(map) = field_value {
                return map.get(path).cloned();
            }
            return None;
        }

        // Parse path with support for both dot notation and array access
        let parts = Self::parse_path_parts(path);
        let mut current = field_value;

        for part in parts {
            match part.as_str() {
                part if part.starts_with('[') && part.ends_with(']') => {
                    // Array index access
                    let index_str = &part[1..part.len()-1];
                    if let Ok(index) = index_str.parse::<usize>() {
                        if let FieldValue::Array(arr) = current {
                            current = arr.get(index)?;
                        } else {
                            return None;
                        }
                    } else {
                        return None;
                    }
                }
                _ => {
                    // Struct field access
                    if let FieldValue::Struct(map) = current {
                        current = map.get(&part)?;
                    } else {
                        return None;
                    }
                }
            }
        }

        Some(current.clone())
    }

    /// Parse path into parts, handling both dot notation and array indices
    fn parse_path_parts(path: &str) -> Vec<String> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut chars = path.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '.' => {
                    if !current.is_empty() {
                        parts.push(current.clone());
                        current.clear();
                    }
                }
                '[' => {
                    if !current.is_empty() {
                        parts.push(current.clone());
                        current.clear();
                    }
                    // Read the array index part
                    current.push(ch);
                    while let Some(next_ch) = chars.next() {
                        current.push(next_ch);
                        if next_ch == ']' {
                            break;
                        }
                    }
                    parts.push(current.clone());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }

        parts
    }
}

impl SqlDataSource for FinancialDataSource {
    fn get_all_records(&self) -> Result<HashMap<String, FieldValue>, SqlError> {
        Ok(self.records.clone())
    }

    fn get_record(&self, key: &str) -> Result<Option<FieldValue>, SqlError> {
        Ok(self.records.get(key).cloned())
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }
}

// We don't implement SqlQueryable since our test is checking individual trait methods
// The production implementation will have SqlQueryable implemented for KTable types

#[test]
fn test_fieldvalue_json_conversion() {
    let source = FinancialDataSource::new();
    let records = source.get_all_records().unwrap();

    // Test that we have all expected records
    assert_eq!(records.len(), 3);
    assert!(records.contains_key("portfolio-001"));
    assert!(records.contains_key("market-AAPL"));
    assert!(records.contains_key("risk-config-001"));

    // Test portfolio structure conversion
    let portfolio = &records["portfolio-001"];
    if let FieldValue::Struct(portfolio_map) = portfolio {
        assert!(portfolio_map.contains_key("user_id"));
        assert!(portfolio_map.contains_key("account_balance"));
        assert!(portfolio_map.contains_key("positions"));
        assert!(portfolio_map.contains_key("transaction_history"));

        // Test nested positions structure
        if let Some(FieldValue::Struct(positions)) = portfolio_map.get("positions") {
            assert!(positions.contains_key("AAPL"));
            assert!(positions.contains_key("MSFT"));
            assert!(positions.contains_key("TSLA"));
        } else {
            panic!("Positions should be a nested structure");
        }

        // Test array conversion for transaction history
        if let Some(FieldValue::Array(transactions)) = portfolio_map.get("transaction_history") {
            assert_eq!(transactions.len(), 3);
            if let FieldValue::Struct(first_txn) = &transactions[0] {
                assert!(first_txn.contains_key("id"));
                assert!(first_txn.contains_key("type"));
                assert!(first_txn.contains_key("symbol"));
            }
        } else {
            panic!("Transaction history should be an array");
        }
    } else {
        panic!("Portfolio should be a struct");
    }
}

#[test]
fn test_sql_operations_with_fieldvalue_data() {
    let source = FinancialDataSource::new();

    // Test basic record retrieval
    let portfolio_record = source.get_record("portfolio-001").unwrap().unwrap();
    if let FieldValue::Struct(portfolio_map) = portfolio_record {
        assert_eq!(portfolio_map.get("user_id").unwrap(), &FieldValue::String("user_001".to_string()));
        assert_eq!(portfolio_map.get("account_balance").unwrap(), &FieldValue::Float(125000.50));
        assert_eq!(portfolio_map.get("active").unwrap(), &FieldValue::Boolean(true));
    } else {
        panic!("Expected portfolio record to be a struct");
    }

    // Test nested field access using our helper method
    let aapl_shares = source.get_field_by_path("portfolio-001", "positions.AAPL.shares").unwrap();
    assert_eq!(aapl_shares, FieldValue::Integer(150));

    let risk_var = source.get_field_by_path("portfolio-001", "risk_metrics.value_at_risk").unwrap();
    assert_eq!(risk_var, FieldValue::Float(12500.75));

    // Test market data access
    let current_price = source.get_field_by_path("market-AAPL", "current_price").unwrap();
    assert_eq!(current_price, FieldValue::Float(187.45));
}

#[test]
fn test_sql_filtering_with_fieldvalue_data() {
    let source = FinancialDataSource::new();

    // Test string filtering
    let premium_users = source.sql_filter("tier = 'premium'").unwrap();
    assert_eq!(premium_users.len(), 1);

    // Test numeric filtering
    let large_balances = source.sql_filter("account_balance > 100000.0").unwrap();
    assert_eq!(large_balances.len(), 1);

    // Test boolean filtering
    let active_accounts = source.sql_filter("active = true").unwrap();
    assert_eq!(active_accounts.len(), 1);

    // Test no matches
    let inactive_accounts = source.sql_filter("active = false").unwrap();
    assert_eq!(inactive_accounts.len(), 0);
}

#[test]
fn test_sql_exists_with_fieldvalue_data() {
    let source = FinancialDataSource::new();

    // Test exists with various conditions
    assert!(source.sql_exists("tier = 'premium'").unwrap());
    assert!(source.sql_exists("account_balance > 50000.0").unwrap());
    assert!(source.sql_exists("active = true").unwrap());

    // Test non-existent conditions
    assert!(!source.sql_exists("tier = 'basic'").unwrap());
    assert!(!source.sql_exists("account_balance > 200000.0").unwrap());
    assert!(!source.sql_exists("active = false").unwrap());
}

#[test]
fn test_sql_column_values_with_fieldvalue_data() {
    let source = FinancialDataSource::new();

    // Test column extraction
    let user_ids = source.sql_column_values("user_id", "active = true").unwrap();
    assert_eq!(user_ids.len(), 1);
    assert_eq!(user_ids[0], FieldValue::String("user_001".to_string()));

    let tiers = source.sql_column_values("tier", "account_balance > 0.0").unwrap();
    assert_eq!(tiers.len(), 1);
    assert_eq!(tiers[0], FieldValue::String("premium".to_string()));

    // Test column extraction with no matches
    let empty_results = source.sql_column_values("user_id", "tier = 'basic'").unwrap();
    assert_eq!(empty_results.len(), 0);
}

#[test]
fn test_sql_scalar_with_fieldvalue_data() {
    let source = FinancialDataSource::new();

    // Test basic field extraction using sql_scalar with WHERE clause
    let user_id = source.sql_scalar("user_id", "user_id = 'user_001'").unwrap();
    assert_eq!(user_id, FieldValue::String("user_001".to_string()));

    let balance = source.sql_scalar("account_balance", "user_id = 'user_001'").unwrap();
    assert_eq!(balance, FieldValue::Float(125000.50));

    // Test boolean field
    let active = source.sql_scalar("active", "user_id = 'user_001'").unwrap();
    assert_eq!(active, FieldValue::Boolean(true));
}

#[test]
fn test_wildcard_queries_with_fieldvalue_data() {
    let source = FinancialDataSource::new();

    // Test wildcard field access
    let position_shares = source.sql_wildcard_values("positions.*.shares").unwrap();
    assert_eq!(position_shares.len(), 3); // AAPL, MSFT, TSLA
    assert!(position_shares.contains(&FieldValue::Integer(150))); // AAPL
    assert!(position_shares.contains(&FieldValue::Integer(75)));  // MSFT
    assert!(position_shares.contains(&FieldValue::Integer(25)));  // TSLA

    // Test wildcard with conditions
    let large_positions = source.sql_wildcard_values("positions.*.shares > 50").unwrap();
    assert_eq!(large_positions.len(), 2); // AAPL and MSFT

    // Test array wildcard access
    let transaction_types = source.sql_wildcard_values("transaction_history[*].type").unwrap();
    assert_eq!(transaction_types.len(), 3);
    assert!(transaction_types.contains(&FieldValue::String("BUY".to_string())));
    assert!(transaction_types.contains(&FieldValue::String("SELL".to_string())));
}

#[test]
fn test_aggregate_functions_with_fieldvalue_data() {
    let source = FinancialDataSource::new();

    // Test COUNT aggregate
    let position_count = source.sql_wildcard_aggregate("COUNT(positions.*)").unwrap();
    assert_eq!(position_count, FieldValue::Integer(3));

    // Test SUM aggregate
    let total_shares = source.sql_wildcard_aggregate("SUM(positions.*.shares)").unwrap();
    assert_eq!(total_shares, FieldValue::Float(250.0)); // 150 + 75 + 25

    // Test MAX aggregate
    let max_shares = source.sql_wildcard_aggregate("MAX(positions.*.shares)").unwrap();
    assert_eq!(max_shares, FieldValue::Float(150.0)); // AAPL

    // Test MIN aggregate
    let min_shares = source.sql_wildcard_aggregate("MIN(positions.*.shares)").unwrap();
    assert_eq!(min_shares, FieldValue::Float(25.0)); // TSLA

    // Test AVG aggregate
    let avg_shares = source.sql_wildcard_aggregate("AVG(positions.*.shares)").unwrap();
    if let FieldValue::Float(avg_val) = avg_shares {
        assert!((avg_val - 83.333).abs() < 0.01); // (150 + 75 + 25) / 3
    } else {
        panic!("AVG should return a Float");
    }
}

#[test]
fn test_complex_nested_access_with_fieldvalue_data() {
    let source = FinancialDataSource::new();

    // Test deep nested field access
    let aapl_sector = source.get_field_by_path("portfolio-001", "positions.AAPL.sector").unwrap();
    assert_eq!(aapl_sector, FieldValue::String("Technology".to_string()));

    let bollinger_upper = source.get_field_by_path("market-AAPL", "technical_indicators.bollinger_upper").unwrap();
    assert_eq!(bollinger_upper, FieldValue::Float(192.50));

    let max_position = source.get_field_by_path("risk-config-001", "risk_limits.max_position_size").unwrap();
    assert_eq!(max_position, FieldValue::Float(100000.00));

    // Test array index access
    let first_alert_type = source.get_field_by_path("risk-config-001", "alerts[0].type").unwrap();
    assert_eq!(first_alert_type, FieldValue::String("POSITION_LIMIT".to_string()));

    let second_alert_threshold = source.get_field_by_path("risk-config-001", "alerts[1].threshold").unwrap();
    assert_eq!(second_alert_threshold, FieldValue::Float(0.35));
}

#[test]
fn test_timestamp_handling_with_fieldvalue_data() {
    let source = FinancialDataSource::new();

    // Test timestamp field conversion
    let created_at = source.get_field_by_path("portfolio-001", "created_at").unwrap();
    match created_at {
        FieldValue::Timestamp(_) => {
            // Success - timestamp was properly parsed
        }
        _ => panic!("created_at should be parsed as Timestamp, got: {:?}", created_at),
    }

    // Test timestamp in transaction history
    let txn_timestamp = source.get_field_by_path("portfolio-001", "transaction_history[0].timestamp").unwrap();
    match txn_timestamp {
        FieldValue::Timestamp(_) => {
            // Success - timestamp was properly parsed
        }
        _ => panic!("Transaction timestamp should be parsed as Timestamp, got: {:?}", txn_timestamp),
    }
}

#[test]
fn test_performance_with_complex_fieldvalue_data() {
    let source = FinancialDataSource::new();

    use std::time::Instant;

    // Test performance of SQL operations
    let start = Instant::now();
    for _ in 0..100 {
        let _ = source.sql_filter("account_balance > 50000.0");
    }
    let filter_duration = start.elapsed();

    let start = Instant::now();
    for _ in 0..100 {
        let _ = source.sql_exists("tier = 'premium'");
    }
    let exists_duration = start.elapsed();

    let start = Instant::now();
    for _ in 0..100 {
        let _ = source.sql_wildcard_values("positions.*.shares");
    }
    let wildcard_duration = start.elapsed();

    let start = Instant::now();
    for _ in 0..100 {
        let _ = source.sql_wildcard_aggregate("SUM(positions.*.shares)");
    }
    let aggregate_duration = start.elapsed();

    // Performance assertions (should be fast even with complex nested data)
    assert!(filter_duration.as_millis() < 100, "SQL filter should be fast");
    assert!(exists_duration.as_millis() < 50, "SQL exists should be fast");
    assert!(wildcard_duration.as_millis() < 100, "Wildcard queries should be fast");
    assert!(aggregate_duration.as_millis() < 100, "Aggregate functions should be fast");

    println!("Performance results for complex FieldValue SQL operations:");
    println!("  SQL filter: {:?}", filter_duration);
    println!("  SQL exists: {:?}", exists_duration);
    println!("  Wildcard queries: {:?}", wildcard_duration);
    println!("  Aggregate functions: {:?}", aggregate_duration);
}