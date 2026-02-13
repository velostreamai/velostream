//! Unified test data generation for performance testing
//!
//! This module provides consistent test data generation to replace
//! the scattered record generation logic across test files.

use chrono::Utc;
use std::collections::HashMap;
use velostream::velostream::sql::execution::types::{FieldValue, StreamRecord};

/// Configuration for test record generation
#[derive(Debug, Clone)]
pub struct TestRecordConfig {
    /// Number of records to generate
    pub count: usize,
    /// Include timestamp fields
    pub include_timestamps: bool,
    /// Include financial precision fields (ScaledInteger)
    pub include_financial: bool,
    /// Include string fields for text processing
    pub include_text: bool,
    /// Include nested/complex fields
    pub include_complex: bool,
}

impl Default for TestRecordConfig {
    fn default() -> Self {
        Self {
            count: 1000,
            include_timestamps: true,
            include_financial: true,
            include_text: true,
            include_complex: false,
        }
    }
}

impl TestRecordConfig {
    /// Create config for basic benchmarks
    pub fn basic(count: usize) -> Self {
        Self {
            count,
            include_timestamps: true,
            include_financial: false,
            include_text: false,
            include_complex: false,
        }
    }

    /// Create config for financial benchmarks
    pub fn financial(count: usize) -> Self {
        Self {
            count,
            include_timestamps: true,
            include_financial: true,
            include_text: false,
            include_complex: false,
        }
    }

    /// Create config for complex benchmarks
    pub fn complex(count: usize) -> Self {
        Self {
            count,
            include_timestamps: true,
            include_financial: true,
            include_text: true,
            include_complex: true,
        }
    }
}

/// Generate standardized test records for benchmarking
pub fn generate_test_records(config: &TestRecordConfig) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(config.count);

    for i in 0..config.count {
        let mut fields = HashMap::new();

        // Always include basic fields
        fields.insert("id".to_string(), FieldValue::Integer(i as i64));
        fields.insert("sequence".to_string(), FieldValue::Integer(i as i64));

        // Conditional timestamp fields
        if config.include_timestamps {
            let timestamp = Utc::now() - chrono::Duration::seconds(i as i64);
            fields.insert(
                "timestamp".to_string(),
                FieldValue::Timestamp(timestamp.naive_utc()),
            );
        }

        // Financial precision fields
        if config.include_financial {
            // Price with 4 decimal precision (e.g., $123.4567)
            let price_cents = 123450 + (i % 10000) as i64;
            fields.insert(
                "price".to_string(),
                FieldValue::ScaledInteger(price_cents, 4),
            );

            // Quantity with 2 decimal precision
            let quantity = 100 + (i % 500) as i64;
            fields.insert(
                "quantity".to_string(),
                FieldValue::ScaledInteger(quantity, 2),
            );

            // Amount calculated field
            let amount = price_cents * quantity / 100; // Adjust scaling
            fields.insert("amount".to_string(), FieldValue::ScaledInteger(amount, 4));
        }

        // Text processing fields
        if config.include_text {
            fields.insert(
                "category".to_string(),
                FieldValue::String(get_test_category(i)),
            );
            fields.insert(
                "description".to_string(),
                FieldValue::String(get_test_description(i)),
            );
        }

        // Complex/nested fields
        if config.include_complex {
            fields.insert(
                "metadata_json".to_string(),
                FieldValue::String(format!(
                    r#"{{"batch": {}, "source": "performance_test", "processed_at": "{}"}}"#,
                    i / 100,
                    Utc::now().to_rfc3339()
                )),
            );
        }

        records.push(StreamRecord::new(fields));
    }

    records
}

/// Generate web analytics test records (for web analytics scenarios)
pub fn generate_web_analytics_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);
    let pages = ["home", "products", "checkout", "profile", "help"];
    let events = ["page_view", "click", "scroll", "form_submit"];

    for i in 0..count {
        let mut fields = HashMap::new();

        fields.insert(
            "session_id".to_string(),
            FieldValue::String(format!("session_{}", i % 1000)),
        );
        fields.insert(
            "user_id".to_string(),
            FieldValue::Integer((i % 10000) as i64),
        );
        fields.insert(
            "event_type".to_string(),
            FieldValue::String(events[i % events.len()].to_string()),
        );
        fields.insert(
            "page".to_string(),
            FieldValue::String(pages[i % pages.len()].to_string()),
        );

        let timestamp = Utc::now() - chrono::Duration::milliseconds((count - i) as i64 * 100);
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Timestamp(timestamp.naive_utc()),
        );

        // Simulate late data (5% of events arrive late)
        if i % 20 == 0 {
            let late_timestamp = timestamp - chrono::Duration::minutes(5);
            fields.insert(
                "event_time".to_string(),
                FieldValue::Timestamp(late_timestamp.naive_utc()),
            );
        } else {
            fields.insert(
                "event_time".to_string(),
                FieldValue::Timestamp(timestamp.naive_utc()),
            );
        }

        records.push(StreamRecord::new(fields));
    }

    records
}

/// Generate financial transaction records (for fraud detection scenarios)
pub fn generate_financial_transaction_records(count: usize) -> Vec<StreamRecord> {
    let mut records = Vec::with_capacity(count);
    let merchants = ["Amazon", "Walmart", "Target", "Best Buy", "Starbucks"];
    let card_types = ["Visa", "MasterCard", "Amex"];

    for i in 0..count {
        let mut fields = HashMap::new();

        fields.insert(
            "transaction_id".to_string(),
            FieldValue::String(format!("txn_{}", i)),
        );
        fields.insert(
            "account_id".to_string(),
            FieldValue::Integer((i % 5000) as i64),
        );
        fields.insert(
            "merchant".to_string(),
            FieldValue::String(merchants[i % merchants.len()].to_string()),
        );
        fields.insert(
            "card_type".to_string(),
            FieldValue::String(card_types[i % card_types.len()].to_string()),
        );

        // Amount with financial precision
        let amount = 1000 + (i % 50000) as i64; // $10.00 to $500.00
        fields.insert("amount".to_string(), FieldValue::ScaledInteger(amount, 2));

        let timestamp = Utc::now() - chrono::Duration::seconds(i as i64);
        fields.insert(
            "timestamp".to_string(),
            FieldValue::Timestamp(timestamp.naive_utc()),
        );

        // Risk indicators
        fields.insert("is_high_risk".to_string(), FieldValue::Boolean(i % 100 < 5)); // 5% high risk
        fields.insert(
            "risk_score".to_string(),
            FieldValue::Integer((i % 100) as i64),
        );

        records.push(StreamRecord::new(fields));
    }

    records
}

fn get_test_category(index: usize) -> String {
    let categories = ["Electronics", "Clothing", "Books", "Home", "Sports"];
    categories[index % categories.len()].to_string()
}

fn get_test_description(index: usize) -> String {
    let descriptions = [
        "High-quality product with excellent features",
        "Premium item with advanced functionality",
        "Budget-friendly option with good value",
        "Professional-grade equipment for experts",
        "Popular choice among customers",
    ];
    descriptions[index % descriptions.len()].to_string()
}
