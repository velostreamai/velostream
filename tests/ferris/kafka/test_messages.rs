use serde::{Serialize, Deserialize};
use chrono::Utc;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TestMessage {
    pub id: u32,
    pub content: String,
    pub timestamp: Option<String>,
}

impl TestMessage {
    pub fn new(id: u32, content: &str) -> Self {
        Self {
            id,
            content: content.to_string(),
            timestamp: Some(Utc::now().to_rfc3339()),
        }
    }

    pub fn basic(id: u32, content: &str) -> Self {
        Self {
            id,
            content: content.to_string(),
            timestamp: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct User {
    pub id: u64,
    pub name: String,
    pub email: Option<String>,
}

impl User {
    pub fn new(id: u64, name: &str, email: Option<&str>) -> Self {
        Self {
            id,
            name: name.to_string(),
            email: email.map(|e| e.to_string()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Product {
    pub id: String,
    pub name: String,
    pub price: f64,
    pub available: bool,
}

impl Product {
    pub fn new(id: &str, name: &str, price: f64, available: bool) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            price,
            available,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Hash, Eq)]
pub enum OrderStatus {
    Created,
    Paid,
    Shipped,
    Delivered,
    Cancelled,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct OrderEvent {
    pub order_id: String,
    pub customer_id: String,
    pub amount: f64,
    pub status: OrderStatus,
    pub timestamp: i64,
}

impl OrderEvent {
    pub fn new(order_id: &str, customer_id: &str, amount: f64, status: OrderStatus) -> Self {
        Self {
            order_id: order_id.to_string(),
            customer_id: customer_id.to_string(),
            amount,
            status,
            timestamp: Utc::now().timestamp(),
        }
    }
}

// Additional test message types for serialization testing
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TestUser {
    pub id: u64,
    pub name: String,
    pub email: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct InnerStruct {
    pub value: i32,
    pub flag: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct NestedStruct {
    pub outer: String,
    pub inner: InnerStruct,
    pub numbers: Vec<i32>,
}