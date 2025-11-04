//! Stdout DataWriter implementation for debugging and console output

use crate::velostream::datasource::DataWriter;
use crate::velostream::serialization::helpers::field_value_to_json;
use crate::velostream::sql::execution::types::StreamRecord;
use async_trait::async_trait;
use serde_json::Value;
use std::error::Error;

/// A simple DataWriter that outputs records to stdout for debugging
pub struct StdoutWriter {
    format: OutputFormat,
    records_written: u64,
    include_metadata: bool,
}

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Json,
    JsonPretty,
    Table,
}

impl StdoutWriter {
    /// Create a new stdout writer with JSON format
    pub fn new() -> Self {
        Self {
            format: OutputFormat::Json,
            records_written: 0,
            include_metadata: false,
        }
    }

    /// Create with pretty-printed JSON format
    pub fn new_pretty() -> Self {
        Self {
            format: OutputFormat::JsonPretty,
            records_written: 0,
            include_metadata: true,
        }
    }

    /// Create with table format (for debugging)
    pub fn new_table() -> Self {
        Self {
            format: OutputFormat::Table,
            records_written: 0,
            include_metadata: true,
        }
    }

    /// Convert StreamRecord to JSON value
    fn record_to_json(&self, record: &StreamRecord) -> Result<Value, Box<dyn Error + Send + Sync>> {
        let mut json_obj = serde_json::Map::new();

        // Convert all fields to JSON
        for (field_name, field_value) in &record.fields {
            let json_value = field_value_to_json(field_value)
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
            json_obj.insert(field_name.clone(), json_value);
        }

        // Add metadata if requested
        if self.include_metadata {
            json_obj.insert(
                "_timestamp".to_string(),
                Value::Number(serde_json::Number::from(record.timestamp)),
            );
            json_obj.insert(
                "_offset".to_string(),
                Value::Number(serde_json::Number::from(record.offset)),
            );
            json_obj.insert(
                "_partition".to_string(),
                Value::Number(serde_json::Number::from(record.partition)),
            );
        }

        Ok(Value::Object(json_obj))
    }

    /// Format record as table row (simple debug format)
    fn record_to_table(&self, record: &StreamRecord) -> String {
        let mut parts = Vec::new();

        if self.include_metadata {
            parts.push(format!("ts={}", record.timestamp));
            parts.push(format!("off={}", record.offset));
            parts.push(format!("part={}", record.partition));
        }

        for (key, value) in &record.fields {
            parts.push(format!("{}={:?}", key, value));
        }

        format!("[{}]", parts.join(", "))
    }
}

impl Default for StdoutWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataWriter for StdoutWriter {
    async fn write(&mut self, record: StreamRecord) -> Result<(), Box<dyn Error + Send + Sync>> {
        let output = match self.format {
            OutputFormat::Json => {
                let json_value = self.record_to_json(&record)?;
                serde_json::to_string(&json_value)?
            }
            OutputFormat::JsonPretty => {
                let json_value = self.record_to_json(&record)?;
                serde_json::to_string_pretty(&json_value)?
            }
            OutputFormat::Table => self.record_to_table(&record),
        };

        println!("STDOUT[{}]: {}", self.records_written, output);
        self.records_written += 1;

        Ok(())
    }

    async fn write_batch(
        &mut self,
        records: Vec<std::sync::Arc<StreamRecord>>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("STDOUT_BATCH: Writing {} records to stdout", records.len());

        for record_arc in records {
            // Dereference Arc and clone for write() which takes ownership
            self.write((*record_arc).clone()).await?;
        }

        Ok(())
    }

    async fn update(
        &mut self,
        key: &str,
        record: StreamRecord,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!(
            "STDOUT_UPDATE[{}]: {}",
            key,
            serde_json::to_string(&self.record_to_json(&record)?)?
        );
        Ok(())
    }

    async fn delete(&mut self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("STDOUT_DELETE: {}", key);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("STDOUT_FLUSH: Flushed {} records", self.records_written);
        use std::io::{self, Write};
        io::stdout()
            .flush()
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    async fn commit(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("STDOUT_COMMIT: Committed {} records", self.records_written);
        self.flush().await
    }

    async fn rollback(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("STDOUT_ROLLBACK: Rolling back");
        Ok(())
    }

    async fn begin_transaction(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        println!("STDOUT_BEGIN_TX: Starting transaction");
        Ok(false) // Stdout doesn't support real transactions
    }

    async fn commit_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("STDOUT_COMMIT_TX: Committing transaction");
        Ok(())
    }

    async fn abort_transaction(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("STDOUT_ABORT_TX: Aborting transaction");
        Ok(())
    }

    fn supports_transactions(&self) -> bool {
        false
    }
}
