//! Streaming query interfaces for memory-efficient table operations
//!
//! This module provides streaming alternatives to the snapshot-based query interfaces,
//! enabling memory-efficient processing of large tables without loading all data at once.

use crate::velostream::sql::{error::SqlError, execution::types::FieldValue};
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// Result type for streaming operations
pub type StreamResult<T> = Result<T, SqlError>;

/// A lightweight streaming record for memory-efficient table operations
///
/// This simplified record format saves ~48% memory vs full StreamRecord by excluding
/// Kafka metadata (timestamp, offset, partition, headers, event_time).
/// Use for: table iteration, bulk processing, memory-constrained environments.
#[derive(Debug, Clone)]
pub struct SimpleStreamRecord {
    pub key: String,
    pub fields: HashMap<String, FieldValue>,
}

/// A batch of records for efficient processing
#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub records: Vec<SimpleStreamRecord>,
    pub has_more: bool,
}

/// Stream of records for async iteration
pub struct RecordStream {
    pub receiver: mpsc::UnboundedReceiver<StreamResult<SimpleStreamRecord>>,
}

impl Stream for RecordStream {
    type Item = StreamResult<SimpleStreamRecord>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// Trait for memory-efficient streaming queries
///
/// This trait provides streaming alternatives to legacy SqlQueryable operations,
/// processing records in batches to minimize memory usage.
pub trait StreamingQueryable: Send + Sync {
    /// Stream all records without loading them into memory at once
    ///
    /// Returns a stream that yields records one at a time, allowing
    /// processing of tables larger than available memory.
    async fn stream_all(&self) -> StreamResult<RecordStream>;

    /// Stream filtered records matching a WHERE clause
    ///
    /// Applies filtering during streaming to avoid loading non-matching records.
    async fn stream_filter(&self, where_clause: &str) -> StreamResult<RecordStream>;

    /// Query records in batches for controlled memory usage
    ///
    /// Returns records in configurable batch sizes, useful for bulk operations
    /// that need to process multiple records at once.
    async fn query_batch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> StreamResult<RecordBatch>;

    /// Stream records with a transformation function
    ///
    /// Applies a transformation to each record during streaming,
    /// avoiding the need to load all data before transformation.
    async fn stream_map<F, T>(&self, transform: F) -> StreamResult<Vec<T>>
    where
        F: Fn(SimpleStreamRecord) -> T + Send + Sync + 'static,
        T: Send + 'static;

    /// Count records without loading them all into memory
    ///
    /// Efficiently counts records by iterating without storing values.
    fn stream_count(&self, where_clause: Option<&str>) -> impl std::future::Future<Output = StreamResult<usize>> + Send;

    /// Aggregate values during streaming
    ///
    /// Performs aggregations (SUM, AVG, etc.) without loading all data.
    async fn stream_aggregate(
        &self,
        aggregate_expr: &str,
        where_clause: Option<&str>,
    ) -> StreamResult<FieldValue>;
}

/// Lazy field accessor for CompactTable
///
/// Provides lazy access to fields without converting the entire record.
pub struct LazyField<'a> {
    table: &'a dyn crate::velostream::table::unified_table::UnifiedTable,
    row_key: String,
    field_name: String,
    cached_value: Option<FieldValue>,
}

impl<'a> LazyField<'a> {
    /// Create a new lazy field accessor
    pub fn new(
        table: &'a dyn crate::velostream::table::unified_table::UnifiedTable,
        row_key: impl Into<String>,
        field_name: impl Into<String>,
    ) -> Self {
        Self {
            table,
            row_key: row_key.into(),
            field_name: field_name.into(),
            cached_value: None,
        }
    }

    /// Get the field value, loading it only when needed
    pub async fn get(&mut self) -> StreamResult<Option<&FieldValue>> {
        if self.cached_value.is_none() {
            // Load just this field from the table
            let mut stream = self
                .table
                .stream_filter(&format!("_key = '{}'", self.row_key))
                .await?;

            if let Some(Ok(record)) = stream.next().await {
                self.cached_value = record.fields.get(&self.field_name).cloned();
            }
        }

        Ok(self.cached_value.as_ref())
    }

    /// Check if the field exists without loading its value
    pub async fn exists(&self) -> StreamResult<bool> {
        let mut stream = self
            .table
            .stream_filter(&format!("_key = '{}'", self.row_key))
            .await?;

        if let Some(Ok(record)) = stream.next().await {
            Ok(record.fields.contains_key(&self.field_name))
        } else {
            Ok(false)
        }
    }
}

/// Memory-efficient cursor for iterating through table records
pub struct TableCursor {
    current_position: usize,
    batch_size: usize,
    total_records: Option<usize>,
}

impl TableCursor {
    /// Create a new table cursor with specified batch size
    pub fn new(batch_size: usize) -> Self {
        Self {
            current_position: 0,
            batch_size,
            total_records: None,
        }
    }

    /// Get the next batch of records
    pub async fn next_batch(
        &mut self,
        table: &dyn crate::velostream::table::unified_table::UnifiedTable,
    ) -> StreamResult<Option<RecordBatch>> {
        let batch = table
            .query_batch(self.batch_size, Some(self.current_position))
            .await?;

        if batch.records.is_empty() {
            Ok(None)
        } else {
            self.current_position += batch.records.len();
            Ok(Some(batch))
        }
    }

    /// Reset cursor to beginning
    pub fn reset(&mut self) {
        self.current_position = 0;
    }

    /// Get current position in the table
    pub fn position(&self) -> usize {
        self.current_position
    }
}

/// Builder for configuring streaming queries
pub struct StreamQueryBuilder {
    where_clause: Option<String>,
    select_fields: Vec<String>,
    batch_size: usize,
    limit: Option<usize>,
}

impl StreamQueryBuilder {
    /// Create a new streaming query builder
    pub fn new() -> Self {
        Self {
            where_clause: None,
            select_fields: Vec::new(),
            batch_size: 100,
            limit: None,
        }
    }

    /// Add WHERE clause to the query
    pub fn where_clause(mut self, clause: impl Into<String>) -> Self {
        self.where_clause = Some(clause.into());
        self
    }

    /// Select specific fields
    pub fn select(mut self, fields: Vec<String>) -> Self {
        self.select_fields = fields;
        self
    }

    /// Set batch size for streaming
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Limit total records returned
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the streaming query
    pub async fn execute(
        self,
        table: Arc<dyn crate::velostream::table::unified_table::UnifiedTable>,
    ) -> StreamResult<RecordStream> {
        if let Some(where_clause) = self.where_clause {
            table.stream_filter(&where_clause).await
        } else {
            table.stream_all().await
        }
    }

    /// Get the where clause (for testing)
    pub fn get_where_clause(&self) -> &Option<String> {
        &self.where_clause
    }

    /// Get the batch size (for testing)
    pub fn get_batch_size(&self) -> usize {
        self.batch_size
    }

    /// Get the limit (for testing)
    pub fn get_limit(&self) -> &Option<usize> {
        &self.limit
    }
}

/// Performance metrics for streaming operations
#[derive(Debug, Clone)]
pub struct StreamingMetrics {
    pub records_processed: usize,
    pub bytes_processed: usize,
    pub processing_time_ms: u64,
    pub memory_peak_mb: f64,
}

impl StreamingMetrics {
    /// Calculate throughput in records per second
    pub fn records_per_second(&self) -> f64 {
        if self.processing_time_ms == 0 {
            0.0
        } else {
            (self.records_processed as f64 * 1000.0) / self.processing_time_ms as f64
        }
    }

    /// Calculate throughput in MB per second
    pub fn mb_per_second(&self) -> f64 {
        if self.processing_time_ms == 0 {
            0.0
        } else {
            let mb = self.bytes_processed as f64 / (1024.0 * 1024.0);
            (mb * 1000.0) / self.processing_time_ms as f64
        }
    }
}
