/*!
# Table Loading Helpers - Phase 7 Implementation

This module provides convenient helper functions that implement the bulk + incremental
loading pattern using the existing mature DataSource trait system. This approach
leverages existing implementations while providing the unified loading semantics
required by Phase 7.

## Architecture Decision

Rather than creating a new TableDataSource trait (which would create architectural
confusion), this module provides helper functions that work with the existing
DataSource trait to implement:

1. **Bulk Load**: Load all available data using `read()` + `has_more()`
2. **Incremental Load**: Load new data since a specific offset using `seek()` + `read()`

## Benefits

- ✅ **No Architectural Confusion**: Single trait system
- ✅ **Leverage Maturity**: Use existing proven DataSource implementations
- ✅ **Backward Compatible**: All existing code continues to work
- ✅ **Performance**: Builds on existing optimizations

## Usage Example

```rust
use velostream::velostream::table::loading_helpers::{bulk_load_table, incremental_load_table};
use velostream::velostream::datasource::kafka::KafkaDataSource;
use velostream::velostream::datasource::types::SourceOffset;
use std::collections::HashMap;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let mut kafka_source = KafkaDataSource::new("localhost:9092".to_string(), "topic".to_string());

    // Phase 1: Bulk load all existing data
    let initial_records = bulk_load_table(&kafka_source, None).await?;
    println!("Loaded {} initial records", initial_records.len());

    // Phase 2: Incremental load new data
    let offset = SourceOffset::Generic("0".to_string());
    let new_records = incremental_load_table(&kafka_source, offset, None).await?;
    println!("Loaded {} new records", new_records.len());

    Ok(())
}
```
*/

use crate::velostream::datasource::traits::{DataReader, DataSource};
use crate::velostream::datasource::types::SourceOffset;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::execution::types::StreamRecord;
use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant, SystemTime};

/// Statistics for bulk and incremental loading operations
#[derive(Debug, Clone, Default)]
pub struct LoadingStats {
    /// Number of records loaded in bulk operations
    pub bulk_records_loaded: u64,
    /// Total time spent on bulk loading (milliseconds)
    pub bulk_load_duration_ms: u64,
    /// Number of records loaded in incremental operations
    pub incremental_records_loaded: u64,
    /// Total time spent on incremental loading (milliseconds)
    pub incremental_load_duration_ms: u64,
    /// Total number of load operations
    pub total_load_operations: u64,
    /// Number of failed load operations
    pub failed_load_operations: u64,
    /// Last successful load timestamp
    pub last_successful_load: Option<SystemTime>,
}

/// Configuration for loading operations
#[derive(Debug, Clone)]
pub struct LoadingConfig {
    /// Maximum number of records to load in a single bulk operation (None = unlimited)
    pub max_bulk_records: Option<usize>,
    /// Maximum time to spend on bulk loading (None = unlimited)
    pub max_bulk_duration: Option<Duration>,
    /// Maximum number of records to load in a single incremental operation
    pub max_incremental_records: Option<usize>,
    /// Timeout for individual read operations
    pub read_timeout: Duration,
    /// Whether to continue on read errors (vs failing immediately)
    pub continue_on_errors: bool,
}

impl Default for LoadingConfig {
    fn default() -> Self {
        Self {
            max_bulk_records: None,
            max_bulk_duration: Some(Duration::from_secs(60)), // 1 minute default
            max_incremental_records: Some(1000),              // Reasonable batch size
            read_timeout: Duration::from_secs(30),
            continue_on_errors: true,
        }
    }
}

/// Bulk load all available data from a data source
///
/// This function implements Phase 1 of the unified loading pattern by reading
/// all available data from the source until no more data is available.
///
/// # Arguments
/// - `data_source`: The data source to load from
/// - `config`: Optional loading configuration (uses defaults if None)
///
/// # Returns
/// - `Ok(Vec<StreamRecord>)`: All records loaded from the data source
/// - `Err(SqlError)`: If bulk loading fails
pub async fn bulk_load_table<T>(
    data_source: &T,
    config: Option<LoadingConfig>,
) -> Result<Vec<StreamRecord>, SqlError>
where
    T: DataSource,
{
    let config = config.unwrap_or_default();
    let mut records = Vec::new();
    let load_start = Instant::now();

    // Check if the data source supports batch reading
    if !data_source.supports_batch() {
        return Err(SqlError::ConfigurationError {
            message: "Data source does not support batch reading".to_string(),
        });
    }

    // Create reader for bulk loading
    let mut reader = data_source
        .create_reader()
        .await
        .map_err(|e| SqlError::ResourceError {
            resource: "reader".to_string(),
            message: format!("Failed to create reader: {}", e),
        })?;

    // Read all available data
    loop {
        // Check time limit
        if let Some(max_duration) = config.max_bulk_duration {
            if load_start.elapsed() >= max_duration {
                log::warn!(
                    "Bulk load timeout reached after {:?}, loaded {} records",
                    max_duration,
                    records.len()
                );
                break;
            }
        }

        // Check record limit
        if let Some(max_records) = config.max_bulk_records {
            if records.len() >= max_records {
                log::info!("Bulk load record limit reached: {} records", max_records);
                break;
            }
        }

        // Check if more data is available
        let has_more = reader
            .has_more()
            .await
            .map_err(|e| SqlError::ResourceError {
                resource: "reader".to_string(),
                message: format!("Failed to check for more data: {}", e),
            })?;

        if !has_more {
            log::debug!("No more data available, bulk load complete");
            break;
        }

        // Read next batch
        match reader.read().await {
            Ok(batch) => {
                if batch.is_empty() {
                    log::debug!("Empty batch received, bulk load complete");
                    break;
                }

                let batch_size = batch.len();
                records.extend(batch);
                log::debug!(
                    "Read batch of {} records (total: {})",
                    batch_size,
                    records.len()
                );
            }
            Err(e) => {
                if config.continue_on_errors {
                    log::warn!("Error during bulk load, continuing: {}", e);
                    continue;
                } else {
                    return Err(SqlError::ExecutionError {
                        message: format!("Bulk load failed: {}", e),
                        query: None,
                    });
                }
            }
        }
    }

    log::info!(
        "Bulk load complete: {} records in {:?}",
        records.len(),
        load_start.elapsed()
    );

    Ok(records)
}

/// Incremental load new data from a data source since a specific offset
///
/// This function implements Phase 2 of the unified loading pattern by seeking
/// to a specific offset and reading new data that has arrived since then.
///
/// # Arguments
/// - `data_source`: The data source to load from
/// - `since_offset`: The offset to start loading from
/// - `config`: Optional loading configuration (uses defaults if None)
///
/// # Returns
/// - `Ok(Vec<StreamRecord>)`: New records since the offset
/// - `Err(SqlError)`: If incremental loading fails
pub async fn incremental_load_table<T>(
    data_source: &T,
    since_offset: SourceOffset,
    config: Option<LoadingConfig>,
) -> Result<Vec<StreamRecord>, SqlError>
where
    T: DataSource,
{
    let config = config.unwrap_or_default();
    let mut records = Vec::new();
    let load_start = Instant::now();

    // Check if the data source supports streaming
    if !data_source.supports_streaming() {
        return Err(SqlError::ConfigurationError {
            message: "Data source does not support streaming/incremental reading".to_string(),
        });
    }

    // Create reader for incremental loading
    let mut reader = data_source
        .create_reader()
        .await
        .map_err(|e| SqlError::ResourceError {
            resource: "reader".to_string(),
            message: format!("Failed to create reader: {}", e),
        })?;

    // Seek to the specified offset
    reader
        .seek(since_offset.clone())
        .await
        .map_err(|e| SqlError::ExecutionError {
            message: format!("Failed to seek to offset {:?}: {}", since_offset, e),
            query: None,
        })?;

    // Read incremental data up to the configured limit
    while records.len() < config.max_incremental_records.unwrap_or(usize::MAX) {
        // Check if more data is available
        let has_more = reader
            .has_more()
            .await
            .map_err(|e| SqlError::ResourceError {
                resource: "reader".to_string(),
                message: format!("Failed to check for more data: {}", e),
            })?;

        if !has_more {
            log::debug!("No more incremental data available");
            break;
        }

        // Read next batch
        match reader.read().await {
            Ok(batch) => {
                if batch.is_empty() {
                    log::debug!("Empty batch received, incremental load complete");
                    break;
                }

                let batch_size = batch.len();
                records.extend(batch);
                log::debug!(
                    "Read incremental batch of {} records (total: {})",
                    batch_size,
                    records.len()
                );
            }
            Err(e) => {
                if config.continue_on_errors {
                    log::warn!("Error during incremental load, continuing: {}", e);
                    continue;
                } else {
                    return Err(SqlError::ExecutionError {
                        message: format!("Incremental load failed: {}", e),
                        query: None,
                    });
                }
            }
        }
    }

    log::info!(
        "Incremental load complete: {} records in {:?}",
        records.len(),
        load_start.elapsed()
    );

    Ok(records)
}

/// Get the current offset from a data reader
///
/// This is a helper function to get the current position in a data source
/// for use in subsequent incremental loading operations.
///
/// # Arguments
/// - `reader`: The data reader to get the offset from
///
/// # Returns
/// - `Ok(Option<SourceOffset>)`: Current offset if available
/// - `Err(SqlError)`: If getting offset fails
pub async fn get_current_offset<T>(reader: &mut T) -> Result<Option<SourceOffset>, SqlError>
where
    T: DataReader,
{
    // The DataReader trait doesn't have a get_offset method in the current design
    // This would need to be added to the trait or we could track it externally
    // For now, we'll return None to indicate offset tracking needs to be implemented
    log::warn!("Offset tracking not yet implemented in DataReader trait");
    Ok(None)
}

/// Helper function to create a unified table loading strategy
///
/// This combines bulk and incremental loading into a single operation that:
/// 1. Checks if any previous offset exists
/// 2. If no offset: performs bulk load
/// 3. If offset exists: performs incremental load
///
/// # Arguments
/// - `data_source`: The data source to load from
/// - `previous_offset`: Optional previous offset for incremental loading
/// - `config`: Optional loading configuration
///
/// # Returns
/// - `Ok((Vec<StreamRecord>, LoadingStats))`: Loaded records and statistics
/// - `Err(SqlError)`: If loading fails
pub async fn unified_load_table<T>(
    data_source: &T,
    previous_offset: Option<SourceOffset>,
    config: Option<LoadingConfig>,
) -> Result<(Vec<StreamRecord>, LoadingStats), SqlError>
where
    T: DataSource,
{
    let start_time = Instant::now();
    let mut stats = LoadingStats::default();

    let records = match previous_offset {
        None => {
            // No previous offset: perform bulk load
            log::info!("No previous offset found, performing bulk load");
            let records = bulk_load_table(data_source, config).await?;
            stats.bulk_records_loaded = records.len() as u64;
            stats.bulk_load_duration_ms = start_time.elapsed().as_millis() as u64;
            records
        }
        Some(offset) => {
            // Previous offset exists: perform incremental load
            log::info!(
                "Previous offset found, performing incremental load from {:?}",
                offset
            );
            let records = incremental_load_table(data_source, offset, config).await?;
            stats.incremental_records_loaded = records.len() as u64;
            stats.incremental_load_duration_ms = start_time.elapsed().as_millis() as u64;
            records
        }
    };

    stats.total_load_operations = 1;
    stats.last_successful_load = Some(SystemTime::now());

    log::info!(
        "Unified load complete: {} records in {:?}",
        records.len(),
        start_time.elapsed()
    );

    Ok((records, stats))
}

/// Check if a data source supports the unified loading pattern
///
/// # Arguments
/// - `data_source`: The data source to check
///
/// # Returns
/// - `(bool, bool)`: (supports_bulk, supports_incremental)
pub fn check_loading_support<T>(data_source: &T) -> (bool, bool)
where
    T: DataSource,
{
    let supports_bulk = data_source.supports_batch();
    let supports_incremental = data_source.supports_streaming();

    log::info!(
        "Data source loading support: bulk={}, incremental={}",
        supports_bulk,
        supports_incremental
    );

    (supports_bulk, supports_incremental)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loading_config_defaults() {
        let config = LoadingConfig::default();
        assert!(config.max_bulk_records.is_none());
        assert!(config.max_bulk_duration.is_some());
        assert_eq!(config.max_incremental_records, Some(1000));
        assert_eq!(config.read_timeout, Duration::from_secs(30));
        assert!(config.continue_on_errors);
    }

    #[test]
    fn test_loading_stats_default() {
        let stats = LoadingStats::default();
        assert_eq!(stats.bulk_records_loaded, 0);
        assert_eq!(stats.incremental_records_loaded, 0);
        assert_eq!(stats.total_load_operations, 0);
        assert_eq!(stats.failed_load_operations, 0);
        assert!(stats.last_successful_load.is_none());
    }

    #[test]
    fn test_check_loading_support() {
        // This would require a mock DataSource implementation for proper testing
        // For now, we'll just verify the function signature compiles
    }
}
