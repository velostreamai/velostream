//! # Migration Examples: Legacy to Enhanced FerrisStreams
//!
//! This module provides practical examples showing how to migrate from legacy
//! FerrisStreams operation to the enhanced streaming engine with Phase 1B
//! watermarks and Phase 2 error handling.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::time::timeout;

// Import the enhanced streaming components
use crate::ferris::sql::{
    config::{CircuitBreakerConfig, EmitMode, ResourceLimits, StreamingConfig, WatermarkConfig},
    execution::{
        circuit_breaker::{CircuitBreaker, CircuitBreakerState},
        error::{ErrorSeverity, StreamingError},
        resource_manager::{ResourceManager, ResourceMetrics},
        watermarks::WatermarkManager,
    },
};

/// Example 1: Financial Trading System Migration
///
/// Shows migration from a simple trading processor to an enhanced version
/// with full error handling, resource management, and watermark processing
pub mod financial_trading {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Trade {
        pub symbol: String,
        pub price: f64,
        pub quantity: i64,
        pub timestamp: SystemTime,
        pub trade_id: String,
    }

    #[derive(Debug)]
    pub struct Position {
        pub symbol: String,
        pub quantity: i64,
        pub avg_price: f64,
        pub unrealized_pnl: f64,
    }

    /// Legacy trading processor - simple implementation
    pub struct LegacyTradingProcessor {
        positions: HashMap<String, Position>,
    }

    impl LegacyTradingProcessor {
        pub fn new() -> Self {
            Self {
                positions: HashMap::new(),
            }
        }

        /// Simple trade processing without error handling
        pub fn process_trade(&mut self, trade: Trade) -> Result<(), String> {
            let position = self
                .positions
                .entry(trade.symbol.clone())
                .or_insert(Position {
                    symbol: trade.symbol.clone(),
                    quantity: 0,
                    avg_price: 0.0,
                    unrealized_pnl: 0.0,
                });

            // Simple position update
            let total_cost =
                position.quantity as f64 * position.avg_price + trade.quantity as f64 * trade.price;
            position.quantity += trade.quantity;

            if position.quantity != 0 {
                position.avg_price = total_cost / position.quantity as f64;
            }

            Ok(())
        }
    }

    /// Enhanced trading processor with full production features
    pub struct EnhancedTradingProcessor {
        positions: HashMap<String, Position>,
        circuit_breaker: CircuitBreaker,
        resource_manager: ResourceManager,
        watermark_manager: WatermarkManager,
        config: StreamingConfig,
        processed_count: u64,
        error_count: u64,
    }

    impl EnhancedTradingProcessor {
        pub fn new() -> Self {
            let config = Self::create_trading_config();
            let circuit_breaker = CircuitBreaker::new(config.circuit_breaker_config.clone());
            let mut resource_manager = ResourceManager::new(config.resource_limits.clone());
            resource_manager.enable(); // Enable resource monitoring

            Self {
                positions: HashMap::new(),
                circuit_breaker,
                resource_manager,
                watermark_manager: WatermarkManager::new(config.watermark_config.clone()),
                config,
                processed_count: 0,
                error_count: 0,
            }
        }

        fn create_trading_config() -> StreamingConfig {
            StreamingConfig {
                enable_watermarks: true,
                enable_circuit_breakers: true,
                enable_resource_monitoring: true,

                // Financial trading optimized settings
                watermark_config: WatermarkConfig {
                    late_data_threshold: Duration::from_secs(30), // 30 second tolerance for market data
                    emit_mode: EmitMode::OnWatermark,
                    ..Default::default()
                },

                circuit_breaker_config: CircuitBreakerConfig {
                    failure_threshold: 15,             // Allow some market volatility errors
                    timeout: Duration::from_secs(120), // 2 minute circuit breaker
                    recovery_timeout: Duration::from_secs(60), // 1 minute recovery test
                    ..Default::default()
                },

                resource_limits: ResourceLimits {
                    max_total_memory: Some(4 * 1024 * 1024 * 1024), // 4GB for large position books
                    max_operator_memory: Some(1024 * 1024 * 1024),  // 1GB per operator
                    max_processing_time_per_record: Some(50),       // 50ms max latency for trading
                    max_concurrent_operations: Some(1000),          // High concurrency for trading
                    ..Default::default()
                },

                ..Default::default()
            }
        }

        /// Enhanced trade processing with full error handling
        pub async fn process_trade(&mut self, trade: Trade) -> Result<(), StreamingError> {
            // Step 1: Resource monitoring
            let trade_size = std::mem::size_of_val(&trade) as u64;
            self.resource_manager
                .check_allocation("total_memory", trade_size)?;
            self.resource_manager
                .update_resource_usage("active_trades", self.processed_count + 1)?;

            // Step 2: Watermark processing
            let processing_time = SystemTime::now();
            if self.config.enable_watermarks {
                let is_late = self
                    .watermark_manager
                    .is_late_data(&trade.timestamp, &processing_time);
                if is_late {
                    return Err(StreamingError::WatermarkViolation {
                        event_time: trade.timestamp,
                        watermark: self.watermark_manager.get_current_watermark(),
                        message: format!("Late trade data for symbol: {}", trade.symbol),
                    });
                }
            }

            // Step 3: Circuit breaker protected processing
            let process_start = std::time::Instant::now();
            let result = self
                .circuit_breaker
                .execute(|| async { self.process_trade_internal(trade.clone()).await })
                .await;

            // Step 4: Handle results and update metrics
            match result {
                Ok(_) => {
                    self.processed_count += 1;
                    let processing_duration = process_start.elapsed();
                    self.resource_manager
                        .record_processing_time("trade_processing", processing_duration)?;

                    // Update resource usage after successful processing
                    self.resource_manager.update_memory_usage(
                        "positions",
                        self.positions.len() as u64 * std::mem::size_of::<Position>() as u64,
                    )?;

                    Ok(())
                }
                Err(StreamingError::CircuitBreakerOpen { recovery_time }) => {
                    log::warn!("Trading circuit breaker open until: {:?}", recovery_time);
                    self.implement_trading_halt().await?;
                    Err(StreamingError::CircuitBreakerOpen { recovery_time })
                }
                Err(e) => {
                    self.error_count += 1;
                    if e.is_retryable() {
                        log::warn!("Retryable trading error (will auto-retry): {}", e);
                    } else {
                        log::error!("Fatal trading error: {}", e);
                    }
                    Err(e)
                }
            }
        }

        async fn process_trade_internal(&mut self, trade: Trade) -> Result<(), StreamingError> {
            // Validate trade data
            if trade.price <= 0.0 {
                return Err(StreamingError::ValidationError {
                    field: "price".to_string(),
                    value: trade.price.to_string(),
                    message: "Trade price must be positive".to_string(),
                });
            }

            if trade.quantity == 0 {
                return Err(StreamingError::ValidationError {
                    field: "quantity".to_string(),
                    value: trade.quantity.to_string(),
                    message: "Trade quantity cannot be zero".to_string(),
                });
            }

            // Process the trade with enhanced error handling
            let position = self
                .positions
                .entry(trade.symbol.clone())
                .or_insert(Position {
                    symbol: trade.symbol.clone(),
                    quantity: 0,
                    avg_price: 0.0,
                    unrealized_pnl: 0.0,
                });

            // Enhanced position calculation with overflow protection
            let current_value = position
                .quantity
                .checked_mul(position.avg_price as i64)
                .ok_or_else(|| StreamingError::ProcessingError {
                    operation: "position_calculation".to_string(),
                    message: "Overflow in current position value calculation".to_string(),
                    retry_possible: false,
                    source: None,
                })?;

            let trade_value = trade
                .quantity
                .checked_mul(trade.price as i64)
                .ok_or_else(|| StreamingError::ProcessingError {
                    operation: "trade_calculation".to_string(),
                    message: "Overflow in trade value calculation".to_string(),
                    retry_possible: false,
                    source: None,
                })?;

            let total_cost = (current_value + trade_value) as f64;
            let new_quantity = position.quantity + trade.quantity;

            position.quantity = new_quantity;
            if new_quantity != 0 {
                position.avg_price = total_cost / new_quantity as f64;
            } else {
                position.avg_price = 0.0;
            }

            // Calculate unrealized P&L (simplified)
            position.unrealized_pnl = (trade.price - position.avg_price) * position.quantity as f64;

            Ok(())
        }

        async fn implement_trading_halt(&mut self) -> Result<(), StreamingError> {
            log::warn!("Implementing trading halt due to circuit breaker");

            // In a real system, this would:
            // 1. Stop accepting new orders
            // 2. Cancel pending orders
            // 3. Notify risk management systems
            // 4. Send alerts to operations team

            // For this example, we'll just log and clean up resources
            self.cleanup_stale_positions().await?;
            Ok(())
        }

        async fn cleanup_stale_positions(&mut self) -> Result<(), StreamingError> {
            let initial_count = self.positions.len();

            // Remove positions with zero quantity
            self.positions.retain(|_, position| position.quantity != 0);

            let cleaned_count = initial_count - self.positions.len();
            log::info!("Cleaned up {} stale positions", cleaned_count);

            Ok(())
        }

        /// Get current trading metrics
        pub fn get_metrics(&self) -> TradingMetrics {
            let resource_usage = self.resource_manager.get_all_resource_usage();
            let circuit_breaker_metrics = self.circuit_breaker.get_metrics();

            TradingMetrics {
                processed_trades: self.processed_count,
                error_count: self.error_count,
                active_positions: self.positions.len() as u64,
                circuit_breaker_state: circuit_breaker_metrics.state,
                memory_usage: resource_usage
                    .get("total_memory")
                    .map(|m| m.current)
                    .unwrap_or(0),
                processing_errors: circuit_breaker_metrics.failure_count,
            }
        }
    }

    #[derive(Debug)]
    pub struct TradingMetrics {
        pub processed_trades: u64,
        pub error_count: u64,
        pub active_positions: u64,
        pub circuit_breaker_state: CircuitBreakerState,
        pub memory_usage: u64,
        pub processing_errors: u32,
    }
}

/// Example 2: IoT Sensor Data Processing Migration
///
/// Shows migration from simple sensor aggregation to enhanced processing
/// with resource management and late data handling
pub mod iot_sensors {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SensorReading {
        pub sensor_id: String,
        pub device_type: String,
        pub value: f64,
        pub unit: String,
        pub timestamp: SystemTime,
        pub location: String,
    }

    #[derive(Debug)]
    pub struct SensorAggregate {
        pub sensor_id: String,
        pub min_value: f64,
        pub max_value: f64,
        pub avg_value: f64,
        pub count: u64,
        pub last_updated: SystemTime,
    }

    /// Legacy sensor processor
    pub struct LegacySensorProcessor {
        aggregates: HashMap<String, SensorAggregate>,
    }

    impl LegacySensorProcessor {
        pub fn new() -> Self {
            Self {
                aggregates: HashMap::new(),
            }
        }

        pub fn process_reading(&mut self, reading: SensorReading) -> Result<(), String> {
            let aggregate =
                self.aggregates
                    .entry(reading.sensor_id.clone())
                    .or_insert(SensorAggregate {
                        sensor_id: reading.sensor_id.clone(),
                        min_value: reading.value,
                        max_value: reading.value,
                        avg_value: reading.value,
                        count: 0,
                        last_updated: reading.timestamp,
                    });

            // Simple aggregation
            aggregate.min_value = aggregate.min_value.min(reading.value);
            aggregate.max_value = aggregate.max_value.max(reading.value);
            aggregate.avg_value = (aggregate.avg_value * aggregate.count as f64 + reading.value)
                / (aggregate.count + 1) as f64;
            aggregate.count += 1;
            aggregate.last_updated = reading.timestamp;

            Ok(())
        }
    }

    /// Enhanced sensor processor with full production features
    pub struct EnhancedSensorProcessor {
        aggregates: HashMap<String, SensorAggregate>,
        circuit_breaker: CircuitBreaker,
        resource_manager: ResourceManager,
        watermark_manager: WatermarkManager,
        config: StreamingConfig,
        processed_count: u64,
        late_data_count: u64,
        degraded_mode: bool,
    }

    impl EnhancedSensorProcessor {
        pub fn new() -> Self {
            let config = Self::create_iot_config();
            let circuit_breaker = CircuitBreaker::new(config.circuit_breaker_config.clone());
            let mut resource_manager = ResourceManager::new(config.resource_limits.clone());
            resource_manager.enable();

            Self {
                aggregates: HashMap::new(),
                circuit_breaker,
                resource_manager,
                watermark_manager: WatermarkManager::new(config.watermark_config.clone()),
                config,
                processed_count: 0,
                late_data_count: 0,
                degraded_mode: false,
            }
        }

        fn create_iot_config() -> StreamingConfig {
            StreamingConfig {
                enable_watermarks: true,
                enable_circuit_breakers: true,
                enable_resource_monitoring: true,

                // IoT sensor optimized settings
                watermark_config: WatermarkConfig {
                    late_data_threshold: Duration::from_secs(300), // 5 minute tolerance for IoT
                    emit_mode: EmitMode::OnWatermark,
                    ..Default::default()
                },

                circuit_breaker_config: CircuitBreakerConfig {
                    failure_threshold: 25,             // Allow sensor noise and connectivity issues
                    timeout: Duration::from_secs(180), // 3 minute circuit breaker
                    recovery_timeout: Duration::from_secs(30), // Quick recovery for IoT
                    ..Default::default()
                },

                resource_limits: ResourceLimits {
                    max_total_memory: Some(2 * 1024 * 1024 * 1024), // 2GB for sensor aggregates
                    max_windows_per_key: Some(10000),               // Many sensors
                    max_aggregation_groups: Some(50000),            // Many device groups
                    max_processing_time_per_record: Some(500), // 500ms tolerance for complex sensors
                    ..Default::default()
                },

                ..Default::default()
            }
        }

        pub async fn process_reading(
            &mut self,
            reading: SensorReading,
        ) -> Result<(), StreamingError> {
            // Resource monitoring
            let reading_size = std::mem::size_of_val(&reading) as u64;
            self.resource_manager
                .check_allocation("total_memory", reading_size)?;

            // Watermark processing
            let processing_time = SystemTime::now();
            if self.config.enable_watermarks {
                let is_late = self
                    .watermark_manager
                    .is_late_data(&reading.timestamp, &processing_time);
                if is_late {
                    self.late_data_count += 1;
                    log::warn!(
                        "Late sensor data from {}: {:?}",
                        reading.sensor_id,
                        reading.timestamp
                    );

                    // For IoT, we might still process late data but mark it
                    if self.should_process_late_data(&reading) {
                        return self.process_late_reading(reading).await;
                    } else {
                        return Err(StreamingError::WatermarkViolation {
                            event_time: reading.timestamp,
                            watermark: self.watermark_manager.get_current_watermark(),
                            message: format!("Sensor reading too late: {}", reading.sensor_id),
                        });
                    }
                }
            }

            // Circuit breaker protected processing
            let result = self
                .circuit_breaker
                .execute(|| async { self.process_reading_internal(reading.clone()).await })
                .await;

            match result {
                Ok(_) => {
                    self.processed_count += 1;

                    // Update resource metrics
                    self.resource_manager
                        .update_resource_usage("sensor_aggregates", self.aggregates.len() as u64)?;

                    // Exit degraded mode if we were in it
                    if self.degraded_mode {
                        self.degraded_mode = false;
                        log::info!("Exited degraded mode - sensor processing recovered");
                    }

                    Ok(())
                }
                Err(StreamingError::CircuitBreakerOpen { .. }) => {
                    log::warn!("Sensor circuit breaker open - entering degraded mode");
                    self.enter_degraded_mode().await?;
                    Err(StreamingError::CircuitBreakerOpen {
                        recovery_time: SystemTime::now()
                            + self.config.circuit_breaker_config.recovery_timeout,
                    })
                }
                Err(StreamingError::ResourceExhausted { resource_type, .. }) => {
                    log::warn!(
                        "Resource exhausted: {} - cleaning sensor buffers",
                        resource_type
                    );
                    self.cleanup_sensor_buffers().await?;
                    Err(StreamingError::ResourceExhausted {
                        resource_type,
                        current_usage: 0, // Will be updated by cleanup
                        limit: 0,
                        message: "Cleaned up and retrying".to_string(),
                    })
                }
                Err(e) => {
                    log::error!("Sensor processing error: {}", e);
                    Err(e)
                }
            }
        }

        async fn process_reading_internal(
            &mut self,
            reading: SensorReading,
        ) -> Result<(), StreamingError> {
            // Validate sensor reading
            if !reading.value.is_finite() {
                return Err(StreamingError::ValidationError {
                    field: "value".to_string(),
                    value: reading.value.to_string(),
                    message: "Sensor value must be finite".to_string(),
                });
            }

            // Enhanced aggregation with overflow protection
            let aggregate =
                self.aggregates
                    .entry(reading.sensor_id.clone())
                    .or_insert(SensorAggregate {
                        sensor_id: reading.sensor_id.clone(),
                        min_value: reading.value,
                        max_value: reading.value,
                        avg_value: reading.value,
                        count: 0,
                        last_updated: reading.timestamp,
                    });

            // Protect against overflow in averaging
            if aggregate.count >= u64::MAX - 1 {
                // Reset aggregate to prevent overflow
                *aggregate = SensorAggregate {
                    sensor_id: reading.sensor_id.clone(),
                    min_value: reading.value,
                    max_value: reading.value,
                    avg_value: reading.value,
                    count: 1,
                    last_updated: reading.timestamp,
                };
            } else {
                // Normal aggregation
                aggregate.min_value = aggregate.min_value.min(reading.value);
                aggregate.max_value = aggregate.max_value.max(reading.value);
                aggregate.avg_value = (aggregate.avg_value * aggregate.count as f64
                    + reading.value)
                    / (aggregate.count + 1) as f64;
                aggregate.count += 1;
                aggregate.last_updated = reading.timestamp;
            }

            Ok(())
        }

        async fn process_late_reading(
            &mut self,
            reading: SensorReading,
        ) -> Result<(), StreamingError> {
            log::info!("Processing late sensor reading from {}", reading.sensor_id);
            // Simplified processing for late data
            self.process_reading_internal(reading).await
        }

        fn should_process_late_data(&self, reading: &SensorReading) -> bool {
            // For critical sensors, always process late data
            reading.device_type == "critical" || reading.device_type == "safety"
        }

        async fn enter_degraded_mode(&mut self) -> Result<(), StreamingError> {
            self.degraded_mode = true;
            log::warn!("Entering degraded sensor processing mode");

            // In degraded mode, we might:
            // 1. Use simpler aggregation algorithms
            // 2. Process only critical sensors
            // 3. Reduce memory usage by clearing old data

            self.cleanup_sensor_buffers().await?;
            Ok(())
        }

        async fn cleanup_sensor_buffers(&mut self) -> Result<(), StreamingError> {
            let initial_count = self.aggregates.len();
            let cutoff_time = SystemTime::now() - Duration::from_secs(3600); // 1 hour

            // Remove old aggregates
            self.aggregates
                .retain(|_, aggregate| aggregate.last_updated > cutoff_time);

            let cleaned_count = initial_count - self.aggregates.len();
            log::info!("Cleaned up {} old sensor aggregates", cleaned_count);

            Ok(())
        }

        pub fn get_metrics(&self) -> SensorMetrics {
            let resource_usage = self.resource_manager.get_all_resource_usage();
            let circuit_breaker_metrics = self.circuit_breaker.get_metrics();

            SensorMetrics {
                processed_readings: self.processed_count,
                late_data_count: self.late_data_count,
                active_sensors: self.aggregates.len() as u64,
                degraded_mode: self.degraded_mode,
                circuit_breaker_state: circuit_breaker_metrics.state,
                memory_usage: resource_usage
                    .get("total_memory")
                    .map(|m| m.current)
                    .unwrap_or(0),
            }
        }
    }

    #[derive(Debug)]
    pub struct SensorMetrics {
        pub processed_readings: u64,
        pub late_data_count: u64,
        pub active_sensors: u64,
        pub degraded_mode: bool,
        pub circuit_breaker_state: CircuitBreakerState,
        pub memory_usage: u64,
    }
}

/// Example 3: Simple Migration Example
///
/// Shows the most basic migration from legacy to enhanced mode
pub mod simple_migration {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct SimpleRecord {
        pub id: String,
        pub value: f64,
        pub timestamp: SystemTime,
    }

    /// Step-by-step migration example
    pub struct MigrationExample {
        config: StreamingConfig,
        records: Vec<SimpleRecord>,
    }

    impl MigrationExample {
        /// Step 1: Start with legacy configuration (baseline)
        pub fn new_legacy() -> Self {
            Self {
                config: StreamingConfig::default(), // All features disabled
                records: Vec::new(),
            }
        }

        /// Step 2: Enable watermarks only (Phase 1B)
        pub fn enable_watermarks(mut self) -> Self {
            self.config.enable_watermarks = true;
            self.config.watermark_config = WatermarkConfig {
                late_data_threshold: Duration::from_secs(60),
                emit_mode: EmitMode::OnWatermark,
                ..Default::default()
            };
            self
        }

        /// Step 3: Add circuit breakers (Phase 2 partial)
        pub fn enable_circuit_breakers(mut self) -> Self {
            self.config.enable_circuit_breakers = true;
            self.config.circuit_breaker_config = CircuitBreakerConfig {
                failure_threshold: 5,
                timeout: Duration::from_secs(60),
                recovery_timeout: Duration::from_secs(30),
                ..Default::default()
            };
            self
        }

        /// Step 4: Enable full resource monitoring (Phase 2 complete)
        pub fn enable_resource_monitoring(mut self) -> Self {
            self.config.enable_resource_monitoring = true;
            self.config.resource_limits = ResourceLimits {
                max_total_memory: Some(1024 * 1024 * 1024), // 1GB
                max_processing_time_per_record: Some(100),  // 100ms
                ..Default::default()
            };
            self
        }

        /// Process records with current configuration
        pub async fn process_records(
            &mut self,
            records: Vec<SimpleRecord>,
        ) -> Result<usize, StreamingError> {
            let mut processed = 0;

            // Create processors based on configuration
            let mut circuit_breaker = if self.config.enable_circuit_breakers {
                Some(CircuitBreaker::new(
                    self.config.circuit_breaker_config.clone(),
                ))
            } else {
                None
            };

            let mut resource_manager = if self.config.enable_resource_monitoring {
                let mut rm = ResourceManager::new(self.config.resource_limits.clone());
                rm.enable();
                Some(rm)
            } else {
                None
            };

            let watermark_manager = if self.config.enable_watermarks {
                Some(WatermarkManager::new(self.config.watermark_config.clone()))
            } else {
                None
            };

            for record in records {
                // Resource checking
                if let Some(ref rm) = resource_manager {
                    let record_size = std::mem::size_of_val(&record) as u64;
                    rm.check_allocation("total_memory", record_size)?;
                }

                // Watermark checking
                if let Some(ref wm) = watermark_manager {
                    let processing_time = SystemTime::now();
                    if wm.is_late_data(&record.timestamp, &processing_time) {
                        log::warn!("Late data detected for record: {}", record.id);
                        continue; // Skip late data in this simple example
                    }
                }

                // Circuit breaker protected processing
                let process_result = if let Some(ref cb) = circuit_breaker {
                    cb.execute(|| async { Self::process_single_record(record.clone()).await })
                        .await
                } else {
                    // Legacy processing without circuit breaker
                    Self::process_single_record(record).await
                };

                match process_result {
                    Ok(_) => processed += 1,
                    Err(StreamingError::CircuitBreakerOpen { .. }) => {
                        log::warn!("Circuit breaker open - stopping processing");
                        break;
                    }
                    Err(e) => {
                        log::error!("Processing error: {}", e);
                        if !e.is_retryable() {
                            return Err(e);
                        }
                    }
                }
            }

            Ok(processed)
        }

        async fn process_single_record(record: SimpleRecord) -> Result<(), StreamingError> {
            // Simulate processing work
            if record.value < 0.0 {
                return Err(StreamingError::ValidationError {
                    field: "value".to_string(),
                    value: record.value.to_string(),
                    message: "Value cannot be negative".to_string(),
                });
            }

            // Simulate some processing time
            tokio::time::sleep(Duration::from_millis(10)).await;

            Ok(())
        }

        pub fn get_config(&self) -> &StreamingConfig {
            &self.config
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_financial_trading_migration() {
        let mut processor = financial_trading::EnhancedTradingProcessor::new();

        let trade = financial_trading::Trade {
            symbol: "AAPL".to_string(),
            price: 150.0,
            quantity: 100,
            timestamp: SystemTime::now(),
            trade_id: "trade_001".to_string(),
        };

        let result = processor.process_trade(trade).await;
        assert!(result.is_ok());

        let metrics = processor.get_metrics();
        assert_eq!(metrics.processed_trades, 1);
        assert_eq!(metrics.active_positions, 1);
    }

    #[tokio::test]
    async fn test_iot_sensor_migration() {
        let mut processor = iot_sensors::EnhancedSensorProcessor::new();

        let reading = iot_sensors::SensorReading {
            sensor_id: "temp_001".to_string(),
            device_type: "temperature".to_string(),
            value: 23.5,
            unit: "celsius".to_string(),
            timestamp: SystemTime::now(),
            location: "warehouse_a".to_string(),
        };

        let result = processor.process_reading(reading).await;
        assert!(result.is_ok());

        let metrics = processor.get_metrics();
        assert_eq!(metrics.processed_readings, 1);
        assert_eq!(metrics.active_sensors, 1);
    }

    #[tokio::test]
    async fn test_simple_migration_steps() {
        let records = vec![
            simple_migration::SimpleRecord {
                id: "record_1".to_string(),
                value: 42.0,
                timestamp: SystemTime::now(),
            },
            simple_migration::SimpleRecord {
                id: "record_2".to_string(),
                value: 84.0,
                timestamp: SystemTime::now(),
            },
        ];

        // Step 1: Legacy mode
        let mut processor = simple_migration::MigrationExample::new_legacy();
        let result = processor.process_records(records.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);

        // Step 2: With watermarks
        let mut processor = simple_migration::MigrationExample::new_legacy().enable_watermarks();
        let result = processor.process_records(records.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);

        // Step 3: Full enhanced mode
        let mut processor = simple_migration::MigrationExample::new_legacy()
            .enable_watermarks()
            .enable_circuit_breakers()
            .enable_resource_monitoring();
        let result = processor.process_records(records).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
    }
}
