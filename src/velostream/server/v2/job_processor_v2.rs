//! V2 JobProcessor implementation for AdaptiveJobProcessor
//!
//! Implements the JobProcessor trait for multi-partition parallel execution.
//!
//! ## Phase 6.3a Architecture
//!
//! This implementation properly delegates to AdaptiveJobProcessor:
//! - Creates per-partition StreamExecutionEngine instances (no shared RwLock)
//! - Routes records using pluggable partitioning strategies
//! - Each partition processes independently with its own state
//! - Enables true parallelism with zero cross-partition contention
//!
//! ## Critical Architecture Note
//!
//! V2 requires GROUP BY key-based routing to maintain state consistency:
//! - Records with the SAME GROUP BY key MUST go to the SAME partition
//! - This ensures state aggregations are not fragmented across partitions
//! - Round-robin by index would break streaming aggregations
//!
//! ## State Management
//!
//! - Each partition owns independent StreamExecutionEngine (via PartitionStateManager)
//! - ProcessorContext state is per-partition only
//! - No shared state between partitions
//! - HashMap cloning is per-partition (not cross-partition)
//!
//! See: `src/velostream/server/v2/coordinator.rs` for coordinator implementation
//! See: `src/velostream/server/v2/partition_manager.rs` for per-partition state management

use crate::velostream::datasource::{DataReader, DataWriter};
use crate::velostream::server::processors::{
    JobProcessor, ProcessorMetrics, common::JobExecutionStats,
};
use crate::velostream::server::v2::AdaptiveJobProcessor;
use crate::velostream::sql::StreamExecutionEngine;
use crate::velostream::sql::StreamingQuery;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Implement JobProcessor trait for AdaptiveJobProcessor (V2 Architecture)
///
/// This enables V2 to be used interchangeably with V1 via the JobProcessor trait.
#[async_trait::async_trait]
impl JobProcessor for AdaptiveJobProcessor {
    fn num_partitions(&self) -> usize {
        self.num_partitions()
    }

    fn processor_name(&self) -> &str {
        "AdaptiveJobProcessor"
    }

    fn processor_version(&self) -> &str {
        "V2"
    }

    fn metrics(&self) -> ProcessorMetrics {
        let metrics_collector = self.metrics_collector();
        ProcessorMetrics {
            version: self.processor_version().to_string(),
            name: self.processor_name().to_string(),
            num_partitions: self.num_partitions(),
            lifecycle_state: metrics_collector.lifecycle_state(),
            total_records: metrics_collector.total_records(),
            failed_records: metrics_collector.failed_records(),
            throughput_rps: metrics_collector.throughput_rps(),
            uptime_secs: metrics_collector.uptime_secs(),
        }
    }

    async fn process_job(
        &self,
        mut reader: Box<dyn DataReader>,
        writer: Option<Box<dyn DataWriter>>,
        _engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
        // Phase 6.6: V2 Coordinator-Based Job Processing with Synchronous Receivers
        //
        // Architecture:
        // 1. Create PartitionReceiver instances with owned engines (no Arc/Mutex)
        // 2. Read batches and route to receivers as batch units
        // 3. Each receiver processes batches synchronously
        // 4. No cross-partition contention
        // 5. Expected 15-25% architectural overhead reduction
        //
        // This is the PRIMARY implementation for V2 JobProcessor.
        // All job processing now uses Phase 6.6 synchronous batch-based receivers.

        info!(
            "V2 AdaptiveJobProcessor::process_job: {} (Phase 6.6 - Synchronous batch receivers)",
            job_name
        );

        let start_time = std::time::Instant::now();
        let mut aggregated_stats = JobExecutionStats::new();

        // Step 1: Wrap query in Arc for partition initialization
        let query_arc = Arc::new(query);

        // Step 2: Initialize partitions with Phase 6.6 synchronous receivers
        // Each partition owns its StreamExecutionEngine directly (no Arc/Mutex)
        // Enables 15-25% architectural overhead elimination
        // Pass query directly to initialize_partitions_v6_6, which will distribute to each partition

        // Wrap writer in Arc<Mutex<>> for sharing across partitions
        let shared_writer = writer.map(|w| Arc::new(tokio::sync::Mutex::new(w)));

        let (batch_queues, _metrics, eof_flags, task_handles) =
            self.initialize_partitions_v6_6(query_arc.clone(), shared_writer.clone());

        info!(
            "Initialized {} Phase 6.8 partition receivers with lock-free queues for job: {}",
            batch_queues.len(),
            job_name
        );

        // Yield control to allow spawned partition receiver tasks to start
        // This prevents deadlock where the main loop awaits on channel send
        // while receivers haven't been scheduled yet
        tokio::task::yield_now().await;

        // Step 3: Read batches and route to receivers (busy-spin pattern)
        // Shutdown is controlled via stop_flag, EOF detection via has_more()
        let mut total_routed = 0u64;
        let mut consecutive_empty_batches = 0;
        let max_empty_batches = self.config().empty_batch_count as usize;

        loop {
            // Check if processor stop signal was raised
            if self.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                info!("Stop signal received from processor, exiting read loop");
                break;
            }

            // Non-blocking check for shutdown signal from stream_job_server
            if shutdown_rx.try_recv().is_ok() {
                info!("Shutdown signal received from stream_job_server, exiting read loop");
                break;
            }

            match reader.read().await {
                Ok(batch) => {
                    if batch.is_empty() {
                        // Data source returned empty batch
                        consecutive_empty_batches += 1;

                        // Check if source has indicated no more data (true EOF)
                        match reader.has_more().await {
                            Ok(has_more) => {
                                if !has_more {
                                    debug!(
                                        "Read loop: Source has_more() = false, exiting read loop"
                                    );
                                    break;
                                }
                            }
                            Err(e) => {
                                log::warn!("Error checking has_more(): {:?}", e);
                            }
                        }

                        // Source says it has more data - check if we've polled too many times
                        if consecutive_empty_batches >= max_empty_batches {
                            debug!(
                                "Read loop: Received {} consecutive empty batches (max: {}), assuming EOF",
                                consecutive_empty_batches, max_empty_batches
                            );
                            break;
                        }

                        // Yield and continue polling
                        tokio::task::yield_now().await;
                        continue;
                    }

                    // Got non-empty batch - reset empty counter
                    consecutive_empty_batches = 0;
                    let batch_size = batch.len();

                    // Route batch to receivers (Phase 6.8 lock-free queue routing)
                    // Records are grouped by partition and pushed to lock-free queues
                    match self.process_batch_for_receivers(batch, &batch_queues).await {
                        Ok(routed_count) => {
                            aggregated_stats.records_processed += routed_count as u64;
                            aggregated_stats.batches_processed += 1;
                            total_routed += routed_count as u64;
                        }
                        Err(e) => {
                            log::warn!("Error routing batch: {:?}", e);
                            aggregated_stats.records_failed += batch_size as u64;
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Error reading batch: {:?}", e);
                    // Continue on error - stop_flag will eventually be set to shutdown
                    consecutive_empty_batches += 1;
                    if consecutive_empty_batches >= max_empty_batches {
                        debug!("Read loop: Reached max error count, exiting");
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
        }

        // Step 4: Signal EOF to all partition receivers
        // This tells each partition receiver to exit once all batches are processed
        for eof_flag in &eof_flags {
            eof_flag.store(true, std::sync::atomic::Ordering::Release);
        }

        // Step 5: Wait for receiver tasks to complete
        // The partition receiver tasks spawned in initialize_partitions_v6_6() will
        // process all queued batches and then exit when EOF is signaled
        // Join on all task handles to ensure they complete
        for handle in task_handles {
            if let Err(e) = handle.await {
                warn!("Partition receiver task failed to complete: {:?}", e);
            }
        }
        debug!("All partition receiver tasks completed");

        // Step 6: Finalize writer
        // Partition receivers process records internally and update per-partition state
        if let Some(writer_arc) = shared_writer {
            let mut w = writer_arc.lock().await;
            let _ = w.flush().await;
            let _ = w.commit().await;
        }

        // Step 7: Finalize reader
        let _ = reader.commit().await;

        aggregated_stats.total_processing_time = start_time.elapsed();

        info!(
            "V2 AdaptiveJobProcessor::process_job: {} completed\n  Records: {} routed in {:?}\n  Phase 6.6 batch-based processing: {} partitions with synchronous receivers (owned engines)",
            job_name,
            total_routed,
            aggregated_stats.total_processing_time,
            self.num_partitions()
        );

        Ok(aggregated_stats)
    }

    async fn process_multi_job(
        &self,
        readers: HashMap<String, Box<dyn DataReader>>,
        writers: HashMap<String, Box<dyn DataWriter>>,
        _engine: Arc<tokio::sync::RwLock<StreamExecutionEngine>>,
        query: StreamingQuery,
        job_name: String,
        _shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<JobExecutionStats, Box<dyn std::error::Error + Send + Sync>> {
        // Phase 6.6: V2 Coordinator-Based Multi-Job Processing with Synchronous Receivers
        //
        // This is the unified API for processing multiple data sources and sinks.
        // It wraps the existing coordinator logic but handles multiple readers/writers.

        info!(
            "V2 AdaptiveJobProcessor::process_multi_job: {} ({} sources, {} sinks)",
            job_name,
            readers.len(),
            writers.len()
        );

        let start_time = std::time::Instant::now();
        let mut aggregated_stats = JobExecutionStats::new();
        let mut writers = writers;

        // Process each reader-writer pair
        for (reader_name, mut reader) in readers.into_iter() {
            // Get the corresponding writer if available
            let writer = writers.remove(&reader_name);

            let query_arc = Arc::new(query.clone());

            // Wrap writer in Arc<Mutex<>> for sharing across partitions
            let shared_writer = writer.map(|w| Arc::new(tokio::sync::Mutex::new(w)));

            let (batch_queues, _metrics, eof_flags, task_handles) =
                self.initialize_partitions_v6_6(query_arc, shared_writer.clone());

            info!(
                "Initialized {} Phase 6.8 partition receivers with lock-free queues for source: {} (job: {})",
                batch_queues.len(),
                reader_name,
                job_name
            );

            // Yield control to allow spawned partition receiver tasks to start
            tokio::task::yield_now().await;

            // Read batches and route to receivers (busy-spin pattern)
            let mut total_routed = 0u64;
            let mut consecutive_empty_batches = 0;
            let max_empty_batches = self.config().empty_batch_count as usize;

            loop {
                // Check if processor stop signal was raised
                if self.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    info!(
                        "Stop signal received, exiting read loop for reader {}",
                        reader_name
                    );
                    break;
                }

                match reader.read().await {
                    Ok(batch) => {
                        if batch.is_empty() {
                            // Data source returned empty batch
                            consecutive_empty_batches += 1;

                            // Check if source has indicated no more data (true EOF)
                            match reader.has_more().await {
                                Ok(has_more) => {
                                    if !has_more {
                                        debug!(
                                            "Read loop (reader {}): Source has_more() = false, exiting",
                                            reader_name
                                        );
                                        break;
                                    }
                                }
                                Err(e) => {
                                    log::warn!(
                                        "Error checking has_more() for {}: {:?}",
                                        reader_name,
                                        e
                                    );
                                }
                            }

                            // Source says it has more data - check if we've polled too many times
                            if consecutive_empty_batches >= max_empty_batches {
                                debug!(
                                    "Read loop (reader {}): Received {} consecutive empty batches (max: {}), assuming EOF",
                                    reader_name, consecutive_empty_batches, max_empty_batches
                                );
                                break;
                            }

                            // Yield and continue polling
                            tokio::task::yield_now().await;
                            continue;
                        }

                        // Got non-empty batch - reset empty counter
                        consecutive_empty_batches = 0;
                        let batch_size = batch.len();

                        // Route batch to receivers (Phase 6.8 lock-free queue routing)
                        match self.process_batch_for_receivers(batch, &batch_queues).await {
                            Ok(routed_count) => {
                                aggregated_stats.records_processed += routed_count as u64;
                                aggregated_stats.batches_processed += 1;
                                total_routed += routed_count as u64;
                            }
                            Err(e) => {
                                log::warn!("Error routing batch: {:?}", e);
                                aggregated_stats.records_failed += batch_size as u64;
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("Error reading batch: {:?}", e);
                        // Continue on error - stop_flag will eventually be set to shutdown
                        consecutive_empty_batches += 1;
                        if consecutive_empty_batches >= max_empty_batches {
                            debug!(
                                "Read loop (reader {}): Reached max error count, exiting",
                                reader_name
                            );
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                }
            }

            // Signal EOF to all partition receivers
            for eof_flag in &eof_flags {
                eof_flag.store(true, std::sync::atomic::Ordering::Release);
            }

            // Wait for receiver tasks to complete by joining on task handles
            for handle in task_handles {
                if let Err(e) = handle.await {
                    warn!("Partition receiver task failed to complete: {:?}", e);
                }
            }
            debug!(
                "All partition receiver tasks completed for source: {}",
                reader_name
            );

            // Finalize writer and reader for this source
            if let Some(writer_arc) = shared_writer {
                let mut w = writer_arc.lock().await;
                let _ = w.flush().await;
                let _ = w.commit().await;
            }
            let _ = reader.commit().await;

            info!(
                "Source {} completed: {} records routed",
                reader_name, total_routed
            );
        }

        aggregated_stats.total_processing_time = start_time.elapsed();

        info!(
            "V2 AdaptiveJobProcessor::process_multi_job: {} completed\n  Records: {}\n  Processing time: {:?}",
            job_name, aggregated_stats.records_processed, aggregated_stats.total_processing_time
        );

        Ok(aggregated_stats)
    }

    async fn stop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use std::sync::atomic::Ordering;
        self.stop_flag.store(true, Ordering::Relaxed);
        info!("AdaptiveJobProcessor (V2) stop signal set");
        Ok(())
    }

    fn get_partition_metrics(&self) -> Vec<Arc<crate::velostream::server::v2::PartitionMetrics>> {
        // TODO: Return actual partition metrics from adaptive processor
        // Currently partition metrics are internal to partition receivers
        Vec::new()
    }
}
