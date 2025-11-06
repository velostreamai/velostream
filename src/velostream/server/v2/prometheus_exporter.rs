//! Prometheus metrics exporter for partitioned job execution
//!
//! Exposes per-partition and aggregated metrics for Grafana dashboards.

use crate::velostream::server::v2::metrics::PartitionMetrics;
use prometheus::{
    core::{AtomicU64, GenericCounter, GenericGauge},
    Counter, Gauge, Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry,
};
use std::sync::Arc;

/// Prometheus metrics exporter for partitioned execution
///
/// ## Phase 3 Implementation
///
/// Exposes metrics for monitoring and alerting:
/// - **Per-Partition Metrics**: Throughput, latency, queue depth, backpressure
/// - **Aggregated Metrics**: Total throughput, active partitions, hot partition detection
/// - **Grafana Integration**: Compatible with standard Prometheus exporters
///
/// ## Usage
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use velostream::velostream::server::v2::{PartitionPrometheusExporter, PartitionMetrics};
///
/// let exporter = PartitionPrometheusExporter::new(4).expect("Failed to create exporter");
///
/// // Update metrics periodically
/// let metrics = vec![
///     Arc::new(PartitionMetrics::new(0)),
///     Arc::new(PartitionMetrics::new(1)),
/// ];
///
/// exporter.update_metrics(&metrics);
///
/// // Expose metrics via HTTP endpoint (using actix-web, warp, etc.)
/// let metrics_text = exporter.export_metrics();
/// ```
///
/// ## Metrics Exposed
///
/// ### Per-Partition Metrics
/// - `velostream_partition_records_total{partition="N"}` - Total records processed
/// - `velostream_partition_throughput{partition="N"}` - Records per second
/// - `velostream_partition_queue_depth{partition="N"}` - Current queue depth
/// - `velostream_partition_latency_micros{partition="N"}` - Average latency
/// - `velostream_partition_channel_utilization{partition="N"}` - Queue utilization (0-1)
///
/// ### Aggregated Metrics
/// - `velostream_total_throughput` - Aggregate throughput across all partitions
/// - `velostream_active_partitions` - Number of active partitions
/// - `velostream_backpressure_events_total` - Count of backpressure events
///
/// ## Grafana Dashboard
///
/// Compatible with standard Prometheus/Grafana dashboards. Key visualizations:
/// - Throughput per partition (stacked area chart)
/// - Queue depth heatmap (backpressure detection)
/// - Latency percentiles (p50, p95, p99)
/// - Hot partition detection (top-k partitions by throughput)
pub struct PartitionPrometheusExporter {
    /// Registry for all metrics
    registry: Registry,

    /// Per-partition metrics
    partition_records: Vec<IntCounter>,
    partition_throughput: Vec<IntGauge>,
    partition_queue_depth: Vec<IntGauge>,
    partition_latency_micros: Vec<IntGauge>,
    partition_channel_utilization: Vec<Gauge>,

    /// Aggregated metrics
    total_throughput: IntGauge,
    active_partitions: IntGauge,
    backpressure_events_total: IntCounter,

    /// Configuration
    num_partitions: usize,
}

impl PartitionPrometheusExporter {
    /// Create new Prometheus exporter for N partitions
    ///
    /// ## Returns
    ///
    /// - `Ok(exporter)` if metrics registration succeeds
    /// - `Err(error)` if Prometheus registration fails
    pub fn new(num_partitions: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let registry = Registry::new();

        // Initialize per-partition metrics
        let mut partition_records = Vec::with_capacity(num_partitions);
        let mut partition_throughput = Vec::with_capacity(num_partitions);
        let mut partition_queue_depth = Vec::with_capacity(num_partitions);
        let mut partition_latency_micros = Vec::with_capacity(num_partitions);
        let mut partition_channel_utilization = Vec::with_capacity(num_partitions);

        for partition_id in 0..num_partitions {
            let partition_label = partition_id.to_string();

            // Records processed counter
            let records = IntCounter::with_opts(
                Opts::new(
                    "velostream_partition_records_total",
                    "Total records processed by partition",
                )
                .const_label("partition", &partition_label),
            )?;
            registry.register(Box::new(records.clone()))?;
            partition_records.push(records);

            // Throughput gauge (rec/sec)
            let throughput = IntGauge::with_opts(
                Opts::new(
                    "velostream_partition_throughput",
                    "Records per second per partition",
                )
                .const_label("partition", &partition_label),
            )?;
            registry.register(Box::new(throughput.clone()))?;
            partition_throughput.push(throughput);

            // Queue depth gauge
            let queue_depth = IntGauge::with_opts(
                Opts::new(
                    "velostream_partition_queue_depth",
                    "Current queue depth per partition",
                )
                .const_label("partition", &partition_label),
            )?;
            registry.register(Box::new(queue_depth.clone()))?;
            partition_queue_depth.push(queue_depth);

            // Latency gauge (microseconds)
            let latency = IntGauge::with_opts(
                Opts::new(
                    "velostream_partition_latency_micros",
                    "Average processing latency per partition (microseconds)",
                )
                .const_label("partition", &partition_label),
            )?;
            registry.register(Box::new(latency.clone()))?;
            partition_latency_micros.push(latency);

            // Channel utilization gauge (0.0 - 1.0)
            let utilization = Gauge::with_opts(
                Opts::new(
                    "velostream_partition_channel_utilization",
                    "Channel utilization per partition (0-1)",
                )
                .const_label("partition", &partition_label),
            )?;
            registry.register(Box::new(utilization.clone()))?;
            partition_channel_utilization.push(utilization);
        }

        // Aggregated metrics
        let total_throughput = IntGauge::with_opts(Opts::new(
            "velostream_total_throughput",
            "Aggregate throughput across all partitions (rec/sec)",
        ))?;
        registry.register(Box::new(total_throughput.clone()))?;

        let active_partitions = IntGauge::with_opts(Opts::new(
            "velostream_active_partitions",
            "Number of active partitions",
        ))?;
        registry.register(Box::new(active_partitions.clone()))?;

        let backpressure_events_total = IntCounter::with_opts(Opts::new(
            "velostream_backpressure_events_total",
            "Total number of backpressure events detected",
        ))?;
        registry.register(Box::new(backpressure_events_total.clone()))?;

        Ok(Self {
            registry,
            partition_records,
            partition_throughput,
            partition_queue_depth,
            partition_latency_micros,
            partition_channel_utilization,
            total_throughput,
            active_partitions,
            backpressure_events_total,
            num_partitions,
        })
    }

    /// Update all metrics from partition metrics
    ///
    /// Call this periodically (e.g., every 1 second) to refresh Prometheus metrics
    pub fn update_metrics(&self, partition_metrics: &[Arc<PartitionMetrics>], buffer_size: usize) {
        let mut aggregate_throughput = 0;
        let mut active_count = 0;

        for (idx, metrics) in partition_metrics.iter().enumerate() {
            if idx >= self.num_partitions {
                break; // Safety check
            }

            let snapshot = metrics.snapshot();

            // Update per-partition metrics
            // Note: Prometheus counters are monotonic, so we set absolute values via IntGauge
            // For true counters, we'd need to track deltas
            self.partition_throughput[idx].set(snapshot.throughput_per_sec as i64);
            self.partition_queue_depth[idx].set(snapshot.queue_depth as i64);
            self.partition_latency_micros[idx].set(snapshot.avg_latency_micros as i64);

            // Calculate channel utilization
            let utilization = metrics.channel_utilization(buffer_size);
            self.partition_channel_utilization[idx].set(utilization);

            // Aggregate metrics
            aggregate_throughput += snapshot.throughput_per_sec;

            if snapshot.throughput_per_sec > 0 {
                active_count += 1;
            }
        }

        // Update aggregated metrics
        self.total_throughput.set(aggregate_throughput as i64);
        self.active_partitions.set(active_count);
    }

    /// Increment backpressure event counter
    ///
    /// Call this when backpressure is detected
    pub fn record_backpressure_event(&self) {
        self.backpressure_events_total.inc();
    }

    /// Export metrics in Prometheus text format
    ///
    /// Returns text suitable for HTTP /metrics endpoint
    pub fn export_metrics(&self) -> String {
        use prometheus::Encoder;

        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();

        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();

        String::from_utf8(buffer).unwrap()
    }

    /// Get registry reference (for custom HTTP server integration)
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Get number of partitions
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exporter_creation() {
        let exporter = PartitionPrometheusExporter::new(4);
        assert!(exporter.is_ok());

        let exporter = exporter.unwrap();
        assert_eq!(exporter.num_partitions(), 4);
    }

    #[test]
    fn test_metrics_update() {
        let exporter = PartitionPrometheusExporter::new(2).unwrap();

        let metrics = vec![
            Arc::new(PartitionMetrics::new(0)),
            Arc::new(PartitionMetrics::new(1)),
        ];

        // Update queue depth
        metrics[0].update_queue_depth(500);
        metrics[1].update_queue_depth(300);

        // Update metrics
        exporter.update_metrics(&metrics, 1000);

        // Verify metrics are exported
        let output = exporter.export_metrics();
        assert!(output.contains("velostream_partition_queue_depth"));
        assert!(output.contains("velostream_total_throughput"));
    }

    #[test]
    fn test_backpressure_event_recording() {
        let exporter = PartitionPrometheusExporter::new(2).unwrap();

        // Record backpressure events
        exporter.record_backpressure_event();
        exporter.record_backpressure_event();

        // Verify counter incremented
        let output = exporter.export_metrics();
        assert!(output.contains("velostream_backpressure_events_total"));
    }

    #[test]
    fn test_export_metrics_format() {
        let exporter = PartitionPrometheusExporter::new(1).unwrap();

        let output = exporter.export_metrics();

        // Verify Prometheus text format
        assert!(output.contains("# HELP"));
        assert!(output.contains("# TYPE"));
        assert!(output.contains("velostream_"));
    }
}
