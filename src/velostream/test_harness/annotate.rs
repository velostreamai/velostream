//! SQL Annotation Generator
//!
//! Analyzes SQL files and generates:
//! - Annotation templates with suggested metrics, observability, and deployment configs
//! - Monitoring infrastructure (Prometheus, Grafana dashboards, Tempo tracing)
//!
//! # Usage
//!
//! ```bash
//! # Generate annotation templates for SQL file
//! velo-test annotate app.sql --output annotated_app.sql
//!
//! # Generate monitoring infrastructure
//! velo-test annotate app.sql --monitoring ./monitoring
//! ```

use crate::velostream::sql::ast::{Expr, SelectField, StreamSource, StreamingQuery, WindowSpec};
use crate::velostream::sql::parser::StreamingSqlParser;
use serde_json::{Value as JsonValue, json};
use std::collections::HashSet;
use std::path::Path;

/// Configuration for annotation generation
#[derive(Debug, Clone)]
pub struct AnnotateConfig {
    /// Application name (defaults to SQL filename)
    pub app_name: String,
    /// Application version
    pub version: String,
    /// Generate monitoring infrastructure
    pub generate_monitoring: bool,
    /// Monitoring output directory
    pub monitoring_dir: Option<String>,
    /// Prometheus scrape port
    pub prometheus_port: u16,
    /// Telemetry port
    pub telemetry_port: u16,
}

impl Default for AnnotateConfig {
    fn default() -> Self {
        Self {
            app_name: "my_app".to_string(),
            version: "1.0.0".to_string(),
            generate_monitoring: false,
            monitoring_dir: None,
            prometheus_port: 8080,
            telemetry_port: 9091,
        }
    }
}

/// Detected metric from SQL analysis
#[derive(Debug, Clone)]
pub struct DetectedMetric {
    /// Metric name (Prometheus format)
    pub name: String,
    /// Metric type: counter, gauge, histogram
    pub metric_type: MetricType,
    /// Help text
    pub help: String,
    /// Label fields from SQL
    pub labels: Vec<String>,
    /// Field to measure (for gauge/histogram)
    pub field: Option<String>,
    /// Histogram buckets (for histogram)
    pub buckets: Option<Vec<f64>>,
    /// Source query name
    pub query_name: String,
}

/// Prometheus metric type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

impl MetricType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
        }
    }

    pub fn options_comment(&self) -> &'static str {
        "  # Options: counter, gauge, histogram"
    }
}

/// Analysis result from SQL file
#[derive(Debug, Clone)]
pub struct SqlAnalysis {
    /// Detected queries
    pub queries: Vec<QueryAnalysis>,
    /// All detected metrics
    pub metrics: Vec<DetectedMetric>,
    /// Source topics/streams
    pub sources: HashSet<String>,
    /// Sink topics/streams
    pub sinks: HashSet<String>,
    /// Has window operations
    pub has_windows: bool,
    /// Has aggregations
    pub has_aggregations: bool,
    /// Has joins
    pub has_joins: bool,
    /// Detected GROUP BY fields (good for labels)
    pub group_by_fields: HashSet<String>,
    /// Detected numeric fields (good for gauges)
    pub numeric_fields: HashSet<String>,
}

/// Analysis of a single query
#[derive(Debug, Clone)]
pub struct QueryAnalysis {
    /// Query name (from CREATE STREAM/TABLE)
    pub name: String,
    /// Query type
    pub query_type: QueryType,
    /// Has COUNT aggregation
    pub has_count: bool,
    /// Has SUM aggregation
    pub has_sum: bool,
    /// Has AVG aggregation
    pub has_avg: bool,
    /// Has MIN/MAX aggregation
    pub has_minmax: bool,
    /// Has window specification
    pub has_window: bool,
    /// Window size in seconds (if detected)
    pub window_size_secs: Option<u64>,
    /// GROUP BY fields
    pub group_by: Vec<String>,
    /// SELECT fields
    pub select_fields: Vec<String>,
    /// Numeric fields in SELECT
    pub numeric_select_fields: Vec<String>,
}

/// Query type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    CreateStream,
    CreateTable,
    Select,
}

/// SQL Annotation Generator
pub struct Annotator {
    config: AnnotateConfig,
}

impl Annotator {
    /// Create new annotator with config
    pub fn new(config: AnnotateConfig) -> Self {
        Self { config }
    }

    /// Analyze SQL content and return analysis
    pub fn analyze(&self, sql_content: &str) -> Result<SqlAnalysis, String> {
        let parser = StreamingSqlParser::new();
        let mut analysis = SqlAnalysis {
            queries: Vec::new(),
            metrics: Vec::new(),
            sources: HashSet::new(),
            sinks: HashSet::new(),
            has_windows: false,
            has_aggregations: false,
            has_joins: false,
            group_by_fields: HashSet::new(),
            numeric_fields: HashSet::new(),
        };

        // Split by semicolons
        for statement in sql_content.split(';') {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Strip leading comments to get the actual SQL
            let sql_without_leading_comments = Self::strip_leading_comments(trimmed);
            if sql_without_leading_comments.is_empty() {
                continue;
            }

            match parser.parse(trimmed) {
                Ok(query) => {
                    let query_analysis = self.analyze_query(&query);

                    // Update global analysis
                    if query_analysis.has_window {
                        analysis.has_windows = true;
                    }
                    if query_analysis.has_count
                        || query_analysis.has_sum
                        || query_analysis.has_avg
                        || query_analysis.has_minmax
                    {
                        analysis.has_aggregations = true;
                    }

                    // Collect GROUP BY fields
                    for field in &query_analysis.group_by {
                        analysis.group_by_fields.insert(field.clone());
                    }

                    // Collect numeric fields
                    for field in &query_analysis.numeric_select_fields {
                        analysis.numeric_fields.insert(field.clone());
                    }

                    // Extract sources and sinks
                    if let Some(source) = self.extract_source(&query) {
                        analysis.sources.insert(source);
                    }
                    if !query_analysis.name.is_empty() {
                        analysis.sinks.insert(query_analysis.name.clone());
                    }

                    // Check for joins
                    if self.has_join(&query) {
                        analysis.has_joins = true;
                    }

                    // Generate metrics for this query
                    let metrics = self.generate_metrics_for_query(&query_analysis);
                    analysis.metrics.extend(metrics);

                    analysis.queries.push(query_analysis);
                }
                Err(e) => {
                    log::debug!("Failed to parse statement: {}", e);
                }
            }
        }

        Ok(analysis)
    }

    /// Analyze a single query
    fn analyze_query(&self, query: &StreamingQuery) -> QueryAnalysis {
        let mut analysis = QueryAnalysis {
            name: String::new(),
            query_type: QueryType::Select,
            has_count: false,
            has_sum: false,
            has_avg: false,
            has_minmax: false,
            has_window: false,
            window_size_secs: None,
            group_by: Vec::new(),
            select_fields: Vec::new(),
            numeric_select_fields: Vec::new(),
        };

        // Extract info based on query type
        match query {
            StreamingQuery::CreateStream {
                name, as_select, ..
            } => {
                analysis.name = name.clone();
                analysis.query_type = QueryType::CreateStream;
                // Recursively analyze the inner SELECT
                let inner = self.analyze_query(as_select);
                analysis.has_count = inner.has_count;
                analysis.has_sum = inner.has_sum;
                analysis.has_avg = inner.has_avg;
                analysis.has_minmax = inner.has_minmax;
                analysis.has_window = inner.has_window;
                analysis.window_size_secs = inner.window_size_secs;
                analysis.group_by = inner.group_by;
                analysis.select_fields = inner.select_fields;
                analysis.numeric_select_fields = inner.numeric_select_fields;
            }
            StreamingQuery::CreateTable {
                name, as_select, ..
            } => {
                analysis.name = name.clone();
                analysis.query_type = QueryType::CreateTable;
                // Recursively analyze the inner SELECT
                let inner = self.analyze_query(as_select);
                analysis.has_count = inner.has_count;
                analysis.has_sum = inner.has_sum;
                analysis.has_avg = inner.has_avg;
                analysis.has_minmax = inner.has_minmax;
                analysis.has_window = inner.has_window;
                analysis.window_size_secs = inner.window_size_secs;
                analysis.group_by = inner.group_by;
                analysis.select_fields = inner.select_fields;
                analysis.numeric_select_fields = inner.numeric_select_fields;
            }
            StreamingQuery::Select {
                fields,
                group_by,
                window,
                ..
            } => {
                // Analyze SELECT fields
                for field in fields {
                    let field_name = self.get_field_name(field);
                    analysis.select_fields.push(field_name.clone());

                    // Check for aggregation functions
                    if let SelectField::Expression { expr, .. } = field {
                        if self.is_count_expr(expr) {
                            analysis.has_count = true;
                        }
                        if self.is_sum_expr(expr) {
                            analysis.has_sum = true;
                        }
                        if self.is_avg_expr(expr) {
                            analysis.has_avg = true;
                        }
                        if self.is_minmax_expr(expr) {
                            analysis.has_minmax = true;
                        }

                        // Detect numeric fields (price, volume, amount, etc.)
                        if self.is_likely_numeric_field(&field_name) {
                            analysis.numeric_select_fields.push(field_name);
                        }
                    }
                }

                // Analyze GROUP BY
                if let Some(group_exprs) = group_by {
                    for group_expr in group_exprs {
                        if let Expr::Column(name) = group_expr {
                            analysis.group_by.push(name.clone());
                        }
                    }
                }

                // Analyze window
                if window.is_some() {
                    analysis.has_window = true;
                    analysis.window_size_secs = self.extract_window_size(window);
                }
            }
            _ => {
                // Other query types (SHOW, START JOB, etc.) - no analysis needed
            }
        }

        analysis
    }

    /// Sanitize a field name for use in a Prometheus metric name
    /// Prometheus metric names only allow [a-zA-Z_:][a-zA-Z0-9_:]*
    fn sanitize_metric_name_part(&self, name: &str) -> String {
        name.chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '_' || c == ':' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }

    /// Generate metrics for a query based on analysis
    fn generate_metrics_for_query(&self, query: &QueryAnalysis) -> Vec<DetectedMetric> {
        let mut metrics = Vec::new();
        let prefix = format!("velo_{}", self.config.app_name.replace('-', "_"));
        let query_name = self.sanitize_metric_name_part(&query.name);

        // Always generate a records counter for each query
        metrics.push(DetectedMetric {
            name: format!("{}_{}_records_total", prefix, query_name),
            metric_type: MetricType::Counter,
            help: format!("Total records processed by {}", query.name),
            labels: query.group_by.clone(),
            field: None,
            buckets: None,
            query_name: query.name.clone(),
        });

        // If has COUNT aggregation, suggest counter for count results
        if query.has_count {
            metrics.push(DetectedMetric {
                name: format!("{}_{}_count", prefix, query_name),
                metric_type: MetricType::Counter,
                help: format!("Count aggregation from {}", query.name),
                labels: query.group_by.clone(),
                field: Some("count".to_string()),
                buckets: None,
                query_name: query.name.clone(),
            });
        }

        // If has SUM/AVG, suggest gauge for aggregated values
        if query.has_sum || query.has_avg {
            for field in &query.numeric_select_fields {
                let sanitized_field = self.sanitize_metric_name_part(field);
                metrics.push(DetectedMetric {
                    name: format!("{}_{}_{}", prefix, query_name, sanitized_field),
                    metric_type: MetricType::Gauge,
                    help: format!("Current {} from {}", field, query.name),
                    labels: query.group_by.clone(),
                    field: Some(field.clone()),
                    buckets: None,
                    query_name: query.name.clone(),
                });
            }
        }

        // If has window with latency-like fields, suggest histogram
        if query.has_window {
            for field in &query.numeric_select_fields {
                if self.is_latency_field(field) {
                    let sanitized_field = self.sanitize_metric_name_part(field);
                    metrics.push(DetectedMetric {
                        name: format!("{}_{}_{}_seconds", prefix, query_name, sanitized_field),
                        metric_type: MetricType::Histogram,
                        help: format!("Distribution of {} from {}", field, query.name),
                        labels: query.group_by.clone(),
                        field: Some(field.clone()),
                        buckets: Some(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
                        query_name: query.name.clone(),
                    });
                }
            }
        }

        // If query has price-like fields, suggest gauge
        for field in &query.numeric_select_fields {
            if self.is_price_field(field)
                && !metrics.iter().any(|m| m.field.as_ref() == Some(field))
            {
                let sanitized_field = self.sanitize_metric_name_part(field);
                metrics.push(DetectedMetric {
                    name: format!("{}_current_{}", prefix, sanitized_field),
                    metric_type: MetricType::Gauge,
                    help: format!("Current {} value", field),
                    labels: query.group_by.clone(),
                    field: Some(field.clone()),
                    buckets: None,
                    query_name: query.name.clone(),
                });
            }
        }

        metrics
    }

    /// Generate annotated SQL content
    pub fn generate_annotated_sql(
        &self,
        original_sql: &str,
        analysis: &SqlAnalysis,
    ) -> Result<String, String> {
        let mut output = String::new();

        // Generate header annotations
        output.push_str(&self.generate_header_annotations(analysis));
        output.push('\n');

        // Generate deployment annotations
        output.push_str(&self.generate_deployment_annotations());
        output.push('\n');

        // Generate observability annotations
        output.push_str(&self.generate_observability_annotations(analysis));
        output.push('\n');

        // Generate job processing annotations
        output.push_str(&self.generate_job_annotations(analysis));
        output.push('\n');

        // Generate SLA annotations (metrics are now placed before each query, not in header)
        output.push_str(&self.generate_sla_annotations(analysis));
        output.push('\n');

        // Add separator
        output.push_str(
            "-- =============================================================================\n",
        );
        output.push_str("-- SQL QUERIES\n");
        output.push_str(
            "-- =============================================================================\n\n",
        );

        // Insert metrics before each CREATE STREAM/TABLE statement
        output.push_str(&self.insert_metrics_before_queries(original_sql, analysis));

        Ok(output)
    }

    /// Insert metric annotations immediately before each CREATE STREAM/TABLE statement
    fn insert_metrics_before_queries(&self, original_sql: &str, analysis: &SqlAnalysis) -> String {
        // Group metrics by query_name
        let mut metrics_by_query: std::collections::HashMap<String, Vec<&DetectedMetric>> =
            std::collections::HashMap::new();
        for metric in &analysis.metrics {
            metrics_by_query
                .entry(metric.query_name.clone())
                .or_default()
                .push(metric);
        }

        let mut result = String::new();
        let mut current_pos = 0;
        let sql_upper = original_sql.to_uppercase();

        // Find all CREATE STREAM and CREATE TABLE positions
        let create_patterns = ["CREATE STREAM", "CREATE TABLE"];

        // Collect all CREATE positions with their query names
        let mut create_positions: Vec<(usize, String)> = Vec::new();

        for pattern in &create_patterns {
            let mut search_pos = 0;
            while let Some(pos) = sql_upper[search_pos..].find(pattern) {
                let absolute_pos = search_pos + pos;

                // Extract the query name (word after CREATE STREAM/TABLE)
                let after_pattern = &original_sql[absolute_pos + pattern.len()..];
                let query_name = after_pattern
                    .trim_start()
                    .split(|c: char| !c.is_alphanumeric() && c != '_')
                    .next()
                    .unwrap_or("")
                    .to_string();

                if !query_name.is_empty() {
                    create_positions.push((absolute_pos, query_name));
                }

                search_pos = absolute_pos + pattern.len();
            }
        }

        // Sort by position
        create_positions.sort_by_key(|(pos, _)| *pos);

        // Build result with metrics inserted before each CREATE
        for (create_pos, query_name) in create_positions {
            // Add SQL content before this CREATE statement
            result.push_str(&original_sql[current_pos..create_pos]);

            // Add metrics for this query if any exist
            if let Some(metrics) = metrics_by_query.get(&query_name) {
                result.push_str(&self.format_metrics_for_query(metrics, &query_name));
            }

            current_pos = create_pos;
        }

        // Add remaining SQL after the last CREATE
        result.push_str(&original_sql[current_pos..]);

        result
    }

    /// Format metrics as SQL comments for a specific query
    /// Note: Comments must NOT appear on the same line as annotation values
    /// because the parser reads the full value after `: ` including any comments
    fn format_metrics_for_query(&self, metrics: &[&DetectedMetric], query_name: &str) -> String {
        let mut output = String::new();
        output.push_str(
            "-- -----------------------------------------------------------------------------\n",
        );
        output.push_str(&format!("-- METRICS for {}\n", query_name));
        output.push_str(
            "-- -----------------------------------------------------------------------------\n",
        );

        for metric in metrics {
            output.push_str(&format!("-- @metric: {}\n", metric.name));
            output.push_str(&format!(
                "-- @metric_type: {}\n",
                metric.metric_type.as_str()
            ));
            output.push_str(&format!("-- @metric_help: \"{}\"\n", metric.help));

            if !metric.labels.is_empty() {
                output.push_str(&format!(
                    "-- @metric_labels: {}\n",
                    metric.labels.join(", ")
                ));
            }

            if let Some(ref field) = metric.field {
                output.push_str(&format!("-- @metric_field: {}\n", field));
            }

            if let Some(ref buckets) = metric.buckets {
                let bucket_str = buckets
                    .iter()
                    .map(|b| b.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                output.push_str(&format!("-- @metric_buckets: [{}]\n", bucket_str));
            }
            output.push_str("--\n");
        }

        output
    }

    /// Generate header annotations
    fn generate_header_annotations(&self, _analysis: &SqlAnalysis) -> String {
        format!(
            r#"-- =============================================================================
-- APPLICATION: {}
-- =============================================================================
-- @app: {}  # Application identifier
-- @version: {}  # Semantic version
-- @description: TODO - Describe your application  # Human-readable description
-- @phase: development  # Options: development, staging, production
"#,
            self.config.app_name, self.config.app_name, self.config.version
        )
    }

    /// Generate deployment annotations
    fn generate_deployment_annotations(&self) -> String {
        format!(
            r#"--
-- DEPLOYMENT CONTEXT
-- =============================================================================
-- @deployment.node_id: ${{POD_NAME:{}-1}}  # Unique node identifier (supports env vars)
-- @deployment.node_name: {} Platform  # Human-readable node name
-- @deployment.region: ${{AWS_REGION:us-east-1}}  # Deployment region
"#,
            self.config.app_name, self.config.app_name
        )
    }

    /// Generate observability annotations
    fn generate_observability_annotations(&self, analysis: &SqlAnalysis) -> String {
        let metrics_enabled = !analysis.metrics.is_empty();
        let tracing_enabled = analysis.has_joins || analysis.has_windows;
        let profiling = if analysis.has_aggregations {
            "prod"
        } else {
            "off"
        };

        format!(
            r#"--
-- OBSERVABILITY
-- =============================================================================
-- @observability.metrics.enabled: {}  # Enable Prometheus metrics collection
-- @observability.tracing.enabled: {}  # Enable distributed tracing (OpenTelemetry)
-- @observability.profiling.enabled: {}  # Options: off, dev (8-10% overhead), prod (2-3% overhead)
-- @observability.error_reporting.enabled: true  # Enable structured error reporting
"#,
            metrics_enabled, tracing_enabled, profiling
        )
    }

    /// Generate job processing annotations
    fn generate_job_annotations(&self, analysis: &SqlAnalysis) -> String {
        // Recommend job mode based on query complexity
        let job_mode = if analysis.has_aggregations || analysis.has_windows {
            "adaptive"
        } else {
            "simple"
        };

        let batch_size = if analysis.has_aggregations { 1000 } else { 100 };

        let partitioning = if !analysis.group_by_fields.is_empty() {
            "hash"
        } else {
            "sticky"
        };

        format!(
            r#"--
-- JOB PROCESSING
-- =============================================================================
-- @job_mode: {}  # Options: simple (low latency), transactional (exactly-once), adaptive (parallel)
-- @batch_size: {}  # Records per batch (higher = throughput, lower = latency)
-- @num_partitions: 8  # Parallel partitions for adaptive mode (default: CPU cores)
-- @partitioning_strategy: {}  # Options: sticky, hash, smart, roundrobin, fanin
"#,
            job_mode, batch_size, partitioning
        )
    }

    /// Generate metric annotations
    fn generate_metric_annotations(&self, analysis: &SqlAnalysis) -> String {
        let mut output = String::new();
        output.push_str("--\n");
        output.push_str("-- METRICS (SQL-Native Prometheus Integration)\n");
        output.push_str(
            "-- =============================================================================\n",
        );

        // Generate up to 5 most relevant metrics as examples
        for (i, metric) in analysis.metrics.iter().take(5).enumerate() {
            if i > 0 {
                output.push_str("--\n");
            }

            output.push_str(&format!("-- @metric: {}\n", metric.name));
            output.push_str(&format!(
                "-- @metric_type: {}{}\n",
                metric.metric_type.as_str(),
                metric.metric_type.options_comment()
            ));
            output.push_str(&format!("-- @metric_help: \"{}\"\n", metric.help));

            if !metric.labels.is_empty() {
                output.push_str(&format!(
                    "-- @metric_labels: {}  # Fields used as Prometheus labels\n",
                    metric.labels.join(", ")
                ));
            }

            if let Some(ref field) = metric.field {
                output.push_str(&format!(
                    "-- @metric_field: {}  # Field to measure (required for gauge/histogram)\n",
                    field
                ));
            }

            if let Some(ref buckets) = metric.buckets {
                output.push_str(&format!(
                    "-- @metric_buckets: {:?}  # Histogram bucket boundaries\n",
                    buckets
                ));
            }
        }

        if analysis.metrics.len() > 5 {
            output.push_str(&format!(
                "--\n-- ... and {} more metrics (see monitoring/grafana/dashboards for full list)\n",
                analysis.metrics.len() - 5
            ));
        }

        output
    }

    /// Generate SLA annotations
    fn generate_sla_annotations(&self, analysis: &SqlAnalysis) -> String {
        // Calculate suggested latency based on window size
        let latency = if let Some(query) = analysis.queries.first() {
            if let Some(window_secs) = query.window_size_secs {
                format!("{}ms", (window_secs * 100).min(1000))
            } else if analysis.has_aggregations {
                "100ms".to_string()
            } else {
                "50ms".to_string()
            }
        } else {
            "100ms".to_string()
        };

        format!(
            r#"--
-- SLA & GOVERNANCE
-- =============================================================================
-- @sla.latency.p99: {}  # Expected P99 latency target
-- @sla.availability: 99.9%  # Availability target
-- @data_retention: 7d  # Data retention policy
-- @compliance: []  # Compliance requirements (e.g., SOX, GDPR, PCI-DSS)
"#,
            latency
        )
    }

    /// Generate monitoring infrastructure
    pub fn generate_monitoring(
        &self,
        analysis: &SqlAnalysis,
        output_dir: &Path,
    ) -> Result<(), String> {
        // Create directory structure
        let grafana_dashboards = output_dir.join("grafana/dashboards");
        let grafana_provisioning_dashboards = output_dir.join("grafana/provisioning/dashboards");
        let grafana_provisioning_datasources = output_dir.join("grafana/provisioning/datasources");
        let tempo_dir = output_dir.join("tempo");

        std::fs::create_dir_all(&grafana_dashboards)
            .map_err(|e| format!("Failed to create dashboards dir: {}", e))?;
        std::fs::create_dir_all(&grafana_provisioning_dashboards)
            .map_err(|e| format!("Failed to create provisioning/dashboards dir: {}", e))?;
        std::fs::create_dir_all(&grafana_provisioning_datasources)
            .map_err(|e| format!("Failed to create provisioning/datasources dir: {}", e))?;
        std::fs::create_dir_all(&tempo_dir)
            .map_err(|e| format!("Failed to create tempo dir: {}", e))?;

        // Generate prometheus.yml
        let prometheus_yml = self.generate_prometheus_yml();
        std::fs::write(output_dir.join("prometheus.yml"), prometheus_yml)
            .map_err(|e| format!("Failed to write prometheus.yml: {}", e))?;

        // Generate datasources
        let prometheus_datasource = self.generate_prometheus_datasource();
        std::fs::write(
            grafana_provisioning_datasources.join("prometheus.yml"),
            prometheus_datasource,
        )
        .map_err(|e| format!("Failed to write prometheus datasource: {}", e))?;

        let tempo_datasource = self.generate_tempo_datasource();
        std::fs::write(
            grafana_provisioning_datasources.join("tempo.yml"),
            tempo_datasource,
        )
        .map_err(|e| format!("Failed to write tempo datasource: {}", e))?;

        // Generate dashboard provisioning config
        let dashboard_yml = self.generate_dashboard_provisioning();
        std::fs::write(
            grafana_provisioning_dashboards.join("dashboard.yml"),
            &dashboard_yml,
        )
        .map_err(|e| format!("Failed to write dashboard.yml: {}", e))?;

        // Also copy to dashboards dir for direct access
        std::fs::write(grafana_dashboards.join("dashboard.yml"), &dashboard_yml)
            .map_err(|e| format!("Failed to write dashboard.yml to dashboards: {}", e))?;

        // Generate main dashboard JSON
        let dashboard_json = self.generate_dashboard_json(analysis);
        let dashboard_filename = format!("{}-dashboard.json", self.config.app_name);
        std::fs::write(
            grafana_dashboards.join(&dashboard_filename),
            serde_json::to_string_pretty(&dashboard_json).unwrap(),
        )
        .map_err(|e| format!("Failed to write dashboard JSON: {}", e))?;

        // Generate tempo config
        let tempo_yml = self.generate_tempo_yml();
        std::fs::write(tempo_dir.join("tempo.yaml"), tempo_yml)
            .map_err(|e| format!("Failed to write tempo.yaml: {}", e))?;

        Ok(())
    }

    /// Generate prometheus.yml
    fn generate_prometheus_yml(&self) -> String {
        format!(
            r#"global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  # - "first_rules.yml"
  # - "second_rules.yml"

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9092']
    metrics_path: /metrics
    scrape_interval: 10s

  - job_name: 'velo-sql'
    static_configs:
      - targets: ['host.docker.internal:{}']
    metrics_path: /metrics
    scrape_interval: 10s
    scrape_timeout: 5s

  - job_name: 'velo-sql-telemetry'
    static_configs:
      - targets: ['host.docker.internal:{}']
    metrics_path: /metrics
    scrape_interval: 5s
    scrape_timeout: 3s

  - job_name: 'node-exporter'
    static_configs:
      - targets: ['host.docker.internal:9100']
    scrape_interval: 10s
"#,
            self.config.prometheus_port, self.config.telemetry_port
        )
    }

    /// Generate Prometheus datasource config
    fn generate_prometheus_datasource(&self) -> String {
        r#"apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
"#
        .to_string()
    }

    /// Generate Tempo datasource config
    fn generate_tempo_datasource(&self) -> String {
        r#"apiVersion: 1

datasources:
  - name: Tempo
    type: tempo
    access: proxy
    uid: tempo
    url: http://tempo:3200
    jsonData:
      httpMethod: GET
      tracesToLogs:
        datasourceUid: 'prometheus'
      tracesToMetrics:
        datasourceUid: 'prometheus'
      serviceMap:
        datasourceUid: 'prometheus'
      nodeGraph:
        enabled: true
      search:
        hide: false
      lokiSearch:
        datasourceUid: 'prometheus'
"#
        .to_string()
    }

    /// Generate dashboard provisioning config
    fn generate_dashboard_provisioning(&self) -> String {
        format!(
            r#"apiVersion: 1

providers:
  - name: '{}'
    orgId: 1
    folder: 'Velostream'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards
"#,
            self.config.app_name
        )
    }

    /// Generate Tempo tracing config
    fn generate_tempo_yml(&self) -> String {
        format!(
            r#"server:
  http_listen_port: 3200

distributor:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317
        http:
          endpoint: 0.0.0.0:4318
    zipkin:
      endpoint: 0.0.0.0:9411

ingester:
  max_block_duration: 5m

compactor:
  compaction:
    block_retention: 1h

metrics_generator:
  registry:
    external_labels:
      source: tempo
      cluster: velostream-{}
  storage:
    path: /tmp/tempo/generator/wal
    remote_write:
      - url: http://prometheus:9090/api/v1/write
        send_exemplars: true
  traces_storage:
    path: /tmp/tempo/generator/traces
  processor:
    service_graphs:
      dimensions:
        - service.name
        - service.namespace
    span_metrics:
      dimensions:
        - service.name
        - span.name
        - span.kind
        - status.code
      enable_target_info: true

storage:
  trace:
    backend: local
    wal:
      path: /tmp/tempo/wal
    local:
      path: /tmp/tempo/blocks

overrides:
  metrics_generator_processors:
    - service-graphs
    - span-metrics
"#,
            self.config.app_name
        )
    }

    /// Generate Grafana dashboard JSON with varied widget types
    fn generate_dashboard_json(&self, analysis: &SqlAnalysis) -> JsonValue {
        let mut panels: Vec<JsonValue> = Vec::new();
        let mut panel_id = 1;
        let mut y_pos = 0;

        // Row 1: Status overview (3 stat panels + 1 gauge)
        // Error status stat
        panels.push(self.create_stat_panel(
            panel_id,
            "🚨 Error Status",
            "velo_sql_query_errors_total OR on() vector(0)",
            0,
            y_pos,
            6,
            4,
        ));
        panel_id += 1;

        // Active jobs stat
        panels.push(self.create_stat_panel(
            panel_id,
            "📊 Active Queries",
            "velo_sql_active_queries OR on() vector(0)",
            6,
            y_pos,
            6,
            4,
        ));
        panel_id += 1;

        // Records processed stat
        panels.push(self.create_stat_panel(
            panel_id,
            "📈 Records Processed",
            "sum(velo_streaming_records_total) OR on() vector(0)",
            12,
            y_pos,
            6,
            4,
        ));
        panel_id += 1;

        // Throughput gauge
        panels.push(self.create_gauge_panel(
            panel_id,
            "⚡ Throughput",
            "sum(rate(velo_streaming_records_total[5m]))",
            18,
            y_pos,
            6,
            4,
            "reqps",
            10000.0,
        ));
        panel_id += 1;
        y_pos += 4;

        // Row 2: Service health timeseries + Records by symbol pie chart
        panels.push(self.create_timeseries_panel(
            panel_id,
            "Service Health Over Time",
            "up",
            0,
            y_pos,
            12,
            8,
            "short",
        ));
        panel_id += 1;

        // Add pie chart for records by query (if we have metrics)
        if !analysis.metrics.is_empty() {
            panels.push(self.create_piechart_panel(
                panel_id,
                "Records Distribution by Query",
                "sum by (query) (velo_query_records_total)",
                12,
                y_pos,
                12,
                8,
            ));
            panel_id += 1;
        } else {
            panels.push(self.create_timeseries_panel(
                panel_id,
                "Records Throughput",
                "rate(velo_streaming_records_total[5m])",
                12,
                y_pos,
                12,
                8,
                "reqps",
            ));
            panel_id += 1;
        }
        y_pos += 8;

        // Row 3: Bar gauge for top metrics by symbol + Latency histogram
        if analysis.metrics.len() >= 2 {
            // Find a counter metric for bar gauge
            let counter_metric = analysis
                .metrics
                .iter()
                .find(|m| matches!(m.metric_type, MetricType::Counter));

            if let Some(metric) = counter_metric {
                panels.push(self.create_bargauge_panel(
                    panel_id,
                    &format!("Top Symbols: {}", metric.help),
                    &format!("topk(5, sum by (symbol) ({}))", metric.name),
                    0,
                    y_pos,
                    12,
                    6,
                    "short",
                ));
                panel_id += 1;
            }

            // Find a histogram or gauge for latency
            let latency_metric = analysis
                .metrics
                .iter()
                .find(|m| matches!(m.metric_type, MetricType::Histogram));

            if let Some(metric) = latency_metric {
                panels.push(self.create_gauge_panel(
                    panel_id,
                    &format!("P95 {}", metric.help),
                    &format!("histogram_quantile(0.95, rate({}_bucket[5m]))", metric.name),
                    12,
                    y_pos,
                    6,
                    6,
                    "s",
                    1.0,
                ));
                panel_id += 1;

                panels.push(self.create_gauge_panel(
                    panel_id,
                    &format!("P99 {}", metric.help),
                    &format!("histogram_quantile(0.99, rate({}_bucket[5m]))", metric.name),
                    18,
                    y_pos,
                    6,
                    6,
                    "s",
                    2.0,
                ));
                panel_id += 1;
            }
            y_pos += 6;
        }

        // Add panels for each detected metric with varied types
        let metrics_to_show: Vec<_> = analysis.metrics.iter().take(12).collect();
        for (i, metric) in metrics_to_show.iter().enumerate() {
            let panel_type = i % 4; // Cycle through different panel types
            let x_pos = (i % 2) * 12;
            if i % 2 == 0 && i > 0 {
                y_pos += 8;
            }

            let expr = match metric.metric_type {
                MetricType::Counter => format!("rate({}[5m])", metric.name),
                MetricType::Gauge => metric.name.clone(),
                MetricType::Histogram => {
                    format!("histogram_quantile(0.95, rate({}_bucket[5m]))", metric.name)
                }
            };

            let unit = match metric.metric_type {
                MetricType::Counter => "reqps",
                MetricType::Gauge => "short",
                MetricType::Histogram => "s",
            };

            // Vary panel type based on position and metric type
            let panel = match (panel_type, &metric.metric_type) {
                // First in each row: gauge for gauges, timeseries for counters
                (0, MetricType::Gauge) => self.create_gauge_panel(
                    panel_id,
                    &metric.help,
                    &expr,
                    x_pos as i32,
                    y_pos,
                    12,
                    8,
                    unit,
                    1000.0,
                ),
                // Second: bar gauge for counters with symbol grouping
                (1, MetricType::Counter) => self.create_bargauge_panel(
                    panel_id,
                    &metric.help,
                    &format!("topk(5, sum by (symbol) ({}))", metric.name),
                    x_pos as i32,
                    y_pos,
                    12,
                    8,
                    unit,
                ),
                // Third: table for showing raw metric values
                (2, _) if i < 8 => self.create_table_panel(
                    panel_id,
                    &metric.help,
                    &metric.name,
                    x_pos as i32,
                    y_pos,
                    12,
                    8,
                ),
                // Default: timeseries
                _ => self.create_timeseries_panel(
                    panel_id,
                    &metric.help,
                    &expr,
                    x_pos as i32,
                    y_pos,
                    12,
                    8,
                    unit,
                ),
            };
            panels.push(panel);
            panel_id += 1;
        }

        // Build dashboard JSON
        json!({
            "annotations": {
                "list": [{
                    "builtIn": 1,
                    "datasource": {
                        "type": "grafana",
                        "uid": "-- Grafana --"
                    },
                    "enable": true,
                    "hide": true,
                    "iconColor": "rgba(0, 211, 255, 1)",
                    "name": "Annotations & Alerts",
                    "type": "dashboard"
                }]
            },
            "editable": true,
            "fiscalYearStartMonth": 0,
            "graphTooltip": 0,
            "id": null,
            "links": [],
            "liveNow": false,
            "panels": panels,
            "refresh": "5s",
            "schemaVersion": 37,
            "style": "dark",
            "tags": ["velostream", self.config.app_name.clone(), "auto-generated"],
            "templating": {"list": []},
            "time": {
                "from": "now-15m",
                "to": "now"
            },
            "timepicker": {},
            "timezone": "",
            "title": format!("Velostream {}", self.config.app_name),
            "uid": format!("velostream-{}", self.config.app_name),
            "version": 1,
            "weekStart": ""
        })
    }

    /// Create a stat panel
    fn create_stat_panel(
        &self,
        id: i32,
        title: &str,
        expr: &str,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
    ) -> JsonValue {
        json!({
            "datasource": {
                "type": "prometheus",
                "uid": "PBFA97CFB590B2093"
            },
            "description": title,
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "thresholds"},
                    "mappings": [{
                        "options": {
                            "0": {
                                "color": "green",
                                "index": 0,
                                "text": "✅ HEALTHY"
                            }
                        },
                        "type": "value"
                    }],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": null},
                            {"color": "yellow", "value": 1},
                            {"color": "red", "value": 2}
                        ]
                    },
                    "unit": "short"
                },
                "overrides": []
            },
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "id": id,
            "options": {
                "colorMode": "background",
                "graphMode": "none",
                "justifyMode": "center",
                "orientation": "auto",
                "reduceOptions": {
                    "calcs": ["lastNotNull"],
                    "fields": "",
                    "values": false
                },
                "textMode": "value_and_name"
            },
            "pluginVersion": "9.0.0",
            "targets": [{
                "datasource": {
                    "type": "prometheus",
                    "uid": "PBFA97CFB590B2093"
                },
                "expr": expr,
                "interval": "",
                "legendFormat": title,
                "refId": "A"
            }],
            "title": title,
            "type": "stat"
        })
    }

    /// Create a timeseries panel
    fn create_timeseries_panel(
        &self,
        id: i32,
        title: &str,
        expr: &str,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
        unit: &str,
    ) -> JsonValue {
        json!({
            "datasource": {
                "type": "prometheus",
                "uid": "PBFA97CFB590B2093"
            },
            "description": title,
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "palette-classic"},
                    "custom": {
                        "axisLabel": "",
                        "axisPlacement": "auto",
                        "barAlignment": 0,
                        "drawStyle": "line",
                        "fillOpacity": 10,
                        "gradientMode": "none",
                        "hideFrom": {"legend": false, "tooltip": false, "vis": false},
                        "lineInterpolation": "linear",
                        "lineWidth": 1,
                        "pointSize": 5,
                        "scaleDistribution": {"type": "linear"},
                        "showPoints": "never",
                        "spanNulls": false,
                        "stacking": {"group": "A", "mode": "none"},
                        "thresholdsStyle": {"mode": "off"}
                    },
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": null},
                            {"color": "red", "value": 80}
                        ]
                    },
                    "unit": unit
                },
                "overrides": []
            },
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "id": id,
            "options": {
                "legend": {
                    "calcs": [],
                    "displayMode": "list",
                    "placement": "bottom"
                },
                "tooltip": {"mode": "single", "sort": "none"}
            },
            "targets": [{
                "datasource": {
                    "type": "prometheus",
                    "uid": "PBFA97CFB590B2093"
                },
                "expr": expr,
                "interval": "",
                "legendFormat": "{{job}}",
                "refId": "A"
            }],
            "title": title,
            "type": "timeseries"
        })
    }

    /// Create a gauge panel (for current values like throughput, latency)
    fn create_gauge_panel(
        &self,
        id: i32,
        title: &str,
        expr: &str,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
        unit: &str,
        max_value: f64,
    ) -> JsonValue {
        json!({
            "datasource": {
                "type": "prometheus",
                "uid": "PBFA97CFB590B2093"
            },
            "description": title,
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "thresholds"},
                    "mappings": [],
                    "max": max_value,
                    "min": 0,
                    "thresholds": {
                        "mode": "percentage",
                        "steps": [
                            {"color": "green", "value": null},
                            {"color": "yellow", "value": 70},
                            {"color": "red", "value": 90}
                        ]
                    },
                    "unit": unit
                },
                "overrides": []
            },
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "id": id,
            "options": {
                "orientation": "auto",
                "reduceOptions": {
                    "calcs": ["lastNotNull"],
                    "fields": "",
                    "values": false
                },
                "showThresholdLabels": false,
                "showThresholdMarkers": true
            },
            "pluginVersion": "9.0.0",
            "targets": [{
                "datasource": {
                    "type": "prometheus",
                    "uid": "PBFA97CFB590B2093"
                },
                "expr": expr,
                "interval": "",
                "legendFormat": title,
                "refId": "A"
            }],
            "title": title,
            "type": "gauge"
        })
    }

    /// Create a bar gauge panel (for comparisons across labels)
    fn create_bargauge_panel(
        &self,
        id: i32,
        title: &str,
        expr: &str,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
        unit: &str,
    ) -> JsonValue {
        json!({
            "datasource": {
                "type": "prometheus",
                "uid": "PBFA97CFB590B2093"
            },
            "description": title,
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "palette-classic"},
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": null}
                        ]
                    },
                    "unit": unit
                },
                "overrides": []
            },
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "id": id,
            "options": {
                "displayMode": "gradient",
                "minVizHeight": 10,
                "minVizWidth": 0,
                "orientation": "horizontal",
                "reduceOptions": {
                    "calcs": ["lastNotNull"],
                    "fields": "",
                    "values": false
                },
                "showUnfilled": true
            },
            "pluginVersion": "9.0.0",
            "targets": [{
                "datasource": {
                    "type": "prometheus",
                    "uid": "PBFA97CFB590B2093"
                },
                "expr": expr,
                "interval": "",
                "legendFormat": "{{symbol}}",
                "refId": "A"
            }],
            "title": title,
            "type": "bargauge"
        })
    }

    /// Create a pie chart panel (for distributions)
    fn create_piechart_panel(
        &self,
        id: i32,
        title: &str,
        expr: &str,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
    ) -> JsonValue {
        json!({
            "datasource": {
                "type": "prometheus",
                "uid": "PBFA97CFB590B2093"
            },
            "description": title,
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "palette-classic"},
                    "custom": {
                        "hideFrom": {"legend": false, "tooltip": false, "vis": false}
                    },
                    "mappings": []
                },
                "overrides": []
            },
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "id": id,
            "options": {
                "displayLabels": ["name", "percent"],
                "legend": {
                    "displayMode": "table",
                    "placement": "right",
                    "showLegend": true,
                    "values": ["value", "percent"]
                },
                "pieType": "pie",
                "reduceOptions": {
                    "calcs": ["lastNotNull"],
                    "fields": "",
                    "values": false
                },
                "tooltip": {"mode": "single", "sort": "none"}
            },
            "pluginVersion": "9.0.0",
            "targets": [{
                "datasource": {
                    "type": "prometheus",
                    "uid": "PBFA97CFB590B2093"
                },
                "expr": expr,
                "instant": true,
                "interval": "",
                "legendFormat": "{{symbol}}",
                "refId": "A"
            }],
            "title": title,
            "type": "piechart"
        })
    }

    /// Create a table panel (for recent events, alerts)
    fn create_table_panel(
        &self,
        id: i32,
        title: &str,
        expr: &str,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
    ) -> JsonValue {
        json!({
            "datasource": {
                "type": "prometheus",
                "uid": "PBFA97CFB590B2093"
            },
            "description": title,
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "thresholds"},
                    "custom": {
                        "align": "auto",
                        "displayMode": "auto",
                        "filterable": true
                    },
                    "mappings": [],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": null}
                        ]
                    }
                },
                "overrides": []
            },
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "id": id,
            "options": {
                "footer": {
                    "countRows": false,
                    "fields": "",
                    "reducer": ["sum"],
                    "show": false
                },
                "showHeader": true,
                "sortBy": [{"desc": true, "displayName": "Value"}]
            },
            "pluginVersion": "9.0.0",
            "targets": [{
                "datasource": {
                    "type": "prometheus",
                    "uid": "PBFA97CFB590B2093"
                },
                "expr": expr,
                "format": "table",
                "instant": true,
                "interval": "",
                "legendFormat": "",
                "refId": "A"
            }],
            "title": title,
            "transformations": [{
                "id": "organize",
                "options": {
                    "excludeByName": {"Time": true, "__name__": true},
                    "indexByName": {},
                    "renameByName": {"Value": "Count"}
                }
            }],
            "type": "table"
        })
    }

    // Helper methods

    fn get_field_name(&self, field: &SelectField) -> String {
        match field {
            SelectField::Column(name) => name.clone(),
            SelectField::AliasedColumn { alias, .. } => alias.clone(),
            SelectField::Expression { alias, expr } => {
                alias.clone().unwrap_or_else(|| self.expr_to_name(expr))
            }
            SelectField::Wildcard => "*".to_string(),
        }
    }

    fn expr_to_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Column(name) => name.clone(),
            Expr::Function { name, .. } => name.clone(),
            _ => "expr".to_string(),
        }
    }

    /// Strip leading comment lines from SQL to find actual SQL content
    fn strip_leading_comments(sql: &str) -> String {
        let mut result = Vec::new();
        let mut found_sql = false;

        for line in sql.lines() {
            let trimmed = line.trim();
            if found_sql {
                result.push(line);
            } else if !trimmed.is_empty() && !trimmed.starts_with("--") {
                found_sql = true;
                result.push(line);
            }
        }

        result.join("\n")
    }

    fn is_count_expr(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Function { name, .. } if name.to_uppercase() == "COUNT")
    }

    fn is_sum_expr(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Function { name, .. } if name.to_uppercase() == "SUM")
    }

    fn is_avg_expr(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Function { name, .. } if name.to_uppercase() == "AVG")
    }

    fn is_minmax_expr(&self, expr: &Expr) -> bool {
        matches!(expr, Expr::Function { name, .. } if name.to_uppercase() == "MIN" || name.to_uppercase() == "MAX")
    }

    fn is_likely_numeric_field(&self, name: &str) -> bool {
        let lower = name.to_lowercase();
        lower.contains("price")
            || lower.contains("amount")
            || lower.contains("volume")
            || lower.contains("quantity")
            || lower.contains("count")
            || lower.contains("total")
            || lower.contains("sum")
            || lower.contains("avg")
            || lower.contains("rate")
            || lower.contains("latency")
            || lower.contains("duration")
            || lower.ends_with("_ms")
            || lower.ends_with("_seconds")
    }

    fn is_latency_field(&self, name: &str) -> bool {
        let lower = name.to_lowercase();
        lower.contains("latency")
            || lower.contains("duration")
            || lower.contains("time")
            || lower.ends_with("_ms")
            || lower.ends_with("_seconds")
    }

    fn is_price_field(&self, name: &str) -> bool {
        let lower = name.to_lowercase();
        lower.contains("price")
            || lower.contains("amount")
            || lower.contains("value")
            || lower.contains("cost")
    }

    fn extract_source(&self, query: &StreamingQuery) -> Option<String> {
        match query {
            StreamingQuery::Select { from, .. } => Some(match from {
                StreamSource::Stream(name) => name.clone(),
                StreamSource::Table(name) => name.clone(),
                StreamSource::Uri(uri) => uri.clone(),
                StreamSource::Subquery(_) => "subquery".to_string(),
            }),
            StreamingQuery::CreateStream { as_select, .. } => self.extract_source(as_select),
            StreamingQuery::CreateTable { as_select, .. } => self.extract_source(as_select),
            _ => None,
        }
    }

    fn has_join(&self, query: &StreamingQuery) -> bool {
        match query {
            StreamingQuery::Select { joins, .. } => joins.as_ref().is_some_and(|j| !j.is_empty()),
            StreamingQuery::CreateStream { as_select, .. } => self.has_join(as_select),
            StreamingQuery::CreateTable { as_select, .. } => self.has_join(as_select),
            _ => false,
        }
    }

    fn extract_window_size(&self, window: &Option<WindowSpec>) -> Option<u64> {
        // Extract window size in seconds from WindowSpec
        // WindowSpec uses std::time::Duration directly
        window.as_ref().and_then(|w| match w {
            WindowSpec::Tumbling { size, .. } => Some(size.as_secs()),
            WindowSpec::Sliding { size, .. } => Some(size.as_secs()),
            WindowSpec::Session { gap, .. } => Some(gap.as_secs()),
            WindowSpec::Rows { .. } => None, // Row-based windows don't have time duration
        })
    }
}
