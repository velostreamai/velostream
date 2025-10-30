use clap::{Parser, Subcommand};
use log::{debug, error, info};
use std::fs;
use std::time::Duration;
use velostream::velostream::{
    server::stream_job_server::StreamJobServer,
    sql::{
        annotation_parser::SqlAnnotationParser, app_parser::SqlApplicationParser,
        validator::SqlValidator,
    },
};

#[derive(Parser)]
#[command(name = "velo-sql-multi")]
#[command(about = "Velostream StreamJobServer - Execute multiple streaming SQL jobs concurrently")]
#[command(version = "1.0.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the StreamJobServer for concurrent job execution
    Server {
        /// Kafka broker addresses
        #[arg(long, default_value = "localhost:9092")]
        brokers: String,

        /// Server port for SQL commands
        #[arg(long, default_value = "8080")]
        port: u16,

        /// Base consumer group ID (jobs will append suffixes)
        #[arg(long, default_value = "velo-sql-server")]
        group_id: String,

        /// Maximum concurrent jobs
        #[arg(long, default_value = "10")]
        max_jobs: usize,

        /// Enable performance monitoring
        #[arg(long)]
        enable_metrics: bool,

        /// Metrics endpoint port (if different from server port)
        #[arg(long)]
        metrics_port: Option<u16>,
    },
    /// Deploy a SQL application from a .sql file
    DeployApp {
        /// Path to the .sql application file
        #[arg(long)]
        file: String,

        /// Kafka broker addresses
        #[arg(long, default_value = "localhost:9092")]
        brokers: String,

        /// Base consumer group ID
        #[arg(long, default_value = "velo-sql-app")]
        group_id: String,

        /// Default topic for statements without explicit topics
        #[arg(long)]
        default_topic: Option<String>,

        /// Don't monitor jobs after deployment (exit immediately)
        #[arg(long, default_value = "false")]
        no_monitor: bool,

        /// Enable distributed tracing (OpenTelemetry)
        #[arg(long)]
        enable_tracing: bool,

        /// Tracing sampling ratio (0.0-1.0). Default: 1.0 (100%) for dev, 0.01 (1%) for prod
        #[arg(long)]
        sampling_ratio: Option<f64>,

        /// Enable Prometheus metrics export
        #[arg(long)]
        enable_metrics: bool,

        /// Prometheus metrics port
        #[arg(long, default_value = "9091")]
        metrics_port: u16,

        /// Enable performance profiling
        #[arg(long)]
        enable_profiling: bool,

        /// OpenTelemetry OTLP endpoint
        #[arg(long)]
        otlp_endpoint: Option<String>,
    },
}

// Velostream StreamJobServer - Execute multiple streaming SQL jobs concurrently

async fn start_stream_job_server(
    brokers: String,
    port: u16,
    group_id: String,
    max_jobs: usize,
    enable_metrics: bool,
    metrics_port: Option<u16>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting Velostream StreamJobServer on port {}", port);
    info!("Max concurrent jobs: {}", max_jobs);

    let server = StreamJobServer::new_with_monitoring(
        brokers.clone(),
        group_id.clone(),
        max_jobs,
        enable_metrics,
    )
    .await;

    if enable_metrics {
        let metrics_port = metrics_port.unwrap_or(port + 1000); // Default to main port + 1000
        info!(
            "Performance monitoring enabled - metrics available on port {}",
            metrics_port
        );

        // Start metrics server
        let server_clone = server.clone();
        tokio::spawn(async move {
            start_metrics_server_multi(server_clone, metrics_port)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to start metrics server: {}", e);
                });
        });
    }

    info!("StreamJobServer ready - no jobs deployed");
    info!("Use 'deploy-app' command or HTTP API to deploy SQL applications");

    // Status monitoring loop
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;

        let jobs = server.list_jobs().await;
        info!("Active jobs: {}", jobs.len());

        for job in &jobs {
            info!(
                "  Job '{}' ({}): {:?} - {} records processed",
                job.name, job.topic, job.status, job.metrics.records_processed
            );
        }

        if jobs.is_empty() {
            info!("No active jobs. Server will continue running...");
        }
    }
}

/// Start a simple HTTP server for metrics endpoints (StreamJobServer version)
async fn start_metrics_server_multi(
    server: StreamJobServer,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("StreamJobServer metrics server listening on port {}", port);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                debug!("StreamJobServer metrics request from: {}", addr);
                let server = server.clone();

                tokio::spawn(async move {
                    let mut buffer = [0; 1024];
                    loop {
                        match stream.try_read(&mut buffer) {
                            Ok(0) => return,
                            Ok(n) => {
                                let request = String::from_utf8_lossy(&buffer[0..n]);
                                let response =
                                    handle_multi_metrics_request(request.as_ref(), &server).await;

                                if let Err(e) = stream.try_write(response.as_bytes()) {
                                    error!("Failed to write response: {}", e);
                                }
                                return;
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                tokio::task::yield_now().await;
                                continue;
                            }
                            Err(e) => {
                                error!("Failed to read from socket: {}", e);
                                return;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

/// Handle HTTP metrics requests for StreamJobServer
async fn handle_multi_metrics_request(request: &str, server: &StreamJobServer) -> String {
    // Parse the request path
    let path = if let Some(first_line) = request.lines().next() {
        first_line.split_whitespace().nth(1).unwrap_or("/")
    } else {
        "/"
    };

    match path {
        "/metrics" => {
            // Prometheus metrics endpoint
            if let Some(metrics) = server.get_performance_metrics() {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                    metrics.len(),
                    metrics
                )
            } else {
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\nMetrics not enabled".to_string()
            }
        }
        "/health" => {
            // Health check endpoint
            if let Some(health) = server.get_health_status() {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    health.len(),
                    health
                )
            } else {
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\nHealth monitoring not enabled".to_string()
            }
        }
        "/report" => {
            // Detailed performance report
            if let Some(report) = server.get_performance_report() {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                    report.len(),
                    report
                )
            } else {
                "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\n\r\nPerformance monitoring not enabled".to_string()
            }
        }
        "/jobs" => {
            // Job status endpoint
            let jobs = server.list_jobs().await;
            let job_info = serde_json::to_string_pretty(&jobs)
                .unwrap_or_else(|_| "Error serializing job information".to_string());

            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                job_info.len(),
                job_info
            )
        }
        "/" => {
            // Root endpoint with available endpoints
            let response_body = if server.has_performance_monitoring() {
                r#"{
    "service": "velo-sql-multi-server",
    "status": "running",
    "endpoints": {
        "/metrics": "Prometheus metrics export",
        "/health": "System health status (JSON)",
        "/report": "Detailed performance report (text)",
        "/jobs": "List all running jobs (JSON)"
    },
    "monitoring": "enabled"
}"#
            } else {
                r#"{
    "service": "velo-sql-multi-server", 
    "status": "running",
    "endpoints": {
        "/jobs": "List all running jobs (JSON)"
    },
    "monitoring": "disabled",
    "note": "Use --enable-metrics to enable performance monitoring"
}"#
            };

            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                response_body.len(),
                response_body
            )
        }
        _ => "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\nEndpoint not found"
            .to_string(),
    }
}

#[tokio::main]
async fn main() -> velostream::velostream::error::VeloResult<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Log build timestamp
    info!("🏗️  Binary built: {}", env!("BUILD_TIME"));

    let cli = Cli::parse();

    match cli.command {
        Commands::Server {
            brokers,
            port,
            group_id,
            max_jobs,
            enable_metrics,
            metrics_port,
        } => {
            start_stream_job_server(
                brokers,
                port,
                group_id,
                max_jobs,
                enable_metrics,
                metrics_port,
            )
            .await?;
        }
        Commands::DeployApp {
            file,
            brokers,
            group_id,
            default_topic,
            no_monitor,
            enable_tracing,
            sampling_ratio,
            enable_metrics,
            metrics_port,
            enable_profiling,
            otlp_endpoint,
        } => {
            deploy_sql_application_from_file(
                file,
                brokers,
                group_id,
                default_topic,
                no_monitor,
                enable_tracing,
                sampling_ratio,
                enable_metrics,
                metrics_port,
                enable_profiling,
                otlp_endpoint,
            )
            .await?;
        }
    }

    Ok(())
}

async fn deploy_sql_application_from_file(
    file_path: String,
    brokers: String,
    group_id: String,
    default_topic: Option<String>,
    no_monitor: bool,
    enable_tracing: bool,
    sampling_ratio: Option<f64>,
    enable_metrics: bool,
    metrics_port: u16,
    enable_profiling: bool,
    otlp_endpoint: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Log observability configuration
    if enable_tracing || enable_metrics || enable_profiling {
        info!("🔍 Observability Configuration:");
        if enable_tracing {
            // Dev default: 100% sampling (1.0), Prod default: 1% sampling (0.01)
            let actual_sampling = sampling_ratio.unwrap_or(1.0); // Default to dev mode
            info!(
                "  • Distributed Tracing: ENABLED (sampling: {})",
                actual_sampling
            );
            if let Some(ref endpoint) = otlp_endpoint {
                info!("  • OTLP Endpoint: {}", endpoint);
            } else {
                info!("  • OTLP Endpoint: http://localhost:4317 (default)");
            }
        }
        if enable_metrics {
            info!("  • Prometheus Metrics: ENABLED (port: {})", metrics_port);
        }
        if enable_profiling {
            info!("  • Performance Profiling: ENABLED");
        }
    }
    println!("Starting deployment from file: {}", file_path);

    // Read the SQL application file
    println!("Reading SQL file: {}", file_path);
    let content = fs::read_to_string(&file_path).map_err(|e| {
        let error_msg = format!("Failed to read SQL file '{}': {}", file_path, e);
        println!("ERROR: {}", error_msg);
        eprintln!("ERROR: {}", error_msg);
        e
    })?;
    println!("Successfully read {} bytes from file", content.len());

    // Parse deployment context from SQL annotations
    println!("Extracting deployment context from SQL annotations...");
    let sql_deployment_ctx = SqlAnnotationParser::parse_deployment_context(&content);
    if sql_deployment_ctx.node_id.is_some()
        || sql_deployment_ctx.node_name.is_some()
        || sql_deployment_ctx.region.is_some()
    {
        println!("✅ Deployment context extracted from annotations:");
        if let Some(ref node_id) = sql_deployment_ctx.node_id {
            println!("   • Node ID: {}", node_id);
            unsafe {
                std::env::set_var("NODE_ID", node_id);
            }
        }
        if let Some(ref node_name) = sql_deployment_ctx.node_name {
            println!("   • Node Name: {}", node_name);
            unsafe {
                std::env::set_var("NODE_NAME", node_name);
            }
        }
        if let Some(ref region) = sql_deployment_ctx.region {
            println!("   • Region: {}", region);
            unsafe {
                std::env::set_var("REGION", region);
            }
        }
    } else {
        println!("ℹ️  No deployment context annotations found in SQL file");
    }

    // Validate SQL before deployment using SqlValidator
    println!("Validating SQL application...");
    let validator = SqlValidator::new();
    let validation_result = validator.validate_application(std::path::Path::new(&file_path));

    if !validation_result.is_valid {
        let mut error_msg = String::from("❌ SQL validation failed!\n");

        if let Some(app_name) = &validation_result.application_name {
            error_msg.push_str(&format!("📦 Application: {}\n", app_name));
        }

        error_msg.push_str(&format!(
            "📊 Queries: {} total, {} valid, {} failed\n",
            validation_result.total_queries,
            validation_result.valid_queries,
            validation_result.total_queries - validation_result.valid_queries
        ));

        if !validation_result.global_errors.is_empty() {
            error_msg.push_str("\n🚨 Global Errors:\n");
            for error in &validation_result.global_errors {
                error_msg.push_str(&format!("  • {}\n", error));
            }
        }

        // Show query-specific errors
        for (idx, query_result) in validation_result.query_results.iter().enumerate() {
            if !query_result.is_valid {
                error_msg.push_str(&format!(
                    "\n❌ Query #{} (Line {}):\n",
                    idx + 1,
                    query_result.start_line
                ));

                if !query_result.parsing_errors.is_empty() {
                    error_msg.push_str("  📝 Parsing Errors:\n");
                    for error in &query_result.parsing_errors {
                        error_msg.push_str(&format!("    • {}\n", error.message));
                    }
                }

                if !query_result.configuration_errors.is_empty() {
                    error_msg.push_str("  ⚙️ Configuration Errors:\n");
                    for error in &query_result.configuration_errors {
                        error_msg.push_str(&format!("    • {}\n", error));
                    }
                }

                if !query_result.missing_source_configs.is_empty() {
                    error_msg.push_str("  📥 Missing Source Configs:\n");
                    for config in &query_result.missing_source_configs {
                        error_msg.push_str(&format!(
                            "    • {}: {}\n",
                            config.name,
                            config.missing_keys.join(", ")
                        ));
                    }
                }

                if !query_result.missing_sink_configs.is_empty() {
                    error_msg.push_str("  📤 Missing Sink Configs:\n");
                    for config in &query_result.missing_sink_configs {
                        error_msg.push_str(&format!(
                            "    • {}: {}\n",
                            config.name,
                            config.missing_keys.join(", ")
                        ));
                    }
                }
            }
        }

        if !validation_result.recommendations.is_empty() {
            error_msg.push_str("\n💡 Recommendations:\n");
            for rec in &validation_result.recommendations {
                error_msg.push_str(&format!("  • {}\n", rec));
            }
        }

        println!("{}", error_msg);
        eprintln!("{}", error_msg);
        return Err("SQL validation failed. Please fix the errors above before deployment.".into());
    }

    println!("✅ SQL validation passed!");
    if let Some(app_name) = &validation_result.application_name {
        println!("📦 Application: {}", app_name);
    }
    println!(
        "📊 {} queries validated successfully",
        validation_result.total_queries
    );

    // Parse the SQL application
    println!("Parsing SQL application...");
    let app_parser = SqlApplicationParser::new();
    let app = app_parser.parse_application(&content).map_err(|e| {
        let error_msg = format!("Failed to parse SQL application: {}", e);
        println!("ERROR: {}", error_msg);
        eprintln!("ERROR: {}", error_msg);
        e
    })?;
    println!(
        "Successfully parsed application: {} v{} with {} statements",
        app.metadata.name,
        app.metadata.version,
        app.statements.len()
    );

    // Keep logging for server operations, but rely on println! for deployment output

    // Create a temporary server instance for deployment with observability configuration
    println!(
        "Creating StreamJobServer with brokers: {}, group_id: {}",
        brokers, group_id
    );

    // Build StreamingConfig with observability features based on CLI flags
    use velostream::velostream::sql::execution::config::{
        PrometheusConfig, StreamingConfig, TracingConfig,
    };

    let mut streaming_config = StreamingConfig::default();

    // Configure distributed tracing if enabled
    if enable_tracing {
        // Start with development defaults and override with CLI arguments
        let mut tracing_config = TracingConfig::development();

        // Override with custom values from CLI
        tracing_config.service_name =
            format!("velo-sql-{}", file_path.split('/').last().unwrap_or("app"));
        tracing_config.sampling_ratio = sampling_ratio.unwrap_or(1.0); // Dev default: 100%, Prod: use --sampling-ratio 0.01

        if let Some(endpoint) = otlp_endpoint.clone() {
            tracing_config.otlp_endpoint = Some(endpoint);
        }

        info!(
            "🔍 Initializing distributed tracing with service name: {}",
            tracing_config.service_name
        );
        streaming_config = streaming_config.with_tracing_config(tracing_config);
    }

    // Configure Prometheus metrics if enabled
    if enable_metrics {
        let prometheus_config = PrometheusConfig {
            metrics_path: "/metrics".to_string(),
            bind_address: "0.0.0.0".to_string(),
            port: metrics_port,
            enable_histograms: true,
            enable_query_metrics: true,
            enable_streaming_metrics: true,
            collection_interval_seconds: 15,
            max_labels_per_metric: 10,
        };
        streaming_config = streaming_config.with_prometheus_config(prometheus_config);
    }

    // Configure performance profiling if enabled
    if enable_profiling {
        use velostream::velostream::sql::execution::config::ProfilingConfig;
        streaming_config = streaming_config.with_profiling_config(ProfilingConfig::development());
    }

    // Create server with full observability configuration
    let server = StreamJobServer::new_with_observability(
        brokers,
        group_id,
        100, // High limit for app deployment
        streaming_config,
    )
    .await;

    // Deploy the application
    println!(
        "Deploying application with {} statements...",
        app.statements.len()
    );
    let deployed_jobs = server
        .deploy_sql_application_with_filename(app.clone(), default_topic, Some(file_path.clone()))
        .await
        .map_err(|e| {
            let error_msg = format!("Failed to deploy SQL application: {}", e);
            println!("ERROR: {}", error_msg);
            eprintln!("ERROR: {}", error_msg);
            e
        })?;
    println!(
        "Deployment completed. Jobs deployed: {}",
        deployed_jobs.len()
    );

    // Output results to stdout for integration tests
    println!("SQL application deployment completed!");
    println!(
        "Application: {} v{}",
        app.metadata.name, app.metadata.version
    );
    println!("Deployed {} jobs: {:?}", deployed_jobs.len(), deployed_jobs);

    if let Some(description) = &app.metadata.description {
        println!("Description: {}", description);
    }

    if let Some(author) = &app.metadata.author {
        println!("Author: {}", author);
    }

    // Start metrics HTTP server if metrics are enabled
    if enable_metrics {
        let server_clone = server.clone();
        tokio::spawn(async move {
            start_metrics_server_multi(server_clone, metrics_port)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to start metrics server: {}", e);
                });
        });
        println!(
            "📊 Metrics server started on http://0.0.0.0:{}/metrics",
            metrics_port
        );
        info!("Metrics server listening on port {}", metrics_port);
    }

    if no_monitor {
        println!("Deployment completed. Jobs are running in the background.");
        return Ok(());
    }

    // Keep the jobs running
    info!("Jobs are now running. Use Ctrl+C to stop.");

    // Status monitoring loop for the deployed application
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;

        let jobs = server.list_jobs().await;
        info!(
            "Application '{}' - Active jobs: {}",
            app.metadata.name,
            jobs.len()
        );

        for job in &jobs {
            info!(
                "  Job '{}' ({}): {:?} - {} records processed",
                job.name, job.topic, job.status, job.metrics.records_processed
            );
        }

        if jobs.is_empty() {
            info!(
                "No active jobs remaining for application '{}'",
                app.metadata.name
            );
            break;
        }
    }

    Ok(())
}
