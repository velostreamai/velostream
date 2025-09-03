use clap::{Parser, Subcommand};
use ferrisstreams::ferris::{sql::SqlApplicationParser, MultiJobSqlServer};
use log::{error, info};
use std::fs;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "ferris-sql-multi")]
#[command(about = "FerrisStreams Multi-Job SQL Server - Execute multiple SQL jobs concurrently")]
#[command(version = "1.0.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the multi-job SQL server
    Server {
        /// Kafka broker addresses
        #[arg(long, default_value = "localhost:9092")]
        brokers: String,

        /// Server port for SQL commands
        #[arg(long, default_value = "8080")]
        port: u16,

        /// Base consumer group ID (jobs will append suffixes)
        #[arg(long, default_value = "ferris-sql-server")]
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
        #[arg(long, default_value = "ferris-sql-app")]
        group_id: String,

        /// Default topic for statements without explicit topics
        #[arg(long)]
        default_topic: Option<String>,

        /// Don't monitor jobs after deployment (exit immediately)
        #[arg(long, default_value = "false")]
        no_monitor: bool,
    },
}

// All struct definitions moved to src/ferris/modern_multi_job_server.rs

async fn start_multi_job_server(
    brokers: String,
    port: u16,
    group_id: String,
    max_jobs: usize,
    enable_metrics: bool,
    metrics_port: Option<u16>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Starting FerrisStreams Multi-Job SQL Server on port {}",
        port
    );
    info!("Max concurrent jobs: {}", max_jobs);

    let server = MultiJobSqlServer::new_with_monitoring(
        brokers.clone(),
        group_id.clone(),
        max_jobs,
        enable_metrics,
    );

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

    info!("Multi-job SQL server ready - no jobs deployed");
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

/// Start a simple HTTP server for metrics endpoints (multi-job version)
async fn start_metrics_server_multi(
    server: MultiJobSqlServer,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("Multi-job metrics server listening on port {}", port);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("Multi-job metrics request from: {}", addr);
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

/// Handle HTTP metrics requests for multi-job server
async fn handle_multi_metrics_request(request: &str, server: &MultiJobSqlServer) -> String {
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
    "service": "ferris-sql-multi-server",
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
    "service": "ferris-sql-multi-server", 
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
async fn main() -> ferrisstreams::ferris::error::FerrisResult<()> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

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
            start_multi_job_server(
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
        } => {
            deploy_sql_application_from_file(file, brokers, group_id, default_topic, no_monitor)
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
) -> Result<(), Box<dyn std::error::Error>> {
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

    // Create a temporary server instance for deployment
    println!(
        "Creating multi-job server with brokers: {}, group_id: {}",
        brokers, group_id
    );
    let server = MultiJobSqlServer::new(brokers, group_id, 100); // High limit for app deployment

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
