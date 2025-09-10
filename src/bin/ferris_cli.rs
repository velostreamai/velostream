use clap::{Parser, Subcommand};
use serde_json::Value;
use std::process::Command;
use std::time::Duration;
use tokio::time;

#[derive(Parser)]
#[command(name = "ferris-cli")]
#[command(about = "FerrisStreams CLI - Monitor and manage FerrisStreams components")]
#[command(version = "1.0.0")]
struct Cli {
    /// FerrisStreams SQL server host
    #[arg(long, default_value = "localhost", global = true)]
    sql_host: String,

    /// FerrisStreams SQL server port
    #[arg(long, default_value = "8080", global = true)]
    sql_port: u16,

    /// Kafka broker addresses
    #[arg(long, default_value = "localhost:9092", global = true)]
    kafka_brokers: String,

    /// Remote mode - don't check local Docker/processes
    #[arg(long, global = true)]
    remote: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show status of all FerrisStreams components
    Status {
        /// Include verbose output
        #[arg(short, long)]
        verbose: bool,

        /// Refresh interval in seconds (0 for single check)
        #[arg(short, long, default_value = "0")]
        refresh: u64,
    },
    /// Show Kafka cluster information
    Kafka {
        /// Kafka brokers
        #[arg(long, default_value = "localhost:9092")]
        brokers: String,

        /// Show topic details
        #[arg(short, long)]
        topics: bool,

        /// Show consumer groups
        #[arg(short, long)]
        groups: bool,
    },
    /// Show SQL server information
    Sql {
        /// SQL server port
        #[arg(long, default_value = "8080")]
        port: u16,

        /// Show job details
        #[arg(short, long)]
        jobs: bool,
    },
    /// Show Docker containers status
    Docker {
        /// Show only FerrisStreams related containers
        #[arg(short, long)]
        ferris_only: bool,
    },
    /// Show process information
    Processes {
        /// Show all processes or just FerrisStreams
        #[arg(short, long)]
        all: bool,
    },
    /// Quick health check of all components
    Health,
    /// Show detailed job and task information
    Jobs {
        /// Show running data generators
        #[arg(short, long)]
        generators: bool,

        /// Show topic message counts
        #[arg(short, long)]
        topics: bool,

        /// Show active SQL processing
        #[arg(short, long)]
        sql: bool,
    },
    /// Start demo components
    Start {
        /// Demo duration in minutes
        #[arg(short, long, default_value = "5")]
        duration: u64,
    },
    /// Stop all demo components
    Stop,
}

#[derive(Debug)]
struct ComponentStatus {
    name: String,
    status: String,
    details: Vec<String>,
    healthy: bool,
}

struct FerrisStreamsMonitor {
    verbose: bool,
    sql_host: String,
    sql_port: u16,
    kafka_brokers: String,
    remote: bool,
}

impl FerrisStreamsMonitor {
    fn new(
        verbose: bool,
        sql_host: String,
        sql_port: u16,
        kafka_brokers: String,
        remote: bool,
    ) -> Self {
        Self {
            verbose,
            sql_host,
            sql_port,
            kafka_brokers,
            remote,
        }
    }

    async fn check_all_status(&self) -> Vec<ComponentStatus> {
        let mut statuses = Vec::new();

        if !self.remote {
            // Check Docker containers (local only)
            statuses.push(self.check_docker_status().await);

            // Check processes (local only)
            statuses.push(self.check_processes_status().await);
        }

        // Check Kafka (local or remote)
        statuses.push(self.check_kafka_status().await);

        // Check SQL Server (local or remote)
        statuses.push(self.check_stream_job_server_status().await);

        // Check SQL Jobs (local or remote)
        statuses.push(self.check_sql_jobs_status().await);

        statuses
    }

    async fn check_docker_status(&self) -> ComponentStatus {
        let output = Command::new("docker")
            .args([
                "ps",
                "--format",
                "table {{.Names}}\t{{.Status}}\t{{.Ports}}",
            ])
            .output();

        match output {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let lines: Vec<&str> = stdout.lines().collect();

                let ferris_containers: Vec<&str> = lines
                    .iter()
                    .filter(|line| {
                        line.contains("kafka")
                            || line.contains("zookeeper")
                            || line.contains("ferris")
                            || line.contains("prometheus")
                            || line.contains("grafana")
                    })
                    .cloned()
                    .collect();

                let healthy = !ferris_containers.is_empty()
                    && ferris_containers.iter().any(|line| line.contains("Up"));

                ComponentStatus {
                    name: "Docker Containers".to_string(),
                    status: if healthy { "Running" } else { "Stopped" }.to_string(),
                    details: ferris_containers.iter().map(|s| s.to_string()).collect(),
                    healthy,
                }
            }
            Err(e) => ComponentStatus {
                name: "Docker Containers".to_string(),
                status: "Error".to_string(),
                details: vec![format!("Docker command failed: {}", e)],
                healthy: false,
            },
        }
    }

    async fn check_kafka_status(&self) -> ComponentStatus {
        if self.remote {
            // For remote, we can't easily check Kafka without kafka client tools
            // We'll indicate remote mode and that Kafka should be checked separately
            ComponentStatus {
                name: "Kafka Cluster".to_string(),
                status: "Remote Mode".to_string(),
                details: vec![
                    format!("Brokers: {}", self.kafka_brokers),
                    "Use 'kafka' command to check remote cluster".to_string(),
                ],
                healthy: true, // Assume healthy in remote mode
            }
        } else {
            // Try to connect to Kafka and list topics - find Kafka container dynamically
            let kafka_container = self.get_kafka_container_name().await;

            if let Some(container_name) = kafka_container {
                let output = Command::new("docker")
                    .args([
                        "exec",
                        &container_name,
                        "kafka-topics",
                        "--bootstrap-server",
                        &self.kafka_brokers,
                        "--list",
                    ])
                    .output();

                match output {
                    Ok(output) => {
                        if output.status.success() {
                            let stdout = String::from_utf8_lossy(&output.stdout);
                            let topics: Vec<&str> =
                                stdout.lines().filter(|line| !line.is_empty()).collect();

                            ComponentStatus {
                                name: "Kafka Cluster".to_string(),
                                status: "Connected".to_string(),
                                details: if topics.is_empty() {
                                    vec!["No topics found".to_string()]
                                } else {
                                    vec![
                                        format!("Topics: {}", topics.len()),
                                        format!("Container: {}", container_name),
                                        format!("Brokers: {}", self.kafka_brokers),
                                    ]
                                },
                                healthy: true,
                            }
                        } else {
                            let stderr = String::from_utf8_lossy(&output.stderr);
                            ComponentStatus {
                                name: "Kafka Cluster".to_string(),
                                status: "Connection Failed".to_string(),
                                details: vec![stderr.to_string()],
                                healthy: false,
                            }
                        }
                    }
                    Err(e) => ComponentStatus {
                        name: "Kafka Cluster".to_string(),
                        status: "Error".to_string(),
                        details: vec![format!("Command failed: {}", e)],
                        healthy: false,
                    },
                }
            } else {
                ComponentStatus {
                    name: "Kafka Cluster".to_string(),
                    status: "No Container".to_string(),
                    details: vec!["No Kafka container found".to_string()],
                    healthy: false,
                }
            }
        }
    }

    async fn check_stream_job_server_status(&self) -> ComponentStatus {
        if self.remote {
            // Try HTTP health check for remote server
            let health_url = format!("http://{}:{}/health", self.sql_host, self.sql_port);

            match reqwest::get(&health_url).await {
                Ok(response) => {
                    if response.status().is_success() {
                        ComponentStatus {
                            name: "SQL Server".to_string(),
                            status: "Running".to_string(),
                            details: vec![format!(
                                "Remote server: {}:{}",
                                self.sql_host, self.sql_port
                            )],
                            healthy: true,
                        }
                    } else {
                        ComponentStatus {
                            name: "SQL Server".to_string(),
                            status: "Unhealthy".to_string(),
                            details: vec![format!("HTTP {}: {}", response.status(), health_url)],
                            healthy: false,
                        }
                    }
                }
                Err(e) => ComponentStatus {
                    name: "SQL Server".to_string(),
                    status: "Connection Failed".to_string(),
                    details: vec![format!("Failed to connect to {}: {}", health_url, e)],
                    healthy: false,
                },
            }
        } else {
            // Check local processes
            let output = Command::new("pgrep").args(["-f", "ferris-sql"]).output();

            match output {
                Ok(output) => {
                    if output.status.success() {
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        let pids: Vec<&str> =
                            stdout.lines().filter(|line| !line.is_empty()).collect();

                        ComponentStatus {
                            name: "SQL Server".to_string(),
                            status: "Running".to_string(),
                            details: vec![format!("Local PIDs: {}", pids.join(", "))],
                            healthy: true,
                        }
                    } else {
                        ComponentStatus {
                            name: "SQL Server".to_string(),
                            status: "Stopped".to_string(),
                            details: vec!["No ferris-sql processes found".to_string()],
                            healthy: false,
                        }
                    }
                }
                Err(e) => ComponentStatus {
                    name: "SQL Server".to_string(),
                    status: "Error".to_string(),
                    details: vec![format!("Process check failed: {}", e)],
                    healthy: false,
                },
            }
        }
    }

    async fn check_sql_jobs_status(&self) -> ComponentStatus {
        if self.remote {
            // Try to get job information from remote HTTP API
            let jobs_url = format!("http://{}:{}/jobs", self.sql_host, self.sql_port);

            match reqwest::get(&jobs_url).await {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<Value>().await {
                            Ok(jobs_data) => {
                                let mut job_details = Vec::new();
                                let mut job_count = 0;

                                if let Some(jobs) = jobs_data.as_array() {
                                    job_count = jobs.len();
                                    for job in jobs {
                                        if let Some(name) = job.get("name").and_then(|n| n.as_str())
                                        {
                                            if let Some(status) =
                                                job.get("status").and_then(|s| s.as_str())
                                            {
                                                job_details
                                                    .push(format!("Job '{}': {}", name, status));
                                            }
                                        }
                                    }
                                } else if let Some(job_count_val) = jobs_data.get("count") {
                                    job_count = job_count_val.as_u64().unwrap_or(0) as usize;
                                    job_details.push(format!(
                                        "Remote server reports {} active jobs",
                                        job_count
                                    ));
                                }

                                ComponentStatus {
                                    name: "SQL Jobs & Tasks".to_string(),
                                    status: if job_count > 0 { "Active" } else { "No Jobs" }
                                        .to_string(),
                                    details: if job_details.is_empty() {
                                        vec![format!(
                                            "Remote server: {}:{}",
                                            self.sql_host, self.sql_port
                                        )]
                                    } else {
                                        job_details
                                    },
                                    healthy: true,
                                }
                            }
                            Err(_) => ComponentStatus {
                                name: "SQL Jobs & Tasks".to_string(),
                                status: "API Error".to_string(),
                                details: vec!["Failed to parse jobs response".to_string()],
                                healthy: false,
                            },
                        }
                    } else {
                        ComponentStatus {
                            name: "SQL Jobs & Tasks".to_string(),
                            status: "API Unavailable".to_string(),
                            details: vec![format!("/jobs endpoint returned {}", response.status())],
                            healthy: false,
                        }
                    }
                }
                Err(e) => ComponentStatus {
                    name: "SQL Jobs & Tasks".to_string(),
                    status: "Connection Failed".to_string(),
                    details: vec![format!("Failed to connect to {}: {}", jobs_url, e)],
                    healthy: false,
                },
            }
        } else {
            // Local process checking (existing logic)
            let output = Command::new("ps").args(["aux"]).output();

            match output {
                Ok(output) => {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let ferris_sql_lines: Vec<&str> = stdout
                        .lines()
                        .filter(|line| line.contains("ferris-sql") && !line.contains("grep"))
                        .collect();

                    let mut job_details = Vec::new();
                    let mut job_count = 0;

                    // Extract job information from running processes
                    for line in &ferris_sql_lines {
                        if line.contains("ferris-sql-multi") {
                            job_details.push("StreamJobServer: Running".to_string());
                            job_count += 1;

                            // Extract process details
                            let parts: Vec<&str> = line.split_whitespace().collect();
                            if let Some(pid) = parts.get(1) {
                                if let Some(time) = parts.get(9) {
                                    job_details.push(format!("  PID: {}, Runtime: {}", pid, time));
                                }
                            }
                        } else if line.contains("ferris-sql") && line.contains("server") {
                            job_details.push("Single-Job SQL Server: Running".to_string());
                            job_count += 1;

                            // Extract process details
                            let parts: Vec<&str> = line.split_whitespace().collect();
                            if let Some(pid) = parts.get(1) {
                                if let Some(time) = parts.get(9) {
                                    job_details.push(format!("  PID: {}, Runtime: {}", pid, time));
                                }
                            }
                        }
                    }

                    let healthy = job_count > 0;
                    let status = if healthy { "Active" } else { "No Jobs" };

                    ComponentStatus {
                        name: "SQL Jobs & Tasks".to_string(),
                        status: status.to_string(),
                        details: if job_details.is_empty() {
                            vec!["No active SQL server processes detected".to_string()]
                        } else {
                            job_details
                        },
                        healthy,
                    }
                }
                Err(e) => ComponentStatus {
                    name: "SQL Jobs & Tasks".to_string(),
                    status: "Error".to_string(),
                    details: vec![format!("Job check failed: {}", e)],
                    healthy: false,
                },
            }
        }
    }

    async fn get_kafka_container_name(&self) -> Option<String> {
        // Try multiple patterns to find Kafka containers
        let patterns = [
            ("ancestor=confluentinc/cp-kafka", "Confluent Kafka"),
            ("ancestor=bitnami/kafka", "Bitnami Kafka"),
            ("name=kafka", "Named kafka"),
            ("name=simple-kafka", "Simple kafka"),
        ];

        for (filter, _desc) in &patterns {
            let output = Command::new("docker")
                .args(["ps", "--filter", filter, "--format", "{{.Names}}"])
                .output();

            if let Ok(output) = output {
                let stdout = String::from_utf8_lossy(&output.stdout);
                if let Some(name) = stdout.lines().next().map(|s| s.to_string()) {
                    if !name.trim().is_empty() {
                        return Some(name);
                    }
                }
            }
        }

        None
    }

    async fn check_processes_status(&self) -> ComponentStatus {
        let output = Command::new("pgrep").args(["-f", "ferris"]).output();

        match output {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let pids: Vec<&str> = stdout.lines().filter(|line| !line.is_empty()).collect();

                let details = if pids.is_empty() {
                    vec!["No FerrisStreams processes running".to_string()]
                } else {
                    vec![
                        format!("Active processes: {}", pids.len()),
                        format!("PIDs: {}", pids.join(", ")),
                    ]
                };

                ComponentStatus {
                    name: "FerrisStreams Processes".to_string(),
                    status: if pids.is_empty() { "Idle" } else { "Active" }.to_string(),
                    details,
                    healthy: true, // Idle is also healthy
                }
            }
            Err(e) => ComponentStatus {
                name: "FerrisStreams Processes".to_string(),
                status: "Error".to_string(),
                details: vec![format!("Process check failed: {}", e)],
                healthy: false,
            },
        }
    }

    fn print_status(&self, statuses: &[ComponentStatus]) {
        println!("\n🔍 FerrisStreams Status Overview");
        println!("================================");

        let healthy_count = statuses.iter().filter(|s| s.healthy).count();
        let total_count = statuses.len();

        println!(
            "📊 Health: {}/{} components healthy\n",
            healthy_count, total_count
        );

        for status in statuses {
            let status_emoji = if status.healthy { "✅" } else { "❌" };
            println!("{} {}: {}", status_emoji, status.name, status.status);

            if self.verbose {
                for detail in &status.details {
                    println!("   {}", detail);
                }
                println!();
            }
        }

        if !self.verbose && statuses.iter().any(|s| !s.healthy) {
            println!("\n💡 Use --verbose for more details on issues");
        }
    }

    async fn show_kafka_info(&self, brokers: &str, show_topics: bool, show_groups: bool) {
        println!("\n🔗 Kafka Cluster Information");
        println!("============================");
        println!("Brokers: {}\n", brokers);

        if show_topics {
            println!("📋 Topics:");
            let output = Command::new("docker")
                .args([
                    "exec",
                    "simple-kafka",
                    "kafka-topics",
                    "--bootstrap-server",
                    brokers,
                    "--list",
                ])
                .output();

            match output {
                Ok(output) if output.status.success() => {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let topics: Vec<&str> =
                        stdout.lines().filter(|line| !line.is_empty()).collect();

                    if topics.is_empty() {
                        println!("   No topics found");
                    } else {
                        for topic in topics {
                            println!("   • {}", topic);
                        }
                    }
                }
                _ => println!("   ❌ Could not list topics"),
            }
            println!();
        }

        if show_groups {
            println!("👥 Consumer Groups:");
            let output = Command::new("docker")
                .args([
                    "exec",
                    "simple-kafka",
                    "kafka-consumer-groups",
                    "--bootstrap-server",
                    brokers,
                    "--list",
                ])
                .output();

            match output {
                Ok(output) if output.status.success() => {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let groups: Vec<&str> =
                        stdout.lines().filter(|line| !line.is_empty()).collect();

                    if groups.is_empty() {
                        println!("   No consumer groups found");
                    } else {
                        for group in groups {
                            println!("   • {}", group);
                        }
                    }
                }
                _ => println!("   ❌ Could not list consumer groups"),
            }
        }
    }

    async fn health_check(&self) {
        println!("\n🏥 FerrisStreams Health Check");
        println!("=============================");

        let statuses = self.check_all_status().await;
        let all_healthy = statuses.iter().all(|s| s.healthy);

        for status in &statuses {
            let emoji = if status.healthy { "✅" } else { "❌" };
            println!("{} {}", emoji, status.name);
        }

        println!(
            "\n{}",
            if all_healthy {
                "🎉 All systems healthy!"
            } else {
                "⚠️  Some components need attention"
            }
        );

        if !all_healthy {
            println!("💡 Run 'ferris-cli status --verbose' for details");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Status { verbose, refresh } => {
            let monitor = FerrisStreamsMonitor::new(
                verbose,
                cli.sql_host.clone(),
                cli.sql_port,
                cli.kafka_brokers.clone(),
                cli.remote,
            );

            if refresh > 0 {
                println!(
                    "🔄 Monitoring FerrisStreams (refresh every {}s, Ctrl+C to stop)",
                    refresh
                );
                loop {
                    let statuses = monitor.check_all_status().await;
                    print!("\x1B[2J\x1B[1;1H"); // Clear screen
                    monitor.print_status(&statuses);
                    time::sleep(Duration::from_secs(refresh)).await;
                }
            } else {
                let statuses = monitor.check_all_status().await;
                monitor.print_status(&statuses);
            }
        }

        Commands::Kafka {
            brokers,
            topics,
            groups,
        } => {
            let monitor = FerrisStreamsMonitor::new(
                true,
                cli.sql_host.clone(),
                cli.sql_port,
                brokers.clone(),
                cli.remote,
            );
            monitor.show_kafka_info(&brokers, topics, groups).await;
        }

        Commands::Sql { port, jobs } => {
            println!("\n⚙️  SQL Server Information");
            println!("========================");
            println!("Server: {}:{}", cli.sql_host, port);

            if jobs {
                println!("\n📋 Active Jobs & Tasks:");
                let monitor = FerrisStreamsMonitor::new(
                    true,
                    cli.sql_host.clone(),
                    port,
                    cli.kafka_brokers.clone(),
                    cli.remote,
                );
                let job_status = monitor.check_sql_jobs_status().await;

                for detail in &job_status.details {
                    println!("   {}", detail);
                }

                // Check for general streaming activity
                println!("\n💾 Kafka Topics:");
                let kafka_container = monitor.get_kafka_container_name().await;
                if let Some(container_name) = kafka_container {
                    let output = Command::new("docker")
                        .args([
                            "exec",
                            &container_name,
                            "kafka-topics",
                            "--bootstrap-server",
                            "localhost:9092",
                            "--list",
                        ])
                        .output();

                    if let Ok(output) = output {
                        if output.status.success() {
                            let stdout = String::from_utf8_lossy(&output.stdout);
                            let topics: Vec<&str> = stdout
                                .lines()
                                .filter(|line| !line.is_empty() && !line.starts_with("__"))
                                .collect();

                            if topics.is_empty() {
                                println!("   📊 No user topics found");
                            } else {
                                println!("   📊 {} topics available", topics.len());
                                for topic in topics.iter().take(5) {
                                    println!("     • {}", topic);
                                }
                                if topics.len() > 5 {
                                    println!("     • ... and {} more", topics.len() - 5);
                                }
                            }
                        }
                    }
                } else {
                    println!("   ❌ No Kafka container found");
                }
            }
        }

        Commands::Docker { ferris_only } => {
            println!("\n🐳 Docker Containers");
            println!("===================");

            let args = vec![
                "ps",
                "--format",
                "table {{.Names}}\t{{.Status}}\t{{.Ports}}",
            ];
            if ferris_only {
                // We'll filter the output instead of using docker filters
            }

            let output = Command::new("docker").args(&args).output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);

            for line in stdout.lines() {
                if !ferris_only
                    || line.contains("kafka")
                    || line.contains("zookeeper")
                    || line.contains("ferris")
                    || line.contains("prometheus")
                    || line.contains("grafana")
                    || line.contains("NAMES")
                {
                    println!("{}", line);
                }
            }
        }

        Commands::Processes { all } => {
            println!("\n🔄 Process Information");
            println!("=====================");

            let pattern = if all { "." } else { "ferris|trading" };
            let output = Command::new("sh")
                .args([
                    "-c",
                    &format!("ps aux | grep -E '{}' | grep -v grep", pattern),
                ])
                .output()?;

            let stdout = String::from_utf8_lossy(&output.stdout);
            if stdout.trim().is_empty() {
                println!("No matching processes found");
            } else {
                println!("{}", stdout);
            }
        }

        Commands::Health => {
            let monitor = FerrisStreamsMonitor::new(
                false,
                cli.sql_host.clone(),
                cli.sql_port,
                cli.kafka_brokers.clone(),
                cli.remote,
            );
            monitor.health_check().await;
        }

        Commands::Jobs {
            generators,
            topics,
            sql,
        } => {
            let monitor = FerrisStreamsMonitor::new(
                true,
                cli.sql_host.clone(),
                cli.sql_port,
                cli.kafka_brokers.clone(),
                cli.remote,
            );

            println!("\n🔄 FerrisStreams Jobs & Tasks");
            println!("=============================");

            // Show all by default if no specific flags
            let show_all = !generators && !topics && !sql;

            if show_all || sql {
                println!("\n⚙️  SQL Processing:");
                let job_status = monitor.check_sql_jobs_status().await;
                for detail in &job_status.details {
                    println!("   {}", detail);
                }
            }

            if show_all || generators {
                println!("\n🔄 Data Producers & Generators:");
                let output = Command::new("ps").args(["aux"]).output();

                if let Ok(output) = output {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let producer_lines: Vec<&str> = stdout
                        .lines()
                        .filter(|line| {
                            (line.contains("producer")
                                || line.contains("generator")
                                || line.contains("data"))
                                && line.contains("ferris")
                                && !line.contains("grep")
                        })
                        .collect();

                    if producer_lines.is_empty() {
                        println!("   ⏸️  No data producers/generators detected");
                    } else {
                        for line in producer_lines {
                            let parts: Vec<&str> = line.split_whitespace().collect();
                            if let Some(pid) = parts.get(1) {
                                if let Some(cmd) = parts.get(10..) {
                                    let cmd_str = cmd.join(" ");
                                    println!("   ✅ Producer (PID: {})", pid);
                                    println!(
                                        "      Command: {}",
                                        cmd_str.chars().take(60).collect::<String>()
                                    );
                                    if let Some(time) = parts.get(9) {
                                        println!("      Runtime: {}", time);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if show_all || topics {
                println!("\n📊 Topic Activity & Message Counts:");
                let kafka_container = monitor.get_kafka_container_name().await;

                if let Some(ref container_name) = kafka_container {
                    // First get all topics
                    let topics_output = Command::new("docker")
                        .args([
                            "exec",
                            container_name,
                            "kafka-topics",
                            "--bootstrap-server",
                            "localhost:9092",
                            "--list",
                        ])
                        .output();

                    if let Ok(topics_output) = topics_output {
                        if topics_output.status.success() {
                            let topics_stdout = String::from_utf8_lossy(&topics_output.stdout);
                            let topics: Vec<&str> = topics_stdout
                                .lines()
                                .filter(|line| !line.is_empty() && !line.starts_with("__"))
                                .collect();

                            if topics.is_empty() {
                                println!("   📊 No user topics found");
                            } else {
                                for topic in topics.iter().take(10) {
                                    // Show first 10 topics
                                    let output = Command::new("docker")
                                        .args([
                                            "exec",
                                            container_name,
                                            "kafka-run-class",
                                            "kafka.tools.GetOffsetShell",
                                            "--bootstrap-server",
                                            "localhost:9092",
                                            "--topic",
                                            topic,
                                            "--time",
                                            "-1",
                                        ])
                                        .output();

                                    if let Ok(output) = output {
                                        if output.status.success() {
                                            let stdout = String::from_utf8_lossy(&output.stdout);
                                            if !stdout.trim().is_empty() {
                                                // Parse partition offsets and sum them
                                                let total_messages: i64 = stdout
                                                    .lines()
                                                    .filter_map(|line| {
                                                        line.split(':')
                                                            .next_back()?
                                                            .parse::<i64>()
                                                            .ok()
                                                    })
                                                    .sum();

                                                if total_messages > 0 {
                                                    println!(
                                                        "   📈 {}: {} messages",
                                                        topic, total_messages
                                                    );
                                                } else {
                                                    println!("   📊 {}: No messages", topic);
                                                }
                                            }
                                        } else {
                                            println!("   ❓ {}: Unable to check", topic);
                                        }
                                    }
                                }

                                if topics.len() > 10 {
                                    println!("   ... and {} more topics", topics.len() - 10);
                                }
                            }
                        }
                    }
                } else {
                    println!("   ❌ No Kafka container found");
                }

                // Also show consumer groups if topics are requested
                println!("\n👥 Consumer Groups:");
                if let Some(ref container_name) = kafka_container {
                    let output = Command::new("docker")
                        .args([
                            "exec",
                            container_name,
                            "kafka-consumer-groups",
                            "--bootstrap-server",
                            "localhost:9092",
                            "--list",
                        ])
                        .output();

                    if let Ok(output) = output {
                        if output.status.success() {
                            let stdout = String::from_utf8_lossy(&output.stdout);
                            let groups: Vec<&str> =
                                stdout.lines().filter(|line| !line.is_empty()).collect();

                            if groups.is_empty() {
                                println!("   ⏸️  No active consumer groups");
                            } else {
                                for group in groups {
                                    println!("   👤 {}", group);
                                }
                            }
                        }
                    }
                } else {
                    println!("   ❌ No Kafka container found");
                }
            }
        }

        Commands::Start { duration } => {
            println!("🚀 Starting FerrisStreams demo ({}m duration)", duration);
            println!(
                "💡 Use 'cd demo/trading && DEMO_DURATION={} ./run_demo.sh'",
                duration
            );
        }

        Commands::Stop => {
            println!("🛑 Stopping FerrisStreams demo");
            println!("💡 Use 'cd demo/trading && ./stop_demo.sh'");
        }
    }

    Ok(())
}
