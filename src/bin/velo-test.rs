//! Velostream SQL Application Test Harness
//!
//! CLI tool for testing SQL applications with schema-driven data generation,
//! testcontainers-based execution, and comprehensive assertions.
//!
//! Usage:
//!   velo-test run app.sql --spec test_spec.yaml
//!   velo-test validate app.sql
//!   velo-test init app.sql --output test_spec.yaml
//!   velo-test infer-schema app.sql --data-dir data/ --output schemas/

// Command handlers module
mod velo_test;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Duration;

// File-only execution imports
use velostream::velostream::server::config::StreamJobServerConfig;
use velostream::velostream::server::stream_job_server::{JobStatus, StreamJobServer};
use velostream::velostream::sql::app_parser::SqlApplicationParser;
use velostream::velostream::sql::query_analyzer::{DataSinkType, DataSourceType};

#[derive(Parser)]
#[command(name = "velo-test")]
#[command(about = "Velostream SQL Application Test Harness")]
#[command(version = "0.1.0")]
#[command(author = "Velostream Team")]
#[command(after_help = r#"Common Flags (for 'run' command):
  --use-testcontainers    Auto-start Kafka via Docker (recommended for local dev)
  --reuse-containers      Reuse existing container (faster iteration)
  --spec <FILE>           Test specification YAML file
  --schemas <DIR>         Schema definitions directory
  --timeout-ms <MS>       Query timeout in milliseconds [default: 30000]
  -q, --query <NAME>      Run only a specific query

Examples:
  velo-test run app.sql --use-testcontainers
  velo-test run app.sql --spec test.yaml --reuse-containers
  velo-test quickstart app.sql
  velo-test validate app.sql"#)]
struct Cli {
    /// Enable verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Interactive quickstart wizard - guides you through testing a SQL file
    #[command(display_order = 1)]
    Quickstart {
        /// Path to the SQL file to test
        sql_file: PathBuf,
    },

    /// Run tests for a SQL application
    #[command(display_order = 5)]
    Run {
        /// Path to the SQL file to test
        sql_file: PathBuf,

        /// Path to the test specification YAML file (auto-discovered if not specified)
        #[arg(short, long)]
        spec: Option<PathBuf>,

        /// Directory containing schema definitions (auto-discovered if not specified)
        #[arg(long)]
        schemas: Option<PathBuf>,

        /// Run only a specific query by name
        #[arg(short, long)]
        query: Option<String>,

        /// Output format: text, json, junit
        #[arg(short, long, default_value = "text")]
        output: String,

        /// Timeout per query in milliseconds
        #[arg(long, default_value = "30000")]
        timeout_ms: u64,

        /// Use AI for failure analysis
        #[arg(long)]
        ai: bool,

        /// Auto-start Kafka using testcontainers (requires Docker)
        /// If not specified, will auto-start when no external Kafka is available
        #[arg(long)]
        use_testcontainers: bool,

        /// External Kafka bootstrap servers (overrides config file)
        #[arg(long)]
        kafka: Option<String>,

        /// Keep testcontainers running after test completion (for debugging)
        /// Use 'docker ps' to find the container, 'docker stop <id>' to clean up
        #[arg(long)]
        keep_containers: bool,

        /// Reuse existing test container if available (faster iteration)
        /// When enabled, looks for an existing velostream test container and reuses it
        /// instead of starting a new one. Much faster for iterative development.
        #[arg(long)]
        reuse_containers: bool,

        /// Execute statements one at a time (step mode)
        /// Pauses after each statement and displays results
        #[arg(long)]
        step: bool,

        /// Data-only mode: generate test data to Kafka without running SQL
        /// Use this when SQL jobs are deployed separately (e.g., via velo-sql deploy-app)
        #[arg(long)]
        data_only: bool,

        /// Skip interactive prompts and use auto-discovered defaults
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Debug SQL application with interactive stepping and breakpoints
    #[command(display_order = 6)]
    Debug {
        /// Path to the SQL file to debug
        sql_file: PathBuf,

        /// Path to the test specification YAML file (auto-discovered if not specified)
        #[arg(short, long)]
        spec: Option<PathBuf>,

        /// Directory containing schema definitions (auto-discovered if not specified)
        #[arg(long)]
        schemas: Option<PathBuf>,

        /// Set breakpoints on specific queries (can be used multiple times)
        #[arg(short, long)]
        breakpoint: Vec<String>,

        /// Timeout per query in milliseconds
        #[arg(long, default_value = "30000")]
        timeout_ms: u64,

        /// External Kafka bootstrap servers (overrides config file)
        #[arg(long)]
        kafka: Option<String>,

        /// Keep testcontainers running after completion
        #[arg(long)]
        keep_containers: bool,

        /// Reuse existing test container if available (faster iteration)
        #[arg(long)]
        reuse_containers: bool,

        /// Skip interactive prompts and use auto-discovered defaults
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Validate SQL syntax without execution
    #[command(display_order = 2)]
    Validate {
        /// Path to the SQL file to validate
        sql_file: PathBuf,

        /// Show verbose validation output
        #[arg(short, long)]
        verbose: bool,
    },

    /// Generate a test specification template from SQL file
    #[command(display_order = 3)]
    Init {
        /// Path to the SQL file to analyze
        sql_file: PathBuf,

        /// Output path for generated test_spec.yaml (default: test_spec.yaml in SQL file directory)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Use AI for intelligent assertion generation
        #[arg(long)]
        ai: bool,

        /// Skip interactive prompts and use defaults
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Infer schemas from SQL and data files
    #[command(display_order = 4)]
    InferSchema {
        /// Path to the SQL file to analyze
        sql_file: PathBuf,

        /// Directory containing CSV/JSON data files (default: data/ in SQL file directory)
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Output directory for generated schemas (default: schemas/ in SQL file directory)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Use AI for intelligent constraint inference
        #[arg(long)]
        ai: bool,

        /// Skip interactive prompts and use defaults
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Run all tests in a directory
    #[command(display_order = 7)]
    RunAll {
        /// Directory containing SQL apps and test specs
        #[arg(default_value = ".")]
        directory: PathBuf,

        /// Output format: text, json
        #[arg(short, long, default_value = "text")]
        output: String,

        /// Timeout per query in milliseconds
        #[arg(long, default_value = "90000")]
        timeout_ms: u64,

        /// Auto-start Kafka using testcontainers (requires Docker)
        #[arg(long)]
        use_testcontainers: bool,

        /// External Kafka bootstrap servers (overrides config file)
        #[arg(long)]
        kafka: Option<String>,

        /// Keep testcontainers running after test completion
        #[arg(long)]
        keep_containers: bool,

        /// Reuse existing test container if available (faster iteration)
        #[arg(long)]
        reuse_containers: bool,

        /// Pattern to match SQL files (default: apps/*.sql)
        #[arg(long)]
        pattern: Option<String>,

        /// Skip apps matching this pattern
        #[arg(long)]
        skip: Option<String>,
    },

    /// Run stress tests with high volume
    #[command(display_order = 8)]
    Stress {
        /// Path to the SQL file to test
        sql_file: PathBuf,

        /// Path to the test specification YAML file (auto-discovered if not specified)
        #[arg(short, long)]
        spec: Option<PathBuf>,

        /// Enable verbose output
        #[arg(short, long)]
        verbose: bool,

        /// Number of records to generate per source
        #[arg(long)]
        records: Option<usize>,

        /// Duration to run stress test in seconds
        #[arg(long)]
        duration: Option<u64>,

        /// Output format: text, json
        #[arg(short, long, default_value = "text")]
        output: String,

        /// Skip interactive prompts and use defaults
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Generate a velo-test.sh runner script for a project
    #[command(display_order = 9)]
    Scaffold {
        /// Directory to analyze and generate script for
        #[arg(default_value = ".")]
        directory: PathBuf,

        /// Output path for generated script (default: velo-test.sh in target directory)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Project name for script header (auto-detected from directory)
        #[arg(short, long)]
        name: Option<String>,

        /// Force a specific script style: simple, tiered, minimal
        #[arg(long)]
        style: Option<String>,

        /// Relative path to velo-test binary
        #[arg(long, default_value = "../../target/release/velo-test")]
        velo_test_path: String,

        /// Skip interactive prompts and use defaults
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Generate SQL annotations and monitoring infrastructure
    #[command(display_order = 8)]
    Annotate {
        /// Path to the SQL file to analyze
        sql_file: PathBuf,

        /// Output path for annotated SQL (default: adds .annotated.sql suffix)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Application name (auto-detected from filename)
        #[arg(short, long)]
        name: Option<String>,

        /// Application version
        #[arg(long)]
        version: Option<String>,

        /// Generate monitoring infrastructure (Prometheus, Grafana, Tempo)
        #[arg(long)]
        monitoring: Option<PathBuf>,

        /// Prometheus metrics port
        #[arg(long)]
        prometheus_port: Option<u16>,

        /// Telemetry port
        #[arg(long)]
        telemetry_port: Option<u16>,

        /// Skip interactive prompts and use defaults
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Check health of Velostream infrastructure (Docker, Kafka, topics, processes)
    #[command(display_order = 10)]
    Health {
        /// Kafka bootstrap servers (e.g., localhost:9092)
        #[arg(short, long)]
        broker: Option<String>,

        /// Output format: text, json
        #[arg(short, long, default_value = "text")]
        output: String,

        /// Specific checks to run (comma-separated): docker,kafka,topics,processes,consumers
        /// If not specified, runs all checks
        #[arg(short, long)]
        check: Option<String>,

        /// Timeout for operations in seconds
        #[arg(long, default_value = "10")]
        timeout: u64,

        /// Container names to check (comma-separated, default: simple-kafka,simple-zookeeper)
        #[arg(long)]
        containers: Option<String>,

        /// Topic names to check (comma-separated, default: all topics)
        #[arg(long)]
        topics: Option<String>,
    },
}

/// Build information constants
const VERSION: &str = env!("CARGO_PKG_VERSION");
const BUILD_TIME: &str = env!("BUILD_TIME");
const GIT_HASH: &str = env!("GIT_HASH");
const GIT_BRANCH: &str = env!("GIT_BRANCH");

/// Print version and build information
fn print_version_info() {
    println!(
        "velo-test v{} ({} @ {}) built {}",
        VERSION, GIT_HASH, GIT_BRANCH, BUILD_TIME
    );
}

/// Helper module for auto-discovery and interactive prompts
mod discovery {
    use std::io::{BufRead, Write};
    use std::path::{Path, PathBuf};

    /// Prompt user for a value with a default
    pub fn prompt_with_default(prompt: &str, default: &str) -> String {
        print!("{} [{}]: ", prompt, default);
        std::io::stdout().flush().unwrap();
        let mut input = String::new();
        std::io::stdin().lock().read_line(&mut input).unwrap();
        let trimmed = input.trim();
        if trimmed.is_empty() {
            default.to_string()
        } else {
            trimmed.to_string()
        }
    }

    /// Prompt user for yes/no with a default
    pub fn prompt_yes_no(prompt: &str, default: bool) -> bool {
        let default_str = if default { "Y/n" } else { "y/N" };
        print!("{} [{}]: ", prompt, default_str);
        std::io::stdout().flush().unwrap();
        let mut input = String::new();
        std::io::stdin().lock().read_line(&mut input).unwrap();
        let trimmed = input.trim().to_lowercase();
        if trimmed.is_empty() {
            default
        } else {
            trimmed == "y" || trimmed == "yes"
        }
    }

    /// Auto-discover test spec file relative to SQL file
    pub fn discover_spec(sql_file: &Path) -> Option<PathBuf> {
        let sql_dir = sql_file.parent()?;
        let file_stem = sql_file.file_stem()?.to_string_lossy();

        // Check common spec file names in order of preference
        // App-specific specs take priority over generic test_spec.yaml
        let mut candidates = vec![
            // App-specific specs in same directory
            sql_dir.join(format!("{}.test.yaml", file_stem)),
            sql_dir.join(format!("{}.spec.yaml", file_stem)),
            // Generic spec in same directory
            sql_dir.join("test_spec.yaml"),
            sql_dir.join("test_spec.yml"),
        ];

        // Also check parent directory and its tests/ subdirectory
        // App-specific specs in tests/ take priority over generic test_spec.yaml
        if let Some(parent) = sql_dir.parent() {
            candidates.insert(2, parent.join(format!("tests/{}.test.yaml", file_stem)));
            candidates.insert(3, parent.join(format!("tests/{}.spec.yaml", file_stem)));
            candidates.push(parent.join("test_spec.yaml"));
        }

        candidates.into_iter().find(|p| p.exists())
    }

    /// Auto-discover schemas directory relative to SQL file
    pub fn discover_schemas(sql_file: &Path) -> Option<PathBuf> {
        let sql_dir = sql_file.parent()?;

        // Check common schema directory names
        let candidates = [
            sql_dir.join("schemas"),
            sql_dir.join("schema"),
            sql_dir.parent()?.join("schemas"),
        ];

        candidates.into_iter().find(|p| p.exists() && p.is_dir())
    }

    /// Auto-discover data directory relative to SQL file
    pub fn discover_data_dir(sql_file: &Path) -> Option<PathBuf> {
        let sql_dir = sql_file.parent()?;

        // Check common data directory names
        let candidates = [sql_dir.join("data"), sql_dir.parent()?.join("data")];

        candidates.into_iter().find(|p| p.exists() && p.is_dir())
    }

    /// Get default output path for test spec
    pub fn default_spec_output(sql_file: &Path) -> PathBuf {
        sql_file
            .parent()
            .map(|p| p.join("test_spec.yaml"))
            .unwrap_or_else(|| PathBuf::from("test_spec.yaml"))
    }

    /// Get default output path for schemas
    pub fn default_schemas_output(sql_file: &Path) -> PathBuf {
        sql_file
            .parent()
            .map(|p| p.join("schemas"))
            .unwrap_or_else(|| PathBuf::from("schemas"))
    }

    /// Offer to save an auto-generated test spec
    /// Returns the path if saved, None otherwise
    pub fn offer_save_spec(
        sql_file: &Path,
        spec: &velostream::velostream::test_harness::spec::TestSpec,
        yes: bool,
    ) -> Option<PathBuf> {
        // Determine default output path
        let default_path = sql_file
            .parent()
            .map(|p| {
                p.join(format!(
                    "{}.test.yaml",
                    sql_file.file_stem().unwrap_or_default().to_string_lossy()
                ))
            })
            .unwrap_or_else(|| PathBuf::from("test_spec.yaml"));

        // In non-interactive mode, don't save unless explicitly requested
        if yes {
            return None;
        }

        // Ask if user wants to save the spec
        let save = prompt_yes_no(
            &format!("Save generated test spec to {}?", default_path.display()),
            false,
        );

        if !save {
            return None;
        }

        // Allow user to customize path
        let path_str = prompt_with_default("Output path", &default_path.to_string_lossy());
        let output_path = PathBuf::from(path_str);

        // Serialize and save
        match serde_yaml::to_string(spec) {
            Ok(yaml) => {
                // Add header comment
                let content = format!(
                    "# Auto-generated test spec for {}\n# Generated by velo-test\n\n{}",
                    sql_file.file_name().unwrap_or_default().to_string_lossy(),
                    yaml
                );

                match std::fs::write(&output_path, content) {
                    Ok(()) => {
                        println!("✅ Saved test spec to {}", output_path.display());
                        Some(output_path)
                    }
                    Err(e) => {
                        eprintln!("⚠️  Failed to save spec: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                eprintln!("⚠️  Failed to serialize spec: {}", e);
                None
            }
        }
    }
}

/// Execute file-only SQL queries without Kafka infrastructure.
/// Returns Ok(true) if file-only execution was performed, Ok(false) if Kafka is needed.
async fn run_file_only_execution(
    sql_file: &PathBuf,
    validation_result: &velostream::velostream::sql::validator::ApplicationValidationResult,
    timeout_ms: u64,
    test_spec: Option<&velostream::velostream::test_harness::spec::TestSpec>,
    spec_dir: Option<&std::path::Path>,
) -> Result<bool, Box<dyn std::error::Error>> {
    // Check if all sources and sinks are file-based
    // Note: .all() returns true for empty iterators, so we must check for non-empty sources/sinks
    let is_file_only = validation_result.query_results.iter().all(|qr| {
        // Must have at least one source or sink to be considered file-only
        let has_sources_or_sinks = !qr.sources_found.is_empty() || !qr.sinks_found.is_empty();

        // All sources must be file-based (or no sources)
        let all_sources_file = qr
            .sources_found
            .iter()
            .all(|s| s.source_type == DataSourceType::File);

        // All sinks must be file-based (or no sinks)
        let all_sinks_file = qr
            .sinks_found
            .iter()
            .all(|s| s.sink_type == DataSinkType::File);

        has_sources_or_sinks && all_sources_file && all_sinks_file
    });

    // Also check that we have at least one query result with sources/sinks
    let has_any_query = !validation_result.query_results.is_empty();

    if !is_file_only || !has_any_query {
        return Ok(false);
    }

    println!("📁 File-only mode detected (no Kafka required)");
    println!();

    // Get SQL directory for resolving relative paths
    let sql_dir = sql_file
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());

    // Load and parse SQL
    let sql_content = std::fs::read_to_string(sql_file)?;
    let app = SqlApplicationParser::new().parse_application(&sql_content)?;

    println!("🚀 Executing file-only queries...");
    println!();

    let start_time = std::time::Instant::now();

    // Create StreamJobServer (Kafka config unused for file I/O)
    let config = StreamJobServerConfig::new(
        "localhost:9092".to_string(), // Placeholder - not used for file sources/sinks
        "velo-test-file".to_string(),
    )
    .with_max_jobs(10)
    .with_base_dir(&sql_dir);

    let server = StreamJobServer::with_config(config);

    // Deploy the SQL application
    let source_filename = sql_file
        .file_name()
        .and_then(|n| n.to_str())
        .map(String::from);

    let job_names = server
        .deploy_sql_application_with_filename(app, None, source_filename)
        .await?;

    println!("   ✅ Deployed {} jobs: {:?}", job_names.len(), job_names);

    // Wait for jobs to complete (file sources are finite)
    let timeout = Duration::from_millis(timeout_ms);
    let wait_start = std::time::Instant::now();

    loop {
        if wait_start.elapsed() > timeout {
            println!("   ⏱️  Timeout waiting for completion");
            break;
        }

        let jobs = server.list_jobs().await;
        // For file-based jobs, completion is signaled by:
        // 1. Status transitions to Stopped/Failed, OR
        // 2. Processing has finished (records_processed > 0 and total_processing_time > 0)
        let all_done = !jobs.is_empty()
            && jobs.iter().all(|j| {
                matches!(j.status, JobStatus::Stopped | JobStatus::Failed(_))
                    || (j.stats.records_processed > 0
                        && j.stats.total_processing_time > Duration::ZERO)
            });

        if all_done {
            for job in &jobs {
                let (icon, status_str) = if job.stats.total_processing_time > Duration::ZERO
                    && job.stats.records_processed > 0
                {
                    ("✅", "completed")
                } else {
                    match &job.status {
                        JobStatus::Stopped => ("✅", "completed"),
                        JobStatus::Failed(_) => ("❌", "failed"),
                        JobStatus::Running => ("🔄", "running"),
                        JobStatus::Starting => ("🚀", "starting"),
                        JobStatus::Paused => ("⏸️", "paused"),
                    }
                };
                println!(
                    "   {} {}: {} records in {:.2}s ({})",
                    icon,
                    job.name,
                    job.stats.records_processed,
                    job.stats.total_processing_time.as_secs_f64(),
                    status_str
                );
            }
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!();
    println!(
        "🎉 File-only execution completed in {:?}",
        start_time.elapsed()
    );
    println!();

    // Check output files
    println!("🔍 Checking output files...");
    for qr in &validation_result.query_results {
        for sink in &qr.sinks_found {
            if let Some(path) = sink
                .properties
                .get("path")
                .or_else(|| sink.properties.get("sink.path"))
            {
                let output_path = if path.starts_with("./") || path.starts_with("../") {
                    sql_dir.join(path)
                } else {
                    PathBuf::from(path)
                };

                if output_path.exists() {
                    let size = std::fs::metadata(&output_path)
                        .map(|m| m.len())
                        .unwrap_or(0);
                    println!("   ✅ {} ({} bytes)", output_path.display(), size);
                } else {
                    println!("   ⚠️  {} (not found)", output_path.display());
                }
            }
        }
    }

    // Run assertions from test spec (if provided)
    if let Some(spec) = test_spec {
        use velostream::velostream::test_harness::assertions::AssertionRunner;
        use velostream::velostream::test_harness::executor::CapturedOutput;

        let assertion_base_dir = spec_dir.unwrap_or(&sql_dir);
        let assertion_runner =
            AssertionRunner::new().with_base_dir(assertion_base_dir.to_path_buf());

        println!();
        println!("🔍 Running assertions...");

        let mut total_assertions = 0;
        let mut passed_assertions = 0;
        let mut failed_assertions = 0;

        for query_spec in &spec.queries {
            if query_spec.skip {
                continue;
            }

            let all_assertions = query_spec.all_assertions();
            if all_assertions.is_empty() {
                continue;
            }

            println!(
                "   Query '{}': {} assertions",
                query_spec.name,
                all_assertions.len()
            );

            // Create an empty captured output (file assertions don't need captured records)
            let empty_output = CapturedOutput {
                query_name: query_spec.name.clone(),
                sink_name: String::new(),
                topic: None,
                records: Vec::new(),
                execution_time_ms: 0,
                warnings: Vec::new(),
                memory_peak_bytes: None,
                memory_growth_bytes: None,
            };

            // Convert Vec<&AssertionConfig> to Vec<AssertionConfig> for the runner
            let owned_assertions: Vec<_> = all_assertions.into_iter().cloned().collect();
            let results = assertion_runner.run_assertions(&empty_output, &owned_assertions);
            for result in &results {
                total_assertions += 1;
                if result.passed {
                    passed_assertions += 1;
                    println!("      ✅ {}: {}", result.assertion_type, result.message);
                } else {
                    failed_assertions += 1;
                    println!("      ❌ {}: {}", result.assertion_type, result.message);
                    if !result.details.is_empty() {
                        for (key, value) in &result.details {
                            println!("         {}: {}", key, value);
                        }
                    }
                }
            }
        }

        println!();
        println!(
            "📊 Assertion Summary: {} passed, {} failed, {} total",
            passed_assertions, failed_assertions, total_assertions
        );

        if failed_assertions > 0 {
            println!("❌ Some assertions failed!");
            std::process::exit(1);
        } else if total_assertions > 0 {
            println!("✅ All assertions passed!");
        }
    }

    Ok(true)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging with capture support for debugger step execution
    // This wraps env_logger and captures ERROR/WARN logs to a ring buffer
    velostream::velostream::test_harness::init_capturing_logger();

    // Print version info at startup
    print_version_info();
    println!();

    let cli = Cli::parse();

    match cli.command {
        Commands::Quickstart { sql_file } => velo_test::commands::quickstart(sql_file).await?,

        Commands::Run {
            sql_file,
            spec,
            schemas,
            query,
            output,
            timeout_ms,
            ai,
            use_testcontainers,
            kafka,
            keep_containers,
            reuse_containers,
            step,
            data_only,
            yes,
        } => {
            velo_test::commands::run(
                sql_file,
                spec,
                schemas,
                query,
                output,
                timeout_ms,
                ai,
                use_testcontainers,
                kafka,
                keep_containers,
                reuse_containers,
                step,
                data_only,
                yes,
            )
            .await?
        }

        Commands::Validate { sql_file, verbose } => {
            velo_test::commands::validate(sql_file, verbose).await?
        }

        Commands::Init {
            sql_file,
            output,
            ai,
            yes,
        } => velo_test::commands::init(sql_file, output, ai, yes).await?,

        Commands::InferSchema {
            sql_file,
            data_dir,
            output,
            ai,
            yes,
        } => velo_test::commands::infer_schema(sql_file, data_dir, output, ai, yes).await?,

        Commands::RunAll {
            directory,
            output,
            timeout_ms,
            use_testcontainers,
            kafka,
            keep_containers,
            reuse_containers,
            pattern,
            skip,
        } => {
            velo_test::commands::run_all(
                directory,
                output,
                timeout_ms,
                use_testcontainers,
                kafka,
                keep_containers,
                reuse_containers,
                pattern,
                skip,
            )
            .await?
        }

        Commands::Stress {
            sql_file,
            spec,
            verbose: _,
            records,
            duration,
            output,
            yes,
        } => velo_test::commands::stress(sql_file, spec, records, duration, output, yes)?,

        Commands::Debug {
            sql_file,
            spec,
            schemas,
            breakpoint,
            timeout_ms,
            kafka,
            keep_containers,
            reuse_containers,
            yes,
        } => {
            velo_test::commands::debug(
                sql_file,
                spec,
                schemas,
                breakpoint,
                timeout_ms,
                kafka,
                keep_containers,
                reuse_containers,
                yes,
            )
            .await?
        }

        Commands::Scaffold {
            directory,
            output,
            name,
            style,
            velo_test_path,
            yes,
        } => velo_test::commands::scaffold(directory, output, name, style, velo_test_path, yes)?,

        Commands::Annotate {
            sql_file,
            output,
            name,
            version,
            monitoring,
            prometheus_port,
            telemetry_port,
            yes,
        } => {
            velo_test::commands::annotate(
                sql_file,
                output,
                name,
                version,
                monitoring,
                prometheus_port,
                telemetry_port,
                yes,
            )
            .await?
        }

        Commands::Health {
            broker,
            output,
            check,
            timeout,
            containers,
            topics,
        } => {
            velo_test::commands::health(broker, output, check, timeout, containers, topics).await?
        }
    }

    Ok(())
}
