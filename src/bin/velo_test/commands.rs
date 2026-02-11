//! Command handlers for velo-test CLI
//!
//! This module contains the implementation of all velo-test subcommands,
//! extracted from the main() function for better organization and maintainability.

use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

// Test harness imports for Run command
use velostream::velostream::test_harness::SpecGenerator;
use velostream::velostream::test_harness::assertions::AssertionRunner;
use velostream::velostream::test_harness::config_override::ConfigOverrideBuilder;
use velostream::velostream::test_harness::executor::QueryExecutor;
use velostream::velostream::test_harness::infra::TestHarnessInfra;
use velostream::velostream::test_harness::report::{OutputFormat, ReportGenerator, write_report};
use velostream::velostream::test_harness::schema::SchemaRegistry;
use velostream::velostream::test_harness::spec::TestSpec;

// Command handler functions will be added here
// Each function will handle one Commands:: variant from the CLI

/// Type alias for command results
pub type CommandResult = Result<(), Box<dyn Error>>;
/// Run tests for a SQL application
///
/// This function executes the full test harness workflow:
/// 1. Load or generate test specification
/// 2. Load schemas
/// 3. Validate SQL
/// 4. Create test infrastructure (Kafka via testcontainers or external)
/// 5. Execute queries
/// 6. Run assertions
/// 7. Generate report
/// 8. Cleanup infrastructure
///
/// # Arguments
/// * `sql_file` - Path to the SQL file to test
/// * `spec` - Optional path to test specification YAML
/// * `schemas` - Optional directory containing schema definitions
/// * `query` - Optional filter to run only a specific query
/// * `output` - Output format (text, json, junit)
/// * `timeout_ms` - Timeout per query in milliseconds
/// * `ai` - Enable AI for failure analysis
/// * `use_testcontainers` - Auto-start Kafka using testcontainers
/// * `kafka` - External Kafka bootstrap servers
/// * `keep_containers` - Keep containers running after completion
/// * `reuse_containers` - Reuse existing containers
/// * `step` - Execute statements one at a time
/// * `data_only` - Generate test data without running SQL
/// * `yes` - Skip interactive prompts
#[allow(clippy::too_many_arguments)]
pub async fn run(
    sql_file: PathBuf,
    spec: Option<PathBuf>,
    schemas: Option<PathBuf>,
    query: Option<String>,
    output: String,
    timeout_ms: u64,
    ai: bool,
    use_testcontainers: bool,
    kafka: Option<String>,
    keep_containers: bool,
    reuse_containers: bool,
    step: bool,
    data_only: bool,
    yes: bool,
) -> CommandResult {
    println!("🧪 Velostream SQL Test Harness");
    println!("════════════════════════════════════════");

    // Auto-resolve .sql extension if file doesn't exist
    let sql_file = if !sql_file.exists() && sql_file.extension().is_none() {
        let with_ext = sql_file.with_extension("sql");
        if with_ext.exists() {
            with_ext
        } else {
            sql_file
        }
    } else {
        sql_file
    };

    println!("SQL File: {}", sql_file.display());

    // Auto-discover spec file if not provided
    let spec = spec.or_else(|| {
        let discovered = crate::discovery::discover_spec(&sql_file);
        if let Some(ref p) = discovered {
            println!("📁 Auto-discovered spec: {}", p.display());
        }
        discovered
    });

    // Auto-discover schemas directory if not provided
    let schemas = schemas.or_else(|| {
        let discovered = crate::discovery::discover_schemas(&sql_file);
        if let Some(ref p) = discovered {
            println!("📁 Auto-discovered schemas: {}", p.display());
        }
        discovered
    });

    // Interactive mode: prompt for missing options if not using -y
    let spec = if spec.is_none() && !yes {
        let default_spec = crate::discovery::default_spec_output(&sql_file);
        if crate::discovery::prompt_yes_no("No test spec found. Generate from SQL?", true) {
            None // Will auto-generate later
        } else {
            let path = crate::discovery::prompt_with_default(
                "Spec file path",
                &default_spec.display().to_string(),
            );
            let p = PathBuf::from(path);
            if p.exists() { Some(p) } else { None }
        }
    } else {
        spec
    };

    if let Some(ref s) = spec {
        println!("Test Spec: {}", s.display());
    }
    if let Some(ref s) = schemas {
        println!("Schemas: {}", s.display());
    }
    if let Some(ref q) = query {
        println!("Query Filter: {}", q);
    }
    println!("Output Format: {}", output);
    println!("Timeout: {}ms", timeout_ms);
    if ai {
        println!("AI Analysis: enabled");
    }
    println!();

    // Step 1: Load or generate test spec
    let test_spec: TestSpec = if let Some(ref spec_path) = spec {
        let content = match std::fs::read_to_string(spec_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("❌ Failed to read test spec: {}", e);
                std::process::exit(1);
            }
        };
        match serde_yaml::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("❌ Failed to parse test spec: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        // Generate from SQL file
        let generator = SpecGenerator::new();
        match generator.generate_from_sql(&sql_file) {
            Ok(s) => {
                println!("📝 Auto-generated test spec from SQL");
                // Offer to save the generated spec
                crate::discovery::offer_save_spec(&sql_file, &s, yes);
                s
            }
            Err(e) => {
                eprintln!("❌ Failed to analyze SQL file: {}", e);
                std::process::exit(1);
            }
        }
    };

    println!("📊 Test Configuration:");
    println!("   Application: {}", test_spec.application);
    println!("   Queries: {}", test_spec.queries.len());
    println!();

    // Step 2: Load schemas if provided
    let mut schema_registry = SchemaRegistry::new();
    if let Some(ref schema_dir) = schemas
        && schema_dir.exists()
    {
        println!("📁 Loading schemas from {}", schema_dir.display());
        if let Ok(entries) = std::fs::read_dir(schema_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path
                    .extension()
                    .is_some_and(|ext| ext == "yaml" || ext == "yml")
                {
                    match std::fs::read_to_string(&path) {
                        Ok(content) => {
                            match serde_yaml::from_str::<velostream::velostream::test_harness::Schema>(
                                &content,
                            ) {
                                Ok(mut schema) => {
                                    schema.source_path = Some(path.display().to_string());
                                    let key_info = schema
                                        .key_field
                                        .as_ref()
                                        .map(|k| format!(", key_field: {}", k))
                                        .unwrap_or_default();
                                    println!(
                                        "   ✓ {} ({} fields{}) ← {}",
                                        schema.name,
                                        schema.fields.len(),
                                        key_info,
                                        path.display()
                                    );
                                    schema_registry.register(schema);
                                }
                                Err(e) => {
                                    eprintln!("   ⚠️  Failed to parse {}: {}", path.display(), e);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("   ⚠️  Failed to read {}: {}", path.display(), e);
                        }
                    }
                }
            }
        }
    }

    // Step 2b: Generate schemas from @data.* hints in SQL file
    // Only generate for sources that don't already have a schema file
    {
        use velostream::velostream::test_harness::{DataHintParser, generate_schema_from_hints};

        let sql_content = std::fs::read_to_string(&sql_file).unwrap_or_default();
        let mut hint_parser = DataHintParser::new();

        if hint_parser.parse(&sql_content).is_ok() {
            let field_hints = hint_parser.get_field_hints();
            let global_hints = hint_parser.get_global_hints();

            if !field_hints.is_empty() {
                // Determine source name from hints or default
                let source_name = global_hints
                    .source_name
                    .clone()
                    .unwrap_or_else(|| "generated_source".to_string());

                // Only generate if no schema already exists for this source
                if schema_registry.get(&source_name).is_none() {
                    match generate_schema_from_hints(&source_name, global_hints, &field_hints) {
                        Ok(schema) => {
                            println!(
                                "   ✓ {} ({} fields) ← @data.* hints in SQL",
                                schema.name,
                                schema.fields.len()
                            );
                            schema_registry.register(schema);
                        }
                        Err(e) => {
                            eprintln!("   ⚠️  Failed to generate schema from hints: {}", e);
                        }
                    }
                }
            }
        }
    }
    println!();

    // Step 3: Validate SQL and extract configuration
    println!("🔧 Initializing test infrastructure...");
    use velostream::velostream::sql::validator::SqlValidator;
    // Use the SQL file's directory as base for resolving relative config_file paths
    // This allows SQL files to use paths like '../configs/source.yaml' correctly
    // Canonicalize to get absolute path, so relative paths like '../configs/' resolve correctly
    let sql_dir = sql_file
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
    let validator = SqlValidator::with_base_dir(&sql_dir);
    let validation_result = validator.validate_application_file(&sql_file);

    // Extract bootstrap.servers and schema files from the validated sources
    let mut bootstrap_servers: Option<String> = None;

    // Keys to check for bootstrap servers (in order of preference)
    let bootstrap_keys = [
        "bootstrap.servers",
        "brokers",
        "datasource.consumer_config.bootstrap.servers",
        "datasource.config.bootstrap.servers",
        "consumer_config.bootstrap.servers",
    ];

    for query_result in &validation_result.query_results {
        for source in &query_result.sources_found {
            // Extract bootstrap.servers from any of the possible key locations
            if bootstrap_servers.is_none() {
                for key in &bootstrap_keys {
                    if let Some(bs) = source.properties.get(*key) {
                        // Check if this is a ${VAR:default} pattern where the env var is not set
                        // If so, we should start testcontainers (leave bootstrap_servers as None)
                        let resolved = if bs.contains("${") && bs.contains("}") {
                            // Use runtime env var substitution
                            use velostream::velostream::sql::config::substitute_env_vars;
                            let substituted = substitute_env_vars(bs);
                            // If the substitution resulted in the default value (e.g. localhost:9092/9999),
                            // it means the env var wasn't set - we should use testcontainers
                            if substituted.starts_with("localhost:") {
                                log::info!(
                                    "bootstrap.servers '{}' resolved to default '{}' - will use testcontainers",
                                    bs,
                                    substituted
                                );
                                None // Trigger testcontainers
                            } else {
                                log::info!(
                                    "bootstrap.servers '{}' resolved to '{}' from env var",
                                    bs,
                                    substituted
                                );
                                Some(substituted)
                            }
                        } else {
                            // Plain value, use as-is
                            Some(bs.clone())
                        };

                        if let Some(bs_resolved) = resolved {
                            bootstrap_servers = Some(bs_resolved.clone());
                            log::info!(
                                "Found bootstrap.servers from source '{}' (key: {}): {}",
                                source.name,
                                key,
                                bs_resolved
                            );
                        }
                        break;
                    }
                }
            }

            // Extract and load schema file if specified
            // Supports both 'datasource.schema.value.schema.file' and shorter variants
            let schema_file_keys = [
                "datasource.schema.value.schema.file",
                "schema.file",
                "value.schema.file",
            ];
            for key in &schema_file_keys {
                if let Some(schema_path) = source.properties.get(*key) {
                    // Use the testable helper function for path resolution
                    let full_path =
                        velostream::velostream::test_harness::utils::resolve_schema_path(
                            schema_path,
                        );
                    log::info!(
                        "Loading schema for source '{}' from: {}",
                        source.name,
                        full_path.display()
                    );
                    if full_path.exists() {
                        if let Ok(content) = std::fs::read_to_string(&full_path) {
                            match serde_yaml::from_str::<velostream::velostream::test_harness::Schema>(
                                &content,
                            ) {
                                Ok(mut schema) => {
                                    schema.source_path = Some(full_path.display().to_string());
                                    let key_info = schema
                                        .key_field
                                        .as_ref()
                                        .map(|k| format!(", key_field: {}", k))
                                        .unwrap_or_default();
                                    println!(
                                        "   ✓ {} ({} fields{}) ← {} (from SQL config)",
                                        schema.name,
                                        schema.fields.len(),
                                        key_info,
                                        full_path.display()
                                    );
                                    schema_registry.register(schema);
                                }
                                Err(e) => {
                                    log::warn!(
                                        "Failed to parse schema file '{}': {}",
                                        full_path.display(),
                                        e
                                    );
                                }
                            }
                        }
                    } else {
                        log::warn!(
                            "Schema file not found: {} (resolved from '{}')",
                            full_path.display(),
                            schema_path
                        );
                    }
                    break; // Found a schema file key, stop checking others
                }
            }
        }
    }

    // Try file-only execution (no Kafka needed)
    let spec_dir_path = spec
        .as_ref()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf());
    if crate::run_file_only_execution(
        &sql_file,
        &validation_result,
        timeout_ms,
        Some(&test_spec),
        spec_dir_path.as_deref(),
    )
    .await?
    {
        return Ok(());
    }

    // Priority for Kafka connection:
    // 1. --kafka CLI flag (explicit override)
    // 2. Config file bootstrap.servers
    // 3. KAFKA_BOOTSTRAP_SERVERS env var
    // 4. --use-testcontainers flag (or auto-start if none of above)

    // Check CLI flag first
    if let Some(ref k) = kafka {
        bootstrap_servers = Some(k.clone());
        log::info!("Using bootstrap.servers from --kafka flag: {}", k);
    }

    // Check environment variable as fallback
    if bootstrap_servers.is_none()
        && let Ok(bs) = std::env::var("KAFKA_BOOTSTRAP_SERVERS")
    {
        bootstrap_servers = Some(bs);
        log::info!("Using bootstrap.servers from KAFKA_BOOTSTRAP_SERVERS env var");
    }

    // Create test infrastructure
    // If --use-testcontainers or no Kafka configured, start testcontainers
    let mut infra = if use_testcontainers || bootstrap_servers.is_none() {
        if reuse_containers {
            println!("♻️  Looking for reusable Kafka container...");
        } else {
            println!("🐳 Starting Kafka via testcontainers (requires Docker)...");
        }
        match TestHarnessInfra::with_testcontainers_reuse(reuse_containers).await {
            Ok(infra) => {
                let mode = if reuse_containers {
                    "reused"
                } else {
                    "testcontainers"
                };
                println!(
                    "   Kafka: {} ({})",
                    infra.bootstrap_servers().unwrap_or("unknown"),
                    mode
                );
                infra
            }
            Err(e) => {
                if bootstrap_servers.is_some() {
                    // Fall back to configured Kafka if testcontainers fails
                    eprintln!("⚠️  Testcontainers failed ({}), using configured Kafka", e);
                    TestHarnessInfra::with_kafka(bootstrap_servers.as_ref().unwrap())
                } else {
                    eprintln!("❌ Failed to start testcontainers Kafka: {}", e);
                    eprintln!("   Make sure Docker is running and try again");
                    eprintln!();
                    eprintln!(
                        "   Alternatively, specify --kafka <bootstrap_servers> to use an external Kafka"
                    );
                    std::process::exit(1);
                }
            }
        }
    } else if let Some(ref bs) = bootstrap_servers {
        println!("   Kafka: {}", bs);
        TestHarnessInfra::with_kafka(bs)
    } else {
        TestHarnessInfra::new()
    };

    // Start the infrastructure (creates topics, etc.)
    if let Err(e) = infra.start().await {
        eprintln!("❌ Failed to start test infrastructure: {}", e);
        // Cleanup testcontainers before exiting (unless --keep-containers)
        if !keep_containers {
            let _ = infra.stop().await;
        }
        std::process::exit(1);
    }

    // Set VELOSTREAM_KAFKA_BROKERS env var to override config file bootstrap.servers
    // This ensures SQL job sinks use the same Kafka as the test harness
    // SAFETY: This is safe because we are single-threaded at this point in startup,
    // before any async tasks or threads are spawned that might read env vars.
    if let Some(bs) = infra.bootstrap_servers() {
        unsafe {
            std::env::set_var("VELOSTREAM_KAFKA_BROKERS", bs);
        }
        log::info!("Set VELOSTREAM_KAFKA_BROKERS={}", bs);
    }

    // Check if Kafka is available
    if infra.bootstrap_servers().is_none() {
        eprintln!("⚠️  No Kafka bootstrap servers configured");
        eprintln!("   Use --use-testcontainers to auto-start Kafka (requires Docker)");
        eprintln!("   Or specify --kafka <bootstrap_servers> to use an external Kafka");
        eprintln!();
        eprintln!("   Running in validation-only mode (no execution)");
        println!();

        // Use the validation result we already have
        let result = validation_result;

        if result.is_valid {
            println!(
                "✅ SQL validation passed ({} queries)",
                result.valid_queries
            );
        } else {
            println!("❌ SQL validation failed");
            // Cleanup testcontainers before exiting (unless --keep-containers)
            if !keep_containers {
                let _ = infra.stop().await;
            }
            std::process::exit(1);
        }
        // Cleanup infra even on success in validation-only mode (unless --keep-containers)
        if !keep_containers {
            let _ = infra.stop().await;
        }
        return Ok(());
    } else {
        // Step 4: Create config overrides
        let run_id = format!(
            "{:08x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u32
        );
        // Get SQL file's parent directory for resolving relative config file paths
        let sql_dir = sql_file
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .canonicalize()
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());

        // Configure test infrastructure overrides (bootstrap servers, config paths, etc.)
        let overrides = ConfigOverrideBuilder::new(&run_id)
            .bootstrap_servers(infra.bootstrap_servers().unwrap())
            .base_dir(sql_dir.clone())
            .build();

        // Determine the spec directory for resolving relative file paths
        // If a spec file was provided, use its parent directory; otherwise use the SQL file's directory
        let spec_dir = spec
            .as_ref()
            .and_then(|p| p.parent())
            .unwrap_or(&sql_dir)
            .to_path_buf();

        // Step 5: Create executor with StreamJobServer for SQL execution
        let executor = QueryExecutor::new(infra)
            .with_timeout(Duration::from_millis(timeout_ms))
            .with_overrides(overrides)
            .with_schema_registry(schema_registry)
            .with_spec_dir(&spec_dir);

        // Check if any queries have metric assertions - if so, enable metrics collection
        let has_metric_assertions = test_spec
            .queries
            .iter()
            .any(|q| !q.metric_assertions.is_empty());

        // Initialize StreamJobServer for actual SQL execution (skip in data_only mode)
        // Pass SQL file's parent directory so the server can resolve relative config file paths
        // Enable metrics collection if any queries have metric_assertions
        let mut executor = if data_only {
            println!("   SQL execution: disabled (--data-only mode)");
            println!("   Data will be published to Kafka for external SQL processing");
            executor
        } else {
            match executor
                .with_server_and_observability(Some(sql_dir), has_metric_assertions)
                .await
            {
                Ok(e) => {
                    if has_metric_assertions {
                        println!(
                            "   SQL execution: enabled with metrics (in-process StreamJobServer)"
                        );
                    } else {
                        println!("   SQL execution: enabled (in-process StreamJobServer)");
                    }
                    e
                }
                Err(e) => {
                    eprintln!("⚠️  Warning: Failed to initialize SQL server: {}", e);
                    eprintln!("   Running in data-only mode (no SQL execution)");
                    // Can't recover the executor after with_server() fails, so recreate
                    QueryExecutor::new(TestHarnessInfra::with_kafka(
                        bootstrap_servers.as_ref().unwrap(),
                    ))
                    .with_timeout(Duration::from_millis(timeout_ms))
                    .with_spec_dir(&spec_dir)
                }
            }
        };

        // Step 5b: Load and parse SQL file to get sink topic info
        if let Err(e) = executor.load_sql_file(&sql_file) {
            eprintln!("⚠️  Warning: Failed to parse SQL file: {}", e);
            // Continue anyway - will fall back to default topic naming
        }

        // Handle step mode with StatementExecutor (not available in data_only mode)
        if step && !data_only {
            use std::io::{BufRead, Write};
            use velostream::velostream::test_harness::statement_executor::{
                ExecutionMode, SessionState, StatementExecutor,
            };

            println!();
            println!("🐛 Step Mode - Execute statements one at a time");
            println!("   Commands: [Enter]=step, q=quit, r=run all remaining");
            println!();

            let mut stmt_executor =
                StatementExecutor::with_executor(executor).with_mode(ExecutionMode::Step);

            // Set spec for input data generation
            stmt_executor = stmt_executor.with_spec(test_spec.clone());

            if let Err(e) = stmt_executor.load_sql(&sql_file) {
                eprintln!("❌ Failed to load SQL: {}", e);
                std::process::exit(1);
            }

            let statements = stmt_executor.statements();
            println!("📝 Loaded {} statements:", statements.len());
            for stmt in statements {
                println!(
                    "   [{}] {} ({})",
                    stmt.index + 1,
                    stmt.name,
                    stmt.statement_type.display_name()
                );
            }
            println!();

            let stdin = std::io::stdin();
            let mut stdout = std::io::stdout();
            let mut failed = false;

            loop {
                // Check if completed
                if matches!(
                    stmt_executor.state(),
                    SessionState::Completed | SessionState::Error
                ) {
                    break;
                }

                // Show current statement
                let idx = stmt_executor.current_index();
                if idx < stmt_executor.statements().len() {
                    let stmt = &stmt_executor.statements()[idx];
                    println!(
                        "▶️  [{}/{}] {} ({})",
                        idx + 1,
                        stmt_executor.statements().len(),
                        stmt.name,
                        stmt.statement_type.display_name()
                    );
                }

                // Prompt for input
                print!("Step> ");
                stdout.flush().ok();

                let mut input = String::new();
                if stdin.lock().read_line(&mut input).is_err() {
                    break;
                }

                let input = input.trim();
                match input {
                    "q" | "quit" | "exit" => {
                        println!("🛑 Exiting step mode");
                        break;
                    }
                    "r" | "run" => {
                        println!("🚀 Running all remaining statements...");
                        // Switch to full mode and execute all
                        match stmt_executor.continue_execution().await {
                            Ok(results) => {
                                for result in &results {
                                    let icon = if result.success { "✅" } else { "❌" };
                                    println!(
                                        "   {} {} ({}ms)",
                                        icon, result.name, result.execution_time_ms
                                    );
                                    if let Some(ref err) = result.error {
                                        println!("      Error: {}", err);
                                        failed = true;
                                    }
                                    // Show table snapshot for CREATE TABLE statements
                                    if let Some(ref snapshot) = result.table_snapshot {
                                        println!(
                                            "      📊 Table: {} rows",
                                            snapshot.stats.record_count
                                        );
                                    }
                                    // Show captured ERROR/WARN logs (top 5 per statement)
                                    if !result.captured_logs.is_empty() {
                                        let log_count = result.captured_logs.len();
                                        let display_count = log_count.min(5);
                                        println!(
                                            "      📋 Logs ({} captured, showing {}):",
                                            log_count, display_count
                                        );
                                        for entry in result.captured_logs.iter().take(5) {
                                            println!("         {}", entry.display_short());
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("❌ Execution error: {}", e);
                                failed = true;
                            }
                        }
                        break;
                    }
                    "" | "s" | "step" => {
                        // Execute next statement
                        match stmt_executor.step_next().await {
                            Ok(Some(result)) => {
                                let icon = if result.success { "✅" } else { "❌" };
                                println!("   {} Completed in {}ms", icon, result.execution_time_ms);
                                if let Some(ref err) = result.error {
                                    println!("      Error: {}", err);
                                    failed = true;
                                }
                                if let Some(ref output) = result.output {
                                    println!(
                                        "      Output: {} records to {}",
                                        output.records.len(),
                                        output.sink_name
                                    );
                                }
                                // Show table snapshot for CREATE TABLE statements
                                if let Some(ref snapshot) = result.table_snapshot {
                                    println!(
                                        "      📊 Table: {} rows",
                                        snapshot.stats.record_count
                                    );
                                    if !snapshot.records.is_empty() {
                                        println!("      Sample (first 5):");
                                        for (idx, (key, fields)) in
                                            snapshot.records.iter().take(5).enumerate()
                                        {
                                            let field_str: String = fields
                                                .iter()
                                                .take(4)
                                                .map(|(k, v)| format!("{}={:?}", k, v))
                                                .collect::<Vec<_>>()
                                                .join(", ");
                                            println!(
                                                "         [{}] {} → {{{}}}",
                                                idx + 1,
                                                key,
                                                field_str
                                            );
                                        }
                                    }
                                }
                                // Show captured ERROR/WARN logs (top 5)
                                if !result.captured_logs.is_empty() {
                                    let log_count = result.captured_logs.len();
                                    let display_count = log_count.min(5);
                                    println!(
                                        "      📋 Logs ({} captured, showing {}):",
                                        log_count, display_count
                                    );
                                    for entry in result.captured_logs.iter().take(5) {
                                        println!("         {}", entry.display_short());
                                    }
                                }
                                println!();
                            }
                            Ok(None) => {
                                println!("✅ All statements executed");
                                break;
                            }
                            Err(e) => {
                                eprintln!("❌ Execution error: {}", e);
                                failed = true;
                                break;
                            }
                        }
                    }
                    _ => {
                        println!("Unknown command: {}", input);
                        println!("Commands: [Enter]=step, q=quit, r=run all remaining");
                    }
                }
            }

            // Summary
            println!();
            println!("📊 Step Mode Summary");
            println!("════════════════════");
            let results = stmt_executor.results();
            let passed = results.iter().filter(|r| r.success).count();
            let total = results.len();
            println!(
                "   Executed: {}/{} statements",
                total,
                stmt_executor.statements().len()
            );
            println!("   Passed: {}/{}", passed, total);

            // Cleanup
            if !keep_containers && let Err(e) = stmt_executor.executor_mut().stop().await {
                log::warn!("Failed to stop executor: {}", e);
            }

            if failed || passed < total {
                std::process::exit(1);
            }
            return Ok(());
        }

        // Step 6: Filter queries if specified
        let queries_to_run: Vec<_> = if let Some(ref query_filter) = query {
            test_spec
                .queries
                .iter()
                .filter(|q| q.name == *query_filter)
                .collect()
        } else {
            test_spec.queries.iter().filter(|q| !q.skip).collect()
        };

        println!();
        if data_only {
            println!(
                "📤 Publishing data for {} queries (data-only mode)...",
                queries_to_run.len()
            );
        } else {
            println!("🚀 Running {} queries...", queries_to_run.len());
        }
        println!();

        // Step 7: Execute queries and run assertions (or just publish data in data_only mode)
        let mut report_gen = ReportGenerator::new(&test_spec.application, &run_id);
        let assertion_runner = AssertionRunner::new();

        // Track if we were interrupted
        let mut was_interrupted = false;

        // Data-only mode: just publish inputs without SQL execution
        if data_only {
            let mut total_records = 0usize;
            for query_test in &queries_to_run {
                println!("▶️  Publishing inputs for: {}", query_test.name);

                let publish_result = tokio::select! {
                    result = executor.publish_query_inputs(query_test) => result,
                    _ = tokio::signal::ctrl_c() => {
                        eprintln!("\n⚠️  Interrupted - cleaning up...");
                        was_interrupted = true;
                        break;
                    }
                };

                if was_interrupted {
                    break;
                }

                match publish_result {
                    Ok(count) => {
                        total_records += count;
                        println!("   ✅ Published {} input records", count);
                    }
                    Err(e) => {
                        eprintln!("   ❌ Failed to publish: {}", e);
                    }
                }
            }

            if was_interrupted {
                println!("🧹 Cleaning up containers after interrupt...");
                if let Err(e) = executor.stop().await {
                    log::warn!("Failed to stop executor: {}", e);
                }
                println!("✅ Containers cleaned up");
                std::process::exit(130);
            }

            // Summary for data-only mode
            println!();
            println!("════════════════════════════════════════");
            println!("📊 Data-Only Mode Complete");
            println!("════════════════════════════════════════");
            println!("   Total records published: {}", total_records);
            println!("   Topics: Kafka input topics from test spec");
            println!();
            println!("💡 Data is now available for external SQL processing");
            println!("   Use 'velo-sql deploy-app' to run SQL jobs against this data");
            println!();

            // Cleanup
            if !keep_containers {
                if let Err(e) = executor.stop().await {
                    log::warn!("Failed to stop executor: {}", e);
                }
            } else {
                println!("🐳 Keeping containers running (--keep-containers)");
            }

            return Ok(());
        }

        for query_test in &queries_to_run {
            println!("▶️  Executing: {}", query_test.name);

            // Use select! to race query execution against Ctrl+C
            // This allows us to cancel long-running operations
            let exec_result = tokio::select! {
                result = executor.execute_query(query_test) => result,
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("\n⚠️  Interrupted - cleaning up containers...");
                    was_interrupted = true;
                    break;
                }
            };

            // If we broke out of select due to interrupt, exit loop
            if was_interrupted {
                break;
            }

            // Process the result
            match exec_result {
                Ok(exec_result) => {
                    // Run assertions on captured output
                    let mut assertion_results = Vec::new();

                    for output in &exec_result.outputs {
                        // Get assertions for this specific sink (includes fallback to top-level)
                        let sink_assertions: Vec<_> = query_test
                            .assertions_for_sink(&output.sink_name)
                            .into_iter()
                            .cloned()
                            .collect();
                        let results = assertion_runner.run_assertions(output, &sink_assertions);
                        assertion_results.extend(results);
                    }

                    // If no outputs captured, create empty output for assertions
                    // Use all_assertions() to include per-sink assertions from outputs[]
                    let all_assertions: Vec<_> =
                        query_test.all_assertions().into_iter().cloned().collect();
                    if exec_result.outputs.is_empty() && !all_assertions.is_empty() {
                        use velostream::velostream::test_harness::executor::CapturedOutput;
                        let empty_output = CapturedOutput {
                            query_name: query_test.name.clone(),
                            sink_name: format!("{}_output", query_test.name),
                            topic: None, // No topic for empty output
                            records: Vec::new(),
                            execution_time_ms: 0,
                            warnings: Vec::new(),
                            memory_peak_bytes: None,
                            memory_growth_bytes: None,
                        };
                        let results =
                            assertion_runner.run_assertions(&empty_output, &all_assertions);
                        assertion_results.extend(results);
                    }

                    // Add metric assertion results (if any)
                    if !exec_result.metric_assertion_results.is_empty() {
                        assertion_results.extend(exec_result.metric_assertion_results.clone());
                    }

                    // Report results - single pass through assertions
                    let total = assertion_results.len();
                    let failed_count = assertion_results.iter().filter(|a| !a.passed).count();

                    if failed_count == 0 {
                        println!("   ✅ Passed ({}/{} assertions)", total, total);
                    } else {
                        println!("   ❌ Failed ({}/{} assertions)", failed_count, total);
                    }

                    // Print details of all assertions
                    for result in &assertion_results {
                        let icon = if result.passed { "✅" } else { "❌" };
                        println!(
                            "      {} {}: {}",
                            icon, result.assertion_type, result.message
                        );
                        // Show expected/actual for failed assertions
                        if !result.passed
                            && let (Some(expected), Some(actual)) =
                                (&result.expected, &result.actual)
                        {
                            println!("         Expected: {}", expected);
                            println!("         Actual:   {}", actual);
                        }
                    }

                    report_gen.add_query_result(&exec_result, &assertion_results);
                }
                Err(e) => {
                    println!("   💥 Error: {}", e);
                    // Create error result
                    use velostream::velostream::test_harness::executor::ExecutionResult;
                    let error_result = ExecutionResult {
                        query_name: query_test.name.clone(),
                        success: false,
                        error: Some(e.to_string()),
                        outputs: Vec::new(),
                        execution_time_ms: 0,
                        metric_assertion_results: Vec::new(),
                    };
                    report_gen.add_query_result(&error_result, &[]);
                }
            }
        }

        // Step 8: Generate and output report
        let report = report_gen.generate();
        let output_format: OutputFormat = output.parse().unwrap_or(OutputFormat::Text);

        println!();
        let mut stdout = std::io::stdout();
        if let Err(e) = write_report(&report, output_format, &mut stdout) {
            eprintln!("❌ Failed to write report: {}", e);
        }

        // Cleanup executor and infrastructure (stops testcontainers)
        // Always cleanup on interrupt, regardless of keep_containers/reuse_containers flags
        if (keep_containers || reuse_containers) && !was_interrupted {
            if reuse_containers {
                println!();
                println!("♻️  Keeping container running for reuse (--reuse-containers)");
                println!("   Next run with --reuse-containers will be faster");
                // Clean up topics but keep container
                if let Err(e) = executor.stop_with_reuse(true).await {
                    log::warn!("Failed to stop executor with reuse: {}", e);
                }
            } else {
                println!();
                println!("🐳 Keeping containers running for debugging (--keep-containers)");
                println!("   Use 'docker ps' to find containers");
                println!("   Use 'docker stop <id>' to clean up when done");
            }
        } else {
            if was_interrupted {
                println!("🧹 Cleaning up containers after interrupt...");
            }
            if let Err(e) = executor.stop().await {
                log::warn!("Failed to stop executor: {}", e);
            }
            if was_interrupted {
                println!("✅ Containers cleaned up");
                std::process::exit(130); // Standard exit code for SIGINT
            }
        }

        // Exit with appropriate code
        if report.summary.failed > 0 || report.summary.errors > 0 {
            std::process::exit(1);
        }
        Ok(())
    }
}

/// Check health of Velostream infrastructure
///
/// This function performs health checks on:
/// - Docker daemon availability
/// - Kafka cluster connectivity
/// - Topic existence and configuration
/// - Process states
/// - Consumer group health
///
/// # Arguments
/// * `broker` - Optional Kafka bootstrap servers
/// * `output` - Output format (text or json)
/// * `check` - Optional comma-separated list of specific checks to run
/// * `timeout` - Timeout for operations in seconds
/// * `containers` - Optional comma-separated list of container names to check
/// * `topics` - Optional comma-separated list of topic names to check
pub async fn health(
    broker: Option<String>,
    output: String,
    check: Option<String>,
    timeout: u64,
    containers: Option<String>,
    topics: Option<String>,
) -> CommandResult {
    use velostream::velostream::test_harness::health::{
        HealthCheckType, HealthChecker, HealthConfig,
    };

    // Determine which checks to run
    let check_types = if let Some(ref checks) = check {
        HealthCheckType::parse_list(checks)
    } else {
        HealthCheckType::all()
    };

    if check_types.is_empty() {
        eprintln!("❌ No valid checks specified");
        eprintln!("   Valid checks: docker, kafka, topics, processes, consumers");
        std::process::exit(1);
    }

    // Build configuration
    let mut config = HealthConfig {
        bootstrap_servers: broker.clone(),
        timeout: Duration::from_secs(timeout),
        ..Default::default()
    };

    if let Some(ref c) = containers {
        config.container_names = c.split(',').map(|s| s.trim().to_string()).collect();
    }

    if let Some(ref t) = topics {
        config.topic_names = t.split(',').map(|s| s.trim().to_string()).collect();
    }

    // Create health checker and run checks
    let checker = HealthChecker::with_config(config);
    let report = checker.run_checks(&check_types).await;

    // Output in requested format
    match output.as_str() {
        "json" => {
            println!("{}", report.format_json());
        }
        _ => {
            println!("{}", report.format_text());
        }
    }

    // Exit with appropriate status code
    if !report.is_healthy() {
        std::process::exit(1);
    }

    Ok(())
}

/// Generate SQL annotations and monitoring infrastructure
///
/// This function analyzes a SQL file and generates:
/// 1. Annotated SQL with @metric, @observability, and other annotations
/// 2. Optional monitoring infrastructure (Prometheus, Grafana, Tempo)
///
/// # Arguments
/// * `sql_file` - Path to the SQL file to annotate
/// * `output` - Optional output path for annotated SQL
/// * `name` - Optional application name
/// * `version` - Optional application version
/// * `monitoring` - Optional monitoring output directory
/// * `prometheus_port` - Prometheus metrics port
/// * `telemetry_port` - Telemetry port for traces
/// * `yes` - Skip interactive prompts
#[allow(clippy::too_many_arguments)]
pub async fn annotate(
    sql_file: PathBuf,
    output: Option<PathBuf>,
    name: Option<String>,
    version: Option<String>,
    monitoring: Option<PathBuf>,
    prometheus_port: Option<u16>,
    telemetry_port: Option<u16>,
    yes: bool,
) -> CommandResult {
    use std::io::{BufRead, Write};
    use velostream::velostream::test_harness::annotate::{AnnotateConfig, Annotator};

    println!("📝 Generate SQL Annotations & Monitoring");
    println!("════════════════════════════════════════");
    println!("SQL File: {}", sql_file.display());
    println!();

    // Helper for interactive prompts
    let prompt_with_default = |prompt: &str, default: &str| -> String {
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
    };

    let prompt_yes_no = |prompt: &str, default: bool| -> bool {
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
    };

    // Auto-detect app name from filename
    let default_app_name = sql_file
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "my_app".to_string());

    // Determine if we need interactive mode
    let interactive = !yes && (name.is_none() || version.is_none() || monitoring.is_none());

    // Get app name
    let app_name = if let Some(n) = name {
        n
    } else if interactive {
        prompt_with_default("Application name", &default_app_name)
    } else {
        default_app_name
    };

    // Get version
    let version = if let Some(v) = version {
        v
    } else if interactive {
        prompt_with_default("Version", "1.0.0")
    } else {
        "1.0.0".to_string()
    };

    // Get monitoring directory
    let monitoring = if monitoring.is_some() {
        monitoring
    } else if interactive {
        if prompt_yes_no(
            "Generate monitoring infrastructure (Prometheus, Grafana, Tempo)?",
            true,
        ) {
            let default_dir = sql_file
                .parent()
                .map(|p| p.join("monitoring"))
                .unwrap_or_else(|| PathBuf::from("./monitoring"));
            let dir = prompt_with_default(
                "Monitoring output directory",
                &default_dir.display().to_string(),
            );
            Some(PathBuf::from(dir))
        } else {
            None
        }
    } else {
        None
    };

    // Get ports (only if monitoring is enabled)
    let prometheus_port = prometheus_port.unwrap_or_else(|| {
        if interactive && monitoring.is_some() {
            prompt_with_default("Prometheus metrics port", "8080")
                .parse()
                .unwrap_or(8080)
        } else {
            8080
        }
    });

    let telemetry_port = telemetry_port.unwrap_or_else(|| {
        if interactive && monitoring.is_some() {
            prompt_with_default("Telemetry port", "9091")
                .parse()
                .unwrap_or(9091)
        } else {
            9091
        }
    });

    // Show configuration summary
    println!();
    println!("📋 Configuration:");
    println!("   Application: {}", app_name);
    println!("   Version: {}", version);
    if let Some(ref m) = monitoring {
        println!("   Monitoring: {}", m.display());
        println!("   Prometheus port: {}", prometheus_port);
        println!("   Telemetry port: {}", telemetry_port);
    } else {
        println!("   Monitoring: (not generating)");
    }
    println!();

    // Read SQL content
    let sql_content = match std::fs::read_to_string(&sql_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("❌ Failed to read SQL file: {}", e);
            std::process::exit(1);
        }
    };

    // Create annotator config
    let config = AnnotateConfig {
        app_name: app_name.clone(),
        version,
        generate_monitoring: monitoring.is_some(),
        monitoring_dir: monitoring.as_ref().map(|p| p.display().to_string()),
        prometheus_port,
        telemetry_port,
    };

    let annotator = Annotator::new(config);

    // Analyze SQL
    println!("🔍 Analyzing SQL...");
    let analysis = match annotator.analyze(&sql_content) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("❌ Failed to analyze SQL: {}", e);
            std::process::exit(1);
        }
    };

    println!("   Queries: {}", analysis.queries.len());
    println!("   Sources: {}", analysis.sources.len());
    println!("   Sinks: {}", analysis.sinks.len());
    println!("   Metrics detected: {}", analysis.metrics.len());
    if analysis.has_windows {
        println!("   ✓ Has window operations");
    }
    if analysis.has_aggregations {
        println!("   ✓ Has aggregations");
    }
    if analysis.has_joins {
        println!("   ✓ Has joins");
    }
    println!();

    // Show detected metrics summary
    if !analysis.metrics.is_empty() {
        println!("📊 Detected Metrics:");
        for metric in analysis.metrics.iter().take(5) {
            println!(
                "   • {} ({}) - {}",
                metric.name,
                metric.metric_type.as_str(),
                metric.help
            );
        }
        if analysis.metrics.len() > 5 {
            println!("   ... and {} more", analysis.metrics.len() - 5);
        }
        println!();
    }

    // Generate annotated SQL
    println!("📝 Generating annotated SQL...");
    let annotated_sql = match annotator.generate_annotated_sql(&sql_content, &analysis) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ Failed to generate annotated SQL: {}", e);
            std::process::exit(1);
        }
    };

    // Determine output path
    let output_path = output.unwrap_or_else(|| {
        let stem = sql_file
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "output".to_string());
        sql_file.with_file_name(format!("{}.annotated.sql", stem))
    });

    // Write annotated SQL
    match std::fs::write(&output_path, &annotated_sql) {
        Ok(_) => {
            println!("✅ Generated: {}", output_path.display());
        }
        Err(e) => {
            eprintln!("❌ Failed to write annotated SQL: {}", e);
            std::process::exit(1);
        }
    }

    // Generate monitoring infrastructure if requested
    if let Some(ref monitoring_dir) = monitoring {
        println!();
        println!("🔧 Generating monitoring infrastructure...");

        match annotator.generate_monitoring(&analysis, monitoring_dir) {
            Ok(_) => {
                println!("✅ Generated monitoring files:");
                println!("   • {}/prometheus.yml", monitoring_dir.display());
                println!(
                    "   • {}/grafana/provisioning/datasources/prometheus.yml",
                    monitoring_dir.display()
                );
                println!(
                    "   • {}/grafana/provisioning/datasources/tempo.yml",
                    monitoring_dir.display()
                );
                println!(
                    "   • {}/grafana/provisioning/dashboards/dashboard.yml",
                    monitoring_dir.display()
                );
                println!(
                    "   • {}/grafana/dashboards/{}-dashboard.json",
                    monitoring_dir.display(),
                    app_name
                );
                println!("   • {}/tempo/tempo.yaml", monitoring_dir.display());
            }
            Err(e) => {
                eprintln!("❌ Failed to generate monitoring: {}", e);
                std::process::exit(1);
            }
        }
    }

    println!();
    println!("💡 Next steps:");
    println!(
        "   1. Review and customize annotations in {}",
        output_path.display()
    );
    if monitoring.is_some() {
        println!("   2. Start monitoring with: docker-compose -f monitoring/docker-compose.yml up");
        println!("   3. View dashboards at: http://localhost:3000");
    }

    Ok(())
}

/// Interactive SQL debugger with breakpoints
///
/// This function provides an interactive debugging environment for SQL applications:
/// 1. Load or generate test specification
/// 2. Load schemas and data hints
/// 3. Create infrastructure (Kafka, testcontainers)
/// 4. Execute SQL with breakpoints
/// 5. Interactive command loop
/// 6. Data visibility (messages, head, tail, filter, export)
/// 7. Cleanup
///
/// # Arguments
/// * `sql_file` - Path to the SQL file to debug
/// * `spec` - Optional path to test specification YAML
/// * `schemas` - Optional directory containing schema definitions
/// * `breakpoint` - Statement names to pause at
/// * `timeout_ms` - Timeout per query in milliseconds
/// * `kafka` - External Kafka bootstrap servers
/// * `keep_containers` - Keep containers running after completion
/// * `reuse_containers` - Reuse existing containers
/// * `yes` - Skip interactive prompts
#[allow(clippy::too_many_arguments)]
pub async fn debug(
    sql_file: PathBuf,
    spec: Option<PathBuf>,
    schemas: Option<PathBuf>,
    breakpoint: Vec<String>,
    timeout_ms: u64,
    kafka: Option<String>,
    keep_containers: bool,
    reuse_containers: bool,
    yes: bool,
) -> CommandResult {
    use std::io::{BufRead, Write};
    use velostream::velostream::test_harness::SpecGenerator;
    use velostream::velostream::test_harness::config_override::ConfigOverrideBuilder;
    use velostream::velostream::test_harness::infra::TestHarnessInfra;
    use velostream::velostream::test_harness::schema::SchemaRegistry;
    use velostream::velostream::test_harness::spec::TestSpec;
    use velostream::velostream::test_harness::statement_executor::{
        CommandResult as DebugCommandResult, DebugCommand, DebugSession, ExecutionMode,
        ExportFormat, FilterOperator, SessionState, StatementExecutor,
    };

    println!("🐛 Velostream SQL Debugger");
    println!("════════════════════════════════════════");
    println!("SQL File: {}", sql_file.display());

    // Auto-discover or prompt for spec
    let spec = if spec.is_some() {
        spec
    } else if let Some(discovered) = crate::discovery::discover_spec(&sql_file) {
        println!("📂 Auto-discovered spec: {}", discovered.display());
        Some(discovered)
    } else if !yes {
        let prompt_spec = crate::discovery::prompt_yes_no(
            "No spec file found. Would you like to auto-generate from SQL?",
            true,
        );
        if prompt_spec {
            None // Will generate from SQL
        } else {
            eprintln!("❌ No spec file and user declined auto-generation");
            std::process::exit(1);
        }
    } else {
        None // Will generate from SQL
    };

    // Auto-discover schemas directory
    let schemas = if schemas.is_some() {
        schemas
    } else if let Some(discovered) = crate::discovery::discover_schemas(&sql_file) {
        println!("📂 Auto-discovered schemas: {}", discovered.display());
        Some(discovered)
    } else {
        None
    };

    if let Some(ref s) = spec {
        println!("Test Spec: {}", s.display());
    }
    if let Some(ref s) = schemas {
        println!("Schemas: {}", s.display());
    }
    if !breakpoint.is_empty() {
        println!("Breakpoints: {:?}", breakpoint);
    }
    println!("Timeout: {}ms", timeout_ms);
    println!();

    // Step 1: Load or generate test spec
    let test_spec: TestSpec = if let Some(ref spec_path) = spec {
        let content = match std::fs::read_to_string(spec_path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("❌ Failed to read test spec: {}", e);
                std::process::exit(1);
            }
        };
        match serde_yaml::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("❌ Failed to parse test spec: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        // Generate from SQL file
        let generator = SpecGenerator::new();
        match generator.generate_from_sql(&sql_file) {
            Ok(s) => {
                println!("📝 Auto-generated test spec from SQL");
                // Offer to save the generated spec
                crate::discovery::offer_save_spec(&sql_file, &s, yes);
                s
            }
            Err(e) => {
                eprintln!("❌ Failed to analyze SQL file: {}", e);
                std::process::exit(1);
            }
        }
    };

    // Step 2: Load schemas if provided
    let mut schema_registry = SchemaRegistry::new();
    if let Some(ref schema_dir) = schemas
        && schema_dir.exists()
    {
        println!("📁 Loading schemas from {}", schema_dir.display());
        if let Ok(entries) = std::fs::read_dir(schema_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path
                    .extension()
                    .is_some_and(|ext| ext == "yaml" || ext == "yml")
                    && let Ok(content) = std::fs::read_to_string(&path)
                    && let Ok(mut schema) = serde_yaml::from_str::<
                        velostream::velostream::test_harness::Schema,
                    >(&content)
                {
                    schema.source_path = Some(path.display().to_string());
                    let key_info = schema
                        .key_field
                        .as_ref()
                        .map(|k| format!(", key_field: {}", k))
                        .unwrap_or_default();
                    println!(
                        "   ✓ {} ({} fields{}) ← {}",
                        schema.name,
                        schema.fields.len(),
                        key_info,
                        path.display()
                    );
                    schema_registry.register(schema);
                }
            }
        }
    }

    // Step 2b: Generate schemas from @data.* hints in SQL file
    {
        use velostream::velostream::test_harness::{DataHintParser, generate_schema_from_hints};

        let sql_content = std::fs::read_to_string(&sql_file).unwrap_or_default();
        let mut hint_parser = DataHintParser::new();

        if hint_parser.parse(&sql_content).is_ok() {
            let field_hints = hint_parser.get_field_hints();
            let global_hints = hint_parser.get_global_hints();

            if !field_hints.is_empty() {
                let source_name = global_hints
                    .source_name
                    .clone()
                    .unwrap_or_else(|| "generated_source".to_string());

                if schema_registry.get(&source_name).is_none()
                    && let Ok(schema) =
                        generate_schema_from_hints(&source_name, global_hints, &field_hints)
                {
                    println!(
                        "   ✓ {} ({} fields) ← @data.* hints",
                        schema.name,
                        schema.fields.len()
                    );
                    schema_registry.register(schema);
                }
            }
        }
    }

    // Step 3: Create infrastructure
    println!("🔧 Initializing debug infrastructure...");

    // Check for Kafka from CLI or environment
    let bootstrap_servers = kafka
        .clone()
        .or_else(|| std::env::var("KAFKA_BOOTSTRAP_SERVERS").ok());

    let mut infra = if let Some(ref bs) = bootstrap_servers {
        println!("   Kafka: {}", bs);
        TestHarnessInfra::with_kafka(bs)
    } else {
        if reuse_containers {
            println!("♻️  Looking for reusable Kafka container...");
        } else {
            println!("🐳 Starting Kafka via testcontainers...");
        }
        match TestHarnessInfra::with_testcontainers_reuse(reuse_containers).await {
            Ok(i) => {
                let mode = if reuse_containers {
                    "reused"
                } else {
                    "testcontainers"
                };
                println!(
                    "   Kafka: {} ({})",
                    i.bootstrap_servers().unwrap_or("unknown"),
                    mode
                );
                i
            }
            Err(e) => {
                eprintln!("❌ Failed to start testcontainers: {}", e);
                std::process::exit(1);
            }
        }
    };

    if let Err(e) = infra.start().await {
        eprintln!("❌ Failed to start infrastructure: {}", e);
        std::process::exit(1);
    }

    // Set environment for SQL job
    if let Some(bs) = infra.bootstrap_servers() {
        unsafe {
            std::env::set_var("VELOSTREAM_KAFKA_BROKERS", bs);
        }
    }

    // Step 4: Create executor
    let run_id = format!(
        "{:08x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u32
    );
    // Get SQL file's parent directory for resolving relative config file paths
    let sql_dir = sql_file
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());

    let overrides = ConfigOverrideBuilder::new(&run_id)
        .bootstrap_servers(infra.bootstrap_servers().unwrap_or("localhost:9092"))
        .base_dir(sql_dir.clone())
        .build();

    // Determine the spec directory for resolving relative file paths
    let spec_dir = spec
        .as_ref()
        .and_then(|p| p.parent())
        .unwrap_or(&sql_dir)
        .to_path_buf();

    use velostream::velostream::test_harness::executor::QueryExecutor;
    let executor = QueryExecutor::new(infra)
        .with_timeout(Duration::from_millis(timeout_ms))
        .with_overrides(overrides)
        .with_schema_registry(schema_registry)
        .with_spec_dir(&spec_dir);

    // Initialize with server
    let executor = match executor.with_server(Some(sql_dir)).await {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ Failed to initialize SQL server: {}", e);
            std::process::exit(1);
        }
    };

    // Step 5: Create debug session
    let mut stmt_executor = StatementExecutor::with_executor(executor)
        .with_mode(ExecutionMode::Breakpoint)
        .with_spec(test_spec);

    // Add breakpoints
    for bp in &breakpoint {
        stmt_executor.add_breakpoint(bp);
        println!("   Breakpoint set: {}", bp);
    }

    // Load SQL file
    if let Err(e) = stmt_executor.load_sql(&sql_file) {
        eprintln!("❌ Failed to load SQL: {}", e);
        std::process::exit(1);
    }

    let mut session = DebugSession::new(stmt_executor);

    // Print statements with full SQL
    println!();
    println!("📝 SQL Statements:");
    let statements = session.executor().statements();
    for stmt in statements {
        let bp_marker = if session.executor().breakpoints().contains(&stmt.name) {
            "*"
        } else {
            " "
        };
        println!(
            "  {} [{}] {} ({})",
            bp_marker,
            stmt.index + 1,
            stmt.name,
            stmt.statement_type.display_name()
        );
        // Show full SQL indented
        for sql_line in stmt.sql_text.lines() {
            println!("      {}", sql_line);
        }
        println!(); // Blank line between statements
    }

    // Helper function to resolve statement number to name
    fn resolve_statement_arg(
        arg: &str,
        statements: &[velostream::velostream::test_harness::statement_executor::ParsedStatement],
    ) -> Option<String> {
        // First try to parse as a number (1-indexed)
        if let Ok(num) = arg.parse::<usize>() {
            if num >= 1 && num <= statements.len() {
                return Some(statements[num - 1].name.clone());
            } else {
                println!(
                    "Invalid statement number: {}. Valid range: 1-{}",
                    num,
                    statements.len()
                );
                return None;
            }
        }
        // Otherwise use as name directly
        Some(arg.to_string())
    }

    // Helper function to prompt user to select a statement from a list
    fn prompt_statement_selection(
        statements: &[velostream::velostream::test_harness::statement_executor::ParsedStatement],
        stdin: &std::io::Stdin,
        stdout: &mut std::io::Stdout,
    ) -> Option<String> {
        use std::io::Write;

        if statements.is_empty() {
            println!("No statements available.");
            return None;
        }

        println!("Select a statement:");
        for (i, stmt) in statements.iter().enumerate() {
            println!(
                "  [{}] {} ({})",
                i + 1,
                stmt.name,
                stmt.statement_type.display_name()
            );
            // Show full SQL indented
            for sql_line in stmt.sql_text.lines() {
                println!("      {}", sql_line);
            }
            println!(); // Blank line between statements
        }
        print!("Enter number [1-{}]: ", statements.len());
        let _ = stdout.flush();

        let mut input = String::new();
        if stdin.read_line(&mut input).is_ok() {
            let input = input.trim();
            if let Ok(num) = input.parse::<usize>()
                && (1..=statements.len()).contains(&num)
            {
                return Some(statements[num - 1].name.clone());
            }
            if !input.is_empty() {
                println!("Invalid selection: {}", input);
            }
        }
        None
    }

    /// Parse a filter expression like "field=value" or "field > value"
    fn parse_filter_expr(expr: &str) -> (String, Option<FilterOperator>, String) {
        // Try operators in order of length (longest first to avoid partial matches)
        let operators = [
            (">=", FilterOperator::Gte),
            ("<=", FilterOperator::Lte),
            ("!=", FilterOperator::Ne),
            ("<>", FilterOperator::Ne),
            ("==", FilterOperator::Eq),
            ("=", FilterOperator::Eq),
            (">", FilterOperator::Gt),
            ("<", FilterOperator::Lt),
            ("~", FilterOperator::Contains),
        ];

        for (op_str, op) in operators {
            if let Some(idx) = expr.find(op_str) {
                let field = expr[..idx].trim().to_string();
                let value = expr[idx + op_str.len()..].trim().to_string();
                return (field, Some(op), value);
            }
        }

        // No operator found
        (expr.to_string(), None, String::new())
    }

    // Interactive debug loop
    println!();
    println!("🐛 Debug Commands:");
    println!("   s, step       - Execute next statement");
    println!("   c, continue   - Run until next breakpoint");
    println!("   r, run        - Run all remaining statements");
    println!("   b <N>         - Set breakpoint on statement N");
    println!("   u <N>         - Remove breakpoint from statement N");
    println!("   l, list       - List all statements");
    println!("   i <N>         - Inspect output from statement N");
    println!("   st, status    - Show current state");
    println!("   topics        - List all Kafka topics with offsets");
    println!("   schema <topic> - Show schema for topic (inferred)");
    println!("   consumers     - List all consumer groups");
    println!("   jobs          - List all jobs with source/sink status");
    println!();
    println!("📊 Data Visibility:");
    println!("   messages <topic|N> [--last N] [--first N] - Peek at topic messages");
    println!("      (Use topic number from 'topics' list, e.g., 'messages 1')");
    println!("   head <stmt|N> [-n N]   - Show first N records (default: 10)");
    println!("   tail <stmt|N> [-n N]   - Show last N records (default: 10)");
    println!("   filter <stmt|N> <field><op><value> - Filter records (op: =,!=,>,<,~)");
    println!("   export <stmt|N> <file> - Export records to JSON/CSV file");
    println!();
    println!("   q, quit       - Exit debugger");
    println!();

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();

    let mut execution_finished = false;
    // Cache topic names for numbered references (e.g., "messages 1" instead of "messages my_long_topic")
    let mut last_topic_list: Vec<String> = Vec::new();

    loop {
        // Show state
        let state = session.executor().state();
        let idx = session.executor().current_index();
        let total = session.executor().statements().len();

        match state {
            SessionState::Completed => {
                if !execution_finished {
                    println!("✅ All statements completed");
                    println!();
                    println!("📊 Execution finished. You can still inspect data:");
                    println!("   l, list      - List all statements with SQL");
                    println!("   topics       - View topic state and offsets");
                    println!("   jobs         - View job details and statistics");
                    println!("   i <N>        - Inspect output from statement N");
                    println!("   messages <topic|N> - Peek at topic messages");
                    println!("   head <N>     - Show first records from statement N");
                    println!("   tail <N>     - Show last records from statement N");
                    println!("   filter <N> <expr> - Filter records (e.g., status=FAILED)");
                    println!("   export <N> <file> - Export to JSON/CSV");
                    println!("   q            - Exit and cleanup");
                    execution_finished = true;
                }
                println!();
                print!("🏁 ");
            }
            SessionState::Error => {
                if !execution_finished {
                    println!("❌ Execution stopped due to error");
                    println!();
                    println!("📊 You can still inspect data:");
                    println!("   l, list      - List all statements with SQL");
                    println!("   topics       - View topic state and offsets");
                    println!("   jobs         - View job details and statistics");
                    println!("   i <N>        - Inspect output from statement N");
                    println!("   messages <topic|N> - Peek at topic messages");
                    println!("   head <N>     - Show first records from statement N");
                    println!("   tail <N>     - Show last records from statement N");
                    println!("   filter <N> <expr> - Filter records (e.g., status=FAILED)");
                    println!("   export <N> <file> - Export to JSON/CSV");
                    println!("   q            - Exit and cleanup");
                    execution_finished = true;
                }
                println!();
                print!("🔴 ");
            }
            SessionState::Paused(stmt_idx) => {
                let stmt = &session.executor().statements()[*stmt_idx];
                println!(
                    "⏸️  Paused at [{}/{}] {} ({})",
                    stmt_idx + 1,
                    total,
                    stmt.name,
                    stmt.statement_type.display_name()
                );
            }
            SessionState::NotStarted => {
                if idx < total {
                    let stmt = &session.executor().statements()[idx];
                    println!("▶️  Ready to execute [{}/{}] {}", idx + 1, total, stmt.name);
                }
            }
            SessionState::Running => {
                // Shouldn't happen in interactive mode
            }
        }

        // Show appropriate prompt based on state
        if execution_finished {
            print!("🏁 (inspect) ");
        } else {
            print!("(debug) ");
        }
        stdout.flush().ok();

        let mut input = String::new();
        if stdin.lock().read_line(&mut input).is_err() {
            break;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        let cmd = parts.first().copied().unwrap_or("");

        let debug_cmd = match cmd {
            "" | "s" | "step" => {
                if execution_finished {
                    println!("Execution complete. Use 'topics', 'jobs', 'i <N>', or 'q' to exit.");
                    None
                } else {
                    Some(DebugCommand::Step)
                }
            }
            "c" | "continue" => {
                if execution_finished {
                    println!("Execution complete. Use 'topics', 'jobs', 'i <N>', or 'q' to exit.");
                    None
                } else {
                    Some(DebugCommand::Continue)
                }
            }
            "r" | "run" => {
                if execution_finished {
                    println!("Execution complete. Use 'topics', 'jobs', 'i <N>', or 'q' to exit.");
                    None
                } else {
                    Some(DebugCommand::Run)
                }
            }
            "b" | "break" => {
                if execution_finished {
                    println!("Execution complete. Breakpoints are not available.");
                    None
                } else if let Some(arg) = parts.get(1) {
                    let statements = session.executor().statements();
                    resolve_statement_arg(arg, statements).map(DebugCommand::Break)
                } else {
                    println!("Usage: b <N>  (e.g., b 1)");
                    None
                }
            }
            "u" | "unbreak" => {
                if execution_finished {
                    println!("Execution complete. Breakpoints are not available.");
                    None
                } else if let Some(arg) = parts.get(1) {
                    let statements = session.executor().statements();
                    resolve_statement_arg(arg, statements).map(DebugCommand::Unbreak)
                } else {
                    println!("Usage: u <N>  (e.g., u 1)");
                    None
                }
            }
            "cb" | "clear" => {
                if execution_finished {
                    println!("Execution complete. Breakpoints are not available.");
                    None
                } else {
                    Some(DebugCommand::Clear)
                }
            }
            "l" | "list" => Some(DebugCommand::List),
            "i" | "inspect" => {
                if let Some(arg) = parts.get(1) {
                    let statements = session.executor().statements();
                    resolve_statement_arg(arg, statements).map(DebugCommand::Inspect)
                } else {
                    println!("Usage: i <N>  (e.g., i 1)");
                    None
                }
            }
            "ia" | "inspect-all" => Some(DebugCommand::InspectAll),
            "hi" | "history" => Some(DebugCommand::History),
            "st" | "status" => Some(DebugCommand::Status),
            "topics" | "lt" | "list-topics" => Some(DebugCommand::ListTopics),
            "consumers" | "lc" | "list-consumers" => Some(DebugCommand::ListConsumers),
            "jobs" | "lj" | "list-jobs" => Some(DebugCommand::ListJobs),
            "schema" | "sc" => {
                if let Some(arg) = parts.get(1) {
                    // Resolve topic name - first check if it's a statement number
                    let statements = session.executor().statements();
                    if let Ok(num) = arg.parse::<usize>() {
                        // It's a number - try to get the sink topic from that statement
                        if num >= 1 && num <= statements.len() {
                            let topic_name = statements[num - 1]
                                .sink_topic
                                .clone()
                                .unwrap_or_else(|| arg.to_string());
                            Some(DebugCommand::ShowSchema(topic_name))
                        } else {
                            println!(
                                "Invalid statement number: {}. Valid range: 1-{}",
                                num,
                                statements.len()
                            );
                            None
                        }
                    } else {
                        // It's a topic name
                        Some(DebugCommand::ShowSchema(arg.to_string()))
                    }
                } else {
                    println!("Usage: schema <topic_name> or schema <N>");
                    None
                }
            }

            // === Data Visibility Commands ===
            "messages" | "msg" | "m" => {
                if let Some(topic_arg) = parts.get(1) {
                    // Resolve topic: either a number (from topics list) or a name
                    let topic_name = if let Ok(num) = topic_arg.parse::<usize>() {
                        // It's a number - look up in cached topic list
                        if num == 0 || num > last_topic_list.len() {
                            if last_topic_list.is_empty() {
                                println!(
                                    "No topics cached. Run 'topics' first to see available topics."
                                );
                            } else {
                                println!(
                                    "Topic number {} out of range (1-{}). Run 'topics' to see list.",
                                    num,
                                    last_topic_list.len()
                                );
                            }
                            None
                        } else {
                            Some(last_topic_list[num - 1].clone())
                        }
                    } else {
                        // It's a name - use directly
                        Some(topic_arg.to_string())
                    };

                    if let Some(topic) = topic_name {
                        // Parse options: --last N, --first N, --offset O, --partition P
                        let mut last: Option<usize> = None;
                        let mut first: Option<usize> = None;
                        let mut offset: Option<i64> = None;
                        let mut partition: Option<i32> = None;

                        let mut i = 2;
                        while i < parts.len() {
                            match parts[i] {
                                "--last" | "-l" => {
                                    if let Some(n) = parts.get(i + 1) {
                                        last = n.parse().ok();
                                        i += 1;
                                    }
                                }
                                "--first" | "-f" => {
                                    if let Some(n) = parts.get(i + 1) {
                                        first = n.parse().ok();
                                        i += 1;
                                    }
                                }
                                "--offset" | "-o" => {
                                    if let Some(o) = parts.get(i + 1) {
                                        offset = o.parse().ok();
                                        i += 1;
                                    }
                                }
                                "--partition" | "-p" => {
                                    if let Some(p) = parts.get(i + 1) {
                                        partition = p.parse().ok();
                                        i += 1;
                                    }
                                }
                                _ => {}
                            }
                            i += 1;
                        }

                        // Default to last 5 if no option specified
                        if last.is_none() && first.is_none() && offset.is_none() {
                            last = Some(5);
                        }

                        Some(DebugCommand::Messages {
                            topic,
                            last,
                            first,
                            offset,
                            partition,
                        })
                    } else {
                        None
                    }
                } else {
                    println!(
                        "Usage: messages <topic|N> [--last N] [--first N] [--offset O] [--partition P]"
                    );
                    println!("  e.g., messages my_topic --last 10");
                    println!("  e.g., messages 1 --last 5  (uses topic #1 from 'topics' list)");
                    None
                }
            }

            "head" | "hd" => {
                let statements = session.executor().statements();
                let limit = parts
                    .iter()
                    .position(|p| *p == "-n")
                    .and_then(|idx| parts.get(idx + 1))
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(10);

                let stmt_name = if let Some(arg) = parts.get(1) {
                    // Skip if arg is "-n" (user typed "head -n 5")
                    if *arg == "-n" {
                        prompt_statement_selection(statements, &stdin, &mut stdout)
                    } else {
                        resolve_statement_arg(arg, statements)
                    }
                } else {
                    prompt_statement_selection(statements, &stdin, &mut stdout)
                };

                stmt_name.map(|stmt| DebugCommand::Head {
                    statement: stmt,
                    limit,
                })
            }

            "tail" | "tl" => {
                let statements = session.executor().statements();
                let limit = parts
                    .iter()
                    .position(|p| *p == "-n")
                    .and_then(|idx| parts.get(idx + 1))
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(10);

                let stmt_name = if let Some(arg) = parts.get(1) {
                    // Skip if arg is "-n" (user typed "tail -n 5")
                    if *arg == "-n" {
                        prompt_statement_selection(statements, &stdin, &mut stdout)
                    } else {
                        resolve_statement_arg(arg, statements)
                    }
                } else {
                    prompt_statement_selection(statements, &stdin, &mut stdout)
                };

                stmt_name.map(|stmt| DebugCommand::Tail {
                    statement: stmt,
                    limit,
                })
            }

            "filter" | "f" => {
                // Usage: filter <statement> <field>=<value>
                // or: filter <statement> <field> <op> <value>
                let statements = session.executor().statements();

                if parts.len() >= 3 {
                    let arg = parts[1];

                    if let Some(statement) = resolve_statement_arg(arg, statements) {
                        let expr = parts[2..].join(" ");
                        // Parse expression: field=value or field > value etc.
                        let (field, operator, value) = parse_filter_expr(&expr);

                        if let Some(op) = operator {
                            Some(DebugCommand::Filter {
                                statement,
                                field,
                                operator: op,
                                value,
                            })
                        } else {
                            println!(
                                "Invalid filter expression. Use: field=value, field>value, field<value, field~value (contains)"
                            );
                            None
                        }
                    } else {
                        None
                    }
                } else if parts.len() == 1 {
                    // No args - prompt for statement first, then expression
                    use std::io::Write;
                    if let Some(statement) =
                        prompt_statement_selection(statements, &stdin, &mut stdout)
                    {
                        print!("Filter expression (e.g., status=FAILED): ");
                        let _ = stdout.flush();
                        let mut expr_input = String::new();
                        if stdin.read_line(&mut expr_input).is_ok() {
                            let expr = expr_input.trim();
                            if !expr.is_empty() {
                                let (field, operator, value) = parse_filter_expr(expr);
                                if let Some(op) = operator {
                                    Some(DebugCommand::Filter {
                                        statement,
                                        field,
                                        operator: op,
                                        value,
                                    })
                                } else {
                                    println!("Invalid filter expression.");
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    println!("Usage: filter <statement> <field>=<value>");
                    println!("  e.g., filter 1 status=FAILED");
                    println!("  Operators: = != > < >= <= ~ (contains)");
                    None
                }
            }

            "export" | "ex" => {
                let statements = session.executor().statements();

                if parts.len() >= 3 {
                    let arg = parts[1];
                    let path_str = parts[2];

                    if let Some(statement) = resolve_statement_arg(arg, statements) {
                        let path = std::path::PathBuf::from(path_str);
                        let format = if path_str.ends_with(".csv") {
                            ExportFormat::Csv
                        } else {
                            ExportFormat::Json
                        };

                        Some(DebugCommand::Export {
                            statement,
                            path,
                            format,
                        })
                    } else {
                        None
                    }
                } else if parts.len() == 1 {
                    // No args - prompt for statement first, then filename
                    use std::io::Write;
                    if let Some(statement) =
                        prompt_statement_selection(statements, &stdin, &mut stdout)
                    {
                        print!("Export filename (e.g., results.json or data.csv): ");
                        let _ = stdout.flush();
                        let mut path_input = String::new();
                        if stdin.read_line(&mut path_input).is_ok() {
                            let path_str = path_input.trim();
                            if !path_str.is_empty() {
                                let path = std::path::PathBuf::from(path_str);
                                let format = if path_str.ends_with(".csv") {
                                    ExportFormat::Csv
                                } else {
                                    ExportFormat::Json
                                };
                                Some(DebugCommand::Export {
                                    statement,
                                    path,
                                    format,
                                })
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    println!("Usage: export <statement> <file.json|file.csv>");
                    println!("  e.g., export 1 results.json");
                    None
                }
            }

            "q" | "quit" | "exit" => Some(DebugCommand::Quit),
            "h" | "help" | "?" => {
                if execution_finished {
                    println!("Inspection Commands (execution complete):");
                    println!("  l, list        - List all statements with SQL");
                    println!("  i <N>          - Inspect output from statement N");
                    println!("  ia, inspect-all - Inspect all captured outputs");
                    println!("  hi, history    - Show command history");
                    println!("  st, status     - Show current state");
                    println!("  topics         - List all Kafka topics with offsets");
                    println!("  schema <topic> - Show schema for topic (inferred from message)");
                    println!("  consumers      - List all consumer groups");
                    println!("  jobs           - List all jobs with source/sink status");
                    println!();
                    println!("Data Visibility:");
                    println!(
                        "  messages <topic|N> [--last N] [--first N] - Peek at topic messages"
                    );
                    println!("     (Use topic number from 'topics' list, e.g., 'messages 1')");
                    println!("  head <stmt|N> [-n N]   - Show first N records (default: 10)");
                    println!("  tail <stmt|N> [-n N]   - Show last N records (default: 10)");
                    println!(
                        "  filter <stmt|N> <field><op><value> - Filter records (op: =,!=,>,<,~)"
                    );
                    println!("  export <stmt|N> <file> - Export records to JSON/CSV file");
                    println!();
                    println!("  q, quit        - Exit and cleanup");
                } else {
                    println!("Debug Commands:");
                    println!("  s, step        - Execute next statement");
                    println!("  c, continue    - Run until next breakpoint");
                    println!("  r, run         - Run all remaining statements");
                    println!("  b <N>          - Set breakpoint on statement N");
                    println!("  u <N>          - Remove breakpoint from statement N");
                    println!("  cb, clear      - Clear all breakpoints");
                    println!("  l, list        - List all statements");
                    println!("  i <N>          - Inspect output from statement N");
                    println!("  ia, inspect-all - Inspect all captured outputs");
                    println!("  hi, history    - Show command history");
                    println!("  st, status     - Show current state");
                    println!("  topics         - List all Kafka topics with offsets");
                    println!("  schema <topic> - Show schema for topic (inferred from message)");
                    println!("  consumers      - List all consumer groups");
                    println!("  jobs           - List all jobs with source/sink status");
                    println!();
                    println!("Data Visibility:");
                    println!(
                        "  messages <topic|N> [--last N] [--first N] - Peek at topic messages"
                    );
                    println!("     (Use topic number from 'topics' list, e.g., 'messages 1')");
                    println!("  head <stmt|N> [-n N]   - Show first N records (default: 10)");
                    println!("  tail <stmt|N> [-n N]   - Show last N records (default: 10)");
                    println!(
                        "  filter <stmt|N> <field><op><value> - Filter records (op: =,!=,>,<,~)"
                    );
                    println!("  export <stmt|N> <file> - Export records to JSON/CSV file");
                    println!();
                    println!("  q, quit        - Exit debugger");
                }
                None
            }
            _ => {
                println!("Unknown command: {}. Type 'h' for help.", cmd);
                None
            }
        };

        if let Some(debug_cmd) = debug_cmd {
            match session.execute_command(debug_cmd).await {
                Ok(result) => match result {
                    DebugCommandResult::StepResult(Some(res)) => {
                        let icon = if res.success { "✅" } else { "❌" };
                        println!(
                            "   {} {} completed in {}ms",
                            icon, res.name, res.execution_time_ms
                        );
                        if let Some(ref err) = res.error {
                            println!("      Error: {}", err);
                        }
                        if let Some(ref out) = res.output {
                            println!(
                                "      Output: {} records to {}",
                                out.records.len(),
                                out.sink_name
                            );
                        }
                        // Show table snapshot for CREATE TABLE statements
                        if let Some(ref snapshot) = res.table_snapshot {
                            println!("      📊 Table: {} rows", snapshot.stats.record_count);
                            if !snapshot.records.is_empty() {
                                println!("      Sample (first 5):");
                                for (idx, (key, fields)) in
                                    snapshot.records.iter().take(5).enumerate()
                                {
                                    let field_str: String = fields
                                        .iter()
                                        .take(4)
                                        .map(|(k, v)| format!("{}={:?}", k, v))
                                        .collect::<Vec<_>>()
                                        .join(", ");
                                    println!("         [{}] {} → {{{}}}", idx + 1, key, field_str);
                                }
                            }
                        }
                        // Show captured ERROR/WARN logs (top 5)
                        if !res.captured_logs.is_empty() {
                            let log_count = res.captured_logs.len();
                            let display_count = log_count.min(5);
                            println!(
                                "      📋 Logs ({} captured, showing {}):",
                                log_count, display_count
                            );
                            for entry in res.captured_logs.iter().take(5) {
                                println!("         {}", entry.display_short());
                            }
                        }

                        // Auto-display topic state after step
                        println!();
                        println!("   📋 Topic State:");
                        match session
                            .execute_command(
                                velostream::velostream::test_harness::DebugCommand::ListTopics,
                            )
                            .await
                        {
                            Ok(DebugCommandResult::TopicListing(topics)) => {
                                if topics.is_empty() {
                                    println!("      (no topics)");
                                } else {
                                    for topic in &topics {
                                        let test_marker =
                                            if topic.is_test_topic { " [test]" } else { "" };
                                        println!(
                                            "      • {}{}: {} msgs",
                                            topic.name, test_marker, topic.total_messages
                                        );
                                        for p in &topic.partitions {
                                            if p.message_count > 0 {
                                                println!(
                                                    "        P{}: {} msgs [{}..{}]",
                                                    p.partition,
                                                    p.message_count,
                                                    p.low_offset,
                                                    p.high_offset
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                println!("      (failed to fetch topics: {})", e);
                            }
                        }
                    }
                    DebugCommandResult::StepResult(None) => {
                        println!("✅ No more statements to execute");
                    }
                    DebugCommandResult::ExecutionResults(results) => {
                        for res in &results {
                            let icon = if res.success { "✅" } else { "❌" };
                            println!("   {} {} ({}ms)", icon, res.name, res.execution_time_ms);
                            if let Some(ref err) = res.error {
                                println!("      Error: {}", err);
                            }
                            // Show table snapshot for CREATE TABLE statements
                            if let Some(ref snapshot) = res.table_snapshot {
                                println!("      📊 Table: {} rows", snapshot.stats.record_count);
                            }
                            // Show captured ERROR/WARN logs (top 5 per statement)
                            if !res.captured_logs.is_empty() {
                                let log_count = res.captured_logs.len();
                                let display_count = log_count.min(5);
                                println!(
                                    "      📋 Logs ({} captured, showing {}):",
                                    log_count, display_count
                                );
                                for entry in res.captured_logs.iter().take(5) {
                                    println!("         {}", entry.display_short());
                                }
                            }
                        }
                        println!("   Executed {} statements", results.len());
                    }
                    DebugCommandResult::Message(msg) => {
                        println!("   {}", msg);
                    }
                    DebugCommandResult::Listing(lines) => {
                        for line in lines {
                            println!("   {}", line);
                        }
                    }
                    DebugCommandResult::Output(out) => {
                        println!("   Sink: {}", out.sink_name);
                        println!("   Records: {}", out.records.len());
                        println!("   Execution time: {}ms", out.execution_time_ms);
                        if !out.records.is_empty() {
                            println!("   Sample (first 5):");
                            for (i, rec) in out.records.iter().take(5).enumerate() {
                                let key_json = rec
                                    .key
                                    .as_ref()
                                    .map(|k| {
                                        serde_json::to_string(k)
                                            .unwrap_or_else(|_| format!("{:?}", k))
                                    })
                                    .unwrap_or_else(|| "null".to_string());
                                // Sort fields for consistent output
                                let sorted_fields: std::collections::BTreeMap<_, _> =
                                    rec.fields.iter().collect();
                                let value_json = serde_json::to_string_pretty(&sorted_fields)
                                    .unwrap_or_else(|_| format!("{:?}", rec.fields));
                                let headers_json = if rec.headers.is_empty() {
                                    "{}".to_string()
                                } else {
                                    // Sort headers for consistent output
                                    let sorted_headers: std::collections::BTreeMap<_, _> =
                                        rec.headers.iter().collect();
                                    serde_json::to_string(&sorted_headers)
                                        .unwrap_or_else(|_| format!("{:?}", rec.headers))
                                };
                                println!("     [{}] p{}@{}", i + 1, rec.partition, rec.offset);
                                println!("         key: {}", key_json);
                                println!(
                                    "         value: {}",
                                    value_json.replace('\n', "\n                ")
                                );
                                if !rec.headers.is_empty() {
                                    println!("         headers: {}", headers_json);
                                }
                            }
                        }
                    }
                    DebugCommandResult::OutputWithLogs { output, logs } => {
                        println!("   Sink: {}", output.sink_name);
                        println!("   Records: {}", output.records.len());
                        println!("   Execution time: {}ms", output.execution_time_ms);
                        if !output.records.is_empty() {
                            println!("   Sample (first 5):");
                            for (i, rec) in output.records.iter().take(5).enumerate() {
                                let key_json = rec
                                    .key
                                    .as_ref()
                                    .map(|k| {
                                        serde_json::to_string(k)
                                            .unwrap_or_else(|_| format!("{:?}", k))
                                    })
                                    .unwrap_or_else(|| "null".to_string());
                                // Sort fields for consistent output
                                let sorted_fields: std::collections::BTreeMap<_, _> =
                                    rec.fields.iter().collect();
                                let value_json = serde_json::to_string_pretty(&sorted_fields)
                                    .unwrap_or_else(|_| format!("{:?}", rec.fields));
                                let headers_json = if rec.headers.is_empty() {
                                    "{}".to_string()
                                } else {
                                    // Sort headers for consistent output
                                    let sorted_headers: std::collections::BTreeMap<_, _> =
                                        rec.headers.iter().collect();
                                    serde_json::to_string(&sorted_headers)
                                        .unwrap_or_else(|_| format!("{:?}", rec.headers))
                                };
                                println!("     [{}] p{}@{}", i + 1, rec.partition, rec.offset);
                                println!("         key: {}", key_json);
                                println!(
                                    "         value: {}",
                                    value_json.replace('\n', "\n                ")
                                );
                                if !rec.headers.is_empty() {
                                    println!("         headers: {}", headers_json);
                                }
                            }
                        }
                        // Show captured ERROR/WARN logs (top 5)
                        if !logs.is_empty() {
                            let log_count = logs.len();
                            let display_count = log_count.min(5);
                            println!(
                                "   📋 Logs ({} captured, showing {}):",
                                log_count, display_count
                            );
                            for entry in logs.iter().take(5) {
                                println!("      {}", entry.display_short());
                            }
                        }
                    }
                    DebugCommandResult::AllOutputs(outputs) => {
                        println!("   {} captured outputs:", outputs.len());
                        for (name, out) in &outputs {
                            println!(
                                "   • {} → {} ({} records, {}ms)",
                                name,
                                out.sink_name,
                                out.records.len(),
                                out.execution_time_ms
                            );
                        }
                    }
                    DebugCommandResult::HistoryListing(lines) => {
                        println!("   Command history:");
                        for line in lines {
                            println!("   {}", line);
                        }
                    }
                    DebugCommandResult::TopicListing(topics) => {
                        // Store topic names for numbered references
                        last_topic_list = topics.iter().map(|t| t.name.clone()).collect();

                        println!("   📋 Topics ({}):", topics.len());
                        println!("   (Use number with 'messages N' for quick access)");
                        for (idx, topic) in topics.iter().enumerate() {
                            let test_marker = if topic.is_test_topic { " [test]" } else { "" };
                            println!(
                                "   [{}] {}{} ({} messages)",
                                idx + 1,
                                topic.name,
                                test_marker,
                                topic.total_messages
                            );
                            for p in &topic.partitions {
                                // Format timestamp if available
                                let ts_str = p.latest_timestamp_ms.map(|ts| {
                                    use chrono::{TimeZone, Utc};
                                    Utc.timestamp_millis_opt(ts)
                                        .single()
                                        .map(|dt| dt.format("%H:%M:%S").to_string())
                                        .unwrap_or_else(|| format!("{}ms", ts))
                                });

                                // Format key if available (truncate if too long)
                                let key_str = p.latest_key.as_ref().map(|k| {
                                    if k.len() > 20 {
                                        format!("{}...", &k[..17])
                                    } else {
                                        k.clone()
                                    }
                                });

                                // Build the last message info string
                                // Always show key status (None if not set)
                                let last_info = match (&key_str, &ts_str) {
                                    (Some(k), Some(t)) => {
                                        format!(" [last: key=\"{}\", @{}]", k, t)
                                    }
                                    (Some(k), None) => format!(" [last: key=\"{}\"]", k),
                                    (None, Some(t)) => format!(" [last: key=None, @{}]", t),
                                    (None, None) => {
                                        if p.message_count > 0 {
                                            " [last: key=None]".to_string()
                                        } else {
                                            String::new()
                                        }
                                    }
                                };

                                println!(
                                    "     └─ P{}: {} msgs (offsets: {}..{}){}",
                                    p.partition,
                                    p.message_count,
                                    p.low_offset,
                                    p.high_offset,
                                    last_info
                                );
                            }
                        }
                    }
                    DebugCommandResult::ConsumerListing(consumers) => {
                        println!("   👥 Consumers ({}):", consumers.len());
                        for consumer in &consumers {
                            println!("   • {} ({:?})", consumer.group_id, consumer.state);
                            println!("     Topics: {}", consumer.subscribed_topics.join(", "));
                            for pos in &consumer.positions {
                                println!(
                                    "     └─ {}[P{}]: offset {} (lag: {})",
                                    pos.topic, pos.partition, pos.offset, pos.lag
                                );
                            }
                        }
                    }
                    DebugCommandResult::JobListing(jobs) => {
                        use velostream::velostream::test_harness::{
                            DataSinkType, DataSourceType, JobType,
                        };
                        println!("   🔧 Jobs ({}):", jobs.len());
                        for job in &jobs {
                            let state_icon = match job.state {
                                velostream::velostream::test_harness::JobState::Initializing => "🔄",
                                velostream::velostream::test_harness::JobState::Running => "▶️",
                                velostream::velostream::test_harness::JobState::Completed => "✅",
                                velostream::velostream::test_harness::JobState::CompletedNoOutput => "⚠️",
                                velostream::velostream::test_harness::JobState::Failed => "❌",
                                velostream::velostream::test_harness::JobState::Paused => "⏸️",
                                velostream::velostream::test_harness::JobState::Stopped => "⏹️",
                            };
                            let type_icon = match job.job_type {
                                JobType::Stream => "🌊",
                                JobType::Table => "📊",
                                JobType::Unknown => "❓",
                            };
                            println!(
                                "\n   {} {} {} [{}] ({:?})",
                                state_icon,
                                type_icon,
                                job.name,
                                job.job_type.display_name(),
                                job.state
                            );

                            // Show SQL (truncated if too long)
                            let sql_display = if job.sql.len() > 100 {
                                format!("{}...", &job.sql[..100].replace('\n', " "))
                            } else {
                                job.sql.replace('\n', " ")
                            };
                            println!("     📝 SQL: {}", sql_display);

                            // Show stats
                            println!(
                                "     📈 Stats: {} written, {} errors, {}ms",
                                job.stats.records_written,
                                job.stats.records_errored,
                                job.stats.execution_time_ms
                            );

                            // Show partitioner
                            if let Some(ref partitioner) = job.partitioner {
                                println!("     🔀 Partitioner: {}", partitioner);
                            }

                            // Table-specific stats
                            if job.job_type == JobType::Table {
                                println!("     📊 Table State:");
                                if let Some(count) = job.table_record_count {
                                    println!("       Records: {}", count);
                                }
                                if let Some(keys) = job.table_key_count {
                                    println!("       Unique keys: {}", keys);
                                }
                                if let Some(ref updated) = job.table_last_updated {
                                    println!("       Last updated: {}", updated);
                                }
                            }

                            // Display detailed data sources
                            if !job.sources.is_empty() {
                                println!("     📥 Sources:");
                                for src in &job.sources {
                                    let type_str = match &src.source_type {
                                        DataSourceType::Kafka => "Kafka",
                                        DataSourceType::File => "File",
                                        DataSourceType::Other(s) => s.as_str(),
                                    };
                                    println!("       • {} [{}]", src.name, type_str);
                                    // Show config details
                                    if let Some(ref servers) = src.bootstrap_servers {
                                        println!("         bootstrap.servers: {}", servers);
                                    }
                                    if let Some(ref group) = src.consumer_group {
                                        println!("         group.id: {}", group);
                                    }
                                    if let Some(ref reset) = src.auto_offset_reset {
                                        println!("         auto.offset.reset: {}", reset);
                                    }
                                    if let Some(ref format) = src.format {
                                        println!("         format: {}", format);
                                    }
                                    // Stats
                                    if src.records_read > 0 || src.bytes_read > 0 {
                                        println!(
                                            "         stats: {} records, {} bytes",
                                            src.records_read, src.bytes_read
                                        );
                                    }
                                    // Last record timestamp
                                    if let Some(ref ts) = src.last_record_time {
                                        println!("         last read: {}", ts.format("%H:%M:%S"));
                                    }
                                    if let Some(lag) = src.lag {
                                        println!("         lag: {} messages", lag);
                                    }
                                    if let Some(ref offsets) = src.current_offsets {
                                        let offset_str: Vec<String> = offsets
                                            .iter()
                                            .map(|(p, o)| format!("P{}:{}", p, o))
                                            .collect();
                                        println!("         offsets: {}", offset_str.join(", "));
                                    }
                                    if let Some(ref path) = src.file_path {
                                        let consumed = if src.fully_consumed == Some(true) {
                                            " (EOF)"
                                        } else {
                                            ""
                                        };
                                        println!("         path: {}{}", path, consumed);
                                    }
                                }
                            }

                            // Display detailed data sinks
                            if !job.sinks.is_empty() {
                                println!("     📤 Sinks:");
                                for sink in &job.sinks {
                                    let type_str = match &sink.sink_type {
                                        DataSinkType::Kafka => "Kafka",
                                        DataSinkType::File => "File",
                                        DataSinkType::Other(s) => s.as_str(),
                                    };
                                    println!("       • {} [{}]", sink.name, type_str);
                                    // Show config details
                                    if let Some(ref servers) = sink.bootstrap_servers {
                                        println!("         bootstrap.servers: {}", servers);
                                    }
                                    if let Some(ref format) = sink.format {
                                        println!("         format: {}", format);
                                    }
                                    if let Some(ref compression) = sink.compression {
                                        println!("         compression: {}", compression);
                                    }
                                    if let Some(ref acks) = sink.acks {
                                        println!("         acks: {}", acks);
                                    }
                                    // Stats - only show if there's data, and only show bytes if non-zero
                                    if sink.records_written > 0 || sink.bytes_written > 0 {
                                        if sink.bytes_written > 0 {
                                            println!(
                                                "         stats: {} records, {} bytes",
                                                sink.records_written, sink.bytes_written
                                            );
                                        } else {
                                            println!(
                                                "         stats: {} records",
                                                sink.records_written
                                            );
                                        }
                                    }
                                    // Last record timestamp
                                    if let Some(ref ts) = sink.last_record_time {
                                        println!("         last write: {}", ts.format("%H:%M:%S"));
                                    }
                                    if let Some(ref offsets) = sink.produced_offsets {
                                        let offset_str: Vec<String> = offsets
                                            .iter()
                                            .map(|(p, o)| format!("P{}:{}", p, o))
                                            .collect();
                                        println!("         produced: {}", offset_str.join(", "));
                                    }
                                    if let Some(ref path) = sink.file_path {
                                        let flushed = if sink.is_flushed == Some(true) {
                                            " (flushed)"
                                        } else {
                                            ""
                                        };
                                        println!("         path: {}{}", path, flushed);
                                    }
                                }
                            } else if !job.sink_topics.is_empty() {
                                // Fallback to legacy sink_topics if sinks is empty
                                println!("     📤 Sinks: {}", job.sink_topics.join(", "));
                            }
                        }
                    }
                    DebugCommandResult::SchemaDisplay(schema) => {
                        println!();
                        println!(
                            "📋 Schema for '{}' (sampled {} record{}):",
                            schema.topic,
                            schema.records_sampled,
                            if schema.records_sampled == 1 { "" } else { "s" }
                        );
                        if schema.fields.is_empty() {
                            println!("   (no messages found or topic is empty)");
                        } else {
                            println!(
                                "   Key: {}",
                                if schema.has_keys { "present" } else { "None" }
                            );
                            println!("   Fields:");
                            for (name, type_name) in &schema.fields {
                                println!("     • {}: {}", name, type_name);
                            }
                            if let Some(ref sample) = schema.sample_value {
                                println!("\n   Sample value:");
                                for line in sample.lines() {
                                    println!("     {}", line);
                                }
                            }
                        }
                        println!();
                    }
                    DebugCommandResult::MessagesResult(messages) => {
                        println!();
                        if messages.is_empty() {
                            println!("📨 No messages found");
                        } else {
                            println!(
                                "📨 {} message{}:",
                                messages.len(),
                                if messages.len() == 1 { "" } else { "s" }
                            );
                            for (idx, msg) in messages.iter().enumerate() {
                                // Format timestamp
                                let ts_str = msg.timestamp_ms.map(|ts| {
                                    use chrono::{TimeZone, Utc};
                                    Utc.timestamp_millis_opt(ts)
                                        .single()
                                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
                                        .unwrap_or_else(|| format!("{}ms", ts))
                                });

                                println!("  ─────────────────────────────────────────");
                                println!(
                                    "  [{}] P{}:offset {}",
                                    idx + 1,
                                    msg.partition,
                                    msg.offset
                                );
                                if let Some(ts) = ts_str {
                                    println!("      timestamp: {}", ts);
                                }
                                println!("      key: {}", msg.key.as_deref().unwrap_or("<null>"));
                                if !msg.headers.is_empty() {
                                    println!("      headers:");
                                    for (hk, hv) in &msg.headers {
                                        // Truncate long header values
                                        let display_val = if hv.len() > 50 {
                                            format!("{}...", &hv[..47])
                                        } else {
                                            hv.clone()
                                        };
                                        println!("        {}: {}", hk, display_val);
                                    }
                                }
                                // Format JSON value with sorted keys (compact, single line)
                                let formatted_value =
                                    serde_json::from_str::<serde_json::Value>(&msg.value)
                                        .ok()
                                        .map(|v| {
                                            // Convert to BTreeMap for sorted keys
                                            if let serde_json::Value::Object(map) = v {
                                                let sorted: std::collections::BTreeMap<_, _> =
                                                    map.into_iter().collect();
                                                serde_json::to_string(&sorted)
                                                    .unwrap_or_else(|_| msg.value.clone())
                                            } else {
                                                serde_json::to_string(&v)
                                                    .unwrap_or_else(|_| msg.value.clone())
                                            }
                                        })
                                        .unwrap_or_else(|| msg.value.clone());
                                println!("      value: {}", formatted_value);
                            }
                        }
                        println!();
                    }
                    DebugCommandResult::RecordsResult {
                        statement,
                        records,
                        total_count,
                        showing,
                    } => {
                        println!();
                        if records.is_empty() {
                            println!("📊 No records found for statement '{}'", statement);
                        } else {
                            println!(
                                "📊 {} ({} of {} records) from '{}':",
                                showing,
                                records.len(),
                                total_count,
                                statement
                            );

                            // Extract and display schema/header from first record
                            if let Some(first) = records.first() {
                                let mut field_names: Vec<&String> = first.fields.keys().collect();
                                field_names.sort(); // Consistent ordering
                                println!(
                                    "   Schema: [{}]",
                                    field_names
                                        .iter()
                                        .map(|s| s.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                );
                                println!("   ─────────────────────────────────────────");
                            }

                            for (idx, record) in records.iter().enumerate() {
                                // Format record fields as JSON with sorted keys
                                let sorted_fields: std::collections::BTreeMap<_, _> =
                                    record.fields.iter().collect();
                                let json = serde_json::to_string(&sorted_fields)
                                    .unwrap_or_else(|_| format!("{:?}", record.fields));
                                // Extract key as string
                                let key_str = record
                                    .key
                                    .as_ref()
                                    .map(|k| k.to_string())
                                    .unwrap_or_else(|| "<null>".to_string());
                                // Show partition:offset for context
                                let pos_str = format!("p{}@{}", record.partition, record.offset);
                                // Show headers if present
                                let headers_str = if record.headers.is_empty() {
                                    String::new()
                                } else {
                                    format!(" headers={:?}", record.headers)
                                };
                                println!(
                                    "  [{}] key={} {} value={}{}",
                                    idx + 1,
                                    key_str,
                                    pos_str,
                                    json,
                                    headers_str
                                );
                            }
                        }
                        println!();
                    }
                    DebugCommandResult::FilteredResult {
                        statement,
                        records,
                        total_count,
                        matched_count,
                        filter_expr,
                    } => {
                        println!();
                        println!("🔍 Filtered '{}' where {}:", statement, filter_expr);
                        println!(
                            "   Found {} of {} records matching",
                            matched_count, total_count
                        );
                        if records.is_empty() {
                            println!("   (no matching records)");
                        } else {
                            // Extract and display schema/header from first record
                            if let Some((_, first)) = records.first() {
                                let mut field_names: Vec<&String> = first.fields.keys().collect();
                                field_names.sort(); // Consistent ordering
                                println!(
                                    "   Schema: [{}]",
                                    field_names
                                        .iter()
                                        .map(|s| s.as_str())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                );
                                println!("   ─────────────────────────────────────────");
                            }

                            for (idx, record) in records.iter() {
                                // Format record fields as JSON with sorted keys
                                let sorted_fields: std::collections::BTreeMap<_, _> =
                                    record.fields.iter().collect();
                                let json = serde_json::to_string(&sorted_fields)
                                    .unwrap_or_else(|_| format!("{:?}", record.fields));
                                let key_str = record
                                    .key
                                    .as_ref()
                                    .map(|k| k.to_string())
                                    .unwrap_or_else(|| "<null>".to_string());
                                let pos_str = format!("p{}@{}", record.partition, record.offset);
                                println!("  [{}] key={} {} value={}", idx, key_str, pos_str, json);
                            }
                        }
                        println!();
                    }
                    DebugCommandResult::ExportResult {
                        path,
                        record_count,
                        format,
                    } => {
                        println!();
                        println!(
                            "✅ Exported {} records to {} ({})",
                            record_count,
                            path.display(),
                            format
                        );
                        println!();
                    }
                    DebugCommandResult::Quit => {
                        println!("👋 Exiting debugger");
                        break;
                    }
                },
                Err(e) => {
                    eprintln!("❌ Command error: {}", e);
                }
            }
        }

        println!();
    }

    // Cleanup
    if !keep_containers {
        println!("🧹 Cleaning up...");
        if let Err(e) = session.executor_mut().executor_mut().stop().await {
            log::warn!("Failed to stop executor: {}", e);
        }
    } else {
        println!("🐳 Keeping containers running (--keep-containers)");
    }

    // Summary
    let results = session.executor().results();
    let passed = results.iter().filter(|r| r.success).count();
    let total_stmts = session.executor().statements().len();
    println!();
    println!("📊 Debug Session Summary");
    println!("   Executed: {}/{} statements", results.len(), total_stmts);
    println!("   Passed: {}/{}", passed, results.len());

    if passed < results.len() {
        std::process::exit(1);
    }

    Ok(())
}

/// Validate SQL syntax and structure
///
/// This function validates a SQL application file using SqlValidator.
/// It checks for parsing errors and reports validation results.
///
/// # Arguments
/// * `sql_file` - Path to the SQL file to validate
/// * `verbose` - Show full query text in error output
pub async fn validate(sql_file: PathBuf, verbose: bool) -> CommandResult {
    println!("🔍 Validating SQL: {}", sql_file.display());

    // Use existing SqlValidator with SQL file's directory as base for config_file resolution
    // Canonicalize to get absolute path, so relative paths like '../configs/' resolve correctly
    use velostream::velostream::sql::validator::SqlValidator;

    let sql_dir = sql_file
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .canonicalize()
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
    let validator = SqlValidator::with_base_dir(&sql_dir);
    let result = validator.validate_application_file(&sql_file);

    println!();
    println!("📊 Validation Results");
    println!("═════════════════════");
    println!(
        "Application: {}",
        result.application_name.as_deref().unwrap_or("unknown")
    );
    let invalid = result.total_queries.saturating_sub(result.valid_queries);
    println!(
        "Total Queries: {}, Valid: {}, Invalid: {}",
        result.total_queries, result.valid_queries, invalid
    );
    println!();

    if result.is_valid {
        println!("✅ All queries valid!");
    } else {
        println!("❌ Validation failed");
        for query_result in &result.query_results {
            if !query_result.is_valid {
                println!(
                    "\n  Query #{} (Line {}): {}",
                    query_result.query_index + 1,
                    query_result.start_line,
                    if verbose {
                        &query_result.query_text
                    } else {
                        "..."
                    }
                );
                for error in &query_result.parsing_errors {
                    println!("    ❌ {}", error.message);
                }
            }
        }
    }

    if !result.is_valid {
        std::process::exit(1);
    }

    Ok(())
}

/// Run stress tests on a SQL application
///
/// This function executes stress testing with configurable load:
/// 1. Load or generate test specification
/// 2. Build stress test configuration
/// 3. Load schemas from spec
/// 4. Run stress test with specified records and duration
/// 5. Generate performance report
///
/// # Arguments
/// * `sql_file` - Path to the SQL file to stress test
/// * `spec` - Optional path to test specification YAML
/// * `records` - Optional number of records per source (default: 100000)
/// * `duration` - Optional max duration in seconds (default: 60)
/// * `output` - Output format (text, json)
/// * `yes` - Skip interactive prompts
#[allow(clippy::too_many_arguments)]
pub fn stress(
    sql_file: PathBuf,
    spec: Option<PathBuf>,
    records: Option<usize>,
    duration: Option<u64>,
    output: String,
    yes: bool,
) -> CommandResult {
    use velostream::velostream::test_harness::SpecGenerator;
    use velostream::velostream::test_harness::stress::{StressConfig, StressRunner};

    println!("🔥 Stress Test Mode");
    println!("SQL File: {}", sql_file.display());

    // Auto-discover or prompt for spec
    let spec = if spec.is_some() {
        spec
    } else if let Some(discovered) = crate::discovery::discover_spec(&sql_file) {
        println!("📂 Auto-discovered spec: {}", discovered.display());
        Some(discovered)
    } else if !yes {
        let prompt_spec = crate::discovery::prompt_yes_no(
            "No spec file found. Would you like to auto-generate from SQL?",
            true,
        );
        if prompt_spec {
            None // Will generate from SQL
        } else {
            eprintln!("❌ No spec file and user declined auto-generation");
            std::process::exit(1);
        }
    } else {
        None // Will generate from SQL
    };

    if let Some(ref s) = spec {
        println!("Test Spec: {}", s.display());
    }

    // Get records with default or prompt
    let records = records.unwrap_or_else(|| {
        if yes {
            100000 // Default for stress tests
        } else {
            let input =
                crate::discovery::prompt_with_default("Number of records per source", "100000");
            input.parse().unwrap_or(100000)
        }
    });

    // Get duration with default or prompt
    let duration = duration.unwrap_or_else(|| {
        if yes {
            60 // Default 60 seconds
        } else {
            let input = crate::discovery::prompt_with_default("Max duration (seconds)", "60");
            input.parse().unwrap_or(60)
        }
    });

    println!("Records per source: {}", records);
    println!("Max Duration: {}s", duration);
    println!("Output Format: {}", output);
    println!();

    // Build stress config
    let stress_config = StressConfig::with_records(records)
        .batch_size(1000)
        .measure_latency(true);

    // Try to load test spec if provided, otherwise generate from SQL
    let spec_generator = SpecGenerator::new();
    let test_spec = if let Some(ref spec_path) = spec {
        // Load from file
        let content = std::fs::read_to_string(spec_path)?;
        serde_yaml::from_str(&content)?
    } else {
        // Generate from SQL file
        match spec_generator.generate_from_sql(&sql_file) {
            Ok(s) => {
                println!("📝 Auto-generated test spec from SQL");
                // Offer to save the generated spec
                crate::discovery::offer_save_spec(&sql_file, &s, yes);
                s
            }
            Err(e) => {
                eprintln!("❌ Failed to analyze SQL file: {}", e);
                std::process::exit(1);
            }
        }
    };

    // Get schema directory (same as SQL file directory or spec directory)
    let schema_dir = spec
        .as_ref()
        .and_then(|p| p.parent())
        .unwrap_or_else(|| sql_file.parent().unwrap_or(std::path::Path::new(".")));

    println!("📊 Running stress test...");
    println!("   Sources: {}", test_spec.queries.len());
    println!("   Schema dir: {}", schema_dir.display());
    println!();

    let mut runner = StressRunner::new(stress_config);

    // Load schemas for each input in queries
    for query in &test_spec.queries {
        for input in &query.inputs {
            if let Some(schema_file) = &input.schema {
                let schema_path = schema_dir.join(schema_file);
                if schema_path.exists() {
                    if let Err(e) = runner.load_schema_file(&input.source, &schema_path) {
                        eprintln!("⚠️  Failed to load schema {}: {}", schema_path.display(), e);
                    } else {
                        println!("   ✓ {} ← {}", input.source, schema_path.display());
                    }
                }
            }
        }
    }

    // Run the stress test
    match runner.run() {
        Ok(metrics) => {
            let report = runner.generate_report(&metrics);

            match output.as_str() {
                "json" => {
                    let json = serde_json::to_string_pretty(&metrics)?;
                    println!("{}", json);
                }
                _ => {
                    println!("{}", report);
                }
            }

            println!("✅ Stress test completed successfully");
        }
        Err(e) => {
            eprintln!("❌ Stress test failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}

pub async fn init(
    sql_file: PathBuf,
    output: Option<PathBuf>,
    ai: bool,
    yes: bool,
) -> CommandResult {
    use velostream::velostream::test_harness::SpecGenerator;
    use velostream::velostream::test_harness::ai::AiAssistant;

    println!("📝 Generating test specification");
    println!("SQL File: {}", sql_file.display());

    // Determine output path with auto-discovery and prompting
    let output = output.unwrap_or_else(|| {
        let default_output = crate::discovery::default_spec_output(&sql_file);
        if yes {
            println!("Output: {} (auto)", default_output.display());
            default_output
        } else {
            let path = crate::discovery::prompt_with_default(
                "Output path",
                &default_output.display().to_string(),
            );
            PathBuf::from(path)
        }
    });

    println!("Output: {}", output.display());
    if ai {
        println!("AI Generation: enabled");
    }
    println!();

    let generator = SpecGenerator::new();
    let mut spec_generated = false;

    // If AI mode is enabled, try AI-powered generation first
    if ai {
        let ai_assistant = AiAssistant::new();

        if !ai_assistant.is_available() {
            eprintln!("⚠️  AI mode requested but ANTHROPIC_API_KEY not set");
            eprintln!("   Falling back to rule-based generation");
            eprintln!();
        } else {
            println!("🤖 Using AI-powered test spec generation...");

            // Read SQL content
            let sql_content = match std::fs::read_to_string(&sql_file) {
                Ok(content) => content,
                Err(e) => {
                    eprintln!("❌ Failed to read SQL file: {}", e);
                    std::process::exit(1);
                }
            };

            // Get app name from file stem
            let app_name = sql_file
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "application".to_string());

            println!("   SQL file: {} bytes", sql_content.len());
            println!("   Application: {}", app_name);

            // Call AI to generate test spec
            match ai_assistant
                .generate_test_spec(&sql_content, &app_name)
                .await
            {
                Ok(spec) => {
                    // Write spec to output file
                    match generator.write_spec(&spec, &output) {
                        Ok(_) => {
                            println!("✅ AI Generated test specification: {}", output.display());
                            println!();
                            println!("📊 Summary:");
                            println!("   • Application: {}", spec.application);
                            println!("   • Queries: {}", spec.queries.len());
                            for query in &spec.queries {
                                println!(
                                    "     - {} ({} assertions)",
                                    query.name,
                                    query.assertions.len()
                                );
                            }
                            println!();
                            println!(
                                "💡 Edit {} to customize assertions and input configurations",
                                output.display()
                            );
                            spec_generated = true;
                        }
                        Err(e) => {
                            eprintln!("❌ Failed to write AI-generated spec: {}", e);
                            eprintln!("   Falling back to rule-based generation");
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  ❌ AI test spec generation failed: {}", e);
                    eprintln!("   Falling back to rule-based generation");
                }
            }
        }
    }

    // Rule-based generation (if AI didn't generate, or as fallback)
    if !spec_generated {
        match generator.generate_from_sql(&sql_file) {
            Ok(spec) => {
                // Write spec to output file
                match generator.write_spec(&spec, &output) {
                    Ok(_) => {
                        println!("✅ Generated test specification: {}", output.display());
                        println!();
                        println!("📊 Summary:");
                        println!("   • Application: {}", spec.application);
                        println!("   • Queries: {}", spec.queries.len());
                        for query in &spec.queries {
                            println!(
                                "     - {} ({} assertions)",
                                query.name,
                                query.assertions.len()
                            );
                        }
                        println!();
                        println!(
                            "💡 Edit {} to customize assertions and input configurations",
                            output.display()
                        );
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to write test specification: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Failed to generate test specification: {}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
/// Infer schemas from SQL and CSV data files
///
/// This function analyzes SQL files and CSV data to automatically generate
/// schema definitions for test specifications. It supports both rule-based
/// inference and AI-powered inference using Claude.
///
/// # Arguments
/// * `sql_file` - Path to the SQL file to analyze
/// * `data_dir` - Optional directory containing CSV data files
/// * `output` - Optional output directory for generated schemas
/// * `ai` - Enable AI-powered schema inference using Claude
/// * `yes` - Skip interactive prompts
#[allow(clippy::too_many_arguments)]
pub async fn infer_schema(
    sql_file: PathBuf,
    data_dir: Option<PathBuf>,
    output: Option<PathBuf>,
    ai: bool,
    yes: bool,
) -> CommandResult {
    use velostream::velostream::test_harness::ai::{AiAssistant, CsvSample};
    use velostream::velostream::test_harness::inference::SchemaInferencer;

    println!("🔬 Inferring schemas");
    println!("SQL File: {}", sql_file.display());

    // Auto-discover data directory if not provided
    let data_dir = data_dir.or_else(|| {
        let discovered = crate::discovery::discover_data_dir(&sql_file);
        if let Some(ref p) = discovered {
            println!("📁 Auto-discovered data dir: {}", p.display());
        }
        discovered
    });

    // Prompt for data directory if not found and not using -y
    let data_dir = if data_dir.is_none() && !yes {
        let default_dir = sql_file
            .parent()
            .map(|p| p.join("data"))
            .unwrap_or_else(|| PathBuf::from("data"));
        if crate::discovery::prompt_yes_no("No data directory found. Specify one?", false) {
            let path = crate::discovery::prompt_with_default(
                "Data directory",
                &default_dir.display().to_string(),
            );
            let p = PathBuf::from(path);
            if p.exists() { Some(p) } else { None }
        } else {
            None
        }
    } else {
        data_dir
    };

    // Determine output path with auto-discovery and prompting
    let output = output.unwrap_or_else(|| {
        let default_output = crate::discovery::default_schemas_output(&sql_file);
        if yes {
            println!("Output: {} (auto)", default_output.display());
            default_output
        } else {
            let path = crate::discovery::prompt_with_default(
                "Output directory",
                &default_output.display().to_string(),
            );
            PathBuf::from(path)
        }
    });

    if let Some(ref d) = data_dir {
        println!("Data Dir: {}", d.display());
    }
    println!("Output: {}", output.display());
    if ai {
        println!("AI Inference: enabled");
    }
    println!();

    // Create output directory if it doesn't exist
    std::fs::create_dir_all(&output)?;

    let inferencer = SchemaInferencer::new();
    let mut schema_count = 0;

    // If AI mode is enabled, use Claude for intelligent inference
    if ai {
        let ai_assistant = AiAssistant::new();

        if !ai_assistant.is_available() {
            eprintln!("⚠️  AI mode requested but ANTHROPIC_API_KEY not set");
            eprintln!("   Falling back to rule-based inference");
            eprintln!();
        } else {
            println!("🤖 Using AI-powered schema inference...");

            // Read SQL content
            let sql_content = std::fs::read_to_string(&sql_file)?;

            // Collect CSV samples
            let mut csv_samples = Vec::new();
            if let Some(ref data_path) = data_dir
                && data_path.exists()
                && let Ok(entries) = std::fs::read_dir(data_path)
            {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "csv")
                        && let Ok(content) = std::fs::read_to_string(&path)
                    {
                        let total_rows = content.lines().count();
                        // Take first 50 lines as sample
                        let sample_content: String =
                            content.lines().take(50).collect::<Vec<_>>().join("\n");
                        csv_samples.push(CsvSample {
                            name: path
                                .file_stem()
                                .map(|s| s.to_string_lossy().to_string())
                                .unwrap_or_default(),
                            content: sample_content,
                            total_rows,
                        });
                    }
                }
            }

            println!("   SQL file: {} bytes", sql_content.len());
            println!("   CSV samples: {}", csv_samples.len());

            // Call AI to infer schema
            match ai_assistant.infer_schema(&sql_content, &csv_samples).await {
                Ok(schema) => {
                    let output_file = output.join(format!("{}.schema.yaml", schema.name));
                    match inferencer.write_schema(&schema, &output_file) {
                        Ok(_) => {
                            println!(
                                "  ✅ AI Generated {} ({} fields)",
                                output_file.display(),
                                schema.fields.len()
                            );
                            schema_count += 1;
                        }
                        Err(e) => {
                            eprintln!("  ❌ Failed to write {}: {}", output_file.display(), e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("  ❌ AI schema inference failed: {}", e);
                    eprintln!("   Falling back to rule-based inference");
                }
            }
        }
    }

    // Rule-based inference (always runs, or as fallback)
    if schema_count == 0 {
        // Step 1: Infer schemas from SQL file
        println!("📄 Analyzing SQL file...");
        match inferencer.infer_from_sql(&sql_file) {
            Ok(schemas) => {
                for schema in &schemas {
                    let output_file = output.join(format!("{}.schema.yaml", schema.name));
                    match inferencer.write_schema(schema, &output_file) {
                        Ok(_) => {
                            println!(
                                "  ✅ Generated {} ({} fields)",
                                output_file.display(),
                                schema.fields.len()
                            );
                            schema_count += 1;
                        }
                        Err(e) => {
                            eprintln!("  ❌ Failed to write {}: {}", output_file.display(), e);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("  ⚠️  Could not infer from SQL: {}", e);
            }
        }

        // Step 2: Infer schemas from CSV files in data directory
        if let Some(ref data_path) = data_dir
            && data_path.exists()
        {
            println!("\n📊 Analyzing data files in {}...", data_path.display());
            if let Ok(entries) = std::fs::read_dir(data_path) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "csv") {
                        match inferencer.infer_from_csv(&path) {
                            Ok(schema) => {
                                let output_file =
                                    output.join(format!("{}.schema.yaml", schema.name));
                                match inferencer.write_schema(&schema, &output_file) {
                                    Ok(_) => {
                                        println!(
                                            "  ✅ Generated {} ({} fields)",
                                            output_file.display(),
                                            schema.fields.len()
                                        );
                                        schema_count += 1;
                                    }
                                    Err(e) => {
                                        eprintln!(
                                            "  ❌ Failed to write {}: {}",
                                            output_file.display(),
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("  ⚠️  Could not infer from {}: {}", path.display(), e);
                            }
                        }
                    }
                }
            }
        }
    }

    println!();
    if schema_count > 0 {
        println!(
            "✅ Generated {} schema file(s) in {}",
            schema_count,
            output.display()
        );
    } else {
        println!("⚠️  No schemas were generated");
    }

    Ok(())
}

/// Scaffold a test runner script for a directory of tests
///
/// This function generates a shell script that can run velo-test commands
/// for a directory structure containing SQL files and test specifications.
///
/// # Arguments
/// * `directory` - Directory containing SQL files and tests
/// * `output` - Optional output path for the generated script
/// * `name` - Optional project name
/// * `style` - Optional style (simple, tiered, minimal)
/// * `velo_test_path` - Path to velo-test binary
/// * `yes` - Skip interactive prompts
#[allow(clippy::too_many_arguments)]
pub fn scaffold(
    directory: PathBuf,
    output: Option<PathBuf>,
    name: Option<String>,
    style: Option<String>,
    velo_test_path: String,
    yes: bool,
) -> CommandResult {
    use velostream::velostream::test_harness::scaffold::{ScaffoldStyle, Scaffolder};

    println!("🏗️  Scaffold Runner Script");
    println!("════════════════════════════════════════");
    println!("Directory: {}", directory.display());

    // Detect structure first to inform defaults
    let temp_scaffolder = Scaffolder::new().directory(&directory);
    let detected_structure = match temp_scaffolder.detect_structure() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ Failed to analyze directory: {}", e);
            std::process::exit(1);
        }
    };

    println!();
    println!("🔍 Detected structure:");
    println!("   Style: {}", detected_structure.style);
    println!("   SQL files: {}", detected_structure.sql_files.len());
    if !detected_structure.tier_dirs.is_empty() {
        println!("   Tiers: {}", detected_structure.tier_dirs.join(", "));
    }
    if !detected_structure.spec_files.is_empty() {
        println!("   Spec files: {}", detected_structure.spec_files.len());
    }
    println!();

    // Determine output path
    let output = output.unwrap_or_else(|| {
        let default = directory.join("velo-test.sh");
        if yes {
            default
        } else {
            let input =
                crate::discovery::prompt_with_default("Output path", &default.to_string_lossy());
            PathBuf::from(input)
        }
    });

    // Check if output exists and confirm overwrite
    if output.exists()
        && !yes
        && !crate::discovery::prompt_yes_no(
            &format!("{} already exists. Overwrite?", output.display()),
            false,
        )
    {
        println!("❌ Aborted");
        std::process::exit(0);
    }

    // Determine project name
    let name = name.or_else(|| {
        let detected_name = directory
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "project".to_string());

        if yes {
            Some(detected_name)
        } else {
            let input = crate::discovery::prompt_with_default("Project name", &detected_name);
            Some(input)
        }
    });

    // Determine style
    let style = style.or_else(|| {
        if yes {
            None // Use auto-detected
        } else {
            println!("Available styles: simple, tiered, minimal");
            println!("  simple  - Single SQL file, basic test runner");
            println!("  tiered  - Multiple tiers (tier1_basic, tier2_aggregations, etc.)");
            println!("  minimal - Bare minimum, just run tests");
            let detected = detected_structure.style.to_string();
            let input = crate::discovery::prompt_with_default("Style", &detected);
            if input == detected {
                None // Use auto-detected
            } else {
                Some(input)
            }
        }
    });

    println!();
    println!("📋 Configuration:");
    println!("   Output: {}", output.display());
    if let Some(ref n) = name {
        println!("   Project: {}", n);
    }
    if let Some(ref s) = style {
        println!("   Style: {}", s);
    } else {
        println!("   Style: {} (auto-detected)", detected_structure.style);
    }
    println!();

    // Build scaffolder
    let mut scaffolder = Scaffolder::new()
        .directory(&directory)
        .output(&output)
        .velo_test_path(velo_test_path);

    if let Some(n) = name {
        scaffolder = scaffolder.name(n);
    }

    if let Some(s) = style {
        match s.parse::<ScaffoldStyle>() {
            Ok(parsed_style) => {
                scaffolder = scaffolder.style(parsed_style);
            }
            Err(e) => {
                eprintln!("❌ {}", e);
                std::process::exit(1);
            }
        }
    }

    // Generate and write script
    println!("📝 Generating runner script...");
    match scaffolder.write() {
        Ok(path) => {
            println!();
            println!("✅ Generated: {}", path.display());
            println!();
            println!("💡 Next steps:");
            println!("   1. Make executable: chmod +x {}", path.display());
            println!(
                "   2. Run tests:       ./{} run",
                path.file_name().unwrap_or_default().to_string_lossy()
            );
            println!(
                "   3. See options:     ./{} --help",
                path.file_name().unwrap_or_default().to_string_lossy()
            );
        }
        Err(e) => {
            eprintln!("❌ Failed to generate script: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Quickstart wizard to set up tests for a SQL file
///
/// This function provides an interactive wizard that:
/// 1. Validates SQL syntax
/// 2. Checks for existing test artifacts
/// 3. Generates or uses existing test spec
/// 4. Offers schema inference from data
/// 5. Provides next steps and optionally runs tests
///
/// # Arguments
/// * `sql_file` - Path to the SQL file to set up tests for
pub async fn quickstart(sql_file: PathBuf) -> CommandResult {
    use velostream::velostream::sql::parser::StreamingSqlParser;
    use velostream::velostream::test_harness::SpecGenerator;

    println!("🚀 Velostream Test Harness - Quickstart Wizard");
    println!("════════════════════════════════════════════════");
    println!();
    println!("This wizard will guide you through setting up tests for your SQL file.");
    println!("SQL File: {}", sql_file.display());
    println!();

    // Step 1: Validate SQL
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Step 1/5: Validating SQL syntax");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let sql_content = match std::fs::read_to_string(&sql_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("❌ Failed to read SQL file: {}", e);
            std::process::exit(1);
        }
    };

    let parser = StreamingSqlParser::new();
    let statements: Vec<_> = sql_content
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.starts_with("--"))
        .collect();

    let mut valid_count = 0;
    let mut errors = Vec::new();

    for stmt in &statements {
        match parser.parse(stmt) {
            Ok(_) => valid_count += 1,
            Err(e) => errors.push(format!("  • {}", e)),
        }
    }

    if errors.is_empty() {
        println!("✅ All {} statements are valid", valid_count);
    } else {
        println!("⚠️  {} valid, {} with errors:", valid_count, errors.len());
        for err in &errors {
            println!("{}", err);
        }
        if !crate::discovery::prompt_yes_no("Continue anyway?", false) {
            println!("\n💡 Fix the SQL errors and run quickstart again.");
            std::process::exit(1);
        }
    }
    println!();

    // Step 2: Check for existing files
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Step 2/5: Checking existing test artifacts");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let existing_spec = crate::discovery::discover_spec(&sql_file);
    let existing_schemas = crate::discovery::discover_schemas(&sql_file);
    let existing_data = crate::discovery::discover_data_dir(&sql_file);

    if let Some(ref spec) = existing_spec {
        println!("✓ Found test spec: {}", spec.display());
    } else {
        println!("○ No test spec found (will generate)");
    }

    if let Some(ref schemas) = existing_schemas {
        println!("✓ Found schemas: {}", schemas.display());
    } else {
        println!("○ No schemas found (optional)");
    }

    if let Some(ref data) = existing_data {
        println!("✓ Found data dir: {}", data.display());
    } else {
        println!("○ No data dir found (optional)");
    }
    println!();

    // Step 3: Generate or use existing spec
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Step 3/5: Test specification");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let spec_path = if let Some(existing) = existing_spec {
        println!("Using existing spec: {}", existing.display());
        existing
    } else {
        let generator = SpecGenerator::new();
        match generator.generate_from_sql(&sql_file) {
            Ok(spec) => {
                println!("📝 Generated test spec with {} queries", spec.queries.len());

                if spec.queries.is_empty() {
                    println!();
                    println!("⚠️  No queries detected in SQL file.");
                    println!("   This can happen if:");
                    println!("   • SQL uses CREATE STREAM/TABLE without AS SELECT");
                    println!("   • SQL only contains DDL statements");
                    println!("   • Parser couldn't recognize the query pattern");
                    println!();
                    println!("💡 You can manually create a test spec using:");
                    println!(
                        "   velo-test init {} --output test_spec.yaml",
                        sql_file.display()
                    );
                }

                let default_path = sql_file
                    .parent()
                    .map(|p| {
                        p.join(format!(
                            "{}.test.yaml",
                            sql_file.file_stem().unwrap_or_default().to_string_lossy()
                        ))
                    })
                    .unwrap_or_else(|| PathBuf::from("test_spec.yaml"));

                if crate::discovery::prompt_yes_no(
                    &format!("Save test spec to {}?", default_path.display()),
                    true,
                ) {
                    let yaml = serde_yaml::to_string(&spec).unwrap_or_default();
                    let content = format!(
                        "# Auto-generated test spec for {}\n# Generated by velo-test quickstart\n\n{}",
                        sql_file.file_name().unwrap_or_default().to_string_lossy(),
                        yaml
                    );
                    if let Err(e) = std::fs::write(&default_path, content) {
                        eprintln!("⚠️  Failed to save spec: {}", e);
                    } else {
                        println!("✅ Saved: {}", default_path.display());
                    }
                }
                default_path
            }
            Err(e) => {
                eprintln!("❌ Failed to generate spec: {}", e);
                std::process::exit(1);
            }
        }
    };
    println!();

    // Step 4: Schema inference
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Step 4/5: Schema generation (optional)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    if existing_schemas.is_some() {
        println!("✓ Using existing schemas");
    } else if existing_data.is_some() {
        if crate::discovery::prompt_yes_no("Infer schemas from data files?", true) {
            println!("💡 Run: velo-test infer-schema {}", sql_file.display());
            println!("   This will generate .schema.yaml files from your data/");
        } else {
            println!("○ Skipping schema inference");
        }
    } else {
        println!("○ No data directory found - skipping schema inference");
        println!("💡 To enable data-driven testing:");
        println!("   1. Create a data/ directory next to your SQL file");
        println!("   2. Add CSV or JSON files matching your source names");
        println!("   3. Run: velo-test infer-schema {}", sql_file.display());
    }
    println!();

    // Step 5: Ready to run
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📋 Step 5/5: Ready to test!");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("🎉 Setup complete! Here's what you can do next:");
    println!();
    println!("  Run tests:");
    println!(
        "    velo-test run {} --spec {}",
        sql_file.display(),
        spec_path.display()
    );
    println!();
    println!("  Debug interactively:");
    println!("    velo-test debug {}", sql_file.display());
    println!();
    println!("  Stress test:");
    println!(
        "    velo-test stress {} --records 100000",
        sql_file.display()
    );
    println!();
    println!("  Add observability:");
    println!(
        "    velo-test annotate {} --monitoring ./monitoring",
        sql_file.display()
    );
    println!();

    if crate::discovery::prompt_yes_no("Run tests now?", true) {
        println!();
        println!("🧪 Starting test run...");
        println!();

        // Execute the run command
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "run",
                sql_file.to_str().unwrap(),
                "--spec",
                spec_path.to_str().unwrap(),
                "-y",
            ])
            .status();

        match status {
            Ok(s) if s.success() => {
                println!();
                println!("✅ Tests completed successfully!");
            }
            Ok(_) => {
                println!();
                println!("❌ Some tests failed. Use 'velo-test debug' to investigate.");
            }
            Err(e) => {
                eprintln!("❌ Failed to run tests: {}", e);
            }
        }
    } else {
        println!();
        println!(
            "👍 Run 'velo-test run {}' when you're ready!",
            sql_file.display()
        );
    }

    Ok(())
}

/// Run all SQL files in a directory
///
/// This function discovers and executes all SQL files matching a pattern in the specified directory.
/// It reuses the infrastructure across all tests for efficiency, and generates a comprehensive
/// multi-app report summarizing results.
///
/// # Arguments
/// * `directory` - Directory containing SQL files to test
/// * `output` - Output format (text, json, junit)
/// * `timeout_ms` - Timeout per query in milliseconds
/// * `use_testcontainers` - Auto-start Kafka using testcontainers
/// * `kafka` - External Kafka bootstrap servers
/// * `keep_containers` - Keep containers running after completion
/// * `reuse_containers` - Reuse existing containers
/// * `pattern` - Optional glob pattern for SQL files (default: "apps/*.sql")
/// * `skip` - Optional pattern to skip certain files
#[allow(clippy::too_many_arguments)]
pub async fn run_all(
    directory: PathBuf,
    output: String,
    timeout_ms: u64,
    use_testcontainers: bool,
    kafka: Option<String>,
    keep_containers: bool,
    reuse_containers: bool,
    pattern: Option<String>,
    skip: Option<String>,
) -> CommandResult {
    use velostream::velostream::test_harness::infra::TestHarnessInfra;
    use velostream::velostream::test_harness::report::{
        AppResult, MultiAppReport, OutputFormat, write_multi_app_report,
    };

    let mut multi_report = MultiAppReport::new("Test Summary");

    // Determine SQL file pattern
    let sql_pattern = pattern.unwrap_or_else(|| "apps/*.sql".to_string());
    let full_pattern = directory.join(&sql_pattern);

    println!("🧪 Velostream Test Harness - Run All");
    println!("════════════════════════════════════════");
    println!("Directory: {}", directory.display());
    println!("Pattern: {}", sql_pattern);
    println!();

    // Find all matching SQL files using glob pattern matching
    let sql_files: Vec<PathBuf> = {
        let mut files = Vec::new();

        // Simple glob implementation for apps/*.sql pattern
        if let Some(parent) = full_pattern.parent()
            && parent.exists()
            && let Ok(entries) = std::fs::read_dir(parent)
        {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map(|e| e == "sql").unwrap_or(false) {
                    let path_str = path.to_string_lossy();
                    // Skip .annotated.sql files
                    if path_str.contains(".annotated.sql") {
                        continue;
                    }
                    // Apply skip pattern if provided
                    if let Some(ref skip_pattern) = skip
                        && path_str.contains(skip_pattern)
                    {
                        continue;
                    }
                    files.push(path);
                }
            }
        }
        files.sort();
        files
    };

    if sql_files.is_empty() {
        eprintln!(
            "❌ No SQL files found matching pattern: {}",
            full_pattern.display()
        );
        std::process::exit(1);
    }

    println!("Found {} SQL files to test", sql_files.len());
    println!();

    // Initialize infrastructure once for all apps
    let mut infra = if use_testcontainers || kafka.is_none() {
        println!("🔧 Initializing test infrastructure...");
        match TestHarnessInfra::with_testcontainers_reuse(reuse_containers).await {
            Ok(infra) => {
                let mode = if reuse_containers {
                    "reused"
                } else {
                    "testcontainers"
                };
                println!(
                    "   Kafka: {} ({})",
                    infra.bootstrap_servers().unwrap_or("unknown"),
                    mode
                );
                Some(infra)
            }
            Err(e) => {
                eprintln!("❌ Failed to start test infrastructure: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        kafka.as_ref().map(|bs| TestHarnessInfra::with_kafka(bs))
    };

    // Start infrastructure if we have it
    if let Some(ref mut inf) = infra
        && let Err(e) = inf.start().await
    {
        eprintln!("❌ Failed to start test infrastructure: {}", e);
        std::process::exit(1);
    }

    let bootstrap_servers = kafka
        .clone()
        .or_else(|| {
            infra
                .as_ref()
                .and_then(|i| i.bootstrap_servers().map(|s| s.to_string()))
        })
        .unwrap_or_else(|| "localhost:9092".to_string());

    // Set VELOSTREAM_KAFKA_BROKERS env var
    unsafe {
        std::env::set_var("VELOSTREAM_KAFKA_BROKERS", &bootstrap_servers);
    }

    println!();

    // Run each SQL file by spawning velo-test run for each
    // This is simpler and reuses the existing well-tested Run logic
    for sql_file in &sql_files {
        let app_name = sql_file
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("Running: {}", app_name);

        let app_start = std::time::Instant::now();

        // Auto-discover test spec
        let spec_path = crate::discovery::discover_spec(sql_file);

        // Run velo-test for this app using subprocess
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.arg("run")
            .arg(sql_file)
            .arg("--timeout-ms")
            .arg(timeout_ms.to_string())
            .arg("--output")
            .arg("json")
            .arg("--kafka")
            .arg(&bootstrap_servers)
            .arg("-y"); // Skip prompts

        if let Some(ref spec) = spec_path {
            cmd.arg("--spec").arg(spec);
        }

        // Add schemas directory if it exists
        let schemas_dir = directory.join("schemas");
        if schemas_dir.exists() {
            cmd.arg("--schemas").arg(&schemas_dir);
        }

        let output_result = cmd.output();

        let mut app_result = AppResult {
            name: app_name.clone(),
            passed: false,
            queries_total: 0,
            queries_passed: 0,
            assertions_total: 0,
            assertions_passed: 0,
            duration_secs: 0.0,
            error: None,
            report: None,
        };

        match output_result {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout);

                // Try to parse JSON report from stdout
                // The JSON output may be mixed with log lines, so find the JSON part
                if let Some(json_start) = stdout.find('{')
                    && let Some(json_str) = stdout.get(json_start..)
                    && let Ok(report) = serde_json::from_str::<
                        velostream::velostream::test_harness::report::TestReport,
                    >(json_str)
                {
                    app_result.passed = report.summary.failed == 0 && report.summary.errors == 0;
                    app_result.queries_total = report.summary.total;
                    app_result.queries_passed = report.summary.passed;
                    app_result.assertions_total = report.summary.total_assertions;
                    app_result.assertions_passed = report.summary.passed_assertions;
                    app_result.report = Some(report);
                }

                // Check exit code
                if !output.status.success() && !app_result.passed {
                    app_result.passed = false;
                    if app_result.error.is_none() {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        if !stderr.is_empty() {
                            app_result.error =
                                Some(stderr.lines().take(3).collect::<Vec<_>>().join("\n"));
                        }
                    }
                }

                // Print inline summary
                let status = if app_result.passed { "✅" } else { "❌" };
                println!(
                    "  {} {}/{} queries, {}/{} assertions",
                    status,
                    app_result.queries_passed,
                    app_result.queries_total,
                    app_result.assertions_passed,
                    app_result.assertions_total
                );
            }
            Err(e) => {
                app_result.error = Some(format!("Failed to run: {}", e));
                println!("  ❌ Error: {}", e);
            }
        }

        app_result.duration_secs = app_start.elapsed().as_secs_f64();
        multi_report.add_app(app_result);
        println!();
    }

    // Stop shared infrastructure
    if let Some(ref mut inf) = infra {
        if keep_containers {
            // Just stop without cleanup
            let _ = inf.stop().await;
        } else if reuse_containers {
            let _ = inf.stop_with_reuse(true).await;
        } else {
            let _ = inf.stop().await;
        }
    }

    // Finalize and output report
    multi_report.finalize();

    let format = output.parse::<OutputFormat>().unwrap_or(OutputFormat::Text);
    let mut stdout = std::io::stdout();
    if let Err(e) = write_multi_app_report(&multi_report, format, &mut stdout) {
        eprintln!("❌ Failed to write report: {}", e);
        std::process::exit(1);
    }

    // Exit with appropriate code
    if multi_report.summary.apps_failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
