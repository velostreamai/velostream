//! SQL Query and Application Validator
//!
//! Comprehensive validation tool for VeloStream SQL queries and applications.
//! This is the refactored, object-oriented version of the SQL validator.

use log::info;
use std::env;
use std::path::Path;
use velostream::velostream::sql::validation::SqlValidationService;

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        show_usage();
        return;
    }

    let command = &args[1];

    match command.as_str() {
        "query" => {
            if args.len() < 3 {
                eprintln!("Usage: {} query <SQL_QUERY>", args[0]);
                return;
            }
            validate_single_query(&args[2]);
        }
        "file" => {
            if args.len() < 3 {
                eprintln!("Usage: {} file <SQL_FILE>", args[0]);
                return;
            }
            validate_sql_file(&args[2]);
        }
        "dir" => {
            if args.len() < 3 {
                eprintln!("Usage: {} dir <DIRECTORY>", args[0]);
                return;
            }
            validate_directory(&args[2]);
        }
        "--help" | "-h" => show_usage(),
        _ => {
            // Default behavior: treat first argument as file path
            validate_sql_file(command);
        }
    }
}

fn validate_single_query(query: &str) {
    info!("Validating single SQL query");

    let validator = SqlValidationService::new();
    let result = validator.validate_query(query);

    println!("Query Validation Result:");
    println!(
        "Status: {}",
        if result.is_valid {
            "✅ Valid"
        } else {
            "❌ Invalid"
        }
    );
    println!("Issues: {}", result.issue_summary());

    if !result.parsing_errors.is_empty() {
        println!("\nParsing Errors:");
        for error in &result.parsing_errors {
            println!(
                "  • {} (line {}, column {})",
                error.message, error.line, error.column
            );
        }
    }

    if !result.warnings.is_empty() {
        println!("\nWarnings:");
        for warning in &result.warnings {
            println!("  • {}", warning);
        }
    }
}

fn validate_sql_file(file_path: &str) {
    info!("Validating SQL file: {}", file_path);

    let path = Path::new(file_path);
    if !path.exists() {
        eprintln!("Error: File not found: {}", file_path);
        return;
    }

    let validator = SqlValidationService::new();
    let result = validator.validate_application(path);

    // Print results using the error formatter
    let formatter =
        velostream::velostream::sql::validation::ValidationErrorFormatter::new().with_verbose(true);
    let output = formatter.format_application_result(&result);

    for line in output {
        println!("{}", line);
    }

    // Exit with appropriate code
    std::process::exit(if result.is_valid { 0 } else { 1 });
}

fn validate_directory(dir_path: &str) {
    info!("Validating SQL files in directory: {}", dir_path);

    let path = Path::new(dir_path);
    if !path.exists() {
        eprintln!("Error: Directory not found: {}", dir_path);
        return;
    }

    let validator = SqlValidationService::new();
    let results = validator.validate_directory(path);

    if results.is_empty() {
        println!("No SQL files found in directory: {}", dir_path);
        return;
    }

    println!("Directory Validation Results:\n");

    let formatter = velostream::velostream::sql::validation::ValidationErrorFormatter::new();
    let mut all_valid = true;

    for result in &results {
        let output = formatter.format_application_result(result);
        for line in output {
            println!("{}", line);
        }
        println!("{}", "=".repeat(80));

        if !result.is_valid {
            all_valid = false;
        }
    }

    // Summary
    let total_files = results.len();
    let valid_files = results.iter().filter(|r| r.is_valid).count();

    println!("\nDirectory Summary:");
    println!("Files processed: {}", total_files);
    println!("Valid files: {}", valid_files);
    println!("Invalid files: {}", total_files - valid_files);

    std::process::exit(if all_valid { 0 } else { 1 });
}

fn show_usage() {
    println!("VeloStream SQL Validator");
    println!();
    println!("USAGE:");
    println!("    sql_validator <COMMAND> [OPTIONS]");
    println!();
    println!("COMMANDS:");
    println!("    query <SQL>       Validate a single SQL query");
    println!("    file <FILE>       Validate a SQL file");
    println!("    dir <DIRECTORY>   Validate all SQL files in a directory");
    println!();
    println!("EXAMPLES:");
    println!("    sql_validator file my_queries.sql");
    println!("    sql_validator dir ./sql_files/");
    println!("    sql_validator query \"SELECT * FROM stream1\"");
    println!();
    println!("OPTIONS:");
    println!("    -h, --help        Show this help message");
}
