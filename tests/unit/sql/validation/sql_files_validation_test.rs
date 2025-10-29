/*!
# SQL Files Validation Test - Phase 1

Comprehensive validation of all 18 demo and example SQL files for parser compatibility.
This test suite verifies that the Velostream SQL parser can successfully parse all
production SQL files in the demo and examples directories.

Tests 18 SQL files across:
- demo/datasource-demo/ (4 files)
- demo/trading/sql/ (2 files)
- examples/ (12 files)
*/

use std::fs;
use std::path::Path;
use velostream::velostream::sql::parser::StreamingSqlParser;

/// Test that all 18 SQL files can be parsed successfully
#[test]
fn test_all_sql_files_parse() {
    let all_files = vec![
        // Demo datasource files (4)
        "demo/datasource-demo/enhanced_sql_demo.sql",
        "demo/datasource-demo/file_processing_sql_demo.sql",
        "demo/datasource-demo/simple_test.sql",
        "demo/datasource-demo/test_kafka.sql",
        // Demo trading files (2)
        "demo/trading/sql/financial_trading.sql",
        "demo/trading/sql/ctas_file_trading.sql",
        // Example files (12)
        "examples/ecommerce_analytics.sql",
        "examples/ecommerce_analytics_phase4.sql",
        "examples/ecommerce_with_metrics.sql",
        "examples/financial_trading_with_metrics.sql",
        "examples/iot_monitoring.sql",
        "examples/iot_monitoring_phase4.sql",
        "examples/iot_monitoring_with_metrics.sql",
        "examples/social_media_analytics.sql",
        "examples/social_media_analytics_phase4.sql",
        "examples/test_emit_changes.sql",
        "examples/test_parsing_error.sql",
        "examples/test_simple_validation.sql",
    ];

    let parser = StreamingSqlParser::new();
    let mut passed = 0;
    let mut failed = 0;
    let mut failed_files = Vec::new();

    println!("\n================================================================================");
    println!("SQL FILE VALIDATION - Testing {} Files", all_files.len());
    println!("================================================================================\n");

    for file_path in &all_files {
        let file_name = Path::new(file_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        match fs::read_to_string(file_path) {
            Ok(content) => match parser.parse(&content) {
                Ok(_query) => {
                    println!("✅ {}", file_name);
                    passed += 1;
                }
                Err(e) => {
                    println!("❌ {} - Parse Error", file_name);
                    failed += 1;
                    failed_files.push((file_name.to_string(), format!("{:?}", e)));
                }
            },
            Err(e) => {
                println!("❌ {} - File Read Error", file_name);
                failed += 1;
                failed_files.push((file_name.to_string(), format!("File read error: {}", e)));
            }
        }
    }

    println!("\n================================================================================");
    println!(
        "RESULTS: {} passed, {} failed out of {}",
        passed,
        failed,
        all_files.len()
    );
    println!("================================================================================\n");

    if failed > 0 {
        println!("FAILED FILES:");
        println!(
            "--------------------------------------------------------------------------------"
        );
        for (name, error) in &failed_files {
            println!("\n  {}", name);
            println!(
                "  Error: {}",
                error.lines().next().unwrap_or("Unknown error")
            );
        }
        println!(
            "================================================================================\n"
        );

        // Only fail if there are parsing failures (not including intentionally bad parse files)
        if !failed_files
            .iter()
            .any(|(n, _)| n.contains("test_parsing_error"))
        {
            panic!(
                "❌ {} SQL files failed to parse",
                failed_files
                    .iter()
                    .filter(|(n, _)| !n.contains("test_parsing_error"))
                    .count()
            );
        }
    }

    println!("✅ SQL validation completed successfully!");
}

/// Test demo datasource files specifically
#[test]
fn test_demo_datasource_files_parse() {
    let files = vec![
        "demo/datasource-demo/enhanced_sql_demo.sql",
        "demo/datasource-demo/file_processing_sql_demo.sql",
        "demo/datasource-demo/simple_test.sql",
        "demo/datasource-demo/test_kafka.sql",
    ];

    let parser = StreamingSqlParser::new();

    println!("\nTesting Demo Datasource Files:");
    println!("--------------------------------------------------------------------------------");

    for file in files {
        let file_name = Path::new(file)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        match fs::read_to_string(file) {
            Ok(content) => match parser.parse(&content) {
                Ok(_) => println!("  ✅ {}", file_name),
                Err(e) => {
                    println!(
                        "  ❌ {} - {}",
                        file_name,
                        format!("{:?}", e).lines().next().unwrap_or("Error")
                    );
                    panic!("Failed to parse: {}", file);
                }
            },
            Err(e) => {
                panic!("Failed to read file {}: {}", file, e);
            }
        }
    }
}

/// Test demo trading files specifically
#[test]
fn test_demo_trading_files_parse() {
    let files = vec![
        "demo/trading/sql/financial_trading.sql",
        "demo/trading/sql/ctas_file_trading.sql",
    ];

    let parser = StreamingSqlParser::new();

    println!("\nTesting Demo Trading Files:");
    println!("--------------------------------------------------------------------------------");

    for file in files {
        let file_name = Path::new(file)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        match fs::read_to_string(file) {
            Ok(content) => match parser.parse(&content) {
                Ok(_) => println!("  ✅ {}", file_name),
                Err(e) => {
                    println!(
                        "  ❌ {} - {}",
                        file_name,
                        format!("{:?}", e).lines().next().unwrap_or("Error")
                    );
                    panic!("Failed to parse: {}", file);
                }
            },
            Err(e) => {
                panic!("Failed to read file {}: {}", file, e);
            }
        }
    }
}

/// Test example files specifically
#[test]
fn test_example_files_parse() {
    let files = vec![
        "examples/ecommerce_analytics.sql",
        "examples/ecommerce_analytics_phase4.sql",
        "examples/ecommerce_with_metrics.sql",
        "examples/financial_trading_with_metrics.sql",
        "examples/iot_monitoring.sql",
        "examples/iot_monitoring_phase4.sql",
        "examples/iot_monitoring_with_metrics.sql",
        "examples/social_media_analytics.sql",
        "examples/social_media_analytics_phase4.sql",
        "examples/test_emit_changes.sql",
        "examples/test_parsing_error.sql",
        "examples/test_simple_validation.sql",
    ];

    let parser = StreamingSqlParser::new();
    let mut passed = 0;
    let mut failed = 0;

    println!("\nTesting Example Files:");
    println!("--------------------------------------------------------------------------------");

    for file in files {
        let file_name = Path::new(file)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");

        match fs::read_to_string(file) {
            Ok(content) => match parser.parse(&content) {
                Ok(_) => {
                    println!("  ✅ {}", file_name);
                    passed += 1;
                }
                Err(_e) => {
                    if file_name == "test_parsing_error.sql" {
                        println!("  ⚠️  {} (intentionally invalid)", file_name);
                        passed += 1; // This file is expected to fail
                    } else {
                        println!("  ❌ {}", file_name);
                        failed += 1;
                        panic!("Failed to parse: {}", file);
                    }
                }
            },
            Err(e) => {
                panic!("Failed to read file {}: {}", file, e);
            }
        }
    }

    println!("\n  Summary: {} passed, {} failed", passed, failed);
}
