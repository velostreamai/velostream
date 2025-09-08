use ferrisstreams::ferris::sql::SqlValidator;
use std::path::Path;

fn main() {
    println!("=== SQL Validator Demo ===");
    println!("Testing: demo/datasource-demo/enhanced_sql_demo.sql");
    println!();

    let validator = SqlValidator::new();
    // Test the actual enhanced demo file 
    let file_path = Path::new("demo/datasource-demo/enhanced_sql_demo.sql");
    
    // First let's debug by reading the file content directly
    let content = std::fs::read_to_string(file_path).unwrap();
    println!("📄 File Content Length: {} bytes", content.len());
    println!("📄 First 500 chars: {}", content.chars().take(500).collect::<String>());
    println!();
    
    // Add some debug info about statement splitting
    println!("🔍 Debug: Checking if validator can split statements...");
    let result = validator.validate_sql_content(&content);
    println!("🔍 Debug: After validation - found {} queries", result.total_queries);
    
    println!("📁 File: {}", result.file_path);
    if let Some(app_name) = &result.application_name {
        println!("📋 Application: {}", app_name);
    }
    println!("📊 Total Queries: {}", result.total_queries);
    println!("✅ Valid Queries: {}", result.valid_queries);
    println!("❌ Invalid Queries: {}", result.total_queries - result.valid_queries);
    println!("🎯 Overall Valid: {}", result.is_valid);
    println!();

    if !result.global_errors.is_empty() {
        println!("🚫 GLOBAL ERRORS:");
        for error in &result.global_errors {
            println!("   • {}", error);
        }
        println!();
    }

    println!("📝 DETAILED QUERY ANALYSIS:");
    println!("{}", "=".repeat(80));
    
    for (i, query_result) in result.query_results.iter().enumerate() {
        let status = if query_result.is_valid { "✅" } else { "❌" };
        println!();
        println!("{} Query {} (Line {}): {}", 
                 status, 
                 i + 1, 
                 query_result.start_line,
                 if query_result.is_valid { "VALID" } else { "INVALID" });
        
        // Show first 100 chars of the query
        let preview = query_result.query_text
            .chars()
            .take(100)
            .collect::<String>()
            .replace('\n', " ")
            .trim()
            .to_string();
        println!("   📄 SQL: {}...", preview);
        
        // Show parsing errors
        if !query_result.parsing_errors.is_empty() {
            println!("   🔍 PARSING ERRORS:");
            for error in &query_result.parsing_errors {
                println!("      • {} (Line: {:?})", error.message, error.line);
            }
        }
        
        // Show configuration errors  
        if !query_result.configuration_errors.is_empty() {
            println!("   ⚙️  CONFIGURATION ERRORS:");
            for error in &query_result.configuration_errors {
                println!("      • {} (Line: {:?})", error.message, error.line);
            }
        }
        
        // Show warnings
        if !query_result.warnings.is_empty() {
            println!("   ⚠️  WARNINGS:");
            for warning in &query_result.warnings {
                println!("      • {} (Line: {:?})", warning.message, warning.line);
            }
        }
        
        // Show missing configurations
        if !query_result.missing_source_configs.is_empty() {
            println!("   🔗 MISSING SOURCE CONFIGS:");
            for config in &query_result.missing_source_configs {
                println!("      • Source '{}': missing {}", config.name, config.missing_keys.join(", "));
            }
        }
        
        if !query_result.missing_sink_configs.is_empty() {
            println!("   📤 MISSING SINK CONFIGS:");
            for config in &query_result.missing_sink_configs {
                println!("      • Sink '{}': missing {}", config.name, config.missing_keys.join(", "));
            }
        }
    }
    
    println!();
    println!("{}", "=".repeat(80));
    
    // Configuration summary
    if !result.configuration_summary.missing_configurations.is_empty() {
        println!("📋 CONFIGURATION SUMMARY:");
        for missing in &result.configuration_summary.missing_configurations {
            println!("   • {}", missing);
        }
        println!();
    }
    
    // Recommendations
    if !result.recommendations.is_empty() {
        println!("💡 RECOMMENDATIONS:");
        for rec in &result.recommendations {
            println!("   • {}", rec);
        }
        println!();
    }
    
    println!("🏁 VALIDATION COMPLETE");
    println!("Result: {}", if result.is_valid { 
        "✅ All queries are valid and ready for deployment!" 
    } else { 
        "❌ Validation failed - fix the above issues before deployment" 
    });
}