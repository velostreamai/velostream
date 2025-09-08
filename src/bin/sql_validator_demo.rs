//! SQL Validator Demo - Shows How Validation Works
//!
//! This demo program shows the internal workings of SQL validation step by step.

use ferrisstreams::ferris::sql::{query_analyzer::QueryAnalyzer, StreamingSqlParser};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 SQL Validator Internal Process Demo");
    println!("=====================================\n");

    // Create the core components
    let parser = StreamingSqlParser::new();
    let analyzer = QueryAnalyzer::new("demo-validator".to_string());

    // Test different SQL queries to show how validation works
    let test_queries = vec![
        ("Simple SELECT", "SELECT id, name FROM users"),
        ("SELECT with semicolon", "SELECT id, name FROM users;"),
        (
            "CREATE STREAM",
            "CREATE STREAM test AS SELECT id FROM source",
        ),
        (
            "Complex WITH clause",
            "CREATE STREAM test AS SELECT id FROM source WITH ('type' = 'kafka')",
        ),
    ];

    for (name, query) in test_queries {
        println!("📝 Testing: {}", name);
        println!("Query: {}", query);
        println!("Results:");

        // Step 1: SQL Parsing
        print!("  1️⃣ Parsing... ");
        match parser.parse(query) {
            Ok(parsed) => {
                println!("✅ SUCCESS");
                println!(
                    "     Parsed query type: {:?}",
                    std::mem::discriminant(&parsed)
                );

                // Step 2: Query Analysis
                print!("  2️⃣ Analyzing... ");
                match analyzer.analyze(&parsed) {
                    Ok(analysis) => {
                        println!("✅ SUCCESS");
                        println!("     Sources found: {}", analysis.required_sources.len());
                        println!("     Sinks found: {}", analysis.required_sinks.len());
                        println!("     Configurations: {}", analysis.configuration.len());

                        // Show details
                        for source in &analysis.required_sources {
                            println!(
                                "     📥 Source: {} (type: {:?})",
                                source.name, source.source_type
                            );
                            for (key, value) in &source.properties {
                                println!("        - {}: {}", key, value);
                            }
                        }

                        for sink in &analysis.required_sinks {
                            println!("     📤 Sink: {} (type: {:?})", sink.name, sink.sink_type);
                            for (key, value) in &sink.properties {
                                println!("        - {}: {}", key, value);
                            }
                        }
                    }
                    Err(e) => {
                        println!("❌ FAILED");
                        println!("     Analysis error: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("❌ FAILED");
                println!("     Parse error: {}", e);

                // Show where the error occurred
                if let Some(pos) = extract_position(&e.to_string()) {
                    println!("     Error position: {}", pos);
                    show_error_context(query, pos);
                }
            }
        }

        println!(""); // Blank line between tests
    }

    println!("\n🎯 Validation Process Explained:");
    println!("================================");
    println!("1️⃣ **SQL Parsing**: Uses StreamingSqlParser to convert SQL text into AST");
    println!("   - Success: Query is syntactically valid");
    println!("   - Failure: SQL syntax error with position");
    println!("");
    println!("2️⃣ **Query Analysis**: Uses QueryAnalyzer to extract resources needed");
    println!("   - Identifies data sources (FROM clauses)");
    println!("   - Identifies data sinks (INTO clauses)");
    println!("   - Extracts configurations (WITH clauses)");
    println!("");
    println!("3️⃣ **Configuration Validation**: Checks required configs for each resource");
    println!("   - Kafka: bootstrap.servers, topic, group.id");
    println!("   - File: path, format");
    println!("   - Validates file existence and directory access");
    println!("");
    println!("4️⃣ **Performance Analysis**: Identifies potential performance issues");
    println!("   - JOINs without time windows");
    println!("   - ORDER BY without LIMIT");
    println!("   - Complex subqueries in streaming context");
    println!("");
    println!("5️⃣ **Syntax Compatibility**: Flags unsupported constructs");
    println!("   - WINDOW clauses that may not be supported");
    println!("   - Complex SQL features not implemented");

    Ok(())
}

fn extract_position(error_msg: &str) -> Option<usize> {
    // Extract position number from error message like "at position 39"
    if let Some(pos_start) = error_msg.find("position ") {
        let pos_str = &error_msg[pos_start + 9..];
        if let Some(pos_end) = pos_str.find(":") {
            pos_str[..pos_end].parse().ok()
        } else {
            pos_str.split_whitespace().next()?.parse().ok()
        }
    } else {
        None
    }
}

fn show_error_context(query: &str, position: usize) {
    println!("     Context:");

    let chars: Vec<char> = query.chars().collect();
    let start = if position >= 10 { position - 10 } else { 0 };
    let end = if position + 10 < chars.len() {
        position + 10
    } else {
        chars.len()
    };

    let before: String = chars[start..position].iter().collect();
    let at_pos = chars.get(position).unwrap_or(&' ');
    let after: String = chars[position + 1..end].iter().collect();

    println!("        {}<[{}]>{}", before, at_pos, after);
    println!("        {}^", " ".repeat(before.len() + 1));
}
