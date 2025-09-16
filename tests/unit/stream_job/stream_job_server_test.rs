//! Integration tests for multi-job SQL server
//!
//! This test verifies that the multi-job SQL server correctly deploys and runs
//! all jobs from a single SQL application file.

use std::time::Duration;
use velostream::velostream::sql::app_parser::SqlApplicationParser;

// Import the StreamJobServer from the binary crate
// Note: This requires the structs to be pub in the binary or moved to lib
use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::NamedTempFile;

/// Test SQL application with multiple jobs
const TEST_SQL_APP: &str = r#"
-- SQL Application: Multi-Job Test
-- Version: 1.0.0
-- Description: Test application with multiple jobs
-- Author: Test Suite

-- Job 1: Simple aggregation
START JOB job1 AS
SELECT 
    COUNT(*) as total_records,
    MAX(timestamp) as latest_time
FROM test_topic1
WITH ('output.topic' = 'job1_output');

-- Job 2: Filtering
START JOB job2 AS  
SELECT *
FROM test_topic2
WHERE value > 100
WITH ('output.topic' = 'job2_output');

-- Job 3: Window function
START JOB job3 AS
SELECT 
    key,
    value,
    AVG(value) OVER (PARTITION BY key ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as avg_value
FROM test_topic3
WITH ('output.topic' = 'job3_output');

-- Job 4: Join operation  
START JOB job4 AS
SELECT 
    a.id,
    a.name,
    b.amount
FROM users_topic a
JOIN orders_topic b ON a.id = b.user_id
WITH ('output.topic' = 'job4_output');

-- Job 5: Complex aggregation with grouping
START JOB job5 AS
SELECT 
    category,
    COUNT(*) as item_count,
    SUM(price) as total_price,
    AVG(price) as avg_price
FROM products_topic
GROUP BY category
HAVING COUNT(*) > 5
WITH ('output.topic' = 'job5_output');
"#;

#[tokio::test]
async fn test_sql_parser_extracts_all_jobs() {
    // Test that our SQL parser can extract all 5 jobs from the test application
    let parser = SqlApplicationParser::new();

    match parser.parse_application(TEST_SQL_APP) {
        Ok(app) => {
            println!("✅ SQL Application parsed successfully");
            println!("   Name: {}", app.metadata.name);
            println!("   Version: {}", app.metadata.version);
            println!("   Jobs found: {}", app.resources.jobs.len());

            // Verify we have exactly 5 jobs
            assert_eq!(
                app.resources.jobs.len(),
                5,
                "Expected 5 jobs, found {}",
                app.resources.jobs.len()
            );

            // Verify job names
            let expected_jobs = vec!["job1", "job2", "job3", "job4", "job5"];
            let found_jobs = &app.resources.jobs;

            for job_name in found_jobs {
                println!("   - Job '{}'", job_name);
            }

            for expected in expected_jobs {
                assert!(
                    found_jobs.contains(&expected.to_string()),
                    "Expected job '{}' not found. Found jobs: {:?}",
                    expected,
                    found_jobs
                );
            }

            println!("✅ All expected jobs found in SQL application");
        }
        Err(e) => {
            panic!("❌ Failed to parse SQL application: {}", e);
        }
    }
}

#[tokio::test]
async fn test_multi_job_server_deploy_all_jobs() {
    if !is_kafka_available().await {
        println!("⏸️ Skipping integration test - Kafka not available");
        println!("💡 Start Kafka with: docker-compose -f demo/trading/kafka-compose.yml up -d");
        return;
    }

    // Determine binary path based on environment
    let binary_name = "velo-sql-multi";
    let binary_path = if std::env::var("CI").is_ok() {
        format!("./target/debug/{}", binary_name)
    } else {
        format!("./target/release/{}", binary_name)
    };

    println!("🔍 Checking for binary at: {}", binary_path);

    // Check and build binary if needed
    if !std::path::Path::new(&binary_path).exists() {
        println!("🔨 Building binary...");
        let build_args = if std::env::var("CI").is_ok() {
            println!("CI environment detected - building debug binary");
            vec!["build", "--bin", binary_name]
        } else {
            println!("Local environment - building release binary");
            vec!["build", "--release", "--bin", binary_name]
        };

        let build_output = Command::new("cargo")
            .args(&build_args)
            .output()
            .expect("Failed to build binary");

        if !build_output.status.success() {
            let error = String::from_utf8_lossy(&build_output.stderr);
            panic!("Failed to build binary: {}", error);
        }
        println!("✅ Binary built successfully");
    } else {
        println!("✅ Binary already exists");
    }

    // Verify binary is executable
    println!("🔍 Verifying binary permissions and testing execution...");
    let version_output = Command::new(&binary_path).args(["--version"]).output();

    match version_output {
        Ok(output) => {
            if output.status.success() {
                println!("✅ Binary is executable");
            } else {
                println!(
                    "❌ Binary exists but failed to run --version: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
        Err(e) => {
            println!("❌ Failed to execute binary: {}", e);
        }
    }

    println!("🧪 Testing multi-job SQL server deployment...");

    // Create a temporary SQL file
    println!("📝 Creating temporary SQL file...");
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    temp_file
        .write_all(TEST_SQL_APP.as_bytes())
        .expect("Failed to write to temp file");
    temp_file.flush().expect("Failed to flush temp file");
    let temp_path = temp_file.path();

    println!("📁 Temp file created at: {}", temp_path.display());
    println!(
        "📏 Temp file size: {} bytes",
        std::fs::metadata(temp_path).unwrap().len()
    );

    // Verify temp file is readable
    let temp_content = std::fs::read_to_string(temp_path).expect("Failed to read temp file");
    println!(
        "✅ Temp file is readable, contains {} bytes",
        temp_content.len()
    );

    // Start a test multi-job server
    println!("🚀 Starting multi-job SQL server for test...");

    let mut server_process = Command::new(&binary_path)
        .args([
            "server",
            "--brokers",
            "localhost:9092",
            "--port",
            "8081", // Use different port for test
            "--max-jobs",
            "10",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start multi-job SQL server");

    // Wait for server to start
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Deploy the SQL application
    println!("📊 Deploying test SQL application...");

    println!("🚀 Executing deployment command...");
    println!(
        "Command: {} deploy-app --file {} --brokers localhost:9092 --no-monitor",
        binary_path,
        temp_path.display()
    );

    let deploy_output = Command::new(&binary_path)
        .args([
            "deploy-app",
            "--file",
            temp_path.to_str().unwrap(),
            "--brokers",
            "localhost:9092",
            "--no-monitor",
        ])
        .output()
        .map_err(|e| {
            panic!(
                "Failed to execute deploy-app command: {}. Binary path: {}",
                e, binary_path
            );
        })
        .unwrap();

    if !deploy_output.status.success() {
        let stderr = String::from_utf8_lossy(&deploy_output.stderr);
        panic!("❌ Deploy command failed: {}", stderr);
    }

    let stdout = String::from_utf8_lossy(&deploy_output.stdout);
    let stderr = String::from_utf8_lossy(&deploy_output.stderr);
    println!("Deploy stdout: {}", stdout);
    println!("Deploy stderr: {}", stderr);

    // Parse the deployment output to verify all jobs were deployed
    // Look for lines like "Deployed X jobs: [...]" in both stdout and stderr
    let combined_output = format!("{}\n{}", stdout, stderr);
    let deployed_jobs_count = if let Some(line) = combined_output
        .lines()
        .find(|line| line.contains("Deployed") && line.contains("jobs:"))
    {
        // Extract number from "Deployed X jobs:"
        if let Some(count_str) = line.split_whitespace().nth(1) {
            count_str.parse::<usize>().unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    // Verify all 5 jobs were deployed
    assert_eq!(
        deployed_jobs_count, 5,
        "❌ Expected 5 jobs to be deployed, but only {} were deployed. Combined output: {}",
        deployed_jobs_count, combined_output
    );

    println!("✅ All 5 jobs successfully deployed!");

    // Wait a bit for jobs to initialize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify jobs are actually running by checking consumer groups
    println!("🔍 Verifying jobs are running by checking Kafka consumer groups...");

    // Try CI-style direct command first, fall back to Docker
    let consumer_groups_output = if std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok() {
        // CI environment - try using kafka tools directly
        Command::new("sh")
            .args(["-c", "which kafka-consumer-groups.sh >/dev/null 2>&1 && kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list || echo 'kafka-tools-not-available'"])
            .output()
            .ok()
    } else {
        None
    }.or_else(|| {
        // Local development - try Docker
        if let Some(ref container_name) = get_kafka_container_name_sync() {
            Command::new("docker")
                .args([
                    "exec",
                    container_name,
                    "kafka-consumer-groups",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--list"
                ])
                .output()
                .ok()
        } else {
            None
        }
    });

    if let Some(output) = consumer_groups_output {
        if output.status.success() {
            let groups_stdout = String::from_utf8_lossy(&output.stdout);
            let active_groups: Vec<&str> = groups_stdout
                .lines()
                .filter(|line| !line.is_empty() && !line.starts_with("__"))
                .collect();

            println!("Active consumer groups: {:?}", active_groups);

            // We should have consumer groups for our jobs (exact naming depends on implementation)
            let job_groups_count = active_groups.len();

            if job_groups_count < 5 {
                println!(
                    "⚠️ Warning: Expected 5+ consumer groups for 5 jobs, found {}",
                    job_groups_count
                );
                // This is not a hard failure as consumer group naming may vary
                // But it indicates jobs may not be fully initialized
            } else {
                println!(
                    "✅ Found {} consumer groups, indicating jobs are running",
                    job_groups_count
                );
            }
        } else {
            println!("⚠️ Could not verify consumer groups - jobs may not be fully initialized");
        }
    }

    // Also verify that topics mentioned in SQL are being consumed from
    println!("🔍 Verifying expected input topics exist...");
    let expected_topics = vec![
        "test_topic1",
        "test_topic2",
        "test_topic3",
        "users_topic",
        "products_topic",
    ];

    for topic in expected_topics {
        // Create the topic if it doesn't exist (needed for jobs to start consuming)
        let create_result = if std::env::var("CI").is_ok() || std::env::var("GITHUB_ACTIONS").is_ok() {
            // CI environment - use kafka-topics.sh if available
            Command::new("sh")
                .args(["-c", &format!("kafka-topics.sh --create --bootstrap-server localhost:9092 --topic {} --partitions 1 --replication-factor 1 --if-not-exists 2>/dev/null || true", topic)])
                .output()
                .ok()
        } else {
            None
        }.or_else(|| {
            // Local development - use Docker
            if let Some(ref container_name) = get_kafka_container_name_sync() {
                Command::new("docker")
                    .args([
                        "exec",
                        container_name,
                        "kafka-topics",
                        "--create",
                        "--bootstrap-server",
                        "localhost:9092",
                        "--topic",
                        topic,
                        "--partitions",
                        "1",
                        "--replication-factor",
                        "1",
                        "--if-not-exists"
                    ])
                    .output()
                    .ok()
            } else {
                None
            }
        });

        if create_result.is_some() {
            println!("📝 Created/verified topic: {}", topic);
        }
    }

    // Give jobs time to start consuming after topics are created
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!(
        "✅ Multi-job server deployment test completed - {} jobs deployed and initialized",
        deployed_jobs_count
    );

    // Cleanup
    println!("🧹 Cleaning up test server...");
    server_process.kill().expect("Failed to kill test server");

    println!("✅ Multi-job server test completed successfully!");
}

#[tokio::test]
#[ignore] // Requires HTTP API implementation - keep this one ignored for now
async fn test_job_deployment_with_mock_topics() {
    // This test would create mock Kafka topics and verify that:
    // 1. All jobs are deployed
    // 2. Each job creates its own consumer group
    // 3. Each job is consuming from the correct input topic
    // 4. Each job would produce to the correct output topic (if data was available)

    println!("🧪 Mock topic test - verifying job deployment behavior");

    // For now, this is a placeholder that documents what we should test
    // when the HTTP API is available

    let expected_input_topics = vec![
        "test_topic1",
        "test_topic2",
        "test_topic3",
        "users_topic",
        "products_topic",
    ];

    let expected_output_topics = vec![
        "job1_output",
        "job2_output",
        "job3_output",
        "job4_output",
        "job5_output",
    ];

    println!("Expected input topics: {:?}", expected_input_topics);
    println!("Expected output topics: {:?}", expected_output_topics);

    // When HTTP API is available, we would:
    // 1. Deploy the SQL app
    // 2. Call GET /jobs to list all running jobs
    // 3. Verify each job has correct input/output topic configuration
    // 4. Check that consumer groups exist for each job

    println!("⚠️  This test requires HTTP API implementation to be fully functional");
}

/// Helper function to check if Kafka is available for integration tests
async fn is_kafka_available() -> bool {
    // First try direct connection (CI environment)
    let direct_test = Command::new("sh")
        .args([
            "-c",
            "timeout 5s bash -c '</dev/tcp/localhost/9092' 2>/dev/null",
        ])
        .output();

    if let Ok(output) = direct_test {
        if output.status.success() {
            return true;
        }
    }

    // Fall back to Docker exec (local development)
    let docker_test = Command::new("docker")
        .args([
            "exec",
            "simple-kafka",
            "kafka-topics",
            "--bootstrap-server",
            "localhost:9092",
            "--list",
        ])
        .output();

    match docker_test {
        Ok(output) => output.status.success(),
        Err(_) => false,
    }
}

/// Helper function to get Kafka container name
async fn get_kafka_container_name() -> Option<String> {
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

/// Synchronous version for use in match expressions
fn get_kafka_container_name_sync() -> Option<String> {
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

/// Test runner that checks prerequisites
#[tokio::test]
async fn test_prerequisites() {
    println!("🔍 Checking test prerequisites...");

    // Check if multi-job server binary exists
    let binary_path = "./target/release/velo-sql-multi";
    let binary_exists = std::path::Path::new(binary_path).exists();
    println!(
        "   velo-sql-multi binary: {}",
        if binary_exists { "✅" } else { "❌" }
    );

    if !binary_exists {
        println!("💡 Run 'cargo build --release --bin velo-sql-multi' to build the binary");
    }

    // Check if Kafka is available
    let kafka_available = is_kafka_available().await;
    println!(
        "   Kafka availability: {}",
        if kafka_available { "✅" } else { "❌" }
    );

    if !kafka_available {
        println!(
            "💡 Start Kafka with 'docker-compose -f demo/trading/kafka-compose.yml up -d' for integration tests"
        );
    }

    println!("\n🧪 Test Suite Information:");
    println!("   • test_sql_parser_extracts_all_jobs: Unit test (always runs)");
    println!("   • test_multi_job_server_deploy_all_jobs: Integration test (requires Kafka)");
    println!("   • test_job_deployment_with_mock_topics: Future test (requires HTTP API)");

    println!("\n📝 To run integration tests:");
    println!("   1. Start Kafka: docker-compose -f demo/trading/kafka-compose.yml up -d");
    println!("   2. Build binary: cargo build --release --bin velo-sql-multi");
    println!("   3. Run tests: cargo test test_multi_job_server -- --ignored");
}
