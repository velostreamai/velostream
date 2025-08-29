//! Unit tests for MultiJobSqlServer functionality
//!
//! Note: Critical tests have been moved to test_multi_job_critical_unit_tests.rs
//! This file contains additional tests that are not part of the critical test suite.

use ferrisstreams::ferris::MultiJobSqlServer;
// use std::time::Duration; // Unused import

#[tokio::test]
async fn test_concurrent_operations() {
    println!("🧪 Testing concurrent job operations");

    let server = MultiJobSqlServer::new("localhost:9092".to_string(), "test".to_string(), 10);
    let concurrent_jobs = 5;
    let mut handles = Vec::new();

    for i in 0..concurrent_jobs {
        let server_clone = server.clone();
        let handle = tokio::spawn(async move {
            let job_name = format!("job_{}", i);
            let result = server_clone
                .deploy_job(
                    job_name.clone(),
                    "1.0".to_string(),
                    "SELECT * FROM test_topic".to_string(),
                    "test_topic".to_string(),
                )
                .await;
            (job_name, result)
        });
        handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;
    let successful_deploys = results
        .into_iter()
        .filter(|r| r.as_ref().map(|(_name, res)| res.is_ok()).unwrap_or(false))
        .count();

    assert!(
        successful_deploys > 0,
        "At least some concurrent deployments should succeed"
    );
    println!(
        "✅ Concurrent operations validated with {} successful deploys",
        successful_deploys
    );
}

#[tokio::test]
async fn test_resource_cleanup() {
    println!("🧪 Testing resource cleanup and memory management");

    let server = MultiJobSqlServer::new("localhost:9092".to_string(), "test".to_string(), 5);

    // Deploy and immediately stop multiple jobs to test cleanup
    for i in 0..3 {
        let job_name = format!("cleanup_test_{}", i);
        server
            .deploy_job(
                job_name.clone(),
                "1.0".to_string(),
                "SELECT * FROM test_topic".to_string(),
                "test_topic".to_string(),
            )
            .await
            .unwrap();

        // Immediately stop the job
        server.stop_job(&job_name).await.unwrap();
    }

    // Verify all jobs are cleaned up
    assert_eq!(server.list_jobs().await.len(), 0, "All jobs should be cleaned up");
    println!("✅ Resource cleanup validated");
}

#[tokio::test]
async fn test_job_metrics() {
    println!("🧪 Testing job metrics tracking");

    let server = MultiJobSqlServer::new("localhost:9092".to_string(), "test".to_string(), 5);
    let job_name = "metrics_test";

    // Deploy a job and check its metrics
    server
        .deploy_job(
            job_name.to_string(),
            "1.0".to_string(),
            "SELECT * FROM test_topic".to_string(),
            "test_topic".to_string(),
        )
        .await
        .unwrap();

    // Verify metrics are initialized to zero
    if let Some(status) = server.get_job_status(job_name).await {
        assert_eq!(status.metrics.records_processed, 0);
        assert_eq!(status.metrics.errors, 0);
        assert!(status.metrics.last_record_time.is_none());
    }

    println!("✅ Job metrics tracking validated");
}
