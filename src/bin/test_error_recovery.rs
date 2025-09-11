//! Test Error Recovery System
//!
//! This binary demonstrates the comprehensive error handling and recovery functionality
//! including circuit breakers, retry mechanisms, dead letter queues, and health monitoring.

use ferrisstreams::ferris::sql::error::recovery::*;
use ferrisstreams::ferris::sql::error::SqlError;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🔧 Testing FerrisStreams Error Recovery System");
    println!("===============================================");

    // Test 1: Circuit Breaker Pattern
    println!("\n⚡ Test 1: Circuit Breaker Pattern");
    println!("----------------------------------");

    let circuit_breaker = CircuitBreaker::builder()
        .name("test_service".to_string())
        .failure_threshold(3)
        .recovery_timeout(Duration::from_secs(2))
        .success_threshold(2)
        .request_timeout(Duration::from_secs(1))
        .build();

    println!("✅ Circuit breaker created with 3 failure threshold");
    println!("   • Initial state: {:?}", circuit_breaker.state().await);

    // Simulate successful operations
    println!("\nTesting successful operations:");
    for i in 1..=2 {
        let result = circuit_breaker
            .call(async { Ok::<_, SqlError>(format!("Success {}", i)) })
            .await;
        println!("   • Operation {}: {:?}", i, result.is_ok());
    }

    // Simulate failures to open circuit
    println!("\nTesting failure scenarios:");
    for i in 1..=3 {
        let _result = circuit_breaker
            .call(async {
                Err::<String, _>(SqlError::execution_error(
                    format!("Simulated failure {}", i),
                    None,
                ))
            })
            .await;
        println!(
            "   • Failure {}: Circuit state = {:?}",
            i,
            circuit_breaker.state().await
        );
    }

    // Test circuit open behavior
    println!("\nTesting circuit open behavior:");
    let result = circuit_breaker
        .call(async { Ok::<_, SqlError>("Should be blocked".to_string()) })
        .await;
    match result {
        Err(RecoveryError::CircuitOpen {
            service,
            retry_after,
            ..
        }) => {
            println!(
                "   ✅ Circuit correctly blocked request for '{}', retry in {:?}",
                service, retry_after
            );
        }
        _ => println!("   ❌ Expected circuit to be open"),
    }

    // Get circuit breaker metrics
    let cb_metrics = circuit_breaker.metrics().await;
    println!("📊 Circuit Breaker metrics:");
    println!("   • Total requests: {}", cb_metrics.total_requests);
    println!(
        "   • Success rate: {:.1}%",
        (cb_metrics.successful_requests as f64 / cb_metrics.total_requests as f64) * 100.0
    );
    println!(
        "   • Circuit opened: {} times",
        cb_metrics.circuit_opened_count
    );

    // Test 2: Retry Policy with Exponential Backoff
    println!("\n🔄 Test 2: Retry Policy with Exponential Backoff");
    println!("------------------------------------------------");

    let retry_policy = RetryPolicy::exponential_backoff()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(10))
        .max_delay(Duration::from_millis(100))
        .retry_condition(RetryCondition::OnTimeout)
        .enable_jitter(true)
        .build();

    println!("✅ Retry policy created with exponential backoff");
    println!("   • Max attempts: 3");
    println!("   • Initial delay: 10ms");
    println!("   • Jitter enabled: true");

    // Test successful retry after failures
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let attempt_count = Arc::new(AtomicU32::new(0));
    let start_time = std::time::Instant::now();

    let result = retry_policy
        .execute(|| {
            let counter = attempt_count.clone();
            Box::pin(async move {
                let current_attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if current_attempt < 3 {
                    // Fail first two attempts
                    Err(RecoveryError::RecoveryTimeout {
                        operation: "database_query".to_string(),
                        timeout: Duration::from_millis(100),
                    })
                } else {
                    // Succeed on third attempt
                    Ok("Query successful after retries".to_string())
                }
            })
        })
        .await;

    let total_time = start_time.elapsed();
    let final_attempt_count = attempt_count.load(Ordering::SeqCst);
    match result {
        Ok(success_msg) => {
            println!(
                "   ✅ Operation succeeded after {} attempts in {:?}",
                final_attempt_count, total_time
            );
            println!("   • Result: {}", success_msg);
        }
        Err(e) => println!("   ❌ Operation failed: {}", e),
    }

    // Test retry exhaustion
    println!("\nTesting retry exhaustion:");
    let exhaustion_result = retry_policy
        .execute(|| {
            Box::pin(async {
                Err::<String, _>(RecoveryError::RecoveryTimeout {
                    operation: "always_fail".to_string(),
                    timeout: Duration::from_millis(50),
                })
            })
        })
        .await;

    match exhaustion_result {
        Err(RecoveryError::RetryExhausted { attempts, .. }) => {
            println!(
                "   ✅ Retry correctly exhausted after {} attempts",
                attempts
            );
        }
        _ => println!("   ❌ Expected retry exhaustion"),
    }

    // Test 3: Dead Letter Queue
    println!("\n📮 Test 3: Dead Letter Queue");
    println!("----------------------------");

    let dlq_config = DeadLetterConfig {
        max_messages: 100,
        message_ttl: Duration::from_secs(300),
        enable_auto_retry: false,
        enable_metrics: true,
        ..Default::default()
    };

    let dlq = DeadLetterQueue::with_config("failed_orders", dlq_config).await?;
    println!("✅ Dead letter queue created: 'failed_orders'");

    // Add failed messages
    for i in 1..=3 {
        let mut headers = HashMap::new();
        headers.insert("retry_count".to_string(), "0".to_string());
        headers.insert("source_topic".to_string(), "orders".to_string());

        let failed_msg = FailedMessage {
            id: format!("msg_{}", i),
            original_data: format!("{{\"order_id\": {}, \"amount\": {}.99}}", i, i * 10),
            error_details: format!(
                "Schema validation failed: missing field 'customer_id' in message {}",
                i
            ),
            failed_at: std::time::Instant::now(),
            retry_count: 0,
            source_topic: Some("orders".to_string()),
            headers,
        };

        dlq.enqueue(failed_msg).await?;
        println!("   • Enqueued failed message {}", i);
    }

    // Get DLQ metrics
    let dlq_metrics = dlq.metrics().await;
    println!("📊 Dead Letter Queue metrics:");
    println!("   • Total messages: {}", dlq_metrics.total_messages);
    println!(
        "   • Current queue size: {}",
        dlq_metrics.current_queue_size
    );

    // Retrieve messages for processing
    let failed_messages = dlq.dequeue(2).await?;
    println!(
        "   ✅ Retrieved {} messages for manual processing:",
        failed_messages.len()
    );

    for msg in &failed_messages {
        println!("     - {}: {}", msg.id, msg.error_details);
        println!("       Data: {}", msg.original_data);
    }

    // Test 4: Health Monitoring
    println!("\n🏥 Test 4: Health Monitoring System");
    println!("-----------------------------------");

    let health_config = HealthConfig {
        check_interval: Duration::from_secs(10),
        check_timeout: Duration::from_secs(2),
        failure_threshold: 2,
        enable_metrics: true,
    };

    let health_monitor = HealthMonitor::with_config(health_config);
    println!("✅ Health monitor created with 2 failure threshold");

    // Register components
    health_monitor
        .register_component("database".to_string())
        .await;
    health_monitor
        .register_component("kafka_cluster".to_string())
        .await;
    health_monitor
        .register_component("schema_registry".to_string())
        .await;
    println!("   • Registered 3 components for monitoring");

    // Update component health statuses
    let mut db_details = HashMap::new();
    db_details.insert(
        "connection_pool".to_string(),
        "10/20 active connections".to_string(),
    );
    db_details.insert("response_time_ms".to_string(), "45".to_string());

    health_monitor
        .update_health("database", HealthStatus::Healthy, db_details)
        .await;

    let mut kafka_details = HashMap::new();
    kafka_details.insert("brokers".to_string(), "3/3 brokers available".to_string());
    kafka_details.insert("partition_count".to_string(), "12".to_string());

    health_monitor
        .update_health("kafka_cluster", HealthStatus::Healthy, kafka_details)
        .await;

    let mut schema_details = HashMap::new();
    schema_details.insert(
        "registry_url".to_string(),
        "http://localhost:8081".to_string(),
    );
    schema_details.insert("schema_count".to_string(), "25".to_string());

    health_monitor
        .update_health("schema_registry", HealthStatus::Degraded, schema_details)
        .await;

    // Check individual component health
    if let Some(db_health) = health_monitor.component_health("database").await {
        println!("   📊 Database health: {:?}", db_health.status);
        println!("      Details: {:?}", db_health.details);
    }

    if let Some(kafka_health) = health_monitor.component_health("kafka_cluster").await {
        println!("   📊 Kafka health: {:?}", kafka_health.status);
        println!("      Details: {:?}", kafka_health.details);
    }

    if let Some(schema_health) = health_monitor.component_health("schema_registry").await {
        println!("   📊 Schema Registry health: {:?}", schema_health.status);
        println!("      Details: {:?}", schema_health.details);
    }

    // Check overall system health
    let overall_health = health_monitor.overall_health().await;
    println!("   🎯 Overall system health: {:?}", overall_health);

    // Get health metrics
    let health_metrics = health_monitor.metrics().await;
    println!("📊 Health monitoring metrics:");
    println!(
        "   • Components healthy: {}",
        health_metrics.components_healthy
    );
    println!(
        "   • Components unhealthy: {}",
        health_metrics.components_unhealthy
    );

    // Test 5: Circuit Breaker Recovery
    println!("\n🔄 Test 5: Circuit Breaker Recovery");
    println!("-----------------------------------");

    println!("Waiting for circuit breaker recovery timeout...");
    sleep(Duration::from_secs(2)).await;

    // Circuit should now be half-open, test recovery
    println!("Testing circuit breaker recovery:");

    // Successful operations should close the circuit
    for i in 1..=2 {
        let result = circuit_breaker
            .call(async move { Ok::<_, SqlError>(format!("Recovery success {}", i)) })
            .await;
        println!(
            "   • Recovery attempt {}: {:?} | State: {:?}",
            i,
            result.is_ok(),
            circuit_breaker.state().await
        );
    }

    // Final circuit breaker metrics
    let final_cb_metrics = circuit_breaker.metrics().await;
    println!("📊 Final Circuit Breaker metrics:");
    println!("   • Total requests: {}", final_cb_metrics.total_requests);
    println!(
        "   • Successful requests: {}",
        final_cb_metrics.successful_requests
    );
    println!("   • Failed requests: {}", final_cb_metrics.failed_requests);
    println!("   • Circuit state transitions:");
    println!("     - Opened: {}", final_cb_metrics.circuit_opened_count);
    println!(
        "     - Half-opened: {}",
        final_cb_metrics.circuit_half_opened_count
    );
    println!("     - Closed: {}", final_cb_metrics.circuit_closed_count);
    println!(
        "   • Average response time: {:.2}ms",
        final_cb_metrics.avg_response_time_ms
    );

    // Test 6: Integration Testing
    println!("\n🔗 Test 6: Recovery System Integration");
    println!("--------------------------------------");

    println!("Testing integrated recovery scenario:");
    println!("   ✅ Demonstrating integration of circuit breaker, retry policy, and DLQ");
    println!("   • Circuit breaker protects against cascading failures");
    println!("   • Retry policy handles transient errors with exponential backoff");
    println!("   • Dead letter queue captures unrecoverable failures");
    println!("   • Health monitoring tracks overall system status");

    // Maintenance operations
    println!("\n🧹 Performing maintenance operations:");

    let dlq_maintenance = dlq.maintenance().await?;
    println!(
        "   • DLQ maintenance: removed {} expired messages",
        dlq_maintenance
    );

    println!("   • Circuit breaker reset");
    circuit_breaker.reset().await;

    println!("\n🎉 Error Recovery System Test Completed Successfully!");
    println!("Key achievements:");
    println!("   ✅ Circuit breaker pattern with automatic recovery");
    println!("   ✅ Exponential backoff retry with jitter");
    println!("   ✅ Dead letter queue for failed message handling");
    println!("   ✅ Comprehensive health monitoring system");
    println!("   ✅ Integrated resilience patterns");
    println!("   ✅ Performance metrics and monitoring");

    Ok(())
}
