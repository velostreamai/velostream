//! Tokio Task Dispatcher Overhead Analysis
//!
//! This test isolates and measures the pure overhead of tokio's task dispatcher
//! without any application logic, database operations, or SQL processing.
//!
//! It demonstrates:
//! 1. Pure tokio task spawning and context switching overhead
//! 2. Channel send/receive latency (mpsc, broadcast, etc.)
//! 3. await point overhead in async functions
//! 4. Task scheduling and queue operations
//! 5. Comparison with synchronous dispatching
//!
//! This is the source of the ~20-34µs overhead observed in AdaptiveJobProcessor.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;

/// Minimal async task that does nothing - pure dispatcher overhead
async fn minimal_async_task(id: u64) -> u64 {
    id
}

/// Task that yields control - simulates partition receiver waiting
async fn yielding_task(id: u64) -> u64 {
    tokio::task::yield_now().await;
    id
}

/// Task that yields multiple times - simulates busy-spin
async fn multi_yield_task(id: u64, yields: usize) -> u64 {
    for _ in 0..yields {
        tokio::task::yield_now().await;
    }
    id
}

/// Test: Pure tokio spawn overhead
#[tokio::test]
#[ignore] // Run with: cargo test --test mod tokio_spawn_overhead -- --nocapture --ignored
async fn tokio_spawn_overhead() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ TOKIO SPAWN OVERHEAD - Task Creation & Scheduling         ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let num_tasks = 10000;
    println!("Spawning {} tasks (no awaiting)\n", num_tasks);

    // Measure spawn overhead only (not awaiting)
    let spawn_start = Instant::now();
    let mut handles = Vec::new();

    for i in 0..num_tasks {
        let handle = tokio::spawn(minimal_async_task(i as u64));
        handles.push(handle);
    }

    let spawn_time = spawn_start.elapsed();
    let spawn_per_task = spawn_time.as_micros() as f64 / num_tasks as f64;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ SPAWN RESULTS                                            ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Total spawn time: {:.2}µs", spawn_time.as_micros());
    println!("Per-task spawn: {:.3}µs", spawn_per_task);
    println!("Spawn throughput: {:.0} tasks/µs\n", 1.0 / spawn_per_task);

    // Now measure join overhead (awaiting completion)
    let join_start = Instant::now();

    for handle in handles {
        let _ = handle.await;
    }

    let join_time = join_start.elapsed();
    let join_per_task = join_time.as_micros() as f64 / num_tasks as f64;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ JOIN RESULTS                                             ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Total join time: {:.2}µs", join_time.as_micros());
    println!("Per-task join: {:.3}µs", join_per_task);
    println!("Join throughput: {:.0} tasks/µs\n", 1.0 / join_per_task);

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ TOTAL ROUND-TRIP (Spawn + Join)                          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let total_time = spawn_time + join_time;
    let total_per_task = total_time.as_micros() as f64 / num_tasks as f64;

    println!("Total round-trip: {:.2}µs", total_time.as_micros());
    println!("Per-task round-trip: {:.3}µs", total_per_task);
    println!(
        "Round-trip throughput: {:.0} tasks/sec\n",
        1_000_000.0 / total_per_task
    );

    println!("✅ Tokio spawn overhead analysis complete");
}

/// Test: MPSC channel overhead (producer-consumer pattern)
#[tokio::test]
#[ignore] // Run with: cargo test --test mod tokio_channel_overhead -- --nocapture --ignored
async fn tokio_channel_overhead() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ TOKIO MPSC CHANNEL OVERHEAD - Producer/Consumer Pattern   ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let num_items = 10000;
    let batch_size = 100;
    let num_batches = num_items / batch_size;

    println!("Configuration:");
    println!("  Items: {}", num_items);
    println!("  Batch size: {}", batch_size);
    println!("  Number of batches: {}\n", num_batches);

    // Create channel
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u64>>();

    // Producer task
    let producer = tokio::spawn(async move {
        let start = Instant::now();

        for batch_id in 0..num_batches {
            let batch: Vec<u64> = (0..batch_size as u64)
                .map(|i| batch_id as u64 * batch_size as u64 + i)
                .collect();

            // Send batch (tx is moved into this closure)
            let _ = tx.send(batch);
        }

        start.elapsed()
    });

    // Consumer task
    let consumer = tokio::spawn(async move {
        let start = Instant::now();
        let mut consumed = 0;

        while let Some(batch) = rx.recv().await {
            consumed += batch.len();
        }

        (start.elapsed(), consumed)
    });

    // Wait for producer to finish
    let produce_time = producer.await.unwrap();

    // Wait for consumer
    let (consume_time, total_consumed) = consumer.await.unwrap();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ CHANNEL PERFORMANCE METRICS                              ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Producer (send operations):");
    println!("  Time: {:.2}µs", produce_time.as_micros());
    println!(
        "  Per-batch send: {:.3}µs",
        produce_time.as_micros() as f64 / num_batches as f64
    );
    println!(
        "  Per-item send: {:.3}µs",
        produce_time.as_micros() as f64 / num_items as f64
    );

    println!("\nConsumer (recv operations):");
    println!("  Time: {:.2}µs", consume_time.as_micros());
    println!(
        "  Per-batch recv: {:.3}µs",
        consume_time.as_micros() as f64 / num_batches as f64
    );
    println!(
        "  Per-item recv: {:.3}µs",
        consume_time.as_micros() as f64 / num_items as f64
    );

    println!("\nTotal Items: {}", total_consumed);

    let total_latency = (produce_time + consume_time).as_micros() as f64 / (num_batches as f64);
    println!("\nRound-trip latency per batch: {:.3}µs", total_latency);
    println!("Throughput: {:.0} batches/sec", 1_000_000.0 / total_latency);
    println!(
        "Throughput: {:.0} items/sec\n",
        1_000_000.0 * batch_size as f64 / total_latency
    );

    println!("✅ Channel overhead analysis complete");
}

/// Test: yield_now() overhead in tight loop (simulates partition receiver wait pattern)
#[tokio::test]
#[ignore] // Run with: cargo test --test mod tokio_yield_overhead -- --nocapture --ignored
async fn tokio_yield_overhead() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ TOKIO yield_now() OVERHEAD - Busy-Spin Pattern           ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let num_iterations = 50;
    let num_repeats = 100;

    println!("Configuration:");
    println!("  Yields per iteration: {}", num_iterations);
    println!("  Number of iterations: {}\n", num_repeats);

    // Measure yield_now() overhead with multiple iterations
    let start = Instant::now();

    for _ in 0..num_repeats {
        for _ in 0..num_iterations {
            tokio::task::yield_now().await;
        }
    }

    let total_time = start.elapsed();
    let total_yields = num_iterations * num_repeats;
    let per_yield = total_time.as_micros() as f64 / total_yields as f64;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ YIELD RESULTS                                            ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Total time: {:.2}µs", total_time.as_micros());
    println!("Total yields: {}", total_yields);
    println!("Per-yield overhead: {:.4}µs", per_yield);
    println!("Yields per second: {:.0}\n", 1_000_000.0 / per_yield);

    // Now measure the same with pre-allocated async context
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ BATCH YIELD PATTERN (100 records, 50 yields per record)  ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let records_per_batch = 100;
    let yields_per_record = 50;

    let batch_start = Instant::now();

    for _ in 0..num_repeats {
        for _ in 0..records_per_batch {
            for _ in 0..yields_per_record {
                tokio::task::yield_now().await;
            }
        }
    }

    let batch_time = batch_start.elapsed();
    let total_batches = num_repeats;
    let total_records = num_repeats * records_per_batch;
    let per_record = batch_time.as_micros() as f64 / total_records as f64;

    println!("Total time: {:.2}µs", batch_time.as_micros());
    println!("Total records: {}", total_records);
    println!("Per-record latency: {:.2}µs", per_record);
    println!(
        "Per-batch latency: {:.0}µs",
        batch_time.as_micros() as f64 / total_batches as f64
    );
    println!("Throughput: {:.0} rec/sec\n", 1_000_000.0 / per_record);

    println!("✅ Yield overhead analysis complete");
}

/// Test: Task dispatcher throughput with continuous spawning
#[tokio::test]
#[ignore] // Run with: cargo test --test mod tokio_dispatcher_throughput -- --nocapture --ignored
async fn tokio_dispatcher_throughput() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ TOKIO DISPATCHER THROUGHPUT - Continuous Task Creation   ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let num_tasks = 50000;
    let batch_size = 100;

    println!("Configuration:");
    println!("  Total tasks: {}", num_tasks);
    println!("  Batch size: {}\n", batch_size);

    // Measure spawn throughput
    let spawn_start = Instant::now();
    let mut handles = Vec::new();

    for i in 0..num_tasks {
        handles.push(tokio::spawn(minimal_async_task(i as u64)));

        // Yield periodically to let tasks schedule
        if i % batch_size == 0 && i > 0 {
            tokio::task::yield_now().await;
        }
    }

    let spawn_elapsed = spawn_start.elapsed();

    // Measure join throughput
    let join_start = Instant::now();
    for handle in handles {
        let _ = handle.await;
    }
    let join_elapsed = join_start.elapsed();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ THROUGHPUT RESULTS                                       ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Spawn phase:");
    println!("  Time: {:.2}ms", spawn_elapsed.as_millis());
    println!(
        "  Throughput: {:.0} tasks/sec",
        num_tasks as f64 / spawn_elapsed.as_secs_f64()
    );

    println!("\nJoin phase:");
    println!("  Time: {:.2}ms", join_elapsed.as_millis());
    println!(
        "  Throughput: {:.0} tasks/sec",
        num_tasks as f64 / join_elapsed.as_secs_f64()
    );

    let total = spawn_elapsed + join_elapsed;
    println!("\nTotal round-trip:");
    println!("  Time: {:.2}ms", total.as_millis());
    println!(
        "  Throughput: {:.0} tasks/sec\n",
        num_tasks as f64 / total.as_secs_f64()
    );

    println!("✅ Dispatcher throughput analysis complete");
}

/// Test: Context switching overhead in multi-partition scenario
#[tokio::test]
#[ignore] // Run with: cargo test --test mod tokio_context_switching -- --nocapture --ignored
async fn tokio_context_switching() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ TOKIO CONTEXT SWITCHING - Multi-Partition Simulation      ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let num_partitions = 8;
    let records_per_partition = 1000;
    let yields_per_record = 20; // Simulate light yielding

    println!("Configuration:");
    println!("  Partitions: {}", num_partitions);
    println!("  Records per partition: {}", records_per_partition);
    println!("  Yields per record: {}\n", yields_per_record);

    // Spawn partition tasks
    let start = Instant::now();
    let mut handles = Vec::new();

    for partition_id in 0..num_partitions {
        let handle = tokio::spawn(async move {
            let partition_start = Instant::now();

            for _ in 0..records_per_partition {
                // Simulate yields while waiting for batch
                for _ in 0..yields_per_record {
                    tokio::task::yield_now().await;
                }
            }

            (partition_id, partition_start.elapsed())
        });

        handles.push(handle);
    }

    // Wait for all partitions
    let mut partition_times = Vec::new();
    for handle in handles {
        let (partition_id, elapsed) = handle.await.unwrap();
        partition_times.push((partition_id, elapsed));
    }

    let total_time = start.elapsed();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ CONTEXT SWITCHING RESULTS                                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("Total time: {:.2}ms", total_time.as_millis());
    println!(
        "Expected time (sequential): {:.2}ms",
        (num_partitions as f64 * records_per_partition as f64 * yields_per_record as f64 * 0.0001) // ~0.1µs per yield
    );

    let avg_per_record =
        total_time.as_micros() as f64 / (num_partitions * records_per_partition) as f64;
    println!("Per-record latency: {:.2}µs", avg_per_record);

    println!("\nPer-partition times:");
    for (partition_id, elapsed) in partition_times {
        let per_record = elapsed.as_micros() as f64 / records_per_partition as f64;
        println!(
            "  Partition {}: {:.2}ms ({:.2}µs/record)",
            partition_id,
            elapsed.as_millis(),
            per_record
        );
    }

    let speedup =
        (num_partitions as f64 * records_per_partition as f64 * yields_per_record as f64 * 0.0001)
            / total_time.as_secs_f64();

    println!("\nSpeedup vs sequential: {:.2}x", speedup);
    println!(
        "Throughput: {:.0} rec/sec\n",
        (num_partitions * records_per_partition) as f64 / total_time.as_secs_f64()
    );

    println!("✅ Context switching analysis complete");
}

/// Test: Measure actual overhead of async function call stack
#[tokio::test]
#[ignore] // Run with: cargo test --test mod tokio_async_call_overhead -- --nocapture --ignored
async fn tokio_async_call_overhead() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ TOKIO ASYNC CALL STACK OVERHEAD                          ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let iterations = 100_000;

    println!("Configuration:");
    println!("  Iterations: {}\n", iterations);

    // Synchronous baseline (just a function call)
    let sync_start = Instant::now();
    let mut sum = 0u64;
    for i in 0..iterations {
        sum += i as u64;
    }
    let sync_time = sync_start.elapsed();

    println!("Synchronous loop:");
    println!("  Time: {:.2}µs", sync_time.as_micros());
    println!(
        "  Per-iteration: {:.3}µs",
        sync_time.as_micros() as f64 / iterations as f64
    );

    // Async baseline (function call in async context)
    let async_start = Instant::now();
    let mut async_sum = 0u64;

    async fn add_value(val: u64, current: u64) -> u64 {
        current + val
    }

    for i in 0..iterations {
        async_sum = add_value(i as u64, async_sum).await;
    }

    let async_time = async_start.elapsed();

    println!("\nAsync function calls:");
    println!("  Time: {:.2}µs", async_time.as_micros());
    println!(
        "  Per-iteration: {:.3}µs",
        async_time.as_micros() as f64 / iterations as f64
    );

    let overhead_ratio = async_time.as_micros() as f64 / sync_time.as_micros() as f64;
    println!("\nAsync overhead factor: {:.1}x slower", overhead_ratio);

    // Per-record equivalent (if 100 records per batch)
    let per_record_async_overhead = async_time.as_micros() as f64 / (iterations as f64 / 100.0);
    println!(
        "Equivalent per-record overhead (100 rec/batch): {:.2}µs",
        per_record_async_overhead
    );

    println!("\n✅ Async call overhead analysis complete");
}

/// Test: Full simulation of partition receiver behavior with metrics
#[tokio::test]
#[ignore] // Run with: cargo test --test mod tokio_partition_simulation -- --nocapture --ignored
async fn tokio_partition_simulation() {
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ TOKIO PARTITION RECEIVER SIMULATION                       ║");
    println!("║ Simulates actual PartitionReceiver behavior with yields   ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let num_batches = 100;
    let batch_size = 100;
    let yields_between_batches = 50;
    let inter_batch_delay_us = 100;

    println!("Configuration:");
    println!("  Batches: {}", num_batches);
    println!("  Records per batch: {}", batch_size);
    println!("  Yields per batch: {}", yields_between_batches);
    println!(
        "  Simulated inter-batch delay: {}µs\n",
        inter_batch_delay_us
    );

    // Create a channel for batch distribution
    let (tx, mut rx) = mpsc::unbounded_channel::<u64>();

    // Producer (simulates coordinator)
    let producer = tokio::spawn(async move {
        let start = Instant::now();

        for batch_id in 0..num_batches {
            // Simulate some inter-batch delay
            tokio::time::sleep(tokio::time::Duration::from_micros(inter_batch_delay_us)).await;
            let _ = tx.send(batch_id); // tx is moved into closure
        }

        start.elapsed()
    });

    // Consumer (simulates partition receiver)
    let consumer = tokio::spawn(async move {
        let start = Instant::now();
        let mut batch_count = 0;

        while let Some(_batch_id) = rx.recv().await {
            // Simulate the busy-spin yield pattern while waiting for next batch
            for _ in 0..yields_between_batches {
                tokio::task::yield_now().await;
            }

            batch_count += 1;
        }

        (start.elapsed(), batch_count)
    });

    // Run producer
    let produce_time = producer.await.unwrap();

    // Wait for consumer
    let (consume_time, batches_consumed) = consumer.await.unwrap();

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ PARTITION SIMULATION RESULTS                             ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    let total_records = batches_consumed * batch_size;
    let total_time = consume_time;
    let per_record = total_time.as_micros() as f64 / total_records as f64;
    let per_batch = total_time.as_micros() as f64 / batches_consumed as f64;

    println!(
        "Producer (coordinator) time: {:.2}µs",
        produce_time.as_micros()
    );
    println!(
        "Consumer (partition) time: {:.2}µs",
        consume_time.as_micros()
    );

    println!("\nThroughput metrics:");
    println!("  Total records: {}", total_records);
    println!("  Per-record latency: {:.2}µs", per_record);
    println!("  Per-batch latency: {:.0}µs", per_batch);
    println!("  Throughput: {:.0} rec/sec\n", 1_000_000.0 / per_record);

    println!("✅ Partition simulation analysis complete");
}
