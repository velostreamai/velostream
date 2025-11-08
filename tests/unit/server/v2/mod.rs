//! FR-082 Phase 1-6: Job Server V2 hash-partitioned architecture unit tests

pub mod coordinator_test;
pub mod job_processor_v2_test;
pub mod metrics_test;
pub mod partition_manager_test;
pub mod phase4_system_fields_test;
pub mod phase5_window_integration_test;
pub mod phase6_all_scenarios_release_benchmark;
pub mod phase6_batch_profiling;
pub mod phase6_lockless_stp;
pub mod phase6_output_overhead_analysis;
pub mod phase6_profiling_breakdown;
pub mod phase6_raw_engine_performance;
pub mod phase6_stp_bottleneck_analysis;
pub mod phase6_v1_vs_v2_performance_test;
pub mod strategy_integration_test;
pub mod strategy_sql_annotation_test;
pub mod week9_baseline_benchmarks;
pub mod week9_v1_v2_comparison_test;
