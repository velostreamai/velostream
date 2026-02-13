// Processor unit tests

pub mod aggregation_microbench_test;
pub mod data_generation_check_test;
pub mod error_tracking_test;
pub mod factory_config_wiring_test;
pub mod job_processor_factory_test;
pub mod metrics_context_labels_test;
pub mod metrics_emission_test;
pub mod metrics_gauge_field_value_test;
pub mod multi_source_sink_write_test;
pub mod multi_source_test;
pub mod multi_statement_metrics_emission_test;
pub mod observability_wrapper_test;
pub mod profiling_helper_test;
pub mod scenario_4_tumbling_hang_test;
pub mod simple_dlq_test;
pub mod sql_engine_partition_batching_test;
pub mod transactional_kafka_config_test;
pub mod transactional_multi_source_sink_write_test;
pub mod window_adapter_instrumentation_test;
pub mod window_adapter_profiling_test;
pub mod window_state_analysis_test;
