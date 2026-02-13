use std::str::FromStr;
use velostream::velostream::sql::annotation_parser::SqlAnnotationParser;
/// Comprehensive tests for SQL-level job annotation parsing
///
/// Tests the complete annotation system including:
/// - Job mode annotation parsing (@job_mode)
/// - Batch size annotation parsing (@batch_size)
/// - Partition count annotation parsing (@num_partitions)
/// - Partitioning strategy annotation parsing (@partitioning_strategy)
/// - Integration with SQL parser
/// - AST field population
use velostream::velostream::sql::ast::{
    JobProcessorMode, PartitioningStrategyType, SelectBuilder, SelectField, StreamSource,
};

#[test]
fn test_parse_job_mode_simple() {
    let sql = "-- @job_mode: simple\nSELECT * FROM orders";
    let (job_mode, _, _, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(job_mode, Some(JobProcessorMode::Simple));
}

#[test]
fn test_parse_job_mode_transactional() {
    let sql = "-- @job_mode: transactional\nSELECT * FROM orders";
    let (job_mode, _, _, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(job_mode, Some(JobProcessorMode::Transactional));
}

#[test]
fn test_parse_job_mode_adaptive() {
    let sql = "-- @job_mode: adaptive\nSELECT * FROM orders";
    let (job_mode, _, _, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(job_mode, Some(JobProcessorMode::Adaptive));
}

#[test]
fn test_parse_job_mode_case_insensitive() {
    let sql = "-- @job_mode: ADAPTIVE\nSELECT * FROM orders";
    let (job_mode, _, _, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(job_mode, Some(JobProcessorMode::Adaptive));

    let sql_mixed = "-- @job_mode: AdApTiVe\nSELECT * FROM orders";
    let (job_mode_mixed, _, _, _) = SqlAnnotationParser::parse_job_annotations(sql_mixed);
    assert_eq!(job_mode_mixed, Some(JobProcessorMode::Adaptive));
}

#[test]
fn test_parse_batch_size() {
    let sql = "-- @batch_size: 1000\nSELECT * FROM orders";
    let (_, batch_size, _, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(batch_size, Some(1000));
}

#[test]
fn test_parse_batch_size_large() {
    let sql = "-- @batch_size: 1000000\nSELECT * FROM orders";
    let (_, batch_size, _, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(batch_size, Some(1000000));
}

#[test]
fn test_parse_num_partitions() {
    let sql = "-- @num_partitions: 8\nSELECT * FROM orders";
    let (_, _, num_partitions, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(num_partitions, Some(8));
}

#[test]
fn test_parse_partitioning_strategy_sticky() {
    let sql = "-- @partitioning_strategy: sticky\nSELECT * FROM orders";
    let (_, _, _, strategy) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(strategy, Some(PartitioningStrategyType::Sticky));
}

#[test]
fn test_parse_partitioning_strategy_hash() {
    let sql = "-- @partitioning_strategy: hash\nSELECT * FROM orders";
    let (_, _, _, strategy) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(strategy, Some(PartitioningStrategyType::Hash));
}

#[test]
fn test_parse_partitioning_strategy_smart() {
    let sql = "-- @partitioning_strategy: smart\nSELECT * FROM orders";
    let (_, _, _, strategy) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(strategy, Some(PartitioningStrategyType::Smart));
}

#[test]
fn test_parse_partitioning_strategy_roundrobin() {
    let sql = "-- @partitioning_strategy: roundrobin\nSELECT * FROM orders";
    let (_, _, _, strategy) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(strategy, Some(PartitioningStrategyType::RoundRobin));
}

#[test]
fn test_parse_partitioning_strategy_fanin() {
    let sql = "-- @partitioning_strategy: fanin\nSELECT * FROM orders";
    let (_, _, _, strategy) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(strategy, Some(PartitioningStrategyType::FanIn));
}

#[test]
fn test_parse_all_annotations_together() {
    let sql = r#"
        -- @job_mode: adaptive
        -- @batch_size: 2000
        -- @num_partitions: 4
        -- @partitioning_strategy: hash
        SELECT * FROM orders
    "#;

    let (job_mode, batch_size, num_partitions, strategy) =
        SqlAnnotationParser::parse_job_annotations(sql);

    assert_eq!(job_mode, Some(JobProcessorMode::Adaptive));
    assert_eq!(batch_size, Some(2000));
    assert_eq!(num_partitions, Some(4));
    assert_eq!(strategy, Some(PartitioningStrategyType::Hash));
}

#[test]
fn test_parse_annotations_no_annotations() {
    let sql = "SELECT * FROM orders";
    let (job_mode, batch_size, num_partitions, strategy) =
        SqlAnnotationParser::parse_job_annotations(sql);

    assert_eq!(job_mode, None);
    assert_eq!(batch_size, None);
    assert_eq!(num_partitions, None);
    assert_eq!(strategy, None);
}

#[test]
fn test_parse_invalid_job_mode() {
    let sql = "-- @job_mode: invalid_mode\nSELECT * FROM orders";
    let (job_mode, _, _, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(job_mode, None); // Invalid modes are rejected
}

#[test]
fn test_parse_invalid_batch_size() {
    let sql = "-- @batch_size: not_a_number\nSELECT * FROM orders";
    let (_, batch_size, _, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(batch_size, None); // Invalid values are rejected
}

#[test]
fn test_parse_invalid_num_partitions() {
    let sql = "-- @num_partitions: abc\nSELECT * FROM orders";
    let (_, _, num_partitions, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(num_partitions, None); // Invalid values are rejected
}

#[test]
fn test_parse_invalid_partitioning_strategy() {
    let sql = "-- @partitioning_strategy: invalid_strategy\nSELECT * FROM orders";
    let (_, _, _, strategy) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(strategy, None); // Invalid strategies are rejected
}

#[test]
fn test_job_processor_mode_from_str() {
    assert_eq!(
        JobProcessorMode::from_str("simple"),
        Ok(JobProcessorMode::Simple)
    );
    assert_eq!(
        JobProcessorMode::from_str("transactional"),
        Ok(JobProcessorMode::Transactional)
    );
    assert_eq!(
        JobProcessorMode::from_str("adaptive"),
        Ok(JobProcessorMode::Adaptive)
    );
    assert!(JobProcessorMode::from_str("invalid").is_err());
    assert_eq!(
        JobProcessorMode::from_str("SIMPLE"),
        Ok(JobProcessorMode::Simple)
    ); // Case insensitive
}

#[test]
fn test_job_processor_mode_as_str() {
    assert_eq!(JobProcessorMode::Simple.as_str(), "simple");
    assert_eq!(JobProcessorMode::Transactional.as_str(), "transactional");
    assert_eq!(JobProcessorMode::Adaptive.as_str(), "adaptive");
}

#[test]
fn test_partitioning_strategy_type_from_str() {
    assert_eq!(
        PartitioningStrategyType::from_str("sticky"),
        Ok(PartitioningStrategyType::Sticky)
    );
    assert_eq!(
        PartitioningStrategyType::from_str("hash"),
        Ok(PartitioningStrategyType::Hash)
    );
    assert_eq!(
        PartitioningStrategyType::from_str("smart"),
        Ok(PartitioningStrategyType::Smart)
    );
    assert_eq!(
        PartitioningStrategyType::from_str("roundrobin"),
        Ok(PartitioningStrategyType::RoundRobin)
    );
    assert_eq!(
        PartitioningStrategyType::from_str("fanin"),
        Ok(PartitioningStrategyType::FanIn)
    );
    assert!(PartitioningStrategyType::from_str("invalid").is_err());
}

#[test]
fn test_partitioning_strategy_type_from_str_alternate_names() {
    // Test alternate names that might be used in documentation
    assert_eq!(
        PartitioningStrategyType::from_str("stickypartition"),
        Ok(PartitioningStrategyType::Sticky)
    );
    assert_eq!(
        PartitioningStrategyType::from_str("alwayshash"),
        Ok(PartitioningStrategyType::Hash)
    );
    assert_eq!(
        PartitioningStrategyType::from_str("smartrepartition"),
        Ok(PartitioningStrategyType::Smart)
    );
}

#[test]
fn test_partitioning_strategy_type_as_str() {
    assert_eq!(PartitioningStrategyType::Sticky.as_str(), "sticky");
    assert_eq!(PartitioningStrategyType::Hash.as_str(), "hash");
    assert_eq!(PartitioningStrategyType::Smart.as_str(), "smart");
    assert_eq!(PartitioningStrategyType::RoundRobin.as_str(), "roundrobin");
    assert_eq!(PartitioningStrategyType::FanIn.as_str(), "fanin");
}

#[test]
fn test_select_builder_with_annotations() {
    let query = SelectBuilder::new(
        vec![SelectField::Wildcard],
        StreamSource::Stream("orders".to_string()),
    )
    .with_job_mode(JobProcessorMode::Adaptive)
    .with_batch_size(1000)
    .with_num_partitions(8)
    .with_partitioning_strategy(PartitioningStrategyType::Hash)
    .build();

    // Verify the query was built with annotations
    match query {
        velostream::velostream::sql::ast::StreamingQuery::Select {
            job_mode,
            batch_size,
            num_partitions,
            partitioning_strategy,
            ..
        } => {
            assert_eq!(job_mode, Some(JobProcessorMode::Adaptive));
            assert_eq!(batch_size, Some(1000));
            assert_eq!(num_partitions, Some(8));
            assert_eq!(partitioning_strategy, Some(PartitioningStrategyType::Hash));
        }
        _ => panic!("Expected StreamingQuery::Select"),
    }
}

#[test]
fn test_select_builder_without_annotations() {
    let query = SelectBuilder::new(
        vec![SelectField::Wildcard],
        StreamSource::Stream("orders".to_string()),
    )
    .build();

    // Verify annotations default to None
    match query {
        velostream::velostream::sql::ast::StreamingQuery::Select {
            job_mode,
            batch_size,
            num_partitions,
            partitioning_strategy,
            ..
        } => {
            assert_eq!(job_mode, None);
            assert_eq!(batch_size, None);
            assert_eq!(num_partitions, None);
            assert_eq!(partitioning_strategy, None);
        }
        _ => panic!("Expected StreamingQuery::Select"),
    }
}

#[test]
fn test_select_builder_partial_annotations() {
    let query = SelectBuilder::new(
        vec![SelectField::Wildcard],
        StreamSource::Stream("orders".to_string()),
    )
    .with_job_mode(JobProcessorMode::Simple)
    .with_partitioning_strategy(PartitioningStrategyType::Sticky)
    .build();

    // Verify some annotations are set, others are None
    match query {
        velostream::velostream::sql::ast::StreamingQuery::Select {
            job_mode,
            batch_size,
            num_partitions,
            partitioning_strategy,
            ..
        } => {
            assert_eq!(job_mode, Some(JobProcessorMode::Simple));
            assert_eq!(batch_size, None);
            assert_eq!(num_partitions, None);
            assert_eq!(
                partitioning_strategy,
                Some(PartitioningStrategyType::Sticky)
            );
        }
        _ => panic!("Expected StreamingQuery::Select"),
    }
}

#[test]
fn test_annotations_with_complex_sql() {
    let sql = r#"
        -- @job_mode: adaptive
        -- @num_partitions: 16
        -- @partitioning_strategy: hash
        SELECT customer_id, COUNT(*) as order_count
        FROM orders
        WHERE amount > 100
        GROUP BY customer_id
        ORDER BY order_count DESC
        LIMIT 1000
    "#;

    let (job_mode, batch_size, num_partitions, strategy) =
        SqlAnnotationParser::parse_job_annotations(sql);

    assert_eq!(job_mode, Some(JobProcessorMode::Adaptive));
    assert_eq!(batch_size, None); // Not specified
    assert_eq!(num_partitions, Some(16));
    assert_eq!(strategy, Some(PartitioningStrategyType::Hash));
}

#[test]
fn test_annotations_multiple_comment_styles() {
    // Test that annotations work when placed before the SELECT statement
    let sql_before = r#"
        -- @job_mode: transactional
        SELECT * FROM orders
    "#;

    let sql_multiline = r#"
        -- @job_mode: transactional
        -- @batch_size: 500
        SELECT * FROM orders
    "#;

    let (mode1, _, _, _) = SqlAnnotationParser::parse_job_annotations(sql_before);
    let (mode2, batch2, _, _) = SqlAnnotationParser::parse_job_annotations(sql_multiline);

    assert_eq!(mode1, Some(JobProcessorMode::Transactional));
    assert_eq!(mode2, Some(JobProcessorMode::Transactional));
    assert_eq!(batch2, Some(500));
}

#[test]
fn test_zero_partition_count() {
    let sql = "-- @num_partitions: 0\nSELECT * FROM orders";
    let (_, _, num_partitions, _) = SqlAnnotationParser::parse_job_annotations(sql);
    // Zero partitions should be accepted but likely cause runtime errors
    assert_eq!(num_partitions, Some(0));
}

#[test]
fn test_large_partition_count() {
    let sql = "-- @num_partitions: 1024\nSELECT * FROM orders";
    let (_, _, num_partitions, _) = SqlAnnotationParser::parse_job_annotations(sql);
    assert_eq!(num_partitions, Some(1024));
}
