use velostream::velostream::table::streaming::{StreamQueryBuilder, StreamingMetrics};

#[test]
fn test_streaming_metrics() {
    let metrics = StreamingMetrics {
        records_processed: 10000,
        bytes_processed: 10_485_760, // 10 MB
        processing_time_ms: 1000,    // 1 second
        memory_peak_mb: 50.0,
    };

    assert_eq!(metrics.records_per_second(), 10000.0);
    assert_eq!(metrics.mb_per_second(), 10.0);
}

#[test]
fn test_query_builder() {
    let builder = StreamQueryBuilder::new()
        .where_clause("active = true")
        .select(vec!["id".to_string(), "name".to_string()])
        .batch_size(500)
        .limit(1000);

    assert_eq!(builder.where_clause, Some("active = true".to_string()));
    assert_eq!(builder.batch_size, 500);
    assert_eq!(builder.limit, Some(1000));
}
