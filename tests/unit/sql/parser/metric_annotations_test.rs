use velostream::velostream::sql::parser::annotations::{
    MetricAnnotation, MetricType, parse_metric_annotations,
};

#[test]
fn test_parse_simple_counter_annotation() {
    let comments = vec![
        "@metric: test_counter_total".to_string(),
        "@metric_type: counter".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations[0].name, "test_counter_total");
    assert_eq!(annotations[0].metric_type, MetricType::Counter);
    assert_eq!(annotations[0].labels, Vec::<String>::new());
    assert_eq!(annotations[0].sample_rate, 1.0);
}

#[test]
fn test_parse_counter_with_labels() {
    let comments = vec![
        "@metric: events_total".to_string(),
        "@metric_type: counter".to_string(),
        "@metric_labels: status, customer_type".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations[0].name, "events_total");
    assert_eq!(annotations[0].labels, vec!["status", "customer_type"]);
}

#[test]
fn test_parse_counter_with_help() {
    let comments = vec![
        "@metric: api_requests_total".to_string(),
        "@metric_type: counter".to_string(),
        "@metric_help: \"Total number of API requests\"".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    assert_eq!(
        annotations[0].help,
        Some("Total number of API requests".to_string())
    );
}

#[test]
fn test_parse_gauge_annotation() {
    let comments = vec![
        "@metric: active_connections".to_string(),
        "@metric_type: gauge".to_string(),
        "@metric_field: connection_count".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations[0].name, "active_connections");
    assert_eq!(annotations[0].metric_type, MetricType::Gauge);
    assert_eq!(annotations[0].field, Some("connection_count".to_string()));
}

#[test]
fn test_parse_histogram_annotation() {
    let comments = vec![
        "@metric: request_duration_seconds".to_string(),
        "@metric_type: histogram".to_string(),
        "@metric_field: duration_ms".to_string(),
        "@metric_buckets: [0.01, 0.05, 0.1, 0.5, 1.0]".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations[0].name, "request_duration_seconds");
    assert_eq!(annotations[0].metric_type, MetricType::Histogram);
    assert_eq!(annotations[0].field, Some("duration_ms".to_string()));
    assert_eq!(
        annotations[0].buckets,
        Some(vec![0.01, 0.05, 0.1, 0.5, 1.0])
    );
}

#[test]
fn test_parse_annotation_with_condition() {
    let comments = vec![
        "@metric: high_value_orders_total".to_string(),
        "@metric_type: counter".to_string(),
        "@metric_condition: amount > 1000.0".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    assert_eq!(
        annotations[0].condition,
        Some("amount > 1000.0".to_string())
    );
}

#[test]
fn test_parse_annotation_with_sample_rate() {
    let comments = vec![
        "@metric: events_sample_total".to_string(),
        "@metric_type: counter".to_string(),
        "@metric_sample_rate: 0.1".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations[0].sample_rate, 0.1);
}

#[test]
fn test_parse_multiple_annotations() {
    let comments = vec![
        "@metric: first_metric_total".to_string(),
        "@metric_type: counter".to_string(),
        "@metric: second_metric_total".to_string(),
        "@metric_type: counter".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 2);
    assert_eq!(annotations[0].name, "first_metric_total");
    assert_eq!(annotations[1].name, "second_metric_total");
}

#[test]
fn test_parse_complete_annotation() {
    let comments = vec![
        "@metric: velo_trading_volume_spikes_total".to_string(),
        "@metric_type: counter".to_string(),
        "@metric_help: \"Total number of volume spikes detected\"".to_string(),
        "@metric_labels: symbol, spike_ratio".to_string(),
        "@metric_condition: volume > avg_volume * 2.0".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    let annotation = &annotations[0];
    assert_eq!(annotation.name, "velo_trading_volume_spikes_total");
    assert_eq!(annotation.metric_type, MetricType::Counter);
    assert_eq!(
        annotation.help,
        Some("Total number of volume spikes detected".to_string())
    );
    assert_eq!(annotation.labels, vec!["symbol", "spike_ratio"]);
    assert_eq!(
        annotation.condition,
        Some("volume > avg_volume * 2.0".to_string())
    );
}

#[test]
fn test_parse_annotation_skips_non_annotation_comments() {
    let comments = vec![
        "This is a regular comment".to_string(),
        "@metric: test_metric_total".to_string(),
        "@metric_type: counter".to_string(),
        "Another regular comment".to_string(),
    ];

    let annotations = parse_metric_annotations(&comments).unwrap();

    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations[0].name, "test_metric_total");
}

#[test]
fn test_parse_annotation_invalid_metric_type() {
    let comments = vec![
        "@metric: test_metric".to_string(),
        "@metric_type: invalid_type".to_string(),
    ];

    let result = parse_metric_annotations(&comments);
    assert!(result.is_err());
}

#[test]
fn test_parse_annotation_gauge_without_field() {
    let comments = vec![
        "@metric: test_gauge".to_string(),
        "@metric_type: gauge".to_string(),
        // Missing @metric_field
    ];

    let result = parse_metric_annotations(&comments);
    assert!(result.is_err());
}

#[test]
fn test_parse_annotation_invalid_sample_rate() {
    let comments = vec![
        "@metric: test_metric_total".to_string(),
        "@metric_type: counter".to_string(),
        "@metric_sample_rate: 1.5".to_string(), // Out of range
    ];

    let result = parse_metric_annotations(&comments);
    assert!(result.is_err());
}

#[test]
fn test_parse_annotation_invalid_metric_name() {
    let comments = vec![
        "@metric: 123_invalid".to_string(), // Starts with number
        "@metric_type: counter".to_string(),
    ];

    let result = parse_metric_annotations(&comments);
    assert!(result.is_err());
}

#[test]
fn test_parse_annotation_metric_type_without_metric() {
    let comments = vec![
        "@metric_type: counter".to_string(), // No preceding @metric
    ];

    let result = parse_metric_annotations(&comments);
    assert!(result.is_err());
}
