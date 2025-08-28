// Tests for QueryAnalyzer - SQL query analysis for resource requirements
use ferrisstreams::ferris::sql::{
    ast::{
        ConfigProperties, IntoClause, SelectField, ShowResourceType, StreamSource, StreamingQuery,
    },
    query_analyzer::{DataSourceType, QueryAnalysis, QueryAnalyzer},
};
use std::collections::HashMap;

#[test]
fn test_select_query_analysis() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("orders_topic".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let analysis = analyzer.analyze(&query).unwrap();

    // Should have one source for the stream
    assert_eq!(analysis.required_sources.len(), 1);
    let source = &analysis.required_sources[0];
    assert_eq!(source.name, "orders_topic");
    assert_eq!(source.source_type, DataSourceType::Kafka);
    assert_eq!(
        source.properties.get("source.group.id").unwrap(),
        "test-group-orders_topic"
    );
}

#[test]
fn test_select_query_with_table_source() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Table("transactions_table".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let analysis = analyzer.analyze(&query).unwrap();

    // Should have one source for the table
    assert_eq!(analysis.required_sources.len(), 1);
    let source = &analysis.required_sources[0];
    assert_eq!(source.name, "transactions_table");
    assert_eq!(source.source_type, DataSourceType::Kafka);
}

#[test]
fn test_create_stream_analysis() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let mut properties = HashMap::new();
    properties.insert("source.type".to_string(), "kafka".to_string());
    properties.insert("source.topic".to_string(), "input_orders".to_string());
    properties.insert("value.serializer".to_string(), "json".to_string());

    let select_query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("orders".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let query = StreamingQuery::CreateStream {
        name: "processed_orders".to_string(),
        columns: None,
        as_select: Box::new(select_query),
        properties,
    };

    let analysis = analyzer.analyze(&query).unwrap();

    // Should have one source for the nested SELECT
    assert_eq!(analysis.required_sources.len(), 1);
    let source = &analysis.required_sources[0];
    assert_eq!(source.name, "orders");

    // Should have configuration from properties
    assert!(!analysis.configuration.is_empty());
    assert_eq!(analysis.configuration.get("source.type").unwrap(), "kafka");
}

#[test]
fn test_create_stream_into_analysis() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let mut properties = HashMap::new();
    properties.insert("source.type".to_string(), "kafka".to_string());
    properties.insert("sink.type".to_string(), "kafka".to_string());
    properties.insert("sink.topic".to_string(), "output_topic".to_string());

    let select_query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("input_stream".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let into_clause = IntoClause {
        sink_name: "output_sink".to_string(),
        sink_properties: HashMap::new(),
    };

    let config_props = ConfigProperties {
        inline_properties: properties.clone(),
        base_source_config: None,
        source_config: None,
        base_sink_config: None,
        sink_config: None,
        monitoring_config: None,
        security_config: None,
    };

    let query = StreamingQuery::CreateStreamInto {
        name: "stream_with_sink".to_string(),
        columns: None,
        as_select: Box::new(select_query),
        into_clause,
        properties: config_props,
    };

    let analysis = analyzer.analyze(&query).unwrap();

    // Should have both source and sink
    assert_eq!(analysis.required_sources.len(), 1);
    assert_eq!(analysis.required_sinks.len(), 1);

    let source = &analysis.required_sources[0];
    assert_eq!(source.name, "input_stream");

    let sink = &analysis.required_sinks[0];
    assert_eq!(sink.name, "output_sink");
}

#[test]
fn test_show_query_analysis() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let query = StreamingQuery::Show {
        resource_type: ShowResourceType::Streams,
        pattern: None,
    };

    let analysis = analyzer.analyze(&query).unwrap();

    // SHOW queries should not require sources or sinks
    assert_eq!(analysis.required_sources.len(), 0);
    assert_eq!(analysis.required_sinks.len(), 0);
}

#[test]
fn test_avro_serialization_format_analysis() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    // Create a mock analysis with Avro configuration
    let mut analysis = QueryAnalysis {
        required_sources: Vec::new(),
        required_sinks: Vec::new(),
        configuration: HashMap::new(),
    };

    // Add Avro configuration
    analysis
        .configuration
        .insert("value.serializer".to_string(), "avro".to_string());
    analysis.configuration.insert(
        "schema.registry.url".to_string(),
        "http://localhost:8081".to_string(),
    );

    // Test that the analysis holds the configuration
    assert_eq!(
        analysis.configuration.get("value.serializer").unwrap(),
        "avro"
    );
    assert_eq!(
        analysis.configuration.get("schema.registry.url").unwrap(),
        "http://localhost:8081"
    );
}

#[test]
fn test_empty_query_analysis() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("empty_topic".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let analysis = analyzer.analyze(&query).unwrap();

    // Basic validation
    assert!(!analysis.required_sources.is_empty());
    assert!(analysis.required_sinks.is_empty());
}

#[test]
fn test_subquery_analysis() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    let inner_query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("inner_stream".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let outer_query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Subquery(Box::new(inner_query)),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let analysis = analyzer.analyze(&outer_query).unwrap();

    // Should analyze the inner query and create source for inner stream
    assert_eq!(analysis.required_sources.len(), 1);
    let source = &analysis.required_sources[0];
    assert_eq!(source.name, "inner_stream");
}

#[test]
fn test_analyzer_with_custom_group_id() {
    let custom_group_id = "custom-consumer-group";
    let analyzer = QueryAnalyzer::new(custom_group_id.to_string());

    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("test_topic".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let analysis = analyzer.analyze(&query).unwrap();

    assert_eq!(analysis.required_sources.len(), 1);
    let source = &analysis.required_sources[0];
    assert_eq!(
        source.properties.get("source.group.id").unwrap(),
        "custom-consumer-group-test_topic"
    );
}

#[test]
fn test_file_source_inference() {
    let analyzer = QueryAnalyzer::new("test-group".to_string());

    // Test file source pattern recognition
    let query = StreamingQuery::Select {
        fields: vec![SelectField::Wildcard],
        from: StreamSource::Stream("file:///data/test.csv".to_string()),
        joins: None,
        where_clause: None,
        group_by: None,
        having: None,
        window: None,
        order_by: None,
        limit: None,
        emit_mode: None,
    };

    let analysis = analyzer.analyze(&query).unwrap();

    // Should recognize file source pattern and create file source requirement
    assert_eq!(analysis.required_sources.len(), 1);
    let file_source = &analysis.required_sources[0];
    assert_eq!(file_source.name, "file:///data/test.csv");
    assert_eq!(file_source.source_type, DataSourceType::File);
}
