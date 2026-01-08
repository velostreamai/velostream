use velostream::velostream::sql::app_parser::SqlApplicationParser;

/// Test that @app annotation is parsed as shorthand for @application
#[test]
fn test_app_annotation_shorthand() {
    let sql = r#"
-- SQL Application: test_app
-- Version: 1.0.0
-- Description: Test application
-- @app: my_app_id
CREATE STREAM output AS SELECT * FROM input EMIT CHANGES;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser.parse_application(sql).expect("Should parse successfully");

    assert_eq!(app.metadata.application, Some("my_app_id".to_string()));
}

/// Test that @version annotation is parsed
#[test]
fn test_version_annotation() {
    let sql = r#"
-- SQL Application: test_app
-- @version: 2.5.0
CREATE STREAM output AS SELECT * FROM input EMIT CHANGES;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser.parse_application(sql).expect("Should parse successfully");

    assert_eq!(app.metadata.version, "2.5.0");
}

/// Test that @description annotation is parsed
#[test]
fn test_description_annotation() {
    let sql = r#"
-- SQL Application: test_app
-- @description: This is an annotation-style description
CREATE STREAM output AS SELECT * FROM input EMIT CHANGES;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser.parse_application(sql).expect("Should parse successfully");

    assert_eq!(
        app.metadata.description,
        Some("This is an annotation-style description".to_string())
    );
}

/// Test that @version annotation overrides header Version
#[test]
fn test_version_annotation_overrides_header() {
    let sql = r#"
-- SQL Application: test_app
-- Version: 1.0.0
-- @version: 3.0.0
CREATE STREAM output AS SELECT * FROM input EMIT CHANGES;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser.parse_application(sql).expect("Should parse successfully");

    // Annotation should override header
    assert_eq!(app.metadata.version, "3.0.0");
}

/// Test that @description annotation overrides header Description
#[test]
fn test_description_annotation_overrides_header() {
    let sql = r#"
-- SQL Application: test_app
-- Description: Header description
-- @description: Annotation description wins
CREATE STREAM output AS SELECT * FROM input EMIT CHANGES;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser.parse_application(sql).expect("Should parse successfully");

    // Annotation should override header
    assert_eq!(
        app.metadata.description,
        Some("Annotation description wins".to_string())
    );
}

/// Test that @app is equivalent to @application
#[test]
fn test_app_equivalent_to_application() {
    let sql_with_app = r#"
-- SQL Application: test_app
-- @app: my_app
CREATE STREAM output AS SELECT * FROM input EMIT CHANGES;
"#;

    let sql_with_application = r#"
-- SQL Application: test_app
-- @application: my_app
CREATE STREAM output AS SELECT * FROM input EMIT CHANGES;
"#;

    let parser = SqlApplicationParser::new();
    let app1 = parser
        .parse_application(sql_with_app)
        .expect("Should parse @app");
    let app2 = parser
        .parse_application(sql_with_application)
        .expect("Should parse @application");

    assert_eq!(app1.metadata.application, app2.metadata.application);
    assert_eq!(app1.metadata.application, Some("my_app".to_string()));
}

/// Test all three annotations together
#[test]
fn test_all_metadata_annotations() {
    let sql = r#"
-- SQL Application: test_app
-- @app: streaming_processor
-- @version: 4.2.1
-- @description: A comprehensive streaming SQL processor
CREATE STREAM output AS SELECT * FROM input EMIT CHANGES;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser.parse_application(sql).expect("Should parse successfully");

    assert_eq!(
        app.metadata.application,
        Some("streaming_processor".to_string())
    );
    assert_eq!(app.metadata.version, "4.2.1");
    assert_eq!(
        app.metadata.description,
        Some("A comprehensive streaming SQL processor".to_string())
    );
}

/// Test annotations with extra whitespace
#[test]
fn test_annotations_with_whitespace() {
    let sql = r#"
-- SQL Application: test_app
-- @app:   spaced_app
-- @version:   1.2.3
-- @description:   Description with spaces
CREATE STREAM output AS SELECT * FROM input EMIT CHANGES;
"#;

    let parser = SqlApplicationParser::new();
    let app = parser.parse_application(sql).expect("Should parse successfully");

    // Values should be trimmed
    assert_eq!(app.metadata.application, Some("spaced_app".to_string()));
    assert_eq!(app.metadata.version, "1.2.3");
    assert_eq!(
        app.metadata.description,
        Some("Description with spaces".to_string())
    );
}
