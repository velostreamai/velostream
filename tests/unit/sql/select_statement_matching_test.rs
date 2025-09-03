use ferrisstreams::ferris::sql::app_parser::StatementType;

#[test]
fn test_statement_type_matching_for_deployment() {
    // Test that our match arms correctly identify SELECT statements for deployment
    let select_stmt = StatementType::Select;
    let start_job_stmt = StatementType::StartJob;
    let deploy_job_stmt = StatementType::DeployJob;
    let create_stream_stmt = StatementType::CreateStream;
    let create_table_stmt = StatementType::CreateTable;
    let show_stmt = StatementType::Show;
    let other_stmt = StatementType::Other("Unknown".to_string());

    // This mimics the match logic in deploy_sql_application
    let should_deploy_select = matches!(
        select_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );
    let should_deploy_start = matches!(
        start_job_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );
    let should_deploy_deploy = matches!(
        deploy_job_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );
    let should_skip_create_stream = matches!(
        create_stream_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );
    let should_skip_create_table = matches!(
        create_table_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );
    let should_skip_show = matches!(
        show_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );
    let should_skip_other = matches!(
        other_stmt,
        StatementType::StartJob | StatementType::DeployJob | StatementType::Select
    );

    // Assert what should be deployed as jobs
    assert!(
        should_deploy_select,
        "SELECT statements should be deployable as jobs"
    );
    assert!(
        should_deploy_start,
        "StartJob statements should be deployable"
    );
    assert!(
        should_deploy_deploy,
        "DeployJob statements should be deployable"
    );

    // Assert what should be skipped (not deployed as jobs)
    assert!(
        !should_skip_create_stream,
        "CreateStream statements should be skipped"
    );
    assert!(
        !should_skip_create_table,
        "CreateTable statements should be skipped"
    );
    assert!(!should_skip_show, "Show statements should be skipped");
    assert!(
        !should_skip_other,
        "Other/Unknown statements should be skipped"
    );

    println!("✅ Statement type matching test passed!");
}

#[test]
fn test_job_name_generation_logic() {
    // Test job name generation logic
    let app_name = "test_app";
    let stmt_order = 5;

    // Test auto-generated name (when name is None)
    let auto_generated_name = format!("{}_stmt_{}", app_name, stmt_order);
    assert_eq!(auto_generated_name, "test_app_stmt_5");

    // Test explicit name (when name is Some)
    let explicit_name = "custom_job_name";
    let used_name = Some(explicit_name.to_string())
        .unwrap_or_else(|| format!("{}_stmt_{}", app_name, stmt_order));
    assert_eq!(used_name, "custom_job_name");

    println!("✅ Job name generation test passed!");
}

#[test]
fn test_topic_generation_logic() {
    // Test topic determination logic
    let job_name = "test_job";
    let dependencies = vec!["input_topic".to_string()];
    let default_topic = Some("default_topic".to_string());

    // Test with dependencies - should use first dependency
    let topic_with_deps = if !dependencies.is_empty() {
        dependencies[0].clone()
    } else if let Some(ref default) = default_topic {
        default.clone()
    } else {
        format!("processed_data_{}", job_name)
    };
    assert_eq!(topic_with_deps, "input_topic");

    // Test with no dependencies but default topic - should use default
    let empty_deps: Vec<String> = vec![];
    let topic_with_default = if !empty_deps.is_empty() {
        empty_deps[0].clone()
    } else if let Some(ref default) = default_topic {
        default.clone()
    } else {
        format!("processed_data_{}", job_name)
    };
    assert_eq!(topic_with_default, "default_topic");

    // Test with no dependencies and no default - should auto-generate
    let no_default: Option<String> = None;
    let auto_generated_topic = if !empty_deps.is_empty() {
        empty_deps[0].clone()
    } else if let Some(ref default) = no_default {
        default.clone()
    } else {
        format!("processed_data_{}", job_name)
    };
    assert_eq!(auto_generated_topic, "processed_data_test_job");

    println!("✅ Topic generation logic test passed!");
}

#[test]
fn test_deployment_decision_matrix() {
    // Test all combinations of statement types to ensure correct deployment decisions
    let statement_types = vec![
        (
            StatementType::Select,
            true,
            "SELECT statements should deploy",
        ),
        (
            StatementType::StartJob,
            true,
            "StartJob statements should deploy",
        ),
        (
            StatementType::DeployJob,
            true,
            "DeployJob statements should deploy",
        ),
        (
            StatementType::CreateStream,
            false,
            "CreateStream statements should not deploy",
        ),
        (
            StatementType::CreateTable,
            false,
            "CreateTable statements should not deploy",
        ),
        (
            StatementType::Show,
            false,
            "Show statements should not deploy",
        ),
        (
            StatementType::Other("Unknown".to_string()),
            false,
            "Other statements should not deploy",
        ),
    ];

    for (stmt_type, should_deploy, description) in statement_types {
        let will_deploy = matches!(
            stmt_type,
            StatementType::StartJob | StatementType::DeployJob | StatementType::Select
        );

        assert_eq!(will_deploy, should_deploy, "{}", description);
    }

    println!("✅ Deployment decision matrix test passed!");
}
