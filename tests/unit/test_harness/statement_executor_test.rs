//! Tests for statement-by-statement SQL execution
//!
//! Tests the StatementExecutor and DebugSession functionality.

use std::time::Duration;
use velostream::velostream::test_harness::infra::TestHarnessInfra;
use velostream::velostream::test_harness::statement_executor::{
    CommandResult, DebugCommand, DebugSession, ExecutionMode, ParsedStatement, SessionState,
    StatementExecutor, StatementType,
};

// ============================================================================
// StatementType Tests
// ============================================================================

#[test]
fn test_statement_type_create_stream() {
    let sql = "CREATE STREAM output AS SELECT * FROM input";
    assert_eq!(StatementType::from_sql(sql), StatementType::CreateStream);

    // Case insensitive
    let sql_lower = "create stream output as select * from input";
    assert_eq!(
        StatementType::from_sql(sql_lower),
        StatementType::CreateStream
    );
}

#[test]
fn test_statement_type_create_table() {
    let sql = "CREATE TABLE summary AS SELECT region, COUNT(*) FROM events GROUP BY region";
    assert_eq!(StatementType::from_sql(sql), StatementType::CreateTable);
}

#[test]
fn test_statement_type_select() {
    let sql = "SELECT symbol, price FROM market_data WHERE price > 100";
    assert_eq!(StatementType::from_sql(sql), StatementType::Select);
}

#[test]
fn test_statement_type_insert() {
    let sql = "INSERT INTO output SELECT * FROM source";
    assert_eq!(StatementType::from_sql(sql), StatementType::Insert);
}

#[test]
fn test_statement_type_other() {
    let sql = "DROP TABLE old_data";
    assert_eq!(
        StatementType::from_sql(sql),
        StatementType::Other("DROP".to_string())
    );

    let sql2 = "ALTER STREAM output ADD COLUMN new_field STRING";
    assert_eq!(
        StatementType::from_sql(sql2),
        StatementType::Other("ALTER".to_string())
    );
}

#[test]
fn test_statement_type_display_name() {
    assert_eq!(StatementType::CreateStream.display_name(), "CREATE STREAM");
    assert_eq!(StatementType::CreateTable.display_name(), "CREATE TABLE");
    assert_eq!(StatementType::Select.display_name(), "SELECT");
    assert_eq!(StatementType::Insert.display_name(), "INSERT");
    assert_eq!(
        StatementType::Other("DROP".to_string()).display_name(),
        "DROP"
    );
}

// ============================================================================
// ExecutionMode Tests
// ============================================================================

#[test]
fn test_execution_mode_default() {
    let mode = ExecutionMode::default();
    assert_eq!(mode, ExecutionMode::Full);
}

#[test]
fn test_execution_mode_equality() {
    assert_eq!(ExecutionMode::Full, ExecutionMode::Full);
    assert_eq!(ExecutionMode::Step, ExecutionMode::Step);
    assert_eq!(ExecutionMode::Breakpoint, ExecutionMode::Breakpoint);
    assert_ne!(ExecutionMode::Full, ExecutionMode::Step);
}

// ============================================================================
// SessionState Tests
// ============================================================================

#[test]
fn test_session_state_equality() {
    assert_eq!(SessionState::NotStarted, SessionState::NotStarted);
    assert_eq!(SessionState::Running, SessionState::Running);
    assert_eq!(SessionState::Completed, SessionState::Completed);
    assert_eq!(SessionState::Error, SessionState::Error);
    assert_eq!(SessionState::Paused(0), SessionState::Paused(0));
    assert_ne!(SessionState::Paused(0), SessionState::Paused(1));
}

// ============================================================================
// StatementExecutor Tests
// ============================================================================

#[test]
fn test_statement_executor_creation() {
    let infra = TestHarnessInfra::new();
    let executor = StatementExecutor::new(infra, Duration::from_secs(30));

    assert_eq!(executor.state(), &SessionState::NotStarted);
    assert!(executor.breakpoints().is_empty());
    assert!(executor.statements().is_empty());
    assert!(executor.results().is_empty());
    assert_eq!(executor.current_index(), 0);
}

#[test]
fn test_statement_executor_with_mode() {
    let infra = TestHarnessInfra::new();
    let executor =
        StatementExecutor::new(infra, Duration::from_secs(30)).with_mode(ExecutionMode::Step);

    // Mode is private, but we can test behavior
    assert_eq!(executor.state(), &SessionState::NotStarted);
}

#[test]
fn test_breakpoint_add_remove() {
    let infra = TestHarnessInfra::new();
    let mut executor = StatementExecutor::new(infra, Duration::from_secs(30));

    // Initially empty
    assert!(executor.breakpoints().is_empty());

    // Add breakpoints
    executor.add_breakpoint("query1");
    executor.add_breakpoint("query2");
    executor.add_breakpoint("query3");

    assert_eq!(executor.breakpoints().len(), 3);
    assert!(executor.breakpoints().contains("query1"));
    assert!(executor.breakpoints().contains("query2"));
    assert!(executor.breakpoints().contains("query3"));

    // Remove breakpoint
    assert!(executor.remove_breakpoint("query2"));
    assert_eq!(executor.breakpoints().len(), 2);
    assert!(!executor.breakpoints().contains("query2"));

    // Remove non-existent breakpoint
    assert!(!executor.remove_breakpoint("nonexistent"));
    assert_eq!(executor.breakpoints().len(), 2);

    // Clear all
    executor.clear_breakpoints();
    assert!(executor.breakpoints().is_empty());
}

#[test]
fn test_breakpoint_with_string_types() {
    let infra = TestHarnessInfra::new();
    let mut executor = StatementExecutor::new(infra, Duration::from_secs(30));

    // Test with &str
    executor.add_breakpoint("query1");

    // Test with String
    executor.add_breakpoint(String::from("query2"));

    // Test with owned String
    let name = "query3".to_string();
    executor.add_breakpoint(name);

    assert_eq!(executor.breakpoints().len(), 3);
}

#[test]
fn test_state_summary_not_started() {
    let infra = TestHarnessInfra::new();
    let executor = StatementExecutor::new(infra, Duration::from_secs(30));

    let summary = executor.state_summary();
    assert!(summary.contains("Not started"));
    assert!(summary.contains("No statements loaded"));
    assert!(summary.contains("No breakpoints"));
}

#[test]
fn test_state_summary_with_breakpoints() {
    let infra = TestHarnessInfra::new();
    let mut executor = StatementExecutor::new(infra, Duration::from_secs(30));

    executor.add_breakpoint("bp1");
    executor.add_breakpoint("bp2");

    let summary = executor.state_summary();
    assert!(summary.contains("Breakpoints"));
}

// ============================================================================
// ParsedStatement Tests
// ============================================================================

#[test]
fn test_parsed_statement_creation() {
    let stmt = ParsedStatement {
        index: 0,
        name: "test_query".to_string(),
        sql_text: "SELECT * FROM test".to_string(),
        statement_type: StatementType::Select,
        has_sink: true,
        sink_topic: Some("output_topic".to_string()),
        source_topics: vec!["input_topic".to_string()],
    };

    assert_eq!(stmt.index, 0);
    assert_eq!(stmt.name, "test_query");
    assert!(stmt.has_sink);
    assert_eq!(stmt.sink_topic, Some("output_topic".to_string()));
    assert_eq!(stmt.source_topics.len(), 1);
}

// ============================================================================
// DebugSession Tests
// ============================================================================

#[test]
fn test_debug_session_creation() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let session = DebugSession::new(stmt_executor);

    assert!(session.history().is_empty());
    assert_eq!(session.executor().state(), &SessionState::NotStarted);
}

#[test]
fn test_debug_session_executor_access() {
    let infra = TestHarnessInfra::new();
    let mut stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    stmt_executor.add_breakpoint("test_bp");

    let session = DebugSession::new(stmt_executor);

    // Can access executor
    assert!(session.executor().breakpoints().contains("test_bp"));
}

#[test]
fn test_debug_session_mutable_access() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    // Can mutate executor
    session.executor_mut().add_breakpoint("new_bp");
    assert!(session.executor().breakpoints().contains("new_bp"));
}

// ============================================================================
// DebugCommand Tests
// ============================================================================

#[test]
fn test_debug_command_clone() {
    let cmd = DebugCommand::Break("test".to_string());
    let cloned = cmd.clone();

    match cloned {
        DebugCommand::Break(name) => assert_eq!(name, "test"),
        _ => panic!("Expected Break command"),
    }
}

#[test]
fn test_debug_command_variants() {
    // Test all variants can be created
    let _step = DebugCommand::Step;
    let _continue = DebugCommand::Continue;
    let _run = DebugCommand::Run;
    let _break = DebugCommand::Break("query".to_string());
    let _unbreak = DebugCommand::Unbreak("query".to_string());
    let _clear = DebugCommand::Clear;
    let _list = DebugCommand::List;
    let _status = DebugCommand::Status;
    let _inspect = DebugCommand::Inspect("query".to_string());
    let _inspect_all = DebugCommand::InspectAll;
    let _history = DebugCommand::History;
    let _quit = DebugCommand::Quit;
}

// ============================================================================
// Integration-like Tests (without Kafka)
// ============================================================================

#[tokio::test]
async fn test_execute_all_without_statements_returns_error() {
    let infra = TestHarnessInfra::new();
    let mut executor = StatementExecutor::new(infra, Duration::from_secs(30));

    // Should error when no statements loaded
    let result = executor.execute_all().await;
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(err.to_string().contains("No statements loaded"));
}

#[tokio::test]
async fn test_step_next_without_statements() {
    let infra = TestHarnessInfra::new();
    let mut executor = StatementExecutor::new(infra, Duration::from_secs(30));

    // Should return None when no statements
    let result = executor.step_next().await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
    assert_eq!(executor.state(), &SessionState::Completed);
}

#[tokio::test]
async fn test_debug_session_break_command() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    let result = session
        .execute_command(DebugCommand::Break("test_query".to_string()))
        .await;

    assert!(result.is_ok());
    assert!(session.executor().breakpoints().contains("test_query"));
    assert_eq!(session.history().len(), 1);
}

#[tokio::test]
async fn test_debug_session_unbreak_command() {
    let infra = TestHarnessInfra::new();
    let mut stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    stmt_executor.add_breakpoint("test_query");

    let mut session = DebugSession::new(stmt_executor);

    let result = session
        .execute_command(DebugCommand::Unbreak("test_query".to_string()))
        .await;

    assert!(result.is_ok());
    assert!(!session.executor().breakpoints().contains("test_query"));
}

#[tokio::test]
async fn test_debug_session_list_command() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    let result = session.execute_command(DebugCommand::List).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_debug_session_status_command() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    let result = session.execute_command(DebugCommand::Status).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_debug_session_quit_command() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    let result = session.execute_command(DebugCommand::Quit).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_debug_session_history_tracking() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    // Execute multiple commands
    session.execute_command(DebugCommand::Status).await.ok();
    session.execute_command(DebugCommand::List).await.ok();
    session
        .execute_command(DebugCommand::Break("q1".to_string()))
        .await
        .ok();

    assert_eq!(session.history().len(), 3);
}

#[tokio::test]
async fn test_debug_session_clear_command() {
    let infra = TestHarnessInfra::new();
    let mut stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    // Add some breakpoints
    stmt_executor.add_breakpoint("q1");
    stmt_executor.add_breakpoint("q2");
    stmt_executor.add_breakpoint("q3");
    assert_eq!(stmt_executor.breakpoints().len(), 3);

    let mut session = DebugSession::new(stmt_executor);

    let result = session.execute_command(DebugCommand::Clear).await;
    assert!(result.is_ok());

    // All breakpoints should be cleared
    assert!(session.executor().breakpoints().is_empty());

    // Check the message
    if let Ok(CommandResult::Message(msg)) = result {
        assert!(msg.contains("Cleared 3 breakpoint"));
    } else {
        panic!("Expected Message result");
    }
}

#[tokio::test]
async fn test_debug_session_clear_command_no_breakpoints() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    let result = session.execute_command(DebugCommand::Clear).await;
    assert!(result.is_ok());

    // Check the message says 0 cleared
    if let Ok(CommandResult::Message(msg)) = result {
        assert!(msg.contains("Cleared 0 breakpoint"));
    } else {
        panic!("Expected Message result");
    }
}

#[tokio::test]
async fn test_debug_session_inspect_all_no_outputs() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    let result = session.execute_command(DebugCommand::InspectAll).await;
    assert!(result.is_ok());

    // Should return message about no outputs
    if let Ok(CommandResult::Message(msg)) = result {
        assert!(msg.contains("No outputs captured"));
    } else {
        panic!("Expected Message result for no outputs");
    }
}

#[tokio::test]
async fn test_debug_session_history_command() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    // Execute some commands to build history
    session.execute_command(DebugCommand::Status).await.ok();
    session.execute_command(DebugCommand::List).await.ok();
    session
        .execute_command(DebugCommand::Break("test".to_string()))
        .await
        .ok();

    // Now get history
    let result = session.execute_command(DebugCommand::History).await;
    assert!(result.is_ok());

    if let Ok(CommandResult::HistoryListing(lines)) = result {
        // Should have 4 entries (3 previous + 1 for History command itself)
        assert_eq!(lines.len(), 4);
        assert!(lines[0].contains("Status"));
        assert!(lines[1].contains("List"));
        assert!(lines[2].contains("Break"));
        assert!(lines[3].contains("History"));
    } else {
        panic!("Expected HistoryListing result");
    }
}

#[tokio::test]
async fn test_debug_session_history_command_empty() {
    let infra = TestHarnessInfra::new();
    let stmt_executor = StatementExecutor::new(infra, Duration::from_secs(30));
    let mut session = DebugSession::new(stmt_executor);

    // Get history immediately (will contain just the History command itself)
    let result = session.execute_command(DebugCommand::History).await;
    assert!(result.is_ok());

    if let Ok(CommandResult::HistoryListing(lines)) = result {
        // Should have 1 entry (the History command itself)
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("History"));
    } else {
        panic!("Expected HistoryListing result");
    }
}
