//! Pre-computed query metadata for span enrichment.
//!
//! Extracts streaming intelligence from the AST once at job startup and attaches
//! it to every batch and SQL processing span with zero per-batch overhead.

use crate::velostream::sql::ast::{
    EmitMode, Expr, JoinType, StreamSource, StreamingQuery, WindowSpec,
};

/// Pre-computed query metadata for span enrichment.
/// Extracted once from StreamingQuery AST at job startup.
#[derive(Debug, Clone)]
pub struct QuerySpanMetadata {
    pub has_join: bool,
    pub join_type: Option<String>,
    pub join_sources: Option<String>,
    pub join_key_fields: Option<String>,
    pub has_window: bool,
    pub window_type: Option<String>,
    pub window_size_ms: Option<i64>,
    pub has_group_by: bool,
    pub group_by_fields: Option<String>,
    pub emit_mode: Option<String>,
}

impl QuerySpanMetadata {
    /// Extract metadata from a StreamingQuery AST node.
    ///
    /// Navigates wrapper types (CSAS, CTAS, StartJob, DeployJob) to find the
    /// inner SELECT statement and extract join, window, group_by, and emit metadata.
    pub fn from_query(query: &StreamingQuery) -> Self {
        match query {
            StreamingQuery::Select {
                from,
                joins,
                group_by,
                window,
                emit_mode,
                ..
            } => {
                let (has_join, join_type, join_sources, join_key_fields) =
                    Self::extract_join_metadata(from, joins, query);

                let (has_window, window_type, window_size_ms) =
                    Self::extract_window_metadata(window);

                let (has_group_by, group_by_fields) = Self::extract_group_by_metadata(group_by);

                let emit_mode_str = emit_mode.as_ref().map(Self::emit_mode_to_string);

                Self {
                    has_join,
                    join_type,
                    join_sources,
                    join_key_fields,
                    has_window,
                    window_type,
                    window_size_ms,
                    has_group_by,
                    group_by_fields,
                    emit_mode: emit_mode_str,
                }
            }
            // Navigate wrapper types to find the inner SELECT
            StreamingQuery::CreateStream {
                as_select,
                emit_mode,
                ..
            } => {
                let mut metadata = Self::from_query(as_select);
                // CSAS emit_mode overrides inner SELECT if set
                if let Some(em) = emit_mode {
                    metadata.emit_mode = Some(Self::emit_mode_to_string(em));
                }
                metadata
            }
            StreamingQuery::CreateTable {
                as_select,
                emit_mode,
                ..
            } => {
                let mut metadata = Self::from_query(as_select);
                if let Some(em) = emit_mode {
                    metadata.emit_mode = Some(Self::emit_mode_to_string(em));
                }
                metadata
            }
            StreamingQuery::StartJob { query, .. } => Self::from_query(query),
            StreamingQuery::DeployJob { query, .. } => Self::from_query(query),
            // Non-SELECT types have no streaming intelligence
            _ => Self::empty(),
        }
    }

    /// Create an empty metadata instance (all false/None).
    pub fn empty() -> Self {
        Self {
            has_join: false,
            join_type: None,
            join_sources: None,
            join_key_fields: None,
            has_window: false,
            window_type: None,
            window_size_ms: None,
            has_group_by: false,
            group_by_fields: None,
            emit_mode: None,
        }
    }

    fn emit_mode_to_string(em: &EmitMode) -> String {
        match em {
            EmitMode::Changes => "changes".to_string(),
            EmitMode::Final => "final".to_string(),
        }
    }

    /// Extract join metadata from the first join clause.
    ///
    /// Note: only the first join clause is inspected. Multi-join queries
    /// (e.g., `A JOIN B ON ... JOIN C ON ...`) report metadata for the
    /// primary join only, matching `extract_join_keys()` behavior.
    fn extract_join_metadata(
        from: &StreamSource,
        joins: &Option<Vec<crate::velostream::sql::ast::JoinClause>>,
        query: &StreamingQuery,
    ) -> (bool, Option<String>, Option<String>, Option<String>) {
        let has_join = joins.as_ref().is_some_and(|j| !j.is_empty());

        if !has_join {
            return (false, None, None, None);
        }

        let join_clauses = joins.as_ref().unwrap();
        let first_join = &join_clauses[0];

        let join_type = Some(
            match first_join.join_type {
                JoinType::Inner => "INNER",
                JoinType::Left => "LEFT",
                JoinType::Right => "RIGHT",
                JoinType::FullOuter => "FULL_OUTER",
            }
            .to_string(),
        );

        // Build source names: "left_source,right_source"
        let left_name = Self::source_name(from);
        let right_name = Self::source_name(&first_join.right_source);
        let join_sources = Some(format!("{},{}", left_name, right_name));

        // Extract join key fields from both sides of ON equalities.
        // Lists all column names (left and right) comma-separated.
        let keys = query.extract_join_keys();
        let join_key_fields = if keys.is_empty() {
            None
        } else {
            let field_names: Vec<String> = keys
                .iter()
                .flat_map(|(l, r)| [l.clone(), r.clone()])
                .collect();
            Some(field_names.join(","))
        };

        (has_join, join_type, join_sources, join_key_fields)
    }

    fn extract_window_metadata(window: &Option<WindowSpec>) -> (bool, Option<String>, Option<i64>) {
        match window {
            Some(WindowSpec::Tumbling { size, .. }) => (
                true,
                Some("tumbling".to_string()),
                Some(i64::try_from(size.as_millis()).unwrap_or(i64::MAX)),
            ),
            Some(WindowSpec::Sliding { size, .. }) => (
                true,
                Some("sliding".to_string()),
                Some(i64::try_from(size.as_millis()).unwrap_or(i64::MAX)),
            ),
            Some(WindowSpec::Session { gap, .. }) => (
                true,
                Some("session".to_string()),
                Some(i64::try_from(gap.as_millis()).unwrap_or(i64::MAX)),
            ),
            Some(WindowSpec::Rows { buffer_size, .. }) => {
                (true, Some("rows".to_string()), Some(*buffer_size as i64))
            }
            None => (false, None, None),
        }
    }

    fn extract_group_by_metadata(group_by: &Option<Vec<Expr>>) -> (bool, Option<String>) {
        match group_by {
            Some(exprs) if !exprs.is_empty() => {
                let field_names: Vec<String> = exprs
                    .iter()
                    .map(|expr| match expr {
                        Expr::Column(name) => name.clone(),
                        other => format!("{:?}", other),
                    })
                    .collect();
                (true, Some(field_names.join(",")))
            }
            _ => (false, None),
        }
    }

    fn source_name(source: &StreamSource) -> String {
        match source {
            StreamSource::Stream(name) => name.clone(),
            StreamSource::Table(name) => name.clone(),
            StreamSource::Uri(uri) => uri.clone(),
            StreamSource::Subquery(_) => "subquery".to_string(),
        }
    }
}
