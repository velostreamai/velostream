//! Test specification generator
//!
//! Analyzes SQL queries to automatically generate test specifications
//! with appropriate assertions based on query patterns.

use super::error::{TestHarnessError, TestHarnessResult};
use super::spec::{
    AggregateCheckAssertion, AggregateFunction, AssertionConfig, InputConfig,
    JoinCoverageAssertion, NoNullsAssertion, QueryTest, RecordCountAssertion, TestSpec,
};
use crate::velostream::sql::ast::{Expr, SelectField, StreamSource, StreamingQuery, WindowSpec};
use crate::velostream::sql::parser::StreamingSqlParser;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Test specification generator
pub struct SpecGenerator {
    /// Default record count for inputs
    default_record_count: usize,
    /// Default timeout in milliseconds
    default_timeout_ms: u64,
}

impl Default for SpecGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl SpecGenerator {
    /// Create new generator with default settings
    pub fn new() -> Self {
        Self {
            default_record_count: 1000,
            default_timeout_ms: 30000,
        }
    }

    /// Set default record count for inputs
    pub fn with_record_count(mut self, count: usize) -> Self {
        self.default_record_count = count;
        self
    }

    /// Set default timeout in milliseconds
    pub fn with_timeout_ms(mut self, timeout: u64) -> Self {
        self.default_timeout_ms = timeout;
        self
    }

    /// Generate test specification from SQL file
    pub fn generate_from_sql(&self, sql_file: impl AsRef<Path>) -> TestHarnessResult<TestSpec> {
        let sql_file = sql_file.as_ref();
        let content = std::fs::read_to_string(sql_file).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: sql_file.display().to_string(),
        })?;

        let name = sql_file
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "test_spec".to_string());

        self.generate_from_sql_content(&content, name)
    }

    /// Generate test specification from SQL content
    pub fn generate_from_sql_content(
        &self,
        sql_content: &str,
        name: String,
    ) -> TestHarnessResult<TestSpec> {
        let parser = StreamingSqlParser::new();
        let mut queries = Vec::new();

        // Split by semicolons to handle multiple statements
        for statement in sql_content.split(';') {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Parse each statement
            match parser.parse(trimmed) {
                Ok(query) => {
                    if let Some(test_query) = self.generate_query_test(&query)? {
                        queries.push(test_query);
                    }
                }
                Err(e) => {
                    log::debug!("Failed to parse SQL statement: {}", e);
                    // Continue with other statements
                }
            }
        }

        Ok(TestSpec {
            application: name,
            description: Some("Auto-generated test specification".to_string()),
            default_timeout_ms: self.default_timeout_ms,
            default_records: self.default_record_count,
            topic_naming: None,
            config: HashMap::new(),
            queries,
        })
    }

    /// Generate a QueryTest from a StreamingQuery
    fn generate_query_test(&self, query: &StreamingQuery) -> TestHarnessResult<Option<QueryTest>> {
        match query {
            StreamingQuery::CreateStream {
                name,
                as_select,
                properties,
                ..
            } => {
                let analysis = self.analyze_query(as_select)?;

                // Try to extract source from properties
                let source_name = self.extract_source_from_properties(properties);

                // Generate inputs based on source
                let inputs = self.generate_inputs(source_name.as_deref(), &analysis)?;

                // Generate assertions based on query patterns
                let assertions = self.generate_assertions(&analysis);

                Ok(Some(QueryTest {
                    name: name.clone(),
                    description: Some(format!("Test for stream '{}'", name)),
                    inputs,
                    output: None,
                    outputs: Vec::new(),
                    assertions,
                    timeout_ms: Some(self.default_timeout_ms),
                    skip: false,
                }))
            }

            StreamingQuery::CreateTable {
                name,
                as_select,
                properties,
                ..
            } => {
                let analysis = self.analyze_query(as_select)?;

                // Try to extract source from properties
                let source_name = self.extract_source_from_properties(properties);

                // Generate inputs based on source
                let inputs = self.generate_inputs(source_name.as_deref(), &analysis)?;

                // Generate assertions based on query patterns
                let assertions = self.generate_assertions(&analysis);

                Ok(Some(QueryTest {
                    name: name.clone(),
                    description: Some(format!("Test for table '{}'", name)),
                    inputs,
                    output: None,
                    outputs: Vec::new(),
                    assertions,
                    timeout_ms: Some(self.default_timeout_ms),
                    skip: false,
                }))
            }

            StreamingQuery::Select { .. } => {
                let analysis = self.analyze_query(query)?;
                let assertions = self.generate_assertions(&analysis);

                Ok(Some(QueryTest {
                    name: "select_query".to_string(),
                    description: Some("Test for SELECT query".to_string()),
                    inputs: Vec::new(),
                    output: None,
                    outputs: Vec::new(),
                    assertions,
                    timeout_ms: Some(self.default_timeout_ms),
                    skip: false,
                }))
            }

            _ => Ok(None),
        }
    }

    /// Extract source name from properties
    fn extract_source_from_properties(
        &self,
        properties: &HashMap<String, String>,
    ) -> Option<String> {
        // Look for topic property
        properties
            .get("topic")
            .or_else(|| properties.get("kafka.topic"))
            .cloned()
    }

    /// Extract name from StreamSource enum
    fn get_source_name(&self, source: &StreamSource) -> String {
        match source {
            StreamSource::Stream(name) => name.clone(),
            StreamSource::Table(name) => name.clone(),
            StreamSource::Uri(uri) => uri.clone(),
            StreamSource::Subquery(_) => "subquery".to_string(),
        }
    }

    /// Analyze a query to determine patterns
    fn analyze_query(&self, query: &StreamingQuery) -> TestHarnessResult<QueryAnalysis> {
        let mut analysis = QueryAnalysis::default();

        if let StreamingQuery::Select {
            fields,
            from,
            joins,
            where_clause,
            group_by,
            window,
            ..
        } = query
        {
            // Check for joins
            if let Some(joins) = joins {
                analysis.has_joins = !joins.is_empty();
                analysis.join_count = joins.len();
            }

            // Check for GROUP BY
            if let Some(gb) = group_by {
                analysis.has_group_by = true;
                analysis.group_by_fields = gb.len();
            }

            // Check for window
            if let Some(w) = window {
                analysis.has_window = true;
                analysis.window_type = Some(self.get_window_type(w));
            }

            // Analyze SELECT fields for aggregations
            for field in fields {
                if let SelectField::Expression { expr, .. } = field {
                    self.analyze_expression(expr, &mut analysis);
                }
            }

            // Check for WHERE clause
            analysis.has_where = where_clause.is_some();

            // Extract source name from FROM clause
            let source_name = self.get_source_name(from);
            analysis.sources.insert(source_name);
        }

        Ok(analysis)
    }

    /// Analyze an expression for patterns
    #[allow(clippy::only_used_in_recursion)]
    fn analyze_expression(&self, expr: &Expr, analysis: &mut QueryAnalysis) {
        match expr {
            Expr::Function { name, .. } => {
                let upper_name = name.to_uppercase();
                match upper_name.as_str() {
                    "COUNT" => {
                        analysis.has_count = true;
                        analysis.aggregate_functions.insert(upper_name);
                    }
                    "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE" => {
                        analysis.aggregate_functions.insert(upper_name);
                    }
                    "LAG" | "LEAD" | "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE"
                    | "FIRST_VALUE" | "LAST_VALUE" => {
                        analysis.has_window_functions = true;
                    }
                    _ => {}
                }
            }

            Expr::WindowFunction { function_name, .. } => {
                analysis.has_window_functions = true;
                let upper_name = function_name.to_uppercase();
                match upper_name.as_str() {
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                        analysis.aggregate_functions.insert(upper_name);
                    }
                    _ => {}
                }
            }

            Expr::BinaryOp { left, right, .. } => {
                self.analyze_expression(left, analysis);
                self.analyze_expression(right, analysis);
            }

            Expr::Case {
                when_clauses,
                else_clause,
                ..
            } => {
                for (cond, result) in when_clauses {
                    self.analyze_expression(cond, analysis);
                    self.analyze_expression(result, analysis);
                }
                if let Some(else_expr) = else_clause {
                    self.analyze_expression(else_expr, analysis);
                }
            }

            _ => {}
        }
    }

    /// Get window type from WindowSpec
    fn get_window_type(&self, window: &WindowSpec) -> String {
        match window {
            WindowSpec::Tumbling { .. } => "tumbling".to_string(),
            WindowSpec::Sliding { .. } => "sliding".to_string(),
            WindowSpec::Session { .. } => "session".to_string(),
            WindowSpec::Rows { .. } => "rows".to_string(),
        }
    }

    /// Generate input configurations
    fn generate_inputs(
        &self,
        source_name: Option<&str>,
        analysis: &QueryAnalysis,
    ) -> TestHarnessResult<Vec<InputConfig>> {
        let mut inputs = Vec::new();

        // Add input for main source if specified
        if let Some(src) = source_name {
            inputs.push(InputConfig {
                source: src.to_string(),
                source_type: None, // Default to Kafka
                // Schema name should match the 'name' field in the schema YAML, not the filename
                schema: Some(src.to_string()),
                records: Some(self.default_record_count),
                from_previous: None,
                data_file: None,
                time_simulation: None,
                key_field: None,
            });
        }

        // Add inputs for other sources detected in FROM clause
        for source in &analysis.sources {
            if !inputs.iter().any(|i| &i.source == source) {
                inputs.push(InputConfig {
                    source: source.clone(),
                    source_type: None, // Default to Kafka
                    // Schema name should match the 'name' field in the schema YAML, not the filename
                    schema: Some(source.clone()),
                    records: Some(self.default_record_count),
                    from_previous: None,
                    data_file: None,
                    time_simulation: None,
                    key_field: None,
                });
            }
        }

        Ok(inputs)
    }

    /// Generate assertions based on query patterns
    fn generate_assertions(&self, analysis: &QueryAnalysis) -> Vec<AssertionConfig> {
        let mut assertions = Vec::new();

        // Always add record count assertion
        assertions.push(AssertionConfig::RecordCount(RecordCountAssertion {
            equals: None,
            greater_than: Some(0),
            less_than: None,
            between: None,
            expression: None,
        }));

        // For aggregations, add aggregate checks
        if analysis.has_group_by || !analysis.aggregate_functions.is_empty() {
            for func in &analysis.aggregate_functions {
                let agg_func = match func.as_str() {
                    "COUNT" => AggregateFunction::Count,
                    "SUM" => AggregateFunction::Sum,
                    "AVG" => AggregateFunction::Avg,
                    "MIN" => AggregateFunction::Min,
                    "MAX" => AggregateFunction::Max,
                    _ => continue,
                };

                assertions.push(AssertionConfig::AggregateCheck(AggregateCheckAssertion {
                    function: agg_func,
                    field: "TODO_FIELD".to_string(), // Placeholder - user should update
                    expected: "TODO_VALUE".to_string(), // Placeholder - user should update
                    tolerance: Some(0.001),
                }));
            }
        }

        // For joins, add join coverage assertion
        if analysis.has_joins {
            assertions.push(AssertionConfig::JoinCoverage(JoinCoverageAssertion {
                min_match_rate: 0.5, // 50% minimum
                left_source: None,
                right_source: None,
            }));
        }

        // Add no_nulls assertion for key fields
        assertions.push(AssertionConfig::NoNulls(NoNullsAssertion {
            fields: Vec::new(), // Check all fields
        }));

        assertions
    }

    /// Write test specification to file
    pub fn write_spec(
        &self,
        spec: &TestSpec,
        output_path: impl AsRef<Path>,
    ) -> TestHarnessResult<()> {
        let yaml = serde_yaml::to_string(spec).map_err(|e| TestHarnessError::SpecParseError {
            message: format!("Failed to generate YAML: {}", e),
            file: "output".to_string(),
        })?;

        let output_path = output_path.as_ref();
        std::fs::write(output_path, yaml).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: output_path.display().to_string(),
        })?;

        Ok(())
    }
}

/// Analysis results for a query
#[derive(Debug, Default)]
struct QueryAnalysis {
    /// Whether query has joins
    has_joins: bool,
    /// Number of joins
    join_count: usize,
    /// Whether query has GROUP BY
    has_group_by: bool,
    /// Number of GROUP BY fields
    group_by_fields: usize,
    /// Whether query has window clause
    has_window: bool,
    /// Window type if present
    window_type: Option<String>,
    /// Whether query has COUNT
    has_count: bool,
    /// Set of aggregate functions used
    aggregate_functions: HashSet<String>,
    /// Whether query has window functions (LAG, LEAD, etc.)
    has_window_functions: bool,
    /// Whether query has WHERE clause
    has_where: bool,
    /// Source names from FROM clause
    sources: HashSet<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_basic_spec() {
        let sql = r#"
            CREATE STREAM output_stream AS
            SELECT symbol, price
            FROM market_data
        "#;

        let generator = SpecGenerator::new();
        let spec = generator
            .generate_from_sql_content(sql, "test".to_string())
            .unwrap();

        assert_eq!(spec.application, "test");
        assert!(!spec.queries.is_empty());
    }

    #[test]
    fn test_generate_aggregation_spec() {
        let sql = r#"
            CREATE STREAM agg_stream AS
            SELECT symbol, COUNT(*) as trade_count, SUM(volume) as total_volume
            FROM market_data
            GROUP BY symbol
        "#;

        let generator = SpecGenerator::new();
        let spec = generator
            .generate_from_sql_content(sql, "aggregation_test".to_string())
            .unwrap();

        assert_eq!(spec.queries.len(), 1);
        let query = &spec.queries[0];

        // Should have aggregate assertions
        let has_aggregate = query
            .assertions
            .iter()
            .any(|a| matches!(a, AssertionConfig::AggregateCheck(_)));
        assert!(has_aggregate);
    }

    #[test]
    fn test_generate_window_spec() {
        let sql = r#"
            CREATE STREAM windowed_stream AS
            SELECT symbol, AVG(price) as avg_price
            FROM market_data
            GROUP BY symbol
            WINDOW TUMBLING(5m)
        "#;

        let generator = SpecGenerator::new();
        let spec = generator
            .generate_from_sql_content(sql, "window_test".to_string())
            .unwrap();

        assert_eq!(spec.queries.len(), 1);
        // Should have record count assertion for window
        let has_record_count = spec.queries[0]
            .assertions
            .iter()
            .any(|a| matches!(a, AssertionConfig::RecordCount(_)));
        assert!(has_record_count);
    }
}
