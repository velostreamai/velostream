/*!
# SQL Application Parser

This module provides functionality to parse and manage SQL applications - collections
of related SQL statements that work together as a cohesive unit. SQL applications
can be defined in .sql files with multiple statements and metadata.

## Features

- **Multi-Statement Parsing**: Parse multiple SQL statements from a single file
- **Application Metadata**: Extract application name, version, dependencies
- **Statement Dependencies**: Track relationships between statements
- **Deployment Order**: Determine correct execution order for related statements
- **Resource Management**: Track streams, tables, and jobs created by the application

## SQL Application Format

```sql
-- SQL Application: E-commerce Analytics
-- Version: 1.2.0
-- Description: Complete e-commerce data processing pipeline
-- Author: Analytics Team
-- Dependencies: kafka-orders, kafka-users, kafka-products

-- Create base streams
CREATE STREAM raw_orders AS
SELECT * FROM orders_topic;

CREATE STREAM raw_users AS
SELECT * FROM users_topic;

-- Create enriched data streams
CREATE STREAM enriched_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    u.user_name,
    u.user_tier
FROM raw_orders o
JOIN raw_users u ON o.customer_id = u.user_id;

-- Deploy processing jobs
START JOB high_value_orders AS
SELECT * FROM enriched_orders WHERE amount > 1000
WITH ('replicas' = '3');

START JOB user_analytics AS
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent
FROM enriched_orders
GROUP BY customer_id
WINDOW TUMBLING(1h);
```
*/

use crate::ferris::sql::ast::StreamSource;
use crate::ferris::sql::{SqlError, StreamingQuery, StreamingSqlParser};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a complete SQL application with metadata and multiple statements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlApplication {
    pub metadata: ApplicationMetadata,
    pub statements: Vec<SqlStatement>,
    pub resources: ApplicationResources,
}

/// Metadata for a SQL application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationMetadata {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub author: Option<String>,
    pub dependencies: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub tags: HashMap<String, String>,
}

/// Individual SQL statement within an application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlStatement {
    pub id: String,
    pub name: Option<String>,
    pub statement_type: StatementType,
    pub sql: String,
    pub dependencies: Vec<String>,
    pub order: usize,
    pub properties: HashMap<String, String>,
}

/// Type of SQL statement
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StatementType {
    CreateStream,
    CreateTable,
    StartJob,
    DeployJob,
    Select,
    Show,
    Other(String),
}

/// Resources created or used by the application
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct ApplicationResources {
    pub streams: Vec<String>,
    pub tables: Vec<String>,
    pub jobs: Vec<String>,
    pub topics: Vec<String>,
}


/// Parser for SQL application files
pub struct SqlApplicationParser {
    sql_parser: StreamingSqlParser,
}

impl SqlApplicationParser {
    pub fn new() -> Self {
        Self {
            sql_parser: StreamingSqlParser::new(),
        }
    }

    /// Parse a SQL application from file content
    pub fn parse_application(&self, content: &str) -> Result<SqlApplication, SqlError> {
        let mut metadata = self.extract_metadata(content)?;
        let statements = self.parse_statements(content)?;
        let resources = self.extract_resources(&statements);

        // Set created timestamp if not specified
        if metadata.created_at == DateTime::<Utc>::MIN_UTC {
            metadata.created_at = Utc::now();
        }

        Ok(SqlApplication {
            metadata,
            statements,
            resources,
        })
    }

    /// Extract metadata from SQL comments
    fn extract_metadata(&self, content: &str) -> Result<ApplicationMetadata, SqlError> {
        let mut name = String::new();
        let mut version = String::new();
        let mut description = None;
        let mut author = None;
        let mut dependencies = Vec::new();
        let mut tags = HashMap::new();

        for line in content.lines() {
            let line = line.trim();
            if line.starts_with("-- SQL Application:") {
                name = line.replace("-- SQL Application:", "").trim().to_string();
            } else if line.starts_with("-- Version:") {
                version = line.replace("-- Version:", "").trim().to_string();
            } else if line.starts_with("-- Description:") {
                description = Some(line.replace("-- Description:", "").trim().to_string());
            } else if line.starts_with("-- Author:") {
                author = Some(line.replace("-- Author:", "").trim().to_string());
            } else if line.starts_with("-- Dependencies:") {
                let deps_str = line.replace("-- Dependencies:", "");
                let deps_str = deps_str.trim();
                dependencies = deps_str
                    .split(',')
                    .map(|d| d.trim().to_string())
                    .filter(|d| !d.is_empty())
                    .collect();
            } else if line.starts_with("-- Tag:") {
                let tag_str = line.replace("-- Tag:", "");
                let tag_str = tag_str.trim();
                if let Some((key, value)) = tag_str.split_once(':') {
                    tags.insert(key.trim().to_string(), value.trim().to_string());
                }
            }
        }

        if name.is_empty() {
            return Err(SqlError::ParseError {
                message: "SQL Application must have a name (-- SQL Application: <name>)"
                    .to_string(),
                position: None,
            });
        }

        if version.is_empty() {
            version = "1.0.0".to_string();
        }

        Ok(ApplicationMetadata {
            name,
            version,
            description,
            author,
            dependencies,
            created_at: Utc::now(),
            tags,
        })
    }

    /// Parse individual SQL statements
    fn parse_statements(&self, content: &str) -> Result<Vec<SqlStatement>, SqlError> {
        let mut statements = Vec::new();
        let mut current_statement = String::new();
        let mut statement_counter = 0;
        let mut current_name = None;
        let mut current_properties = HashMap::new();

        for line in content.lines() {
            let trimmed = line.trim();

            // Skip metadata comments
            if trimmed.starts_with("-- SQL Application:")
                || trimmed.starts_with("-- Version:")
                || trimmed.starts_with("-- Description:")
                || trimmed.starts_with("-- Author:")
                || trimmed.starts_with("-- Dependencies:")
                || trimmed.starts_with("-- Tag:")
            {
                continue;
            }

            // Extract statement name from comments
            if trimmed.starts_with("-- Name:") {
                current_name = Some(trimmed.replace("-- Name:", "").trim().to_string());
                continue;
            }

            // Extract statement properties
            if trimmed.starts_with("-- Property:") {
                let prop_str = trimmed.replace("-- Property:", "");
                let prop_str = prop_str.trim();
                if let Some((key, value)) = prop_str.split_once('=') {
                    current_properties.insert(key.trim().to_string(), value.trim().to_string());
                }
                continue;
            }

            // Skip empty lines and regular comments
            if trimmed.is_empty()
                || (trimmed.starts_with("--")
                    && !trimmed.starts_with("-- Name:")
                    && !trimmed.starts_with("-- Property:"))
            {
                if !current_statement.trim().is_empty() {
                    current_statement.push('\n');
                }
                continue;
            }

            current_statement.push_str(line);
            current_statement.push('\n');

            // Check if statement is complete (ends with semicolon)
            if trimmed.ends_with(';') {
                let sql = current_statement.trim().trim_end_matches(';').to_string();

                if !sql.is_empty() {
                    // Parse the SQL statement for type determination
                    let parsed_query = self.sql_parser.parse(&sql).ok();

                    let statement_type = self.determine_statement_type(&sql, &parsed_query);
                    let dependencies = self.extract_dependencies(&sql);

                    statements.push(SqlStatement {
                        id: format!("stmt_{}", statement_counter),
                        name: current_name.take(),
                        statement_type,
                        sql,
                        dependencies,
                        order: statement_counter,
                        properties: current_properties.clone(),
                    });

                    statement_counter += 1;
                    current_properties.clear();
                }

                current_statement.clear();
            }
        }

        // Handle any remaining statement without semicolon
        if !current_statement.trim().is_empty() {
            let sql = current_statement.trim().to_string();
            let parsed_query = self.sql_parser.parse(&sql).ok();
            let statement_type = self.determine_statement_type(&sql, &parsed_query);
            let dependencies = self.extract_dependencies(&sql);

            statements.push(SqlStatement {
                id: format!("stmt_{}", statement_counter),
                name: current_name,
                statement_type,
                sql,
                dependencies,
                order: statement_counter,
                properties: current_properties,
            });
        }

        Ok(statements)
    }

    /// Determine the type of SQL statement
    fn determine_statement_type(
        &self,
        sql: &str,
        parsed_query: &Option<StreamingQuery>,
    ) -> StatementType {
        if let Some(query) = parsed_query {
            match query {
                StreamingQuery::CreateStream { .. } => StatementType::CreateStream,
                StreamingQuery::CreateTable { .. } => StatementType::CreateTable,
                StreamingQuery::StartJob { .. } => StatementType::StartJob,
                StreamingQuery::DeployJob { .. } => StatementType::DeployJob,
                StreamingQuery::Select { .. } => StatementType::Select,
                StreamingQuery::Show { .. } => StatementType::Show,
                _ => StatementType::Other("Unknown".to_string()),
            }
        } else {
            // Fallback to text analysis
            let upper_sql = sql.to_uppercase();
            if upper_sql.contains("CREATE STREAM") {
                StatementType::CreateStream
            } else if upper_sql.contains("CREATE TABLE") {
                StatementType::CreateTable
            } else if upper_sql.contains("START JOB") {
                StatementType::StartJob
            } else if upper_sql.contains("DEPLOY JOB") {
                StatementType::DeployJob
            } else if upper_sql.starts_with("SELECT") {
                StatementType::Select
            } else if upper_sql.starts_with("SHOW") {
                StatementType::Show
            } else {
                StatementType::Other("Unknown".to_string())
            }
        }
    }

    /// Extract dependencies from SQL statement using AST analysis
    fn extract_dependencies(&self, sql: &str) -> Vec<String> {
        let mut dependencies = Vec::new();

        // Try to parse the SQL statement and extract dependencies from the AST
        if let Ok(parsed_query) = self.sql_parser.parse(sql) {
            self.extract_dependencies_from_ast(&parsed_query, &mut dependencies);
        } else {
            // Fallback to text-based extraction if parsing fails
            dependencies = self.extract_dependencies_text_fallback(sql);
        }

        dependencies.sort();
        dependencies.dedup();
        dependencies
    }

    /// Extract dependencies from parsed AST
    fn extract_dependencies_from_ast(
        &self,
        query: &StreamingQuery,
        dependencies: &mut Vec<String>,
    ) {
        use crate::ferris::sql::ast::{InsertSource, StreamingQuery};

        match query {
            StreamingQuery::Select { from, joins, .. } => {
                // Extract FROM clause dependencies
                self.extract_source_dependencies(from, dependencies);

                // Extract JOIN clause dependencies
                if let Some(join_clauses) = joins {
                    for join in join_clauses {
                        self.extract_source_dependencies(&join.right_source, dependencies);
                    }
                }
            }
            StreamingQuery::CreateStream { as_select, .. } => {
                // Extract dependencies from the SELECT query that creates the stream
                self.extract_dependencies_from_ast(as_select, dependencies);
            }
            StreamingQuery::CreateTable { as_select, .. } => {
                // Extract dependencies from the SELECT query that creates the table
                self.extract_dependencies_from_ast(as_select, dependencies);
            }
            StreamingQuery::StartJob { query, .. } => {
                // Extract dependencies from the job query
                self.extract_dependencies_from_ast(query, dependencies);
            }
            StreamingQuery::DeployJob { query, .. } => {
                // Extract dependencies from the job query
                self.extract_dependencies_from_ast(query, dependencies);
            }
            StreamingQuery::InsertInto { source, .. } => {
                // The table being inserted into is not a dependency, but the source query is
                match source {
                    InsertSource::Select { query } => {
                        self.extract_dependencies_from_ast(query, dependencies);
                    }
                    InsertSource::Values { .. } => {
                        // VALUES clauses don't have dependencies
                    }
                }
            }
            // Other query types don't typically have dependencies we track
            _ => {}
        }
    }

    /// Extract dependencies from a StreamSource
    fn extract_source_dependencies(&self, source: &StreamSource, dependencies: &mut Vec<String>) {
        match source {
            StreamSource::Stream(name) => {
                dependencies.push(name.clone());
            }
            StreamSource::Table(name) => {
                dependencies.push(name.clone());
            }
            StreamSource::Subquery(subquery) => {
                // Recursively extract dependencies from subquery
                self.extract_dependencies_from_ast(subquery, dependencies);
            }
        }
    }

    /// Fallback text-based dependency extraction for unparseable SQL
    fn extract_dependencies_text_fallback(&self, sql: &str) -> Vec<String> {
        let mut dependencies = Vec::new();
        let words: Vec<&str> = sql.split_whitespace().collect();
        let mut i = 0;

        while i < words.len() {
            let word = words[i].to_uppercase();

            // Look for FROM clauses
            if word == "FROM" && i + 1 < words.len() {
                let table_name = words[i + 1].trim_end_matches(',').trim_end_matches(';');
                dependencies.push(table_name.to_string());
            }

            // Look for JOIN clauses
            if word == "JOIN" && i + 1 < words.len() {
                let table_name = words[i + 1].trim_end_matches(',').trim_end_matches(';');
                dependencies.push(table_name.to_string());
            }

            i += 1;
        }

        dependencies
    }

    /// Extract resources created by the application
    fn extract_resources(&self, statements: &[SqlStatement]) -> ApplicationResources {
        let mut resources = ApplicationResources::default();

        for stmt in statements {
            match stmt.statement_type {
                StatementType::CreateStream => {
                    if let Some(name) = self.extract_create_name(&stmt.sql, "STREAM") {
                        resources.streams.push(name);
                    }
                }
                StatementType::CreateTable => {
                    if let Some(name) = self.extract_create_name(&stmt.sql, "TABLE") {
                        resources.tables.push(name);
                    }
                }
                StatementType::StartJob | StatementType::DeployJob => {
                    if let Some(name) = self.extract_job_name(&stmt.sql) {
                        resources.jobs.push(name);
                    }
                }
                _ => {}
            }
        }

        resources
    }

    /// Extract name from CREATE statements
    fn extract_create_name(&self, sql: &str, create_type: &str) -> Option<String> {
        let pattern = format!("CREATE {} ", create_type);
        if let Some(start) = sql.to_uppercase().find(&pattern.to_uppercase()) {
            let after_create = &sql[start + pattern.len()..];
            let words: Vec<&str> = after_create.split_whitespace().collect();
            if !words.is_empty() {
                return Some(words[0].to_string());
            }
        }
        None
    }

    /// Extract job name from START JOB or DEPLOY JOB statements
    fn extract_job_name(&self, sql: &str) -> Option<String> {
        let upper_sql = sql.to_uppercase();

        if let Some(start) = upper_sql.find("START JOB ") {
            let after_start = &sql[start + 10..];
            let words: Vec<&str> = after_start.split_whitespace().collect();
            if !words.is_empty() {
                return Some(words[0].to_string());
            }
        }

        if let Some(start) = upper_sql.find("DEPLOY JOB ") {
            let after_deploy = &sql[start + 11..];
            let words: Vec<&str> = after_deploy.split_whitespace().collect();
            if !words.is_empty() {
                return Some(words[0].to_string());
            }
        }

        None
    }
}

impl Default for SqlApplicationParser {
    fn default() -> Self {
        Self::new()
    }
}
