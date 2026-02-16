//! DDL and Command Parsing
//!
//! This module contains parsing logic for:
//! - DDL commands (CREATE STREAM, CREATE TABLE)
//! - Job management commands (START, STOP, PAUSE, RESUME, DEPLOY, ROLLBACK)
//! - Introspection commands (SHOW, DESCRIBE)
//!
//! These parsers handle the top-level SQL commands and delegate to other
//! parser modules for sub-expressions (SELECT, window functions, clauses).

use super::common::TokenParser;
use super::*;
use crate::velostream::sql::ast::*;
use crate::velostream::sql::error::SqlError;
use crate::velostream::sql::parser::annotations::parse_metric_annotations;
use std::collections::HashMap;

impl<'a> TokenParser<'a> {
    pub(super) fn parse_create(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Create)?;

        match self.current_token().token_type {
            TokenType::Stream => self.parse_create_stream(),
            TokenType::Table => self.parse_create_table(),
            _ => Err(SqlError::ParseError {
                message: "Expected STREAM or TABLE after CREATE".to_string(),
                position: Some(self.current_token().position),
            }),
        }
    }

    pub(super) fn parse_create_stream(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Stream)?;
        let name = self.expect(TokenType::Identifier)?.value;

        // Optional column definitions
        let columns = if self.current_token().token_type == TokenType::LeftParen {
            Some(self.parse_column_definitions()?)
        } else {
            None
        };

        self.expect(TokenType::As)?;
        // Use parse_select_for_create_table to avoid consuming the WITH clause that belongs to CREATE STREAM
        let as_select = Box::new(self.parse_select_for_create_table()?);

        log::debug!(
            "CREATE STREAM parser: after SELECT, current_token={:?} (value='{}')",
            self.current_token().token_type,
            self.current_token().value
        );

        // Parse EMIT clause if present at wrapper level - must come before WITH per SQL standard
        // If not found at wrapper level, extract from inner SELECT (EMIT is part of SELECT syntax)
        let mut emit_mode = if self.current_token().token_type == TokenType::Emit {
            log::debug!("CREATE STREAM parser: EMIT token detected at wrapper level");
            self.parse_emit_clause()?
        } else {
            None
        };

        // Extract emit_mode from inner SELECT if not set at wrapper level
        if emit_mode.is_none() {
            if let StreamingQuery::Select {
                emit_mode: select_emit_mode,
                ..
            } = as_select.as_ref()
            {
                if select_emit_mode.is_some() {
                    log::debug!(
                        "CREATE STREAM parser: emit_mode extracted from inner SELECT: {:?}",
                        select_emit_mode
                    );
                    emit_mode = select_emit_mode.clone();
                }
            }
        }

        log::debug!("CREATE STREAM parser: final emit_mode={:?}", emit_mode);

        // Parse WITH properties for sink configuration (comes after EMIT)
        let mut properties = if self.current_token().token_type == TokenType::With {
            self.parse_with_properties()?
        } else {
            HashMap::new()
        };

        // Extract properties from the SELECT statement (FROM clause WITH properties)
        if let StreamingQuery::Select {
            properties: select_props,
            ..
        } = as_select.as_ref()
        {
            if let Some(select_properties) = select_props {
                // Merge SELECT properties into stream properties
                for (key, value) in select_properties {
                    properties.insert(key.clone(), value.clone());
                }
            }
        }

        // Consume optional semicolon
        self.consume_semicolon();

        // Parse metric annotations from SQL comments
        let comment_strings = self.get_comment_strings();
        let metric_annotations = parse_metric_annotations(&comment_strings)?;

        Ok(StreamingQuery::CreateStream {
            name,
            columns,
            as_select,
            properties,
            emit_mode,
            metric_annotations,
        })
    }

    pub(super) fn parse_create_table(&mut self) -> Result<StreamingQuery, SqlError> {
        self.expect(TokenType::Table)?;
        let name = self.expect(TokenType::Identifier)?.value;

        // Optional column definitions
        let columns = if self.current_token().token_type == TokenType::LeftParen {
            Some(self.parse_column_definitions()?)
        } else {
            None
        };

        self.expect(TokenType::As)?;
        // Use parse_select_for_create_table to avoid consuming WITH clause
        let as_select = Box::new(self.parse_select_for_create_table()?);

        log::debug!(
            "CREATE TABLE: after SELECT, token: {:?}",
            self.current_token().token_type
        );

        // Parse EMIT clause if present at wrapper level - must come before WITH per SQL standard
        let mut emit_mode = if self.current_token().token_type == TokenType::Emit {
            log::debug!("CREATE TABLE: EMIT detected at wrapper level");
            self.parse_emit_clause()?
        } else {
            None
        };

        // Extract emit_mode from inner SELECT if not set at wrapper level
        if emit_mode.is_none() {
            if let StreamingQuery::Select {
                emit_mode: select_emit_mode,
                ..
            } = as_select.as_ref()
            {
                if select_emit_mode.is_some() {
                    log::debug!(
                        "CREATE TABLE: emit_mode extracted from inner SELECT: {:?}",
                        select_emit_mode
                    );
                    emit_mode = select_emit_mode.clone();
                }
            }
        }

        log::debug!("CREATE TABLE: emit_mode = {:?}", emit_mode);

        // Parse WITH properties for sink configuration (comes after EMIT)
        let mut properties = if self.current_token().token_type == TokenType::With {
            self.parse_with_properties()?
        } else {
            HashMap::new()
        };

        // Extract properties from the SELECT statement (FROM clause WITH properties)
        if let StreamingQuery::Select {
            properties: select_props,
            ..
        } = as_select.as_ref()
        {
            if let Some(select_properties) = select_props {
                // Merge SELECT properties into table properties
                for (key, value) in select_properties {
                    properties.insert(key.clone(), value.clone());
                }
            }
        }

        // If no emit_mode specified at wrapper or in SELECT, check for it again after WITH
        let emit_mode = if emit_mode.is_none() && self.current_token().token_type == TokenType::Emit
        {
            self.parse_emit_clause()?
        } else {
            emit_mode
        };

        // Consume optional semicolon
        self.consume_semicolon();

        Ok(StreamingQuery::CreateTable {
            name,
            columns,
            as_select,
            properties,
            emit_mode,
        })
    }

    pub(super) fn parse_show(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume SHOW or LIST token
        self.advance();

        let resource_type = match self.current_token().token_type {
            TokenType::Streams => {
                self.advance();
                ShowResourceType::Streams
            }
            TokenType::Tables => {
                self.advance();
                ShowResourceType::Tables
            }
            TokenType::Topics => {
                self.advance();
                ShowResourceType::Topics
            }
            TokenType::Functions => {
                self.advance();
                ShowResourceType::Functions
            }
            TokenType::Jobs => {
                self.advance();
                ShowResourceType::Jobs
            }
            TokenType::Identifier if self.current_token().value.to_uppercase() == "STATUS" => {
                self.advance();
                // Optional specific query name
                let name = if self.current_token().token_type == TokenType::Identifier {
                    Some(self.expect(TokenType::Identifier)?.value)
                } else {
                    None
                };
                ShowResourceType::JobStatus { name }
            }
            TokenType::Versions => {
                self.advance();
                // Expect job name after VERSIONS
                let name = self.expect(TokenType::Identifier)?.value;
                ShowResourceType::JobVersions { name }
            }
            TokenType::Identifier if self.current_token().value.to_uppercase() == "METRICS" => {
                self.advance();
                // Optional specific job name
                let name = if self.current_token().token_type == TokenType::Identifier {
                    Some(self.expect(TokenType::Identifier)?.value)
                } else {
                    None
                };
                ShowResourceType::JobMetrics { name }
            }
            TokenType::Schema => {
                self.advance();
                // Expect resource name after SCHEMA
                let name = self.expect(TokenType::Identifier)?.value;
                ShowResourceType::Schema { name }
            }
            TokenType::Identifier if self.current_token().value.to_uppercase() == "PROPERTIES" => {
                self.advance();
                // Expect resource type and name: SHOW PROPERTIES STREAM stream_name
                // Handle both identifier and keyword tokens for resource type
                let resource_type_token = match self.current_token().token_type {
                    TokenType::Identifier => self.expect(TokenType::Identifier)?.value,
                    TokenType::Stream => {
                        let token = self.current_token().clone();
                        self.advance();
                        token.value
                    }
                    TokenType::Table => {
                        let token = self.current_token().clone();
                        self.advance();
                        token.value
                    }
                    _ => {
                        return Err(SqlError::ParseError {
                            message:
                                "Expected resource type (STREAM, TABLE, etc.) after PROPERTIES"
                                    .to_string(),
                            position: Some(self.current_token().position),
                        });
                    }
                };
                let name = self.expect(TokenType::Identifier)?.value;
                ShowResourceType::Properties {
                    resource_type: resource_type_token,
                    name,
                }
            }
            TokenType::Partitions => {
                self.advance();
                // Expect resource name after PARTITIONS
                let name = self.expect(TokenType::Identifier)?.value;
                ShowResourceType::Partitions { name }
            }
            _ => {
                return Err(SqlError::ParseError {
                    message: format!(
                        "Expected resource type (STREAMS, TABLES, TOPICS, FUNCTIONS, JOBS, VERSIONS, SCHEMA, or PARTITIONS), found {}",
                        self.current_token().value
                    ),
                    position: Some(self.current_token().position),
                });
            }
        };

        // Check for optional pattern (e.g., SHOW STREAMS 'prefix%' or SHOW STREAMS LIKE 'prefix%')
        // Consume optional LIKE keyword
        if self.current_token().token_type == TokenType::Like {
            self.advance();
        }

        let pattern = if self.current_token().token_type == TokenType::String {
            let p = self.current_token().value.clone();
            self.advance();
            Some(p)
        } else {
            None
        };

        Ok(StreamingQuery::Show {
            resource_type,
            pattern,
        })
    }

    pub(super) fn parse_start_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume START token
        self.advance();

        // Expect JOB keyword
        self.expect(TokenType::Job)?;

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        // Expect AS keyword
        self.expect(TokenType::As)?;

        // Parse the underlying query (without consuming WITH clause)
        let query = Box::new(self.parse_tokens_inner_no_with()?);

        // Optional WITH properties
        let properties = if self.current_token().token_type == TokenType::With {
            self.parse_with_properties()?
        } else {
            HashMap::new()
        };

        Ok(StreamingQuery::StartJob {
            name,
            query,
            properties,
        })
    }

    pub(super) fn parse_stop_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume STOP token
        self.advance();

        // Expect JOB keyword
        self.expect(TokenType::Job)?;

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        // Check for optional FORCE keyword
        let force = if self.current_token().token_type == TokenType::Force {
            self.advance();
            true
        } else {
            false
        };

        Ok(StreamingQuery::StopJob { name, force })
    }

    pub(super) fn parse_pause_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume PAUSE token
        self.advance();

        // Expect JOB keyword
        self.expect(TokenType::Job)?;

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        Ok(StreamingQuery::PauseJob { name })
    }

    pub(super) fn parse_resume_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume RESUME token
        self.advance();

        // Expect JOB keyword
        self.expect(TokenType::Job)?;

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        Ok(StreamingQuery::ResumeJob { name })
    }

    pub(super) fn parse_deploy_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume DEPLOY token
        self.advance();

        // Expect JOB keyword
        self.expect(TokenType::Job)?;

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        // Expect VERSION keyword
        self.expect(TokenType::Version)?;

        // Get version string
        let version = self.expect(TokenType::String)?.value;

        // Expect AS keyword
        self.expect(TokenType::As)?;

        // Parse the underlying query (without consuming WITH clause)
        let query = Box::new(self.parse_tokens_inner_no_with()?);

        // Optional WITH for properties
        let properties = if self.current_token().token_type == TokenType::With {
            self.parse_with_properties()?
        } else {
            HashMap::new()
        };

        // Optional STRATEGY for deployment strategy
        let strategy = if self.current_token().token_type == TokenType::Strategy {
            self.advance(); // consume STRATEGY
            match self.current_token().token_type {
                TokenType::BlueGreen => {
                    self.advance();
                    DeploymentStrategy::BlueGreen
                }
                TokenType::Canary => {
                    self.advance();
                    // Expect percentage in parentheses
                    self.expect(TokenType::LeftParen)?;
                    let percentage_token = self.expect(TokenType::Number)?;
                    let percentage: u8 =
                        percentage_token
                            .value
                            .parse()
                            .map_err(|_| SqlError::ParseError {
                                message: "Invalid percentage for canary deployment".to_string(),
                                position: Some(percentage_token.position),
                            })?;
                    self.expect(TokenType::RightParen)?;
                    DeploymentStrategy::Canary { percentage }
                }
                TokenType::Rolling => {
                    self.advance();
                    DeploymentStrategy::Rolling
                }
                TokenType::Replace => {
                    self.advance();
                    DeploymentStrategy::Replace
                }
                _ => {
                    return Err(SqlError::ParseError {
                        message: "Expected BLUE_GREEN, CANARY, ROLLING, or REPLACE strategy"
                            .to_string(),
                        position: Some(self.current_token().position),
                    });
                }
            }
        } else {
            DeploymentStrategy::BlueGreen // Default strategy
        };

        Ok(StreamingQuery::DeployJob {
            name,
            version,
            query,
            properties,
            strategy,
        })
    }

    pub(super) fn parse_rollback_job(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume ROLLBACK token
        self.advance();

        // Expect JOB keyword
        self.expect(TokenType::Job)?;

        // Get job name
        let name = self.expect(TokenType::Identifier)?.value;

        // Optional target version
        let target_version = if self.current_token().token_type == TokenType::Version {
            self.advance(); // consume VERSION
            Some(self.expect(TokenType::String)?.value)
        } else {
            None
        };

        Ok(StreamingQuery::RollbackJob {
            name,
            target_version,
        })
    }

    pub(super) fn parse_describe(&mut self) -> Result<StreamingQuery, SqlError> {
        // Consume DESCRIBE token
        self.advance();

        // Get resource name
        let name = self.expect(TokenType::Identifier)?.value;

        Ok(StreamingQuery::Show {
            resource_type: ShowResourceType::Describe { name },
            pattern: None,
        })
    }

    pub(super) fn parse_tokens_inner(&mut self) -> Result<StreamingQuery, SqlError> {
        // This is similar to the main parse_tokens but works on the current parser state
        match self.current_token().token_type {
            TokenType::Select => self.parse_select(),
            TokenType::Create => self.parse_create(),
            _ => Err(SqlError::ParseError {
                message: "Expected SELECT or CREATE statement in START QUERY".to_string(),
                position: Some(self.current_token().position),
            }),
        }
    }

    pub(super) fn parse_tokens_inner_no_with(&mut self) -> Result<StreamingQuery, SqlError> {
        // Similar to parse_tokens_inner but doesn't consume WITH clauses (for job contexts)
        match self.current_token().token_type {
            TokenType::Select => self.parse_select_no_with(),
            TokenType::Create => self.parse_create(),
            _ => Err(SqlError::ParseError {
                message: "Expected SELECT or CREATE statement in job definition".to_string(),
                position: Some(self.current_token().position),
            }),
        }
    }
}
