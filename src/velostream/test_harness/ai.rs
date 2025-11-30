//! AI-powered features using Claude API
//!
//! Provides intelligent assistance for:
//! - Schema inference from SQL and data files
//! - Failure analysis and fix suggestions
//! - Test specification generation

use super::assertions::AssertionResult;
use super::error::{TestHarnessError, TestHarnessResult};
use super::schema::Schema;
use super::spec::TestSpec;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Claude API endpoint
const CLAUDE_API_URL: &str = "https://api.anthropic.com/v1/messages";

/// Default model to use
const DEFAULT_MODEL: &str = "claude-sonnet-4-20250514";

/// AI assistant for test harness
pub struct AiAssistant {
    /// API key
    api_key: Option<String>,

    /// Model to use
    model: String,

    /// Whether AI is enabled
    enabled: bool,

    /// HTTP client
    client: Client,

    /// Max tokens for response
    max_tokens: u32,
}

/// Claude API request
#[derive(Debug, Serialize)]
struct ClaudeRequest {
    model: String,
    max_tokens: u32,
    messages: Vec<ClaudeMessage>,
}

/// Claude message
#[derive(Debug, Serialize, Deserialize)]
struct ClaudeMessage {
    role: String,
    content: String,
}

/// Claude API response
#[derive(Debug, Deserialize)]
struct ClaudeResponse {
    content: Vec<ContentBlock>,
    #[serde(default)]
    stop_reason: Option<String>,
}

/// Content block in response
#[derive(Debug, Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    content_type: String,
    text: String,
}

/// Context for AI analysis
#[derive(Debug)]
pub struct AnalysisContext {
    /// Query SQL
    pub query_sql: String,

    /// Query name
    pub query_name: String,

    /// Input data samples
    pub input_samples: Vec<String>,

    /// Output data samples
    pub output_samples: Vec<String>,

    /// Assertion that failed
    pub assertion: Option<AssertionResult>,
}

/// Result of AI analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiAnalysis {
    /// Summary explanation
    pub summary: String,

    /// Root cause analysis
    pub root_cause: Option<String>,

    /// Suggested fixes
    pub suggestions: Vec<String>,

    /// Confidence level (0.0 to 1.0)
    pub confidence: f64,
}

impl AiAssistant {
    /// Create new AI assistant
    pub fn new() -> Self {
        let api_key = std::env::var("ANTHROPIC_API_KEY").ok();
        let enabled = api_key.is_some();

        Self {
            api_key,
            model: DEFAULT_MODEL.to_string(),
            enabled,
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap_or_default(),
            max_tokens: 4096,
        }
    }

    /// Check if AI is available
    pub fn is_available(&self) -> bool {
        self.enabled && self.api_key.is_some()
    }

    /// Set custom model
    pub fn with_model(mut self, model: &str) -> Self {
        self.model = model.to_string();
        self
    }

    /// Set max tokens
    pub fn with_max_tokens(mut self, tokens: u32) -> Self {
        self.max_tokens = tokens;
        self
    }

    /// Call Claude API
    async fn call_claude(&self, prompt: &str) -> TestHarnessResult<String> {
        let api_key = self
            .api_key
            .as_ref()
            .ok_or_else(|| TestHarnessError::AiError {
                message: "Missing ANTHROPIC_API_KEY environment variable".to_string(),
                source: None,
            })?;

        let request = ClaudeRequest {
            model: self.model.clone(),
            max_tokens: self.max_tokens,
            messages: vec![ClaudeMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            }],
        };

        let response = self
            .client
            .post(CLAUDE_API_URL)
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| TestHarnessError::AiError {
                message: format!("Failed to call Claude API: {}", e),
                source: Some(e.to_string()),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(TestHarnessError::AiError {
                message: format!("Claude API error ({}): {}", status, error_text),
                source: None,
            });
        }

        let claude_response: ClaudeResponse =
            response
                .json()
                .await
                .map_err(|e| TestHarnessError::AiError {
                    message: format!("Failed to parse Claude response: {}", e),
                    source: Some(e.to_string()),
                })?;

        // Extract text from response
        let text = claude_response
            .content
            .iter()
            .filter(|c| c.content_type == "text")
            .map(|c| c.text.clone())
            .collect::<Vec<_>>()
            .join("");

        Ok(text)
    }

    /// Infer schema from SQL and CSV samples
    pub async fn infer_schema(
        &self,
        sql_content: &str,
        csv_samples: &[CsvSample],
    ) -> TestHarnessResult<Schema> {
        if !self.is_available() {
            return Err(TestHarnessError::AiError {
                message: "AI not available (missing ANTHROPIC_API_KEY)".to_string(),
                source: None,
            });
        }

        let prompt = self.build_schema_prompt(sql_content, csv_samples);
        let response = self.call_claude(&prompt).await?;

        // Parse the YAML schema from response
        self.parse_schema_response(&response)
    }

    /// Parse schema from AI response
    fn parse_schema_response(&self, response: &str) -> TestHarnessResult<Schema> {
        // Extract YAML block from response
        let yaml_content = if let Some(start) = response.find("```yaml") {
            let start = start + 7;
            if let Some(end) = response[start..].find("```") {
                &response[start..start + end]
            } else {
                response
            }
        } else if let Some(start) = response.find("```") {
            let start = start + 3;
            if let Some(end) = response[start..].find("```") {
                &response[start..start + end]
            } else {
                response
            }
        } else {
            response
        };

        // Try to parse as YAML
        serde_yaml::from_str(yaml_content.trim()).map_err(|e| TestHarnessError::AiError {
            message: format!("Failed to parse AI-generated schema: {}", e),
            source: Some(e.to_string()),
        })
    }

    /// Analyze assertion failure
    pub async fn analyze_failure(
        &self,
        context: &AnalysisContext,
    ) -> TestHarnessResult<AiAnalysis> {
        if !self.is_available() {
            return Err(TestHarnessError::AiError {
                message: "AI not available (missing ANTHROPIC_API_KEY)".to_string(),
                source: None,
            });
        }

        let prompt = self.build_failure_prompt(context);
        let response = self.call_claude(&prompt).await?;

        // Parse the analysis from response
        self.parse_analysis_response(&response)
    }

    /// Parse analysis from AI response
    fn parse_analysis_response(&self, response: &str) -> TestHarnessResult<AiAnalysis> {
        // Try to extract structured information from response
        let lines: Vec<&str> = response.lines().collect();

        let mut summary = String::new();
        let mut root_cause = None;
        let mut suggestions = Vec::new();
        let mut confidence = 0.7; // Default confidence

        let mut current_section = "";

        for line in lines {
            let line = line.trim();

            if line.starts_with("## Summary") || line.starts_with("**Summary**") {
                current_section = "summary";
            } else if line.starts_with("## Root Cause") || line.starts_with("**Root Cause**") {
                current_section = "root_cause";
            } else if line.starts_with("## Suggestions")
                || line.starts_with("**Suggestions**")
                || line.starts_with("## Fixes")
            {
                current_section = "suggestions";
            } else if line.starts_with("## Confidence") || line.starts_with("**Confidence**") {
                current_section = "confidence";
            } else if !line.is_empty() && !line.starts_with('#') {
                match current_section {
                    "summary" => {
                        if !summary.is_empty() {
                            summary.push(' ');
                        }
                        summary.push_str(line);
                    }
                    "root_cause" => {
                        if root_cause.is_none() {
                            root_cause = Some(line.to_string());
                        }
                    }
                    "suggestions" => {
                        let suggestion = line
                            .trim_start_matches(|c: char| c.is_numeric() || c == '.' || c == '-')
                            .trim();
                        if !suggestion.is_empty() {
                            suggestions.push(suggestion.to_string());
                        }
                    }
                    "confidence" => {
                        // Try to parse confidence percentage
                        if let Some(pct) = line.find('%') {
                            let num_str: String = line[..pct]
                                .chars()
                                .filter(|c| c.is_numeric() || *c == '.')
                                .collect();
                            if let Ok(pct_val) = num_str.parse::<f64>() {
                                confidence = pct_val / 100.0;
                            }
                        }
                    }
                    _ => {
                        // Default to summary if no section identified
                        if summary.is_empty() {
                            summary = line.to_string();
                        }
                    }
                }
            }
        }

        // If we couldn't parse structured response, use the whole thing as summary
        if summary.is_empty() {
            summary = response.lines().take(3).collect::<Vec<_>>().join(" ");
        }

        Ok(AiAnalysis {
            summary,
            root_cause,
            suggestions,
            confidence,
        })
    }

    /// Generate test specification from SQL
    #[allow(unused_variables)]
    pub async fn generate_test_spec(
        &self,
        sql_content: &str,
        app_name: &str,
    ) -> TestHarnessResult<TestSpec> {
        if !self.is_available() {
            return Err(TestHarnessError::AiError {
                message: "AI not available (missing ANTHROPIC_API_KEY)".to_string(),
                source: None,
            });
        }

        let prompt = self.build_test_spec_prompt(sql_content, app_name);
        let response = self.call_claude(&prompt).await?;

        // Parse the test spec from response
        self.parse_test_spec_response(&response, app_name)
    }

    /// Parse test spec from AI response
    #[allow(unused_variables)]
    fn parse_test_spec_response(
        &self,
        response: &str,
        app_name: &str,
    ) -> TestHarnessResult<TestSpec> {
        // Extract YAML block from response
        let yaml_content = if let Some(start) = response.find("```yaml") {
            let start = start + 7;
            if let Some(end) = response[start..].find("```") {
                &response[start..start + end]
            } else {
                response
            }
        } else if let Some(start) = response.find("```") {
            let start = start + 3;
            if let Some(end) = response[start..].find("```") {
                &response[start..start + end]
            } else {
                response
            }
        } else {
            response
        };

        // Try to parse as YAML
        serde_yaml::from_str(yaml_content.trim()).map_err(|e| TestHarnessError::AiError {
            message: format!("Failed to parse AI-generated test spec: {}", e),
            source: Some(e.to_string()),
        })
    }

    /// Build prompt for schema inference
    fn build_schema_prompt(&self, sql: &str, samples: &[CsvSample]) -> String {
        let mut prompt = String::new();

        prompt.push_str("You are an expert data engineer. Analyze the following SQL queries and CSV data samples to generate a YAML schema for test data generation.\n\n");

        prompt.push_str("## SQL Queries\n```sql\n");
        prompt.push_str(sql);
        prompt.push_str("\n```\n\n");

        if !samples.is_empty() {
            prompt.push_str("## CSV Data Samples\n");
            for sample in samples {
                prompt.push_str(&format!("### {}\n```csv\n", sample.name));
                prompt.push_str(&sample.content);
                prompt.push_str("\n```\n\n");
            }
        }

        prompt.push_str(
            r#"Generate a YAML schema with this exact structure:
```yaml
name: <source_name>
description: <description>
record_count: 1000
fields:
  - name: <field_name>
    type: <string|integer|float|boolean|timestamp|date|uuid|decimal>
    nullable: false
    constraints:
      range:
        min: <number>
        max: <number>
      enum_values:
        values: [<value1>, <value2>]
        weights: [<weight1>, <weight2>]
      length:
        min: <number>
        max: <number>
    description: <field description>
```

Requirements:
1. Infer field types from SQL column usage and CSV data
2. Add appropriate constraints based on observed data patterns
3. Include realistic ranges for numeric fields
4. Add enum constraints for categorical fields
5. Set nullable appropriately based on NULL handling in SQL

Return ONLY the YAML schema, no explanations.
"#,
        );

        prompt
    }

    /// Build prompt for failure analysis
    fn build_failure_prompt(&self, context: &AnalysisContext) -> String {
        let mut prompt = String::new();

        prompt.push_str("You are an expert streaming SQL debugger. Analyze the following test failure and provide detailed analysis.\n\n");

        prompt.push_str(&format!("## Query: {}\n```sql\n", context.query_name));
        prompt.push_str(&context.query_sql);
        prompt.push_str("\n```\n\n");

        if !context.input_samples.is_empty() {
            prompt.push_str("## Input Data Samples\n```json\n");
            for sample in &context.input_samples {
                prompt.push_str(sample);
                prompt.push('\n');
            }
            prompt.push_str("```\n\n");
        }

        if !context.output_samples.is_empty() {
            prompt.push_str("## Output Data Samples\n```json\n");
            for sample in &context.output_samples {
                prompt.push_str(sample);
                prompt.push('\n');
            }
            prompt.push_str("```\n\n");
        }

        if let Some(ref assertion) = context.assertion {
            prompt.push_str(&format!(
                "## Failed Assertion: {}\n",
                assertion.assertion_type
            ));
            prompt.push_str(&format!("Message: {}\n", assertion.message));
            if let Some(ref expected) = assertion.expected {
                prompt.push_str(&format!("Expected: {}\n", expected));
            }
            if let Some(ref actual) = assertion.actual {
                prompt.push_str(&format!("Actual: {}\n", actual));
            }
        }

        prompt.push_str(
            r#"
Provide your analysis in this format:

## Summary
<One sentence summary of the issue>

## Root Cause
<Detailed explanation of why the test failed>

## Suggestions
1. <First fix suggestion>
2. <Second fix suggestion>
3. <Third fix suggestion if applicable>

## Confidence
<Your confidence percentage, e.g., 85%>
"#,
        );

        prompt
    }

    /// Build prompt for test spec generation
    fn build_test_spec_prompt(&self, sql: &str, app_name: &str) -> String {
        let mut prompt = String::new();

        prompt.push_str("You are an expert test engineer. Generate a comprehensive test specification for the following SQL streaming application.\n\n");

        prompt.push_str(&format!("## Application: {}\n\n", app_name));

        prompt.push_str("## SQL Queries\n```sql\n");
        prompt.push_str(sql);
        prompt.push_str("\n```\n\n");

        prompt.push_str(
            r#"Generate a YAML test specification with this structure:
```yaml
application: <app_name>
description: <description>
default_timeout_ms: 30000
default_records: 1000
config: {}
queries:
  - name: <query_name>
    description: <description>
    skip: false
    inputs:
      - source: <source_name>
        schema: <source_name>.schema.yaml
        records: 1000
    assertions:
      - type: record_count
        greater_than: 0
      - type: no_nulls
        fields: []
      - type: aggregate_check
        function: <COUNT|SUM|AVG|MIN|MAX>
        field: <field_name>
        expected: <value>
        tolerance: 0.001
    timeout_ms: 30000
```

For each query in the SQL:
1. Identify the query name from CREATE STREAM/TABLE statements
2. Identify input sources from FROM clauses
3. Generate appropriate assertions based on:
   - Aggregation functions → aggregate_check assertions
   - GROUP BY → expect multiple output records
   - JOINs → join_coverage assertions
   - WHERE clauses → field_values assertions
4. Set realistic timeouts based on query complexity

Return ONLY the YAML test spec, no explanations.
"#,
        );

        prompt
    }
}

impl Default for AiAssistant {
    fn default() -> Self {
        Self::new()
    }
}

/// CSV sample for schema inference
#[derive(Debug)]
pub struct CsvSample {
    /// Source name
    pub name: String,

    /// CSV content (first N rows)
    pub content: String,

    /// Number of total rows in file
    pub total_rows: usize,
}

impl CsvSample {
    /// Create from file path
    pub fn from_file(
        path: impl AsRef<std::path::Path>,
        max_rows: usize,
    ) -> TestHarnessResult<Self> {
        let path = path.as_ref();
        let name = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let content = std::fs::read_to_string(path).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: path.display().to_string(),
        })?;

        let lines: Vec<&str> = content.lines().collect();
        let total_rows = lines.len().saturating_sub(1); // Exclude header

        let sample_content = lines
            .iter()
            .take(max_rows + 1) // Include header
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");

        Ok(Self {
            name,
            content: sample_content,
            total_rows,
        })
    }
}

/// Format AI analysis for display
pub fn format_ai_analysis(analysis: &AiAnalysis) -> String {
    let mut output = String::new();

    output.push_str("🤖 AI Analysis:\n");
    output.push_str(&format!("   {}\n", analysis.summary));

    if let Some(ref root_cause) = analysis.root_cause {
        output.push_str(&format!("\n   Root Cause: {}\n", root_cause));
    }

    if !analysis.suggestions.is_empty() {
        output.push_str("\n   Suggestions:\n");
        for (i, suggestion) in analysis.suggestions.iter().enumerate() {
            output.push_str(&format!("   {}. {}\n", i + 1, suggestion));
        }
    }

    output.push_str(&format!(
        "\n   Confidence: {:.0}%\n",
        analysis.confidence * 100.0
    ));

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ai_assistant_creation() {
        let assistant = AiAssistant::new();
        // Should be disabled without API key
        assert!(!assistant.is_available() || std::env::var("ANTHROPIC_API_KEY").is_ok());
    }

    #[test]
    fn test_ai_assistant_with_model() {
        let assistant = AiAssistant::new().with_model("claude-3-opus-20240229");
        assert_eq!(assistant.model, "claude-3-opus-20240229");
    }

    #[test]
    fn test_parse_analysis_response() {
        let assistant = AiAssistant::new();
        let response = r#"
## Summary
The test failed because no output records were produced.

## Root Cause
The window duration is too long for the test data.

## Suggestions
1. Reduce the window size from 5 minutes to 1 minute
2. Increase the test data volume
3. Add more events within the same window

## Confidence
85%
"#;

        let analysis = assistant.parse_analysis_response(response).unwrap();
        assert!(analysis.summary.contains("no output records"));
        assert!(analysis.root_cause.is_some());
        assert!(!analysis.suggestions.is_empty());
        assert!((analysis.confidence - 0.85).abs() < 0.01);
    }

    #[test]
    fn test_build_schema_prompt() {
        let assistant = AiAssistant::new();
        let sql = "SELECT symbol, price FROM market_data";
        let samples = vec![CsvSample {
            name: "market_data".to_string(),
            content: "symbol,price\nAAPL,150.0\nGOOG,2800.0".to_string(),
            total_rows: 2,
        }];

        let prompt = assistant.build_schema_prompt(sql, &samples);
        assert!(prompt.contains("symbol"));
        assert!(prompt.contains("price"));
        assert!(prompt.contains("AAPL"));
    }

    #[test]
    fn test_format_ai_analysis() {
        let analysis = AiAnalysis {
            summary: "Test failed due to missing data".to_string(),
            root_cause: Some("Input schema mismatch".to_string()),
            suggestions: vec!["Fix schema".to_string(), "Add more data".to_string()],
            confidence: 0.9,
        };

        let formatted = format_ai_analysis(&analysis);
        assert!(formatted.contains("Test failed"));
        assert!(formatted.contains("Fix schema"));
        assert!(formatted.contains("90%"));
    }
}
