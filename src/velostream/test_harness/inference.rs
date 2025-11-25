//! Schema inference from SQL and data files
//!
//! Analyzes SQL queries and sample data to automatically generate
//! schema definitions for test data generation.
//!
//! Features:
//! - SQL field type extraction from CREATE STREAM statements
//! - CSV sampling for value ranges and distributions
//! - Enum detection for categorical fields
//! - Constraint inference (min/max, patterns)

use super::error::{TestHarnessError, TestHarnessResult};
use super::schema::{
    Distribution, EnumConstraint, FieldConstraints, FieldDefinition, FieldType, LengthConstraint,
    RangeConstraint, Schema,
};
use crate::velostream::sql::ast::{
    BinaryOperator, DataType, Expr, LiteralValue, SelectField, StreamingQuery,
};
use crate::velostream::sql::parser::StreamingSqlParser;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Schema inference engine
pub struct SchemaInferencer {
    /// Maximum unique values before treating as non-enum
    enum_threshold: usize,
    /// Sample size for CSV analysis
    sample_size: usize,
}

impl Default for SchemaInferencer {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaInferencer {
    /// Create new inferencer with default settings
    pub fn new() -> Self {
        Self {
            enum_threshold: 20,
            sample_size: 10000,
        }
    }

    /// Set enum threshold (max unique values to detect as enum)
    pub fn with_enum_threshold(mut self, threshold: usize) -> Self {
        self.enum_threshold = threshold;
        self
    }

    /// Set sample size for CSV analysis
    pub fn with_sample_size(mut self, size: usize) -> Self {
        self.sample_size = size;
        self
    }

    /// Infer schema from SQL file
    pub fn infer_from_sql(&self, sql_file: impl AsRef<Path>) -> TestHarnessResult<Vec<Schema>> {
        let sql_file = sql_file.as_ref();
        let content = std::fs::read_to_string(sql_file).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: sql_file.display().to_string(),
        })?;

        self.infer_from_sql_content(&content, sql_file.display().to_string())
    }

    /// Infer schemas from SQL content
    pub fn infer_from_sql_content(
        &self,
        sql_content: &str,
        file_name: String,
    ) -> TestHarnessResult<Vec<Schema>> {
        let parser = StreamingSqlParser::new();
        let mut schemas = Vec::new();

        // Split by semicolons to handle multiple statements
        for statement in sql_content.split(';') {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Parse each statement
            match parser.parse(trimmed) {
                Ok(query) => {
                    if let Some(schema) = self.extract_schema_from_query(&query, &file_name)? {
                        schemas.push(schema);
                    }
                }
                Err(e) => {
                    log::debug!("Failed to parse SQL statement: {}", e);
                    // Continue with other statements
                }
            }
        }

        Ok(schemas)
    }

    /// Extract schema from a single query
    fn extract_schema_from_query(
        &self,
        query: &StreamingQuery,
        file_name: &str,
    ) -> TestHarnessResult<Option<Schema>> {
        match query {
            StreamingQuery::CreateStream {
                name,
                as_select,
                columns,
                ..
            } => {
                // First try explicit column definitions
                if let Some(cols) = columns {
                    let fields: Vec<FieldDefinition> = cols
                        .iter()
                        .map(|col| FieldDefinition {
                            name: col.name.clone(),
                            field_type: self.convert_data_type(&col.data_type),
                            constraints: FieldConstraints::default(),
                            nullable: col.nullable,
                            description: None,
                        })
                        .collect();

                    return Ok(Some(Schema {
                        name: name.clone(),
                        description: Some(format!("Schema for stream '{}'", name)),
                        fields,
                        record_count: 1000,
                    }));
                }

                // Extract from inner SELECT
                if let StreamingQuery::Select { fields, .. } = as_select.as_ref() {
                    let field_defs = self.extract_fields_from_select(fields, file_name)?;
                    if !field_defs.is_empty() {
                        return Ok(Some(Schema {
                            name: name.clone(),
                            description: Some(format!("Inferred schema for stream '{}'", name)),
                            fields: field_defs,
                            record_count: 1000,
                        }));
                    }
                }

                Ok(None)
            }

            StreamingQuery::CreateTable {
                name,
                as_select,
                columns,
                ..
            } => {
                // First try explicit column definitions
                if let Some(cols) = columns {
                    let fields: Vec<FieldDefinition> = cols
                        .iter()
                        .map(|col| FieldDefinition {
                            name: col.name.clone(),
                            field_type: self.convert_data_type(&col.data_type),
                            constraints: FieldConstraints::default(),
                            nullable: col.nullable,
                            description: None,
                        })
                        .collect();

                    return Ok(Some(Schema {
                        name: name.clone(),
                        description: Some(format!("Schema for table '{}'", name)),
                        fields,
                        record_count: 1000,
                    }));
                }

                // Extract from inner SELECT
                if let StreamingQuery::Select { fields, .. } = as_select.as_ref() {
                    let field_defs = self.extract_fields_from_select(fields, file_name)?;
                    if !field_defs.is_empty() {
                        return Ok(Some(Schema {
                            name: name.clone(),
                            description: Some(format!("Inferred schema for table '{}'", name)),
                            fields: field_defs,
                            record_count: 1000,
                        }));
                    }
                }

                Ok(None)
            }

            StreamingQuery::Select { fields, .. } => {
                // Standalone SELECT - generate generic schema
                let field_defs = self.extract_fields_from_select(fields, file_name)?;
                if !field_defs.is_empty() {
                    return Ok(Some(Schema {
                        name: "query_result".to_string(),
                        description: Some("Inferred schema from SELECT".to_string()),
                        fields: field_defs,
                        record_count: 1000,
                    }));
                }
                Ok(None)
            }

            _ => Ok(None),
        }
    }

    /// Extract field definitions from SELECT clause
    fn extract_fields_from_select(
        &self,
        fields: &[SelectField],
        _file_name: &str,
    ) -> TestHarnessResult<Vec<FieldDefinition>> {
        let mut defs = Vec::new();

        for field in fields {
            match field {
                SelectField::Wildcard => {
                    // Can't infer schema from wildcard
                    continue;
                }

                SelectField::Column(name) => {
                    defs.push(FieldDefinition {
                        name: name.clone(),
                        field_type: FieldType::String, // Default type
                        constraints: FieldConstraints::default(),
                        nullable: true,
                        description: None,
                    });
                }

                SelectField::AliasedColumn { column, alias } => {
                    defs.push(FieldDefinition {
                        name: alias.clone(),
                        field_type: FieldType::String, // Default type
                        constraints: FieldConstraints::default(),
                        nullable: true,
                        description: Some(format!("Aliased from column '{}'", column)),
                    });
                }

                SelectField::Expression { alias, expr } => {
                    let field_name = alias
                        .clone()
                        .or_else(|| self.extract_field_name_from_expr(expr))
                        .unwrap_or_else(|| format!("field_{}", defs.len()));

                    let field_type = self.infer_type_from_expr(expr);

                    defs.push(FieldDefinition {
                        name: field_name,
                        field_type,
                        constraints: FieldConstraints::default(),
                        nullable: true,
                        description: None,
                    });
                }
            }
        }

        Ok(defs)
    }

    /// Extract field name from expression
    fn extract_field_name_from_expr(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(name) => Some(name.clone()),
            Expr::Function { name, .. } => {
                // For functions, use function name as hint
                Some(name.to_lowercase())
            }
            _ => None,
        }
    }

    /// Infer field type from expression
    fn infer_type_from_expr(&self, expr: &Expr) -> FieldType {
        match expr {
            Expr::Literal(lit) => self.infer_type_from_literal(lit),

            Expr::Column(_) => {
                // Default to string for unknown columns
                FieldType::String
            }

            Expr::Function { name, .. } => {
                // Infer from function return type
                self.infer_function_return_type(name)
            }

            Expr::BinaryOp { op, .. } => {
                // Arithmetic operations typically return numbers
                match op {
                    BinaryOperator::Add
                    | BinaryOperator::Subtract
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo => FieldType::Float,
                    BinaryOperator::GreaterThan
                    | BinaryOperator::LessThan
                    | BinaryOperator::GreaterThanOrEqual
                    | BinaryOperator::LessThanOrEqual
                    | BinaryOperator::Equal
                    | BinaryOperator::NotEqual
                    | BinaryOperator::And
                    | BinaryOperator::Or => FieldType::Boolean,
                    _ => FieldType::String,
                }
            }

            Expr::Case { .. } => FieldType::String,

            Expr::Subquery { .. } => FieldType::String,

            Expr::WindowFunction { function_name, .. } => {
                self.infer_function_return_type(function_name)
            }

            _ => FieldType::String,
        }
    }

    /// Infer type from literal value
    fn infer_type_from_literal(&self, lit: &LiteralValue) -> FieldType {
        match lit {
            LiteralValue::Integer(_) => FieldType::Integer,
            LiteralValue::Float(_) => FieldType::Float,
            LiteralValue::String(_) => FieldType::String,
            LiteralValue::Boolean(_) => FieldType::Boolean,
            LiteralValue::Null => FieldType::String,
            LiteralValue::Decimal(_) => FieldType::Decimal { precision: 4 },
            LiteralValue::Interval { .. } => FieldType::String,
        }
    }

    /// Infer function return type
    fn infer_function_return_type(&self, func_name: &str) -> FieldType {
        match func_name.to_uppercase().as_str() {
            // Aggregate functions
            "COUNT" => FieldType::Integer,
            "SUM" | "AVG" | "STDDEV" | "VARIANCE" => FieldType::Float,
            "MIN" | "MAX" => FieldType::Float, // Could be any type, default to float

            // String functions
            "CONCAT" | "UPPER" | "LOWER" | "TRIM" | "SUBSTR" | "SUBSTRING" | "REPLACE"
            | "COALESCE" => FieldType::String,

            // Date/time functions
            "NOW" | "CURRENT_TIMESTAMP" => FieldType::Timestamp,
            "CURRENT_DATE" | "DATE" => FieldType::Date,
            "EXTRACT" | "DATE_PART" | "EPOCH" => FieldType::Integer,
            "DATE_TRUNC" => FieldType::Timestamp,

            // Numeric functions
            "ABS" | "CEIL" | "FLOOR" | "ROUND" | "SQRT" | "LOG" | "EXP" | "POWER" => {
                FieldType::Float
            }

            // Window functions
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => FieldType::Integer,
            "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" => FieldType::Float,

            // Boolean functions
            "IS_NULL" | "IS_NOT_NULL" => FieldType::Boolean,

            // Default
            _ => FieldType::String,
        }
    }

    /// Convert AST DataType to schema FieldType
    fn convert_data_type(&self, dt: &DataType) -> FieldType {
        match dt {
            DataType::Integer => FieldType::Integer,
            DataType::Float => FieldType::Float,
            DataType::String => FieldType::String,
            DataType::Boolean => FieldType::Boolean,
            DataType::Timestamp => FieldType::Timestamp,
            DataType::Decimal => FieldType::Decimal { precision: 4 },
            DataType::Array(_) => FieldType::String, // Serialize arrays as JSON strings
            DataType::Map(_, _) => FieldType::String, // Serialize maps as JSON strings
            DataType::Struct(_) => FieldType::String, // Serialize structs as JSON strings
        }
    }

    /// Infer schema from CSV file
    pub fn infer_from_csv(&self, csv_file: impl AsRef<Path>) -> TestHarnessResult<Schema> {
        let csv_file = csv_file.as_ref();

        // Read CSV file
        let content = std::fs::read_to_string(csv_file).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: csv_file.display().to_string(),
        })?;

        let schema_name = csv_file
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "csv_schema".to_string());

        self.infer_from_csv_content(&content, schema_name)
    }

    /// Infer schema from CSV content
    pub fn infer_from_csv_content(
        &self,
        csv_content: &str,
        schema_name: String,
    ) -> TestHarnessResult<Schema> {
        // Parse CSV manually (simple implementation without csv crate)
        let mut lines = csv_content.lines();

        // Get headers
        let headers: Vec<String> = match lines.next() {
            Some(header_line) => self.parse_csv_line(header_line),
            None => {
                return Err(TestHarnessError::SchemaParseError {
                    message: "CSV file is empty".to_string(),
                    file: schema_name,
                });
            }
        };

        if headers.is_empty() {
            return Err(TestHarnessError::SchemaParseError {
                message: "CSV file has no headers".to_string(),
                file: schema_name,
            });
        }

        // Analyze column values
        let mut column_stats: Vec<ColumnStats> =
            headers.iter().map(|_| ColumnStats::new()).collect();

        let mut row_count = 0;
        for line in lines {
            if row_count >= self.sample_size {
                break;
            }

            let values = self.parse_csv_line(line);
            for (i, value) in values.iter().enumerate() {
                if i < column_stats.len() {
                    column_stats[i].add_value(value);
                }
            }
            row_count += 1;
        }

        // Generate field definitions
        let fields: Vec<FieldDefinition> = headers
            .iter()
            .zip(column_stats.iter())
            .map(|(name, stats)| self.create_field_definition(name, stats))
            .collect();

        Ok(Schema {
            name: schema_name,
            description: Some(format!("Inferred from CSV ({} rows sampled)", row_count)),
            fields,
            record_count: row_count.max(1000),
        })
    }

    /// Parse a CSV line (simple implementation)
    fn parse_csv_line(&self, line: &str) -> Vec<String> {
        let mut values = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '"' => {
                    if in_quotes && chars.peek() == Some(&'"') {
                        // Escaped quote
                        current.push('"');
                        chars.next();
                    } else {
                        in_quotes = !in_quotes;
                    }
                }
                ',' if !in_quotes => {
                    values.push(current.trim().to_string());
                    current = String::new();
                }
                _ => current.push(c),
            }
        }
        values.push(current.trim().to_string());

        values
    }

    /// Create field definition from column statistics
    fn create_field_definition(&self, name: &str, stats: &ColumnStats) -> FieldDefinition {
        let (field_type, constraints) = self.infer_type_and_constraints(stats);

        FieldDefinition {
            name: name.to_string(),
            field_type,
            constraints,
            nullable: stats.has_nulls,
            description: Some(format!(
                "Inferred from {} samples ({} unique values)",
                stats.count,
                stats.unique_values.len()
            )),
        }
    }

    /// Infer field type and constraints from column stats
    fn infer_type_and_constraints(&self, stats: &ColumnStats) -> (FieldType, FieldConstraints) {
        let mut constraints = FieldConstraints::default();

        // Check for enum (categorical values)
        if stats.unique_values.len() <= self.enum_threshold
            && stats.unique_values.len() < stats.count / 2
            && stats.count > 0
        {
            let values: Vec<String> = stats.unique_values.iter().cloned().collect();

            // Calculate weights based on frequency
            let weights: Option<Vec<f64>> = if !stats.value_counts.is_empty() {
                let total = stats.count as f64;
                Some(
                    values
                        .iter()
                        .map(|v| *stats.value_counts.get(v).unwrap_or(&0) as f64 / total)
                        .collect(),
                )
            } else {
                None
            };

            constraints.enum_values = Some(EnumConstraint { values, weights });

            return (FieldType::String, constraints);
        }

        // Check for integer
        if stats.all_integers && !stats.has_floats && stats.count > 0 {
            constraints.range = Some(RangeConstraint {
                min: stats.min_value,
                max: stats.max_value,
            });

            // Detect distribution
            if stats.count > 100 {
                constraints.distribution = Some(self.detect_distribution(stats));
            }

            return (FieldType::Integer, constraints);
        }

        // Check for float/decimal
        if (stats.has_floats || stats.all_integers) && stats.count > 0 {
            constraints.range = Some(RangeConstraint {
                min: stats.min_value,
                max: stats.max_value,
            });

            // Check if likely decimal (financial)
            let precision = stats.max_decimal_places;
            if precision > 0 && precision <= 8 {
                return (FieldType::Decimal { precision }, constraints);
            }

            return (FieldType::Float, constraints);
        }

        // Check for boolean
        if stats.is_boolean() {
            return (FieldType::Boolean, constraints);
        }

        // Check for timestamp
        if stats.is_timestamp() {
            return (FieldType::Timestamp, constraints);
        }

        // Check for UUID
        if stats.is_uuid() {
            return (FieldType::Uuid, constraints);
        }

        // Default to string with length constraints
        if stats.min_length < stats.max_length && stats.min_length < usize::MAX {
            constraints.length = Some(LengthConstraint {
                min: Some(stats.min_length),
                max: Some(stats.max_length),
            });
        }

        (FieldType::String, constraints)
    }

    /// Detect distribution from column statistics
    fn detect_distribution(&self, stats: &ColumnStats) -> Distribution {
        // Simple heuristic: check if values are uniformly distributed
        // For more sophisticated detection, could use chi-squared test

        let range = stats.max_value - stats.min_value;
        if range > 0.0 && stats.count > 0 {
            let mean = stats.sum / stats.count as f64;
            let variance = stats.sum_sq / stats.count as f64 - mean * mean;
            let std_dev = variance.max(0.0).sqrt();

            // Check if looks normal (bell curve)
            let expected_std = range / 4.0; // For normal, ~95% within 2 std devs
            if expected_std > 0.0 && (std_dev - expected_std).abs() / expected_std < 0.3 {
                return Distribution::Normal { mean, std_dev };
            }
        }

        Distribution::Uniform
    }

    /// Generate schema YAML output
    pub fn generate_yaml(&self, schema: &Schema) -> TestHarnessResult<String> {
        serde_yaml::to_string(schema).map_err(|e| TestHarnessError::SchemaParseError {
            message: format!("Failed to generate YAML: {}", e),
            file: schema.name.clone(),
        })
    }

    /// Write schema to file
    pub fn write_schema(
        &self,
        schema: &Schema,
        output_path: impl AsRef<Path>,
    ) -> TestHarnessResult<()> {
        let yaml = self.generate_yaml(schema)?;
        let output_path = output_path.as_ref();

        std::fs::write(output_path, yaml).map_err(|e| TestHarnessError::IoError {
            message: e.to_string(),
            path: output_path.display().to_string(),
        })?;

        Ok(())
    }
}

/// Statistics collected for a single column
#[derive(Debug)]
struct ColumnStats {
    /// Total value count
    count: usize,
    /// Unique values seen
    unique_values: HashSet<String>,
    /// Value frequency counts
    value_counts: HashMap<String, usize>,
    /// Whether nulls/empty values were seen
    has_nulls: bool,
    /// Whether all values are integers
    all_integers: bool,
    /// Whether any floats were seen
    has_floats: bool,
    /// Minimum numeric value
    min_value: f64,
    /// Maximum numeric value
    max_value: f64,
    /// Sum of numeric values (for mean calculation)
    sum: f64,
    /// Sum of squared values (for variance)
    sum_sq: f64,
    /// Maximum decimal places seen
    max_decimal_places: u8,
    /// Minimum string length
    min_length: usize,
    /// Maximum string length
    max_length: usize,
    /// Sample of timestamp-like values
    timestamp_samples: Vec<String>,
    /// Sample of UUID-like values
    uuid_samples: Vec<String>,
}

impl ColumnStats {
    fn new() -> Self {
        Self {
            count: 0,
            unique_values: HashSet::new(),
            value_counts: HashMap::new(),
            has_nulls: false,
            all_integers: true,
            has_floats: false,
            min_value: f64::MAX,
            max_value: f64::MIN,
            sum: 0.0,
            sum_sq: 0.0,
            max_decimal_places: 0,
            min_length: usize::MAX,
            max_length: 0,
            timestamp_samples: Vec::new(),
            uuid_samples: Vec::new(),
        }
    }

    fn add_value(&mut self, value: &str) {
        self.count += 1;

        // Track string lengths
        let len = value.len();
        self.min_length = self.min_length.min(len);
        self.max_length = self.max_length.max(len);

        // Check for null/empty
        if value.is_empty()
            || value.eq_ignore_ascii_case("null")
            || value.eq_ignore_ascii_case("none")
            || value.eq_ignore_ascii_case("na")
            || value.eq_ignore_ascii_case("n/a")
        {
            self.has_nulls = true;
            return;
        }

        // Track unique values (up to a limit)
        if self.unique_values.len() < 1000 {
            self.unique_values.insert(value.to_string());
        }

        // Track value counts for frequency analysis
        *self.value_counts.entry(value.to_string()).or_insert(0) += 1;

        // Try parsing as number
        if let Ok(int_val) = value.parse::<i64>() {
            let f = int_val as f64;
            self.min_value = self.min_value.min(f);
            self.max_value = self.max_value.max(f);
            self.sum += f;
            self.sum_sq += f * f;
        } else if let Ok(float_val) = value.parse::<f64>() {
            self.all_integers = false;
            self.has_floats = true;
            self.min_value = self.min_value.min(float_val);
            self.max_value = self.max_value.max(float_val);
            self.sum += float_val;
            self.sum_sq += float_val * float_val;

            // Count decimal places
            if let Some(dot_pos) = value.find('.') {
                let decimals = value.len() - dot_pos - 1;
                self.max_decimal_places = self.max_decimal_places.max(decimals as u8);
            }
        } else {
            self.all_integers = false;

            // Sample timestamp-like values
            if self.timestamp_samples.len() < 10 && looks_like_timestamp(value) {
                self.timestamp_samples.push(value.to_string());
            }

            // Sample UUID-like values
            if self.uuid_samples.len() < 10 && looks_like_uuid(value) {
                self.uuid_samples.push(value.to_string());
            }
        }
    }

    fn is_boolean(&self) -> bool {
        if self.unique_values.len() > 2 {
            return false;
        }

        let lower: HashSet<String> = self
            .unique_values
            .iter()
            .map(|s| s.to_lowercase())
            .collect();

        lower.is_subset(
            &["true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        )
    }

    fn is_timestamp(&self) -> bool {
        // If most samples look like timestamps
        if self.timestamp_samples.len() >= 5 {
            return true;
        }

        // Check unique values
        if self.unique_values.len() <= 10 && !self.unique_values.is_empty() {
            return self.unique_values.iter().all(|v| looks_like_timestamp(v));
        }

        false
    }

    fn is_uuid(&self) -> bool {
        // If most samples look like UUIDs
        if self.uuid_samples.len() >= 5 {
            return true;
        }

        // Check unique values
        if self.unique_values.len() <= 10 && !self.unique_values.is_empty() {
            return self.unique_values.iter().all(|v| looks_like_uuid(v));
        }

        false
    }
}

/// Check if a string looks like a timestamp
fn looks_like_timestamp(s: &str) -> bool {
    // Common timestamp patterns
    let patterns = [
        // ISO 8601
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
        // Date with time
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",
        // Date only
        r"^\d{4}-\d{2}-\d{2}$",
        // US date format
        r"^\d{2}/\d{2}/\d{4}",
        // Unix timestamp (10-13 digits)
        r"^\d{10,13}$",
    ];

    for pattern in &patterns {
        if let Ok(re) = regex::Regex::new(pattern) {
            if re.is_match(s) {
                return true;
            }
        }
    }

    // Try parsing as chrono datetime
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
        || chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").is_ok()
        || chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
}

/// Check if a string looks like a UUID
fn looks_like_uuid(s: &str) -> bool {
    // UUID pattern: 8-4-4-4-12 hex characters
    if s.len() != 36 {
        return false;
    }

    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 5 {
        return false;
    }

    let expected_lengths = [8, 4, 4, 4, 12];
    for (part, expected_len) in parts.iter().zip(expected_lengths.iter()) {
        if part.len() != *expected_len {
            return false;
        }
        if !part.chars().all(|c| c.is_ascii_hexdigit()) {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_looks_like_timestamp() {
        assert!(looks_like_timestamp("2024-01-15T10:30:00"));
        assert!(looks_like_timestamp("2024-01-15T10:30:00.123"));
        assert!(looks_like_timestamp("2024-01-15 10:30:00"));
        assert!(looks_like_timestamp("2024-01-15"));
        assert!(looks_like_timestamp("1705312200000")); // Unix timestamp ms

        assert!(!looks_like_timestamp("hello"));
        assert!(!looks_like_timestamp("12345"));
    }

    #[test]
    fn test_looks_like_uuid() {
        assert!(looks_like_uuid("550e8400-e29b-41d4-a716-446655440000"));
        assert!(looks_like_uuid("6ba7b810-9dad-11d1-80b4-00c04fd430c8"));

        assert!(!looks_like_uuid("not-a-uuid"));
        assert!(!looks_like_uuid("550e8400-e29b-41d4-a716"));
    }

    #[test]
    fn test_column_stats_integer() {
        let mut stats = ColumnStats::new();
        for i in 1..=100 {
            stats.add_value(&i.to_string());
        }

        assert!(stats.all_integers);
        assert!(!stats.has_floats);
        assert_eq!(stats.min_value, 1.0);
        assert_eq!(stats.max_value, 100.0);
    }

    #[test]
    fn test_column_stats_float() {
        let mut stats = ColumnStats::new();
        stats.add_value("1.5");
        stats.add_value("2.75");
        stats.add_value("3.125");

        assert!(!stats.all_integers);
        assert!(stats.has_floats);
        assert_eq!(stats.max_decimal_places, 3);
    }

    #[test]
    fn test_column_stats_boolean() {
        let mut stats = ColumnStats::new();
        stats.add_value("true");
        stats.add_value("false");
        stats.add_value("true");

        assert!(stats.is_boolean());
    }

    #[test]
    fn test_parse_csv_line() {
        let inferencer = SchemaInferencer::new();

        // Simple line
        let values = inferencer.parse_csv_line("a,b,c");
        assert_eq!(values, vec!["a", "b", "c"]);

        // Quoted values
        let values = inferencer.parse_csv_line("\"hello, world\",test,123");
        assert_eq!(values, vec!["hello, world", "test", "123"]);

        // Escaped quotes
        let values = inferencer.parse_csv_line("\"a\"\"b\",c");
        assert_eq!(values, vec!["a\"b", "c"]);
    }

    #[test]
    fn test_infer_from_csv() {
        let csv_content = r#"id,name,price,active,created_at
1,Widget A,99.99,true,2024-01-15T10:30:00
2,Widget B,149.50,false,2024-01-16T11:00:00
3,Widget C,75.00,true,2024-01-17T09:15:00
"#;

        let inferencer = SchemaInferencer::new();
        let schema = inferencer
            .infer_from_csv_content(csv_content, "test_schema".to_string())
            .unwrap();

        assert_eq!(schema.name, "test_schema");
        assert_eq!(schema.fields.len(), 5);

        // Check id field
        let id_field = schema.fields.iter().find(|f| f.name == "id").unwrap();
        assert!(matches!(id_field.field_type, FieldType::Integer));

        // Check price field
        let price_field = schema.fields.iter().find(|f| f.name == "price").unwrap();
        assert!(matches!(
            price_field.field_type,
            FieldType::Decimal { .. } | FieldType::Float
        ));

        // Check active field
        let active_field = schema.fields.iter().find(|f| f.name == "active").unwrap();
        assert!(matches!(active_field.field_type, FieldType::Boolean));

        // Check created_at field
        let created_field = schema
            .fields
            .iter()
            .find(|f| f.name == "created_at")
            .unwrap();
        assert!(matches!(created_field.field_type, FieldType::Timestamp));
    }
}
