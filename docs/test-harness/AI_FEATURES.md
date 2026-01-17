# AI-Powered Features Guide

The Velostream Test Harness includes AI-powered features that leverage Claude to provide intelligent assistance for schema inference, test generation, and failure analysis.

## Table of Contents

- [Prerequisites](#prerequisites)
- [AI Schema Inference](#ai-schema-inference)
- [AI Test Generation](#ai-test-generation)
- [AI Failure Analysis](#ai-failure-analysis)
- [Testing with MockAiProvider](#testing-with-mockaiprovider)
- [Troubleshooting](#troubleshooting)
- [API Reference](#api-reference)

---

## Prerequisites

### 1. Set Up Your API Key

AI features require an Anthropic API key. Set it as an environment variable:

```bash
# Linux/macOS
export ANTHROPIC_API_KEY="sk-ant-api03-..."

# Windows (PowerShell)
$env:ANTHROPIC_API_KEY="sk-ant-api03-..."

# Windows (Command Prompt)
set ANTHROPIC_API_KEY=sk-ant-api03-...
```

**Get an API key**: Visit [console.anthropic.com](https://console.anthropic.com) to create an account and generate an API key.

### 2. Verify API Access

```bash
# Check if AI features are available
velo-test init your_app.sql --ai --output test_spec.yaml

# If key is missing, you'll see:
# ⚠️  AI mode requested but ANTHROPIC_API_KEY not set
#    Falling back to rule-based generation
```

### 3. Model Used

The test harness uses `claude-sonnet-4-20250514` by default, which provides a good balance of quality and speed for code analysis tasks.

---

## AI Schema Inference

AI schema inference analyzes your SQL queries and sample data to generate intelligent schema definitions with appropriate constraints.

### Basic Usage

```bash
# Infer schemas from SQL file
velo-test infer-schema app.sql --ai --output schemas/

# Infer from SQL + sample data
velo-test infer-schema app.sql --ai --data-dir data/ --output schemas/
```

### What AI Inference Provides

| Feature | Rule-Based | AI-Powered |
|---------|------------|------------|
| Field type detection | Basic | Context-aware |
| Value ranges | None | Inferred from patterns |
| Enum detection | None | Automatic from categorical data |
| Nullable inference | Basic | SQL-context aware |
| Constraints | None | Realistic domain constraints |
| Descriptions | None | Human-readable field descriptions |

### Example: Financial Data Schema

**Input SQL:**
```sql
CREATE STREAM market_data (
    symbol STRING,
    price DOUBLE,
    quantity INT,
    exchange STRING,
    event_time TIMESTAMP
);

SELECT symbol,
       AVG(price) as avg_price,
       SUM(quantity) as total_volume
FROM market_data
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '5' MINUTE);
```

**AI-Generated Schema:**
```yaml
# AI-generated schema for market_data
name: market_data
description: Real-time market data stream for financial instruments

fields:
  - name: symbol
    type: string
    nullable: false
    description: Stock ticker symbol (e.g., AAPL, GOOGL)
    constraints:
      length:
        min: 1
        max: 10
      enum_values:
        values: [AAPL, GOOGL, MSFT, AMZN, META]
        weights: [25, 20, 20, 20, 15]

  - name: price
    type: decimal
    precision: 4
    nullable: false
    description: Current trading price in USD
    constraints:
      range:
        min: 1.00
        max: 5000.00
      # AI note: Realistic stock price range for US equities

  - name: quantity
    type: integer
    nullable: false
    description: Number of shares in the trade
    constraints:
      range:
        min: 1
        max: 10000
      distribution:
        type: log_normal
        # AI note: Trade sizes typically follow log-normal distribution

  - name: exchange
    type: string
    nullable: false
    description: Exchange where trade occurred
    constraints:
      enum_values:
        values: [NYSE, NASDAQ, BATS, IEX]
        weights: [40, 40, 15, 5]

  - name: event_time
    type: timestamp
    nullable: false
    description: Time when trade was executed
```

### Using Sample Data

Provide CSV files for better inference:

```bash
# data/market_data.csv
symbol,price,quantity,exchange,event_time
AAPL,178.50,100,NASDAQ,2025-01-15T14:30:00Z
GOOGL,142.30,50,NASDAQ,2025-01-15T14:30:01Z
MSFT,425.00,200,NYSE,2025-01-15T14:30:02Z
```

```bash
velo-test infer-schema app.sql --ai --data-dir data/ --output schemas/
```

AI will analyze the CSV patterns to refine constraints:
- Detect actual symbol values used
- Calculate realistic price ranges from data
- Identify quantity distributions
- Infer exchange frequency weights

---

## AI Test Generation

AI test generation analyzes your SQL queries to create intelligent test specifications with appropriate assertions.

### Basic Usage

```bash
# Generate test spec with AI
velo-test init app.sql --ai --output test_spec.yaml
```

### What AI Generation Provides

| Query Pattern | Generated Assertions |
|---------------|---------------------|
| `GROUP BY` + aggregates | `aggregate_check` with expected totals |
| `JOIN` operations | `join_coverage` with realistic match rates |
| `WINDOW` functions | `window_completeness` checks |
| `WHERE` filters | `record_count` with estimated reduction |
| Streaming output | `latency_check` for real-time guarantees |

### Example: Generated Test Spec

**Input SQL:**
```sql
-- Stage 1: Filter high-value trades
CREATE STREAM high_value_trades AS
SELECT * FROM market_data
WHERE price * quantity > 10000;

-- Stage 2: Aggregate by symbol
CREATE TABLE symbol_aggregates AS
SELECT symbol,
       COUNT(*) as trade_count,
       SUM(price * quantity) as total_value
FROM high_value_trades
GROUP BY symbol
WINDOW TUMBLING(INTERVAL '1' MINUTE);
```

**AI-Generated Test Spec:**
```yaml
# AI-generated test spec for trading_pipeline.sql
application: trading_pipeline
description: Multi-stage trading data pipeline with filtering and aggregation

default_timeout_ms: 30000
default_records: 1000

queries:
  - name: high_value_trades
    description: Filter trades where value > $10,000
    # AI note: WHERE filter typically reduces records by 70-90%
    inputs:
      - source: market_data
        schema: market_data.schema.yaml
        records: 1000
    assertions:
      - type: record_count
        operator: between
        min: 100
        max: 300
        # AI note: ~10-30% of trades expected to pass $10k threshold

      - type: field_values
        field: price
        # All output prices should be reasonable
        operator: between
        min: 1.0
        max: 10000.0

  - name: symbol_aggregates
    description: 1-minute aggregation by symbol
    # AI note: Window aggregation with GROUP BY
    assertions:
      - type: record_count
        operator: between
        min: 3
        max: 10
        # AI note: Expected ~5 unique symbols from enum

      - type: aggregate_check
        function: SUM
        field: trade_count
        # Total trades across all symbols should match input
        operator: equals
        expected: "{{outputs.high_value_trades.count}}"

      - type: no_nulls
        fields: [symbol, trade_count, total_value]
        # AI note: Aggregation keys and values should never be null
```

### AI vs Rule-Based Comparison

```bash
# Rule-based (fallback)
velo-test init app.sql --output test_spec.yaml

# AI-powered (recommended)
velo-test init app.sql --ai --output test_spec.yaml
```

| Aspect | Rule-Based | AI-Powered |
|--------|------------|------------|
| Assertion count | 1-2 per query | 3-5 per query |
| Assertion types | Basic record_count | Multiple specialized types |
| Expected values | Generic | Domain-specific estimates |
| Comments | None | Explanatory notes |

---

## AI Failure Analysis

When test assertions fail, AI provides detailed analysis and actionable fix suggestions.

### Enabling AI Analysis

```bash
# Run tests with AI failure analysis
velo-test run app.sql --spec test_spec.yaml --ai
```

### Example: Failure Analysis

**Test Output:**
```
❌ Query #3: enriched_orders
   FAILURE: join_coverage (0% match rate, expected 80%)

   🤖 AI Analysis:
   The JOIN on 'customer_id' produced no matches because:
   - orders table contains customer_ids: [1001, 1002, 1003, ...]
   - customers table contains customer_ids: [C-1001, C-1002, C-1003, ...]

   Root Cause: The customer_id format differs between tables.
   Orders use numeric IDs (1001) while customers use prefixed strings (C-1001).

   Suggestions:
   1. Update the JOIN condition to handle format difference:
      ON orders.customer_id = SUBSTRING(customers.customer_id, 3)
   2. Or standardize the test data generation to use consistent formats
   3. Check if a data transformation step is missing in the pipeline

   Confidence: 92%
```

### Analysis Context

AI receives the following context for analysis:
- Query SQL
- Input data samples (first/last records)
- Output data samples
- Failed assertion details (expected vs actual)

---

## Testing with MockAiProvider

For testing your applications without making real API calls, use `MockAiProvider`.

### Basic Usage

```rust
use velostream::velostream::test_harness::ai::{
    AiProvider, MockAiProvider, AiAnalysis, AnalysisContext
};

#[tokio::test]
async fn test_failure_handling() {
    // Create mock with canned response
    let mock = MockAiProvider::new()
        .with_analysis(AiAnalysis {
            summary: "Test failure due to schema mismatch".to_string(),
            root_cause: Some("Field type incompatibility".to_string()),
            suggestions: vec![
                "Update schema to use STRING type".to_string(),
                "Add type conversion in SQL".to_string(),
            ],
            confidence: 0.85,
        });

    // Create test context
    let context = AnalysisContext {
        query_sql: "SELECT * FROM orders".to_string(),
        query_name: "order_query".to_string(),
        input_samples: vec![],
        output_samples: vec![],
        assertion: None,
    };

    // Use mock instead of real API
    let result = mock.analyze_failure(&context).await.unwrap();

    assert_eq!(result.confidence, 0.85);
    assert!(result.suggestions.len() == 2);
}
```

### Verifying API Calls

```rust
#[tokio::test]
async fn test_api_calls_recorded() {
    let mock = MockAiProvider::new()
        .with_schema(Schema { /* ... */ });

    // Make API call
    let _ = mock.infer_schema("SELECT * FROM test", &[]).await;

    // Verify call was recorded
    let calls = mock.get_calls().await;
    assert_eq!(calls.len(), 1);

    match &calls[0] {
        MockCall::InferSchema { sql, sample_count } => {
            assert!(sql.contains("SELECT"));
            assert_eq!(*sample_count, 0);
        }
        _ => panic!("Expected InferSchema call"),
    }
}
```

### Testing Unavailable AI

```rust
#[tokio::test]
async fn test_ai_unavailable_handling() {
    // Simulate missing API key
    let mock = MockAiProvider::unavailable();

    assert!(!mock.is_available());

    let result = mock.analyze_failure(&context).await;
    assert!(result.is_err());
}
```

### Available Mock Methods

| Method | Description |
|--------|-------------|
| `MockAiProvider::new()` | Create available mock (no responses configured) |
| `MockAiProvider::unavailable()` | Create mock that simulates missing API key |
| `.with_schema(schema)` | Set canned schema response |
| `.with_analysis(analysis)` | Set canned analysis response |
| `.with_test_spec(spec)` | Set canned test spec response |
| `.get_calls().await` | Get recorded API calls |
| `.clear_calls().await` | Clear recorded calls |

---

## Troubleshooting

### API Key Issues

**Problem:** `AI mode requested but ANTHROPIC_API_KEY not set`

**Solution:**
```bash
# Verify key is set
echo $ANTHROPIC_API_KEY

# Set if missing
export ANTHROPIC_API_KEY="sk-ant-api03-..."
```

### Rate Limiting

**Problem:** `Claude API error (429): Rate limit exceeded`

**Solution:**
- Wait a few seconds and retry
- Reduce batch size for schema inference
- Use rule-based fallback for development iterations

### Network Issues

**Problem:** `Failed to call Claude API: connection refused`

**Solution:**
- Check internet connectivity
- Verify no firewall blocking `api.anthropic.com`
- Check proxy settings if applicable

### Invalid Response

**Problem:** `Failed to parse AI-generated schema`

**Solution:**
- AI responses are parsed as YAML; malformed responses fail
- Retry the request (responses can vary)
- Fall back to rule-based generation
- Report persistent issues with example SQL

### Timeout

**Problem:** AI requests taking too long

**Solution:**
- Default timeout is 60 seconds
- Complex SQL files may need more time
- Consider breaking large files into smaller units

---

## API Reference

### AiProvider Trait

```rust
#[async_trait]
pub trait AiProvider: Send + Sync {
    /// Check if AI is available (API key configured)
    fn is_available(&self) -> bool;

    /// Infer schema from SQL and CSV samples
    async fn infer_schema(
        &self,
        sql_content: &str,
        csv_samples: &[CsvSample],
    ) -> TestHarnessResult<Schema>;

    /// Analyze assertion failure and provide suggestions
    async fn analyze_failure(
        &self,
        context: &AnalysisContext
    ) -> TestHarnessResult<AiAnalysis>;

    /// Generate test specification from SQL
    async fn generate_test_spec(
        &self,
        sql_content: &str,
        app_name: &str,
    ) -> TestHarnessResult<TestSpec>;
}
```

### AiAnalysis Structure

```rust
pub struct AiAnalysis {
    /// One-sentence summary of the issue
    pub summary: String,

    /// Detailed root cause explanation
    pub root_cause: Option<String>,

    /// Actionable fix suggestions
    pub suggestions: Vec<String>,

    /// AI confidence level (0.0 to 1.0)
    pub confidence: f64,
}
```

### AnalysisContext Structure

```rust
pub struct AnalysisContext {
    /// The SQL query being analyzed
    pub query_sql: String,

    /// Query name for identification
    pub query_name: String,

    /// Sample input records (JSON strings)
    pub input_samples: Vec<String>,

    /// Sample output records (JSON strings)
    pub output_samples: Vec<String>,

    /// The assertion that failed (if applicable)
    pub assertion: Option<AssertionResult>,
}
```

### CsvSample Structure

```rust
pub struct CsvSample {
    /// Source name (usually filename without extension)
    pub name: String,

    /// CSV content (header + sample rows)
    pub content: String,

    /// Total rows in original file
    pub total_rows: usize,
}
```

---

## Best Practices

1. **Start with AI, refine manually** - Use AI to generate initial schemas/specs, then customize

2. **Provide sample data** - AI inference improves significantly with real data samples

3. **Review AI suggestions** - AI provides good starting points but may need domain adjustments

4. **Use mocks in CI/CD** - Avoid API costs and flakiness by using MockAiProvider in automated tests

5. **Cache generated artifacts** - Commit AI-generated schemas and specs to avoid regenerating

6. **Fall back gracefully** - Always handle AI unavailability with rule-based alternatives
