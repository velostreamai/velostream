-- Simple SQL test file for batch execution (FR-047 compliant)
-- This demonstrates the unified URI syntax for data sources

-- Basic select query with URI-based data source
SELECT transaction_id, customer_id, amount 
FROM 'file://./demo_data/financial_transactions.csv';

-- Aggregation query with URI-based source and destination
SELECT customer_id KEY, SUM(amount) as total_amount
FROM 'file://./demo_data/financial_transactions.csv'
GROUP BY customer_id
INTO 'file://./demo_output/customer_totals.jsonl'
WITH (
    format = 'jsonlines'
);

-- Example with Kafka URI (commented for demo)
-- SELECT transaction_id, amount, currency
-- FROM 'kafka://localhost:9092/financial_transactions'
-- WHERE amount > 1000
-- INTO 'kafka://localhost:9092/high_value_transactions'
-- WITH (
--     key_format = 'string',
--     value_format = 'json'
-- );