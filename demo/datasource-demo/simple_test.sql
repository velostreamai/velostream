-- Simple SQL test file for batch execution
-- This demonstrates the SQL batch execution capability

-- Create a stream for financial transactions
CREATE STREAM financial_transactions (
    transaction_id VARCHAR,
    customer_id VARCHAR,
    amount FLOAT,
    currency VARCHAR,
    timestamp INTEGER,
    merchant_category VARCHAR,
    description VARCHAR
);

-- Basic select query
SELECT transaction_id, customer_id, amount 
FROM financial_transactions;

-- Aggregation query  
SELECT customer_id, SUM(amount) as total_amount
FROM financial_transactions
GROUP BY customer_id;