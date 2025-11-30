-- File-based passthrough query demo
-- Reads trade data from CSV and outputs to CSV

CREATE STREAM trade_passthrough AS
SELECT
    symbol,
    price,
    volume,
    timestamp
FROM trades;
