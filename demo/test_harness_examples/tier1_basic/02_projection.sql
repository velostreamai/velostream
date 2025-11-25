-- Tier 1: Field Projection
-- Tests: SELECT specific fields, aliases, expressions
-- Expected: Only selected fields in output with correct aliases

-- Application metadata
-- @name projection_demo
-- @description Field projection with aliases and simple expressions

-- Source definition
CREATE SOURCE order_input (
    order_id STRING,
    customer_id INTEGER,
    product_id STRING,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    status STRING,
    region STRING,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'order_input',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Project specific fields with aliases
CREATE STREAM order_summary AS
SELECT
    order_id AS id,
    customer_id AS customer,
    quantity,
    unit_price AS price,
    quantity * unit_price AS total,
    status,
    event_time
FROM order_input;

-- Sink definition
CREATE SINK order_summary_sink FOR order_summary WITH (
    'connector' = 'kafka',
    'topic' = 'order_summary',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
