-- Tier 1: Basic Passthrough
-- Tests: Basic SELECT *, Kafka source/sink connectivity
-- Expected: All input records pass through unchanged

-- Application metadata
-- @name passthrough_demo
-- @description Simple passthrough - all records flow unchanged from source to sink

-- Source definition
CREATE SOURCE simple_input (
    id INTEGER,
    value STRING,
    amount DECIMAL(10,2),
    count INTEGER,
    active BOOLEAN,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'simple_input',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Simple passthrough - select all fields
CREATE STREAM passthrough_output AS
SELECT *
FROM simple_input;

-- Sink definition
CREATE SINK passthrough_sink FOR passthrough_output WITH (
    'connector' = 'kafka',
    'topic' = 'passthrough_output',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
