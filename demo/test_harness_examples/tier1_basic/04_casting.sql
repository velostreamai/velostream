-- Tier 1: Type Casting
-- Tests: CAST expressions, type conversions
-- Expected: Correct type conversions in output

-- Application metadata
-- @name casting_demo
-- @description Type casting and conversion operations

-- Source definition
CREATE SOURCE raw_events (
    id INTEGER,
    value STRING,
    amount DECIMAL(10,2),
    count INTEGER,
    active BOOLEAN,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_events',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Demonstrate various type casts
CREATE STREAM typed_events AS
SELECT
    CAST(id AS STRING) AS id_str,
    id AS id_int,
    CAST(amount AS DOUBLE) AS amount_float,
    CAST(count AS BIGINT) AS count_long,
    CAST(active AS STRING) AS active_str,
    CAST(event_time AS STRING) AS time_str,
    event_time
FROM raw_events;

-- Sink definition
CREATE SINK typed_events_sink FOR typed_events WITH (
    'connector' = 'kafka',
    'topic' = 'typed_events',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
