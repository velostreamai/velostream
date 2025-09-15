-- Test SQL Application
-- This is a simple test to show validation working

-- Valid statement with proper configuration
CREATE STREAM test_stream AS
SELECT 
    id,
    name,
    amount
FROM test_source
WITH (
    'test_source.type' = 'kafka_source',
    'test_source.bootstrap.servers' = 'localhost:9092',
    'test_source.topic' = 'test-topic'
)
INTO test_sink
WITH (
    'test_sink.type' = 'kafka_sink',
    'test_sink.bootstrap.servers' = 'localhost:9092',
    'test_sink.topic' = 'output-topic'
);

-- Invalid statement - missing source configuration
CREATE STREAM invalid_stream AS
SELECT 
    id,
    name
FROM unconfigured_source;