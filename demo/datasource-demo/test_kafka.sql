CREATE STREAM test_kafka_stream AS 
SELECT 1 as test 
INTO test_kafka_sink
WITH (
    'test_kafka_sink.type' = 'kafka',
    'test_kafka_sink.topic' = 'test', 
    'test_kafka_sink.bootstrap.servers' = 'localhost:9092', 
    'test_kafka_sink.value.format' = 'json'
);
