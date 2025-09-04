CREATE SINK test_kafka WITH (datasink='kafka', topic='test', brokers='localhost:9092', format='json') AS SELECT 1 as test;
