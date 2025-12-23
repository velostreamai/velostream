-- Tier 3: Stream-Stream Join
-- Tests: JOIN two streams with time window
-- Expected: Matched records from both streams

-- Application metadata
-- @name stream_stream_join_demo
-- @description Stream-to-stream temporal join

CREATE STREAM matched_events AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_total,
    o.event_time AS order_time,
    s.shipment_id,
    s.carrier,
    s.tracking_number,
    s.event_time AS shipment_time
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
    AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '24' HOUR
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic.name' = 'test_orders',
    'orders.config_file' = '../configs/orders_source.yaml',

    'shipments.type' = 'kafka_source',
    'shipments.topic.name' = 'test_shipments',
    'shipments.config_file' = '../configs/shipments_source.yaml',

    'matched_events.type' = 'kafka_sink',
    'matched_events.topic.name' = 'test_matched_events',
    'matched_events.config_file' = '../configs/orders_sink.yaml',

    'join.timeout' = '30s'
);
