-- Tier 3: Stream-Stream JOIN
-- Tests: JOIN two Kafka streams within time window
-- Expected: Matching records from both streams

-- Application metadata
-- @name stream_stream_join_demo
-- @description Join order stream with shipment stream

-- Stream source: orders
CREATE SOURCE orders (
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
    'topic' = 'orders_join',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Stream source: shipments
CREATE SOURCE shipments (
    shipment_id STRING,
    order_id STRING,
    carrier STRING,
    tracking_number STRING,
    shipped_at TIMESTAMP,
    event_time TIMESTAMP
) WITH (
    'connector' = 'kafka',
    'topic' = 'shipments_join',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Join orders with shipments within 1-hour window
CREATE STREAM order_shipments AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    s.shipment_id,
    s.carrier,
    s.tracking_number,
    s.shipped_at,
    o.event_time AS order_time
FROM orders o
INNER JOIN shipments s ON o.order_id = s.order_id
    AND s.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '1' HOUR;

-- Sink definition
CREATE SINK order_shipments_sink FOR order_shipments WITH (
    'connector' = 'kafka',
    'topic' = 'order_shipments',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
