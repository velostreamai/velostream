-- Tier 3: Stream-Table Join
-- Tests: JOIN stream with reference table
-- Expected: Enriched stream with lookup data

-- Application metadata
-- @name stream_table_join_demo
-- @description Stream enrichment via table lookup

CREATE STREAM enriched_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price AS order_total,
    p.product_name,
    p.category,
    p.supplier_id,
    o.event_time
FROM orders o
LEFT JOIN products p ON o.product_id = p.product_id
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic.name' = 'test_orders',
    'orders.config_file' = 'configs/orders_source.yaml',

    'products.type' = 'file_source',
    'products.config_file' = 'configs/products_table.yaml',

    'enriched_orders.type' = 'kafka_sink',
    'enriched_orders.topic.name' = 'test_enriched_orders',
    'enriched_orders.config_file' = 'configs/orders_sink.yaml'
);
