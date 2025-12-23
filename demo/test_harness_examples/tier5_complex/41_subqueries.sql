-- Tier 5: Subqueries
-- Tests: IN (SELECT ...), EXISTS patterns
-- Expected: Correct subquery evaluation

-- Application metadata
-- @name subqueries_demo
-- @description Subquery patterns for filtering

-- Filter orders from VIP customers using subquery pattern
CREATE STREAM vip_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price AS order_total,
    o.status,
    o.event_time
FROM all_orders o
WHERE o.customer_id IN (
    SELECT customer_id FROM vip_customers WHERE tier IN ('gold', 'platinum')
)
EMIT CHANGES
WITH (
    'all_orders.type' = 'kafka_source',
    'all_orders.topic.name' = 'test_all_orders',
    'all_orders.config_file' = '../configs/orders_source.yaml',

    'vip_customers.type' = 'file_source',
    'vip_customers.config_file' = '../configs/customers_table.yaml',

    'vip_orders.type' = 'kafka_sink',
    'vip_orders.topic.name' = 'test_vip_orders',
    'vip_orders.config_file' = '../configs/orders_sink.yaml'
);
