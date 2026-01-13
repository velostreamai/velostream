-- SQL Application: full_outer_join_demo
-- Version: 1.0.0
-- Description: Reconcile orders with shipments (find orphans)
-- =============================================================================
-- Tier 3: FULL OUTER JOIN (Keep All Records from Both Sides)
-- =============================================================================
--
-- WHAT IS FULL OUTER JOIN?
-- ------------------------
-- FULL OUTER JOIN returns all records from both tables:
--   - Matching records: Combined from both sides
--   - Left-only records: Right columns are NULL
--   - Right-only records: Left columns are NULL
--
-- JOIN COMPARISON:
--   INNER JOIN:      Only matching records
--   LEFT JOIN:       All left + matching right
--   RIGHT JOIN:      All right + matching left
--   FULL OUTER JOIN: All from both + matching combined
--
-- STREAMING USE CASE:
--   - Reconciliation between two data sources
--   - Finding orphaned records in either system
--   - Complete audit trails
--
-- SYNTAX:
--   SELECT ... FROM left_stream
--   FULL OUTER JOIN right_stream ON condition
--
-- =============================================================================

-- @app: full_outer_join_demo
-- @description: Reconcile orders with shipments (find orphans)

CREATE STREAM order_shipment_reconciliation AS
SELECT
    COALESCE(o.order_id, s.order_id) AS order_id,   -- Use whichever exists
    o.customer_id AS customer_id,                    -- NULL if order missing
    o.order_total AS order_total,                    -- NULL if order missing
    o.event_time AS order_time,
    s.shipment_id AS shipment_id,                    -- NULL if shipment missing
    s.carrier AS carrier,                            -- NULL if shipment missing
    s.tracking_number AS tracking_number,            -- NULL if shipment missing
    s.event_time AS shipment_time,
    CASE
        WHEN o.order_id IS NULL THEN 'ORPHAN_SHIPMENT'     -- Shipment without order
        WHEN s.shipment_id IS NULL THEN 'UNSHIPPED_ORDER'  -- Order without shipment
        ELSE 'MATCHED'                                      -- Both exist
    END AS reconciliation_status
FROM orders o
FULL OUTER JOIN shipments s ON o.order_id = s.order_id
EMIT CHANGES
WITH (
    'orders.type' = 'kafka_source',
    'orders.topic.name' = 'test_orders',
    'orders.config_file' = '../configs/orders_source.yaml',

    'shipments.type' = 'kafka_source',
    'shipments.topic.name' = 'test_shipments',
    'shipments.config_file' = '../configs/shipments_source.yaml',

    'order_shipment_reconciliation.type' = 'kafka_sink',
    'order_shipment_reconciliation.topic.name' = 'test_reconciliation',
    'order_shipment_reconciliation.config_file' = '../configs/orders_sink.yaml'
);
