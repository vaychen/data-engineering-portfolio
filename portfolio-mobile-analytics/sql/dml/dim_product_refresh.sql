-- =============================================================================
-- DML: dim_product_refresh.sql
-- Layer  : DIM (Dimension)
-- Table  : analytics_dw.dim_product
-- Pattern: Full DELETE + INSERT (SCD Type 1 — overwrite on refresh)
--
-- Jinja params
--   {{ params.backfill_scan_date }}  — informational; not used in the WHERE
--                                      clause for a full-refresh dimension,
--                                      but retained for template consistency
--                                      and audit logging.
-- =============================================================================

-- Step 1: Full refresh — delete all existing rows.
-- This dimension is small enough that a full DELETE + INSERT is cheaper than
-- row-level merge and avoids SCD Type 2 complexity for this use case.
DELETE FROM analytics_dw.dim_product
WHERE 1 = 1;

-- Step 2: Insert current product catalogue.
-- product_id values use short opaque hex identifiers matching the firmware
-- registration system; product_guid uses a UUID v4 format.
INSERT INTO analytics_dw.dim_product
    (product_id, product_guid, product_name, product_category,
     product_line, launch_date, is_active, updated_at)
VALUES
    ('a1b2c3d4', 'f47ac10b-58cc-4372-a567-0e02b2c3d479',
     'PRODUCT_A', 'wireless_headphones',
     'nova_audio', '2021-03-15', TRUE, GETDATE()),

    ('e5f6a7b8', '550e8400-e29b-41d4-a716-446655440001',
     'PRODUCT_B', 'earbuds',
     'nova_audio', '2022-06-01', TRUE, GETDATE()),

    ('c9d0e1f2', '6ba7b810-9dad-11d1-80b4-00c04fd430c9',
     'PRODUCT_C', 'wireless_headphones',
     'nova_audio_pro', '2023-01-10', TRUE, GETDATE()),

    ('33445566', '6ba7b811-9dad-11d1-80b4-00c04fd430ca',
     'PRODUCT_D', 'over_ear_headphones',
     'nova_audio', '2020-09-22', FALSE, GETDATE()),

    ('77889900', '6ba7b812-9dad-11d1-80b4-00c04fd430cb',
     'PRODUCT_E', 'soundbar',
     'nova_home', '2023-09-05', TRUE, GETDATE());

-- Audit: log the refresh event (backfill_scan_date captured for traceability).
-- In production this INSERT targets a pipeline audit table.
-- SELECT {{ params.backfill_scan_date }} AS backfill_scan_date_param,
--        COUNT(*) AS rows_inserted,
--        GETDATE() AS refreshed_at
-- FROM analytics_dw.dim_product;
