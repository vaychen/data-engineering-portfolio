-- sql/dml/dim_device_model_refresh.sql
--
-- Full-refresh of the dim_device_model dimension table.
--
-- Derives distinct mobile client device models from nova app events using
-- the client_os_name and client_os_version columns carried in
-- ods_nova_app_events.  The model name is inferred from client_os_name
-- (which stores the device hardware model string as reported by the SDK).
--
-- Pattern: TRUNCATE + INSERT (full-refresh; table is DISTSTYLE ALL so
-- cross-DAG reads are never stale).

TRUNCATE TABLE analytics_dw.dim_device_model;

INSERT INTO analytics_dw.dim_device_model (
    device_model_key,
    device_model_name,
    manufacturer,
    platform,
    os_version_min,
    os_version_max,
    first_seen_date,
    last_seen_date,
    is_active
)
WITH device_agg AS (
    SELECT
        client_os_name                          AS device_model_name,
        -- Infer platform from OS name prefix
        CASE
            WHEN client_os_name ILIKE 'iphone%'
              OR client_os_name ILIKE 'ipad%'  THEN 'iOS'
            ELSE 'Android'
        END                                     AS platform,
        -- Infer manufacturer
        CASE
            WHEN client_os_name ILIKE 'iphone%'
              OR client_os_name ILIKE 'ipad%'  THEN 'Apple'
            WHEN client_os_name ILIKE 'samsung%' THEN 'Samsung'
            WHEN client_os_name ILIKE 'pixel%'   THEN 'Google'
            WHEN client_os_name ILIKE 'oneplus%' THEN 'OnePlus'
            ELSE 'Other'
        END                                     AS manufacturer,
        MIN(client_os_version)                  AS os_version_min,
        MAX(client_os_version)                  AS os_version_max,
        MIN(partition_date)                     AS first_seen_date,
        MAX(partition_date)                     AS last_seen_date,
        -- Mark as active if seen within the last 90 days
        CASE
            WHEN MAX(partition_date) >= DATEADD(day, -90, CURRENT_DATE)
            THEN TRUE ELSE FALSE
        END                                     AS is_active
    FROM analytics_dw.ods_nova_app_events
    WHERE client_os_name IS NOT NULL
      AND client_os_name <> ''
    GROUP BY
        client_os_name,
        CASE
            WHEN client_os_name ILIKE 'iphone%'
              OR client_os_name ILIKE 'ipad%'  THEN 'iOS'
            ELSE 'Android'
        END,
        CASE
            WHEN client_os_name ILIKE 'iphone%'
              OR client_os_name ILIKE 'ipad%'  THEN 'Apple'
            WHEN client_os_name ILIKE 'samsung%' THEN 'Samsung'
            WHEN client_os_name ILIKE 'pixel%'   THEN 'Google'
            WHEN client_os_name ILIKE 'oneplus%' THEN 'OnePlus'
            ELSE 'Other'
        END
)
SELECT
    ROW_NUMBER() OVER (ORDER BY platform, manufacturer, device_model_name) AS device_model_key,
    device_model_name,
    manufacturer,
    platform,
    os_version_min,
    os_version_max,
    first_seen_date,
    last_seen_date,
    is_active
FROM device_agg;
