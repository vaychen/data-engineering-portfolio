-- sql/dml/dim_date_refresh.sql
--
-- Full-refresh of the dim_date dimension table.
--
-- Redshift does not support WITH RECURSIVE, so the date spine is generated
-- by cross-joining four digit CTEs (0-9) to produce 10,000 candidate rows,
-- then filtering to the desired 8-year window (~2922 rows).
--
-- This approach is set-based, push-down friendly, and avoids any system
-- table or pg_catalog hacks.
--
-- Covers: CURRENT_DATE - 4 years  →  CURRENT_DATE + 4 years

TRUNCATE TABLE analytics_dw.dim_date;

INSERT INTO analytics_dw.dim_date (
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_year,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend,
    is_weekday,
    fiscal_year,
    fiscal_quarter
)
WITH digits AS (
    SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
    UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7
    UNION ALL SELECT 8 UNION ALL SELECT 9
),
sequence AS (
    SELECT
        (d3.d * 1000 + d2.d * 100 + d1.d * 10 + d0.d) AS seq
    FROM digits d0
    CROSS JOIN digits d1
    CROSS JOIN digits d2
    CROSS JOIN digits d3
),
date_spine AS (
    SELECT
        DATEADD(day, seq, DATEADD(year, -4, DATE_TRUNC('year', CURRENT_DATE))) AS full_date
    FROM sequence
    WHERE seq < 3660  -- ~10 years; filter below narrows to 8-year window
),
filtered AS (
    SELECT full_date
    FROM date_spine
    WHERE full_date BETWEEN
        DATEADD(year, -4, CURRENT_DATE)
        AND DATEADD(year,  4, CURRENT_DATE)
)
SELECT
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INTEGER)     AS date_key,
    full_date,
    EXTRACT(year    FROM full_date)::SMALLINT            AS year,
    EXTRACT(quarter FROM full_date)::SMALLINT            AS quarter,
    EXTRACT(month   FROM full_date)::SMALLINT            AS month,
    TO_CHAR(full_date, 'Month')                          AS month_name,
    EXTRACT(week    FROM full_date)::SMALLINT            AS week_of_year,
    EXTRACT(doy     FROM full_date)::SMALLINT            AS day_of_year,
    EXTRACT(day     FROM full_date)::SMALLINT            AS day_of_month,
    EXTRACT(dow     FROM full_date)::SMALLINT            AS day_of_week,  -- 0=Sun
    TO_CHAR(full_date, 'Day')                            AS day_name,
    CASE WHEN EXTRACT(dow FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END  AS is_weekend,
    CASE WHEN EXTRACT(dow FROM full_date) NOT IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekday,
    EXTRACT(year    FROM full_date)::SMALLINT            AS fiscal_year,
    EXTRACT(quarter FROM full_date)::SMALLINT            AS fiscal_quarter
FROM filtered
ORDER BY full_date;
