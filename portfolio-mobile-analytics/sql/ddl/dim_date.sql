-- sql/ddl/dim_date.sql
--
-- Date dimension table.
-- Populated by dim_date_refresh.sql via a 4× cross-joined digit CTE pattern
-- (Redshift does not support WITH RECURSIVE).
-- Covers a rolling 8-year window (~2922 rows).

CREATE TABLE IF NOT EXISTS analytics_dw.dim_date (
    date_key        INTEGER      NOT NULL ENCODE az64,   -- YYYYMMDD surrogate key
    full_date       DATE         NOT NULL ENCODE az64,
    year            SMALLINT     NOT NULL ENCODE az64,
    quarter         SMALLINT     NOT NULL ENCODE az64,   -- 1-4
    month           SMALLINT     NOT NULL ENCODE az64,   -- 1-12
    month_name      VARCHAR(9)   NOT NULL ENCODE zstd,   -- 'January' … 'December'
    week_of_year    SMALLINT     NOT NULL ENCODE az64,   -- ISO week 1-53
    day_of_year     SMALLINT     NOT NULL ENCODE az64,   -- 1-366
    day_of_month    SMALLINT     NOT NULL ENCODE az64,   -- 1-31
    day_of_week     SMALLINT     NOT NULL ENCODE az64,   -- 0=Sunday … 6=Saturday
    day_name        VARCHAR(9)   NOT NULL ENCODE zstd,   -- 'Sunday' … 'Saturday'
    is_weekend      BOOLEAN      NOT NULL ENCODE raw,
    is_weekday      BOOLEAN      NOT NULL ENCODE raw,
    fiscal_year     SMALLINT     NOT NULL ENCODE az64,   -- calendar year (adjust if fiscal offset needed)
    fiscal_quarter  SMALLINT     NOT NULL ENCODE az64
)
DISTSTYLE ALL
SORTKEY (full_date);
