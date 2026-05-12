"""
glue_jobs/marts/product_dim.py

Shared dimension loader for dim_product.

dim_product is a cross-domain dimension used by both the device (firmware)
and app analytics pipelines.  It is populated by a separate refresh
process and read here as a broadcast-friendly lookup.

Schema (mirrors analytics_dw.dim_product — sql/ddl/dim_product.sql):
    product_id          STRING   NOT NULL
    product_guid        STRING   NOT NULL
    product_name        STRING   NOT NULL
    product_category    STRING
    product_line        STRING
    launch_date         DATE
    is_active           BOOLEAN  NOT NULL
    updated_at          TIMESTAMP NOT NULL

Usage:
    from glue_jobs.dims.product_dim import load as load_product_dim

    product_df = load_product_dim(spark, catalog_name, namespace)

    # Join to a fact DataFrame — broadcast hint keeps the shuffle small
    # since dim_product is O(100) rows.
    from pyspark.sql import functions as F
    result = fact_df.join(
        F.broadcast(product_df),
        on="product_id",
        how="left",
    )
"""

import logging
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)

DIM_TABLE = "dim_product"


def load(
    spark: SparkSession,
    catalog_name: str,
    namespace: str,
) -> DataFrame:
    """
    Load the full dim_product dimension from the Iceberg catalog.

    The table is small (O(100) rows) — callers should wrap the result in
    ``F.broadcast()`` when joining to large fact tables to avoid a shuffle.

    Parameters
    ----------
    spark:
        Active SparkSession with the Iceberg catalog already configured.
    catalog_name:
        Glue catalog name (e.g. ``"s3tables_glue_catalog"``).
    namespace:
        Iceberg namespace within the catalog (e.g. ``"analytics_curated_dev"``).

    Returns
    -------
    DataFrame
        Columns: product_id, product_guid, product_name, product_category,
        product_line, launch_date, is_active, updated_at.
    """
    table = f"{catalog_name}.{namespace}.{DIM_TABLE}"
    logger.info("Loading %s", table)

    df = spark.sql(
        f"""
        SELECT
            product_id,
            product_guid,
            product_name,
            product_category,
            product_line,
            launch_date,
            is_active,
            updated_at
        FROM {table}
        WHERE is_active = TRUE
        """
    )

    count = df.count()
    logger.info("Loaded %d rows from %s", count, table)
    return df
