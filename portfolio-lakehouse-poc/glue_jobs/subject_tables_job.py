"""
Subject Tables Window Update Job

Re-computes per-session window aggregates (reboot count, shutdown count) for
the dwd_device_events_reboot_info and dwd_device_events_shutdown_reason
Iceberg tables and writes the updated values back via Iceberg MERGE INTO.

Why MERGE instead of full partition overwrite:
  Iceberg's MERGE performs file-level rewrites only for data files containing
  matched rows, making it far cheaper than replacing an entire partition when
  only a subset of rows change.

Args (Glue job parameters):
  --JOB_NAME
  --namespace          Glue Data Catalog database / Iceberg namespace
  --catalog_name       Spark catalog name (matches SparkSession config)
  --start_date         YYYY-MM-DD  inclusive lower bound for event_local_date
  --end_date           YYYY-MM-DD  inclusive upper bound for event_local_date
"""

import sys
import logging
import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("subject_tables_job")

# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------
REBOOT_TABLE = "dwd_device_events_reboot_info"
SHUTDOWN_TABLE = "dwd_device_events_shutdown_reason"

# Partition key for per-session window function
SESSION_PARTITION_COLS = [
    "client_variant",
    "device_component_guid",
    "device_session_id",
]


# ---------------------------------------------------------------------------
# Spark initialisation
# ---------------------------------------------------------------------------
def build_spark_session(catalog_name: str) -> tuple:
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session

    spark.conf.set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}.warehouse",
        "s3://analytics-tables-bucket/",
    )
    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )

    logger.info("SparkSession ready, catalog=%s", catalog_name)
    return spark, glue_ctx


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "namespace", "catalog_name", "start_date", "end_date"],
    )
    # Validate date strings
    for date_arg in ("start_date", "end_date"):
        try:
            datetime.date.fromisoformat(args[date_arg])
        except ValueError:
            raise ValueError(
                f"Argument --{date_arg} must be in YYYY-MM-DD format, "
                f"got: {args[date_arg]!r}"
            )
    return args


# ---------------------------------------------------------------------------
# Reboot window computation
# ---------------------------------------------------------------------------
def compute_reboot_window_counts(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Read reboot_info rows for the date window and compute
    single_session_reboot_cnt via a COUNT OVER WINDOW partition.

    The window is unbounded on both sides within each
    (client_variant, device_component_guid, device_session_id) group so
    every row in a session receives the total reboot count for that session.
    """
    full_table = f"{catalog}.{namespace}.{REBOOT_TABLE}"
    logger.info("Reading %s for date range [%s, %s]", full_table, start_date, end_date)

    df = spark.sql(
        f"""
        SELECT
            client_variant,
            device_component_guid,
            device_session_id,
            event_local_date,
            event_name,
            device_mono_time
        FROM {full_table}
        WHERE event_local_date BETWEEN '{start_date}' AND '{end_date}'
        """
    )

    session_window = Window.partitionBy(*SESSION_PARTITION_COLS)

    df_with_counts = df.withColumn(
        "single_session_reboot_cnt",
        F.count("*").over(session_window).cast("int"),
    )

    # De-duplicate: one row per session carrying the count
    # (MERGE will update every matching row regardless of de-dup, but
    #  reducing the source side of MERGE cuts shuffle cost significantly)
    deduped = df_with_counts.select(
        *SESSION_PARTITION_COLS,
        "event_local_date",
        "single_session_reboot_cnt",
    ).dropDuplicates(SESSION_PARTITION_COLS + ["event_local_date"])

    logger.info(
        "Reboot window counts computed: %d session-date combinations",
        deduped.count(),
    )
    return deduped


# ---------------------------------------------------------------------------
# Shutdown window computation
# ---------------------------------------------------------------------------
def compute_shutdown_window_counts(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Read shutdown_reason rows for the date window and compute
    single_session_shutdown_cnt via a COUNT OVER WINDOW partition.
    """
    full_table = f"{catalog}.{namespace}.{SHUTDOWN_TABLE}"
    logger.info("Reading %s for date range [%s, %s]", full_table, start_date, end_date)

    df = spark.sql(
        f"""
        SELECT
            client_variant,
            device_component_guid,
            device_session_id,
            event_local_date,
            event_name,
            device_mono_time
        FROM {full_table}
        WHERE event_local_date BETWEEN '{start_date}' AND '{end_date}'
        """
    )

    session_window = Window.partitionBy(*SESSION_PARTITION_COLS)

    df_with_counts = df.withColumn(
        "single_session_shutdown_cnt",
        F.count("*").over(session_window).cast("int"),
    )

    deduped = df_with_counts.select(
        *SESSION_PARTITION_COLS,
        "event_local_date",
        "single_session_shutdown_cnt",
    ).dropDuplicates(SESSION_PARTITION_COLS + ["event_local_date"])

    logger.info(
        "Shutdown window counts computed: %d session-date combinations",
        deduped.count(),
    )
    return deduped


# ---------------------------------------------------------------------------
# Iceberg MERGE INTO
# ---------------------------------------------------------------------------
def merge_reboot_counts(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    updates_df: DataFrame,
    start_date: str,
    end_date: str,
) -> None:
    """
    Register the reboot update DataFrame as a temp view, then execute
    Iceberg MERGE INTO to update single_session_reboot_cnt in-place.
    """
    updates_df.createOrReplaceTempView("reboot_updates")
    full_table = f"{catalog}.{namespace}.{REBOOT_TABLE}"

    merge_sql = f"""
        MERGE INTO {full_table} AS target
        USING (
            SELECT
                client_variant,
                device_component_guid,
                device_session_id,
                event_local_date,
                single_session_reboot_cnt
            FROM reboot_updates
        ) AS source
        ON  target.client_variant         = source.client_variant
        AND target.device_component_guid  = source.device_component_guid
        AND target.device_session_id      = source.device_session_id
        AND target.event_local_date       = source.event_local_date
        AND target.event_local_date BETWEEN '{start_date}' AND '{end_date}'
        WHEN MATCHED THEN UPDATE SET
            target.single_session_reboot_cnt = source.single_session_reboot_cnt
    """

    logger.info("Executing MERGE INTO %s (reboot counts)", full_table)
    spark.sql(merge_sql)
    logger.info("MERGE complete for %s", full_table)


def merge_shutdown_counts(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    updates_df: DataFrame,
    start_date: str,
    end_date: str,
) -> None:
    """
    Register the shutdown update DataFrame as a temp view, then execute
    Iceberg MERGE INTO to update single_session_shutdown_cnt in-place.
    """
    updates_df.createOrReplaceTempView("shutdown_updates")
    full_table = f"{catalog}.{namespace}.{SHUTDOWN_TABLE}"

    merge_sql = f"""
        MERGE INTO {full_table} AS target
        USING (
            SELECT
                client_variant,
                device_component_guid,
                device_session_id,
                event_local_date,
                single_session_shutdown_cnt
            FROM shutdown_updates
        ) AS source
        ON  target.client_variant         = source.client_variant
        AND target.device_component_guid  = source.device_component_guid
        AND target.device_session_id      = source.device_session_id
        AND target.event_local_date       = source.event_local_date
        AND target.event_local_date BETWEEN '{start_date}' AND '{end_date}'
        WHEN MATCHED THEN UPDATE SET
            target.single_session_shutdown_cnt = source.single_session_shutdown_cnt
    """

    logger.info("Executing MERGE INTO %s (shutdown counts)", full_table)
    spark.sql(merge_sql)
    logger.info("MERGE complete for %s", full_table)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()

    catalog = args["catalog_name"]
    namespace = args["namespace"]
    start_date = args["start_date"]
    end_date = args["end_date"]

    spark, glue_ctx = build_spark_session(catalog)

    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    errors = []

    # ── Reboot info ──────────────────────────────────────────────────────
    try:
        reboot_updates = compute_reboot_window_counts(
            spark, catalog, namespace, start_date, end_date
        )
        merge_reboot_counts(
            spark, catalog, namespace, reboot_updates, start_date, end_date
        )
    except Exception as exc:
        logger.error("Reboot window update failed: %s", exc, exc_info=True)
        errors.append(f"reboot: {exc}")

    # ── Shutdown reason ──────────────────────────────────────────────────
    try:
        shutdown_updates = compute_shutdown_window_counts(
            spark, catalog, namespace, start_date, end_date
        )
        merge_shutdown_counts(
            spark, catalog, namespace, shutdown_updates, start_date, end_date
        )
    except Exception as exc:
        logger.error("Shutdown window update failed: %s", exc, exc_info=True)
        errors.append(f"shutdown: {exc}")

    job.commit()

    if errors:
        raise RuntimeError(
            f"Subject tables job completed with {len(errors)} error(s): {errors}"
        )

    logger.info(
        "Subject tables window update complete for [%s, %s]", start_date, end_date
    )


if __name__ == "__main__":
    main()
