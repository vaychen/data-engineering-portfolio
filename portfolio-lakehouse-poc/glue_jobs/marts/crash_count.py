"""
Mart: dws_device_product_crash_session_count_daily

Detects firmware crashes / panics by identifying monotonic-time regressions
across consecutive events within the same device session, then aggregates
crash counts and session counts per (event_local_date, device_component_guid,
product_firmware_version).

Crash detection logic:
  A "crash" (panic / unexpected restart) is indicated when
      LAG(device_mono_time) > device_mono_time
  within the ordered event stream for a device component.  A mono_time
  regression means the device's uptime counter reset — i.e. the device
  restarted without a clean shutdown event.

Derived indicators:
  panic_user_experience_ind     — the crash was preceded by an active BT
                                  connection (user was actively using the
                                  device when it crashed)
  panic_product_experience_ind  — the crash occurred during an active session
                                  (device_session_id was populated)

Idempotency: DELETE rows in [start_date, end_date] then INSERT fresh aggregates.

Source tables:
  ods_device_all_events     (event stream)
  dim_firmware_release      (firmware metadata for version format enrichment)

Target table:
  dws_device_product_crash_session_count_daily
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import BooleanType, IntegerType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_TABLE = "ods_device_all_events"
FIRMWARE_DIM_TABLE = "dim_firmware_release"
TARGET_TABLE = "dws_device_product_crash_session_count_daily"

# Window ordering for LAG-based crash detection
DEVICE_TIME_WINDOW = (
    Window
    .partitionBy("device_component_guid")
    .orderBy("event_local_date", "device_mono_time")
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _load_events(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    start_date: str,
    end_date: str,
) -> DataFrame:
    source = f"{catalog}.{namespace}.{SOURCE_TABLE}"
    logger.info("Loading events from %s for [%s, %s]", source, start_date, end_date)

    return spark.sql(
        f"""
        SELECT
            event_local_date,
            device_component_guid,
            device_session_id,
            device_mono_time,
            client_variant,
            client_firmware_version,
            client_version_core,
            product_sku,
            product_model_name,
            bt_device_state,
            event_name
        FROM {source}
        WHERE event_local_date BETWEEN '{start_date}' AND '{end_date}'
          AND decode_success = true
        """
    )


def _load_firmware_dim(
    spark: SparkSession,
    catalog: str,
    namespace: str,
) -> DataFrame:
    dim = f"{catalog}.{namespace}.{FIRMWARE_DIM_TABLE}"
    logger.info("Loading firmware dim from %s", dim)

    return spark.sql(
        f"""
        SELECT
            firmware_version,
            firmware_version_format,
            release_channel,
            release_date
        FROM {dim}
        """
    )


# ---------------------------------------------------------------------------
# Crash detection
# ---------------------------------------------------------------------------
def detect_crashes(events_df: DataFrame) -> DataFrame:
    """
    Apply LAG-based crash detection within each device component stream.

    Returns the input DataFrame with additional columns:
      prev_mono_time              — LAG(device_mono_time, 1)
      prev_bt_device_state        — BT state of the preceding event
      is_crash                    — True when prev_mono_time > device_mono_time
      panic_user_experience_ind   — crash preceded by bt_device_state='connected'
      panic_product_experience_ind — crash during an active session (non-null session_id)
    """
    df = events_df.withColumn(
        "prev_mono_time",
        F.lag("device_mono_time", 1).over(DEVICE_TIME_WINDOW),
    ).withColumn(
        "prev_bt_device_state",
        F.lag("bt_device_state", 1).over(DEVICE_TIME_WINDOW),
    )

    # Crash indicator: mono_time went backwards → device rebooted unexpectedly
    df = df.withColumn(
        "is_crash",
        (
            F.col("prev_mono_time").isNotNull()
            & (F.col("prev_mono_time") > F.col("device_mono_time"))
        ).cast(BooleanType()),
    )

    # User experience panic: the event immediately before the crash showed
    # the device was connected to a BT host
    df = df.withColumn(
        "panic_user_experience_ind",
        (
            F.col("is_crash")
            & (F.col("prev_bt_device_state") == "connected")
        ).cast(BooleanType()),
    )

    # Product experience panic: crash happened while the device had an active
    # session (i.e. it was in a meaningful operational state)
    df = df.withColumn(
        "panic_product_experience_ind",
        (
            F.col("is_crash")
            & F.col("device_session_id").isNotNull()
            & (F.col("device_session_id") != "")
        ).cast(BooleanType()),
    )

    return df


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def aggregate_crash_counts(crash_df: DataFrame) -> DataFrame:
    """
    Aggregate crash and session counts per
    (event_local_date, device_component_guid, client_firmware_version).

    Aggregated columns:
      crash_cnt_total           — all detected crashes
      crash_cnt_user            — crashes during BT-connected user sessions
      crash_cnt_product         — crashes during active product sessions
      session_cnt               — distinct device_session_id values seen
      bt_disconnected_cnt       — count of bt_device_state='disconnected' events
      client_variant            — (single value per component guid, taken as first)
      product_sku               — (first)
      product_model_name        — (first)
      client_version_core       — (first)
    """
    return crash_df.groupBy(
        "event_local_date",
        "device_component_guid",
        "client_firmware_version",
    ).agg(
        F.sum(F.col("is_crash").cast(IntegerType())).alias("crash_cnt_total"),
        F.sum(F.col("panic_user_experience_ind").cast(IntegerType())).alias("crash_cnt_user"),
        F.sum(F.col("panic_product_experience_ind").cast(IntegerType())).alias("crash_cnt_product"),
        F.countDistinct("device_session_id").alias("session_cnt"),
        F.sum(
            F.when(F.col("bt_device_state") == "disconnected", F.lit(1)).otherwise(F.lit(0))
        ).alias("bt_disconnected_cnt"),
        F.first("client_variant").alias("client_variant"),
        F.first("product_sku").alias("product_sku"),
        F.first("product_model_name").alias("product_model_name"),
        F.first("client_version_core").alias("client_version_core"),
    )


# ---------------------------------------------------------------------------
# Firmware dim enrichment
# ---------------------------------------------------------------------------
def enrich_with_firmware_dim(
    agg_df: DataFrame,
    firmware_dim_df: DataFrame,
) -> DataFrame:
    """
    Left-join the firmware dimension to add firmware_version_format and
    release_channel for BI-friendly filtering.
    """
    return agg_df.join(
        firmware_dim_df,
        on=agg_df["client_firmware_version"] == firmware_dim_df["firmware_version"],
        how="left",
    ).drop("firmware_version")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def run(
    spark: SparkSession,
    namespace: str,
    catalog_name: str,
    start_date: str,
    end_date: str,
) -> int:
    """
    Compute crash session counts for [start_date, end_date] and write to
    the target DWS table.

    Returns:
        int: number of rows written.
    """
    logger.info(
        "crash_count mart: start=%s, end=%s, ns=%s", start_date, end_date, namespace
    )

    target = f"{catalog_name}.{namespace}.{TARGET_TABLE}"

    # ── 1. Load source data ───────────────────────────────────────────────
    events_df = _load_events(spark, catalog_name, namespace, start_date, end_date)

    if events_df.rdd.isEmpty():
        logger.info("No events found for date range — skipping crash_count mart.")
        return 0

    firmware_dim_df = _load_firmware_dim(spark, catalog_name, namespace)

    # ── 2. Crash detection via LAG ────────────────────────────────────────
    crash_df = detect_crashes(events_df)

    total_crashes = crash_df.filter(F.col("is_crash") == True).count()  # noqa: E712
    logger.info("Detected %d crash events in date range", total_crashes)

    # ── 3. Aggregate ──────────────────────────────────────────────────────
    agg_df = aggregate_crash_counts(crash_df)

    # ── 4. Enrich with firmware dimension ────────────────────────────────
    enriched_df = enrich_with_firmware_dim(agg_df, firmware_dim_df)
    enriched_df = enriched_df.withColumn("ingest_timestamp", F.current_timestamp())

    row_count = enriched_df.count()
    logger.info("Aggregated %d rows for target table", row_count)

    if row_count == 0:
        logger.info("No aggregated rows produced — skipping write.")
        return 0

    # ── 5. Idempotent DELETE + INSERT ─────────────────────────────────────
    logger.info(
        "Deleting existing rows from %s for [%s, %s]", target, start_date, end_date
    )
    spark.sql(
        f"""
        DELETE FROM {target}
        WHERE event_local_date BETWEEN '{start_date}' AND '{end_date}'
        """
    )

    logger.info("Inserting %d rows into %s", row_count, target)
    (
        enriched_df
        .writeTo(target)
        .option("write.format.default", "parquet")
        .option("write.target-file-size-bytes", str(128 * 1024 * 1024))
        .append()
    )

    logger.info(
        "crash_count mart complete: %d rows written to %s", row_count, target
    )
    return row_count
