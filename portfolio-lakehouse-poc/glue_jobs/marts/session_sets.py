"""
Mart: dwd_device_product_session_sets_daily

Aggregates ODS events into per-session-per-day summary rows covering:
  - Session time bounds (first / last device_mono_time)
  - Final Bluetooth device state at end of session
  - BT connect / disconnect event counts within the session

Idempotency: DELETE rows in [start_date, end_date] then INSERT fresh aggregates.

Source table : ods_device_all_events
Target table : dwd_device_product_session_sets_daily
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_TABLE = "ods_device_all_events"
TARGET_TABLE = "dwd_device_product_session_sets_daily"

# Columns that uniquely identify a session within a day
SESSION_KEY_COLS = [
    "client_variant",
    "device_component_guid",
    "device_session_id",
    "event_local_date",
]


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
    Compute session sets for [start_date, end_date] and write to target table.

    Returns:
        int: number of rows written to the target table.
    """
    logger.info(
        "session_sets mart: start=%s, end=%s, ns=%s", start_date, end_date, namespace
    )

    source = f"{catalog_name}.{namespace}.{SOURCE_TABLE}"
    target = f"{catalog_name}.{namespace}.{TARGET_TABLE}"

    # ── 1. Read ODS events for the date window ────────────────────────────
    events_df = spark.sql(
        f"""
        SELECT
            client_variant,
            product_sku,
            product_model_name,
            device_component_guid,
            device_session_id,
            event_local_date,
            device_mono_time,
            bt_device_state,
            event_name,
            client_firmware_version,
            client_version_core
        FROM {source}
        WHERE event_local_date BETWEEN '{start_date}' AND '{end_date}'
          AND decode_success = true
        """
    )

    if events_df.rdd.isEmpty():
        logger.info("No ODS events found for date range — skipping mart.")
        return 0

    # ── 2. Window definitions ─────────────────────────────────────────────
    session_window = Window.partitionBy(*SESSION_KEY_COLS)

    # For LAST_VALUE we need an ordered window (by mono_time) to find the
    # final BT state reliably
    session_ordered_window = (
        Window.partitionBy(*SESSION_KEY_COLS)
        .orderBy("device_mono_time")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    # ── 3. Compute window aggregates ──────────────────────────────────────
    windowed_df = (
        events_df
        .withColumn(
            "session_mono_time_start",
            F.min("device_mono_time").over(session_window),
        )
        .withColumn(
            "session_mono_time_end",
            F.max("device_mono_time").over(session_window),
        )
        .withColumn(
            "session_event_count",
            F.count("*").over(session_window),
        )
        # Final (last) BT device state in the session — ignore nulls
        .withColumn(
            "final_bt_device_state",
            F.last(
                F.when(F.col("bt_device_state").isNotNull(), F.col("bt_device_state")),
                ignorenulls=True,
            ).over(session_ordered_window),
        )
        # BT connection event counts
        .withColumn(
            "bt_connect_count",
            F.count(
                F.when(F.col("bt_device_state") == "connected", F.lit(1))
            ).over(session_window),
        )
        .withColumn(
            "bt_disconnect_count",
            F.count(
                F.when(F.col("bt_device_state") == "disconnected", F.lit(1))
            ).over(session_window),
        )
    )

    # ── 4. Collapse to one row per session-day ────────────────────────────
    session_sets_df = (
        windowed_df
        .groupBy(
            *SESSION_KEY_COLS,
            "product_sku",
            "product_model_name",
            "client_firmware_version",
            "client_version_core",
        )
        .agg(
            F.first("session_mono_time_start").alias("session_mono_time_start"),
            F.first("session_mono_time_end").alias("session_mono_time_end"),
            F.first("session_event_count").alias("session_event_count"),
            F.first("final_bt_device_state").alias("final_bt_device_state"),
            F.first("bt_connect_count").alias("bt_connect_count"),
            F.first("bt_disconnect_count").alias("bt_disconnect_count"),
        )
        .withColumn("ingest_timestamp", F.current_timestamp())
    )

    row_count = session_sets_df.count()
    logger.info("Computed %d session-day rows", row_count)

    if row_count == 0:
        logger.info("No session rows produced — skipping write.")
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
        session_sets_df
        .writeTo(target)
        .option("write.format.default", "parquet")
        .option("write.target-file-size-bytes", str(128 * 1024 * 1024))
        .append()
    )

    logger.info("session_sets mart complete: %d rows written to %s", row_count, target)
    return row_count
