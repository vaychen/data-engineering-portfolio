"""
ODS Ingestion Job: Raw S3 → ODS Iceberg table + subject tables

Reads raw gzip JSON telemetry records from S3, decodes protobuf payloads via a
DynamoDB + S3 schema registry, and writes to Apache Iceberg tables hosted on
Amazon S3 Tables using the AWS Glue Data Catalog.

Run modes:
  incremental  — Glue job bookmark tracks last-processed S3 prefix (default)
  backfill     — reads a specific date partition, ignores bookmark
"""

import sys
import json
import base64
import logging
import datetime
import time

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    BooleanType, DoubleType, IntegerType, TimestampType,
)
import boto3

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("ods_job")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SOURCE_PREFIX = "source=device/"
ODS_TABLE = "ods_device_all_events"
INVALID_TABLE = "ods_device_invalid_events"

# Subject table routing: filter column value → target table name
SUBJECT_TABLE_ROUTES = {
    "reboot_info":       "dwd_device_events_reboot_info",
    "shutdown_reason":   "dwd_device_events_shutdown_reason",
    "ota_status":        "dwd_device_events_ota_status",
    "bt_connection":     "dwd_device_events_bt_connection",
}

CLOUDWATCH_NAMESPACE = "Analytics/GlueJobs"


# ---------------------------------------------------------------------------
# Schema registry — executor-local singleton
# ---------------------------------------------------------------------------
class SchemaCache:
    """
    Per-executor singleton that resolves schema_id → protobuf message class.

    Lookup order (all results are cached in-process for the partition lifetime):
      1. In-memory dict (_cache)
      2. DynamoDB GetItem  → descriptor_s3_uri, message_full_name
      3. S3 GetObject      → compiled FileDescriptorSet (.pb bytes)
      4. descriptor_pool   → MessageFactory → message class
    """

    _instance = None

    def __init__(self, registry_table: str, schema_bucket: str) -> None:
        self._registry_table = registry_table
        self._schema_bucket = schema_bucket
        self._cache: dict = {}          # schema_id → message class
        self._dynamo = boto3.client("dynamodb", region_name="us-east-1")
        self._s3 = boto3.client("s3", region_name="us-east-1")
        logger.info(
            "SchemaCache initialised (registry=%s, bucket=%s)",
            registry_table,
            schema_bucket,
        )

    @classmethod
    def get_instance(cls, registry_table: str, schema_bucket: str) -> "SchemaCache":
        if cls._instance is None:
            cls._instance = cls(registry_table, schema_bucket)
        return cls._instance

    # ------------------------------------------------------------------
    def get_message_class(self, schema_id: str):
        """Return the protobuf message class for *schema_id*, with caching."""
        if schema_id in self._cache:
            return self._cache[schema_id]

        # 1. DynamoDB lookup
        response = self._dynamo.get_item(
            TableName=self._registry_table,
            Key={"schema_id": {"S": schema_id}},
            ProjectionExpression="descriptor_s3_uri, message_full_name",
        )
        item = response.get("Item")
        if not item:
            raise KeyError(f"schema_id not found in registry: {schema_id!r}")

        descriptor_s3_uri: str = item["descriptor_s3_uri"]["S"]
        message_full_name: str = item["message_full_name"]["S"]

        # 2. Fetch compiled .pb FileDescriptorSet from S3
        # URI format: s3://<bucket>/<key>
        _, _, rest = descriptor_s3_uri.partition("://")
        bucket, _, key = rest.partition("/")
        s3_resp = self._s3.get_object(Bucket=bucket, Key=key)
        pb_bytes = s3_resp["Body"].read()

        # 3. Load into descriptor pool
        from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

        file_descriptor_set = descriptor_pb2.FileDescriptorSet()
        file_descriptor_set.ParseFromString(pb_bytes)

        pool = descriptor_pool.DescriptorPool()
        for file_proto in file_descriptor_set.file:
            pool.Add(file_proto)

        descriptor = pool.FindMessageTypeByName(message_full_name)
        factory = message_factory.MessageFactory(pool=pool)
        msg_class = factory.GetPrototype(descriptor)

        self._cache[schema_id] = msg_class
        logger.debug("Loaded schema %s → %s", schema_id, message_full_name)
        return msg_class

    # ------------------------------------------------------------------
    def decode_payload(self, schema_id: str, b64_payload: str):
        """
        Decode a base64-encoded protobuf payload.

        Returns:
            (payload_json: str | None, decode_success: bool)
        """
        try:
            from google.protobuf.json_format import MessageToDict

            raw_bytes = base64.b64decode(b64_payload)
            msg_class = self.get_message_class(schema_id)
            msg = msg_class()
            msg.ParseFromString(raw_bytes)
            payload_dict = MessageToDict(
                msg,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
            return json.dumps(payload_dict, ensure_ascii=False), True
        except Exception as exc:
            logger.warning("decode_payload failed for schema_id=%s: %s", schema_id, exc)
            return None, False


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args():
    required = [
        "JOB_NAME",
        "raw_bucket",
        "namespace",
        "catalog_name",
        "schema_registry_table",
        "schema_bucket",
    ]
    optional_defaults = {
        "run_mode": "incremental",       # incremental | backfill
        "start_date": "",                # YYYY-MM-DD, required for backfill
        "job_bookmark_option": "job-bookmark-enable",
    }

    args = getResolvedOptions(sys.argv, required)
    # Merge optional args that may or may not be present on the command line
    for key, default in optional_defaults.items():
        flag = f"--{key}"
        if flag in sys.argv:
            extra = getResolvedOptions(sys.argv, [key])
            args[key] = extra[key]
        else:
            args[key] = default

    logger.info("Job args: %s", {k: v for k, v in args.items() if "secret" not in k})
    return args


# ---------------------------------------------------------------------------
# Spark / Glue context initialisation
# ---------------------------------------------------------------------------
def build_spark_session(catalog_name: str, warehouse_bucket: str) -> SparkSession:
    """
    Create a SparkSession configured for Iceberg on S3 Tables via GlueCatalog.
    The Glue job execution environment already has the Iceberg JARs on the
    classpath (Glue 5.0 bundles them); we only need to set catalog properties.
    """
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
        f"s3://{warehouse_bucket}/",
    )
    spark.conf.set(
        f"spark.sql.catalog.{catalog_name}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    # Enable dynamic partition overwrite (used by some mart patterns)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    logger.info("SparkSession configured with catalog: %s", catalog_name)
    return spark, glue_ctx


# ---------------------------------------------------------------------------
# Raw data reading
# ---------------------------------------------------------------------------
def read_raw_records(
    spark: SparkSession,
    glue_ctx: GlueContext,
    raw_bucket: str,
    run_mode: str,
    start_date: str,
    job_bookmark_option: str,
) -> DataFrame:
    """
    Read raw gzip-compressed JSON records from S3.

    Incremental mode: Glue job bookmark tracks the last S3 prefix processed.
    Backfill mode:    reads a specific date partition, bookmark is ignored.
    """
    if run_mode == "backfill":
        if not start_date:
            raise ValueError("start_date is required when run_mode=backfill")
        # Partition layout: source=device/year=YYYY/month=MM/day=DD/
        dt = datetime.date.fromisoformat(start_date)
        s3_path = (
            f"s3://{raw_bucket}/{SOURCE_PREFIX}"
            f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        )
        logger.info("Backfill mode: reading from %s", s3_path)
        df = spark.read.option("compression", "gzip").json(s3_path)
    else:
        # Incremental: use Glue DynamicFrame with bookmark, then convert
        logger.info("Incremental mode: reading with Glue job bookmark")
        s3_path = f"s3://{raw_bucket}/{SOURCE_PREFIX}"
        datasource = glue_ctx.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [s3_path],
                "recurse": True,
                "groupFiles": "inPartition",
                "groupSize": "134217728",  # 128 MB grouping
            },
            format="json",
            format_options={"withHeader": False},
            transformation_ctx="raw_s3_source",
        )
        df = datasource.toDF()

    logger.info("Raw records schema: %s", df.schema.simpleString())
    return df


# ---------------------------------------------------------------------------
# Flattening
# ---------------------------------------------------------------------------
def flatten_records(df: DataFrame) -> DataFrame:
    """
    Flatten nested JSON structs into a single-level DataFrame.

    Expected top-level columns in raw JSON:
      event_name, event_timestamp_ms, schema_id, raw_payload,
      client_ctx   { variant, firmware_version, locale, ... }
      device_ctx   { component_guid, session_id, mono_time, ... }
      product_ctx  { sku, model_name, hw_revision, ... }
    """
    # Materialise any string-type JSON columns as structs if needed
    # (Glue/Spark may read nested objects as strings depending on format options)
    for ctx_col in ("client_ctx", "device_ctx", "product_ctx"):
        if ctx_col in df.columns:
            col_type = dict(df.dtypes).get(ctx_col, "string")
            if col_type == "string":
                df = df.withColumn(ctx_col, F.from_json(F.col(ctx_col), "map<string,string>"))

    flat = df.select(
        # Top-level scalars
        F.col("event_name"),
        F.col("event_timestamp_ms").cast(LongType()).alias("event_timestamp_ms"),
        F.col("schema_id"),
        F.col("raw_payload"),
        # client_ctx
        F.col("client_ctx.variant").alias("client_variant"),
        F.col("client_ctx.firmware_version").alias("client_firmware_version"),
        F.col("client_ctx.locale").alias("client_locale"),
        F.col("client_ctx.platform").alias("client_platform"),
        # device_ctx
        F.col("device_ctx.component_guid").alias("device_component_guid"),
        F.col("device_ctx.session_id").alias("device_session_id"),
        F.col("device_ctx.mono_time").cast(LongType()).alias("device_mono_time"),
        F.col("device_ctx.schema_name").alias("device_schema_name_raw"),
        # product_ctx
        F.col("product_ctx.sku").alias("product_sku"),
        F.col("product_ctx.model_name").alias("product_model_name"),
        F.col("product_ctx.hw_revision").alias("product_hw_revision"),
        # Ingest metadata
        F.current_timestamp().alias("ingest_timestamp"),
    )

    return flat


# ---------------------------------------------------------------------------
# Protobuf decode via mapPartitions
# ---------------------------------------------------------------------------
def decode_protobuf_column(
    df: DataFrame,
    registry_table: str,
    schema_bucket: str,
) -> DataFrame:
    """
    Decode the base64 protobuf payload column using mapPartitions.

    mapPartitions is preferred over a per-row UDF because SchemaCache is
    initialised once per partition, amortising DynamoDB/S3 I/O across all
    rows in the partition.

    Adds two columns:
      payload_json   (StringType)  — decoded protobuf as JSON string, or null
      decode_success (BooleanType) — True if decode succeeded
    """
    # Capture broadcast-friendly references (no SparkContext in closures)
    _registry_table = registry_table
    _schema_bucket = schema_bucket

    input_schema = df.schema
    output_schema = StructType(
        input_schema.fields
        + [
            StructField("payload_json", StringType(), True),
            StructField("decode_success", BooleanType(), False),
        ]
    )

    def decode_partition(rows):
        cache = SchemaCache.get_instance(_registry_table, _schema_bucket)
        for row in rows:
            row_dict = row.asDict()
            schema_id = row_dict.get("schema_id") or ""
            raw_payload = row_dict.get("raw_payload") or ""
            if schema_id and raw_payload:
                payload_json, success = cache.decode_payload(schema_id, raw_payload)
            else:
                payload_json, success = None, False
            row_dict["payload_json"] = payload_json
            row_dict["decode_success"] = success
            yield Row(**row_dict)

    decoded_rdd = df.rdd.mapPartitions(decode_partition)
    return df.sparkSession.createDataFrame(decoded_rdd, schema=output_schema)


# ---------------------------------------------------------------------------
# ODS column enrichment
# ---------------------------------------------------------------------------
def build_ods_df(df: DataFrame) -> DataFrame:
    """
    Add computed/derived columns to produce the full ODS schema.

    Derived columns:
      event_local_timestamp  — event_timestamp_ms converted to UTC+8
      event_local_date       — date part of event_local_timestamp
      client_version_core    — major.minor.patch from semver string
      device_schema_name     — prefix before first '-' in schema_id
      bt_device_state        — extracted from decoded payload_json
    """
    # UTC+8 offset in seconds
    utc8_offset_seconds = 8 * 3600

    df = df.withColumn(
        "event_local_timestamp",
        (
            F.from_unixtime(F.col("event_timestamp_ms") / 1000 + utc8_offset_seconds)
            .cast(TimestampType())
        ),
    )

    df = df.withColumn(
        "event_local_date",
        F.to_date(F.col("event_local_timestamp")),
    )

    # Semver: take first 3 dot-separated components
    df = df.withColumn(
        "client_version_core",
        F.regexp_extract(F.col("client_firmware_version"), r"^(\d+\.\d+\.\d+)", 1),
    )

    # schema_id format: "<schema_name>-<version>", e.g. "reboot_info-v3"
    df = df.withColumn(
        "device_schema_name",
        F.split(F.col("schema_id"), "-").getItem(0),
    )

    # Extract bt_device_state from decoded JSON payload
    df = df.withColumn(
        "bt_device_state",
        F.get_json_object(F.col("payload_json"), "$.bt_device_state"),
    )

    return df


# ---------------------------------------------------------------------------
# Iceberg write helpers
# ---------------------------------------------------------------------------
def write_to_iceberg(
    df: DataFrame,
    catalog: str,
    namespace: str,
    table_name: str,
    partition_by: str = "event_local_date",
) -> int:
    """Append DataFrame to an Iceberg table, returning row count written."""
    full_table = f"{catalog}.{namespace}.{table_name}"
    count = df.count()
    logger.info("Writing %d rows → %s", count, full_table)
    (
        df.writeTo(full_table)
        .option("write.format.default", "parquet")
        .option("write.target-file-size-bytes", str(128 * 1024 * 1024))
        .append()
    )
    logger.info("Write complete → %s", full_table)
    return count


def fan_out_to_subject_tables(
    df: DataFrame,
    catalog: str,
    namespace: str,
) -> dict:
    """
    Route ODS events to per-event-type subject tables.

    Only rows with decode_success=True and a known event_name are routed.
    Returns a dict of {table_name: row_count}.
    """
    results = {}
    valid_df = df.filter(F.col("decode_success") == True)  # noqa: E712

    for event_name, table_name in SUBJECT_TABLE_ROUTES.items():
        subset = valid_df.filter(F.col("event_name") == event_name)
        count = write_to_iceberg(subset, catalog, namespace, table_name)
        results[table_name] = count
        logger.info("Fan-out: %s → %s rows", table_name, count)

    return results


# ---------------------------------------------------------------------------
# CloudWatch metric emission
# ---------------------------------------------------------------------------
def emit_metric(metric_name: str, value: float, unit: str = "Count") -> None:
    try:
        cw = boto3.client("cloudwatch", region_name="us-east-1")
        cw.put_metric_data(
            Namespace=CLOUDWATCH_NAMESPACE,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": [{"Name": "JobName", "Value": "ods_job"}],
                }
            ],
        )
    except Exception as exc:
        logger.warning("Failed to emit CloudWatch metric %s: %s", metric_name, exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()

    job_start = time.time()

    spark, glue_ctx = build_spark_session(
        catalog_name=args["catalog_name"],
        warehouse_bucket="analytics-tables-bucket",
    )

    # Initialise Glue Job for bookmark management
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    try:
        # ── 1. Read raw records ──────────────────────────────────────────
        raw_df = read_raw_records(
            spark=spark,
            glue_ctx=glue_ctx,
            raw_bucket=args["raw_bucket"],
            run_mode=args["run_mode"],
            start_date=args["start_date"],
            job_bookmark_option=args["job_bookmark_option"],
        )

        if raw_df.rdd.isEmpty():
            logger.info("No new records found. Committing bookmark and exiting.")
            job.commit()
            return

        total_raw = raw_df.count()
        logger.info("Total raw records: %d", total_raw)
        emit_metric("RawRecordsRead", total_raw)

        # ── 2. Flatten nested structs ────────────────────────────────────
        flat_df = flatten_records(raw_df)

        # ── 3. Protobuf decode via mapPartitions ─────────────────────────
        decoded_df = decode_protobuf_column(
            flat_df,
            registry_table=args["schema_registry_table"],
            schema_bucket=args["schema_bucket"],
        )

        # ── 4. ODS column enrichment ─────────────────────────────────────
        ods_df = build_ods_df(decoded_df)

        # Cache the enriched DataFrame — it will be scanned multiple times
        ods_df.cache()

        # ── 5. Split valid / invalid ─────────────────────────────────────
        valid_df = ods_df.filter(F.col("decode_success") == True)   # noqa: E712
        invalid_df = ods_df.filter(F.col("decode_success") == False)  # noqa: E712

        decode_fail_count = invalid_df.count()
        decode_ok_count = valid_df.count()
        logger.info(
            "Decode results: ok=%d, failed=%d", decode_ok_count, decode_fail_count
        )
        emit_metric("DecodeSuccessCount", decode_ok_count)
        emit_metric("DecodeFailCount", decode_fail_count)

        # ── 6. Write ODS table (all records including decode failures) ────
        ods_count = write_to_iceberg(
            ods_df, args["catalog_name"], args["namespace"], ODS_TABLE
        )
        emit_metric("OdsRowsWritten", ods_count)

        # ── 7. Write invalid events sidecar ─────────────────────────────
        if decode_fail_count > 0:
            write_to_iceberg(
                invalid_df, args["catalog_name"], args["namespace"], INVALID_TABLE
            )

        # ── 8. Fan out to subject tables ─────────────────────────────────
        subject_counts = fan_out_to_subject_tables(
            valid_df, args["catalog_name"], args["namespace"]
        )
        for tbl, cnt in subject_counts.items():
            emit_metric(f"SubjectTable_{tbl}", cnt)

        # ── 9. Emit job duration ─────────────────────────────────────────
        elapsed_s = time.time() - job_start
        emit_metric("JobDurationSeconds", elapsed_s, unit="Seconds")
        logger.info("Job completed in %.1f seconds", elapsed_s)

    except Exception:
        logger.exception("ODS job failed")
        raise
    finally:
        # Commit bookmark only on clean exit (exception propagates before this
        # when raise is used above, so bookmark is not advanced on failure)
        pass

    job.commit()
    logger.info("Glue job bookmark committed.")


if __name__ == "__main__":
    main()
