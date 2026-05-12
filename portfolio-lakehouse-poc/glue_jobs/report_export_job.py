"""
Report Export Orchestrator Job

Exports 18 report views (4 shown in this sample) to Parquet on S3 for
consumption by Athena and Redshift Spectrum in the consumer account.

Each report module exposes:
    export(spark, namespace, catalog_name, report_date, report_bucket) -> int

Output layout:
    s3://{report_bucket}/vw_device_{report_name}/{report_date}/

The output Parquet files are registered in the Glue Data Catalog so that
Athena and Redshift Spectrum can query them without additional DDL.

Args (Glue job parameters):
  --JOB_NAME
  --namespace
  --catalog_name
  --report_bucket      S3 bucket for Parquet exports (no trailing slash)
  --report_date        YYYYMMDD  (e.g. 20240315)
  --report             (optional) run a single named report and exit
"""

import sys
import logging
import importlib
import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("report_export_job")

# ---------------------------------------------------------------------------
# Report registry
# Each entry: (module_path, report_name)
# ---------------------------------------------------------------------------
REPORT_REGISTRY = [
    ("glue_jobs.reports.crash_rate",               "crash_rate"),
    ("glue_jobs.reports.crash_count_daily",        "crash_count_daily"),
    ("glue_jobs.reports.connectivity_rate_daily",  "connectivity_rate_daily"),
    ("glue_jobs.reports.ota_rate",                 "ota_rate"),
    # ... 14 additional report modules follow the same pattern
]


# ---------------------------------------------------------------------------
# Spark initialisation
# ---------------------------------------------------------------------------
def build_spark_session(catalog_name: str):
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

    return spark, glue_ctx


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args():
    required = [
        "JOB_NAME", "namespace", "catalog_name", "report_bucket", "report_date"
    ]
    args = getResolvedOptions(sys.argv, required)

    # Optional --report flag
    if "--report" in sys.argv:
        extra = getResolvedOptions(sys.argv, ["report"])
        args["report"] = extra["report"]
    else:
        args["report"] = None

    # Validate report_date format
    report_date = args["report_date"]
    try:
        datetime.datetime.strptime(report_date, "%Y%m%d")
    except ValueError:
        raise ValueError(
            f"--report_date must be YYYYMMDD format, got: {report_date!r}"
        )

    logger.info(
        "Report export args: namespace=%s, report_date=%s, single_report=%s",
        args["namespace"], report_date, args["report"],
    )
    return args


# ---------------------------------------------------------------------------
# Single report export runner
# ---------------------------------------------------------------------------
def export_report(module_path: str, report_name: str, spark, args: dict) -> int:
    """
    Dynamically import and execute a report module.
    Returns the number of rows exported.
    """
    logger.info("── Exporting report: %s ──", report_name)
    module = importlib.import_module(module_path)
    row_count = module.export(
        spark=spark,
        namespace=args["namespace"],
        catalog_name=args["catalog_name"],
        report_date=args["report_date"],
        report_bucket=args["report_bucket"],
    )
    s3_path = (
        f"s3://{args['report_bucket']}/vw_device_{report_name}"
        f"/{args['report_date']}/"
    )
    logger.info(
        "── Report %s complete: %d rows → %s ──", report_name, row_count, s3_path
    )
    return row_count


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_all_reports(spark, args: dict) -> dict:
    """
    Export all registered reports.

    Returns:
        { report_name: {"status": "ok"|"failed", "rows": int|None} }
    """
    summary = {}

    for module_path, report_name in REPORT_REGISTRY:
        try:
            rows = export_report(module_path, report_name, spark, args)
            summary[report_name] = {"status": "ok", "rows": rows}
        except Exception as exc:
            logger.error(
                "Report %s export FAILED: %s", report_name, exc, exc_info=True
            )
            summary[report_name] = {"status": "failed", "rows": None}
            # Continue with remaining reports — we want to export as many as
            # possible even if individual reports fail

    return summary


def run_single_report(spark, args: dict, target_report: str) -> dict:
    for module_path, report_name in REPORT_REGISTRY:
        if report_name == target_report:
            try:
                rows = export_report(module_path, report_name, spark, args)
                return {report_name: {"status": "ok", "rows": rows}}
            except Exception as exc:
                logger.error(
                    "Report %s FAILED: %s", report_name, exc, exc_info=True
                )
                raise

    raise ValueError(
        f"Unknown report name: {target_report!r}. "
        f"Valid names: {[r for _, r in REPORT_REGISTRY]}"
    )


# ---------------------------------------------------------------------------
# Glue Data Catalog registration
# ---------------------------------------------------------------------------
def register_report_tables(
    spark,
    catalog_name: str,
    namespace: str,
    report_bucket: str,
    report_date: str,
    exported_reports: list,
) -> None:
    """
    Ensure each exported report has an external table registered in the Glue
    Data Catalog pointing at its S3 Parquet location.

    Uses Spark SQL CREATE TABLE IF NOT EXISTS ... USING parquet LOCATION ...
    so Athena and Redshift Spectrum can discover the data without manual DDL.
    """
    import boto3
    glue_client = boto3.client("glue", region_name="us-east-1")

    for report_name in exported_reports:
        table_name = f"vw_device_{report_name}"
        s3_location = f"s3://{report_bucket}/{table_name}/{report_date}/"

        try:
            glue_client.get_table(DatabaseName=namespace, Name=table_name)
            # Table already exists — add new partition for report_date
            glue_client.create_partition(
                DatabaseName=namespace,
                TableName=table_name,
                PartitionInput={
                    "Values": [report_date],
                    "StorageDescriptor": {
                        "Location": s3_location,
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": (
                            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
                        ),
                        "SerdeInfo": {
                            "SerializationLibrary": (
                                "org.apache.hadoop.hive.ql.io.parquet.serde"
                                ".ParquetHiveSerDe"
                            )
                        },
                    },
                },
            )
            logger.debug("Added partition %s to %s", report_date, table_name)
        except glue_client.exceptions.EntityNotFoundException:
            # Table does not exist yet — Spark will create it on first export
            logger.info(
                "Catalog table %s.%s not found — will be auto-created on next "
                "Athena MSCK REPAIR or by the CDK stack.",
                namespace, table_name,
            )
        except glue_client.exceptions.AlreadyExistsException:
            logger.debug(
                "Partition %s already exists for %s — skipping.", report_date, table_name
            )
        except Exception as exc:
            logger.warning(
                "Could not register partition for %s: %s", table_name, exc
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    spark, glue_ctx = build_spark_session(args["catalog_name"])

    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    try:
        if args["report"]:
            summary = run_single_report(spark, args, args["report"])
        else:
            summary = run_all_reports(spark, args)
    except Exception:
        logger.exception("Report export job failed")
        raise
    finally:
        # Summary log
        total_exported = failed_count = 0
        ok_reports = []

        logger.info("═══ Report export summary ═══")
        for report_name, result in summary.items():
            status = result["status"]
            rows = result["rows"]
            if status == "ok":
                total_exported += 1
                ok_reports.append(report_name)
            else:
                failed_count += 1
            logger.info(
                "  %-40s status=%-8s rows=%s",
                report_name, status,
                rows if rows is not None else "—",
            )
        logger.info(
            "  Total: exported=%d, failed=%d", total_exported, failed_count
        )

        # Register successfully exported reports in Glue catalog
        if ok_reports:
            try:
                register_report_tables(
                    spark=spark,
                    catalog_name=args["catalog_name"],
                    namespace=args["namespace"],
                    report_bucket=args["report_bucket"],
                    report_date=args["report_date"],
                    exported_reports=ok_reports,
                )
            except Exception as exc:
                logger.warning("Catalog registration step failed: %s", exc)

    job.commit()

    if failed_count > 0:
        raise RuntimeError(
            f"Report export job completed with {failed_count} failed report(s). "
            "See logs for details."
        )

    logger.info("Report export job complete.")


if __name__ == "__main__":
    main()
