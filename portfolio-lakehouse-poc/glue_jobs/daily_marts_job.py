"""
Daily Marts Orchestrator Job

Runs all DWD/DWS mart modules in dependency order for a given date range.
Each mart module exposes a `run(spark, namespace, catalog_name, start_date,
end_date)` function that performs DELETE + INSERT idempotently.

Dependency graph:
  session_sets ──► session_crash_sets
  crash_record ──► crash_session_count
  connectivity_duration

Note: firmware_dim and product_dim are shared dimension loaders
(glue_jobs/dims/firmware_dim.py, glue_jobs/dims/product_dim.py).
They are not registered as marts — marts load them internally as needed.

Args (Glue job parameters):
  --JOB_NAME
  --namespace
  --catalog_name
  --schema_registry_table   (passed through to marts that need schema info)
  --start_date              YYYY-MM-DD
  --end_date                YYYY-MM-DD
  --mart                    (optional) run a single named mart and exit
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
logger = logging.getLogger("daily_marts_job")

# ---------------------------------------------------------------------------
# Mart registry — ordered for dependency resolution
# ---------------------------------------------------------------------------
# Each entry: (module_path, mart_name, critical)
# critical=True  → job fails if this mart fails
# critical=False → warning logged, remaining marts continue
MART_REGISTRY = [
    ("glue_jobs.marts.session_sets",          "session_sets",          True),
    ("glue_jobs.marts.session_crash_sets",    "session_crash_sets",    True),
    ("glue_jobs.marts.connectivity_duration", "connectivity_duration",  False),
    ("glue_jobs.marts.crash_record",          "crash_record",          True),
    ("glue_jobs.marts.crash_count",           "crash_session_count",   True),
]

# Dependency map: mart_name → list of mart_names that must succeed first
DEPENDENCIES = {
    "session_crash_sets":    ["session_sets"],
    "crash_session_count":   ["crash_record"],
}


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
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark, glue_ctx


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args():
    required = ["JOB_NAME", "namespace", "catalog_name",
                "schema_registry_table", "start_date", "end_date"]
    args = getResolvedOptions(sys.argv, required)

    # Optional --mart flag
    if "--mart" in sys.argv:
        extra = getResolvedOptions(sys.argv, ["mart"])
        args["mart"] = extra["mart"]
    else:
        args["mart"] = None

    for date_arg in ("start_date", "end_date"):
        try:
            datetime.date.fromisoformat(args[date_arg])
        except ValueError:
            raise ValueError(
                f"--{date_arg} must be YYYY-MM-DD, got: {args[date_arg]!r}"
            )

    logger.info(
        "Mart job args: namespace=%s, range=[%s, %s], single_mart=%s",
        args["namespace"], args["start_date"], args["end_date"], args["mart"],
    )
    return args


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
def dependencies_met(mart_name: str, succeeded: set) -> bool:
    required = DEPENDENCIES.get(mart_name, [])
    missing = [d for d in required if d not in succeeded]
    if missing:
        logger.warning(
            "Skipping %s — dependency failure: %s not in succeeded set",
            mart_name, missing,
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Single mart runner
# ---------------------------------------------------------------------------
def run_mart(module_path: str, mart_name: str, spark, args: dict) -> int:
    """
    Dynamically import and execute a mart module.
    Returns the number of rows written, or raises on failure.
    """
    logger.info("── Starting mart: %s ──", mart_name)
    module = importlib.import_module(module_path)
    row_count = module.run(
        spark=spark,
        namespace=args["namespace"],
        catalog_name=args["catalog_name"],
        start_date=args["start_date"],
        end_date=args["end_date"],
    )
    logger.info("── Mart %s complete: %d rows written ──", mart_name, row_count)
    return row_count


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_all_marts(spark, args: dict) -> dict:
    """
    Execute all registered marts in order, respecting dependencies.

    Returns a summary dict:
      { mart_name: {"status": "ok"|"skipped"|"failed", "rows": int|None} }
    """
    succeeded: set = set()
    failed: set = set()
    summary: dict = {}

    for module_path, mart_name, critical in MART_REGISTRY:
        # Dependency gate
        if not dependencies_met(mart_name, succeeded):
            summary[mart_name] = {"status": "skipped", "rows": None}
            failed.add(mart_name)
            continue

        try:
            rows = run_mart(module_path, mart_name, spark, args)
            succeeded.add(mart_name)
            summary[mart_name] = {"status": "ok", "rows": rows}
        except Exception as exc:
            logger.error(
                "Mart %s FAILED: %s", mart_name, exc, exc_info=True
            )
            failed.add(mart_name)
            summary[mart_name] = {"status": "failed", "rows": None}

            if critical:
                # Re-raise to fail the Glue job immediately on critical marts
                raise RuntimeError(
                    f"Critical mart '{mart_name}' failed — aborting job."
                ) from exc
            else:
                logger.warning(
                    "Non-critical mart %s failed — continuing with remaining marts.",
                    mart_name,
                )

    return summary


def run_single_mart(spark, args: dict, target_mart: str) -> dict:
    """Run only the mart named *target_mart*, ignoring dependency ordering."""
    for module_path, mart_name, critical in MART_REGISTRY:
        if mart_name == target_mart:
            try:
                rows = run_mart(module_path, mart_name, spark, args)
                return {mart_name: {"status": "ok", "rows": rows}}
            except Exception as exc:
                logger.error("Mart %s FAILED: %s", mart_name, exc, exc_info=True)
                raise

    raise ValueError(
        f"Unknown mart name: {target_mart!r}. "
        f"Valid names: {[m for _, m, _ in MART_REGISTRY]}"
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
        if args["mart"]:
            summary = run_single_mart(spark, args, args["mart"])
        else:
            summary = run_all_marts(spark, args)
    except Exception:
        logger.exception("Daily marts job failed")
        raise
    finally:
        # Print summary regardless of success/failure
        logger.info("═══ Mart execution summary ═══")
        total_rows = 0
        ok_count = failed_count = skipped_count = 0
        for mart_name, result in summary.items():
            status = result["status"]
            rows = result["rows"] or 0
            total_rows += rows
            if status == "ok":
                ok_count += 1
            elif status == "failed":
                failed_count += 1
            else:
                skipped_count += 1
            logger.info(
                "  %-35s status=%-8s rows=%s",
                mart_name, status, rows if result["rows"] is not None else "—",
            )
        logger.info(
            "  Total: ok=%d, failed=%d, skipped=%d, rows_written=%d",
            ok_count, failed_count, skipped_count, total_rows,
        )

    job.commit()

    if failed_count > 0:
        raise RuntimeError(
            f"Daily marts job completed with {failed_count} failed mart(s). "
            "See logs above for details."
        )

    logger.info("Daily marts job complete.")


if __name__ == "__main__":
    main()
