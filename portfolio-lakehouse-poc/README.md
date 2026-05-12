# Lakehouse POC — IoT Device Telemetry on AWS

A production-grade proof-of-concept lakehouse built on **AWS Glue PySpark** and **Amazon S3 Tables (Apache Iceberg v2)** for processing IoT device telemetry at scale. Implemented on AWS with cross-account read access via Redshift Spectrum.

---

## Architecture Overview

```
Raw S3 (gzip JSON)
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  AWS Glue Workflow  (sequential, daily trigger)      │
│                                                      │
│  Job 1: ODS Ingestion                                │
│    raw S3 → decode protobuf → ODS Iceberg table      │
│                  └──────────→ subject Iceberg tables │
│                                                      │
│  Job 2: Subject Table Window Updates                 │
│    reboot_info / shutdown_reason tables              │
│    → MERGE INTO (Iceberg SQL) window columns         │
│                                                      │
│  Job 3: Daily Marts                                  │
│    ODS + subject tables → DWD/DWS mart tables        │
│    (session_sets, crash_record, crash_session_count, │
│     connectivity_duration, firmware/product dims)    │
│                                                      │
│  Job 4: Report Export                                │
│    18 report views → Parquet on S3                   │
│    (Athena-queryable, Redshift Spectrum-readable)    │
└─────────────────────────────────────────────────────┘
      │
      ▼
Cross-account consumer:
  Redshift Spectrum (consumer acct)
    → IAM role assumption + Lake Formation grants
    → reads S3 Tables Parquet directly
```

### Data Layers

| Layer | Storage | Format | Description |
|-------|---------|--------|-------------|
| Raw | `s3://analytics-raw-bucket/source=device/` | gzip JSON | Inbound telemetry events, Glue bookmark managed |
| ODS | S3 Tables namespace | Iceberg v2 | All events flattened + protobuf decoded, append-only |
| Subject | S3 Tables namespace | Iceberg v2 | Per-event-type tables (reboot, shutdown, OTA, BT events) |
| DWD | S3 Tables namespace | Iceberg v2 | Session sets, crash records — daily DELETE+INSERT |
| DWS | S3 Tables namespace | Iceberg v2 | Aggregated daily marts (crash counts, connectivity) |
| Report | `s3://analytics-report-bucket/` | Parquet | 18 export views, partitioned by report date |

---

## Stack

| Component | Technology |
|-----------|-----------|
| Compute | AWS Glue 5.0 (PySpark 3.x) |
| Table format | Apache Iceberg v2 (via Amazon S3 Tables) |
| Catalog | AWS Glue Data Catalog (`GlueCatalog` + `s3tables` catalog impl) |
| Query | Amazon Athena (ad-hoc), Redshift Spectrum (BI dashboards) |
| Schema registry | Amazon DynamoDB + S3 (`.pb` FileDescriptorSet blobs) |
| Infrastructure | AWS CDK (TypeScript) |
| Orchestration | AWS Glue Workflows (sequential job chain, daily schedule) |

---

## Key Engineering Patterns

### 1. Runtime Protobuf Schema Registry

Decouples protobuf schema evolution from Glue job deployment. At decode time, each Spark executor:

1. Looks up `schema_id` in DynamoDB table `device-schema-registry` → gets `descriptor_s3_uri` and `message_full_name`
2. Fetches the compiled `.pb` FileDescriptorSet blob from S3
3. Loads it into `google.protobuf.descriptor_pool` dynamically
4. Decodes the base64 payload without any schema baked into the job

**Result:** New firmware schema versions go live without redeploying Glue jobs — only the registry entry and `.pb` blob need updating.

```python
# Per-partition singleton — avoids repeated DynamoDB/S3 calls per row
class SchemaCache:
    _instance = None

    @classmethod
    def get_instance(cls, registry_table, schema_bucket):
        if cls._instance is None:
            cls._instance = cls(registry_table, schema_bucket)
        return cls._instance

    def get_message_class(self, schema_id):
        # DynamoDB lookup → S3 fetch → descriptor_pool load → message class
        ...
```

### 2. Iceberg MERGE INTO for Window Column Updates

Rather than rewriting entire partitions, the subject tables job issues targeted SQL MERGE statements after recomputing Spark window aggregates:

```sql
MERGE INTO {catalog}.{namespace}.dwd_device_events_reboot_info AS target
USING reboot_updates AS source
ON target.device_component_guid = source.device_component_guid
   AND target.device_session_id   = source.device_session_id
   AND target.event_local_date    = source.event_local_date
WHEN MATCHED THEN UPDATE SET
  single_session_reboot_cnt = source.single_session_reboot_cnt
```

Iceberg handles file-level rewrites only for affected data files — far cheaper than full partition overwrite.

### 3. DELETE + INSERT Idempotency for Daily Marts

All mart jobs are safe to re-run. Each execution:

```python
spark.sql(f"""
    DELETE FROM {catalog}.{ns}.{target_table}
    WHERE event_local_date BETWEEN '{start_date}' AND '{end_date}'
""")
# ... compute aggregation ...
result_df.writeTo(f"{catalog}.{ns}.{target_table}").append()
```

Iceberg's snapshot isolation ensures readers see a consistent view during the swap.

### 4. mapPartitions Protobuf Decode

Protobuf decode uses `mapPartitions` (not `udf`) so the `SchemaCache` singleton is initialized once per executor partition rather than once per row:

```python
def decode_partition(rows):
    cache = SchemaCache.get_instance(registry_table, schema_bucket)
    for row in rows:
        payload_json, success = cache.decode_payload(row.schema_id, row.raw_payload)
        yield Row(**row.asDict(), payload_json=payload_json, decode_success=success)
```

### 5. Cross-Account Read via Lake Formation

The report Parquet layer on S3 Tables is readable from a consumer AWS account:

- Producer account grants `SELECT` on Glue catalog tables to consumer IAM role via Lake Formation
- Consumer Redshift cluster assumes the cross-account IAM role (`arn:aws:iam::123456789012:role/DataPlatformGlueRole`)
- Redshift Spectrum creates external schema pointing at the Glue Data Catalog
- No data movement — Spectrum reads S3 Tables Parquet files directly

---

## Directory Structure

```
portfolio-lakehouse-poc/
├── README.md
├── requirements.txt
├── glue_jobs/
│   ├── ods_job.py               # Job 1: Raw → ODS + subject tables
│   ├── subject_tables_job.py    # Job 2: Window column MERGE INTO
│   ├── daily_marts_job.py       # Job 3: Mart orchestrator
│   ├── report_export_job.py     # Job 4: Report Parquet export
│   └── marts/
│       ├── __init__.py
│       ├── session_sets.py      # DWD session aggregation
│       └── crash_count.py       # DWS crash detection + aggregation
```

---

## Glue Job Configuration

All jobs run under:

- **IAM Role:** `arn:aws:iam::123456789012:role/DataPlatformGlueRole`
- **Glue version:** 5.0
- **Worker type:** G.2X (ODS job); G.1X (marts/export)
- **S3 Tables catalog:** configured via `spark.sql.catalog.s3tables_glue_catalog` Spark property
- **Job bookmarks:** enabled on ODS job for incremental S3 reads

### Iceberg Catalog Spark Config

```python
spark_conf = {
    "spark.sql.extensions":
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.s3tables_glue_catalog":
        "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.s3tables_glue_catalog.catalog-impl":
        "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.s3tables_glue_catalog.warehouse":
        "s3://analytics-tables-bucket/",
    "spark.sql.catalog.s3tables_glue_catalog.io-impl":
        "org.apache.iceberg.aws.s3.S3FileIO",
}
```

---

## Report Views (18 total, 4 shown)

| View name | Source tables | Grain |
|-----------|--------------|-------|
| `vw_device_crash_rate` | crash_session_count, session_sets | product × firmware × day |
| `vw_device_crash_count_daily` | crash_session_count | product × day |
| `vw_device_connectivity_rate_daily` | session_sets | product × day |
| `vw_device_ota_rate` | session_sets, ota_events | product × firmware × day |

Reports are exported to `s3://analytics-report-bucket/vw_device_{name}/{YYYYMMDD}/` as Parquet and registered in the Glue Data Catalog for Athena and Redshift Spectrum access.

---

## Infrastructure (CDK TypeScript — not included in this portfolio sample)

- S3 Tables bucket with namespace provisioning
- Glue Workflow with 4 sequential job nodes and daily EventBridge trigger
- DynamoDB table for schema registry with GSI on `schema_id`
- IAM roles and Lake Formation grants for cross-account access
- CloudWatch dashboard with job duration, decode success rate, row count metrics
