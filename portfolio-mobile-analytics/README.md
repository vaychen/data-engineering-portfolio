# Mobile Analytics Pipeline

A production-grade mobile analytics pipeline built on AWS (Amazon MWAA + Redshift Serverless). The pipeline ingests events from a mobile application ("nova"), transforms them through a 5-layer data warehouse, and exports aggregated reporting tables to Aurora MySQL for downstream BI and application consumption.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        AWS MWAA (Airflow 2.7.2)                         │
│                                                                         │
│  pipeline_main_dag  ──trigger──►  pipeline_source_dag                  │
│       (daily 01:00 UTC)                  │                              │
│                                          ▼                              │
│                              pipeline_etl_dag                           │
│                                          │                              │
│                                          ▼                              │
│                             pipeline_export_dag  ──trigger──►           │
│                                                    pipeline_quality_dag │
└─────────────────────────────────────────────────────────────────────────┘
                    │                     │
                    ▼                     ▼
         Amazon Redshift          Aurora MySQL (reporting)
           Serverless
```

### 5-Stage Trigger-Chain DAG Pipeline

| Stage | DAG | Responsibility |
|---|---|---|
| 1 | `pipeline_main_dag` | Warm-up, DIM refresh, trigger chain entry point |
| 2 | `pipeline_source_dag` | ODS raw data load: nova app events (staging → load → dedupe) |
| 3 | `pipeline_etl_dag` | DWD + DWS transformation: user activity chain |
| 4 | `pipeline_export_dag` | Redshift → MySQL export with idempotent upsert |
| 5 | `pipeline_quality_dag` | Source delivery checks + report data assertions |

DAGs 2–5 have `schedule=None` and are triggered exclusively via `TriggerDagRunOperator`, forming a deterministic sequential chain with controlled parallelism within each stage.

---

## Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.7.2 (AWS MWAA) |
| Data Warehouse | Amazon Redshift Serverless |
| Reporting DB | Aurora MySQL 8.0 |
| Language | Python 3.11 |
| Alerting | AWS SNS |
| Infrastructure | AWS IAM, S3 (SQL assets), Secrets Manager |

---

## Data Layers

```
ODS  →  DIM  →  DWD  →  DWS  →  ADS
```

| Layer | Description | Tables |
|---|---|---|
| **ODS** | Raw operational data, append-only | `ods_nova_app_events` |
| **DIM** | Slowly-changing dimensions, full-refresh daily | `dim_product`, `dim_user_product_relationship`, `dim_date`, `dim_device_model` |
| **DWD** | Cleaned, deduplicated daily facts | `dwd_user_active_daily`, `dwd_product_active_daily` |
| **DWS** | Aggregated service-layer summaries | `dws_user_retention` |
| **ADS** | Application data store — MySQL export targets + BI views | `ads_user_active_report`, `ads_product_active_report`, `vw_app_daily_active_users` |

---

## Key Engineering Patterns

### SQL Externalization with Jinja Params
All SQL lives in `sql/dml/` and `sql/ddl/` directories. The custom `RedshiftSQLOperator` renders each file as a Jinja template before execution, allowing date parameters (`{{ ds }}`, `{{ params.backfill_scan_date }}`) to be injected at runtime without string concatenation.

```python
RedshiftSQLOperator(
    task_id="refresh_dim_product",
    sql_files=["sql/dml/dim_product_refresh.sql"],
    params={"backfill_scan_date": 1},
)
```

### Backfill Window Control via Airflow Variables
The `backfill_scan_date` Airflow Variable controls how many days back each run re-processes. Default is `1` (yesterday only). Setting it to `7` triggers a 7-day restatement without DAG code changes.

### Idempotent DELETE + INSERT
Every DWD/DWS write pattern is:
1. `DELETE FROM target WHERE partition_date BETWEEN <lower_bound> AND <ds>`
2. `INSERT INTO target SELECT ... FROM source WHERE partition_date IN <window>`

This makes every run safe to retry and re-run.

### Streaming Redshift → MySQL Export
`RedshiftToMySQLOperator` uses a server-side cursor to stream large Redshift result sets in configurable chunks (default 20,000 rows), avoiding memory exhaustion on large exports. Each export is idempotent: existing rows for the business date are deleted before re-insert.

### Custom Operators
| Operator | Purpose |
|---|---|
| `RedshiftSQLOperator` | Execute one or more SQL files against Redshift with Jinja rendering |
| `RedshiftToMySQLOperator` | Stream data from Redshift to MySQL with idempotent upsert and rolling window cleanup |

---

## Project Structure

```
.
├── README.md
├── requirements.txt
├── dags/
│   ├── pipeline_main_dag.py
│   ├── pipeline_source_dag.py
│   ├── pipeline_etl_dag.py
│   ├── pipeline_export_dag.py
│   └── pipeline_quality_dag.py
├── plugins/
│   ├── common/
│   │   ├── config.py
│   │   └── notifications.py
│   └── operators/
│       ├── redshift_sql_operator.py
│       └── redshift_to_mysql_operator.py
└── sql/
    ├── ddl/
    │   ├── ods_nova_app_events.sql
    │   ├── ods_nova_events_staging.sql
    │   ├── dim_date.sql
    │   ├── dim_device_model.sql
    │   ├── dim_product.sql
    │   ├── dim_user_product_relationship.sql
    │   ├── dwd_user_active_daily.sql
    │   ├── dwd_product_active_daily.sql
    │   ├── dws_user_retention.sql
    │   └── vw_app_daily_active_users.sql
    └── dml/
        ├── ods_nova_events_staging_truncate.sql
        ├── ods_nova_events_load.sql
        ├── ods_nova_events_dedupe.sql
        ├── dim_date_refresh.sql
        ├── dim_device_model_refresh.sql
        ├── dim_product_refresh.sql
        ├── dim_user_product_relationship.sql
        ├── dwd_user_active_daily.sql
        ├── dwd_product_active_daily.sql
        ├── dws_user_retention.sql
        ├── dqs_source_delivery_check.sql
        └── dqs_report_row_count_check.sql
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `AIRFLOW_ENV` | `local` \| `staging` \| `production` |
| `REDSHIFT_CONN_ID` | Airflow connection ID for Redshift Serverless |
| `MYSQL_CONN_ID` | Airflow connection ID for Aurora MySQL |
| `REDSHIFT_IAM_ROLE_ARN` | IAM role ARN for Redshift Spectrum / S3 access |
| `SNS_ALERT_TOPIC_ARN` | SNS topic ARN for pipeline failure alerts |

---

## Running Locally

```bash
pip install -r requirements.txt

# Set environment
export AIRFLOW_ENV=local
export REDSHIFT_CONN_ID=redshift_default
export MYSQL_CONN_ID=mysql_default
export REDSHIFT_IAM_ROLE_ARN=arn:aws:iam::123456789012:role/RedshiftSpectrumRole
export SNS_ALERT_TOPIC_ARN=arn:aws:sns:us-east-1:123456789012:analytics-pipeline-alerts

# Initialise Airflow metadata DB
airflow db init
airflow users create --username admin --password admin \
    --firstname Admin --lastname User --role Admin --email admin@example.com

# Start scheduler + webserver
airflow scheduler &
airflow webserver --port 8080
```
