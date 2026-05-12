# Telemetry Schema Design & Data Layer — App Events & Firmware SDK (Tercel)

---

## Upwork Portfolio Entry

**Project title** (65 / 70 characters)
`Telemetry Schema Design & Data Layer — App Events & Firmware SDK`

**Role**: Data Engineer

**Project description**

Built the data layer of a serverless firmware telemetry pipeline on AWS China, as part of migrating from a third-party provider to an internally owned platform. Implemented a Kinesis Firehose transform Lambda in Python that downloads the protobuf schema descriptor from S3 at runtime, deserialises binary payloads, and delivers flat JSON — decoupling schema changes from Lambda redeployment. Configured Glue Crawlers for event-driven schema cataloguing and updated the downstream chain (Redshift tables, Airflow DAGs, Power BI models) to align with the new architecture.

**Skills and deliverables**

- Python — Kinesis Firehose transform Lambda (protobuf decode, Pydantic validation, JSON flattening)
- Pydantic — event schema models for firmware telemetry envelope and payload fields
- AWS Glue (Crawlers + Catalog) — event-driven and scheduled crawler configuration for decoded and raw S3 tracks
- Amazon Redshift — table schema updates aligned to new delivery format
- Apache Airflow (AWS MWAA) — DAG updates for new S3 partition structure and delivery paths
- Data contracts — cross-team schema alignment with firmware engineering team
- Unit testing — schema validation, Pydantic edge cases, and protobuf flattening logic

---

## Full Case Study: Telemetry Schema Design & Data Layer — App Events & Firmware SDK (Tercel)

**Role**: Data Engineer — data layer (cloud infrastructure designed and deployed by cloud engineer)  
**Stack**: Python 3.13 · Kinesis Firehose (transform Lambda) · AWS Glue (Crawlers, Catalog) · Amazon S3 · Amazon Redshift · AWS MWAA · Pydantic · protobuf

---

## Problem

As part of the internal SDK migration replacing a third-party telemetry provider, the company's sentinel firmware telemetry needed to move onto an internally owned data pipeline on AWS China (`cn-north-1`). The cloud engineer designed and deployed the serverless ingestion infrastructure (API Gateway → Lambda → SQS → EventBridge Pipes → Kinesis → S3). The data engineering challenges were:

- **Binary protobuf payloads** — sentinel firmware events arrive as binary protobuf. A decode Lambda embedded in the Firehose delivery stream needed to download the protobuf schema descriptor at runtime, deserialise the payload, and output flat JSON — without coupling schema changes to Lambda redeployment
- **Downstream data chain realignment** — the new ingestion architecture changed the schema shape, S3 partition structure, and delivery frequency. Glue Crawlers, Redshift table models, MWAA DAGs, and Power BI dataset models all needed to be updated to align with the new design
- **Dual-track delivery** — decoded JSON (Glue-catalogued, Redshift-queryable) and raw binary (audit and backfill) are delivered to separate S3 tracks; the data layer needed to define the schema and cataloguing strategy for both
- **Cross-team data contract** — firmware engineering needed a published event envelope and payload schema to develop against; without a defined contract, upstream SDK and downstream pipeline would diverge independently

---

## Approach

The cloud engineer owned the three-domain SAM infrastructure (API Gateway, SQS, EventBridge Pipes, Kinesis, Firehose delivery streams, S3 bucket policies, cross-account IAM, CloudWatch alarms, CI/CD). My responsibility covered the data layer within that infrastructure:

1. **Schema definition** — defined the sentinel protobuf envelope schema, established field naming conventions, and maintained the `.proto` schema descriptor in S3. Defined the flat JSON target schema for Redshift loading, aligning field types and names with the existing warehouse convention. Collaborated with firmware engineering to establish the data contract.

2. **Firehose decode Lambda (`DeserializeFunction`)** — implemented the Python transform Lambda embedded in the Firehose delivery stream. The Lambda downloads the protobuf descriptor from S3 at runtime, deserialises the binary payload, unwraps the event envelope, and outputs a flat GZIP JSON record. Schema updates are deployed by uploading a new descriptor to S3 — no Lambda redeployment required.

3. **Glue Crawler configuration** — configured the event-driven delivery crawler (CRAWL_EVENT_MODE, triggered by S3 ObjectCreated via SQS) and the daily raw crawler (CRAWL_NEW_FOLDERS_ONLY). Managed the Glue Catalog schema for both the decoded and raw tracks.

4. **Downstream data chain updates** — updated Redshift table models to align with the new flat JSON schema and S3 partition structure; updated MWAA ETL DAGs for the new delivery paths; updated Power BI dataset models to reflect schema changes.

5. **Unit tests** — wrote unit tests for schema validation, Pydantic model edge cases, and the protobuf flattening logic.

---

## Architecture

The following diagrams show the full pipeline. Cloud infrastructure (grey areas) was designed and deployed by the cloud engineer. Data layer components are highlighted in the narrative above.

### Domain 1: `telemetry-sink`

```mermaid
graph TD
    Client["Client"]

    subgraph telemetry-sink
        APIGW["API Gateway (REST)\n/v1/records/sentinel"]
        FeedPump["SentinelFeedPumpFunction\n(Lambda)\n• Validate schema\n• Batch serialize to JSON\n• Emit metrics"]
        SQ["SentinelIngestionQueue\n(SQS, KMS encrypted\n7-day retention)"]
        DLQ["SentinelIngestionDLQ\n(SQS, after 5 retries)"]
        SSM_Q["SSM: .../Outputs/SentinelQueueArn"]
    end

    Client -->|"POST /records/sentinel"| APIGW
    APIGW --> FeedPump
    FeedPump -->|"send_messages (batch)"| SQ
    SQ -->|"redrive after 5 failures"| DLQ
    SQ -.->|"exports ARN"| SSM_Q
```

---

### Domain 2: `sentinel-record-processor`

```mermaid
graph TD
    SSM_Q["SSM: SentinelQueueArn\n(from telemetry-sink)"]

    subgraph sentinel-record-processor
        Pipe["Pipe\n(EventBridge Pipes)"]
        Enrich["EventEnrichmentFunction\n(Lambda)\n• Unwrap SQS envelope\n• Return plain JSON list"]
        KDS["RecordStream\n(Kinesis, on-demand\nKMS, 14-day retention)"]

        subgraph Firehose-Delivery
            FH_D["DeliveryStream\n(Kinesis Firehose)"]
            Deserialize["DeserializeFunction\n(Lambda) ★ data engineer\n• Download .proto schema from S3\n• Decode binary payload\n• Flatten to JSON"]
            DB["DeliveryBucket\n(S3, Intelligent-Tiering\nPartitioned by date)"]
        end

        subgraph Firehose-Raw
            FH_R["RawStream\n(Kinesis Firehose)"]
            RB["RawBucket\n(S3, Intelligent-Tiering\nPartitioned by date)"]
        end

        PBS["PBSchemaBucket\n(S3, protobuf schema zip)"]
        SSM_PBS["SSM: PB Schema File path"]

        SEQ["S3EventQueue (SQS)\n• Notified on S3 ObjectCreated"]
        SDLQ["S3EventDLQ"]
    end

    SSM_Q -->|"resolve source ARN"| Pipe
    Pipe -->|"enrichment invoke"| Enrich
    Enrich -->|"enriched records"| Pipe
    Pipe -->|"PutRecords (partitioned by event_guid)"| KDS

    KDS -->|"stream source"| FH_D
    KDS -->|"stream source"| FH_R

    FH_D -->|"invoke (transform)"| Deserialize
    Deserialize -->|"reads schema zip"| PBS
    Deserialize -->|"reads schema path"| SSM_PBS
    FH_D -->|"deliver GZIP JSON"| DB

    FH_R -->|"deliver GZIP raw"| RB

    DB -->|"s3:ObjectCreated"| SEQ
    SEQ -->|"redrive"| SDLQ
```

---

### Domain 3: `sentinel-data-domain`

```mermaid
graph TD
    subgraph sentinel-record-processor ["sentinel-record-processor (upstream)"]
        DB_UP["DeliveryBucket (S3)"]
        RB_UP["RawBucket (S3)"]
        SEQ_UP["S3EventQueue (SQS)"]
    end

    subgraph sentinel-data-domain
        GlueDB["GlueDatabase\n(AWS Glue Catalog) ★ data engineer"]
        DataCrawler["DataProductCrawler\n(Glue Crawler) ★ data engineer\n• Event-driven + hourly\n• Crawls DeliveryBucket"]
        RawCrawler["RawProductCrawler\n(Glue Crawler) ★ data engineer\n• Daily at 23:57 UTC\n• Crawls RawBucket"]
        GlueRole["GlueServiceRole\n(IAM)"]
        AccessRole["DataAccessRole\n(IAM, cross-account)\n• For Redshift Spectrum\n• PrincipalTag gated"]
        BucketPol["DataBucketPolicy\n(S3 BucketPolicy)\nGrants consumer accounts read"]
        RawBucketPol["RawBucketPolicy\n(S3 BucketPolicy)\nGrants consumer accounts read"]
        SSM_Glue["SSM: GlueDatabaseName"]
        SSM_RS["SSM: RedshiftAccessRoleArn"]
    end

    ConsumerDW["Consumer DW Account\n(Redshift Spectrum)"]

    DB_UP -->|"crawl"| DataCrawler
    RB_UP -->|"crawl"| RawCrawler
    SEQ_UP -->|"event trigger"| DataCrawler
    DataCrawler -->|"write schema"| GlueDB
    RawCrawler -->|"write schema"| GlueDB
    DataCrawler --> GlueRole
    RawCrawler --> GlueRole

    GlueDB -->|"catalog read"| AccessRole
    DB_UP -->|"data read"| AccessRole
    AccessRole -->|"assumed by"| ConsumerDW

    DB_UP --> BucketPol
    RB_UP --> RawBucketPol
    BucketPol -->|"grants read"| ConsumerDW
    RawBucketPol -->|"grants read"| ConsumerDW

    AccessRole -.->|"exports ARN"| SSM_RS
    GlueDB -.->|"exports name"| SSM_Glue
```

---

### Overview: All Three Domains

```mermaid
graph TD
    Client["Client\n(Mobile App)"]

    subgraph SINK ["telemetry-sink"]
        APIGW["API Gateway\nPOST /records/sentinel"]
        FeedPump["SentinelFeedPumpFunction\n(validate + batch)"]
        SQueue["SentinelIngestionQueue\n(SQS)"]
    end

    subgraph PROC ["sentinel-record-processor"]
        Pipe["Pipe (EventBridge)\nSQS → enrich → Kinesis"]
        Enrich["EventEnrichmentFunction\n(unwrap SQS envelope)"]
        KDS["RecordStream\n(Kinesis)"]
        FH_D["DeliveryStream\n(Firehose + Deserialize ★)"]
        FH_R["RawStream\n(Firehose, raw)"]
        DeliveryS3["DeliveryBucket\n(S3, partitioned JSON)"]
        RawS3["RawBucket\n(S3, partitioned raw)"]
        S3Evt["S3EventQueue\n(SQS notify)"]
    end

    subgraph DOMAIN ["sentinel-data-domain"]
        Crawler_D["DataProductCrawler ★\n(Glue, hourly + event)"]
        Crawler_R["RawProductCrawler ★\n(Glue, daily)"]
        GlueDB["Glue Catalog Database ★"]
        AccessRole["DataAccessRole\n(cross-account)"]
    end

    DW["Consumer DW\n(Redshift Spectrum)"]

    Client -->|"HTTPS POST batch"| APIGW
    APIGW --> FeedPump
    FeedPump -->|"SQS send_messages"| SQueue

    SQueue -->|"SSM ARN ref"| Pipe
    Pipe -->|"enrich"| Enrich
    Enrich --> Pipe
    Pipe -->|"PutRecords"| KDS

    KDS --> FH_D
    KDS --> FH_R
    FH_D -->|"transform + deliver"| DeliveryS3
    FH_R -->|"deliver raw"| RawS3

    DeliveryS3 -->|"ObjectCreated"| S3Evt
    DeliveryS3 -->|"crawl"| Crawler_D
    RawS3 -->|"crawl"| Crawler_R
    S3Evt -->|"event trigger"| Crawler_D

    Crawler_D --> GlueDB
    Crawler_R --> GlueDB
    GlueDB --> AccessRole
    DeliveryS3 --> AccessRole
    AccessRole -->|"cross-account read"| DW
```

*(★ = data engineer owned)*

---

The pipeline runs in three stages. Cloud infrastructure ownership and data layer ownership per domain:

| Stage | Domain | Cloud Engineer | Data Engineer |
|---|---|---|---|
| **Ingest** | `telemetry-sink` | API Gateway, Lambda (FeedPump), SQS, DLQ, alarms | — |
| **Process** | `sentinel-record-processor` | EventBridge Pipes, Kinesis, Firehose delivery streams, S3 buckets | Decode Lambda (DeserializeFunction), protobuf schema descriptor, schema definition |
| **Serve** | `sentinel-data-domain` | IAM cross-account role, S3 bucket policies, SSM exports | Glue Crawlers (configuration + management), Glue Catalog schema |
| **Downstream** | Redshift + MWAA | — | Redshift table models, MWAA DAGs, Power BI dataset models |

---

## Key Capabilities Delivered

| Capability | Detail |
|---|---|
| Runtime protobuf decode | Implemented the Firehose transform Lambda: downloads `.proto` schema descriptor from S3 at delivery time, deserialises binary payload, flattens envelope + context + payload to JSON — schema updates require only an S3 descriptor upload, no redeployment |
| Pydantic schema validation | Defined sentinel event envelope and payload schemas with strict field typing; validated records in the Firehose transform step before delivery to S3 |
| Glue Crawler management | Configured event-driven delivery crawler (CRAWL_EVENT_MODE via S3→SQS trigger) for low-latency schema updates, and daily raw crawler (CRAWL_NEW_FOLDERS_ONLY); managed Glue Catalog schema for both tracks |
| Downstream data chain alignment | Updated Redshift table models, MWAA ETL DAGs, and Power BI dataset models to align with the new schema and S3 partition structure from the new ingestion architecture |
| Cross-team data contract | Collaborated with firmware engineering to define the sentinel event envelope structure; maintained the schema descriptor in S3 for upstream SDK team consumption |
| Unit test coverage | Wrote unit tests for schema validation, Pydantic model edge cases, and protobuf flattening logic |

---

## Outcome

- Firmware telemetry pipeline brought fully in-house on AWS China, replacing the third-party provider — as part of the broader internal SDK migration that also eliminated third-party vendor costs and reduced AWS infrastructure spend by ~30%
- Protobuf schema decoupled from Lambda deployment: schema updates require only an S3 descriptor upload; zero Lambda redeployments for schema evolution
- Downstream Redshift and MWAA pipelines aligned with the new ingestion architecture without disrupting ongoing analytics reporting
- Raw binary track preserved for audit and backfill; decoded track queryable via Glue Catalog and Redshift Spectrum
