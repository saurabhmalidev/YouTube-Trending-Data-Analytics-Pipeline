# YouTube Trending Data Pipeline — AWS (S3 · Glue · Lambda · Athena · Step Functions)

An end-to-end, serverless data pipeline on AWS that ingests YouTube trending video data (live via the YouTube Data API v3, plus a static Kaggle dataset), processes it through a **medallion architecture (Bronze → Silver → Gold)**, runs automated data quality checks, and produces analytics-ready tables queryable via Amazon Athena — fully orchestrated with AWS Step Functions.

---

## Architecture

```
YouTube Data API ─┐
                   ├─► S3 Bronze (raw JSON/CSV, partitioned by region/date)
Kaggle static data ┘
        │
        ▼
  Lambda (JSON → Parquet)  +  Glue (PySpark ETL, CSV/JSON → Parquet)
        │
        ▼
  S3 Silver (cleaned, deduplicated, schema-enforced Parquet)
        │
        ▼
  Lambda Data Quality Gate (row count, null %, schema, value range, freshness — via Athena)
        │
        ▼ (pass only)
  Glue (Silver → Gold aggregations)
        │
        ▼
  S3 Gold (trending_analytics, channel_analytics, category_analytics)
        │
        ▼
  Amazon Athena (SQL) / QuickSight (BI)

Entire flow orchestrated by AWS Step Functions, with SNS alerts on every failure branch.
```

**Step Function orchestration graph:**

![Step Function Graph](assets/StepFunctionMapping.png)

`IngestFromYouTubeAPI → WaitForS3Consistency → ProcessInParallel [TransformReferenceData || RunBronzeToSilverGlueJob] → RunDataQualityChecks → EvaluateDataQuality → RunSilverToGoldGlueJob → NotifySuccess`

Every stage has a `Catch` block wired to its own SNS failure notification (`NotifyIngestionFailure`, `NotifyTransformFailure`, `NotifyDQFailure`, `NotifyGoldFailure`).

---

## Why this exists

Client scenario (hypothetical): a company wants to run a YouTube ad campaign and needs to understand what makes videos trend — by category, region, and channel — before spending marketing budget. Goals: automated ingestion, a proper data lake (medallion architecture), cloud-native ETL, and analytics that scale.

---

## Repo Structure

```
.
├── data/                  # Kaggle static dataset (CSV + JSON per region)
├── lambda-function/       # Lambda source code
│   ├── yt-lambda-ingestion/        # Pulls trending videos + categories from YouTube API → Bronze
│   ├── yt-lambda-json-to-parquet/  # Converts raw JSON reference data → Silver Parquet
│   └── data-quality/               # Runs DQ checks against Silver tables via Athena
├── glue-jobs/             # PySpark Glue ETL scripts
│   ├── bronze_to_silver_statistics.py
│   └── silver_to_gold_analytics.py
├── step-function/         # State machine definition (ASL JSON)
├── scripts/               # AWS CLI / bash upload scripts
└── README.md
```

---

## AWS Services Used

| Service | Purpose |
|---|---|
| **S3** | Bronze/Silver/Gold storage + scripts + Athena query results. Lifecycle rule archives raw data to Glacier Flexible Retrieval after 90 days. |
| **Lambda** | YouTube API ingestion, JSON→Parquet transform, data quality checks |
| **Glue (Spark, v5.1)** | Bronze→Silver and Silver→Gold ETL jobs, Data Catalog, Crawlers |
| **Athena** | Ad-hoc SQL + the query engine behind the Lambda data quality checks |
| **Step Functions** | Orchestrates the full pipeline end-to-end |
| **SNS** | Email alerts on success/failure for every stage |
| **IAM** | Least-privilege roles per service (see below) |

---

## S3 Layout

| Bucket | Purpose |
|---|---|
| `yt-data-pipeline-bronze-dev001` | Raw ingested data (`youtube/raw_statistics/`, `youtube/raw_statistics_reference_data/`) |
| `yt-data-pipeline-silver-dev001` | Cleaned Parquet (`youtube/statistics/`, `youtube/reference_data/`) |
| `yt-data-pipeline-gold-dev001` | Aggregated analytics tables |
| `yt-data-pipeline-scripts-dev001` | Glue script storage |
| Athena results bucket | Query output location for Athena / DQ Lambda |

Bronze data under `/youtube/` transitions to **Glacier Flexible Retrieval after 90 days** via an S3 lifecycle rule.

---

## Glue Data Catalog

| Database | Tables |
|---|---|
| `yt-pipeline-bronze_db` | `raw_statistics`, `raw_statistics_reference_data` |
| `yt-pipeline-silver_db` | `clean_statistics`, `clean_reference_data` |
| `yt-pipeline-gold_db` | `trending_analytics`, `channel_analytics`, `category_analytics` |

---

## IAM Roles (least privilege, not admin)

- **`yt-data-pipeline-lambda-role-dev`** — `AWSLambdaBasicExecutionRole` + inline policy scoped to `s3:GetObject/PutObject/ListBucket` on Bronze/Silver/Gold/scripts buckets only, plus Glue (get/create table, partitions), Athena (query execution), SNS publish.
- **`yt-data-pipeline-glue-role-dev`** — `AWSGlueServiceRole` + `AmazonS3FullAccess` + inline policy for S3/Glue/Athena/SNS.
- **`yt-data-pipeline-stepFunction-role-dev`** — `lambda:InvokeFunction`, `glue:StartJobRun/GetJobRun/GetJobRuns/BatchStopJobRun`, `sns:Publish` scoped to the project's SNS topic.

> Lesson learned the hard way: giving a role access to S3 buckets isn't enough — the Step Function role, the Lambda role, *and* whatever queries Athena all separately need read/write access to the **Athena query-results bucket**, not just the data buckets. Half the debugging time in this build went into `AccessDenied` errors from that being missed.

---

## Lambda Functions

| Function | Trigger | Role |
|---|---|---|
| `yt-data-pipeline-yt-lambda-ingestion-dev` | Scheduled / Step Functions | Pulls trending videos + category mappings from YouTube Data API v3 per region, writes raw JSON to Bronze, Hive-partitioned by `region/date/hour` |
| `yt-data-pipeline-yt-lambda-json-to-parquet-dev` | S3 event on Bronze reference-data prefix | Validates + deduplicates category JSON, writes Parquet to Silver, updates Glue Catalog |
| `yt-data-pipeline-data-quality-dev` | Step Functions | Runs 5 checks (row count, null %, schema, value range, freshness) against Silver tables via Athena; publishes SNS alert on failure |

**Common config:** Python 3.12, 512 MB memory, 1024 MB ephemeral storage, 5 min 3 sec timeout, `AWSSDKPandas-Python312` layer.

**Regions covered:** US, GB, CA, IN (configurable via `YOUTUBE_REGIONS` env var).

---

## Glue Jobs

### `yt-data-pipeline-bronze_to_silver_statistics-dev`
Reads raw CSV (Kaggle format) or JSON (live API format) from Bronze, auto-detects format, enforces schema, parses/standardizes `trending_date`, fills numeric nulls, computes `like_ratio` and `engagement_rate`, deduplicates on `video_id + region + trending_date` via window function, writes partitioned Parquet to Silver + updates Data Catalog.

### `yt-data-pipeline-silver_to_gold-analytics-dev`
Joins clean statistics with category reference data (with a fallback to `"Unknown"` if the join fails or reference data is missing), produces three Gold tables:
- **trending_analytics** — daily region-level summaries
- **channel_analytics** — per-channel performance, ranked by views within region
- **category_analytics** — category trends over time with view-share %

**Common config:** Glue 5.1 (Spark 3.5), `G.1X` workers × 2, job bookmarking disabled.

---

## Data Quality Checks

Run against Silver layer before Gold aggregation is allowed to proceed:

1. **Row count** — minimum threshold
2. **Null percentage** — per critical column (`video_id`, `title`, `channel_title`, `views`, `region`)
3. **Schema validation** — required columns present
4. **Value range** — no negative or absurd (>50B) view counts
5. **Freshness** — data no older than 48 hours (skipped gracefully if no timestamp column, e.g. backfills)

Failure on any check blocks the Gold job and fires an SNS alert with the full failure detail.

---

## Sample Athena Queries

```sql
-- Top trending days by views, US
SELECT region, trending_date_parsed, total_videos, total_views,
       avg_views_per_video, avg_engagement_rate, unique_channels
FROM trending_analytics
WHERE region = 'us'
ORDER BY total_views DESC
LIMIT 10;

-- Cross-region engagement comparison
SELECT region, AVG(avg_engagement_rate) AS avg_engagement,
       SUM(total_views) AS cumulative_views
FROM trending_analytics
GROUP BY region;

-- Viral channels trending in 3+ regions
SELECT channel_title, COUNT(DISTINCT region) AS region_count
FROM channel_analytics
GROUP BY channel_title
HAVING COUNT(DISTINCT region) >= 3
ORDER BY region_count DESC;
```

---

## Setup

1. Create an AWS account and configure the AWS CLI (`aws configure`) with an IAM user/keys scoped for this project (not root).
2. Get a **YouTube Data API v3** key from the Google Cloud Console.
3. Create the S3 buckets (Bronze/Silver/Gold/Scripts/Athena-results) and set the Bronze lifecycle rule.
4. Create the IAM roles above and attach the inline policies.
5. Create the SNS topic and subscribe your email.
6. Deploy the three Lambda functions with their env vars (see each function's docstring for the required variable names).
7. Create the two Glue jobs, pointing `--*` job parameters at your actual bucket/database names.
8. Run Glue Crawlers once to bootstrap the Bronze catalog (afterward, `enableUpdateCatalog` on the Glue sinks keeps Silver/Gold catalogs in sync automatically).
9. Deploy the Step Function state machine (`step-function/`) using the `yt-data-pipeline-stepFunction-role-dev` role.
10. Start an execution and watch it in the Step Functions Graph view.

**Never commit real API keys, account IDs, or ARNs to this repo.** Use environment variables / AWS Secrets Manager, and scrub any setup notes before pushing.

---

## Known Gaps / Next Steps

- No infrastructure-as-code (Terraform/CDK) — everything above was built manually via console. That's the honest state of it: reproducible by a human following steps, not reproducible with one command.
- No CI/CD for Glue script deployment — scripts are pasted directly into the Glue console editor.
- Ingestion is API-key-based with no rotation strategy or secrets manager integration yet.
- Data quality thresholds (`DQ_MIN_ROW_COUNT`, `DQ_MAX_NULL_PERCENT`) are env-var driven but not tuned against real production volumes.
- QuickSight/BI layer described in the architecture is not yet built out.

---

## License

MIT — do whatever you want with it, no warranty.
