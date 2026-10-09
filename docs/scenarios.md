# Scenario catalog

Every scenario the repository implements, where the code is, the job that runs it, and what tests it. "Workspace" means it is exercised by the staging deployment rather than by local tests ([chapter 10](10-testing.md)).

## Ingestion and Bronze

| Scenario | Code | Runs in | Tested by |
|---|---|---|---|
| Incremental file ingestion with Auto Loader | `core/ingest.py`, `stagedoor/bronze.py` | daily job | Spark suites (file source), workspace |
| Schema drift kept in rescued data, never fatal | `read_files_stream` (rescue mode); the generator's `seat_section` | daily job | workspace; notebook 01 |
| Idempotent batch loads of CSV snapshots with COPY INTO | `core/ingest.py:copy_into`, `bronze.reference` | daily job | Spark suites (local equivalent), workspace |
| Kafka or Event Hubs as a streaming source | `read_kafka_stream`, `bronze.clickstream` with `source=kafka` | daily or streaming job | workspace |
| A simulated upstream that drops one realistic day at a time | `stagedoor/datagen.py`, `land.sample_data` | setup and daily jobs | `test_datagen.py` |

## Silver

| Scenario | Code | Runs in | Tested by |
|---|---|---|---|
| Change data capture into SCD Type 2 history | `core/merge.py:scd2_merge`, `silver.customers` | daily job | `test_merge_patterns.py`, `test_differential.py` |
| Out-of-order and duplicated updates (order-independent SCD1) | `scd1_merge`, `silver.orders` | daily job | `test_merge_patterns.py`, `test_differential.py` |
| Exactly-once event ingestion | `insert_new_only`, `silver.clickstream` | daily job | `test_merge_patterns.py`, `test_incremental.py` |
| Parsing dirty strings safely under ANSI mode (`try_cast`) | `stagedoor/silver.py` | daily job | `test_differential.py` |
| Normalizing time zones and codes (UTC, upper and lower case) | `stagedoor/silver.py` | daily job | `test_differential.py` |
| Expectations with quarantine and raw payloads | `core/quality.py`, `stagedoor/rules.py` | daily job | `test_quality.py`, `test_differential.py` |
| A quality gate that stops bad data before Gold | `operations.quality_gate` | daily job | `test_quality.py` |
| Full snapshots that replace | `silver.reference` | daily job | `test_differential.py` |

## Gold and analytics

| Scenario | Code | Runs in | Tested by |
|---|---|---|---|
| A star schema with point-in-time dimension keys | `gold.fct_sales_df` | daily job | `test_differential.py` |
| Revenue and refunds booked on their own dates | `gold.daily_sales_df` | daily job | `test_differential.py`, `test_reference_oracle.py` |
| Late data and catch-up with a high-water mark and `replaceWhere` | `gold._start`, `gold.sales`, `gold.funnel` | daily job | `test_incremental.py` |
| Backfill with a full refresh | `reset_stream`, `full_refresh=true` | any data job | `test_incremental.py` |
| Reproducible snapshots from data dates | `gold.customer_360` | daily job | `test_differential.py` |
| Stateful streaming with windows and watermarks | `gold.live_engagement` | streaming job | workspace |
| Always-on streams on classic compute | `designer/templates/streaming_continuous.json` | streaming job | designer checks |
| Analytics queries over SCD2 facts, sell-through, funnels | `notebooks/02_gold_analytics.py` | notebook | workspace |

## Machine learning

| Scenario | Code | Runs in | Tested by |
|---|---|---|---|
| Point-in-time features in a time-series feature table | `stagedoor/features.py` | training and daily jobs | `test_differential.py`, `test_reference_oracle.py` |
| Out-of-time train and test split | `features.training_set` | training job | `test_ml.py` |
| Training with MLflow tracking and dataset lineage | `ml/jobs.py:train`, `ml/registry.py` | training job | `test_ml.py` (model), workspace |
| Champion/challenger with a notebook report, a task value and a condition task | `ml/policy.py`, `notebooks/model_validation.py` | training job | `test_ml.py`, designer checks |
| Model aliases and one-step rollback | `ml.promote`, `ml.rollback` | training and rollback jobs | workspace |
| Real-time serving with scale to zero | `ml/serving.py` | training and rollback jobs | workspace |
| Distributed batch scoring | `ml/inference.py` | daily job | workspace |
| Feature drift with PSI | `ml/drift.py` | daily job | `test_ml.py` |
| Reverse ETL from the change data feed with bookmarks | `operations.crm_export` | daily job | `test_differential.py` (runs it) |

## Platform, governance and delivery

| Scenario | Code | Runs in | Tested by |
|---|---|---|---|
| Unity Catalog objects and table contracts as code | `core/uc.py`, `stagedoor/tables.py` | setup job | `test_contracts.py` |
| Grants by persona, PII tags and a dynamic secure view | `setup.apply_governance` | setup job (optional) | workspace |
| Row filters and column masks | `notebooks/04_governance_and_operations.py` | notebook | workspace |
| Lineage, job runs and cost from system tables | notebook 04 | notebook | workspace |
| Table maintenance with a for-each task | `operations.maintain_table` | maintenance job | designer checks, `test_bundle.py` |
| A declarative pipeline: expectations, AUTO CDC, a materialized view | `pipelines/stagedoor_declarative.py` | declarative job | workspace |
| A file arrival trigger | `designer/templates/declarative_refresh.json` | declarative job | `test_bundle.py` |
| Per-developer isolation and dev, staging and prod targets | `databricks.yml` | all | `test_bundle.py`, deploy workflow |
| CI/CD with GitHub Actions and a service principal | `.github/workflows/` | CI | CI |
| Designing jobs visually and exporting bundle YAML | `designer/` | browser | `designer/tests/run.js` |
| Running any job's DAG locally | `tasks/local.py` | laptop, CI | Spark suites |
