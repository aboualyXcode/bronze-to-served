# 2. Unity Catalog and governance

Everything the platform creates lives in Unity Catalog, and everything it creates is code: running setup twice changes nothing the second time.

## Catalogs, schemas and volumes

- **One catalog per environment** (`stagedoor_dev`, `stagedoor_staging`, `stagedoor_prod`), so permissions and lineage separate cleanly and a dev job can never write to production by accident.
- **One schema per layer**, prefixed in dev (`alice_silver`), so every developer gets an isolated copy from the same bundle.
- **Volumes instead of DBFS** for files: `landing.raw` receives upstream files, `ops.checkpoints` holds stream checkpoints and Auto Loader schemas, `ops.exports` holds files for downstream systems. Volumes are governed like tables, and on Databricks compute they are POSIX paths, which is why the data generator writes to `/Volumes/...` with plain Python.

Setup (`stagedoor/setup.py:create_objects`, the `stagedoor_setup` job) creates the schemas, volumes and tables. It creates the catalog only with `create_catalog=true`, because that needs the `CREATE CATALOG` privilege and, on many metastores, a managed location. Most teams get the catalog from an administrator.

## Tables as contracts

Each table is declared once (`stagedoor/tables.py`) and rendered to DDL (`core/contracts.py`):

```sql
CREATE TABLE IF NOT EXISTS `stagedoor_dev`.`alice_ml`.`customer_features` (
  `customer_id` STRING NOT NULL COMMENT 'Customer.',
  `as_of_date` DATE NOT NULL COMMENT 'Features describe the customer at the end of this day.',
  ...
  CONSTRAINT `pk_customer_features` PRIMARY KEY (`customer_id`, `as_of_date` TIMESERIES)
) USING DELTA
CLUSTER BY (`as_of_date`)
COMMENT 'Churn features per customer and as-of date, computed only from data up to that date.'
```

The same contract does three more jobs:

| Feature | Where | Why |
|---|---|---|
| Column comments | every column | They appear in Catalog Explorer, AI/BI Genie and every BI tool reading the table. |
| Primary keys (informational) | Silver, Gold, ML | Documents grain; a key with `TIMESERIES` makes `customer_features` a time-series feature table for point-in-time lookups. |
| Liquid clustering | large, filtered tables (`CLUSTER BY`) | Data skipping on the columns queries filter on, without partitioning decisions you cannot undo. |
| Change data feed | `silver.orders`, `silver.customers`, `ml.churn_predictions` | Downstream readers fetch only what changed (the CRM export, [chapter 7](07-ml-lifecycle.md)). |
| `CHECK` constraints | `silver.orders`, `silver.order_items` | A last line of defence: Delta rejects a write that breaks them. |
| `conform(df, spec)` | before every write | Selects exactly the contract's columns, in order, cast to its types, so a write can never drift from the table. |

With **predictive optimization** (on by default for Unity Catalog managed tables in many accounts), Databricks runs `OPTIMIZE`, `VACUUM` and `ANALYZE` for you; the `stagedoor_maintenance` job does it explicitly for workspaces without it.

## Who can do what

The optional governance task (`setup.governance`, enabled with `apply_governance=true` on the setup job) grants by persona:

| Persona | Gets |
|---|---|
| Analysts | `USE CATALOG`; `USE SCHEMA`, `SELECT` on gold |
| Data scientists | read Silver and Gold; read, write and create tables and models in ml |
| Data engineers | `ALL PRIVILEGES` on bronze, silver, gold and ops |

It also tags PII (`contains_pii` on tables, `pii` on `email` and `full_name`), so the catalog can be searched for it and policies can target it.

## Masking and row-level security

Two techniques, shown side by side:

- **A dynamic view** (`gold.customer_360_secure`, created by the governance task) masks `email` unless the reader is in `stagedoor-pii-readers`, and shows only rows for regions the reader belongs to (`stagedoor-region-de`, ...), using `is_account_group_member`. Views are simple and never affect pipeline writes.
- **Row filters and column masks** (`notebooks/04_governance_and_operations.py`) attach SQL functions to the table itself, so every query is filtered, whatever the tool. The notebook applies them to a copy of `customer_360`. On tables the pipeline itself reads, include the pipeline's service principal in the filter's allow list, or downstream tasks silently see fewer rows; check the current limitations for streaming reads and some write paths before attaching them to tables a stream touches.

## Lineage and audit

- Unity Catalog records **table and column lineage** for every job and notebook automatically; `system.access.table_lineage` exposes it to SQL (notebook 04 shows which tables feed the features).
- `mlflow.log_input(mlflow.data.from_spark(...))` links each **model version** to the training table, so you can go from a served model back to its data.
- `ops.task_runs` has one row per task run (status, duration, metrics); `system.lakeflow.*` and `system.billing.usage` add job runs and cost.

## What is deliberately not automated

Creating groups and service principals, assigning catalog owners and managed storage locations, and network configuration belong to account administration, not to a data product's bundle.

**Practise:** run the setup job with `apply_governance=true`, then query `gold.customer_360_secure` as a member and a non-member of `stagedoor-pii-readers`.
