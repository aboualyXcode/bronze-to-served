# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Served: quickstart
# MAGIC
# MAGIC Runs the whole platform from this notebook, step by step, on serverless or any Unity Catalog cluster:
# MAGIC Unity Catalog objects, a year of simulated landing files, Bronze, Silver, the quality gate, Gold,
# MAGIC features, a trained and registered churn model, batch scoring and drift.
# MAGIC
# MAGIC **How:** add this repository as a Git folder (Workspace > Create > Git folder), open this notebook and
# MAGIC *Run all*. It imports the library straight from `../src`, so no wheel is needed. In production the same
# MAGIC tasks run as jobs (see `resources/` and `databricks bundle deploy`).

# COMMAND ----------

# MAGIC %pip install -q "scikit-learn>=1.4" "mlflow>=2.16"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
import uuid

sys.path.insert(0, os.path.abspath("../src"))      # the library, from this Git folder

from bronze_to_served.tasks.cli import make_config, run_task  # noqa: E402

dbutils.widgets.text("catalog", "stagedoor_dev", "Catalog")
dbutils.widgets.text("schema_prefix", "", "Schema prefix (e.g. alice_)")
dbutils.widgets.dropdown("create_catalog", "false", ["true", "false"], "Create the catalog")
dbutils.widgets.text("customers", "2000", "Simulated customers")

cfg = make_config(catalog=dbutils.widgets.get("catalog"), schema_prefix=dbutils.widgets.get("schema_prefix"),
                  runtime="databricks")
RUN_ID = f"quickstart-{uuid.uuid4().hex[:8]}"
USER = spark.sql("SELECT current_user()").first()[0]


def run(task, **params):
    result = run_task(task, cfg, {k: str(v) for k, v in params.items()}, spark=spark, run_id=RUN_ID)
    print(f"{task:24s} {result}")
    return result

# COMMAND ----------

# MAGIC %md ## 1. Unity Catalog objects and a year of landing files

# COMMAND ----------

run("setup.uc_objects", create_catalog=dbutils.widgets.get("create_catalog"))
run("land.sample_data", mode="history", history_days=370, customers=dbutils.widgets.get("customers"))
display(dbutils.fs.ls(cfg.landing_path()))

# COMMAND ----------

# MAGIC %md ## 2. Bronze -> Silver (streaming with `availableNow`) -> quality gate

# COMMAND ----------

for task in ("bronze.orders", "bronze.customers_cdc", "bronze.clickstream", "bronze.reference",
             "silver.orders", "silver.customers", "silver.clickstream", "silver.reference", "quality.gate"):
    run(task)
display(spark.sql(f"SELECT source_table, failed_rules, count(*) AS rows FROM {cfg.table('ops', 'quarantine')} "
                  "GROUP BY ALL ORDER BY rows DESC"))

# COMMAND ----------

# MAGIC %md ## 3. Gold

# COMMAND ----------

for task in ("gold.dimensions", "gold.sales", "gold.event_performance", "gold.customer_360", "gold.funnel"):
    run(task)
display(spark.table(cfg.table("gold", "agg_daily_sales")).orderBy("sales_date", ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md ## 4. Features, training, evaluation, promotion

# COMMAND ----------

experiment = f"/Users/{USER}/bronze-to-served-quickstart"
run("ml.features", mode="training")
run("ml.training_set")
run("ml.train", experiment_path=experiment)
evaluation = run("ml.evaluate")
if evaluation["promote"]:
    run("ml.promote")

# COMMAND ----------

# MAGIC %md ## 5. Score today's customers and check drift

# COMMAND ----------

run("ml.features", mode="scoring")
run("ml.batch_inference")
run("ml.drift")
display(spark.sql(f"SELECT risk_band, count(*) AS customers, round(avg(churn_probability), 3) AS avg_probability "
                  f"FROM {cfg.table('ml', 'churn_predictions')} GROUP BY ALL ORDER BY avg_probability DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Simulate tomorrow
# MAGIC Land one more day and rerun Bronze -> Silver -> Gold: only the new files are read, and Gold recomputes
# MAGIC only the last few days. Repeat to watch the platform move forward one day at a time.

# COMMAND ----------

run("land.sample_data", mode="next_day")
for task in ("bronze.orders", "bronze.customers_cdc", "bronze.clickstream", "bronze.reference", "silver.orders",
             "silver.customers", "silver.clickstream", "silver.reference", "quality.gate", "gold.dimensions",
             "gold.sales", "gold.customer_360", "gold.funnel"):
    run(task)
display(spark.table(cfg.table("ops", "task_runs")).where(f"run_id = '{RUN_ID}'").orderBy("started_at"))
