# Databricks notebook source
# MAGIC %md
# MAGIC # ML walkthrough: point-in-time features to a served model
# MAGIC Interactive version of the training job: inspect point-in-time features, train and compare models with
# MAGIC MLflow, manage Unity Catalog aliases, and query the serving endpoint.

# COMMAND ----------

# MAGIC %pip install -q "scikit-learn>=1.4" "mlflow>=2.16"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))
import mlflow  # noqa: E402
from bronze_to_served.ml.model import TrainParams, train_and_evaluate  # noqa: E402

dbutils.widgets.text("catalog", "stagedoor_dev")
dbutils.widgets.text("schema_prefix", "")
c, p = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix")
ml = f"`{c}`.`{p}ml`"
MODEL = f"{c}.{p}ml.churn_model"
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point in time: one customer across as-of dates
# MAGIC Each row uses only what was known at the end of `as_of_date`. The label looks 60 days ahead, which is
# MAGIC why the newest labelled date is 60 days before the newest data: anything later would leak the future.

# COMMAND ----------

display(spark.sql(f"""
  SELECT f.as_of_date, f.recency_days, f.orders_90d, f.events_7d, f.events_30d, f.engagement_ratio, f.loyalty_tier, l.churned
  FROM {ml}.customer_features f LEFT JOIN {ml}.churn_labels l USING (customer_id, as_of_date)
  WHERE customer_id = (SELECT customer_id FROM {ml}.churn_labels GROUP BY ALL HAVING max(churned) = 1 LIMIT 1)
  ORDER BY as_of_date"""))

# COMMAND ----------

# MAGIC %md ## Train two candidates and compare them in MLflow

# COMMAND ----------

data = spark.table(f"{ml}.churn_training_set")
train_pdf, test_pdf = data.where("split = 'train'").toPandas(), data.where("split = 'test'").toPandas()
mlflow.set_experiment(f"/Users/{spark.sql('SELECT current_user()').first()[0]}/bronze-to-served-walkthrough")
for name, params in {"baseline": TrainParams(), "shallow": TrainParams(max_leaf_nodes=15, learning_rate=0.1)}.items():
    with mlflow.start_run(run_name=name):
        result = train_and_evaluate(train_pdf, test_pdf, params)
        mlflow.log_params(params.__dict__)
        mlflow.log_metrics({k: v for k, v in result.metrics.items() if isinstance(v, float)})
        print(name, {k: round(v, 4) for k, v in result.metrics.items() if isinstance(v, float)})

# COMMAND ----------

# MAGIC %md ## Aliases: what is serving, what was serving, and the evaluation trail

# COMMAND ----------

client = mlflow.MlflowClient()
for alias in ("champion", "challenger", "previous_champion"):
    try:
        print(alias, "-> version", client.get_model_version_by_alias(MODEL, alias).version)
    except Exception:
        print(alias, "-> (none)")
display(spark.table(f"{ml}.model_evaluations").orderBy("evaluated_at", ascending=False))

# COMMAND ----------

# MAGIC %md ## Query the serving endpoint (after the training job deployed it)

# COMMAND ----------

from databricks.sdk import WorkspaceClient  # noqa: E402

records = (spark.table(f"{ml}.customer_features").orderBy("as_of_date", ascending=False).limit(3)
           .drop("customer_id", "as_of_date").toPandas())
records = records.astype({c: float for c in ("revenue_90d", "revenue_365d")}).assign(
    marketing_opt_in=records["marketing_opt_in"].astype(float))
try:
    response = WorkspaceClient().serving_endpoints.query(name=f"stagedoor-churn", dataframe_records=records.to_dict("records"))
    print(response.predictions)    # [[p(no churn), p(churn)], ...]
except Exception as exc:
    print("endpoint not available yet:", exc)
