# Databricks notebook source
# MAGIC %md
# MAGIC # Model validation report
# MAGIC Runs inside the training job after `evaluate`. It shows the challenger next to the champion on the
# MAGIC same out-of-time test set, explains the decision, and sets the task value **`promote`** that the
# MAGIC `promotion_gate` condition task reads. The run page keeps this notebook's output, so every promotion
# MAGIC has a readable record of why it happened.

# COMMAND ----------

dbutils.widgets.text("catalog", "stagedoor_dev")
dbutils.widgets.text("schema_prefix", "")
dbutils.widgets.text("run_id", "")
catalog, prefix, run_id = (dbutils.widgets.get(n) for n in ("catalog", "schema_prefix", "run_id"))
evaluations = f"`{catalog}`.`{prefix}ml`.`model_evaluations`"

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

latest = spark.table(evaluations)
latest = latest.where(F.col("run_id") == run_id) if run_id else latest.where(
    F.col("evaluated_at") == latest.agg(F.max("evaluated_at")).first()[0])
display(latest.select("role", "model_version", "test_rows", "roc_auc", "pr_auc", "log_loss", "brier", "top_decile_lift"))

challenger = latest.where("role = 'challenger'").first()
if challenger is None:
    raise ValueError(f"no challenger evaluation found for run {run_id!r}")
promote = bool(challenger["promote"])
displayHTML("<h3>Decision: " + ("promote" if promote else "keep the current champion") + "</h3><ul>"
            + "".join(f"<li>{reason}</li>" for reason in challenger["reasons"]) + "</ul>")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="promote", value=promote)
dbutils.notebook.exit(str(promote).lower())
