# Databricks notebook source
# MAGIC %md
# MAGIC # Governance and operations
# MAGIC Grants, tags, row filters and column masks; lineage from system tables; and the platform's own
# MAGIC operational tables (task runs, quality metrics) next to Databricks job and billing system tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "stagedoor_dev")
dbutils.widgets.text("schema_prefix", "")
dbutils.widgets.text("pii_group", "stagedoor-pii-readers")
c, p, pii = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix"), dbutils.widgets.get("pii_group")
spark.sql(f"USE CATALOG `{c}`")
gold, ops = f"`{p}gold`", f"`{p}ops`"

# COMMAND ----------

# MAGIC %md ## Who can do what, and where the PII is

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {gold}"))
display(spark.sql(f"""
  SELECT schema_name, table_name, column_name, tag_name, tag_value
  FROM `{c}`.information_schema.column_tags WHERE schema_name LIKE '{p}%' ORDER BY ALL"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row filters and column masks
# MAGIC Applied to a copy of `customer_360`, so the pipeline's own tables are untouched. The functions decide
# MAGIC per query, from the reader's group membership.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {gold}.customer_360_governed AS SELECT * FROM {gold}.customer_360")
spark.sql(f"""CREATE OR REPLACE FUNCTION {ops}.mask_email(email STRING) RETURNS STRING
  RETURN CASE WHEN is_account_group_member('{pii}') THEN email ELSE regexp_replace(email, '^[^@]+', '***') END""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {ops}.region_filter(country STRING) RETURNS BOOLEAN
  RETURN is_account_group_member('stagedoor-all-regions')
      OR is_account_group_member(concat('stagedoor-region-', lower(country)))""")
spark.sql(f"ALTER TABLE {gold}.customer_360_governed ALTER COLUMN email SET MASK {ops}.mask_email")
spark.sql(f"ALTER TABLE {gold}.customer_360_governed SET ROW FILTER {ops}.region_filter ON (country)")
display(spark.sql(f"SELECT customer_id, email, country FROM {gold}.customer_360_governed LIMIT 10"))

# COMMAND ----------

# MAGIC %md ## Lineage: what feeds the churn features, from Unity Catalog's system tables

# COMMAND ----------

display(spark.sql(f"""
  SELECT source_table_full_name, target_table_full_name, max(event_time) AS last_seen
  FROM system.access.table_lineage
  WHERE target_table_full_name LIKE '{c}.{p}%' AND source_table_full_name IS NOT NULL
  GROUP BY ALL ORDER BY target_table_full_name"""))

# COMMAND ----------

# MAGIC %md ## Operations: the platform's own audit trail, then Databricks system tables

# COMMAND ----------

display(spark.sql(f"""
  SELECT task, count(*) AS runs, count_if(status = 'FAILED') AS failed,
         round(avg(timestampdiff(SECOND, started_at, finished_at)), 1) AS avg_seconds
  FROM {ops}.task_runs GROUP BY ALL ORDER BY avg_seconds DESC"""))
display(spark.sql("""
  SELECT j.name, t.result_state, count(*) AS runs, round(avg(t.period_end_time - t.period_start_time) / 60, 1) AS avg_min
  FROM system.lakeflow.job_run_timeline t JOIN system.lakeflow.jobs j USING (workspace_id, job_id)
  WHERE j.name LIKE 'stagedoor%' AND t.period_start_time > current_date() - INTERVAL 30 DAYS
  GROUP BY ALL ORDER BY runs DESC"""))
display(spark.sql("""
  SELECT usage_date, sku_name, sum(usage_quantity) AS dbus
  FROM system.billing.usage
  WHERE usage_metadata.job_name LIKE 'stagedoor%' AND usage_date > current_date() - INTERVAL 30 DAYS
  GROUP BY ALL ORDER BY usage_date DESC"""))
