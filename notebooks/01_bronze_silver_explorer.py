# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze and Silver explorer
# MAGIC What landed, what was rescued or quarantined, how SCD2 history looks, and Delta Lake's history, time
# MAGIC travel and change data feed on the tables the pipeline maintains.

# COMMAND ----------

dbutils.widgets.text("catalog", "stagedoor_dev")
dbutils.widgets.text("schema_prefix", "")
c, p = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix")
spark.sql(f"USE CATALOG `{c}`")
bronze, silver, ops = f"`{p}bronze`", f"`{p}silver`", f"`{p}ops`"

# COMMAND ----------

# MAGIC %md ## Schema drift: fields Auto Loader rescued instead of failing the stream

# COMMAND ----------

display(spark.sql(f"""
  SELECT date(_ingested_at) AS ingested, count(*) AS rows,
         count_if(_rescued_data IS NOT NULL) AS rows_with_rescued_fields,
         any_value(_rescued_data) AS example
  FROM {bronze}.orders_raw GROUP BY ALL ORDER BY ingested DESC"""))
# To promote `seat_section`: add it to the Bronze/Silver contracts and backfill Silver from
# get_json_object(_rescued_data, '$.seat_section') - no data was lost while the contract lagged behind.

# COMMAND ----------

# MAGIC %md ## Quarantine and quality metrics

# COMMAND ----------

display(spark.sql(f"""
  SELECT source_table, explode(failed_rules) AS rule, count(*) AS rows, any_value(payload) AS example
  FROM {ops}.quarantine GROUP BY ALL ORDER BY rows DESC"""))
display(spark.sql(f"""
  SELECT source_table, rule, action, sum(failed_rows) AS failed, sum(total_rows) AS checked,
         round(sum(failed_rows) / sum(total_rows), 5) AS rate
  FROM {ops}.quality_metrics GROUP BY ALL ORDER BY rate DESC"""))

# COMMAND ----------

# MAGIC %md ## SCD Type 2: one customer's history

# COMMAND ----------

customer = spark.sql(f"SELECT customer_id FROM {silver}.customers GROUP BY ALL ORDER BY count(*) DESC LIMIT 1").first()[0]
display(spark.sql(f"""
  SELECT customer_id, loyalty_tier, city, email, valid_from, valid_to, is_current, _start_seq, _end_seq
  FROM {silver}.customers WHERE customer_id = '{customer}' ORDER BY _start_seq"""))

# COMMAND ----------

# MAGIC %md ## Delta history, time travel and the change data feed

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {silver}.orders LIMIT 10"))
version = spark.sql(f"DESCRIBE HISTORY {silver}.orders").agg({"version": "max"}).first()[0]
previous = max(version - 1, 0)
display(spark.sql(f"""
  SELECT 'now' AS at, count(*) AS orders, count_if(status = 'REFUNDED') AS refunded FROM {silver}.orders
  UNION ALL
  SELECT 'version {previous}', count(*), count_if(status = 'REFUNDED') FROM {silver}.orders VERSION AS OF {previous}"""))
display(spark.sql(f"""
  SELECT _change_type, status, count(*) AS rows
  FROM table_changes('{c}.{p}silver.orders', {previous}) GROUP BY ALL ORDER BY rows DESC"""))
# Undo a bad load: RESTORE TABLE silver.orders TO VERSION AS OF <version>
