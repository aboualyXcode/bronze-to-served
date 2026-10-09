# Databricks notebook source
# MAGIC %md
# MAGIC # Gold analytics
# MAGIC The star schema in use. Facts carry the customer *version* in effect at payment time (`customer_sk`), so
# MAGIC revenue by loyalty tier is reported by the tier customers had when they paid, not the tier they have
# MAGIC today. Pin these queries to an AI/BI dashboard, or point any BI tool at a SQL warehouse.

# COMMAND ----------

dbutils.widgets.text("catalog", "stagedoor_dev")
dbutils.widgets.text("schema_prefix", "")
c, p = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema_prefix")
spark.sql(f"USE CATALOG `{c}`")
spark.sql(f"USE SCHEMA `{p}gold`")

# COMMAND ----------

# MAGIC %md ## Revenue by the tier customers had when they paid (SCD2 point-in-time)

# COMMAND ----------

display(spark.sql("""
  SELECT date_trunc('month', f.paid_date) AS month, d.loyalty_tier,
         sum(f.gross_amount - f.refunded_amount) AS net_revenue, count(DISTINCT f.order_id) AS orders
  FROM fct_sales f JOIN dim_customer d ON d.customer_sk = f.customer_sk
  GROUP BY ALL ORDER BY month, loyalty_tier"""))

# COMMAND ----------

# MAGIC %md ## Daily net revenue with a 7-day moving average

# COMMAND ----------

display(spark.sql("""
  SELECT sales_date, sum(net_revenue) AS net_revenue,
         avg(sum(net_revenue)) OVER (ORDER BY sales_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS net_revenue_7d_avg
  FROM agg_daily_sales GROUP BY sales_date ORDER BY sales_date"""))

# COMMAND ----------

# MAGIC %md ## Shows close to selling out, and the funnel

# COMMAND ----------

display(spark.sql("""
  SELECT artist, genre, event_date, capacity, tickets_sold, round(sell_through, 3) AS sell_through, net_revenue
  FROM event_performance WHERE event_date >= (SELECT max(as_of_date) FROM customer_360)
  ORDER BY sell_through DESC LIMIT 20"""))
display(spark.sql("""
  SELECT device, sum(sessions) AS sessions, sum(viewed_sessions) AS viewed, sum(cart_sessions) AS carted,
         sum(purchase_sessions) AS purchased, round(sum(purchase_sessions) / sum(viewed_sessions), 4) AS conversion
  FROM funnel_daily GROUP BY ALL ORDER BY sessions DESC"""))

# COMMAND ----------

# MAGIC %md ## Customers worth a call: high value, quiet lately

# COMMAND ----------

display(spark.sql("""
  SELECT customer_id, loyalty_tier, lifetime_orders, lifetime_net_revenue, favorite_genre, days_since_last_purchase, events_30d
  FROM customer_360 WHERE lifetime_orders >= 5 AND days_since_last_purchase > 60
  ORDER BY lifetime_net_revenue DESC LIMIT 25"""))
