"""Gold: a star schema and business aggregates, rebuilt deterministically from Silver.

Snapshots (dimensions, facts, customer_360, event_performance) are rebuilt in full. Daily aggregates are
recomputed incrementally with replaceWhere from a high-water mark: the latest date already in the table minus
`lookback_days`. That absorbs late data and catches up after missed runs without touching older history
(`full_refresh=true` rebuilds everything). Dates come from the data, never the wall clock.
"""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

from ..core.contracts import conform
from ..core.delta_ops import is_empty, overwrite, replace_where
from ..core.streams import run_stream
from ..tasks.context import TaskContext
from .tables import spec


def data_as_of(spark, cfg) -> date | None:
    """The latest day with data: the date of the most recent order event."""
    return spark.read.table(cfg.table("silver", "orders")).agg(F.max(F.to_date("updated_at"))).collect()[0][0]


def _write(ctx: TaskContext, df, table_ref: str, date_col: str | None = None, start: date | None = None) -> str:
    table = ctx.table(table_ref)
    if start is None:
        overwrite(conform(df, spec(table_ref)), table)
        return "full"
    replace_where(conform(df.filter(F.col(date_col) >= F.lit(start)), spec(table_ref)), table,
                  f"`{date_col}` >= DATE'{start.isoformat()}'")
    return f"since {start.isoformat()}"


def _start(ctx: TaskContext, table_ref: str, date_col: str) -> date | None:
    """First date to recompute: the table's high-water mark minus the lookback (None = rebuild everything)."""
    if ctx.cfg.full_refresh or is_empty(ctx.spark, ctx.table(table_ref)):
        return None
    last = ctx.spark.read.table(ctx.table(table_ref)).agg(F.max(date_col)).collect()[0][0]
    return None if last is None else last - timedelta(days=ctx.cfg.lookback_days)


# ---------------------------------------------------------------- dimensions and facts

def dimensions(ctx: TaskContext) -> dict:
    spark = ctx.spark
    overwrite(conform(spark.read.table(ctx.table("silver.customers")), spec("gold.dim_customer")),
              ctx.table("gold.dim_customer"))
    venues = spark.read.table(ctx.table("silver.venues")).select("venue_id", "venue_name", "city", "country", "capacity")
    events = spark.read.table(ctx.table("silver.events")).join(venues, "venue_id", "left")
    overwrite(conform(events, spec("gold.dim_event")), ctx.table("gold.dim_event"))
    return {"dimensions": 2}


def fct_sales_df(spark, cfg):
    orders = spark.read.table(cfg.table("silver", "orders")).where("paid_at IS NOT NULL")
    lines = orders.join(spark.read.table(cfg.table("silver", "order_items")), "order_id").alias("l")
    versions = spark.read.table(cfg.table("gold", "dim_customer")).select("customer_id", "customer_sk", "valid_from",
                                                                          "valid_to").alias("c")
    as_of_payment = ((F.col("l.customer_id") == F.col("c.customer_id")) & (F.col("c.valid_from") <= F.col("l.paid_at"))
                     & (F.col("c.valid_to").isNull() | (F.col("l.paid_at") < F.col("c.valid_to"))))
    refunded = F.col("l.refunded_at").isNotNull()
    return lines.join(versions, as_of_payment, "left").select(
        "l.order_id", "l.line_no", "l.customer_id", "c.customer_sk", "l.event_id", "l.channel", "l.ticket_type",
        "l.quantity", "l.unit_price", F.col("l.line_amount").alias("gross_amount"),
        F.when(refunded, F.col("l.line_amount")).otherwise(F.lit(0)).cast("decimal(12,2)").alias("refunded_amount"),
        "l.order_date", "l.paid_at", F.to_date("l.paid_at").alias("paid_date"), "l.refunded_at",
        F.to_date("l.refunded_at").alias("refunded_date"))


def daily_sales_df(fct, dim_event, start: date | None = None):
    f = fct.join(dim_event.select("event_id", "venue_id", "genre"), "event_id", "left")
    paid, refunds = f, f.where(F.col("refunded_date").isNotNull())
    if start is not None:
        paid, refunds = paid.where(F.col("paid_date") >= F.lit(start)), refunds.where(F.col("refunded_date") >= F.lit(start))
    p = paid.groupBy(F.col("paid_date").alias("sales_date"), "venue_id", "genre").agg(
        F.countDistinct("order_id").alias("paid_orders"), F.sum("quantity").alias("tickets_sold"),
        F.sum("gross_amount").alias("gross_revenue")).alias("p")
    r = refunds.groupBy(F.col("refunded_date").alias("sales_date"), "venue_id", "genre").agg(
        F.countDistinct("order_id").alias("refunded_orders"), F.sum("gross_amount").alias("refunded_amount")).alias("r")
    keys = [F.col("p.sales_date") == F.col("r.sales_date"), F.col("p.venue_id").eqNullSafe(F.col("r.venue_id")),
            F.col("p.genre").eqNullSafe(F.col("r.genre"))]
    zero = F.lit(0)
    return p.join(r, keys, "full_outer").select(
        F.coalesce("p.sales_date", "r.sales_date").alias("sales_date"), F.coalesce("p.venue_id", "r.venue_id").alias("venue_id"),
        F.coalesce("p.genre", "r.genre").alias("genre"), F.coalesce("p.paid_orders", zero).alias("paid_orders"),
        F.coalesce("p.tickets_sold", zero).alias("tickets_sold"), F.coalesce("p.gross_revenue", zero).alias("gross_revenue"),
        F.coalesce("r.refunded_orders", zero).alias("refunded_orders"),
        F.coalesce("r.refunded_amount", zero).alias("refunded_amount"),
        (F.coalesce("p.gross_revenue", zero) - F.coalesce("r.refunded_amount", zero)).alias("net_revenue"))


def sales(ctx: TaskContext) -> dict:
    spark, cfg = ctx.spark, ctx.cfg
    overwrite(conform(fct_sales_df(spark, cfg), spec("gold.fct_sales")), ctx.table("gold.fct_sales"))
    start = _start(ctx, "gold.agg_daily_sales", "sales_date")
    df = daily_sales_df(spark.read.table(ctx.table("gold.fct_sales")), spark.read.table(ctx.table("gold.dim_event")), start)
    return {"agg_daily_sales": _write(ctx, df, "gold.agg_daily_sales", "sales_date", start)}


def event_performance(ctx: TaskContext) -> dict:
    spark = ctx.spark
    fct = spark.read.table(ctx.table("gold.fct_sales"))
    agg = fct.groupBy("event_id").agg(
        F.sum(F.when(F.col("refunded_at").isNull(), F.col("quantity"))).alias("tickets_sold"),
        F.sum("gross_amount").alias("gross_revenue"), F.sum("refunded_amount").alias("refunded_amount"),
        F.min("paid_at").alias("first_sale_at"), F.max("paid_at").alias("last_sale_at"))
    zero = F.lit(0)
    df = (spark.read.table(ctx.table("gold.dim_event")).join(agg, "event_id", "left")
          .withColumn("tickets_sold", F.coalesce("tickets_sold", zero))
          .withColumn("gross_revenue", F.coalesce("gross_revenue", zero))
          .withColumn("refunded_amount", F.coalesce("refunded_amount", zero))
          .withColumn("net_revenue", F.col("gross_revenue") - F.col("refunded_amount"))
          .withColumn("sell_through", F.when(F.col("capacity") > 0, F.col("tickets_sold") / F.col("capacity"))))
    overwrite(conform(df, spec("gold.event_performance")), ctx.table("gold.event_performance"))
    return {"events": "full"}


def customer_360_df(spark, cfg, as_of: date):
    fct = spark.read.table(cfg.table("gold", "fct_sales"))
    stats = fct.groupBy("customer_id").agg(
        F.min("paid_at").alias("first_purchase_at"), F.max("paid_at").alias("last_purchase_at"),
        F.countDistinct("order_id").alias("lifetime_orders"),
        F.countDistinct(F.when(F.col("refunded_at").isNotNull(), F.col("order_id"))).alias("refunded_orders"),
        (F.sum("gross_amount") - F.sum("refunded_amount")).alias("lifetime_net_revenue"))
    tickets = (fct.join(spark.read.table(cfg.table("gold", "dim_event")).select("event_id", "genre"), "event_id")
               .groupBy("customer_id", "genre").agg(F.sum("quantity").alias("tickets")))
    rank = Window.partitionBy("customer_id").orderBy(F.col("tickets").desc(), F.col("genre").asc())
    favorite = (tickets.withColumn("rn", F.row_number().over(rank)).where("rn = 1")
                .select("customer_id", F.col("genre").alias("favorite_genre")))
    recent = (F.col("event_date") > F.date_sub(F.lit(as_of), 30)) & (F.col("event_date") <= F.lit(as_of))
    clicks = (spark.read.table(cfg.table("silver", "clickstream")).where(F.col("customer_id").isNotNull())
              .groupBy("customer_id").agg(F.sum(F.when(recent, 1).otherwise(0)).alias("events_30d"),
                                          F.max("event_ts").alias("last_seen_at")))
    zero = F.lit(0)
    return (spark.read.table(cfg.table("gold", "dim_customer")).where("is_current")
            .join(stats, "customer_id", "left").join(favorite, "customer_id", "left").join(clicks, "customer_id", "left")
            .withColumn("lifetime_orders", F.coalesce("lifetime_orders", zero))
            .withColumn("refunded_orders", F.coalesce("refunded_orders", zero))
            .withColumn("lifetime_net_revenue", F.coalesce("lifetime_net_revenue", zero))
            .withColumn("events_30d", F.coalesce("events_30d", zero))
            .withColumn("days_since_last_purchase", F.datediff(F.lit(as_of), F.to_date("last_purchase_at")))
            .withColumn("as_of_date", F.lit(as_of)))


def customer_360(ctx: TaskContext) -> dict:
    as_of = data_as_of(ctx.spark, ctx.cfg)
    overwrite(conform(customer_360_df(ctx.spark, ctx.cfg, as_of), spec("gold.customer_360")), ctx.table("gold.customer_360"))
    return {"as_of_date": str(as_of)}


def funnel_df(clicks, start: date | None = None):
    if start is not None:     # sessions last minutes, so one extra day captures every session starting on `start`
        clicks = clicks.where(F.col("event_date") >= F.date_sub(F.lit(start), 1))

    def reached(event_type):
        return F.max((F.col("event_type") == event_type).cast("int"))

    sessions = clicks.groupBy("session_id").agg(
        F.min("event_ts").alias("started_at"), F.min("device").alias("device"), reached("view_event").alias("viewed"),
        reached("add_to_cart").alias("carted"), reached("checkout").alias("checked_out"),
        reached("purchase").alias("purchased"))
    if start is not None:
        sessions = sessions.where(F.to_date("started_at") >= F.lit(start))
    return (sessions.groupBy(F.to_date("started_at").alias("session_date"), "device")
            .agg(F.count(F.lit(1)).alias("sessions"), F.sum("viewed").alias("viewed_sessions"),
                 F.sum("carted").alias("cart_sessions"), F.sum("checked_out").alias("checkout_sessions"),
                 F.sum("purchased").alias("purchase_sessions"))
            .withColumn("view_to_purchase_rate",
                        F.when(F.col("viewed_sessions") > 0, F.col("purchase_sessions") / F.col("viewed_sessions"))))


def funnel(ctx: TaskContext) -> dict:
    start = _start(ctx, "gold.funnel_daily", "session_date")
    df = funnel_df(ctx.spark.read.table(ctx.table("silver.clickstream")), start)
    return {"funnel_daily": _write(ctx, df, "gold.funnel_daily", "session_date", start)}


def live_engagement(ctx: TaskContext) -> dict:
    """Stateful streaming: 5-minute windows, emitted once the 30-minute watermark has passed them (append mode)."""
    clicks = ctx.spark.readStream.option("skipChangeCommits", "true").table(ctx.table("silver.clickstream"))
    windows = (clicks.withWatermark("event_ts", "30 minutes")
               .groupBy(F.window("event_ts", "5 minutes").alias("w"), "event_type")
               .agg(F.count(F.lit(1)).alias("events"), F.approx_count_distinct("session_id").alias("sessions"))
               .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "event_type",
                       "events", "sessions"))
    result = run_stream(conform(windows, spec("gold.live_engagement")), ctx.cfg, name="gold_live_engagement",
                        table=ctx.table("gold.live_engagement"))
    return {"micro_batches": result.batches, "rows": result.input_rows}
