"""Point-in-time churn features and labels.

A feature row (customer, as_of_date) uses only data up to the end of as_of_date: orders paid by then,
refunds made by then, app events by then, and the customer's SCD2 version in effect at that moment.
The label looks forward: churned = no paid order in (as_of_date, as_of_date + 60 days]. Training uses only
as-of dates whose 60-day window is complete; scoring computes the latest date, which has no label yet.
"""
from __future__ import annotations

import math
from datetime import date, timedelta

from pyspark.sql import functions as F

from ..core.contracts import conform
from ..core.delta_ops import date_list_predicate, overwrite, replace_where
from ..tasks.context import TaskContext
from .gold import data_as_of
from .tables import spec

HORIZON_DAYS = 60     # label window
EVERY_DAYS = 14       # one training snapshot every two weeks
WARMUP_DAYS = 90      # history needed before the first snapshot


def training_as_of_dates(first: date, last: date, every: int = EVERY_DAYS, warmup: int = WARMUP_DAYS,
                         horizon: int = HORIZON_DAYS) -> list[date]:
    out, d = [], first + timedelta(days=warmup)
    while d + timedelta(days=horizon) <= last:
        out.append(d)
        d += timedelta(days=every)
    return out


def _within(column: str, days: int):
    """(as_of_date - days, as_of_date]"""
    return (F.col(column) > F.date_sub(F.col("as_of_date"), days)) & (F.col(column) <= F.col("as_of_date"))


def compute_features(spark, cfg, as_of_dates: list[date]):
    dates = spark.createDataFrame([(d,) for d in as_of_dates], "as_of_date date")
    paid = (spark.read.table(cfg.table("silver", "orders")).where("paid_at IS NOT NULL")
            .select("customer_id", "order_id", "order_amount", F.to_date("paid_at").alias("paid_date"),
                    F.to_date("refunded_at").alias("refunded_date")))
    population = dates.join(paid, _within("paid_date", 365)).select("as_of_date", "customer_id").distinct()
    moment = F.date_add(F.col("as_of_date"), 1).cast("timestamp")   # midnight after the as-of date
    versions = spark.read.table(cfg.table("silver", "customers")).select(
        "customer_id", "loyalty_tier", "marketing_opt_in", "country", "signup_date", "valid_from", "valid_to")
    people = (population.join(versions, "customer_id")
              .where((F.col("valid_from") <= moment) & (F.col("valid_to").isNull() | (F.col("valid_to") > moment))))
    keys = people.select("as_of_date", "customer_id")
    orders = keys.join(paid, "customer_id").groupBy("as_of_date", "customer_id").agg(
        F.datediff(F.col("as_of_date"), F.max(F.when(F.col("paid_date") <= F.col("as_of_date"), F.col("paid_date"))))
        .alias("recency_days"),
        *[F.countDistinct(F.when(_within("paid_date", n), F.col("order_id"))).alias(f"orders_{n}d") for n in (30, 90, 365)],
        *[F.sum(F.when(_within("paid_date", n), F.col("order_amount")).otherwise(F.lit(0))).alias(f"revenue_{n}d")
          for n in (90, 365)],
        F.countDistinct(F.when(_within("refunded_date", 365), F.col("order_id"))).alias("refunds_365d"))
    genres = spark.read.table(cfg.table("gold", "dim_event")).select("event_id", "genre")
    lines = (keys.join(spark.read.table(cfg.table("gold", "fct_sales")).select("customer_id", "paid_date", "gross_amount",
                                                                               "quantity", "event_id"), "customer_id")
             .where(_within("paid_date", 365)).join(genres, "event_id", "left")
             .groupBy("as_of_date", "customer_id")
             .agg(F.sum("gross_amount").alias("gross"), F.sum("quantity").alias("tickets"),
                  F.countDistinct("genre").alias("genres_365d")))
    daily = (spark.read.table(cfg.table("silver", "clickstream")).where(F.col("customer_id").isNotNull())
             .groupBy("customer_id", "event_date")
             .agg(F.count(F.lit(1)).alias("events"), F.sum((F.col("event_type") == "add_to_cart").cast("int")).alias("carts"))
             .alias("k"))
    k = keys.alias("p")
    in_month = ((F.col("p.customer_id") == F.col("k.customer_id")) & (F.col("k.event_date") <= F.col("p.as_of_date"))
                & (F.col("k.event_date") > F.date_sub(F.col("p.as_of_date"), 30)))
    engagement = (k.join(daily, in_month, "left")
                  .groupBy(F.col("p.as_of_date").alias("as_of_date"), F.col("p.customer_id").alias("customer_id"))
                  .agg(F.sum(F.when(F.col("k.event_date") > F.date_sub(F.col("p.as_of_date"), 7), F.col("k.events"))
                             .otherwise(0)).alias("events_7d"),
                       F.sum(F.coalesce(F.col("k.events"), F.lit(0))).alias("events_30d"),
                       F.sum(F.coalesce(F.col("k.carts"), F.lit(0))).alias("cart_adds_30d")))
    on = ["as_of_date", "customer_id"]
    return (people.join(orders, on).join(lines, on).join(engagement, on)
            .withColumn("avg_ticket_price_365d", F.when(F.col("tickets") > 0, F.col("gross").cast("double") / F.col("tickets")))
            .withColumn("engagement_ratio", F.when(F.col("events_30d") > 0, F.col("events_7d") / F.col("events_30d")))
            .withColumn("tenure_days", F.datediff("as_of_date", "signup_date")))


def compute_labels(spark, cfg, features, last: date):
    paid = (spark.read.table(cfg.table("silver", "orders")).where("paid_at IS NOT NULL")
            .select("customer_id", F.to_date("paid_at").alias("paid_date")).alias("o"))
    f = features.select("customer_id", "as_of_date").alias("f")
    ahead = ((F.col("f.customer_id") == F.col("o.customer_id")) & (F.col("o.paid_date") > F.col("f.as_of_date"))
             & (F.col("o.paid_date") <= F.date_add(F.col("f.as_of_date"), HORIZON_DAYS)))
    return (f.join(paid, ahead, "left")
            .groupBy(F.col("f.customer_id").alias("customer_id"), F.col("f.as_of_date").alias("as_of_date"))
            .agg(F.max(F.col("o.paid_date").isNotNull().cast("int")).alias("bought"))
            .withColumn("churned", (F.lit(1) - F.col("bought")).cast("int"))
            .where(F.date_add(F.col("as_of_date"), HORIZON_DAYS) <= F.lit(last)))


def data_bounds(spark, cfg) -> tuple[date | None, date | None]:
    first = spark.read.table(cfg.table("silver", "orders")).agg(F.min("order_date")).collect()[0][0]
    return first, data_as_of(spark, cfg)


def features(ctx: TaskContext) -> dict:
    """mode=scoring: features for the latest data date. mode=training: every labelled snapshot, plus labels."""
    spark, cfg = ctx.spark, ctx.cfg
    first, last = data_bounds(spark, cfg)
    if last is None:
        return {"skipped": "no orders yet"}
    mode = ctx.param("mode", "scoring")
    dates = training_as_of_dates(first, last) if mode == "training" else [last]
    if not dates:
        return {"skipped": "not enough history for a labelled snapshot"}
    feats = compute_features(spark, cfg, dates)
    predicate = date_list_predicate("as_of_date", dates)
    replace_where(conform(feats, spec("ml.customer_features")), ctx.table("ml.customer_features"), predicate)
    if mode == "training":
        labels = compute_labels(spark, cfg, spark.read.table(ctx.table("ml.customer_features")).where(predicate), last)
        replace_where(conform(labels, spec("ml.churn_labels")), ctx.table("ml.churn_labels"), predicate)
    return {"mode": mode, "as_of_dates": len(dates), "latest": dates[-1].isoformat()}


def training_set(ctx: TaskContext) -> dict:
    """Join features to labels and hold out the latest 20% of as-of dates: an out-of-time test set."""
    spark = ctx.spark
    data = spark.read.table(ctx.table("ml.customer_features")).join(
        spark.read.table(ctx.table("ml.churn_labels")), ["customer_id", "as_of_date"])
    dates = sorted(r[0] for r in data.select("as_of_date").distinct().collect())
    if len(dates) < 2:
        raise ValueError("need at least two labelled as-of dates; run ml.features with mode=training first")
    test_dates = dates[-max(1, math.ceil(len(dates) * ctx.float_param("test_share", 0.2))):]
    data = data.withColumn("split", F.when(F.col("as_of_date").isin(test_dates), F.lit("test")).otherwise(F.lit("train")))
    overwrite(conform(data, spec("ml.churn_training_set")), ctx.table("ml.churn_training_set"))
    return {"as_of_dates": len(dates), "test_from": test_dates[0].isoformat()}
