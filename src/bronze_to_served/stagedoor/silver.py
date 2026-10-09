"""Silver: type, validate, deduplicate and merge. Each stream reads Bronze incrementally and applies every
micro-batch with an idempotent MERGE, so retries, replays and late data never double count.

    orders       SCD1 with status timestamps (newest event wins; paid_at/refunded_at keep the first time)
    customers    SCD2 history from CDC, ordered by LSN, deletes close the current version
    clickstream  exactly-once inserts keyed by event_id
    reference    latest full snapshot of shows and venues
"""
from __future__ import annotations

from pyspark.sql import functions as F

from ..core.contracts import conform
from ..core.delta_ops import delete_all, overwrite
from ..core.merge import insert_new_only, scd1_merge, scd2_merge
from ..core.quality import annotate, record_quality, valid_rows
from ..core.streams import reset_stream, run_stream
from ..tasks.context import TaskContext
from . import rules, sources
from .tables import spec

ITEMS_SCHEMA = "array<struct<line_no:string,ticket_type:string,quantity:string,unit_price:string>>"
TRACKED = ["email", "full_name", "country", "city", "loyalty_tier", "marketing_opt_in", "signup_date"]


def _clean(column: str):
    return F.trim(F.col(column))


def _try_cast(column: str, sql_type: str):
    # try_cast returns NULL instead of failing (ANSI mode is on by default on serverless and Spark 4)
    return F.expr(f"try_cast(trim(`{column}`) AS {sql_type})")


def _payload(columns) -> "F.Column":
    return F.to_json(F.struct(*[F.col(c) for c in columns])).alias("_payload")


# ---------------------------------------------------------------- parsing (Bronze strings -> typed columns)

def parse_orders(df):
    return df.select(
        _payload(sources.ORDERS_RAW + ("_rescued_data",)),
        _clean("order_id").alias("order_id"), _clean("customer_id").alias("customer_id"),
        _clean("event_id").alias("event_id"), _try_cast("order_ts", "TIMESTAMP").alias("order_ts"),
        _try_cast("updated_at", "TIMESTAMP").alias("updated_at"), F.upper(_clean("status")).alias("status"),
        F.lower(_clean("channel")).alias("channel"), F.upper(_clean("currency")).alias("currency"),
        _clean("promo_code").alias("promo_code"),
        F.expr(f"transform(from_json(items, '{ITEMS_SCHEMA}'), x -> named_struct("
               "'line_no', try_cast(trim(x.line_no) AS INT), 'ticket_type', trim(x.ticket_type), "
               "'quantity', try_cast(trim(x.quantity) AS INT), "
               "'unit_price', try_cast(trim(x.unit_price) AS DECIMAL(10,2))))").alias("items"),
        "_source_file", "_ingested_at")


def parse_customer_changes(df):
    return df.select(
        _payload(sources.CUSTOMERS_CDC_RAW + ("_rescued_data",)),
        _clean("customer_id").alias("customer_id"), _try_cast("lsn", "BIGINT").alias("lsn"),
        F.upper(_clean("op")).alias("op"), _try_cast("changed_at", "TIMESTAMP").alias("changed_at"),
        _clean("email").alias("email"), _clean("full_name").alias("full_name"),
        F.upper(_clean("country")).alias("country"), _clean("city").alias("city"),
        F.lower(_clean("loyalty_tier")).alias("loyalty_tier"),
        _try_cast("marketing_opt_in", "BOOLEAN").alias("marketing_opt_in"),
        _try_cast("signup_date", "DATE").alias("signup_date"), "_source_file", "_ingested_at")


def parse_clickstream(df):
    return df.select(
        _payload(sources.CLICKSTREAM_RAW + ("_rescued_data",)),
        _clean("event_id").alias("event_id"), _clean("session_id").alias("session_id"),
        _clean("customer_id").alias("customer_id"), F.lower(_clean("event_type")).alias("event_type"),
        _clean("event_ref").alias("event_ref"), F.lower(_clean("device")).alias("device"),
        _try_cast("event_ts", "TIMESTAMP").alias("event_ts"), F.col("properties"), "_source_file", "_ingested_at")


# ---------------------------------------------------------------- micro-batch handlers

def orders_batch(cfg, run_id: str):
    orders_table, items_table = cfg.table("silver", "orders"), cfg.table("silver", "order_items")

    def handle(batch_df, batch_id: int) -> None:
        spark = batch_df.sparkSession
        annotated = annotate(parse_orders(batch_df), rules.ORDER_RULES)
        record_quality(spark, cfg, annotated, rules.ORDER_RULES, table_ref="silver.orders", run_id=run_id,
                       key_col="order_id")
        valid = valid_rows(annotated)

        def latest(column):
            return F.max_by(column, "updated_at").alias(column)

        def first_time(status):
            return F.min(F.when(F.col("status") == status, F.col("updated_at")))

        orders = (valid.groupBy("order_id")
                  .agg(latest("customer_id"), latest("event_id"), latest("order_ts"), latest("status"),
                       latest("channel"), latest("currency"), latest("promo_code"), latest("items"),
                       F.max("updated_at").alias("updated_at"), first_time("PAID").alias("paid_at"),
                       first_time("CANCELLED").alias("cancelled_at"), first_time("REFUNDED").alias("refunded_at"),
                       F.min("_ingested_at").alias("_first_ingested_at"),
                       F.max("_ingested_at").alias("_last_ingested_at"))
                  .withColumn("order_date", F.to_date("order_ts"))
                  .withColumn("order_amount", F.expr("aggregate(items, CAST(0 AS DECIMAL(12,2)), "
                                                     "(acc, x) -> CAST(acc + x.quantity * x.unit_price AS DECIMAL(12,2)))"))
                  .withColumn("ticket_count", F.expr("aggregate(items, 0, (acc, x) -> acc + x.quantity)")))
        scd1_merge(spark, orders_table, conform(orders, spec("silver.orders")), keys=["order_id"],
                   sequence_col="updated_at", first_seen=("paid_at", "cancelled_at", "refunded_at", "_first_ingested_at"),
                   last_seen=("_last_ingested_at",))
        items = (valid.select("order_id", F.explode("items").alias("i"))
                 .select("order_id", "i.line_no", "i.ticket_type", "i.quantity", "i.unit_price")
                 .withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)")))
        insert_new_only(spark, items_table, conform(items, spec("silver.order_items")), keys=["order_id", "line_no"])

    return handle


def customers_batch(cfg, run_id: str):
    target = cfg.table("silver", "customers")

    def handle(batch_df, batch_id: int) -> None:
        spark = batch_df.sparkSession
        annotated = annotate(parse_customer_changes(batch_df), rules.CDC_RULES)
        record_quality(spark, cfg, annotated, rules.CDC_RULES, table_ref="silver.customers", run_id=run_id,
                       key_col="customer_id")
        changes = valid_rows(annotated).select("customer_id", "lsn", "changed_at", "op", *TRACKED)
        scd2_merge(spark, target, changes, key="customer_id", sequence_col="lsn", timestamp_col="changed_at",
                   op_col="op", tracked=TRACKED, sk_col="customer_sk")

    return handle


def clickstream_batch(cfg, run_id: str):
    target = cfg.table("silver", "clickstream")

    def handle(batch_df, batch_id: int) -> None:
        spark = batch_df.sparkSession
        annotated = annotate(parse_clickstream(batch_df), rules.CLICKSTREAM_RULES)
        record_quality(spark, cfg, annotated, rules.CLICKSTREAM_RULES, table_ref="silver.clickstream", run_id=run_id,
                       key_col="event_id")
        events = valid_rows(annotated).withColumn("event_date", F.to_date("event_ts"))
        insert_new_only(spark, target, conform(events, spec("silver.clickstream")), keys=["event_id"],
                        prune_col="event_date")

    return handle


def _merge_stream(ctx: TaskContext, source_ref: str, name: str, handler, targets: list[str]) -> dict:
    cfg = ctx.cfg
    if cfg.full_refresh:
        reset_stream(cfg, name)
        for t in targets:
            delete_all(ctx.spark, ctx.table(t))
    stream = ctx.spark.readStream.table(ctx.table(source_ref))
    result = run_stream(stream, cfg, name=name, foreach_batch=handler(cfg, ctx.run_id))
    return {"micro_batches": result.batches, "rows": result.input_rows}


def orders(ctx: TaskContext) -> dict:
    return _merge_stream(ctx, "bronze.orders_raw", "silver_orders", orders_batch, ["silver.orders", "silver.order_items"])


def customers(ctx: TaskContext) -> dict:
    return _merge_stream(ctx, "bronze.customers_cdc_raw", "silver_customers", customers_batch, ["silver.customers"])


def clickstream(ctx: TaskContext) -> dict:
    return _merge_stream(ctx, "bronze.clickstream_raw", "silver_clickstream", clickstream_batch, ["silver.clickstream"])


# ---------------------------------------------------------------- reference snapshots

def _parse_events(df):
    return df.select(_payload(sources.EVENTS_RAW), _clean("event_id").alias("event_id"), _clean("artist").alias("artist"),
                     _clean("genre").alias("genre"), _try_cast("event_date", "DATE").alias("event_date"),
                     _clean("venue_id").alias("venue_id"), _try_cast("base_price", "DECIMAL(10,2)").alias("base_price"),
                     "_snapshot_date", "_source_file")


def _parse_venues(df):
    return df.select(_payload(sources.VENUES_RAW), _clean("venue_id").alias("venue_id"),
                     _clean("venue_name").alias("venue_name"), _clean("city").alias("city"),
                     F.upper(_clean("country")).alias("country"), _try_cast("capacity", "INT").alias("capacity"),
                     "_snapshot_date", "_source_file")


def reference(ctx: TaskContext) -> dict:
    """Rebuild shows and venues from their latest full snapshot (a snapshot replaces, it does not merge)."""
    out = {}
    for name, key, parse, rule_set in (("events", "event_id", _parse_events, rules.EVENT_RULES),
                                       ("venues", "venue_id", _parse_venues, rules.VENUE_RULES)):
        raw = (ctx.spark.read.table(ctx.table(f"bronze.{name}_raw"))
               .withColumn("_snapshot_date", F.to_date(F.regexp_extract("_source_file", r"(\d{8})\.csv$", 1), "yyyyMMdd")))
        latest = raw.agg(F.max("_snapshot_date")).collect()[0][0]
        if latest is None:
            out[name] = 0
            continue
        annotated = annotate(parse(raw.filter(F.col("_snapshot_date") == F.lit(latest))), rule_set)
        record_quality(ctx.spark, ctx.cfg, annotated, rule_set, table_ref=f"silver.{name}", run_id=ctx.run_id, key_col=key)
        rows = valid_rows(annotated).dropDuplicates([key])
        overwrite(conform(rows, spec(f"silver.{name}")), ctx.table(f"silver.{name}"))
        out[name] = latest.isoformat()
    return out
