"""Stagedoor's clickstream and CRM CDC as a Lakeflow Declarative Pipeline (formerly Delta Live Tables).

The same medallion as the PySpark jobs, declared instead of orchestrated: the pipeline infers the
dependency graph, manages checkpoints and retries, records expectation metrics in its event log, and
implements SCD Type 2 with AUTO CDC (apply_changes) in a few lines. It publishes to the
`<prefix>declarative` schema, side by side with the job-based tables, so the two can be compared.

Uses the long-standing `dlt` module. With the newer API, `from pyspark import pipelines as dp` and use
dp.table / dp.materialized_view / dp.create_auto_cdc_flow with the same arguments.
"""
import dlt
from pyspark.sql import functions as F

LANDING = spark.conf.get("stagedoor.landing_path")

CLICK_RULES = {
    "event_id_present": "event_id IS NOT NULL AND event_id <> ''",
    "session_present": "session_id IS NOT NULL AND session_id <> ''",
    "valid_event_ts": "event_ts IS NOT NULL",
    "known_event_type": "event_type IN ('page_view', 'search', 'view_event', 'add_to_cart', 'checkout', 'purchase')",
}
CDC_RULES = {
    "customer_id_present": "customer_id IS NOT NULL AND customer_id <> ''",
    "valid_lsn": "lsn IS NOT NULL",
    "known_op": "op IN ('INSERT', 'UPDATE', 'DELETE')",
    "valid_changed_at": "changed_at IS NOT NULL",
}


def autoloader(source: str):
    """All columns as strings (inferColumnTypes off); unexpected fields go to _rescued_data."""
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("pathGlobFilter", "*.json")
            .load(f"{LANDING}/{source}")
            .select("*", F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingested_at")))


def try_cast(column: str, sql_type: str):
    return F.expr(f"try_cast(trim({column}) AS {sql_type})")


# ------------------------------------------------------------------ clickstream: bronze -> silver -> gold

@dlt.table(comment="Raw web and app events, as delivered.", table_properties={"quality": "bronze"})
def clickstream_bronze():
    return autoloader("clickstream")


@dlt.view(comment="Typed clickstream events, before validation.")
def clickstream_parsed():
    return spark.readStream.table("clickstream_bronze").select(
        F.trim("event_id").alias("event_id"), F.trim("session_id").alias("session_id"),
        F.trim("customer_id").alias("customer_id"), F.lower(F.trim("event_type")).alias("event_type"),
        F.trim("event_ref").alias("event_ref"), F.lower(F.trim("device")).alias("device"),
        try_cast("event_ts", "TIMESTAMP").alias("event_ts"), "properties", "_source_file", "_ingested_at")


@dlt.table(comment="Valid events, deduplicated within a 2-hour watermark.", table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(CLICK_RULES)
def clickstream_silver():
    return (spark.readStream.table("clickstream_parsed")
            .withColumn("event_date", F.to_date("event_ts"))
            .withWatermark("event_ts", "2 hours")
            .dropDuplicatesWithinWatermark(["event_id"]))


@dlt.table(comment="Events that broke a rule, kept for triage.", table_properties={"quality": "quarantine"})
def clickstream_quarantine():
    broken = " OR ".join(f"NOT coalesce({rule}, false)" for rule in CLICK_RULES.values())
    return spark.readStream.table("clickstream_parsed").where(broken)


@dlt.table(comment="Daily engagement: a materialized view, refreshed incrementally.", table_properties={"quality": "gold"})
def engagement_daily():
    return (spark.read.table("clickstream_silver")
            .groupBy("event_date", "event_type")
            .agg(F.count(F.lit(1)).alias("events"), F.countDistinct("session_id").alias("sessions"),
                 F.countDistinct("customer_id").alias("customers")))


# ------------------------------------------------------------------ CRM CDC: bronze -> SCD2 with AUTO CDC

@dlt.table(comment="Raw CRM change events, as delivered.", table_properties={"quality": "bronze"})
def customers_cdc_bronze():
    return autoloader("customers_cdc")


@dlt.view(comment="Typed, valid change events.")
@dlt.expect_all_or_drop(CDC_RULES)
def customers_cdc_clean():
    return spark.readStream.table("customers_cdc_bronze").select(
        F.trim("customer_id").alias("customer_id"), try_cast("lsn", "BIGINT").alias("lsn"),
        F.upper(F.trim("op")).alias("op"), try_cast("changed_at", "TIMESTAMP").alias("changed_at"),
        F.trim("email").alias("email"), F.trim("full_name").alias("full_name"),
        F.upper(F.trim("country")).alias("country"), F.trim("city").alias("city"),
        F.lower(F.trim("loyalty_tier")).alias("loyalty_tier"),
        try_cast("marketing_opt_in", "BOOLEAN").alias("marketing_opt_in"),
        try_cast("signup_date", "DATE").alias("signup_date"))


dlt.create_streaming_table("customers_scd2", comment="Customer history (SCD Type 2) maintained by AUTO CDC.",
                           table_properties={"quality": "silver"})

dlt.apply_changes(
    target="customers_scd2",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by=F.col("lsn"),                       # out-of-order events are applied in LSN order
    apply_as_deletes=F.expr("op = 'DELETE'"),       # a delete closes the current version
    except_column_list=["op", "lsn"],
    stored_as_scd_type=2,                           # adds __START_AT / __END_AT
)
