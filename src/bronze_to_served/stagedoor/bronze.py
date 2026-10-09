"""Bronze: land every raw record, unchanged, with ingestion metadata. Streams never fail on bad data here."""
from __future__ import annotations

from ..core.contracts import conform
from ..core.delta_ops import delete_all
from ..core.ingest import copy_into, read_files_stream, read_kafka_stream, string_schema
from ..core.streams import reset_stream, run_stream
from ..tasks.context import TaskContext
from . import sources
from .tables import spec


def _ingest(ctx: TaskContext, table_ref: str, df_factory) -> dict:
    cfg, name, target = ctx.cfg, table_ref.replace(".", "_"), ctx.table(table_ref)
    if cfg.full_refresh:          # reprocess every file: forget progress, empty the table
        reset_stream(cfg, name)
        delete_all(ctx.spark, target)
    result = run_stream(conform(df_factory(name), spec(table_ref)), cfg, name=name, table=target)
    return {"micro_batches": result.batches, "rows": result.input_rows}


def _files(ctx: TaskContext, source: str, raw: tuple[str, ...]):
    return lambda name: read_files_stream(ctx.spark, ctx.cfg, source=source, fmt="json", schema=string_schema(raw),
                                          stream_name=name)


def orders(ctx: TaskContext) -> dict:
    return _ingest(ctx, "bronze.orders_raw", _files(ctx, "orders", sources.ORDERS_RAW))


def customers_cdc(ctx: TaskContext) -> dict:
    return _ingest(ctx, "bronze.customers_cdc_raw", _files(ctx, "customers_cdc", sources.CUSTOMERS_CDC_RAW))


def clickstream(ctx: TaskContext) -> dict:
    """Files by default; `source=kafka` reads a topic instead (same Bronze table, same downstream)."""
    if ctx.param("source", "files") != "kafka":
        return _ingest(ctx, "bronze.clickstream_raw", _files(ctx, "clickstream", sources.CLICKSTREAM_RAW))
    options = {}
    scope = ctx.param("kafka_secret_scope")
    if scope:
        from databricks.sdk.runtime import dbutils
        user, password = dbutils.secrets.get(scope, "kafka-username"), dbutils.secrets.get(scope, "kafka-password")
        options = {"kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "PLAIN",
                   "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
                                             f'required username="{user}" password="{password}";'}
    return _ingest(ctx, "bronze.clickstream_raw", lambda name: read_kafka_stream(
        ctx.spark, bootstrap_servers=ctx.param("kafka_bootstrap_servers"), topic=ctx.param("kafka_topic", "clickstream"),
        schema=string_schema(sources.CLICKSTREAM_RAW), options=options))


def reference(ctx: TaskContext) -> dict:
    """Weekly and quarterly CSV snapshots: small, batch, idempotent, so COPY INTO rather than a stream."""
    out = {}
    for name, raw in (("events", sources.EVENTS_RAW), ("venues", sources.VENUES_RAW)):
        target = ctx.table(f"bronze.{name}_raw")
        if ctx.cfg.full_refresh:
            delete_all(ctx.spark, target)
        out[f"{name}_rows"] = copy_into(ctx.spark, ctx.cfg, table=target, source="reference", pattern=f"{name}_*.csv",
                                        schema=string_schema(raw), force=ctx.cfg.full_refresh)
    return out
