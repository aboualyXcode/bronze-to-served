"""Operations: the quality gate, table maintenance and a change-data-feed export (reverse ETL)."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from ..core.delta_ops import analyze, latest_version, optimize, read_changes, vacuum
from ..core.quality import DataQualityError, evaluate_gate
from ..tasks.context import TaskContext

DEFAULT_THRESHOLDS = {"silver.orders": 0.02, "silver.customers": 0.01, "silver.clickstream": 0.02,
                      "silver.events": 0.0, "silver.venues": 0.0}


def quality_gate(ctx: TaskContext) -> dict:
    """Stop the run before Gold if this run quarantined too much of any table (a circuit breaker).

    Override limits with thresholds='{"silver.orders": 0.05}'.
    """
    rows = [r.asDict() for r in ctx.spark.read.table(ctx.table("ops.quality_metrics"))
            .where(F.col("run_id") == ctx.run_id).collect()]
    thresholds = {**DEFAULT_THRESHOLDS, **json.loads(ctx.param("thresholds", "{}"))}
    violations = evaluate_gate(rows, thresholds)
    if violations:
        raise DataQualityError("quality gate failed: " + "; ".join(violations))
    return {"tables_checked": len({r["source_table"] for r in rows}), "violations": 0}


def maintain_table(ctx: TaskContext) -> dict:
    """OPTIMIZE, VACUUM and ANALYZE one table (run it for each table with a for-each task).

    Unity Catalog managed tables with predictive optimization enabled do this automatically; keep this for
    external tables or workspaces without it.
    """
    table = ctx.table(ctx.param("table"))
    done = []
    for op in [o.strip() for o in ctx.param("operations", "optimize,vacuum,analyze").split(",") if o.strip()]:
        if op == "optimize":
            optimize(ctx.spark, table)
        elif op == "vacuum":
            vacuum(ctx.spark, table, ctx.int_param("retain_hours", 168))
        elif op == "analyze":
            if ctx.cfg.is_local:
                continue
            analyze(ctx.spark, table)
        else:
            raise ValueError(f"unknown maintenance operation {op!r}")
        done.append(op)
    return {"table": table, "operations": done}


def crm_export(ctx: TaskContext) -> dict:
    """Send newly scored high-risk customers to the CRM: read only what changed since the last export
    (change data feed), write a JSON file to the exports volume, then move the bookmark."""
    spark, cfg = ctx.spark, ctx.cfg
    name, source = ctx.param("export_name", "crm_high_risk"), ctx.table("ml.churn_predictions")
    bookmarks = ctx.table("ops.export_bookmarks")
    last = spark.read.table(bookmarks).where(F.col("export_name") == name).agg(F.max("last_version")).collect()[0][0]
    current = latest_version(spark, source)
    start = 0 if last is None else int(last) + 1
    if start > current:
        return {"exported": 0, "reason": "no new versions"}
    changes = (read_changes(spark, source, start, current)
               .where(F.col("_change_type").isin("insert", "update_postimage"))
               .where(F.col("risk_band") == ctx.param("risk_band", "high"))
               .select("customer_id", "as_of_date", "churn_probability", "risk_band", "model_version", "_commit_version"))
    count = changes.count()
    if count:
        folder = cfg.export_path("crm", re.sub(r"[^A-Za-z0-9_-]", "_", f"run_{ctx.run_id}"))
        changes.coalesce(1).write.mode("overwrite").json(folder)
    mark = spark.createDataFrame([(name, source, current, datetime.now(timezone.utc))],
                                 "export_name string, table_name string, last_version bigint, updated_at timestamp")
    (DeltaTable.forName(spark, bookmarks).alias("t").merge(mark.alias("s"), "t.export_name = s.export_name")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    return {"exported": count, "versions": f"{start}..{current}"}
