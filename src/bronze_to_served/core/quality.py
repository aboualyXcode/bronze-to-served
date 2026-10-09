"""Data quality: expectations, quarantine, quality metrics and a quality gate.

An Expectation is a SQL condition that a valid row satisfies. A row passes only when the condition is
TRUE: a NULL result counts as a failure (unlike a CHECK constraint, where NULL passes).

    drop   failing rows go to ops.quarantine with the names of the rules they broke; the rest continue
    warn   failing rows continue, and are counted in ops.quality_metrics
    fail   any failing row stops the task (for contracts that must never be broken)

The Spark functions import pyspark lazily so the pure parts (rules, the gate) are testable without Spark.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

QUARANTINED = "__quarantined__"   # the metric row that counts quarantined rows per batch
ACTIONS = ("drop", "warn", "fail")
METRICS_SCHEMA = ("run_id string, source_table string, rule string, action string, failed_rows bigint, "
                  "total_rows bigint, measured_at timestamp")


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class Expectation:
    name: str
    condition: str
    action: str = "drop"
    description: str = ""

    def __post_init__(self) -> None:
        if self.action not in ACTIONS:
            raise ValueError(f"{self.name}: action must be one of {ACTIONS}")
        if not self.name.isidentifier():
            raise ValueError(f"rule names must be identifiers, got {self.name!r}")


def evaluate_gate(metrics: list[dict], thresholds: dict[str, float]) -> list[str]:
    """Return a violation message for every table whose quarantine rate exceeds its threshold.

    `metrics` are ops.quality_metrics rows; thresholds map a table ref (or '*') to a maximum rate.
    """
    totals: dict[str, int] = defaultdict(int)
    bad: dict[str, int] = defaultdict(int)
    for m in metrics:
        if m["rule"] == QUARANTINED:
            totals[m["source_table"]] += int(m["total_rows"])
            bad[m["source_table"]] += int(m["failed_rows"])
    violations = []
    for table in sorted(totals):
        total = totals[table]
        rate = bad[table] / total if total else 0.0
        limit = thresholds.get(table, thresholds.get("*", 1.0))
        if rate > limit:
            violations.append(f"{table}: {bad[table]} of {total} rows quarantined ({rate:.2%}, limit {limit:.2%})")
    return violations


# ---------------------------------------------------------------- Spark side

def _passes(rule: Expectation):
    from pyspark.sql import functions as F
    return F.coalesce(F.expr(rule.condition), F.lit(False))


def _failed_names(rules: list[Expectation]):
    from pyspark.sql import functions as F
    if not rules:
        return F.array().cast("array<string>")
    return F.filter(F.array(*[F.when(~_passes(r), F.lit(r.name)) for r in rules]), lambda name: name.isNotNull())


def annotate(df, rules: list[Expectation]):
    """Add `_failed_rules` (drop and fail rules) and `_warnings` (warn rules) array columns."""
    blocking = [r for r in rules if r.action in ("drop", "fail")]
    warnings = [r for r in rules if r.action == "warn"]
    return df.withColumn("_failed_rules", _failed_names(blocking)).withColumn("_warnings", _failed_names(warnings))


def valid_rows(annotated):
    from pyspark.sql import functions as F
    return annotated.filter(F.size("_failed_rules") == 0).drop("_failed_rules", "_warnings")


def measure(annotated, rules: list[Expectation]) -> dict[str, int]:
    """Total rows, quarantined rows and failures per rule, in one pass."""
    from pyspark.sql import functions as F
    exprs = [F.count(F.lit(1)).alias("_total"),
             F.sum(F.when(F.size("_failed_rules") > 0, 1).otherwise(0)).alias("_quarantined")]
    for r in rules:
        column = "_warnings" if r.action == "warn" else "_failed_rules"
        exprs.append(F.sum(F.when(F.array_contains(column, r.name), 1).otherwise(0)).alias(r.name))
    row = annotated.agg(*exprs).collect()[0].asDict()
    return {k: int(v or 0) for k, v in row.items()}


def record_quality(spark, cfg, annotated, rules: list[Expectation], *, table_ref: str, run_id: str,
                   key_col: str) -> dict[str, int]:
    """Write metrics and quarantined rows for one batch; raise if a `fail` rule was broken.

    `annotated` must carry `_payload` (the raw record as JSON) and `_source_file` columns.
    """
    from pyspark.sql import functions as F
    counts = measure(annotated, rules)
    if counts["_total"] == 0:
        return counts
    now = datetime.now(timezone.utc)
    rows = [(run_id, table_ref, r.name, r.action, counts[r.name], counts["_total"], now) for r in rules]
    rows.append((run_id, table_ref, QUARANTINED, "drop", counts["_quarantined"], counts["_total"], now))
    (spark.createDataFrame(rows, METRICS_SCHEMA).write.format("delta").mode("append")
     .saveAsTable(cfg.table("ops", "quality_metrics")))
    if counts["_quarantined"]:
        (annotated.filter(F.size("_failed_rules") > 0)
         .select(F.lit(table_ref).alias("source_table"), F.col(key_col).cast("string").alias("record_key"),
                 F.col("_failed_rules").alias("failed_rules"), F.col("_payload").alias("payload"),
                 F.col("_source_file").alias("source_file"), F.lit(run_id).alias("run_id"),
                 F.current_timestamp().alias("quarantined_at"))
         .write.format("delta").mode("append").saveAsTable(cfg.table("ops", "quarantine")))
    broken = [r.name for r in rules if r.action == "fail" and counts[r.name]]
    if broken:
        raise DataQualityError(f"{table_ref}: rows broke rules that must never fail: {broken}")
    return counts
