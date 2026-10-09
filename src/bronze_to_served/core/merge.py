"""MERGE patterns that stay correct when data arrives late, twice or out of order.

scd1_merge       keep the latest version of each key. Order-independent: regular columns come from the
                 row with the highest sequence, 'first seen' columns keep the earliest non-null value and
                 'last seen' columns the latest, so replaying batches in any order gives the same table.
scd2_merge       full history from change data capture: one row per version with valid_from / valid_to,
                 deletes close the current version, no-op changes do not create versions, and events at
                 or below the last applied sequence are ignored (replays are harmless).
insert_new_only  exactly-once appends: insert rows whose key is not already in the table.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def _on(keys: list[str]) -> str:
    return " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)


def latest_per_key(df: DataFrame, keys: list[str], order_by: list) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(*order_by)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def row_hash(columns: list[str]):
    """A change-detection hash over the tracked columns (NULL and '' hash differently)."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]), 256)


def scd1_merge(spark, target: str, source: DataFrame, *, keys: list[str], sequence_col: str,
               first_seen: tuple[str, ...] = (), last_seen: tuple[str, ...] = ()) -> None:
    """Upsert one row per key into `target`; `source` must already have one row per key."""
    special = set(keys) | {sequence_col} | set(first_seen) | set(last_seen)
    regular = [c for c in source.columns if c not in special]
    newer = f"s.`{sequence_col}` > t.`{sequence_col}`"
    updates = {c: f"CASE WHEN {newer} THEN s.`{c}` ELSE t.`{c}` END" for c in regular}
    updates[sequence_col] = f"greatest(t.`{sequence_col}`, s.`{sequence_col}`)"
    changed = [newer]
    for c in first_seen:
        updates[c] = f"least(t.`{c}`, s.`{c}`)"          # least/greatest skip NULLs
        changed.append(f"(s.`{c}` IS NOT NULL AND (t.`{c}` IS NULL OR s.`{c}` < t.`{c}`))")
    for c in last_seen:
        updates[c] = f"greatest(t.`{c}`, s.`{c}`)"
        changed.append(f"(s.`{c}` IS NOT NULL AND (t.`{c}` IS NULL OR s.`{c}` > t.`{c}`))")
    (DeltaTable.forName(spark, target).alias("t")
     .merge(source.alias("s"), _on(keys))
     .whenMatchedUpdate(condition=" OR ".join(changed), set=updates)      # exact replays rewrite nothing
     .whenNotMatchedInsert(values={c: f"s.`{c}`" for c in source.columns})
     .execute())


def scd2_merge(spark, target: str, changes: DataFrame, *, key: str, sequence_col: str, timestamp_col: str,
               op_col: str, tracked: list[str], sk_col: str, delete_op: str = "DELETE") -> None:
    """Apply a batch of change events (any order, possibly several per key) as SCD Type 2 history.

    The target has: sk_col, key, *tracked, valid_from, valid_to, is_current, _start_seq, _end_seq, _row_hash.
    The batch is applied with a single atomic MERGE: it closes the current version of each changed key
    and inserts every new version, with versions created inside the batch already closed correctly.
    """
    state = (spark.read.table(target).join(changes.select(key).distinct(), key, "left_semi")
             .groupBy(key)
             .agg(F.max(F.greatest(F.col("_start_seq"), F.col("_end_seq"))).alias("_last_seq"),
                  F.max(F.when(F.col("is_current"), F.col("_row_hash"))).alias("_current_hash")))
    is_delete = F.col(op_col) == F.lit(delete_op)
    ch = (changes.dropDuplicates([key, sequence_col])
          .join(state, key, "left")
          .filter(F.col("_last_seq").isNull() | (F.col(sequence_col) > F.col("_last_seq")))
          .withColumn("_row_hash", F.when(is_delete, F.lit(None).cast("string")).otherwise(row_hash(tracked))))
    w = Window.partitionBy(key).orderBy(sequence_col)
    ch = (ch.withColumn("_rn", F.row_number().over(w))
          # the state just before this event: the target's current version, or the previous event
          .withColumn("_prev_hash", F.when(F.col("_rn") == 1, F.col("_current_hash"))
                      .otherwise(F.lag("_row_hash").over(w))))
    creates = ~is_delete & (F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_row_hash")))
    ch = (ch.withColumn("_creates", creates)
          .withColumn("_closes", F.col("_prev_hash").isNotNull() & (is_delete | F.col("_creates"))))
    following = w.rowsBetween(1, Window.unboundedFollowing)
    ch = (ch.withColumn("_next_close_ts", F.first(F.when(F.col("_closes"), F.col(timestamp_col)), ignorenulls=True)
                        .over(following))
          .withColumn("_next_close_seq", F.first(F.when(F.col("_closes"), F.col(sequence_col)), ignorenulls=True)
                      .over(following)))
    versions = ch.filter("_creates").select(
        F.lit(None).cast("string").alias("_merge_key"),
        F.sha2(F.concat_ws("|", F.col(key).cast("string"), F.col(sequence_col).cast("string")), 256).alias(sk_col),
        F.col(key), *[F.col(c) for c in tracked],
        F.col(timestamp_col).alias("valid_from"), F.col("_next_close_ts").alias("valid_to"),
        F.col("_next_close_ts").isNull().alias("is_current"),
        F.col(sequence_col).alias("_start_seq"), F.col("_next_close_seq").alias("_end_seq"), F.col("_row_hash"))
    closes = (ch.filter(F.col("_closes") & F.col("_current_hash").isNotNull())
              .withColumn("_crn", F.row_number().over(Window.partitionBy(key).orderBy(sequence_col)))
              .filter("_crn = 1")
              .select(F.col(key).cast("string").alias("_merge_key"), F.col(timestamp_col).alias("_close_ts"),
                      F.col(sequence_col).alias("_close_seq")))
    staged = versions.unionByName(closes, allowMissingColumns=True)
    insert_cols = [sk_col, key, *tracked, "valid_from", "valid_to", "is_current", "_start_seq", "_end_seq", "_row_hash"]
    (DeltaTable.forName(spark, target).alias("t")
     .merge(staged.alias("s"), f"t.`{key}` = s._merge_key AND t.is_current = true")
     .whenMatchedUpdate(set={"valid_to": "s._close_ts", "is_current": "false", "_end_seq": "s._close_seq"})
     .whenNotMatchedInsert(condition="s._merge_key IS NULL", values={c: f"s.`{c}`" for c in insert_cols})
     .execute())


def insert_new_only(spark, target: str, source: DataFrame, *, keys: list[str], prune_col: str | None = None) -> None:
    """Insert rows whose keys are not in `target` yet. `prune_col` (a date) limits the files MERGE reads."""
    source = source.dropDuplicates(keys)
    condition = _on(keys)
    if prune_col:
        lo, hi = source.agg(F.min(prune_col), F.max(prune_col)).collect()[0]
        if lo is None:
            return
        condition += f" AND t.`{prune_col}` BETWEEN DATE'{lo.isoformat()}' AND DATE'{hi.isoformat()}'"
    DeltaTable.forName(spark, target).alias("t").merge(source.alias("s"), condition).whenNotMatchedInsertAll().execute()
