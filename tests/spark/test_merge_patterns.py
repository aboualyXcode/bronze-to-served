"""The core MERGE patterns on small, hand-made batches: late, duplicated and out-of-order data."""
from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from bronze_to_served.core.merge import insert_new_only, scd1_merge, scd2_merge  # noqa: E402
from bronze_to_served.core.quality import Expectation, annotate, measure  # noqa: E402

TS = datetime(2025, 3, 1, 12, 0, 0)


def _table(spark, name, ddl):
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(f"CREATE TABLE {name} ({ddl}) USING DELTA")
    return name


def _scd2_target(spark, name):
    return _table(spark, name, "customer_sk STRING, customer_id STRING, tier STRING, valid_from TIMESTAMP, "
                               "valid_to TIMESTAMP, is_current BOOLEAN, _start_seq BIGINT, _end_seq BIGINT, _row_hash STRING")


def _changes(spark, events):
    rows = [(cid, seq, datetime(2025, 3, 1, 0, 0, seq), op, tier) for cid, seq, op, tier in events]
    return spark.createDataFrame(rows, "customer_id string, lsn bigint, changed_at timestamp, op string, tier string")


def _apply(spark, target, events):
    scd2_merge(spark, target, _changes(spark, events), key="customer_id", sequence_col="lsn", timestamp_col="changed_at",
               op_col="op", tracked=["tier"], sk_col="customer_sk")


def _history(spark, target):
    return [(r.customer_id, r.tier, r._start_seq, r._end_seq, r.is_current)
            for r in spark.read.table(target).orderBy("customer_id", "_start_seq").collect()]


def test_scd2_versions_deletes_and_no_ops(spark):
    t = _scd2_target(spark, "scd2_basic")
    _apply(spark, t, [("A", 1, "INSERT", "silver"), ("B", 2, "INSERT", "gold")])
    _apply(spark, t, [("A", 3, "UPDATE", "silver"),          # no-op: same attributes, no new version
                      ("A", 5, "UPDATE", "gold"), ("A", 4, "UPDATE", "platinum"),   # out of order in the batch
                      ("B", 6, "DELETE", None), ("B", 7, "INSERT", "standard")])     # delete, then re-insert
    assert _history(spark, t) == [
        ("A", "silver", 1, 4, False), ("A", "platinum", 4, 5, False), ("A", "gold", 5, None, True),
        ("B", "gold", 2, 6, False), ("B", "standard", 7, None, True)]
    version_rows = spark.read.table(t).where("customer_id = 'A' AND tier = 'platinum'").first()
    assert version_rows.valid_to == datetime(2025, 3, 1, 0, 0, 5)


def test_scd2_replays_and_stale_events_change_nothing(spark):
    t = _scd2_target(spark, "scd2_replay")
    batch = [("A", 1, "INSERT", "silver"), ("A", 2, "UPDATE", "gold")]
    _apply(spark, t, batch)
    before = _history(spark, t)
    _apply(spark, t, batch)                                   # the same batch again (a retried micro-batch)
    _apply(spark, t, [("A", 1, "UPDATE", "platinum")])        # stale: below the last applied LSN
    assert _history(spark, t) == before


def test_scd2_delete_of_an_unknown_customer_is_ignored(spark):
    t = _scd2_target(spark, "scd2_unknown")
    _apply(spark, t, [("Z", 1, "DELETE", None)])
    assert _history(spark, t) == []


def _orders_target(spark, name):
    return _table(spark, name, "order_id STRING, status STRING, updated_at TIMESTAMP, paid_at TIMESTAMP")


def _order_events(spark, events):
    return spark.createDataFrame([(o, s, datetime(2025, 3, 1, 0, m), datetime(2025, 3, 1, 0, p) if p else None)
                                  for o, s, m, p in events], "order_id string, status string, updated_at timestamp, paid_at timestamp")


def test_scd1_merge_is_order_independent(spark):
    placed, paid, refunded = ("O1", "PLACED", 1, None), ("O1", "PAID", 2, 2), ("O1", "REFUNDED", 9, None)
    results = []
    for name, batches in (("scd1_a", [[placed], [paid], [refunded]]), ("scd1_b", [[refunded], [paid], [placed]])):
        t = _orders_target(spark, name)
        for batch in batches:
            scd1_merge(spark, t, _order_events(spark, batch), keys=["order_id"], sequence_col="updated_at",
                       first_seen=("paid_at",))
        results.append(spark.read.table(t).collect())
    assert results[0] == results[1]
    row = results[0][0]
    assert row.status == "REFUNDED" and row.paid_at == datetime(2025, 3, 1, 0, 2)


def test_insert_new_only_is_exactly_once(spark):
    t = _table(spark, "events_once", "event_id STRING, event_date DATE")
    df = spark.createDataFrame([("e1", "2025-03-01"), ("e1", "2025-03-01"), ("e2", "2025-03-02")], "event_id string, d string") \
        .select("event_id", F.to_date("d").alias("event_date"))
    insert_new_only(spark, t, df, keys=["event_id"], prune_col="event_date")
    insert_new_only(spark, t, df, keys=["event_id"], prune_col="event_date")
    assert sorted(r.event_id for r in spark.read.table(t).collect()) == ["e1", "e2"]


def test_expectations_count_failures_and_treat_null_as_failure(spark):
    df = spark.createDataFrame([(1, "a"), (None, "b"), (-1, None)], "q int, c string")
    rules = [Expectation("positive_q", "q > 0"), Expectation("c_present", "c IS NOT NULL", action="warn")]
    counts = measure(annotate(df, rules), rules)
    assert counts == {"_total": 3, "_quarantined": 2, "positive_q": 2, "c_present": 1}
