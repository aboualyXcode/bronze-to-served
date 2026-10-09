# 4. The medallion

Each layer has one job. Silver makes the data trustworthy: typed, valid, and correct no matter how it arrived. Gold makes it useful. This chapter covers the patterns that make both hold under late, duplicated and out-of-order data.

## Silver: parse, validate, merge

Every Silver stream reads its Bronze table incrementally and handles each micro-batch in `foreachBatch`:

1. **Parse** strings into types with `try_cast` (ANSI mode is on by default on serverless and in Spark 4, where a plain `CAST` of a bad value fails the whole batch).
2. **Validate** with expectations (`stagedoor/rules.py`); rows that break a `drop` rule go to `ops.quarantine` with the rule names and the raw record, and every rule's counts go to `ops.quality_metrics`.
3. **Merge** the valid rows with a pattern chosen for the entity.

### Orders: latest state, whatever the arrival order (SCD1)

An order emits PLACED, then PAID or CANCELLED, and maybe REFUNDED weeks later; any of these can arrive a day late, or twice. `core/merge.py:scd1_merge` makes the result independent of arrival order:

| Column kind | Rule | Example |
|---|---|---|
| Regular columns | taken from the event with the highest `updated_at` | `status` |
| First-seen columns | `least(old, new)`: the earliest non-null value | `paid_at`, `refunded_at` |
| Last-seen columns | `greatest(old, new)` | `_last_ingested_at` |

Because each rule is commutative, applying batches in any order gives the same table (`tests/spark/test_merge_patterns.py` applies them backwards). The MERGE also skips exact replays, so a retried micro-batch rewrites nothing. Keeping the *time* of each status change, not just the latest status, is what lets the features be point-in-time correct ([chapter 7](07-ml-lifecycle.md)).

Order lines are immutable, so they are inserted exactly once with an insert-only MERGE.

### Customers: full history from CDC (SCD2)

`core/merge.py:scd2_merge` applies a batch of change events, possibly several per customer and in any order, with one atomic MERGE:

1. Read the current state of the keys in the batch: the last applied LSN and the hash of the current version.
2. Drop events at or below the last applied LSN: replays and stale events change nothing.
3. Hash the tracked attributes of each event; a DELETE has no hash.
4. Order events by LSN; compare each event's hash with the state just before it (the previous event, or the current version for the first). Only a different hash creates a version, so a CRM "touch" that changes nothing does not.
5. Each new version ends at the next event that changes state; the first such event also closes the version currently in the table.
6. MERGE: close current versions (matched on the key) and insert new versions (a NULL merge key never matches).

The guarantees: one current version at most per customer; versions never overlap; a delete closes the current version and a later insert starts a new one; `customer_sk = sha256(customer_id|lsn)` is deterministic, so a rebuild produces the same keys. The declarative variant does the same with AUTO CDC in a few lines ([chapter 6](06-declarative-pipelines.md)).

### Clickstream: exactly once

Events are delivered at least once. `insert_new_only` inserts rows whose `event_id` is not in the table yet, after removing duplicates within the batch, and limits the MERGE to the dates in the batch so it reads a few files, not the table. Unlike streaming deduplication with a watermark, it never forgets: a duplicate that arrives a week late is still caught.

### Shows and venues: snapshots replace

A snapshot is the whole truth on its date, so Silver rebuilds from the latest one. A show missing from the new snapshot is gone, which is what a snapshot means.

## Quarantine and the quality gate

| Action | On failure | Use for |
|---|---|---|
| `drop` | the row goes to `ops.quarantine`, the rest continue | anything that makes a row unusable |
| `warn` | counted in `ops.quality_metrics`, the row continues | suspicious but usable (an unknown sales channel) |
| `fail` | the task fails | contracts that must never break |

A condition passes only when it is TRUE: a NULL counts as a failure, so `quantity > 0` rejects a missing quantity. Quarantined rows keep the raw record (`payload`) and source file, so they can be fixed and replayed.

Quarantining a few rows is normal; quarantining many means something upstream broke. The `quality.gate` task reads this run's metrics and fails the job before Gold if any table quarantined more than its threshold (2% of orders, 1% of CRM changes by default), so dashboards keep yesterday's correct numbers instead of today's broken ones.

## Gold: a star schema with time in it

- **`dim_customer`** keeps the full SCD2 history. **`fct_sales`** (order-line grain) carries the `customer_sk` of the version in effect **when the order was paid**, found with a range join on `valid_from <= paid_at < valid_to`. Revenue by loyalty tier then reports the tier customers had when they bought, not the tier they have now.
- **Revenue is booked on the payment date and refunds on the refund date** in `agg_daily_sales`. A refund then never changes a past day, so a past day is final once its late data is in.
- **Incremental recompute from a high-water mark**: daily aggregates rebuild only from the latest date already in the table minus `lookback_days` (3), with `replaceWhere`. That absorbs late data, catches up after missed runs, and leaves older history alone. `full_refresh=true` rebuilds everything.
- **Data dates, not the wall clock**: `customer_360.as_of_date` is the latest date in the data, so rebuilding last month's tables reproduces last month's numbers.

## What happens when data is late

| Arrives late | What happens |
|---|---|
| An order's PAID event, a day late | SCD1 MERGE fills `paid_at` with the earliest value; the order's status is unchanged if a newer event exists |
| A refund, weeks later | `refunded_at` is set; revenue for the original day is untouched; the refund lands on its own day |
| A duplicate click, any time | ignored: its `event_id` is already in the table |
| A CRM change below the last applied LSN | ignored (by contract it is a re-delivery) |
| Several days at once, after an outage | every stream reads all new files; Gold recomputes from its high-water mark |
| Data older than the lookback window | Gold misses it until a full refresh; widen `lookback_days` if your sources are that late |

**Practise:** run the daily job twice without new data and compare the Silver and Gold tables: `tests/spark/test_incremental.py` checks that nothing changes, and that four daily runs equal one run over the same days.
