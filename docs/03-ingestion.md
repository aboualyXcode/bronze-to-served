# 3. Ingestion

Bronze is the only layer that talks to the outside world, so it is built to never break: every field arrives as a string, nothing is validated, and anything unexpected is kept rather than rejected.

| Source | Arrives as | Ingested with | Why |
|---|---|---|---|
| Order events | daily JSON lines | Auto Loader stream | Many small files, forever; only new ones should be read |
| CRM changes (CDC) | daily JSON lines, LSN-ordered per customer | Auto Loader stream | Same, and order matters (handled in Silver) |
| Clickstream | daily JSON lines, or a Kafka topic | Auto Loader stream, or the Kafka source | High volume; can also be truly streaming |
| Shows and venues | weekly and quarterly CSV snapshots | `COPY INTO` | A few files, batch by nature, must load exactly once |

## Auto Loader

`core/ingest.py:read_files_stream` configures `cloudFiles` the way production pipelines need it:

```python
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", cfg.checkpoint_path(f"{name}/_schema"))
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .option("pathGlobFilter", "*.json")
    .schema(string_schema(ORDERS_RAW))
```

- **An explicit schema of strings** is the Bronze contract. Nothing is coerced at the edge, so a malformed value can never fail ingestion; Silver types it with `try_cast` and quarantines what does not parse.
- **Rescue mode** never fails the stream on new upstream fields: they land in `_rescued_data` as JSON. The generator adds a `seat_section` field halfway through the timeline, so you can watch it happen.
- **`_metadata.file_path` and `file_modification_time`** give every row its provenance (do not use `input_file_name()`, which Unity Catalog standard access mode does not support).
- **`pathGlobFilter`** skips half-written files (the generator writes `*.tmp` and renames).
- **The checkpoint** records which files were processed, so each run reads only new files, and a failed run resumes where it stopped.
- **`trigger(availableNow=True)`** processes everything new and stops: scheduled, cheap, and supported on serverless ([chapter 5](05-streaming.md)). For directories with millions of files, Auto Loader's file notification mode avoids listing the directory on every run.

## COPY INTO

Reference snapshots are small and batch, and loading one twice would duplicate a snapshot. `COPY INTO` remembers which files it loaded, so re-running it is harmless:

```sql
COPY INTO bronze.venues_raw
FROM (SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
      FROM '/Volumes/stagedoor_dev/landing/raw/reference')
FILEFORMAT = CSV
PATTERN = 'venues_*.csv'
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
COPY_OPTIONS ('mergeSchema' = 'false', 'force' = 'false')
```

A full refresh sets `force = true` after emptying the table, so the files load again.

## Kafka

`bronze.clickstream` reads Kafka with `source=kafka` and lands rows in **the same Bronze table** with the same columns: `_source_file` becomes `kafka://topic/partition/offset` and an unparseable message is kept whole in `_rescued_data`. Everything downstream is unchanged. Credentials come from a secret scope (`kafka_secret_scope`), never from job parameters; the same code works against Event Hubs and MSK through their Kafka endpoints.

## Change data capture files

The CRM export follows a contract that most CDC tools keep (and that `tests/unit/test_datagen.py` checks on the generated data): every change has a log sequence number (LSN); changes to one customer never arrive in a file earlier than a change with a lower LSN; re-deliveries are exact copies. Inside a file, order is arbitrary, which is why Silver orders by LSN and never by file position ([chapter 4](04-medallion.md)).

## When upstream changes its schema

1. A new field appears; Auto Loader rescues it and nothing breaks.
2. `notebooks/01_bronze_silver_explorer.py` shows how many rows carry rescued fields, with an example.
3. You decide: add the field to the Bronze and Silver contracts (`sources.py`, `tables.py`), or ignore it.
4. Backfill Silver from `get_json_object(_rescued_data, '$.seat_section')`: nothing was lost while the contract lagged behind.

A removed or renamed field shows up as NULLs in Silver, and the quarantine catches it if a rule needs the field.

**Practise:** land two more days (`land.sample_data` with `mode=next_day`), run `bronze.orders` twice, and compare `numInputRows` in `ops.task_runs`: the second run reads nothing.
