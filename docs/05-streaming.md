# 5. Streaming

Most of the platform uses Structured Streaming, but most of it runs as scheduled jobs. Streaming here means *incremental*: each run processes exactly what is new, and the checkpoint remembers where it stopped. Latency is a separate choice.

## Two triggers, one code path

| Trigger | Behaviour | Runs on | Used by |
|---|---|---|---|
| `availableNow` | process everything new, then stop | serverless or classic; scheduled or file-arrival jobs | the daily job: every Bronze and Silver stream |
| `processingTime=30 seconds` | keep running; pick up new data every interval | classic compute, in a continuous job | `stagedoor_streaming`: clickstream with seconds of latency |

The trigger is configuration (`PlatformConfig.trigger`, set per task with the `trigger` parameter), so the same function runs both ways. Serverless compute runs streams with `availableNow`; for processing-time triggers use classic compute (the designer reports a serverless stream with a processing-time trigger as an error).

## Checkpoints

Each stream checkpoints to its own folder in the `ops.checkpoints` volume, named after the stream (`silver_orders`). The checkpoint holds the source offsets and, for Auto Loader, the list of processed files and the inferred schema. Three rules keep checkpoints healthy:

- **One writer per checkpoint.** Every job sets `max_concurrent_runs: 1`, and the continuous clickstream job must not run alongside the daily job's clickstream tasks; they share checkpoints.
- **Resetting is a decision.** `reset_stream` (used by `full_refresh=true`) deletes the folder and empties the target, so the stream reprocesses its whole source.
- **Checkpoints belong to their query.** Changing a stream's source or its stateful logic usually needs a new checkpoint.

## Idempotent micro-batches

A micro-batch can run twice: the job fails after the MERGE committed but before the checkpoint recorded it, and the retry replays the batch. Every Silver handler is written so a replay changes nothing:

| Stream | Why a replay is harmless |
|---|---|
| `silver_orders` | the MERGE condition skips rows identical to the target; `least`/`greatest` rules are idempotent |
| `silver_customers` | events at or below the last applied LSN are filtered out before the MERGE |
| `silver_clickstream` | insert-only on `event_id` |

Handlers use `batch_df.sparkSession`, never a session captured from outside, and avoid `cache()` and the RDD API, so they also run on serverless and standard access mode, which use Spark Connect.

## Stateful streaming: windows and watermarks

`gold.live_engagement` counts events in 5-minute windows:

```python
clicks.withWatermark("event_ts", "30 minutes")
      .groupBy(F.window("event_ts", "5 minutes"), "event_type")
      .agg(F.count(F.lit(1)).alias("events"), F.approx_count_distinct("session_id").alias("sessions"))
```

In append mode a window is written once the watermark (the latest event time seen, minus 30 minutes) passes its end, and events older than the watermark are dropped. That is the trade-off of stateful streaming: bounded state and final results in exchange for discarding very late data. The daily Gold tables make the opposite choice ([chapter 4](04-medallion.md)): recompute from the table, never drop.

The stream reads `silver.clickstream` with `skipChangeCommits`, so maintenance commits on the source cannot break it.

## Continuous jobs

`resources/stagedoor_streaming.job.yml` runs three always-on streams on a job cluster: Bronze, Silver and live engagement, side by side with no dependencies between them (each reads the previous table as a stream). A continuous job restarts its tasks when they stop or fail. It is deployed paused: start it from the Jobs UI, and pause the daily job's clickstream tasks while it runs.

## Kafka

With `source=kafka`, `bronze.clickstream` reads a topic with the same trigger choices. `startingOffsets=earliest` applies only to a new checkpoint; afterwards the checkpoint decides. `failOnDataLoss=false` keeps the stream alive when retention deleted offsets it had not read, which you should alert on rather than ignore.

## Watching streams

`run_stream` reports micro-batches and input rows from `recentProgress`, and every task writes them to `ops.task_runs`. In a notebook, `display()` of a streaming DataFrame shows live progress; in production, the Jobs UI shows each run, and stream metrics can be sent to your monitoring system with a `StreamingQueryListener`.

**Practise:** deploy the bundle to dev, start `stagedoor_streaming`, land a day of files with `land.sample_data`, and watch `gold.live_engagement` fill as watermarks pass.
