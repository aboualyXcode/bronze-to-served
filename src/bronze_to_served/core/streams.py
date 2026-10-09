"""Running Structured Streaming queries the same way in scheduled jobs, continuous jobs and tests.

trigger "availableNow": process everything that is new, then stop. This is incremental batch: the job
    can run on a schedule or a file-arrival trigger, on serverless or classic compute, and costs nothing
    between runs. It is the only trigger serverless compute supports.
trigger "processingTime=<interval>": keep running and pick up new data every interval (continuous jobs
    on classic compute, for low latency).

Checkpoints live in a Unity Catalog volume, one folder per stream: deleting the folder (reset_stream)
reprocesses the source from the start.
"""
from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass

from ..config import PlatformConfig

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class StreamResult:
    name: str
    batches: int
    input_rows: int


def apply_trigger(writer, trigger: str):
    if trigger == "availableNow":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=trigger.split("=", 1)[1].strip())


def _rows(progress) -> int:
    value = progress.get("numInputRows") if isinstance(progress, dict) else getattr(progress, "numInputRows", 0)
    return int(value or 0)


def run_stream(df, cfg: PlatformConfig, *, name: str, table: str | None = None, foreach_batch=None,
               output_mode: str = "append") -> StreamResult:
    """Start a stream into a Delta table (or a foreachBatch function), wait for it, report its progress."""
    if (table is None) == (foreach_batch is None):
        raise ValueError("pass exactly one of table or foreach_batch")
    writer = df.writeStream.queryName(name).option("checkpointLocation", cfg.checkpoint_path(name))
    writer = apply_trigger(writer, cfg.trigger)
    if foreach_batch is not None:
        query = writer.foreachBatch(foreach_batch).start()
    else:
        query = writer.format("delta").outputMode(output_mode).toTable(table)
    query.awaitTermination()
    progress = list(query.recentProgress or [])
    result = StreamResult(name, len(progress), sum(_rows(p) for p in progress))
    log.info("stream %s: %d micro-batches, %d input rows", name, result.batches, result.input_rows)
    return result


def reset_stream(cfg: PlatformConfig, name: str) -> None:
    """Forget a stream's progress (and Auto Loader's file log and schema) so it reprocesses everything."""
    path = cfg.checkpoint_path(name)
    if os.path.exists(path):
        shutil.rmtree(path)
        log.warning("reset checkpoint %s", path)
