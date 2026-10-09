"""One audit row per task run in ops.task_runs: what ran, when, how it ended and what it did."""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone

log = logging.getLogger(__name__)
SCHEMA = ("run_id string, task string, status string, started_at timestamp, finished_at timestamp, "
          "metrics string, error string")


@contextmanager
def task_run(spark, cfg, task: str, run_id: str):
    started = datetime.now(timezone.utc)
    record = {"metrics": {}}
    status, error = "SUCCEEDED", None
    try:
        yield record
    except BaseException as exc:
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"[:4000]
        raise
    finally:
        try:
            row = [(run_id, task, status, started, datetime.now(timezone.utc),
                    json.dumps(record["metrics"], default=str, sort_keys=True), error)]
            (spark.createDataFrame(row, SCHEMA).write.format("delta").mode("append")
             .saveAsTable(cfg.table("ops", "task_runs")))
        except Exception as audit_error:      # never hide the task's own outcome behind an audit failure
            log.warning("could not write the audit row for %s: %s", task, audit_error)
