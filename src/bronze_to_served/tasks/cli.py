"""`b2s`: the one entry point every Databricks job task calls, and a handy command locally.

    b2s --list
    b2s --task silver.orders --catalog stagedoor_dev --schema_prefix alice_ --run_id 123
    b2s --task bronze.clickstream --param source=kafka --param kafka_bootstrap_servers=host:9092

Databricks passes a wheel task's named parameters as --name=value; unknown ones become task parameters,
so job parameters pushed down to every task never break a task that does not use them.
"""
from __future__ import annotations

import argparse
import logging
import os
import uuid

from ..config import PlatformConfig
from .catalog import COMPONENTS, component, resolve
from .context import TRUE, TaskContext

log = logging.getLogger("b2s")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="b2s", description="Run a Bronze to Served task.")
    p.add_argument("--task", help="component id, e.g. silver.orders")
    p.add_argument("--list", action="store_true", help="list components and exit")
    p.add_argument("--catalog", default="stagedoor_dev")
    p.add_argument("--schema_prefix", "--schema-prefix", default="")
    p.add_argument("--runtime", default="auto", choices=["auto", "databricks", "local"])
    p.add_argument("--local_root", "--local-root", default=".local")
    p.add_argument("--run_id", "--run-id", default="")
    p.add_argument("--trigger", default="availableNow")
    p.add_argument("--lookback_days", "--lookback-days", type=int, default=3)
    p.add_argument("--full_refresh", "--full-refresh", default="false")
    p.add_argument("--param", action="append", default=[], help="key=value task parameter (repeatable)")
    return p


def _extra_params(tokens: list[str]) -> dict[str, str]:
    params, i = {}, 0
    while i < len(tokens):
        token = tokens[i]
        if token.startswith("--"):
            key, eq, value = token[2:].partition("=")
            if not eq and i + 1 < len(tokens) and not tokens[i + 1].startswith("--"):
                value, i = tokens[i + 1], i + 1
            params[key.replace("-", "_")] = value
        i += 1
    return params


def make_config(catalog="stagedoor_dev", schema_prefix="", runtime="auto", local_root=".local",
                trigger="availableNow", lookback_days=3, full_refresh=False) -> PlatformConfig:
    from ..spark import is_databricks
    if runtime == "auto":
        runtime = "databricks" if is_databricks() or os.environ.get("B2S_DATABRICKS_CONNECT") == "1" else "local"
    common = dict(schema_prefix=schema_prefix, trigger=trigger or "availableNow", lookback_days=int(lookback_days),
                  full_refresh=str(full_refresh).lower() in TRUE)
    if runtime == "local":
        return PlatformConfig.local(local_root, **common)
    return PlatformConfig(catalog=catalog, **common)


def run_task(task_id: str, cfg: PlatformConfig, params: dict | None = None, spark=None, run_id: str | None = None) -> dict:
    """Run one python component with auditing. Notebooks call this directly with the session they have."""
    from ..core.audit import task_run
    from ..spark import apply_session_defaults, get_spark

    c = component(task_id)
    if c.kind != "python":
        raise ValueError(f"{task_id} is a {c.kind} component; it runs as a {c.kind} task, not through b2s")
    spark = spark or get_spark(local_root=cfg.local_root or None, persistent=cfg.is_local)
    apply_session_defaults(spark)
    ctx = TaskContext(spark, cfg, run_id or f"manual-{uuid.uuid4().hex[:12]}", dict(params or {}))
    with task_run(spark, cfg, task_id, ctx.run_id) as audit:
        metrics = resolve(c)(ctx) or {}
        audit["metrics"] = metrics
    log.info("%s finished: %s", task_id, metrics)
    return metrics


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args, unknown = _parser().parse_known_args(argv)
    if args.list:
        for c in COMPONENTS:
            print(f"{c.id:28s} {c.kind:9s} {c.layer:11s} {c.title}")
        return
    if not args.task:
        raise SystemExit("--task is required (b2s --list shows them)")
    params = _extra_params(unknown)
    params.update(dict(p.split("=", 1) for p in args.param))
    cfg = make_config(args.catalog, args.schema_prefix, args.runtime, args.local_root, args.trigger, args.lookback_days,
                      args.full_refresh)
    run_task(args.task, cfg, params, run_id=args.run_id or None)


if __name__ == "__main__":
    main()
