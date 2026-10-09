"""Run a designer template's whole DAG locally, in one process and one Spark session.

    python -m bronze_to_served.tasks.local designer/templates/platform_daily.json --root .local

The same templates generate the Databricks jobs (resources/*.job.yml), so this runs the production DAG:
tasks in dependency order, condition tasks evaluated, notebook and pipeline tasks skipped (they need a
workspace). The Spark integration tests use it too.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import uuid

from .catalog import component
from .cli import make_config, run_task

log = logging.getLogger(__name__)
OPS = {"EQUAL_TO": lambda a, b: a == b, "NOT_EQUAL": lambda a, b: a != b}


def load(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def order(design: dict) -> list[dict]:
    """Nodes in dependency order (Kahn's algorithm, ties in template order); raises on a cycle."""
    nodes = {n["id"]: n for n in design["nodes"]}
    indegree = {nid: 0 for nid in nodes}
    for e in design["edges"]:
        indegree[e["to"]] += 1
    ready = [nid for nid in nodes if indegree[nid] == 0]
    out = []
    while ready:
        nid = ready.pop(0)
        out.append(nodes[nid])
        for e in design["edges"]:
            if e["from"] == nid:
                indegree[e["to"]] -= 1
                if indegree[e["to"]] == 0:
                    ready.append(e["to"])
    if len(out) != len(nodes):
        raise ValueError("the design has a cycle")
    return out


def substitute(value: str, job_params: dict, run_id: str, task_values: dict) -> str:
    value = str(value).replace("{{job.run_id}}", run_id)
    value = re.sub(r"\{\{job\.parameters\.(\w+)\}\}", lambda m: str(job_params.get(m.group(1), "")), value)
    value = re.sub(r"\{\{tasks\.(\w+)\.values\.(\w+)\}\}", lambda m: str(task_values.get((m.group(1), m.group(2)), "")), value)
    return "" if "${" in value else value      # bundle variables do not exist locally: fall back to defaults


def run_design(design: dict, root: str, overrides: dict | None = None, skip: tuple = (), spark=None,
               schema_prefix: str = "") -> list:
    job_params = {p["name"]: p.get("default", "") for p in design["job"].get("parameters", [])}
    job_params.update(overrides or {})
    run_id = f"local-{uuid.uuid4().hex[:12]}"
    cfg = make_config(runtime="local", local_root=root, schema_prefix=schema_prefix, lookback_days=job_params.get("lookback_days") or 3,
                      full_refresh=job_params.get("full_refresh") or False)
    results, status, task_values = [], {}, {}
    for node in order(design):
        incoming = [e for e in design["edges"] if e["to"] == node["id"]]
        blocked = any(status.get(e["from"]) != "ok" or (e.get("outcome") and status.get(e["from"] + ":outcome") != e["outcome"])
                      for e in incoming)
        c = component(node["component"])
        key = node["task_key"]
        if blocked and node.get("run_if") not in ("ALL_DONE", "AT_LEAST_ONE_SUCCESS"):
            status[node["id"]] = "excluded"
            results.append((key, "excluded"))
            continue
        if c.kind == "condition":
            cond = node.get("condition") or {}
            left = substitute(cond.get("left", ""), job_params, run_id, task_values)
            right = substitute(cond.get("right", ""), job_params, run_id, task_values)
            outcome = "true" if OPS.get(cond.get("op", "EQUAL_TO"), OPS["EQUAL_TO"])(left, right) else "false"
            status[node["id"]], status[node["id"] + ":outcome"] = "ok", outcome
            results.append((key, f"condition {outcome}"))
            continue
        if c.kind != "python" or c.id in skip:
            status[node["id"]] = "ok"
            results.append((key, "skipped locally"))
            continue
        items = json.loads(node["for_each"]["inputs"]) if node.get("for_each") else [None]
        outcomes = []
        for item in items:                      # a for-each task runs once per input, with {{input}} set
            params = {k: substitute(str(v).replace("{{input}}", str(item)) if item is not None else v, job_params, run_id,
                                    task_values) for k, v in (node.get("params") or {}).items()}
            overrides = {"trigger": params.pop("trigger", "") or "availableNow"}
            if params.get("full_refresh"):
                overrides["full_refresh"] = params.pop("full_refresh").lower() == "true"
            if params.get("lookback_days"):
                overrides["lookback_days"] = int(params.pop("lookback_days"))
            log.info("running %s (%s)%s", key, c.id, f" for {item}" if item is not None else "")
            outcomes.append(run_task(c.id, cfg.with_overrides(**overrides), params, spark=spark, run_id=run_id))
        results.append((key, outcomes[0] if len(outcomes) == 1 else {"iterations": len(outcomes)}))
        status[node["id"]] = "ok"
    return results


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description="Run a designer template locally.")
    p.add_argument("template")
    p.add_argument("--root", default=".local")
    p.add_argument("--set", action="append", default=[], help="job parameter override key=value")
    p.add_argument("--skip", action="append", default=[], help="component id to skip")
    a = p.parse_args(argv)
    for key, outcome in run_design(load(a.template), a.root, dict(s.split("=", 1) for s in a.set), tuple(a.skip)):
        print(f"{key:32s} {outcome}")


if __name__ == "__main__":
    main()
