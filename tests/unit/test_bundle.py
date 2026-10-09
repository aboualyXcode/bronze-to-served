"""The bundle must deploy: every job task points at a real component, notebook, pipeline or environment,
dependencies form a DAG, and every ${var.*}, {{job.parameters.*}} and {{tasks.*}} reference resolves."""
import glob
import os
import re

import yaml

from bronze_to_served.tasks.catalog import COMPONENTS

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PYTHON = {c.id for c in COMPONENTS if c.kind == "python"}


def _load(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


BUNDLE = _load(os.path.join(ROOT, "databricks.yml"))
FILES = sorted(glob.glob(os.path.join(ROOT, "resources", "*.yml")))
RESOURCES = [_load(f)["resources"] for f in FILES]
JOBS = {k: v for r in RESOURCES for k, v in r.get("jobs", {}).items()}
PIPELINES = {k: v for r in RESOURCES for k, v in r.get("pipelines", {}).items()}


def _tasks(job):
    for t in job["tasks"]:
        yield t, t.get("for_each_task", {}).get("task", t)


def test_targets_and_includes():
    assert BUNDLE["include"] == ["resources/*.yml"]
    targets = BUNDLE["targets"]
    assert targets["dev"]["mode"] == "development" and targets["dev"]["default"] is True
    assert targets["staging"]["mode"] == targets["prod"]["mode"] == "production"
    assert len(JOBS) == 7 and set(PIPELINES) == {"stagedoor_declarative"}


def test_resource_keys_are_unique_across_types():
    # Asset Bundles reject a key used twice, even for different resource types (a job and a pipeline).
    keys = [key for r in RESOURCES for entries in r.values() for key in entries]
    duplicates = sorted({k for k in keys if keys.count(k) > 1})
    assert not duplicates, f"bundle resource keys must be unique across all resource types: {duplicates}"


def test_every_variable_reference_is_defined():
    defined = set(BUNDLE["variables"])
    for path in [os.path.join(ROOT, "databricks.yml")] + FILES:
        with open(path, encoding="utf-8") as f:
            used = set(re.findall(r"\$\{var\.([A-Za-z0-9_]+)\}", f.read()))
        assert used <= defined, f"{os.path.basename(path)}: undefined {used - defined}"


def test_jobs_are_acyclic_and_reference_real_things():
    for name, job in JOBS.items():
        keys = [t["task_key"] for t in job["tasks"]]
        assert len(keys) == len(set(keys)), name
        params = {p["name"] for p in job.get("parameters", [])}
        envs = {e["environment_key"] for e in job.get("environments", [])}
        clusters = {c["job_cluster_key"] for c in job.get("job_clusters", [])}
        deps = {t["task_key"]: [d["task_key"] for d in t.get("depends_on", [])] for t in job["tasks"]}
        kinds = {t["task_key"]: ("condition" if "condition_task" in t else "other") for t in job["tasks"]}
        for outer, t in _tasks(job):
            for d in outer.get("depends_on", []):
                assert d["task_key"] in keys, f"{name}: {d}"
                assert "outcome" not in d or kinds[d["task_key"]] == "condition", f"{name}: outcome on a non-condition"
            if "python_wheel_task" in t:
                w = t["python_wheel_task"]
                assert w["package_name"] == "bronze_to_served" and w["entry_point"] == "b2s"
                assert w["named_parameters"]["task"] in PYTHON, f"{name}: {w['named_parameters']['task']}"
                assert t.get("environment_key") in envs or t.get("job_cluster_key") in clusters, f"{name}: no compute"
            if "notebook_task" in t:
                path = os.path.normpath(os.path.join(ROOT, "resources", t["notebook_task"]["notebook_path"]))
                assert os.path.exists(path), path
            if "pipeline_task" in t:
                ref = re.fullmatch(r"\$\{resources\.pipelines\.(\w+)\.id\}", t["pipeline_task"]["pipeline_id"])
                assert ref and ref.group(1) in PIPELINES
            text = yaml.safe_dump(outer)
            for p in re.findall(r"\{\{job\.parameters\.(\w+)\}\}", text):
                assert p in params, f"{name}: {{{{job.parameters.{p}}}}} is not a job parameter"
            for src in re.findall(r"\{\{tasks\.([\w-]+)\.values\.\w+\}\}", text):
                assert src in _ancestors(deps, outer["task_key"]), f"{name}: {outer['task_key']} reads {src} before it runs"
        _assert_acyclic(name, deps)


def _ancestors(deps, key):
    seen, stack = set(), list(deps.get(key, []))
    while stack:
        k = stack.pop()
        if k not in seen:
            seen.add(k)
            stack.extend(deps.get(k, []))
    return seen


def _assert_acyclic(name, deps):
    for key in deps:
        assert key not in _ancestors(deps, key), f"{name}: {key} depends on itself"


def test_serverless_environments_install_the_wheel():
    for name, job in JOBS.items():
        for env in job.get("environments", []):
            assert "../dist/*.whl" in env["spec"]["dependencies"], name


def test_jobs_are_generated_from_templates_and_pipeline_code_exists():
    for path in FILES:
        if path.endswith(".job.yml"):
            with open(path, encoding="utf-8") as f:
                assert f.readline().startswith("# Generated by the Bronze to Served pipeline designer"), path
    for pipeline in PIPELINES.values():
        for lib in pipeline["libraries"]:
            assert os.path.exists(os.path.normpath(os.path.join(ROOT, "resources", lib["file"]["path"])))
