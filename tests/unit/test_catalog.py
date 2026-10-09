"""The catalog is the contract between the CLI, the designer and the bundle: keep it honest."""
import ast
import os

from bronze_to_served.tasks.catalog import COMPONENTS, LAYERS, catalog_js
from bronze_to_served.stagedoor.tables import ALL_TABLES

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TABLES = {s.ref for s in ALL_TABLES} | {"gold.customer_360_secure", "declarative.clickstream_silver",
                                        "declarative.customers_scd2", "declarative.engagement_daily"}


def _functions(module: str) -> set:
    path = os.path.join(ROOT, "src", *module.split(".")) + ".py"
    with open(path, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    return {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}


def test_ids_are_unique_and_layers_and_kinds_are_known():
    ids = [c.id for c in COMPONENTS]
    assert len(ids) == len(set(ids))
    layers = {layer for layer, _, _ in LAYERS}
    for c in COMPONENTS:
        assert c.layer in layers and c.kind in ("python", "notebook", "condition", "pipeline"), c.id
        assert c.environment in ("default", "ml") and c.summary.endswith("."), c.id


def test_every_python_component_points_at_a_real_function():
    for c in COMPONENTS:
        if c.kind == "python":
            module, _, function = c.impl.partition(":")
            assert function in _functions(module), c.id


def test_notebooks_and_pipelines_exist():
    for c in COMPONENTS:
        if c.kind == "notebook":
            assert os.path.exists(os.path.join(ROOT, c.path + ".py")), c.path
        if c.kind == "pipeline":
            assert os.path.exists(os.path.join(ROOT, "resources", f"{c.path}.pipeline.yml")), c.path


def test_lineage_refers_to_real_tables():
    for c in COMPONENTS:
        for ref in (*c.reads, *c.writes):
            ref = ref.rstrip("?")
            if ref.startswith(("landing/", "model:", "serving:", "taskvalue:")):
                continue
            assert ref in TABLES, f"{c.id}: {ref}"


def test_designer_catalog_is_in_sync():
    with open(os.path.join(ROOT, "designer", "js", "catalog.js"), encoding="utf-8") as f:
        assert f.read() == catalog_js(), "run: python -m bronze_to_served.tasks.catalog"
