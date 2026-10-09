"""Daily incremental runs must end where a single run over all the data ends, and re-running is harmless."""
import pytest

pytest.importorskip("pyspark")

from bronze_to_served.stagedoor.datagen import GeneratorSettings, write_days  # noqa: E402
from bronze_to_served.tasks.cli import run_task  # noqa: E402
from bronze_to_served.tasks.local import load, run_design  # noqa: E402

from helpers import WORKSPACE_ONLY, rows, template  # noqa: E402

SETTINGS = GeneratorSettings(customers=200, days=60, clickstream_scale=0.5)
SILVER_GOLD = [("silver", "orders"), ("silver", "order_items"), ("silver", "customers"), ("silver", "clickstream"),
               ("gold", "fct_sales"), ("gold", "agg_daily_sales"), ("gold", "funnel_daily"), ("gold", "customer_360")]
TECHNICAL = {"_first_ingested_at", "_last_ingested_at", "_ingested_at"}


def _snapshot(spark, cfg):
    out = {}
    for layer, name in SILVER_GOLD:
        out[name] = sorted(tuple(sorted((k, str(v)) for k, v in r.items() if k not in TECHNICAL))
                           for r in rows(spark, cfg.table(layer, name)))
    return out


@pytest.fixture(autouse=True)
def lenient_gate(monkeypatch):
    """Tiny daily batches make quarantine rates jumpy; the gate itself is tested in tests/unit/test_quality.py."""
    from bronze_to_served.stagedoor import operations
    monkeypatch.setattr(operations, "DEFAULT_THRESHOLDS", {"*": 1.0})


def _platform(spark, cfg, root):
    run_design(load(template("platform_daily")), root, skip=WORKSPACE_ONLY, spark=spark, schema_prefix=cfg.schema_prefix)


def test_daily_runs_equal_one_run_and_reruns_are_harmless(spark, tmp_path_factory):
    from bronze_to_served.config import PlatformConfig
    results = []
    for name, chunks in (("once", [(0, 59)]), ("daily", [(0, 29), (30, 44), (45, 52), (53, 59)])):
        root = str(tmp_path_factory.mktemp(name))
        cfg = PlatformConfig.local(root, schema_prefix=f"inc_{name}_", lookback_days=3)
        run_task("setup.uc_objects", cfg, spark=spark, run_id="setup")
        for first, last in chunks:
            write_days(SETTINGS, cfg.landing_path(), first, last)
            _platform(spark, cfg, root)
        results.append((cfg, root, _snapshot(spark, cfg)))
    assert results[0][2] == results[1][2]
    cfg, root, before = results[1]
    _platform(spark, cfg, root)                      # nothing new arrived: nothing may change
    assert _snapshot(spark, cfg) == before


def test_full_refresh_rebuilds_the_same_tables(spark, tmp_path_factory):
    from bronze_to_served.config import PlatformConfig
    root = str(tmp_path_factory.mktemp("refresh"))
    cfg = PlatformConfig.local(root, schema_prefix="inc_refresh_")
    run_task("setup.uc_objects", cfg, spark=spark, run_id="setup")
    write_days(SETTINGS, cfg.landing_path(), 0, 59)
    _platform(spark, cfg, root)
    before = _snapshot(spark, cfg)
    run_design(load(template("platform_daily")), root, overrides={"full_refresh": "true"}, skip=WORKSPACE_ONLY,
               spark=spark, schema_prefix=cfg.schema_prefix)
    assert _snapshot(spark, cfg) == before
