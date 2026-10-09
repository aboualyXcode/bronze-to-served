"""The Spark pipeline, run as the production DAG, must match the independent reference implementation
row for row: Silver, Gold, the quarantine and the ML features and labels."""
import shutil
from collections import Counter

import pytest

pytest.importorskip("pyspark")

from bronze_to_served.tasks.local import load, run_design  # noqa: E402
from reference.oracle import build  # noqa: E402
from reference.worlds import landing  # noqa: E402

from helpers import WORKSPACE_ONLY, assert_same, cents, norm, rows, template  # noqa: E402


@pytest.fixture(scope="module")
def world(spark, tmp_path_factory):
    from bronze_to_served.config import PlatformConfig
    from bronze_to_served.tasks.cli import run_task
    root = str(tmp_path_factory.mktemp("differential"))
    cfg = PlatformConfig.local(root, schema_prefix="diff_")
    run_task("setup.uc_objects", cfg, spark=spark, run_id="setup")
    shutil.copytree(landing("small"), cfg.landing_path(), dirs_exist_ok=True)
    run_design(load(template("platform_daily")), root, skip=WORKSPACE_ONLY, spark=spark, schema_prefix="diff_")
    run_design(load(template("ml_training")), root, skip=WORKSPACE_ONLY, spark=spark, schema_prefix="diff_")
    return cfg, build(cfg.landing_path())


def _expected(records, mapping=None, drop=()):
    out = []
    for r in records:
        row = {k: norm(v) for k, v in r.items() if k not in drop}
        for old, (new, fn) in (mapping or {}).items():
            row[new] = fn(row.pop(old))
        out.append(row)
    return out


def test_silver_orders_and_lines(spark, world):
    cfg, ref = world
    assert_same(rows(spark, cfg.table("silver", "orders")), _expected(ref.orders), ["order_id"], label="silver.orders")
    assert_same(rows(spark, cfg.table("silver", "order_items")), _expected(ref.order_items), ["order_id", "line_no"],
                label="silver.order_items")


def test_silver_customers_scd2(spark, world):
    cfg, ref = world
    assert_same(rows(spark, cfg.table("silver", "customers")), _expected(ref.customers), ["customer_sk"],
                label="silver.customers")


def test_silver_clickstream_and_reference(spark, world):
    import json
    cfg, ref = world
    actual = rows(spark, cfg.table("silver", "clickstream"))
    for r in actual:
        r["properties"] = json.loads(r["properties"])
    expected = _expected(ref.clickstream, {"properties": ("properties", json.loads)})
    assert_same(actual, expected, ["event_id"], label="silver.clickstream")
    assert_same(rows(spark, cfg.table("silver", "events")), _expected(ref.events), ["event_id"], label="silver.events")
    assert_same(rows(spark, cfg.table("silver", "venues")), _expected(ref.venues), ["venue_id"], label="silver.venues")


def test_quarantine_matches(spark, world):
    cfg, ref = world
    actual = Counter((r["source_table"], tuple(sorted(r["failed_rules"])))
                     for r in rows(spark, cfg.table("ops", "quarantine")) if r["source_table"] != "silver.events")
    source = {"orders": "silver.orders", "customers_cdc": "silver.customers", "clickstream": "silver.clickstream"}
    expected = Counter((source[q["source"]], tuple(q["rules"])) for q in ref.quarantine)
    assert actual == expected


GOLD = {
    "dim_event": (["event_id"], {"base_cents": ("base_price", cents)}, ()),
    "fct_sales": (["order_id", "line_no"], {"unit_cents": ("unit_price", cents), "gross_cents": ("gross_amount", cents),
                                            "refunded_cents": ("refunded_amount", cents)}, ()),
    "agg_daily_sales": (["sales_date", "venue_id", "genre"], {"gross_cents": ("gross_revenue", cents),
                                                              "refunded_cents": ("refunded_amount", cents),
                                                              "net_cents": ("net_revenue", cents)}, ()),
    "event_performance": (["event_id"], {"gross_cents": ("gross_revenue", cents), "refunded_cents": ("refunded_amount", cents),
                                         "net_cents": ("net_revenue", cents)}, ("sell_through",)),
    "customer_360": (["customer_id"], {"net_cents": ("lifetime_net_revenue", cents),
                                       "marketing_opt_in": ("marketing_opt_in", lambda v: None if v is None else bool(v))}, ()),
    "funnel_daily": (["session_date", "device"], {}, ("view_to_purchase_rate",)),
}


@pytest.mark.parametrize("table", sorted(GOLD))
def test_gold(spark, world, table):
    cfg, ref = world
    key, mapping, floats = GOLD[table]
    expected = _expected(ref.gold[table], mapping)
    assert_same(rows(spark, cfg.table("gold", table), list(expected[0])), expected, key, floats, label=f"gold.{table}")


def test_features_and_labels(spark, world):
    cfg, ref = world
    mapping = {"revenue_90d_cents": ("revenue_90d", cents), "revenue_365d_cents": ("revenue_365d", cents),
               "marketing_opt_in": ("marketing_opt_in", lambda v: None if v is None else bool(v))}
    expected = _expected(ref.features, mapping)
    dates = {r["as_of_date"] for r in expected}       # the daily run also scored the latest date, which has no label
    actual = [r for r in rows(spark, cfg.table("ml", "customer_features")) if r["as_of_date"] in dates]
    assert_same(actual, expected, ["customer_id", "as_of_date"], ("avg_ticket_price_365d", "engagement_ratio"),
                label="ml.customer_features")
    assert_same(rows(spark, cfg.table("ml", "churn_labels")), _expected(ref.labels), ["customer_id", "as_of_date"],
                label="ml.churn_labels")
