"""The reference implementation is only a useful oracle if its own output is internally consistent."""
from collections import Counter, defaultdict

from reference.worlds import reference

KNOWN_RULES = {"order_id_present", "customer_id_present", "event_id_present", "valid_order_ts", "valid_updated_at",
               "known_status", "has_items", "valid_items", "valid_lsn", "known_op", "valid_changed_at",
               "event_id_present", "session_present", "valid_event_ts", "known_event_type"}


def test_scd2_history_is_well_formed():
    by_customer = defaultdict(list)
    for v in reference().customers:
        by_customer[v["customer_id"]].append(v)
    assert len(by_customer) > 500
    for versions in by_customer.values():
        versions.sort(key=lambda v: v["_start_seq"])
        assert sum(v["is_current"] for v in versions) <= 1
        for v in versions:
            assert v["is_current"] == (v["valid_to"] is None)
            assert v["valid_to"] is None or v["valid_from"] < v["valid_to"]
        for older, newer in zip(versions, versions[1:]):
            assert older["valid_to"] is not None and older["valid_to"] <= newer["valid_from"]


def test_history_includes_changes_and_deletions():
    counts = Counter(v["customer_id"] for v in reference().customers)
    current = {v["customer_id"] for v in reference().customers if v["is_current"]}
    assert any(n > 1 for n in counts.values()), "expected SCD2 history"
    assert set(counts) - current, "expected deleted customers"


def test_order_status_matches_its_timestamps():
    for o in reference().orders:
        assert (o["status"] == "REFUNDED") == (o["refunded_at"] is not None)
        assert (o["status"] == "CANCELLED") == (o["cancelled_at"] is not None)
        for ts in (o["paid_at"], o["cancelled_at"], o["refunded_at"]):
            assert ts is None or ts >= o["order_ts"]


def test_gold_reconciles_with_silver():
    ref = reference()
    g = ref.gold
    paid = sum(o["order_amount"] for o in ref.orders if o["paid_at"] is not None)
    gross = sum(r["gross_cents"] for r in g["fct_sales"])
    assert gross == sum(r["gross_cents"] for r in g["agg_daily_sales"]) == int(paid * 100)
    refunded = sum(r["refunded_cents"] for r in g["fct_sales"])
    assert refunded == sum(r["refunded_cents"] for r in g["agg_daily_sales"]) > 0
    assert all(r["net_cents"] == r["gross_cents"] - r["refunded_cents"] for r in g["agg_daily_sales"])
    net_tickets = sum(r["quantity"] for r in g["fct_sales"] if r["refunded_at"] is None)
    assert sum(r["tickets_sold"] for r in g["event_performance"]) == net_tickets
    with_sk = sum(1 for r in g["fct_sales"] if r["customer_sk"])
    assert with_sk / len(g["fct_sales"]) > 0.99


def test_features_only_look_backwards():
    ref = reference()
    assert ref.features and len(ref.labels) == len(ref.features)
    for f in ref.features:
        assert f["recency_days"] >= 0 and f["orders_365d"] >= 1
        assert f["orders_30d"] <= f["orders_90d"] <= f["orders_365d"]
        assert f["events_7d"] <= f["events_30d"]
    churn = Counter(l["churned"] for l in ref.labels)
    assert 0.1 < churn[1] / (churn[0] + churn[1]) < 0.7


def test_quarantine_uses_known_rules():
    sources = Counter(q["source"] for q in reference().quarantine)
    assert sources["orders"] > 0 and sources["clickstream"] > 0
    assert {r for q in reference().quarantine for r in q["rules"]} <= KNOWN_RULES
