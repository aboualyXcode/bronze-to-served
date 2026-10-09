import pytest

from bronze_to_served.core.quality import QUARANTINED, Expectation, evaluate_gate
from bronze_to_served.stagedoor import rules
from reference import oracle


def _m(table, failed, total):
    return {"source_table": table, "rule": QUARANTINED, "failed_rows": failed, "total_rows": total}


def test_gate_passes_within_limits_and_fails_beyond():
    metrics = [_m("silver.orders", 10, 1000), _m("silver.orders", 0, 500), _m("silver.clickstream", 30, 1000)]
    assert evaluate_gate(metrics, {"silver.orders": 0.02, "silver.clickstream": 0.05}) == []
    violations = evaluate_gate(metrics, {"silver.orders": 0.005, "*": 0.01})
    assert len(violations) == 2 and violations[0].startswith("silver.clickstream: 30 of 1000")


def test_gate_ignores_per_rule_rows_and_empty_runs():
    rule_row = {"source_table": "silver.orders", "rule": "valid_items", "failed_rows": 999, "total_rows": 1000}
    assert evaluate_gate([rule_row], {"*": 0.0}) == []
    assert evaluate_gate([], {"*": 0.0}) == []


def test_expectations_are_validated():
    with pytest.raises(ValueError):
        Expectation("x", "a > 0", action="explode")
    with pytest.raises(ValueError):
        Expectation("not an identifier", "a > 0")


def test_pipeline_rules_have_the_same_names_as_the_reference_implementation():
    # an empty record breaks every rule, so the oracle reports all of its rule names
    def names(rule_list):
        return {r.name for r in rule_list if r.action != "warn"}
    assert names(rules.ORDER_RULES) == set(oracle.order_failures(oracle.parse_order({})))
    assert names(rules.CDC_RULES) == set(oracle.cdc_failures(oracle.parse_cdc({})))
    assert names(rules.CLICKSTREAM_RULES) == set(oracle.click_failures(oracle.parse_click({})))
