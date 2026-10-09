import pytest

from bronze_to_served.tasks.cli import _extra_params, make_config, main
from bronze_to_served.tasks.local import order, substitute


def test_databricks_named_parameters_become_task_parameters():
    assert _extra_params(["--mode=training", "--source", "kafka", "--kafka-topic=t", "--flag"]) == {
        "mode": "training", "source": "kafka", "kafka_topic": "t", "flag": ""}


def test_local_config(tmp_path):
    cfg = make_config(runtime="local", local_root=str(tmp_path), full_refresh="true", lookback_days="5")
    assert cfg.is_local and cfg.full_refresh and cfg.lookback_days == 5


def test_list_runs_without_spark(capsys):
    main(["--list"])
    assert "silver.customers" in capsys.readouterr().out


def test_dependency_order_and_cycles():
    design = {"nodes": [{"id": i} for i in "abcd"],
              "edges": [{"from": "a", "to": "c"}, {"from": "b", "to": "c"}, {"from": "c", "to": "d"}]}
    assert [n["id"] for n in order(design)] == ["a", "b", "c", "d"]
    design["edges"].append({"from": "d", "to": "a"})
    with pytest.raises(ValueError):
        order(design)


def test_dynamic_value_references():
    values = {("validation_report", "promote"): "true"}
    assert substitute("{{tasks.validation_report.values.promote}}", {}, "r1", values) == "true"
    assert substitute("{{job.parameters.mode}}-{{job.run_id}}", {"mode": "x"}, "r1", {}) == "x-r1"
    assert substitute("${var.catalog}", {}, "r1", {}) == ""
