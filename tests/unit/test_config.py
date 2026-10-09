import os

import pytest

from bronze_to_served.config import PlatformConfig


def test_unity_catalog_names_include_the_schema_prefix():
    cfg = PlatformConfig(catalog="stagedoor_prod", schema_prefix="")
    assert cfg.table("silver", "orders") == "`stagedoor_prod`.`silver`.`orders`"
    dev = PlatformConfig(catalog="stagedoor_dev", schema_prefix="alice_")
    assert dev.table_ref("gold.customer_360") == "`stagedoor_dev`.`alice_gold`.`customer_360`"
    assert dev.model_name("churn_model") == "stagedoor_dev.alice_ml.churn_model"
    assert dev.landing_path("orders") == "/Volumes/stagedoor_dev/alice_landing/raw/orders"
    assert dev.checkpoint_path("silver_orders") == "/Volumes/stagedoor_dev/alice_ops/checkpoints/silver_orders"
    assert dev.export_path("crm") == "/Volumes/stagedoor_dev/alice_ops/exports/crm"


def test_local_runtime_uses_two_level_names_and_folders(tmp_path):
    cfg = PlatformConfig.local(str(tmp_path))
    assert cfg.table("bronze", "orders_raw") == "`bronze`.`orders_raw`"
    assert cfg.landing_path("orders") == os.path.join(str(tmp_path), "volumes", "landing", "raw", "orders")
    assert cfg.is_local and cfg.warehouse_dir.endswith("warehouse")


@pytest.mark.parametrize("kwargs", [{"catalog": "bad-name"}, {"schema_prefix": "x y"}, {"runtime": "laptop"},
                                    {"runtime": "local"}, {"trigger": "every minute"}, {"lookback_days": -1}])
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        PlatformConfig(**kwargs)


def test_invalid_names_are_rejected():
    cfg = PlatformConfig()
    for call in (lambda: cfg.table("platinum", "x"), lambda: cfg.table("gold", "drop table"), lambda: cfg.table_ref("orders")):
        with pytest.raises(ValueError):
            call()


def test_processing_time_trigger_is_accepted():
    assert PlatformConfig(trigger="processingTime=30 seconds").trigger.endswith("30 seconds")
