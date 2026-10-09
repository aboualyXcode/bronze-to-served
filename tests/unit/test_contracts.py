import os

import pytest

from bronze_to_served.config import PlatformConfig
from bronze_to_served.core.contracts import Column, TableSpec, create_table_sql, data_dictionary
from bronze_to_served.stagedoor.tables import ALL_TABLES, spec

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_unity_catalog_ddl_has_keys_clustering_comments_and_cdf():
    sql = create_table_sql(spec("ml.customer_features"), PlatformConfig(catalog="c", schema_prefix="p_"))
    assert sql.startswith("CREATE TABLE IF NOT EXISTS `c`.`p_ml`.`customer_features`")
    assert "PRIMARY KEY (`customer_id`, `as_of_date` TIMESERIES)" in sql
    assert "CLUSTER BY (`as_of_date`)" in sql and "COMMENT 'Churn features" in sql
    assert "'delta.enableChangeDataFeed' = 'true'" in create_table_sql(spec("silver.orders"), PlatformConfig())


def test_local_ddl_leaves_out_unity_catalog_features(tmp_path):
    sql = create_table_sql(spec("silver.orders"), PlatformConfig.local(str(tmp_path)))
    assert "PRIMARY KEY" not in sql and "CLUSTER BY" not in sql and "`order_id` STRING NOT NULL" in sql


def test_comments_with_quotes_are_escaped():
    s = TableSpec("gold", "t", "it's", (Column("a", "INT", "the 'a' column"),))
    assert "COMMENT 'it\\'s'" in create_table_sql(s, PlatformConfig())


def test_contracts_are_validated():
    with pytest.raises(ValueError):
        TableSpec("gold", "t", "", (Column("a", "INT"), Column("a", "INT")))
    with pytest.raises(ValueError):
        TableSpec("gold", "t", "", (Column("a", "INT"),), primary_key=("a",))      # nullable key
    with pytest.raises(ValueError):
        TableSpec("gold", "t", "", (Column("a", "INT"),), cluster_by=("b",))


def test_every_table_is_documented_and_the_dictionary_is_in_sync():
    assert all(s.comment and all(c.comment for c in s.columns) for s in ALL_TABLES)
    with open(os.path.join(ROOT, "docs", "data-dictionary.md"), encoding="utf-8") as f:
        assert f.read() == data_dictionary(ALL_TABLES) + "\n", "run: python -m bronze_to_served.stagedoor.tables"
