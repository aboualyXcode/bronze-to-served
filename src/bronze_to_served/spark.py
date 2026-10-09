"""One way to get a SparkSession, wherever the code runs.

* On Databricks (classic clusters, serverless jobs, notebooks) the runtime already has a session.
* With Databricks Connect (B2S_DATABRICKS_CONNECT=1) your laptop drives a remote cluster or serverless.
* Otherwise a local Spark with Delta Lake is started (tests, laptops): pip install -e '.[local]'.
"""
from __future__ import annotations

import os


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "bronze-to-served", local_root: str | None = None, persistent: bool = False):
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if is_databricks():
        return SparkSession.builder.getOrCreate()
    if os.environ.get("B2S_DATABRICKS_CONNECT") == "1":
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    return local_spark(app_name, local_root or os.path.abspath(".local"), persistent=persistent)


def local_spark(app_name: str, root: str, persistent: bool = False):
    """A small local Spark with Delta Lake. `persistent` keeps table definitions between processes (Derby)."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]").appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.path.join(root, "warehouse"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    if persistent:
        builder = (builder.config("spark.sql.catalogImplementation", "hive")
                   .config("javax.jdo.option.ConnectionURL",
                           f"jdbc:derby:;databaseName={os.path.join(root, 'metastore_db')};create=true"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def apply_session_defaults(spark) -> None:
    """Settings every task relies on. UTC makes dates and timestamps reproducible everywhere."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
