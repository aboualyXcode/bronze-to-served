"""Local Spark + Delta Lake fixtures. Needs: pip install -e '.[local,ml,dev]' and Java 17."""
import itertools

import pytest

_prefixes = itertools.count(1)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from bronze_to_served.spark import local_spark
    session = local_spark("bronze-to-served-tests", str(tmp_path_factory.mktemp("spark")))
    yield session
    session.stop()


@pytest.fixture
def platform(spark, tmp_path):
    """A fresh, isolated platform: its own schema prefix, folders and every table created."""
    from bronze_to_served.config import PlatformConfig
    from bronze_to_served.tasks.cli import run_task
    cfg = PlatformConfig.local(str(tmp_path), schema_prefix=f"t{next(_prefixes)}_")
    run_task("setup.uc_objects", cfg, spark=spark, run_id="setup")
    return cfg
