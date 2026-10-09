"""Helpers shared by the Spark tests: collect tables in a comparable form, run templates locally."""
from __future__ import annotations

import os
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Tasks that need a Databricks workspace (MLflow registry, Model Serving) or would overwrite the test data.
WORKSPACE_ONLY = ("land.sample_data", "ml.train", "ml.evaluate", "ml.promote", "ml.rollback", "ml.deploy_endpoint",
                  "ml.batch_inference")


def template(name: str) -> str:
    return os.path.join(ROOT, "designer", "templates", f"{name}.json")


def rows(spark, table: str, columns: list[str] | None = None) -> list[dict]:
    """Collect a table with timestamps and dates rendered as UTC strings (no Python time zone surprises)."""
    df = spark.read.table(table)
    cols = []
    for field in df.schema.fields:
        if columns and field.name not in columns:
            continue
        if isinstance(field.dataType, TimestampType):
            cols.append(F.date_format(field.name, "yyyy-MM-dd HH:mm:ss.SSSSSS").alias(field.name))
        elif isinstance(field.dataType, DateType):
            cols.append(F.date_format(field.name, "yyyy-MM-dd").alias(field.name))
        else:
            cols.append(F.col(field.name))
    return [r.asDict() for r in df.select(*cols).collect()]


def norm(value):
    """Reference values -> the Spark-side representation used by rows()."""
    from datetime import date, datetime
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, date):
        return value.isoformat()
    return value


def cents(value) -> Decimal | None:
    return None if value is None else (Decimal(value) / 100).quantize(Decimal("0.01"))


def assert_same(actual: list[dict], expected: list[dict], key: list[str], floats: tuple = (), label: str = "") -> None:
    def index(rs):
        return {tuple(r[k] for k in key): r for r in rs}
    a, e = index(actual), index(expected)
    missing, extra = sorted(set(e) - set(a))[:5], sorted(set(a) - set(e))[:5]
    assert not missing and not extra, f"{label}: missing {missing}, unexpected {extra}"
    assert len(actual) == len(expected), f"{label}: duplicate keys"
    for k, exp in e.items():
        act = a[k]
        for col, value in exp.items():
            got = act[col]
            if col in floats and value is not None and got is not None:
                assert abs(float(got) - float(value)) <= 1e-9 * max(1.0, abs(float(value))), f"{label} {k} {col}: {got} != {value}"
            else:
                assert got == value, f"{label} {k} {col}: {got!r} != {value!r}"
