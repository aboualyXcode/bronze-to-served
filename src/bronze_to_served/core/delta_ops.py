"""Delta Lake operations used across layers."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException


def overwrite(df: DataFrame, table: str) -> None:
    """Replace a table's contents, keeping its definition (comments, constraints, clustering)."""
    df.write.format("delta").mode("overwrite").saveAsTable(table)


def replace_where(df: DataFrame, table: str, predicate: str) -> None:
    """Atomically replace only the rows matching `predicate` (incremental recompute of a window)."""
    df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(table)


def append(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("append").saveAsTable(table)


def delete_all(spark, table: str) -> None:
    spark.sql(f"DELETE FROM {table}")


def table_exists(spark, table: str) -> bool:
    try:
        spark.read.table(table).schema
        return True
    except AnalysisException:
        return False


def is_empty(spark, table: str) -> bool:
    return len(spark.read.table(table).limit(1).collect()) == 0


def latest_version(spark, table: str) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]["version"])


def read_changes(spark, table: str, starting_version: int, ending_version: int | None = None) -> DataFrame:
    """Rows changed since a version, from the change data feed (_change_type, _commit_version, ...)."""
    reader = spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", starting_version)
    if ending_version is not None:
        reader = reader.option("endingVersion", ending_version)
    return reader.table(table)


def date_list_predicate(column: str, dates) -> str:
    return f"`{column}` IN (" + ", ".join(f"DATE'{d.isoformat()}'" for d in dates) + ")"


def optimize(spark, table: str) -> None:
    """Compact small files; with liquid clustering this also clusters new data incrementally."""
    spark.sql(f"OPTIMIZE {table}")


def vacuum(spark, table: str, retain_hours: int = 168) -> None:
    spark.sql(f"VACUUM {table} RETAIN {int(retain_hours)} HOURS")


def analyze(spark, table: str) -> None:
    spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")
