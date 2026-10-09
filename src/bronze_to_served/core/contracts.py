"""Table contracts: every table's columns, types, comments, keys and clustering, declared once in code.

A contract is used three ways: to create the table (with comments that show up in Unity Catalog), to
conform every DataFrame before it is written (so a write can never drift from the contract), and to
generate the data dictionary in docs/.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..config import PlatformConfig


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    comment: str = ""
    nullable: bool = True


@dataclass(frozen=True)
class TableSpec:
    layer: str
    name: str
    comment: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...] = ()
    timeseries_key: str | None = None
    cluster_by: tuple[str, ...] = ()
    change_data_feed: bool = False
    checks: tuple[tuple[str, str], ...] = ()   # (constraint name, SQL expression) -> Delta CHECK constraints

    @property
    def ref(self) -> str:
        return f"{self.layer}.{self.name}"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def __post_init__(self) -> None:
        names = self.column_names
        if len(names) != len(set(names)):
            raise ValueError(f"{self.ref}: duplicate column names")
        for key in (*self.primary_key, *self.cluster_by):
            if key not in names:
                raise ValueError(f"{self.ref}: key column {key!r} is not a column")
        for key in self.primary_key:
            if next(c for c in self.columns if c.name == key).nullable:
                raise ValueError(f"{self.ref}: primary key column {key!r} must be NOT NULL")


def sql_string(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def create_table_sql(spec: TableSpec, cfg: PlatformConfig) -> str:
    """CREATE TABLE IF NOT EXISTS for Unity Catalog, or a plain Delta table for the local runtime.

    Primary keys (informational, used by feature tables and BI tools) and liquid clustering are
    Unity Catalog / Databricks features, so the local dialect leaves them out.
    """
    defs = []
    for c in spec.columns:
        d = f"`{c.name}` {c.type}" + ("" if c.nullable else " NOT NULL")
        if c.comment:
            d += f" COMMENT {sql_string(c.comment)}"
        defs.append(d)
    if spec.primary_key and not cfg.is_local:
        keys = ", ".join(f"`{k}`" + (" TIMESERIES" if k == spec.timeseries_key else "") for k in spec.primary_key)
        defs.append(f"CONSTRAINT `pk_{spec.name}` PRIMARY KEY ({keys})")
    sql = f"CREATE TABLE IF NOT EXISTS {cfg.table(spec.layer, spec.name)} (\n  " + ",\n  ".join(defs) + "\n) USING DELTA"
    if spec.cluster_by and not cfg.is_local:
        sql += "\nCLUSTER BY (" + ", ".join(f"`{c}`" for c in spec.cluster_by) + ")"
    sql += f"\nCOMMENT {sql_string(spec.comment)}"
    if spec.change_data_feed:
        sql += "\nTBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    return sql


def conform(df, spec: TableSpec):
    """Select exactly the contract's columns, in order, cast to the contract's types."""
    from pyspark.sql import functions as F

    missing = [n for n in spec.column_names if n not in df.columns]
    if missing:
        raise ValueError(f"{spec.ref} is missing columns {missing}")
    return df.select(*[F.col(c.name).cast(c.type).alias(c.name) for c in spec.columns])


def data_dictionary(specs: list[TableSpec]) -> str:
    """Markdown data dictionary (docs/data-dictionary.md is generated from this and checked in CI)."""
    out = ["# Data dictionary", "", "Generated from `src/bronze_to_served/stagedoor/tables.py` by "
           "`python -m bronze_to_served.stagedoor.tables`. Do not edit by hand.", ""]
    for layer in ("bronze", "silver", "gold", "ml", "ops"):
        layer_specs = [s for s in specs if s.layer == layer]
        if not layer_specs:
            continue
        out += [f"## {layer.capitalize()}", ""]
        for s in layer_specs:
            extras = []
            if s.primary_key:
                extras.append("primary key " + ", ".join(f"`{k}`" for k in s.primary_key)
                              + (" (time series)" if s.timeseries_key else ""))
            if s.cluster_by:
                extras.append("clustered by " + ", ".join(f"`{k}`" for k in s.cluster_by))
            if s.change_data_feed:
                extras.append("change data feed on")
            out += [f"### `{s.ref}`", "", s.comment + (f" ({'; '.join(extras)}.)" if extras else ""), "",
                    "| Column | Type | Description |", "|---|---|---|"]
            out += [f"| `{c.name}` | `{c.type}`{'' if c.nullable else ' NOT NULL'} | {c.comment.replace('|', chr(92) + '|')} |"
                    for c in s.columns]
            out.append("")
    return "\n".join(out).rstrip("\n")
