"""Unity Catalog objects as idempotent code: run it twice and nothing changes the second time."""
from __future__ import annotations

import logging
import os

from ..config import LAYERS, PlatformConfig
from .contracts import TableSpec, create_table_sql, sql_string

log = logging.getLogger(__name__)

SCHEMA_COMMENTS = {
    "landing": "Raw files from upstream systems, in the `raw` volume.",
    "bronze": "Raw records with ingestion metadata. Append-only and replayable.",
    "silver": "Typed, validated, deduplicated and merged records (SCD1 and SCD2).",
    "gold": "Star schema and business aggregates for BI, apps and ML.",
    "ml": "Feature tables, labels, training sets, predictions, evaluations and the churn model.",
    "ops": "Quarantine, quality metrics, task runs, stream checkpoints and exports.",
    "declarative": "The same medallion built by Lakeflow Declarative Pipelines.",
}


def ensure_catalog(spark, cfg: PlatformConfig, create: bool) -> None:
    if not cfg.is_local and create:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cfg.catalog}`")


def ensure_schemas(spark, cfg: PlatformConfig) -> None:
    for layer in LAYERS:
        comment = "" if cfg.is_local else f" COMMENT {sql_string(SCHEMA_COMMENTS[layer])}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}{comment}")


def ensure_volumes(spark, cfg: PlatformConfig) -> None:
    volumes = [("landing", cfg.landing_volume, "Raw files dropped by upstream systems."),
               ("ops", cfg.checkpoint_volume, "Structured Streaming checkpoints and Auto Loader schemas."),
               ("ops", cfg.export_volume, "Files exported to downstream systems (reverse ETL).")]
    for layer, volume, comment in volumes:
        if cfg.is_local:
            os.makedirs(cfg.volume_path(layer, volume), exist_ok=True)
        else:
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema(layer)}.`{volume}` COMMENT {sql_string(comment)}")


def ensure_table(spark, cfg: PlatformConfig, spec: TableSpec) -> None:
    spark.sql(create_table_sql(spec, cfg))
    if spec.checks:
        table = cfg.table(spec.layer, spec.name)
        existing = {r["key"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        for name, expression in spec.checks:
            if f"delta.constraints.{name.lower()}" not in existing:
                spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expression})")


def grant(spark, securable: str, privileges: list[str], principal: str) -> None:
    spark.sql(f"GRANT {', '.join(privileges)} ON {securable} TO `{principal}`")
    log.info("granted %s on %s to %s", privileges, securable, principal)


def set_table_tags(spark, table: str, tags: dict[str, str]) -> None:
    pairs = ", ".join(f"{sql_string(k)} = {sql_string(v)}" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {table} SET TAGS ({pairs})")


def set_column_tags(spark, table: str, column: str, tags: dict[str, str]) -> None:
    pairs = ", ".join(f"{sql_string(k)} = {sql_string(v)}" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{column}` SET TAGS ({pairs})")
