"""Platform configuration: one object that knows every table, volume and checkpoint name.

Every task builds a PlatformConfig from its job parameters, so the same code runs in a per-developer
schema prefix, in staging and in production, and locally against a file-system "catalog" for tests.

Unity Catalog layout (Databricks runtime):

    <catalog>.<prefix>landing   volume `raw`          raw files land here (/Volumes/<catalog>/<prefix>landing/raw)
    <catalog>.<prefix>bronze    tables                raw records + ingestion metadata, append-only
    <catalog>.<prefix>silver    tables                typed, validated, deduplicated, merged (SCD1/SCD2)
    <catalog>.<prefix>gold      tables                star schema and business aggregates
    <catalog>.<prefix>ml        tables + models       features, labels, predictions, evaluations, churn_model
    <catalog>.<prefix>ops       tables + volumes      quarantine, quality metrics, task runs, checkpoints, exports
    <catalog>.<prefix>declarative tables              the same medallion built by Lakeflow Declarative Pipelines

Local runtime (tests, laptops): there is no catalog, so tables are `<prefix><layer>.<table>` in the
Spark session catalog and volumes are folders under `local_root`.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, replace

LAYERS = ("landing", "bronze", "silver", "gold", "ml", "ops", "declarative")
RUNTIMES = ("databricks", "local")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_ident(kind: str, value: str, allow_empty: bool = False) -> None:
    if allow_empty and value == "":
        return
    if not _IDENT.match(value):
        raise ValueError(f"{kind} must contain only letters, digits and underscores, got {value!r}")


@dataclass(frozen=True)
class PlatformConfig:
    catalog: str = "stagedoor_dev"
    schema_prefix: str = ""
    runtime: str = "databricks"
    local_root: str = ""
    landing_volume: str = "raw"
    checkpoint_volume: str = "checkpoints"
    export_volume: str = "exports"
    # "availableNow" processes everything new and stops (scheduled jobs, serverless);
    # "processingTime=30 seconds" keeps the stream running (continuous jobs on classic compute).
    trigger: str = "availableNow"
    lookback_days: int = 3
    full_refresh: bool = False

    def __post_init__(self) -> None:
        _check_ident("catalog", self.catalog)
        _check_ident("schema_prefix", self.schema_prefix, allow_empty=True)
        _check_ident("landing_volume", self.landing_volume)
        _check_ident("checkpoint_volume", self.checkpoint_volume)
        if self.runtime not in RUNTIMES:
            raise ValueError(f"runtime must be one of {RUNTIMES}, got {self.runtime!r}")
        if self.runtime == "local" and not self.local_root:
            raise ValueError("the local runtime needs local_root (a folder for tables, volumes and checkpoints)")
        if self.lookback_days < 0:
            raise ValueError("lookback_days must be >= 0")
        if not (self.trigger == "availableNow" or self.trigger.startswith("processingTime=")):
            raise ValueError("trigger must be 'availableNow' or 'processingTime=<interval>'")

    @property
    def is_local(self) -> bool:
        return self.runtime == "local"

    def with_overrides(self, **changes) -> "PlatformConfig":
        return replace(self, **changes)

    # ---------- Unity Catalog names ----------
    def schema_name(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        return f"{self.schema_prefix}{layer}"

    def schema(self, layer: str) -> str:
        """Quoted schema identifier: `catalog`.`schema` on Databricks, `schema` locally."""
        if self.is_local:
            return f"`{self.schema_name(layer)}`"
        return f"`{self.catalog}`.`{self.schema_name(layer)}`"

    def table(self, layer: str, name: str) -> str:
        """Quoted table identifier, usable in SQL, spark.table(), toTable() and DeltaTable.forName()."""
        _check_ident("table name", name)
        return f"{self.schema(layer)}.`{name}`"

    def table_ref(self, ref: str) -> str:
        """Resolve a lineage reference such as 'silver.orders' to a quoted table identifier."""
        layer, _, name = ref.partition(".")
        if not name:
            raise ValueError(f"expected '<layer>.<table>', got {ref!r}")
        return self.table(layer, name)

    def model_name(self, name: str) -> str:
        """Three-level Unity Catalog model name (MLflow wants it unquoted)."""
        _check_ident("model name", name)
        if self.is_local:
            return f"{self.schema_name('ml')}.{name}"
        return f"{self.catalog}.{self.schema_name('ml')}.{name}"

    def function(self, layer: str, name: str) -> str:
        return self.table(layer, name)

    # ---------- volumes and checkpoints ----------
    def volume_path(self, layer: str, volume: str, *parts: str) -> str:
        _check_ident("volume", volume)
        schema = self.schema_name(layer)
        if self.is_local:
            return os.path.join(self.local_root, "volumes", schema, volume, *parts)
        return "/".join(["/Volumes", self.catalog, schema, volume, *parts])

    def landing_path(self, *parts: str) -> str:
        return self.volume_path("landing", self.landing_volume, *parts)

    def checkpoint_path(self, stream: str) -> str:
        return self.volume_path("ops", self.checkpoint_volume, stream)

    def export_path(self, *parts: str) -> str:
        return self.volume_path("ops", self.export_volume, *parts)

    @property
    def warehouse_dir(self) -> str:
        return os.path.join(self.local_root, "warehouse")

    @classmethod
    def local(cls, root: str, **overrides) -> "PlatformConfig":
        """A config for the local runtime rooted at `root` (tests and laptops)."""
        return cls(runtime="local", local_root=os.path.abspath(root), catalog="local", **overrides)
