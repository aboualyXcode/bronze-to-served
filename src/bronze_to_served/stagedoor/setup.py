"""Platform setup: Unity Catalog objects, simulated upstream drops, and governance."""
from __future__ import annotations

from ..core.uc import ensure_catalog, ensure_schemas, ensure_table, ensure_volumes, grant, set_column_tags, set_table_tags
from ..tasks.context import TaskContext
from .datagen import GeneratorSettings, landed_through, write_days
from .tables import ALL_TABLES


def create_objects(ctx: TaskContext) -> dict:
    """Catalog (optional), schemas, volumes and every table contract. Idempotent."""
    ensure_catalog(ctx.spark, ctx.cfg, ctx.bool_param("create_catalog", False))
    ensure_schemas(ctx.spark, ctx.cfg)
    ensure_volumes(ctx.spark, ctx.cfg)
    for table in ALL_TABLES:
        ensure_table(ctx.spark, ctx.cfg, table)
    return {"tables": len(ALL_TABLES)}


def land_sample_data(ctx: TaskContext) -> dict:
    """Stand in for the upstream systems: write simulated files into the landing volume.

    mode=history writes the first `history_days` days (setup); mode=next_day writes the next `days_per_run`
    days after what is already landed (each scheduled run); mode=all writes the whole timeline. Set
    enabled=false (or delete the task) once real sources deliver files.
    """
    if not ctx.bool_param("enabled", True):
        return {"skipped": "simulation disabled"}
    settings = GeneratorSettings(customers=ctx.int_param("customers", 5000), days=ctx.int_param("days", 400),
                                 seed=ctx.int_param("seed", 20250101),
                                 clickstream_scale=ctx.float_param("clickstream_scale", 1.0))
    root = ctx.cfg.landing_path()
    done = landed_through(root)
    mode = ctx.param("mode", "next_day")
    if mode == "history":
        first, last = 0, ctx.int_param("history_days", 370) - 1
        if done is not None and done >= last:
            return {"skipped": f"history already landed through day {done}"}
    elif mode == "next_day":
        first = 0 if done is None else done + 1
        last = first + ctx.int_param("days_per_run", 1) - 1
    elif mode == "all":
        first, last = 0, settings.days - 1
    else:
        raise ValueError(f"unknown mode {mode!r}")
    if first >= settings.days:
        return {"skipped": "the simulated timeline is fully landed"}
    paths = write_days(settings, root, first, last)
    return {"files": len(paths), "first_day": settings.day_date(first).isoformat(),
            "last_day": settings.day_date(min(last, settings.days - 1)).isoformat()}


def apply_governance(ctx: TaskContext) -> dict:
    """Grants by persona, PII tags, and a secure view with masking and row-level filtering.

    Groups come from parameters; empty ones are skipped. Row filters and column masks applied directly to
    tables are shown in notebooks/04_governance_and_operations.py on a copy of the table.
    """
    if ctx.cfg.is_local:
        return {"skipped": "governance needs Unity Catalog"}
    spark, cfg = ctx.spark, ctx.cfg
    analysts, scientists, engineers = (ctx.param("analysts_group", ""), ctx.param("scientists_group", ""),
                                       ctx.param("engineers_group", ""))
    pii_readers, all_regions = ctx.param("pii_group", "stagedoor-pii-readers"), ctx.param("all_regions_group", "stagedoor-all-regions")
    catalog = f"CATALOG `{cfg.catalog}`"
    done = []
    for group, layers, privileges in ((analysts, ("gold",), ["USE SCHEMA", "SELECT"]),
                                      (scientists, ("silver", "gold"), ["USE SCHEMA", "SELECT"]),
                                      (scientists, ("ml",), ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE", "CREATE MODEL"]),
                                      (engineers, ("bronze", "silver", "gold", "ops"), ["ALL PRIVILEGES"])):
        if not group:
            continue
        grant(spark, catalog, ["USE CATALOG"], group)
        for layer in layers:
            grant(spark, f"SCHEMA {cfg.schema(layer)}", privileges, group)
            done.append(f"{group}:{layer}")
    for ref in ("silver.customers", "gold.dim_customer", "gold.customer_360"):
        set_table_tags(spark, ctx.table(ref), {"contains_pii": "true", "domain": "customer"})
        for column in ("email", "full_name"):
            set_column_tags(spark, ctx.table(ref), column, {"pii": column})
    spark.sql(f"""
        CREATE OR REPLACE VIEW {ctx.table('gold.customer_360_secure')}
        COMMENT 'customer_360 for broad access: emails masked unless you are in {pii_readers}; rows limited to
                 your regions unless you are in {all_regions}.'
        AS SELECT customer_id,
                  CASE WHEN is_account_group_member('{pii_readers}') THEN email
                       ELSE regexp_replace(email, '^[^@]+', '***') END AS email,
                  country, city, loyalty_tier, lifetime_orders, lifetime_net_revenue, favorite_genre,
                  days_since_last_purchase, as_of_date
           FROM {ctx.table('gold.customer_360')}
           WHERE is_account_group_member('{all_regions}')
              OR is_account_group_member(concat('stagedoor-region-', lower(country)))""")
    return {"grants": done, "secure_view": "gold.customer_360_secure"}
