"""The component catalog: every task the platform can run, with its lineage and parameters.

It is the single source of truth for the `b2s` CLI (what --task values exist and which function runs), the
visual pipeline designer (its palette, generated into designer/js/catalog.js) and the bundle tests (every
job task must name a component here). Pure data: importing it never needs Spark.

Lineage: tables are 'layer.table', landing folders 'landing/<source>', models 'model:<name>@<alias>'.
A read ending in '?' is informational only and is not used to wire dependencies automatically.
"""
from __future__ import annotations

import importlib
import json
import sys
from dataclasses import asdict, dataclass

LAYERS = (  # display order and colours in the designer
    ("setup", "Setup", "#64748b"), ("landing", "Landing", "#0e7490"), ("bronze", "Bronze", "#a0612b"),
    ("silver", "Silver", "#6b7a8c"), ("quality", "Quality", "#b45309"), ("gold", "Gold", "#b8901c"),
    ("ml", "ML", "#7c3aed"), ("serving", "Serving", "#0f766e"), ("declarative", "Declarative", "#2563eb"),
    ("control", "Control flow", "#475569"), ("ops", "Operations", "#57534e"),
)


@dataclass(frozen=True)
class Param:
    name: str
    default: str = ""
    description: str = ""


@dataclass(frozen=True)
class Component:
    id: str
    title: str
    layer: str
    kind: str                     # python | notebook | condition | pipeline
    summary: str
    impl: str = ""                # python: "module:function"
    path: str = ""                # notebook: repo-relative path without extension; pipeline: bundle resource key
    reads: tuple = ()
    writes: tuple = ()
    params: tuple = ()
    environment: str = "default"  # serverless environment: default (the wheel) or ml (+ scikit-learn, mlflow)
    streaming: bool = False
    capabilities: tuple = ()


P = Param
S = "bronze_to_served.stagedoor"
M = "bronze_to_served.ml.jobs"
INCREMENTAL = (P("lookback_days", "3", "Days of history recomputed on each run."),
               P("full_refresh", "false", "Rebuild from scratch."))
STREAM = (P("trigger", "availableNow", "availableNow (incremental batch) or processingTime=<interval> (continuous)."),
          P("full_refresh", "false", "Reset the checkpoint and reprocess everything."))

COMPONENTS: tuple[Component, ...] = (
    Component("setup.uc_objects", "Unity Catalog objects", "setup", "python",
              "Create schemas, volumes and every table contract with comments, keys and clustering. Idempotent.",
              f"{S}.setup:create_objects", params=(P("create_catalog", "false", "Also create the catalog."),),
              capabilities=("Unity Catalog", "Volumes", "Liquid clustering", "Constraints")),
    Component("setup.governance", "Governance", "setup", "python",
              "Grants by persona, PII tags, and a secure view with column masking and row-level filtering.",
              f"{S}.setup:apply_governance", reads=("gold.customer_360",), writes=("gold.customer_360_secure",),
              params=(P("analysts_group"), P("scientists_group"), P("engineers_group"),
                      P("pii_group", "stagedoor-pii-readers"), P("all_regions_group", "stagedoor-all-regions")),
              capabilities=("Grants", "Tags", "Dynamic views")),
    Component("land.sample_data", "Simulated upstream drop", "landing", "python",
              "Write the next day of generated files (or the whole history) into the landing volume.",
              f"{S}.setup:land_sample_data",
              writes=("landing/orders", "landing/customers_cdc", "landing/clickstream", "landing/reference"),
              params=(P("mode", "next_day", "history, next_day or all."), P("history_days", "370"),
                      P("days_per_run", "1"), P("enabled", "true", "false once real sources deliver files."),
                      P("customers", "5000"), P("days", "400"), P("clickstream_scale", "1.0")),
              capabilities=("Volumes",)),
    Component("bronze.orders", "Ingest orders", "bronze", "python",
              "Auto Loader stream of order events; unexpected fields are rescued, never fatal.",
              f"{S}.bronze:orders", reads=("landing/orders",), writes=("bronze.orders_raw",), params=STREAM,
              streaming=True, capabilities=("Auto Loader", "Structured Streaming", "Rescued data")),
    Component("bronze.customers_cdc", "Ingest CRM changes", "bronze", "python",
              "Auto Loader stream of CRM change data capture events.",
              f"{S}.bronze:customers_cdc", reads=("landing/customers_cdc",), writes=("bronze.customers_cdc_raw",),
              params=STREAM, streaming=True, capabilities=("Auto Loader", "CDC")),
    Component("bronze.clickstream", "Ingest clickstream", "bronze", "python",
              "Web and app events from files (Auto Loader) or a Kafka topic, into the same Bronze table.",
              f"{S}.bronze:clickstream", reads=("landing/clickstream",), writes=("bronze.clickstream_raw",),
              params=STREAM + (P("source", "files", "files or kafka."), P("kafka_bootstrap_servers"),
                               P("kafka_topic", "clickstream"), P("kafka_secret_scope")),
              streaming=True, capabilities=("Auto Loader", "Kafka", "Structured Streaming")),
    Component("bronze.reference", "Load reference snapshots", "bronze", "python",
              "COPY INTO for the show calendar and venue CSV snapshots: idempotent batch loads.",
              f"{S}.bronze:reference", reads=("landing/reference",), writes=("bronze.events_raw", "bronze.venues_raw"),
              params=(P("full_refresh", "false"),), capabilities=("COPY INTO",)),
    Component("silver.orders", "Orders (SCD1)", "silver", "python",
              "Validate order events, quarantine bad ones, and MERGE the latest status per order; lines exactly once.",
              f"{S}.silver:orders", reads=("bronze.orders_raw",),
              writes=("silver.orders", "silver.order_items", "ops.quarantine", "ops.quality_metrics"), params=STREAM,
              streaming=True, capabilities=("foreachBatch", "MERGE", "Expectations", "Quarantine")),
    Component("silver.customers", "Customers (SCD2)", "silver", "python",
              "Apply CRM changes in LSN order as SCD Type 2 history; deletes close the current version.",
              f"{S}.silver:customers", reads=("bronze.customers_cdc_raw",),
              writes=("silver.customers", "ops.quarantine", "ops.quality_metrics"), params=STREAM, streaming=True,
              capabilities=("CDC", "SCD Type 2", "MERGE")),
    Component("silver.clickstream", "Clickstream (exactly once)", "silver", "python",
              "Validate events and insert each event_id exactly once (duplicates and replays are absorbed).",
              f"{S}.silver:clickstream", reads=("bronze.clickstream_raw",),
              writes=("silver.clickstream", "ops.quarantine", "ops.quality_metrics"), params=STREAM, streaming=True,
              capabilities=("Insert-only MERGE", "Expectations")),
    Component("silver.reference", "Shows and venues", "silver", "python",
              "Rebuild shows and venues from their latest full snapshot.",
              f"{S}.silver:reference", reads=("bronze.events_raw", "bronze.venues_raw"),
              writes=("silver.events", "silver.venues", "ops.quality_metrics")),
    Component("quality.gate", "Quality gate", "quality", "python",
              "Fail the run before Gold if this run quarantined more than the allowed share of any table.",
              f"{S}.operations:quality_gate", reads=("ops.quality_metrics",),
              params=(P("thresholds", "{}", 'JSON, e.g. {"silver.orders": 0.05}.'),), capabilities=("Circuit breaker",)),
    Component("gold.dimensions", "Dimensions", "gold", "python",
              "dim_customer (full SCD2 history) and dim_event (shows with their venues).",
              f"{S}.gold:dimensions", reads=("silver.customers", "silver.events", "silver.venues"),
              writes=("gold.dim_customer", "gold.dim_event"), capabilities=("Star schema",)),
    Component("gold.sales", "Sales fact and daily sales", "gold", "python",
              "fct_sales at order-line grain with point-in-time customer keys; daily sales recomputed with replaceWhere.",
              f"{S}.gold:sales", reads=("silver.orders", "silver.order_items", "gold.dim_customer", "gold.dim_event"),
              writes=("gold.fct_sales", "gold.agg_daily_sales"), params=INCREMENTAL,
              capabilities=("Point-in-time joins", "replaceWhere")),
    Component("gold.event_performance", "Show performance", "gold", "python",
              "Tickets sold net of refunds, revenue and sell-through per show.",
              f"{S}.gold:event_performance", reads=("gold.fct_sales", "gold.dim_event"),
              writes=("gold.event_performance",)),
    Component("gold.customer_360", "Customer 360", "gold", "python",
              "One row per current customer: purchases, favourite genre, engagement, recency.",
              f"{S}.gold:customer_360",
              reads=("gold.fct_sales", "gold.dim_customer", "gold.dim_event", "silver.clickstream", "silver.orders"),
              writes=("gold.customer_360",)),
    Component("gold.funnel", "Conversion funnel", "gold", "python",
              "Sessions reaching view, cart, checkout and purchase, by day and device.",
              f"{S}.gold:funnel", reads=("silver.clickstream", "silver.orders"), writes=("gold.funnel_daily",),
              params=INCREMENTAL),
    Component("gold.live_engagement", "Live engagement", "gold", "python",
              "Stateful streaming: event counts in 5-minute windows with a 30-minute watermark.",
              f"{S}.gold:live_engagement", reads=("silver.clickstream",), writes=("gold.live_engagement",),
              params=STREAM, streaming=True, capabilities=("Watermarks", "Windowed aggregation")),
    Component("ml.features", "Churn features", "ml", "python",
              "Point-in-time features per customer and as-of date (scoring: latest date; training: every snapshot + labels).",
              f"{S}.features:features",
              reads=("silver.orders", "silver.customers", "silver.clickstream", "gold.fct_sales", "gold.dim_event"),
              writes=("ml.customer_features", "ml.churn_labels"),
              params=(P("mode", "scoring", "scoring or training."),), capabilities=("Feature table", "Point in time")),
    Component("ml.training_set", "Training set", "ml", "python",
              "Join features and labels; hold out the latest 20% of as-of dates as an out-of-time test set.",
              f"{S}.features:training_set", reads=("ml.customer_features", "ml.churn_labels"),
              writes=("ml.churn_training_set",), params=(P("test_share", "0.2"),)),
    Component("ml.train", "Train", "ml", "python",
              "Train the churn pipeline, log it to MLflow with lineage, register it in Unity Catalog as @challenger.",
              f"{M}:train", reads=("ml.churn_training_set",), writes=("model:churn_model@challenger",),
              params=(P("experiment_path", "", "MLflow experiment path."), P("hyperparameters", "{}", "JSON overrides.")),
              environment="ml", capabilities=("MLflow", "Unity Catalog models")),
    Component("ml.evaluate", "Evaluate", "ml", "python",
              "Score challenger and champion on the same test set; record metrics and the promotion decision.",
              f"{M}:evaluate", reads=("ml.churn_training_set", "model:churn_model@challenger", "model:churn_model@champion?"),
              writes=("ml.model_evaluations",), params=(P("policy", "{}", "PromotionPolicy overrides as JSON."),),
              environment="ml", capabilities=("Champion/challenger",)),
    Component("notebook.model_validation", "Validation report", "ml", "notebook",
              "Readable challenger-versus-champion report; sets the task value `promote` for the gate.",
              path="notebooks/model_validation", reads=("ml.model_evaluations",), writes=("taskvalue:promote",),
              capabilities=("Notebook task", "Task values")),
    Component("control.condition", "Condition", "control", "condition",
              "Branch on a task value or job parameter. Downstream tasks follow its true or false outcome.",
              capabilities=("Condition task",)),
    Component("ml.promote", "Promote", "serving", "python",
              "Move @champion to the challenger and keep the old champion as @previous_champion.",
              f"{M}:promote", reads=("model:churn_model@challenger",), writes=("model:churn_model@champion",),
              environment="ml", capabilities=("Model aliases",)),
    Component("ml.rollback", "Roll back", "serving", "python",
              "Swap @champion and @previous_champion.",
              f"{M}:rollback", reads=("model:churn_model@previous_champion?",), writes=("model:churn_model@champion",),
              environment="ml"),
    Component("ml.deploy_endpoint", "Deploy endpoint", "serving", "python",
              "Create or update the Model Serving endpoint to serve the champion (scale to zero).",
              f"{M}:deploy_endpoint", reads=("model:churn_model@champion",), writes=("serving:stagedoor-churn",),
              params=(P("enabled", "false", "true to deploy."), P("endpoint_name", "stagedoor-churn"),
                      P("workload_size", "Small")), environment="ml", capabilities=("Model Serving",)),
    Component("ml.batch_inference", "Batch scoring", "serving", "python",
              "Score the latest features with the champion (distributed with mapInPandas).",
              f"{M}:batch_inference", reads=("ml.customer_features", "model:churn_model@champion?"),
              writes=("ml.churn_predictions",), environment="ml", capabilities=("mapInPandas",)),
    Component("ml.drift", "Drift monitor", "serving", "python",
              "Population stability index of every feature: latest scoring set versus training data.",
              f"{M}:drift", reads=("ml.churn_training_set?", "ml.customer_features", "ml.churn_predictions"),
              writes=("ml.feature_drift",), params=(P("fail_on_drift", "false"),), environment="ml",
              capabilities=("Monitoring",)),
    Component("serve.crm_export", "CRM export", "serving", "python",
              "Send newly scored high-risk customers to the CRM using the change data feed and a bookmark.",
              f"{S}.operations:crm_export", reads=("ml.churn_predictions",), writes=("ops.export_bookmarks",),
              params=(P("risk_band", "high"),), capabilities=("Change data feed", "Reverse ETL")),
    Component("ops.maintain_table", "Maintain a table", "ops", "python",
              "OPTIMIZE, VACUUM and ANALYZE one table; run it over many tables with a for-each task.",
              f"{S}.operations:maintain_table",
              params=(P("table", "", "e.g. silver.orders, or {{input}} in a for-each."),
                      P("operations", "optimize,vacuum,analyze"), P("retain_hours", "168")),
              capabilities=("OPTIMIZE", "VACUUM", "For-each task")),
    Component("pipeline.declarative", "Declarative pipeline", "declarative", "pipeline",
              "Lakeflow Declarative Pipeline: clickstream and CRM CDC with expectations, AUTO CDC (SCD2) and a materialized view.",
              path="stagedoor_declarative", reads=("landing/clickstream", "landing/customers_cdc"),
              writes=("declarative.clickstream_silver", "declarative.customers_scd2", "declarative.engagement_daily"),
              capabilities=("Lakeflow Declarative Pipelines", "Expectations", "AUTO CDC")),
)

_BY_ID = {c.id: c for c in COMPONENTS}


def component(component_id: str) -> Component:
    try:
        return _BY_ID[component_id]
    except KeyError:
        raise KeyError(f"unknown component {component_id!r}; run `b2s --list`") from None


def resolve(c: Component):
    """Import the function behind a python component."""
    module, _, function = c.impl.partition(":")
    return getattr(importlib.import_module(module), function)


def export() -> dict:
    return {"layers": [{"id": i, "title": t, "color": col} for i, t, col in LAYERS],
            "components": [asdict(c) for c in COMPONENTS]}


def catalog_js() -> str:
    return ("/* Generated from src/bronze_to_served/tasks/catalog.py by `python -m bronze_to_served.tasks.catalog`.\n"
            "   Do not edit: the designer palette, the CLI and the bundle tests all read the same catalog. */\n"
            "(function (root) {\n  'use strict';\n  root.B2S = root.B2S || {};\n  root.B2S.CATALOG = "
            + json.dumps(export(), indent=1) + ";\n})(typeof window !== 'undefined' ? window : globalThis);\n")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "designer/js/catalog.js"
    with open(out, "w", encoding="utf-8") as f:
        f.write(catalog_js())
    print(f"wrote {out} ({len(COMPONENTS)} components)")
