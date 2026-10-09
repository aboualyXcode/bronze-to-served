/* Generated from src/bronze_to_served/tasks/catalog.py by `python -m bronze_to_served.tasks.catalog`.
   Do not edit: the designer palette, the CLI and the bundle tests all read the same catalog. */
(function (root) {
  'use strict';
  root.B2S = root.B2S || {};
  root.B2S.CATALOG = {
 "layers": [
  {
   "id": "setup",
   "title": "Setup",
   "color": "#64748b"
  },
  {
   "id": "landing",
   "title": "Landing",
   "color": "#0e7490"
  },
  {
   "id": "bronze",
   "title": "Bronze",
   "color": "#a0612b"
  },
  {
   "id": "silver",
   "title": "Silver",
   "color": "#6b7a8c"
  },
  {
   "id": "quality",
   "title": "Quality",
   "color": "#b45309"
  },
  {
   "id": "gold",
   "title": "Gold",
   "color": "#b8901c"
  },
  {
   "id": "ml",
   "title": "ML",
   "color": "#7c3aed"
  },
  {
   "id": "serving",
   "title": "Serving",
   "color": "#0f766e"
  },
  {
   "id": "declarative",
   "title": "Declarative",
   "color": "#2563eb"
  },
  {
   "id": "control",
   "title": "Control flow",
   "color": "#475569"
  },
  {
   "id": "ops",
   "title": "Operations",
   "color": "#57534e"
  }
 ],
 "components": [
  {
   "id": "setup.uc_objects",
   "title": "Unity Catalog objects",
   "layer": "setup",
   "kind": "python",
   "summary": "Create schemas, volumes and every table contract with comments, keys and clustering. Idempotent.",
   "impl": "bronze_to_served.stagedoor.setup:create_objects",
   "path": "",
   "reads": [],
   "writes": [],
   "params": [
    {
     "name": "create_catalog",
     "default": "false",
     "description": "Also create the catalog."
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Unity Catalog",
    "Volumes",
    "Liquid clustering",
    "Constraints"
   ]
  },
  {
   "id": "setup.governance",
   "title": "Governance",
   "layer": "setup",
   "kind": "python",
   "summary": "Grants by persona, PII tags, and a secure view with column masking and row-level filtering.",
   "impl": "bronze_to_served.stagedoor.setup:apply_governance",
   "path": "",
   "reads": [
    "gold.customer_360"
   ],
   "writes": [
    "gold.customer_360_secure"
   ],
   "params": [
    {
     "name": "analysts_group",
     "default": "",
     "description": ""
    },
    {
     "name": "scientists_group",
     "default": "",
     "description": ""
    },
    {
     "name": "engineers_group",
     "default": "",
     "description": ""
    },
    {
     "name": "pii_group",
     "default": "stagedoor-pii-readers",
     "description": ""
    },
    {
     "name": "all_regions_group",
     "default": "stagedoor-all-regions",
     "description": ""
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Grants",
    "Tags",
    "Dynamic views"
   ]
  },
  {
   "id": "land.sample_data",
   "title": "Simulated upstream drop",
   "layer": "landing",
   "kind": "python",
   "summary": "Write the next day of generated files (or the whole history) into the landing volume.",
   "impl": "bronze_to_served.stagedoor.setup:land_sample_data",
   "path": "",
   "reads": [],
   "writes": [
    "landing/orders",
    "landing/customers_cdc",
    "landing/clickstream",
    "landing/reference"
   ],
   "params": [
    {
     "name": "mode",
     "default": "next_day",
     "description": "history, next_day or all."
    },
    {
     "name": "history_days",
     "default": "370",
     "description": ""
    },
    {
     "name": "days_per_run",
     "default": "1",
     "description": ""
    },
    {
     "name": "enabled",
     "default": "true",
     "description": "false once real sources deliver files."
    },
    {
     "name": "customers",
     "default": "5000",
     "description": ""
    },
    {
     "name": "days",
     "default": "400",
     "description": ""
    },
    {
     "name": "clickstream_scale",
     "default": "1.0",
     "description": ""
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Volumes"
   ]
  },
  {
   "id": "bronze.orders",
   "title": "Ingest orders",
   "layer": "bronze",
   "kind": "python",
   "summary": "Auto Loader stream of order events; unexpected fields are rescued, never fatal.",
   "impl": "bronze_to_served.stagedoor.bronze:orders",
   "path": "",
   "reads": [
    "landing/orders"
   ],
   "writes": [
    "bronze.orders_raw"
   ],
   "params": [
    {
     "name": "trigger",
     "default": "availableNow",
     "description": "availableNow (incremental batch) or processingTime=<interval> (continuous)."
    },
    {
     "name": "full_refresh",
     "default": "false",
     "description": "Reset the checkpoint and reprocess everything."
    }
   ],
   "environment": "default",
   "streaming": true,
   "capabilities": [
    "Auto Loader",
    "Structured Streaming",
    "Rescued data"
   ]
  },
  {
   "id": "bronze.customers_cdc",
   "title": "Ingest CRM changes",
   "layer": "bronze",
   "kind": "python",
   "summary": "Auto Loader stream of CRM change data capture events.",
   "impl": "bronze_to_served.stagedoor.bronze:customers_cdc",
   "path": "",
   "reads": [
    "landing/customers_cdc"
   ],
   "writes": [
    "bronze.customers_cdc_raw"
   ],
   "params": [
    {
     "name": "trigger",
     "default": "availableNow",
     "description": "availableNow (incremental batch) or processingTime=<interval> (continuous)."
    },
    {
     "name": "full_refresh",
     "default": "false",
     "description": "Reset the checkpoint and reprocess everything."
    }
   ],
   "environment": "default",
   "streaming": true,
   "capabilities": [
    "Auto Loader",
    "CDC"
   ]
  },
  {
   "id": "bronze.clickstream",
   "title": "Ingest clickstream",
   "layer": "bronze",
   "kind": "python",
   "summary": "Web and app events from files (Auto Loader) or a Kafka topic, into the same Bronze table.",
   "impl": "bronze_to_served.stagedoor.bronze:clickstream",
   "path": "",
   "reads": [
    "landing/clickstream"
   ],
   "writes": [
    "bronze.clickstream_raw"
   ],
   "params": [
    {
     "name": "trigger",
     "default": "availableNow",
     "description": "availableNow (incremental batch) or processingTime=<interval> (continuous)."
    },
    {
     "name": "full_refresh",
     "default": "false",
     "description": "Reset the checkpoint and reprocess everything."
    },
    {
     "name": "source",
     "default": "files",
     "description": "files or kafka."
    },
    {
     "name": "kafka_bootstrap_servers",
     "default": "",
     "description": ""
    },
    {
     "name": "kafka_topic",
     "default": "clickstream",
     "description": ""
    },
    {
     "name": "kafka_secret_scope",
     "default": "",
     "description": ""
    }
   ],
   "environment": "default",
   "streaming": true,
   "capabilities": [
    "Auto Loader",
    "Kafka",
    "Structured Streaming"
   ]
  },
  {
   "id": "bronze.reference",
   "title": "Load reference snapshots",
   "layer": "bronze",
   "kind": "python",
   "summary": "COPY INTO for the show calendar and venue CSV snapshots: idempotent batch loads.",
   "impl": "bronze_to_served.stagedoor.bronze:reference",
   "path": "",
   "reads": [
    "landing/reference"
   ],
   "writes": [
    "bronze.events_raw",
    "bronze.venues_raw"
   ],
   "params": [
    {
     "name": "full_refresh",
     "default": "false",
     "description": ""
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "COPY INTO"
   ]
  },
  {
   "id": "silver.orders",
   "title": "Orders (SCD1)",
   "layer": "silver",
   "kind": "python",
   "summary": "Validate order events, quarantine bad ones, and MERGE the latest status per order; lines exactly once.",
   "impl": "bronze_to_served.stagedoor.silver:orders",
   "path": "",
   "reads": [
    "bronze.orders_raw"
   ],
   "writes": [
    "silver.orders",
    "silver.order_items",
    "ops.quarantine",
    "ops.quality_metrics"
   ],
   "params": [
    {
     "name": "trigger",
     "default": "availableNow",
     "description": "availableNow (incremental batch) or processingTime=<interval> (continuous)."
    },
    {
     "name": "full_refresh",
     "default": "false",
     "description": "Reset the checkpoint and reprocess everything."
    }
   ],
   "environment": "default",
   "streaming": true,
   "capabilities": [
    "foreachBatch",
    "MERGE",
    "Expectations",
    "Quarantine"
   ]
  },
  {
   "id": "silver.customers",
   "title": "Customers (SCD2)",
   "layer": "silver",
   "kind": "python",
   "summary": "Apply CRM changes in LSN order as SCD Type 2 history; deletes close the current version.",
   "impl": "bronze_to_served.stagedoor.silver:customers",
   "path": "",
   "reads": [
    "bronze.customers_cdc_raw"
   ],
   "writes": [
    "silver.customers",
    "ops.quarantine",
    "ops.quality_metrics"
   ],
   "params": [
    {
     "name": "trigger",
     "default": "availableNow",
     "description": "availableNow (incremental batch) or processingTime=<interval> (continuous)."
    },
    {
     "name": "full_refresh",
     "default": "false",
     "description": "Reset the checkpoint and reprocess everything."
    }
   ],
   "environment": "default",
   "streaming": true,
   "capabilities": [
    "CDC",
    "SCD Type 2",
    "MERGE"
   ]
  },
  {
   "id": "silver.clickstream",
   "title": "Clickstream (exactly once)",
   "layer": "silver",
   "kind": "python",
   "summary": "Validate events and insert each event_id exactly once (duplicates and replays are absorbed).",
   "impl": "bronze_to_served.stagedoor.silver:clickstream",
   "path": "",
   "reads": [
    "bronze.clickstream_raw"
   ],
   "writes": [
    "silver.clickstream",
    "ops.quarantine",
    "ops.quality_metrics"
   ],
   "params": [
    {
     "name": "trigger",
     "default": "availableNow",
     "description": "availableNow (incremental batch) or processingTime=<interval> (continuous)."
    },
    {
     "name": "full_refresh",
     "default": "false",
     "description": "Reset the checkpoint and reprocess everything."
    }
   ],
   "environment": "default",
   "streaming": true,
   "capabilities": [
    "Insert-only MERGE",
    "Expectations"
   ]
  },
  {
   "id": "silver.reference",
   "title": "Shows and venues",
   "layer": "silver",
   "kind": "python",
   "summary": "Rebuild shows and venues from their latest full snapshot.",
   "impl": "bronze_to_served.stagedoor.silver:reference",
   "path": "",
   "reads": [
    "bronze.events_raw",
    "bronze.venues_raw"
   ],
   "writes": [
    "silver.events",
    "silver.venues",
    "ops.quality_metrics"
   ],
   "params": [],
   "environment": "default",
   "streaming": false,
   "capabilities": []
  },
  {
   "id": "quality.gate",
   "title": "Quality gate",
   "layer": "quality",
   "kind": "python",
   "summary": "Fail the run before Gold if this run quarantined more than the allowed share of any table.",
   "impl": "bronze_to_served.stagedoor.operations:quality_gate",
   "path": "",
   "reads": [
    "ops.quality_metrics"
   ],
   "writes": [],
   "params": [
    {
     "name": "thresholds",
     "default": "{}",
     "description": "JSON, e.g. {\"silver.orders\": 0.05}."
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Circuit breaker"
   ]
  },
  {
   "id": "gold.dimensions",
   "title": "Dimensions",
   "layer": "gold",
   "kind": "python",
   "summary": "dim_customer (full SCD2 history) and dim_event (shows with their venues).",
   "impl": "bronze_to_served.stagedoor.gold:dimensions",
   "path": "",
   "reads": [
    "silver.customers",
    "silver.events",
    "silver.venues"
   ],
   "writes": [
    "gold.dim_customer",
    "gold.dim_event"
   ],
   "params": [],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Star schema"
   ]
  },
  {
   "id": "gold.sales",
   "title": "Sales fact and daily sales",
   "layer": "gold",
   "kind": "python",
   "summary": "fct_sales at order-line grain with point-in-time customer keys; daily sales recomputed with replaceWhere.",
   "impl": "bronze_to_served.stagedoor.gold:sales",
   "path": "",
   "reads": [
    "silver.orders",
    "silver.order_items",
    "gold.dim_customer",
    "gold.dim_event"
   ],
   "writes": [
    "gold.fct_sales",
    "gold.agg_daily_sales"
   ],
   "params": [
    {
     "name": "lookback_days",
     "default": "3",
     "description": "Days of history recomputed on each run."
    },
    {
     "name": "full_refresh",
     "default": "false",
     "description": "Rebuild from scratch."
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Point-in-time joins",
    "replaceWhere"
   ]
  },
  {
   "id": "gold.event_performance",
   "title": "Show performance",
   "layer": "gold",
   "kind": "python",
   "summary": "Tickets sold net of refunds, revenue and sell-through per show.",
   "impl": "bronze_to_served.stagedoor.gold:event_performance",
   "path": "",
   "reads": [
    "gold.fct_sales",
    "gold.dim_event"
   ],
   "writes": [
    "gold.event_performance"
   ],
   "params": [],
   "environment": "default",
   "streaming": false,
   "capabilities": []
  },
  {
   "id": "gold.customer_360",
   "title": "Customer 360",
   "layer": "gold",
   "kind": "python",
   "summary": "One row per current customer: purchases, favourite genre, engagement, recency.",
   "impl": "bronze_to_served.stagedoor.gold:customer_360",
   "path": "",
   "reads": [
    "gold.fct_sales",
    "gold.dim_customer",
    "gold.dim_event",
    "silver.clickstream",
    "silver.orders"
   ],
   "writes": [
    "gold.customer_360"
   ],
   "params": [],
   "environment": "default",
   "streaming": false,
   "capabilities": []
  },
  {
   "id": "gold.funnel",
   "title": "Conversion funnel",
   "layer": "gold",
   "kind": "python",
   "summary": "Sessions reaching view, cart, checkout and purchase, by day and device.",
   "impl": "bronze_to_served.stagedoor.gold:funnel",
   "path": "",
   "reads": [
    "silver.clickstream",
    "silver.orders"
   ],
   "writes": [
    "gold.funnel_daily"
   ],
   "params": [
    {
     "name": "lookback_days",
     "default": "3",
     "description": "Days of history recomputed on each run."
    },
    {
     "name": "full_refresh",
     "default": "false",
     "description": "Rebuild from scratch."
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": []
  },
  {
   "id": "gold.live_engagement",
   "title": "Live engagement",
   "layer": "gold",
   "kind": "python",
   "summary": "Stateful streaming: event counts in 5-minute windows with a 30-minute watermark.",
   "impl": "bronze_to_served.stagedoor.gold:live_engagement",
   "path": "",
   "reads": [
    "silver.clickstream"
   ],
   "writes": [
    "gold.live_engagement"
   ],
   "params": [
    {
     "name": "trigger",
     "default": "availableNow",
     "description": "availableNow (incremental batch) or processingTime=<interval> (continuous)."
    },
    {
     "name": "full_refresh",
     "default": "false",
     "description": "Reset the checkpoint and reprocess everything."
    }
   ],
   "environment": "default",
   "streaming": true,
   "capabilities": [
    "Watermarks",
    "Windowed aggregation"
   ]
  },
  {
   "id": "ml.features",
   "title": "Churn features",
   "layer": "ml",
   "kind": "python",
   "summary": "Point-in-time features per customer and as-of date (scoring: latest date; training: every snapshot + labels).",
   "impl": "bronze_to_served.stagedoor.features:features",
   "path": "",
   "reads": [
    "silver.orders",
    "silver.customers",
    "silver.clickstream",
    "gold.fct_sales",
    "gold.dim_event"
   ],
   "writes": [
    "ml.customer_features",
    "ml.churn_labels"
   ],
   "params": [
    {
     "name": "mode",
     "default": "scoring",
     "description": "scoring or training."
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Feature table",
    "Point in time"
   ]
  },
  {
   "id": "ml.training_set",
   "title": "Training set",
   "layer": "ml",
   "kind": "python",
   "summary": "Join features and labels; hold out the latest 20% of as-of dates as an out-of-time test set.",
   "impl": "bronze_to_served.stagedoor.features:training_set",
   "path": "",
   "reads": [
    "ml.customer_features",
    "ml.churn_labels"
   ],
   "writes": [
    "ml.churn_training_set"
   ],
   "params": [
    {
     "name": "test_share",
     "default": "0.2",
     "description": ""
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": []
  },
  {
   "id": "ml.train",
   "title": "Train",
   "layer": "ml",
   "kind": "python",
   "summary": "Train the churn pipeline, log it to MLflow with lineage, register it in Unity Catalog as @challenger.",
   "impl": "bronze_to_served.ml.jobs:train",
   "path": "",
   "reads": [
    "ml.churn_training_set"
   ],
   "writes": [
    "model:churn_model@challenger"
   ],
   "params": [
    {
     "name": "experiment_path",
     "default": "",
     "description": "MLflow experiment path."
    },
    {
     "name": "hyperparameters",
     "default": "{}",
     "description": "JSON overrides."
    }
   ],
   "environment": "ml",
   "streaming": false,
   "capabilities": [
    "MLflow",
    "Unity Catalog models"
   ]
  },
  {
   "id": "ml.evaluate",
   "title": "Evaluate",
   "layer": "ml",
   "kind": "python",
   "summary": "Score challenger and champion on the same test set; record metrics and the promotion decision.",
   "impl": "bronze_to_served.ml.jobs:evaluate",
   "path": "",
   "reads": [
    "ml.churn_training_set",
    "model:churn_model@challenger",
    "model:churn_model@champion?"
   ],
   "writes": [
    "ml.model_evaluations"
   ],
   "params": [
    {
     "name": "policy",
     "default": "{}",
     "description": "PromotionPolicy overrides as JSON."
    }
   ],
   "environment": "ml",
   "streaming": false,
   "capabilities": [
    "Champion/challenger"
   ]
  },
  {
   "id": "notebook.model_validation",
   "title": "Validation report",
   "layer": "ml",
   "kind": "notebook",
   "summary": "Readable challenger-versus-champion report; sets the task value `promote` for the gate.",
   "impl": "",
   "path": "notebooks/model_validation",
   "reads": [
    "ml.model_evaluations"
   ],
   "writes": [
    "taskvalue:promote"
   ],
   "params": [],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Notebook task",
    "Task values"
   ]
  },
  {
   "id": "control.condition",
   "title": "Condition",
   "layer": "control",
   "kind": "condition",
   "summary": "Branch on a task value or job parameter. Downstream tasks follow its true or false outcome.",
   "impl": "",
   "path": "",
   "reads": [],
   "writes": [],
   "params": [],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Condition task"
   ]
  },
  {
   "id": "ml.promote",
   "title": "Promote",
   "layer": "serving",
   "kind": "python",
   "summary": "Move @champion to the challenger and keep the old champion as @previous_champion.",
   "impl": "bronze_to_served.ml.jobs:promote",
   "path": "",
   "reads": [
    "model:churn_model@challenger"
   ],
   "writes": [
    "model:churn_model@champion"
   ],
   "params": [],
   "environment": "ml",
   "streaming": false,
   "capabilities": [
    "Model aliases"
   ]
  },
  {
   "id": "ml.rollback",
   "title": "Roll back",
   "layer": "serving",
   "kind": "python",
   "summary": "Swap @champion and @previous_champion.",
   "impl": "bronze_to_served.ml.jobs:rollback",
   "path": "",
   "reads": [
    "model:churn_model@previous_champion?"
   ],
   "writes": [
    "model:churn_model@champion"
   ],
   "params": [],
   "environment": "ml",
   "streaming": false,
   "capabilities": []
  },
  {
   "id": "ml.deploy_endpoint",
   "title": "Deploy endpoint",
   "layer": "serving",
   "kind": "python",
   "summary": "Create or update the Model Serving endpoint to serve the champion (scale to zero).",
   "impl": "bronze_to_served.ml.jobs:deploy_endpoint",
   "path": "",
   "reads": [
    "model:churn_model@champion"
   ],
   "writes": [
    "serving:stagedoor-churn"
   ],
   "params": [
    {
     "name": "enabled",
     "default": "false",
     "description": "true to deploy."
    },
    {
     "name": "endpoint_name",
     "default": "stagedoor-churn",
     "description": ""
    },
    {
     "name": "workload_size",
     "default": "Small",
     "description": ""
    }
   ],
   "environment": "ml",
   "streaming": false,
   "capabilities": [
    "Model Serving"
   ]
  },
  {
   "id": "ml.batch_inference",
   "title": "Batch scoring",
   "layer": "serving",
   "kind": "python",
   "summary": "Score the latest features with the champion (distributed with mapInPandas).",
   "impl": "bronze_to_served.ml.jobs:batch_inference",
   "path": "",
   "reads": [
    "ml.customer_features",
    "model:churn_model@champion?"
   ],
   "writes": [
    "ml.churn_predictions"
   ],
   "params": [],
   "environment": "ml",
   "streaming": false,
   "capabilities": [
    "mapInPandas"
   ]
  },
  {
   "id": "ml.drift",
   "title": "Drift monitor",
   "layer": "serving",
   "kind": "python",
   "summary": "Population stability index of every feature: latest scoring set versus training data.",
   "impl": "bronze_to_served.ml.jobs:drift",
   "path": "",
   "reads": [
    "ml.churn_training_set?",
    "ml.customer_features",
    "ml.churn_predictions"
   ],
   "writes": [
    "ml.feature_drift"
   ],
   "params": [
    {
     "name": "fail_on_drift",
     "default": "false",
     "description": ""
    }
   ],
   "environment": "ml",
   "streaming": false,
   "capabilities": [
    "Monitoring"
   ]
  },
  {
   "id": "serve.crm_export",
   "title": "CRM export",
   "layer": "serving",
   "kind": "python",
   "summary": "Send newly scored high-risk customers to the CRM using the change data feed and a bookmark.",
   "impl": "bronze_to_served.stagedoor.operations:crm_export",
   "path": "",
   "reads": [
    "ml.churn_predictions"
   ],
   "writes": [
    "ops.export_bookmarks"
   ],
   "params": [
    {
     "name": "risk_band",
     "default": "high",
     "description": ""
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Change data feed",
    "Reverse ETL"
   ]
  },
  {
   "id": "ops.maintain_table",
   "title": "Maintain a table",
   "layer": "ops",
   "kind": "python",
   "summary": "OPTIMIZE, VACUUM and ANALYZE one table; run it over many tables with a for-each task.",
   "impl": "bronze_to_served.stagedoor.operations:maintain_table",
   "path": "",
   "reads": [],
   "writes": [],
   "params": [
    {
     "name": "table",
     "default": "",
     "description": "e.g. silver.orders, or {{input}} in a for-each."
    },
    {
     "name": "operations",
     "default": "optimize,vacuum,analyze",
     "description": ""
    },
    {
     "name": "retain_hours",
     "default": "168",
     "description": ""
    }
   ],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "OPTIMIZE",
    "VACUUM",
    "For-each task"
   ]
  },
  {
   "id": "pipeline.declarative",
   "title": "Declarative pipeline",
   "layer": "declarative",
   "kind": "pipeline",
   "summary": "Lakeflow Declarative Pipeline: clickstream and CRM CDC with expectations, AUTO CDC (SCD2) and a materialized view.",
   "impl": "",
   "path": "stagedoor_declarative",
   "reads": [
    "landing/clickstream",
    "landing/customers_cdc"
   ],
   "writes": [
    "declarative.clickstream_silver",
    "declarative.customers_scd2",
    "declarative.engagement_daily"
   ],
   "params": [],
   "environment": "default",
   "streaming": false,
   "capabilities": [
    "Lakeflow Declarative Pipelines",
    "Expectations",
    "AUTO CDC"
   ]
  }
 ]
};
})(typeof window !== 'undefined' ? window : globalThis);
