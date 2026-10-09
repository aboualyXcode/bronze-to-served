/* Generated from designer/templates/*.json by designer/scripts/build.js. Do not edit. */
(function (root) {
  'use strict';
  root.B2S = root.B2S || {};
  root.B2S.TEMPLATES = [
 {
  "file": "designer/templates/declarative_refresh.json",
  "design": {
   "version": 1,
   "job": {
    "key": "stagedoor_declarative_refresh",
    "name": "stagedoor-declarative-refresh",
    "description": "The declarative variant: refresh the Lakeflow Declarative Pipeline whenever new clickstream files land (file arrival trigger).",
    "trigger": {
     "type": "file_arrival",
     "url": "/Volumes/${var.catalog}/${var.schema_prefix}landing/raw/clickstream/",
     "min_seconds": 300
    },
    "compute": "serverless",
    "max_concurrent_runs": 1,
    "tags": {
     "project": "bronze-to-served",
     "domain": "declarative"
    },
    "notifications": {
     "on_failure": [
      "${var.alert_email}"
     ]
    },
    "parameters": [
     {
      "name": "catalog",
      "default": "${var.catalog}"
     },
     {
      "name": "schema_prefix",
      "default": "${var.schema_prefix}"
     }
    ]
   },
   "nodes": [
    {
     "id": "refresh",
     "component": "pipeline.declarative",
     "task_key": "refresh_declarative_pipeline"
    }
   ],
   "edges": []
  }
 },
 {
  "file": "designer/templates/maintenance.json",
  "design": {
   "version": 1,
   "job": {
    "key": "stagedoor_maintenance",
    "name": "stagedoor-maintenance",
    "description": "Weekly OPTIMIZE, VACUUM and ANALYZE for the busiest tables, four at a time with a for-each task. Not needed where predictive optimization is on.",
    "trigger": {
     "type": "schedule",
     "cron": "0 0 5 ? * SUN",
     "timezone": "UTC"
    },
    "compute": "serverless",
    "max_concurrent_runs": 1,
    "timeout_seconds": 7200,
    "tags": {
     "project": "bronze-to-served",
     "domain": "operations"
    },
    "notifications": {
     "on_failure": [
      "${var.alert_email}"
     ]
    },
    "parameters": [
     {
      "name": "catalog",
      "default": "${var.catalog}"
     },
     {
      "name": "schema_prefix",
      "default": "${var.schema_prefix}"
     }
    ]
   },
   "nodes": [
    {
     "id": "maintain",
     "component": "ops.maintain_table",
     "task_key": "maintain_tables",
     "params": {
      "table": "{{input}}",
      "operations": "optimize,vacuum,analyze"
     },
     "for_each": {
      "inputs": "[\"silver.orders\", \"silver.order_items\", \"silver.customers\", \"silver.clickstream\", \"gold.fct_sales\", \"gold.agg_daily_sales\", \"gold.customer_360\", \"gold.funnel_daily\", \"ml.customer_features\"]",
      "concurrency": 4
     }
    }
   ],
   "edges": []
  }
 },
 {
  "file": "designer/templates/ml_rollback.json",
  "design": {
   "version": 1,
   "job": {
    "key": "stagedoor_ml_rollback",
    "name": "stagedoor-ml-rollback",
    "description": "Manual: move @champion back to @previous_champion and point the serving endpoint at it.",
    "trigger": {
     "type": "manual"
    },
    "compute": "serverless",
    "max_concurrent_runs": 1,
    "tags": {
     "project": "bronze-to-served",
     "domain": "ml"
    },
    "notifications": {
     "on_failure": [
      "${var.alert_email}"
     ]
    },
    "parameters": [
     {
      "name": "catalog",
      "default": "${var.catalog}"
     },
     {
      "name": "schema_prefix",
      "default": "${var.schema_prefix}"
     },
     {
      "name": "deploy_endpoint",
      "default": "${var.deploy_serving_endpoint}"
     },
     {
      "name": "endpoint_name",
      "default": "${var.serving_endpoint_name}"
     }
    ]
   },
   "nodes": [
    {
     "id": "rollback",
     "component": "ml.rollback",
     "task_key": "rollback_champion"
    },
    {
     "id": "deploy",
     "component": "ml.deploy_endpoint",
     "task_key": "deploy_endpoint",
     "params": {
      "enabled": "{{job.parameters.deploy_endpoint}}",
      "endpoint_name": "{{job.parameters.endpoint_name}}"
     }
    }
   ],
   "edges": [
    {
     "from": "rollback",
     "to": "deploy"
    }
   ]
  }
 },
 {
  "file": "designer/templates/ml_training.json",
  "design": {
   "version": 1,
   "job": {
    "key": "stagedoor_ml_training",
    "name": "stagedoor-ml-training",
    "description": "Weekly: rebuild point-in-time features and labels, train a challenger, compare it with the champion, and promote and deploy it only if it wins.",
    "trigger": {
     "type": "schedule",
     "cron": "0 0 4 ? * SUN",
     "timezone": "UTC"
    },
    "compute": "serverless",
    "max_concurrent_runs": 1,
    "timeout_seconds": 10800,
    "tags": {
     "project": "bronze-to-served",
     "domain": "ml"
    },
    "notifications": {
     "on_failure": [
      "${var.alert_email}"
     ]
    },
    "parameters": [
     {
      "name": "catalog",
      "default": "${var.catalog}"
     },
     {
      "name": "schema_prefix",
      "default": "${var.schema_prefix}"
     },
     {
      "name": "experiment_path",
      "default": "${var.experiment_path}"
     },
     {
      "name": "deploy_endpoint",
      "default": "${var.deploy_serving_endpoint}"
     },
     {
      "name": "endpoint_name",
      "default": "${var.serving_endpoint_name}"
     }
    ]
   },
   "nodes": [
    {
     "id": "features",
     "component": "ml.features",
     "task_key": "features_history",
     "params": {
      "mode": "training"
     }
    },
    {
     "id": "tset",
     "component": "ml.training_set",
     "task_key": "training_set"
    },
    {
     "id": "train",
     "component": "ml.train",
     "task_key": "train",
     "params": {
      "experiment_path": "{{job.parameters.experiment_path}}"
     },
     "timeout_seconds": 3600
    },
    {
     "id": "evaluate",
     "component": "ml.evaluate",
     "task_key": "evaluate"
    },
    {
     "id": "report",
     "component": "notebook.model_validation",
     "task_key": "validation_report"
    },
    {
     "id": "gate",
     "component": "control.condition",
     "task_key": "promotion_gate",
     "condition": {
      "op": "EQUAL_TO",
      "left": "{{tasks.validation_report.values.promote}}",
      "right": "true"
     }
    },
    {
     "id": "promote",
     "component": "ml.promote",
     "task_key": "promote"
    },
    {
     "id": "deploy",
     "component": "ml.deploy_endpoint",
     "task_key": "deploy_endpoint",
     "params": {
      "enabled": "{{job.parameters.deploy_endpoint}}",
      "endpoint_name": "{{job.parameters.endpoint_name}}"
     }
    }
   ],
   "edges": [
    {
     "from": "features",
     "to": "tset"
    },
    {
     "from": "tset",
     "to": "train"
    },
    {
     "from": "train",
     "to": "evaluate"
    },
    {
     "from": "evaluate",
     "to": "report"
    },
    {
     "from": "report",
     "to": "gate"
    },
    {
     "from": "gate",
     "to": "promote",
     "outcome": "true"
    },
    {
     "from": "promote",
     "to": "deploy"
    }
   ]
  }
 },
 {
  "file": "designer/templates/platform_daily.json",
  "design": {
   "version": 1,
   "job": {
    "key": "stagedoor_platform",
    "name": "stagedoor-platform",
    "description": "Daily lakehouse run: land, Bronze, Silver, quality gate, Gold, features, batch scoring, drift and the CRM export.",
    "trigger": {
     "type": "schedule",
     "cron": "0 0 2 * * ?",
     "timezone": "UTC"
    },
    "compute": "serverless",
    "max_concurrent_runs": 1,
    "timeout_seconds": 10800,
    "tags": {
     "project": "bronze-to-served",
     "domain": "platform"
    },
    "notifications": {
     "on_failure": [
      "${var.alert_email}"
     ]
    },
    "parameters": [
     {
      "name": "catalog",
      "default": "${var.catalog}"
     },
     {
      "name": "schema_prefix",
      "default": "${var.schema_prefix}"
     },
     {
      "name": "lookback_days",
      "default": "3"
     },
     {
      "name": "full_refresh",
      "default": "false"
     },
     {
      "name": "simulate_upstream",
      "default": "${var.simulate_upstream}"
     }
    ]
   },
   "nodes": [
    {
     "id": "land",
     "component": "land.sample_data",
     "task_key": "simulate_upstream_drop",
     "params": {
      "mode": "next_day",
      "enabled": "{{job.parameters.simulate_upstream}}"
     }
    },
    {
     "id": "b_orders",
     "component": "bronze.orders",
     "task_key": "bronze_orders",
     "params": {
      "full_refresh": "{{job.parameters.full_refresh}}"
     },
     "max_retries": 1
    },
    {
     "id": "b_customers",
     "component": "bronze.customers_cdc",
     "task_key": "bronze_customers",
     "params": {
      "full_refresh": "{{job.parameters.full_refresh}}"
     },
     "max_retries": 1
    },
    {
     "id": "b_clicks",
     "component": "bronze.clickstream",
     "task_key": "bronze_clickstream",
     "params": {
      "full_refresh": "{{job.parameters.full_refresh}}"
     },
     "max_retries": 1
    },
    {
     "id": "b_reference",
     "component": "bronze.reference",
     "task_key": "bronze_reference",
     "params": {
      "full_refresh": "{{job.parameters.full_refresh}}"
     },
     "max_retries": 1
    },
    {
     "id": "s_orders",
     "component": "silver.orders",
     "task_key": "silver_orders",
     "params": {
      "full_refresh": "{{job.parameters.full_refresh}}"
     },
     "max_retries": 1
    },
    {
     "id": "s_customers",
     "component": "silver.customers",
     "task_key": "silver_customers",
     "params": {
      "full_refresh": "{{job.parameters.full_refresh}}"
     },
     "max_retries": 1
    },
    {
     "id": "s_clicks",
     "component": "silver.clickstream",
     "task_key": "silver_clickstream",
     "params": {
      "full_refresh": "{{job.parameters.full_refresh}}"
     },
     "max_retries": 1
    },
    {
     "id": "s_reference",
     "component": "silver.reference",
     "task_key": "silver_reference"
    },
    {
     "id": "gate",
     "component": "quality.gate",
     "task_key": "quality_gate"
    },
    {
     "id": "g_dims",
     "component": "gold.dimensions",
     "task_key": "gold_dimensions"
    },
    {
     "id": "g_sales",
     "component": "gold.sales",
     "task_key": "gold_sales",
     "params": {
      "lookback_days": "{{job.parameters.lookback_days}}",
      "full_refresh": "{{job.parameters.full_refresh}}"
     }
    },
    {
     "id": "g_events",
     "component": "gold.event_performance",
     "task_key": "gold_event_performance"
    },
    {
     "id": "g_360",
     "component": "gold.customer_360",
     "task_key": "gold_customer_360"
    },
    {
     "id": "g_funnel",
     "component": "gold.funnel",
     "task_key": "gold_funnel",
     "params": {
      "lookback_days": "{{job.parameters.lookback_days}}",
      "full_refresh": "{{job.parameters.full_refresh}}"
     }
    },
    {
     "id": "features",
     "component": "ml.features",
     "task_key": "ml_features",
     "params": {
      "mode": "scoring"
     }
    },
    {
     "id": "score",
     "component": "ml.batch_inference",
     "task_key": "batch_scoring"
    },
    {
     "id": "drift",
     "component": "ml.drift",
     "task_key": "drift_monitor"
    },
    {
     "id": "crm",
     "component": "serve.crm_export",
     "task_key": "crm_export"
    }
   ],
   "edges": [
    {
     "from": "land",
     "to": "b_orders"
    },
    {
     "from": "land",
     "to": "b_customers"
    },
    {
     "from": "land",
     "to": "b_clicks"
    },
    {
     "from": "land",
     "to": "b_reference"
    },
    {
     "from": "b_orders",
     "to": "s_orders"
    },
    {
     "from": "b_customers",
     "to": "s_customers"
    },
    {
     "from": "b_clicks",
     "to": "s_clicks"
    },
    {
     "from": "b_reference",
     "to": "s_reference"
    },
    {
     "from": "s_orders",
     "to": "gate"
    },
    {
     "from": "s_customers",
     "to": "gate"
    },
    {
     "from": "s_clicks",
     "to": "gate"
    },
    {
     "from": "s_reference",
     "to": "gate"
    },
    {
     "from": "gate",
     "to": "g_dims"
    },
    {
     "from": "g_dims",
     "to": "g_sales"
    },
    {
     "from": "g_sales",
     "to": "g_events"
    },
    {
     "from": "g_sales",
     "to": "g_360"
    },
    {
     "from": "gate",
     "to": "g_funnel"
    },
    {
     "from": "g_sales",
     "to": "features"
    },
    {
     "from": "features",
     "to": "score"
    },
    {
     "from": "score",
     "to": "drift"
    },
    {
     "from": "score",
     "to": "crm"
    }
   ]
  }
 },
 {
  "file": "designer/templates/setup.json",
  "design": {
   "version": 1,
   "job": {
    "key": "stagedoor_setup",
    "name": "stagedoor-setup",
    "description": "One-off and idempotent: Unity Catalog objects, a year of simulated landing files, and optional governance.",
    "trigger": {
     "type": "manual"
    },
    "compute": "serverless",
    "max_concurrent_runs": 1,
    "timeout_seconds": 7200,
    "tags": {
     "project": "bronze-to-served",
     "domain": "setup"
    },
    "notifications": {
     "on_failure": [
      "${var.alert_email}"
     ]
    },
    "parameters": [
     {
      "name": "catalog",
      "default": "${var.catalog}"
     },
     {
      "name": "schema_prefix",
      "default": "${var.schema_prefix}"
     },
     {
      "name": "create_catalog",
      "default": "false"
     },
     {
      "name": "history_days",
      "default": "370"
     },
     {
      "name": "customers",
      "default": "5000"
     },
     {
      "name": "apply_governance",
      "default": "false"
     },
     {
      "name": "analysts_group",
      "default": ""
     },
     {
      "name": "scientists_group",
      "default": ""
     },
     {
      "name": "engineers_group",
      "default": ""
     }
    ]
   },
   "nodes": [
    {
     "id": "uc",
     "component": "setup.uc_objects",
     "task_key": "unity_catalog_objects",
     "params": {
      "create_catalog": "{{job.parameters.create_catalog}}"
     }
    },
    {
     "id": "land",
     "component": "land.sample_data",
     "task_key": "land_history",
     "params": {
      "mode": "history",
      "history_days": "{{job.parameters.history_days}}",
      "customers": "{{job.parameters.customers}}"
     }
    },
    {
     "id": "cond",
     "component": "control.condition",
     "task_key": "governance_enabled",
     "condition": {
      "op": "EQUAL_TO",
      "left": "{{job.parameters.apply_governance}}",
      "right": "true"
     }
    },
    {
     "id": "gov",
     "component": "setup.governance",
     "task_key": "apply_governance",
     "params": {
      "analysts_group": "{{job.parameters.analysts_group}}",
      "scientists_group": "{{job.parameters.scientists_group}}",
      "engineers_group": "{{job.parameters.engineers_group}}"
     }
    }
   ],
   "edges": [
    {
     "from": "uc",
     "to": "land"
    },
    {
     "from": "land",
     "to": "cond"
    },
    {
     "from": "cond",
     "to": "gov",
     "outcome": "true"
    }
   ]
  }
 },
 {
  "file": "designer/templates/streaming_continuous.json",
  "design": {
   "version": 1,
   "job": {
    "key": "stagedoor_streaming",
    "name": "stagedoor-streaming",
    "description": "Low-latency variant: clickstream Bronze, Silver and live engagement as always-on streams with processing-time triggers on classic compute. Pause the daily job's clickstream tasks while this runs.",
    "trigger": {
     "type": "continuous"
    },
    "compute": "job_cluster",
    "max_concurrent_runs": 1,
    "tags": {
     "project": "bronze-to-served",
     "domain": "streaming"
    },
    "notifications": {
     "on_failure": [
      "${var.alert_email}"
     ]
    },
    "parameters": [
     {
      "name": "catalog",
      "default": "${var.catalog}"
     },
     {
      "name": "schema_prefix",
      "default": "${var.schema_prefix}"
     }
    ]
   },
   "nodes": [
    {
     "id": "bronze",
     "component": "bronze.clickstream",
     "task_key": "clickstream_bronze_stream",
     "params": {
      "trigger": "processingTime=30 seconds"
     }
    },
    {
     "id": "silver",
     "component": "silver.clickstream",
     "task_key": "clickstream_silver_stream",
     "params": {
      "trigger": "processingTime=30 seconds"
     }
    },
    {
     "id": "live",
     "component": "gold.live_engagement",
     "task_key": "live_engagement_stream",
     "params": {
      "trigger": "processingTime=1 minute"
     }
    }
   ],
   "edges": []
  }
 }
];
})(typeof window !== 'undefined' ? window : globalThis);
