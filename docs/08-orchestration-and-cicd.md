# 8. Orchestration and CI/CD

The whole platform is one Databricks Asset Bundle (`databricks.yml`): a wheel, seven jobs, one pipeline, and three deployment targets. `databricks bundle deploy -t <target>` builds the wheel and creates or updates everything; `databricks bundle destroy` removes it.

## The jobs

| Job | Trigger | Compute | What it shows |
|---|---|---|---|
| `stagedoor_setup` | manual | serverless | idempotent setup; a condition task that runs governance only when asked |
| `stagedoor_platform` | daily, 02:00 UTC | serverless | the medallion DAG: parallel stages, retries, the quality gate, scoring, the CDF export |
| `stagedoor_ml_training` | weekly, Sunday 04:00 UTC | serverless with an ML environment | a notebook task, a task value, a condition task, promotion and deployment |
| `stagedoor_streaming` | continuous (deployed paused) | job cluster | processing-time streams as always-on tasks |
| `stagedoor_maintenance` | weekly, Sunday 05:00 UTC | serverless | a for-each task over nine tables, four at a time |
| `stagedoor_declarative_refresh` | when a clickstream file lands | serverless | a file arrival trigger running a pipeline task |
| `stagedoor_ml_rollback` | manual | serverless | rolling the champion and the endpoint back |

Every job is generated from a design in `designer/templates/` ([chapter 9](09-pipeline-designer.md)).

## Task types

| Type | Used for | Example |
|---|---|---|
| `python_wheel_task` | every platform task | `entry_point: b2s`, `named_parameters: {task: silver.orders, ...}` |
| `notebook_task` | a report people read | `validation_report`, which sets a task value |
| `condition_task` | branching | `promotion_gate`, `governance_enabled` |
| `pipeline_task` | running a declarative pipeline | `refresh_declarative_pipeline` |
| `for_each_task` | the same task over a list | `maintain_tables` over nine tables |

## Parameters and references

Job parameters (`catalog`, `schema_prefix`, `lookback_days`, `full_refresh`, `simulate_upstream`, ...) default to bundle variables and can be overridden per run, for example a backfill with `full_refresh=true`. Tasks receive them through dynamic value references:

| Reference | Resolves to |
|---|---|
| `{{job.parameters.catalog}}` | the run's job parameter |
| `{{job.run_id}}` | the run id, which ties every `ops` row of a run together |
| `{{tasks.validation_report.values.promote}}` | a value set by an upstream task |
| `{{input}}` | the current item of a for-each task |

Wheel tasks receive named parameters as `--name=value`. `b2s` maps the platform ones (catalog, prefix, trigger, lookback, full refresh) to `PlatformConfig` and passes the rest to the task, and it tolerates parameters a task does not use.

## Reliability settings

Every job runs one instance at a time (`max_concurrent_runs: 1`, with queueing), because streams own their checkpoints and MERGE targets. Ingestion and Silver tasks retry once after a minute, training has a timeout, and failures email `alert_email`. Tasks can also set `run_if` (for example `ALL_DONE` for a clean-up step), which the designer exposes.

## Compute

Serverless tasks declare an environment: `default` installs the wheel, `ml` adds scikit-learn, MLflow and the Databricks SDK. The environment version is a bundle variable (`serverless_environment_version`). The streaming job uses a job cluster (Databricks Runtime `classic_spark_version`, worker type `node_type_id`, dedicated access mode) and attaches the wheel as a library. Choose per job in the designer.

## Targets

| Target | Mode | Differences |
|---|---|---|
| `dev` (default) | development | names prefixed with `[dev you]`, schedules and triggers paused, your own schema prefix, experiment and endpoint |
| `staging` | production | shared root path, `stagedoor_staging` catalog, endpoint deployment on |
| `prod` | production | `stagedoor_prod`, simulated upstream off, endpoint deployment on; set `run_as` to a service principal |

Hosts are not hard-coded: authenticate with `databricks auth login --host <url>` locally, and with environment variables in CI.

## CI/CD with GitHub Actions

`.github/workflows/ci.yml` runs on every push and pull request:

| Job | Runs |
|---|---|
| Unit tests | `pytest tests/unit` on Python 3.11 and 3.12 |
| Spark and Delta tests | `pytest tests/spark` with Java 17 |
| Designer | `node designer/tests/run.js` and `node designer/scripts/build.js --check` |
| Wheel | `python -m build --wheel` |
| Bundle | `databricks bundle validate`, when workspace secrets are configured |

`.github/workflows/deploy.yml` deploys to staging on every push to `main` and runs the setup and daily jobs as an integration test, then deploys to production when a release is published. Both use GitHub environments, so production can require an approval, and authenticate as a service principal with OAuth (`DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`); GitHub OIDC federation removes the client secret if your account supports it. Until the secrets exist, both workflows skip their Databricks steps with a notice instead of failing.

A change therefore travels: branch, tests, pull request, merge, staging deployment with a real run, release, production.

**Practise:** run `databricks bundle deploy -t dev`, then `databricks bundle run -t dev stagedoor_platform --params full_refresh=true` to backfill.
