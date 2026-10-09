# 10. Testing

A data platform is correct when it produces the right rows: on the first run, on the hundredth, after a retry, and when data arrives late, twice or out of order. The tests are built around that.

| Suite | Command | Needs | Proves |
|---|---|---|---|
| Unit | `pytest tests/unit` | Python, scikit-learn, PyYAML | the generator is deterministic and keeps its contracts; the oracle is consistent; names, DDL, rules, the quality gate, the catalog, the CLI, the model, the promotion policy, drift and the bundle are right |
| Designer | `node designer/tests/run.js` | Node 20 | the YAML writer, graph rules, every check, the layout, and that `resources/*.job.yml` match their templates |
| MERGE patterns | `pytest tests/spark/test_merge_patterns.py` | Java 17, PySpark, Delta Lake | SCD2 versions, deletes, re-inserts, no-ops, replays and stale events; SCD1 order independence; exactly-once inserts; expectation counting |
| Differential | `pytest tests/spark/test_differential.py` | same | the production DAG matches the reference implementation row for row: Silver, Gold, the quarantine, features and labels |
| Incremental | `pytest tests/spark/test_incremental.py` | same | four daily runs equal one run over the same days; re-running changes nothing; a full refresh rebuilds the same tables |
| Staging | the deploy workflow | a Databricks workspace | Auto Loader, COPY INTO, Unity Catalog DDL, the MLflow registry, Model Serving and the declarative pipeline, in a real run |

## The differential test

`tests/reference/oracle.py` implements Silver, Gold and the features a second time, deliberately differently:

- **Silver is an event-by-event replay in plain Python** (apply each order event; apply each CRM change in LSN order), where the pipeline uses set-based Spark and one Delta MERGE per micro-batch. The replay is obviously right; the MERGE is fast. If they agree, the MERGE is right.
- **Gold and the features are SQL over SQLite**, where the pipeline uses PySpark DataFrames.

Money is held as integer cents and timestamps as UTC strings, so comparisons are exact; only ratios use a tolerance. The test runs the daily and training jobs **from the same templates that generate the deployed jobs** (`tasks/local.py`), so it tests the production DAG, not a hand-made sequence.

The oracle is itself tested (`tests/unit/test_reference_oracle.py`): SCD2 versions never overlap, every order's status matches its timestamps, Gold reconciles to the cent with Silver, features never look ahead, and the churn rate is plausible. Rule names are shared: an empty record must break the same rules in both implementations.

## What runs where

Open-source Spark with Delta Lake runs everything except a few Databricks features, which the local runtime replaces:

| Databricks | Locally | Covered by |
|---|---|---|
| Auto Loader | Spark's file source, same schema and metadata columns | Spark suites (the shared logic); staging (Auto Loader itself) |
| COPY INTO | an anti-join on the source file path | Spark suites; staging |
| Unity Catalog names, primary keys, clustering | two-level names, no keys or clustering | unit tests render the Unity Catalog DDL; staging creates it |
| MLflow registry, Model Serving | skipped (`WORKSPACE_ONLY` in `tests/spark/helpers.py`) | unit tests for training, metrics and the policy; staging |
| Lakeflow Declarative Pipelines | not available | staging |

The model is trained and evaluated in the unit tests on features from the oracle, so the training code, the metrics and the promotion policy are tested without Spark or MLflow; it must reach an out-of-time ROC AUC above 0.72.

## Running the tests

```sh
pip install -e ".[ml,dev]"            # unit tests
pytest tests/unit -q
node designer/tests/run.js

pip install -e ".[ml,local,dev]"      # Spark and Delta tests (Java 17)
pytest tests/spark -q
```

## Adding a test

- A new Silver or Gold rule: implement it in the pipeline and in the oracle, then extend the differential mapping in `tests/spark/test_differential.py`.
- A new MERGE edge case: add a hand-made batch to `tests/spark/test_merge_patterns.py`.
- A new design check: add a failing design to `designer/tests/run.js`.
