# 6. Declarative pipelines

`pipelines/stagedoor_declarative.py` builds two of the same flows as a Lakeflow Declarative Pipeline (formerly Delta Live Tables): clickstream from files to a daily engagement table, and CRM changes into SCD Type 2 history. It publishes to `<prefix>declarative`, next to the job-built tables, so both approaches can be compared on the same files.

## What you declare, and what the pipeline does for you

```python
@dlt.table(comment="Valid events, deduplicated within a 2-hour watermark.")
@dlt.expect_all_or_drop(CLICK_RULES)
def clickstream_silver():
    return (spark.readStream.table("clickstream_parsed")
            .withColumn("event_date", F.to_date("event_ts"))
            .withWatermark("event_ts", "2 hours")
            .dropDuplicatesWithinWatermark(["event_id"]))
```

You declare tables and the queries that define them. The pipeline derives the dependency graph from the queries, creates and evolves the tables, manages checkpoints, retries failures, records how many rows passed and failed each expectation in its event log, and draws the graph in its UI. SCD Type 2 is one call:

```python
dlt.create_streaming_table("customers_scd2")
dlt.apply_changes(target="customers_scd2", source="customers_cdc_clean", keys=["customer_id"],
                  sequence_by=F.col("lsn"), apply_as_deletes=F.expr("op = 'DELETE'"),
                  except_column_list=["op", "lsn"], stored_as_scd_type=2)
```

## Side by side

| Concern | Jobs and PySpark (the main path) | Declarative pipeline |
|---|---|---|
| Orchestration | an explicit task DAG in a job | inferred from the queries |
| Ingestion | Auto Loader with an explicit string schema | Auto Loader with string columns inferred |
| Data quality | expectations, `ops.quarantine` with the raw record, a quality gate | `@expect_*` decorators, a quarantine table of inverted rules, metrics in the event log |
| Deduplication | insert-only MERGE, which never forgets a key | `dropDuplicatesWithinWatermark`, with bounded state |
| SCD Type 2 | `scd2_merge`: about 60 lines you own and test | `apply_changes` (AUTO CDC): a few lines, adds `__START_AT` and `__END_AT` |
| Gold | batch rebuilds and `replaceWhere` | materialized views, refreshed incrementally where possible |
| Tests | unit and Spark tests, locally and in CI | in a workspace, with development-mode updates |

## Choosing

Choose a declarative pipeline when the flow is mostly streaming tables and materialized views, and you want lineage, retries and quality metrics without writing them. Choose jobs and PySpark when you need full control of MERGE semantics (order-independent upserts, your own SCD rules), logic that is not table-shaped (training models, calling APIs, exporting files), or tests that run outside a workspace. Many platforms use both: a job can run a pipeline as one of its tasks (`pipeline_task`), which is how the `stagedoor_declarative_refresh` job refreshes this one.

The two implementations can differ in edge cases, and comparing them is instructive: how a change that alters no tracked column is versioned, how long a duplicate key is remembered (two hours with the watermark, forever with the insert-only MERGE), and the history column names.

## Running it

The bundle defines the pipeline (`resources/stagedoor_declarative.pipeline.yml`: serverless, `CURRENT` channel) and a job that refreshes it whenever a clickstream file lands, using a file arrival trigger on the landing volume. After `databricks bundle deploy`, update the pipeline with `databricks bundle run -t dev stagedoor_declarative`, run the job with `databricks bundle run -t dev stagedoor_declarative_refresh`, or start an update from the pipeline UI. The two keys differ because an Asset Bundle needs every resource key to be unique across jobs, pipelines and every other resource type.

The code uses the long-standing `dlt` module. Databricks also offers the same model as `from pyspark import pipelines as dp`, where `dp.table`, `dp.materialized_view` and `dp.create_auto_cdc_flow` take the arguments used here.

**Practise:** run both implementations on the same landing files and compare `declarative.customers_scd2` with `silver.customers` for a customer whose record the CRM touched without changing it.
