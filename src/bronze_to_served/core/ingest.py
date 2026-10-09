"""Readers that turn raw sources into Bronze DataFrames with ingestion metadata.

Every Bronze row carries where it came from (`_source_file`), when the file was written
(`_file_modified_at`), when it was ingested (`_ingested_at`) and anything that did not fit the declared
schema (`_rescued_data`), so Bronze is a faithful, replayable record of what arrived.
"""
from __future__ import annotations

import glob
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ..config import PlatformConfig


def string_schema(names) -> StructType:
    """Bronze keeps raw values as strings; Silver types them with try_cast."""
    return StructType([StructField(n, StringType(), True) for n in names])


def with_ingestion_metadata(df: DataFrame) -> DataFrame:
    return df.select("*", F.col("_metadata.file_path").alias("_source_file"),
                     F.col("_metadata.file_modification_time").alias("_file_modified_at"),
                     F.current_timestamp().alias("_ingested_at"))


def read_files_stream(spark: SparkSession, cfg: PlatformConfig, *, source: str, fmt: str, schema: StructType,
                      stream_name: str, options: dict | None = None) -> DataFrame:
    """Incrementally read new files from the landing volume.

    On Databricks this is Auto Loader (cloudFiles): it tracks which files were processed in the
    checkpoint, scales to millions of files and, in `rescue` mode, never fails on new upstream columns;
    they land in `_rescued_data` instead. Locally it is Spark's file source with the same schema.
    """
    path = cfg.landing_path(source)
    ext = "json" if fmt == "json" else fmt
    if cfg.is_local:
        reader = spark.readStream.format(fmt).schema(schema).option("pathGlobFilter", f"*.{ext}")
        if fmt == "csv":
            reader = reader.option("header", "true")
        for key, value in (options or {}).items():
            if not key.startswith("cloudFiles."):
                reader = reader.option(key, value)
        # _metadata is read straight off the file source, then the rescue column Auto Loader would add
        return with_ingestion_metadata(reader.load(path)).withColumn("_rescued_data", F.lit(None).cast("string"))
    else:
        reader = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", fmt)
                  .option("cloudFiles.schemaLocation", cfg.checkpoint_path(f"{stream_name}/_schema"))
                  .option("cloudFiles.schemaEvolutionMode", "rescue")
                  .option("rescuedDataColumn", "_rescued_data")
                  .option("pathGlobFilter", f"*.{ext}")      # never pick up half-written *.tmp files
                  .schema(schema))
        if fmt == "csv":
            reader = reader.option("header", "true")
        for key, value in (options or {}).items():
            reader = reader.option(key, value)
        df = reader.load(path)
    return with_ingestion_metadata(df)


def read_kafka_stream(spark: SparkSession, *, bootstrap_servers: str, topic: str, schema: StructType,
                      starting_offsets: str = "earliest", options: dict | None = None) -> DataFrame:
    """Read JSON messages from Kafka (or Event Hubs / MSK via their Kafka endpoints) into the Bronze shape.

    Pass credentials through `options`, read from a secret scope, e.g.
    {"kafka.security.protocol": "SASL_SSL", "kafka.sasl.jaas.config": dbutils.secrets.get(...)}.
    """
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", bootstrap_servers)
              .option("subscribe", topic)
              .option("startingOffsets", starting_offsets)
              .option("failOnDataLoss", "false"))
    for key, value in (options or {}).items():
        reader = reader.option(key, value)
    raw = reader.load()
    payload = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("p"),
                         F.col("value").cast("string").alias("_value"), "topic", "partition", "offset", "timestamp")
    return payload.select(
        *[F.col(f"p.{f.name}").alias(f.name) for f in schema.fields],
        F.when(F.col("p").isNull(), F.col("_value")).alias("_rescued_data"),
        F.format_string("kafka://%s/%d/%d", "topic", "partition", "offset").alias("_source_file"),
        F.col("timestamp").alias("_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"))


def copy_into(spark: SparkSession, cfg: PlatformConfig, *, table: str, source: str, pattern: str,
              schema: StructType, force: bool = False) -> int:
    """Idempotent batch load of CSV files with COPY INTO: files already loaded are skipped automatically.

    Locally (no COPY INTO in open-source Spark) the same idempotency comes from an anti-join on the
    source file path. `force` reloads files already loaded (after a full refresh emptied the table).
    Returns the number of rows inserted.
    """
    path = cfg.landing_path(source)
    if not cfg.is_local:
        result = spark.sql(f"""
            COPY INTO {table}
            FROM (SELECT *, _metadata.file_path AS _source_file,
                         _metadata.file_modification_time AS _file_modified_at,
                         current_timestamp() AS _ingested_at
                  FROM '{path}')
            FILEFORMAT = CSV
            PATTERN = '{pattern}'
            FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
            COPY_OPTIONS ('mergeSchema' = 'false', 'force' = '{str(force).lower()}')""").collect()
        return int(result[0]["num_inserted_rows"]) if result else 0
    files = sorted(glob.glob(os.path.join(path, pattern)))
    if not files:
        return 0
    df = with_ingestion_metadata(spark.read.format("csv").option("header", "true").schema(schema).load(files))
    loaded = spark.read.table(table).select("_source_file").distinct()
    new_rows = df if force else df.join(loaded, "_source_file", "left_anti")
    count = new_rows.count()
    if count:
        new_rows.select(*spark.read.table(table).columns).write.format("delta").mode("append").saveAsTable(table)
    return count
