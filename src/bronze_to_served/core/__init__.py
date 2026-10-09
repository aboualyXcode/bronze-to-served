"""Reusable, domain-agnostic building blocks for Databricks lakehouse pipelines.

contracts   table contracts (columns, types, comments, keys, clustering) and DDL rendering
quality     expectations, quarantine, quality metrics and the quality gate
ingest      Auto Loader, local file streams, Kafka and COPY INTO readers with ingestion metadata
streams     running Structured Streaming queries with availableNow or processing-time triggers
merge       order-independent SCD1 upserts, SCD2 history from CDC, insert-only deduplication
delta_ops   overwrite, replaceWhere, change data feed, OPTIMIZE / VACUUM / ANALYZE
uc          Unity Catalog objects: catalogs, schemas, volumes, tables, grants and tags
audit       an audit row per task run
"""
