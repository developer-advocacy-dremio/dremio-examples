# SQL Ingestion Examples

This directory covers methods for ingesting data into Dremio tables.

## Files

- **[copy_into.sql](copy_into.sql)**: Using the `COPY INTO` command to bulk load data from external files (CSV, Parquet, JSON) into Iceberg tables.
- **[continuous_ingestion_pipes.sql](continuous_ingestion_pipes.sql)**: Setting up continuous data ingestion workflows (Auto-ingest).
- **[ctas_insert_merge.sql](ctas_insert_merge.sql)**: Creating tables from query results (`CTAS`), appending data (`INSERT`), and merging updates (`MERGE`).
