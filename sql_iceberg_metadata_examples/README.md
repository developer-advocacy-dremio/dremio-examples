# Iceberg Metadata Query Examples

This directory demonstrates how to query Apache Iceberg metadata tables directly within Dremio.

## Files

- **[history_and_snapshots.sql](history_and_snapshots.sql)**: Querying the `table_history` and `snapshots` metadata tables to audit changes, time travel, and rollback table states.
- **[files_and_partitions.sql](files_and_partitions.sql)**: Inspecting `data_files`, `manifests`, and `partitions` metadata to understand storage layout and optimize performance.
