# Connectivity References for Dremio Catalog

This directory contains configuration guides for connecting various compute engines, libraries, and tools to Dremio's built-in **Iceberg REST Catalog**.

Dremio serves as the central catalog for your Lakehouse, allowing these tools to read and write data that is immediately governable and queryable within Dremio.

## 📊 Analytic & Streaming Engines
Heavy-hitting distributed engines for ETL and BI.

-   **[Apache Spark](apache_spark.md)**: Production ETL pipelines (supports Dremio Auth Manager).
-   **[Apache Flink](apache_flink.md)**: Real-time streaming ingestion via Flink SQL Client.
-   **[Trino](trino.md)**: Federated SQL queries against Dremio-managed tables.
-   **[StarRocks](starrocks.md)**: Real-time OLAP and sub-second analytics.

## 🐍 Python & Data Science
Tools for local analytics, dataframes, and python-native pipelines.

-   **[PyIceberg](pyiceberg.md)**: The official Python implementation. Prerequisite for many other tools.
-   **[DuckDB](duckdb.md)**: In-process SQL OLAP, great for local analysis and simplified ETL.
-   **[Daft](daft.md)**: Distributed Python dataframe API for heavy application workloads.
-   **[Polars](polars.md)**: Lightning-fast single-node dataframes (Read-optimized).

## 🛠️ Native & Low-Level APIs
Language bindings for building custom applications or microservices.

-   **[Java (Native)](iceberg_java.md)**: Using the Iceberg Java API directly without a heavy engine like Spark.
-   **[Rust](iceberg_rust.md)**: High-performance connectivity for Rust applications.
-   **[Go](iceberg_go.md)**: Go language bindings for the REST Catalog.

## 🤖 AI & Acceleration
Tools for AI workloads and query acceleration.

-   **[Spice.ai](spiceai.md)**: AI-native SQL query acceleration and materialization.
