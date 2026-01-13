# Dremio Interfaces & Python Libraries

This directory covers tools and interfaces for interacting with the **Dremio Query Engine and Control Plane**.

## 🆚 Dremio Interface vs. Iceberg REST Catalog

It is important to understand the difference between connecting to **Dremio (the Engine)** and connecting to **Dremio (the Catalog)**.

| Feature | **Dremio Interface** (This Directory) | **Iceberg REST Catalog** (`other_engines/`) |
| :--- | :--- | :--- |
| **Primary Goal** | **Querying Data** (SQL) & **Admin** | **Managing Tables** (Read/Write Files) |
| **Protocol** | Arrow Flight, JDBC/ODBC, REST API | Apache Iceberg REST Specification |
| **Data Engine** | Dremio Engine (Compute) | Spark, Flink, Trino, etc. (Compute) |
| **Use Case** | BI Dashboards, Ad-hoc SQL, View/VDS Management | Data Engineering, ETL Pipelines, Lakehouse Writes |
| **You send...** | SQL Queries ("SELECT * FROM ...") | Metadata Operations (Create Table, Append Data) |
| **You receive...** | Result Sets (Arrow/JSON/Rows) | Table Metadata / Commit Confirmations |

---

## 📚 Standard Interfaces

-   **[Arrow Flight](arrow_flight.md)**: **🚀 Fastest.** Streams binary Arrow data. Best for large data retrieval (ML/Data Science).
-   **[REST API](rest_api.md)**: **🌐 Universal.** Good for lightweight integrations, web apps, and submitting async jobs.
-   **[JDBC/ODBC](jdbc.md)**: **🔌 Compatible.** Standard legacy connectivity for tools that require JDBC.

## 🐍 Python Libraries

-   **[dremio-simple-query](dremio_simple_query.md)**: Easy button for Arrow Flight. Get a DataFrame in 2 lines.
-   **[dremioframe](dremioframe.md)**: DataFrame-style syntax (`df.filter()`) that compiles to Dremio SQL.
-   **[dremio-cli](dremio_cli.md)**: Command-line tool for administration, CI/CD, and catalog management.
