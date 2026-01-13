# Connecting Apache Flink to Dremio Catalog

Apache Flink can access Dremio's Iceberg Catalog via the generic Iceberg REST Catalog interface.

## Prerequisites
- Flink 1.16+
- Flink Iceberg Connector (`iceberg-flink-runtime`)
- Dremio PAT

## SQL Client Configuration

You can register the Dremio catalog in the Flink SQL Client using a simple `CREATE CATALOG` statement.

```sql
CREATE CATALOG dremio WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'https://catalog.dremio.cloud/api/iceberg',
    'warehouse' = '<PROJECT_NAME>',
    'credential' = '<YOUR_PAT_HERE>',
    'header.X-Iceberg-Access-Delegation' = 'vended-credentials',
    'io-impl' = 'org.apache.iceberg.io.ResolvingFileIO'
);

USE CATALOG dremio;
```

## Properties Explanation

| Property | Value | Notes |
| :--- | :--- | :--- |
| `type` | `iceberg` | Identifies the connector. |
| `catalog-type` | `rest` | Specifies the REST specification. |
| `credential` | `<PAT>` | Used as the Bearer token for authentication. |
| `header.X-Iceberg-Access-Delegation` | `vended-credentials` | Enables Credential Vending for direct storage access. |

## Streaming Ingestion Example

```sql
-- Create a table in Dremio Catalog
CREATE TABLE IF NOT EXISTS dremio.iot.sensors (
    sensor_id INT,
    temperature DOUBLE,
    ts TIMESTAMP(3)
);

-- Insert data from a datagen source (Mock Stream)
INSERT INTO dremio.iot.sensors
SELECT 
    id, 
    RAND() * 100, 
    CURRENT_TIMESTAMP 
FROM datagen;
```

## Compatibility Note
Flink's built-in REST catalog support is generally compatible with Dremio. If you encounter authentication timeouts on long-running streaming jobs, ensure your Flink deployment uses a mechanism to refresh the token or restart the job before the token (typically 24h) expires.
