# Connecting Trino to Dremio Catalog

Trino (formerly PrestoSQL) has native support for Iceberg and the REST Catalog. You can configure it to query access tables managed by Dremio.

## Configuration (`etc/catalog/dremio.properties`)

Create a new catalog properties file in your Trino configuration directory.

```properties
connector.name=iceberg
iceberg.catalog.type=rest

# Dremio REST Endpoint
iceberg.rest-catalog.uri=https://catalog.dremio.cloud/api/iceberg

# Dremio Cloud Project Name
iceberg.rest-catalog.warehouse=<PROJECT_NAME>

# Authentication (Bearer Token)
iceberg.rest-catalog.oauth2.token=<YOUR_PAT_HERE>

# Credential Vending Support
# Passes the header to request temporary write keys
iceberg.rest-catalog.register-table-invokes-cleanup=true
iceberg.file-io-impl=org.apache.iceberg.io.ResolvingFileIO
```

*Note: Trino doesn't accept arbitrary custom headers like `header.X-Iceberg-Access-Delegation` in all versions. Check your specific Trino version's documentation for `iceberg.rest-catalog.headers` support if write access fails due to missing credentials.*

## Usage

```sql
-- Querying a Dremio table from Trino
SELECT * FROM dremio.marketing.campaigns LIMIT 10;

-- Creating a table
CREATE TABLE dremio.marketing.clicks (
    user_id BIGINT,
    url VARCHAR,
    ts TIMESTAMP
) WITH (
    format = 'PARQUET'
);
```

## Interop Value
Connecting Trino to Dremio's catalog allows you to use Trino for specific ad-hoc queries or existing workloads while keeping Dremio as the central governance and catalog layer for your Iceberg Lakehouse.
