# Connecting StarRocks to Dremio Catalog

[StarRocks](https://www.starrocks.io/) is a real-time OLAP database that can query Iceberg tables as external catalogs.

## Creating the External Catalog

Use the `CREATE EXTERNAL CATALOG` command. StarRocks uses standard Iceberg REST properties.

```sql
CREATE EXTERNAL CATALOG dremio
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "https://catalog.dremio.cloud/api/iceberg",
    "iceberg.catalog.warehouse" = "<PROJECT_NAME>",
    -- Credential Vending (Critical)
    "iceberg.catalog.header.X-Iceberg-Access-Delegation" = "vended-credentials",
    -- Authentication
    "iceberg.catalog.oauth2-server-uri" = "https://login.dremio.cloud/oauth/token",
    "iceberg.catalog.credential" = "<YOUR_PAT_HERE>"
);
```

**Property Breakdown:**
*   `iceberg.catalog.header.X-Iceberg-Access-Delegation`: Ensures StarRocks gets temporary S3/Azure creds from Dremio instead of needing hardcoded cloud keys.
*   `iceberg.catalog.credential`: Your Dremio Personal Access Token.

## Usage

```sql
-- Switch to the Dremio catalog
SET CATALOG dremio;

-- List tables
SHOW TABLES FROM marketing;

-- Query
SELECT * FROM marketing.campaigns LIMIT 10;
```
