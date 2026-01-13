# Connecting DuckDB to Dremio Catalog

[DuckDB](https://duckdb.org/) is a high-performance in-process SQL OLAP database. It can read and write to Dremio-managed Iceberg tables using its official `iceberg` extension.

## 1. Load Extensions

You need the `iceberg` extension for table logic and `httpfs` for S3/Network connectivity.

```sql
INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;
```

## 2. Configure Authentication (Secrets)

DuckDB uses a SECRET object to store credentials safely. Use `oauth2-server-uri` if you want automatic token refreshes (if supported by your specific DuckDB version's Iceberg implementation), or simply pass the token.

```sql
CREATE OR REPLACE SECRET dremio_secret (
    TYPE ICEBERG,
    -- Dremio Oauth2 Token Endpoint
    OAUTH2_SERVER_URI 'https://login.dremio.cloud/oauth/token',
    -- Use 'dremio' as client_id for PATs
    CLIENT_ID 'dremio', 
    CLIENT_SECRET 'YOUR_PAT_HERE'
);
```

*Note: For Dremio Cloud, `CLIENT_SECRET` is your Personal Access Token.*

## 3. Attach the Catalog

```sql
ATTACH 'https://catalog.dremio.cloud/api/iceberg' AS dremio (
    TYPE ICEBERG,
    SECRET dremio_secret,
    -- Enable credential vending for S3 access
    "header.X-Iceberg-Access-Delegation" 'vended-credentials'
);
```

## 4. Querying Data

Once attached, you can query tables using the `dremio` prefix (or whatever alias you gave the catalog).

```sql
-- List tables in a specific namespace
SHOW TABLES IN dremio.marketing;

-- Query a table
SELECT * FROM dremio.marketing.campaigns LIMIT 10;

-- Write Data (Insert)
INSERT INTO dremio.marketing.campaigns 
VALUES (3, 'Autumn Sale', 'Retail');
```
