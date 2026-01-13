# Connecting Spice.ai to Dremio Catalog

[Spice.ai](https://spice.ai/) is a unified SQL interface for data and AI apps. It uses a `spicepod.yaml` configuration to accelerate datasets locally.

## Configuration (`spicepod.yaml`)

Define your Dremio-backed Iceberg table as a dataset.

```yaml
version: v1beta1
kind: Spicepod
name: dremio-app

datasets:
  - from: iceberg:marketing.campaigns  # Fully qualified path in Dremio
    name: campaigns
    params:
      iceberg_catalog_type: rest
      iceberg_uri: https://catalog.dremio.cloud/api/iceberg
      # Credential Vending for S3 access
      iceberg_header_X-Iceberg-Access-Delegation: vended-credentials
    # Reference secrets for security
    secrets:
      iceberg_token: env:DREMIO_PAT 
```

## Running Spice

1.  **Export Token**: `export DREMIO_PAT=your_token`
2.  **Start Runtime**: `spice run`

## Querying

Spice exposes a SQL interface (default port 3000) or Flight endpoint (50051).

```sql
-- Query the accelerated table
SELECT * FROM campaigns WHERE category = 'Retail';
```

## Acceleration
Spice can materialize these tables locally (DuckDB/SQLite) or in-memory (Arrow) for sub-second API responses, refreshing them automatically from Dremio.
