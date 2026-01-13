# Connecting Apache Spark to Dremio Catalog

Apache Spark can connect to Dremio's Catalog using the standard Iceberg Spark Runtime. For robust, long-running production jobs, Dremio provides an **Auth Manager** to handle OAuth2 token lifecycle (refreshing tokens automatically).

## Prerequisites
- Spark 3.3+ (3.5 Recommended)
- Dremio Personal Access Token (PAT)

## Configuration (`spark-defaults.conf` or `spark-submit`)

### Recommended Production Setup (Auth Manager)
This setup ensures tokens are refreshed automatically, preventing job failures due to token expiration.

```bash
# Required Jars (Iceberg + AWS/Azure/GCP Bundle + Dremio Auth Manager)
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,com.dremio.iceberg.authmgr:authmgr-oauth2-runtime:0.0.5

# Enable Iceberg Extensions
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# Catalog Configuration
--conf spark.sql.catalog.dremio=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.dremio.catalog-impl=org.apache.iceberg.rest.RESTCatalog
--conf spark.sql.catalog.dremio.uri=https://catalog.dremio.cloud/api/iceberg
--conf spark.sql.catalog.dremio.warehouse=<PROJECT_NAME>
--conf spark.sql.catalog.dremio.header.X-Iceberg-Access-Delegation=vended-credentials

# Dremio Auth Manager Configuration
--conf spark.sql.catalog.dremio.rest.auth.type=com.dremio.iceberg.authmgr.oauth2.OAuth2Manager
--conf spark.sql.catalog.dremio.rest.auth.oauth2.token-endpoint=https://login.dremio.cloud/oauth/token
--conf spark.sql.catalog.dremio.rest.auth.oauth2.grant-type=token_exchange
--conf spark.sql.catalog.dremio.rest.auth.oauth2.client-id=dremio
--conf spark.sql.catalog.dremio.rest.auth.oauth2.token-exchange.subject-token=<YOUR_PAT>
--conf spark.sql.catalog.dremio.rest.auth.oauth2.token-exchange.subject-token-type=urn:ietf:params:oauth:token-type:dremio:personal-access-token
```

### Simple Setup (Development/Notebooks)
For quick tests, you can pass the token directly as a bearer token. **Note:** This token typically expires in 24 hours.

```bash
--conf spark.sql.catalog.dremio.token=<YOUR_PAT>
--conf spark.sql.catalog.dremio.oauth2-server-uri=https://login.dremio.cloud/oauth/token
```

## PySpark Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DremioIcebergIngest") \
    .config("spark.sql.catalog.dremio", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.dremio.catalog-impl", "org.apache.iceberg.rest.RESTCatalog") \
    .getOrCreate()

# Create Namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS dremio.logistics")

# Create Table
spark.sql("""
    CREATE TABLE IF NOT EXISTS dremio.logistics.shipments (
        shipment_id INT,
        origin STRING,
        destination STRING
    ) USING iceberg
""")

# Insert Data
spark.sql("INSERT INTO dremio.logistics.shipments VALUES (101, 'NY', 'CA')")

print("Ingestion Complete")
```

## Important Notes
1.  **Vended Credentials**: The `header.X-Iceberg-Access-Delegation=vended-credentials` flag is crucial. It tells Dremio to provide temporary AWS/Azure/GCP credentials to Spark, allowing it to write files directly to the lakehouse storage without needing hardcoded cloud keys.
2.  **Dependencies**: Ensure your `iceberg-spark-runtime` version matches your Spark version.
