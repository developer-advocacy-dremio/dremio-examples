# Dremio Simple Query

`dremio-simple-query` is a wrapper around PyArrow Flight designed to simplify the connection process, especially for Dremio Cloud. It handles authentication nuances and provides a clean API for getting data into Pandas, Polars, or DuckDB.

## Installation
```bash
pip install dremio-simple-query
```

## Example (V2 Client)

The V2 client is recommended for Dremio Cloud support and better stability.

```python
from dremio_simple_query.connectv2 import Connect
import os

# 1. Initialize Connection
# It auto-detects Software vs Cloud based on the host
client = Connect(
    host="data.dremio.cloud",
    token=os.environ.get("DREMIO_PAT")
)

# 2. Query to Pandas
df = client.toPandas("SELECT * FROM Samples.\"samples.dremio.com\".\"NYC-taxi-trips\" LIMIT 5")
print(df)

# 3. Query to Polars
pl_df = client.toPolars("SELECT * FROM sys.options LIMIT 5")
print(pl_df)

# 4. Query to DuckDB
# Creates a table in the localized DuckDB instance
client.toDuckDB("SELECT * FROM sys.options LIMIT 5", "my_duck_table")
```

## Use Case
Best for **Analysts and Data Scientists** who want the "fastest path to a DataFrame" without dealing with gRPC boilerplate or complex flight tickets.
