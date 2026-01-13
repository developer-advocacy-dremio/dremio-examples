# Connecting Daft to Dremio Catalog

[Daft](https://www.getdaft.io/) is a distributed Python query engine. It integrates natively with **PyIceberg**, allowing it to use Dremio's catalog for metadata while handling heavy lifting for reads and writes.

## Prerequisites
```bash
pip install getdaft pyiceberg[s3fs,adlfs,gcsfs]
```

## Usage

Daft uses the standard `PyIceberg` catalog configuration.

```python
import daft
from pyiceberg.catalog import load_catalog
import os

# 1. Configure Catalog via PyIceberg
catalog = load_catalog(
    "dremio",
    **{
        "uri": "https://catalog.dremio.cloud/api/iceberg",
        "type": "rest",
        "warehouse": "<PROJECT_NAME>",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "credential": os.environ.get("DREMIO_PAT")
    }
)

# 2. Reading Data
# Load table from PyIceberg
iceberg_table = catalog.load_table("marketing.campaigns")

# Create a Daft DataFrame
df = daft.read_iceberg(iceberg_table)
df.show()

# 3. Writing Data
# Create dummy data
new_data = daft.from_pydict({
    "id": [4, 5], 
    "name": ["Spring Launch", "Black Friday"],
    "category": ["Retail", "Retail"]
})

# Write back to Dremio-managed table
new_data.write_iceberg(iceberg_table, mode="append")

print("Write complete.")
```

## Why Daft?
Daft is excellent for heavy ETL workloads in Python that need to process more data than fits in memory but don't require a full Spark cluster.
