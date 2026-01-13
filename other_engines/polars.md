# Connecting Polars to Dremio Catalog

[Polars](https://pola.rs/) is a blazingly fast DataFrame library using Arrow. It reads Iceberg tables by piggybacking on `PyIceberg` to handle the catalog metadata.

## Reading Data

```python
import polars as pl
from pyiceberg.catalog import load_catalog
import os

# 1. Load Catalog
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

# 2. Load Table Metadata
tbl = catalog.load_table("marketing.campaigns")

# 3. Create LazyFrame
# scan_iceberg handles predicate pushdown and partition pruning
lf = pl.scan_iceberg(tbl)

# 4. Execute Query
results = (
    lf.filter(pl.col("category") == "Retail")
      .collect()
)

print(results)
```

## Writing Data
*Note: Native Iceberg write support in Polars is currently experimental/unstable.* 

For now, the recommended pattern is to convert your Polars DataFrame to Arrow and write using **PyIceberg**.

```python
# Convert to Arrow Table
arrow_table = results.to_arrow()

# Append to Iceberg Table
tbl.append(arrow_table)
```
