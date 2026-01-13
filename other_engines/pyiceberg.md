# Connecting PyIceberg to Dremio Catalog

[PyIceberg](https://py.iceberg.apache.org/) is the official Python library for Apache Iceberg. It supports the REST catalog specification, making it a native fit for Dremio's built-in catalog.

## Prerequisites
- Dremio Cloud Project Name (Warehouse)
- Personal Access Token (PAT)
- `pyiceberg` installed (`pip install "pyiceberg[pyarrow,s3fs,adlfs,gcsfs]"`)

## Configuration (`~/.pyiceberg.yaml`)

You can configure PyIceberg globally using a YAML file. This is the recommended approach for keeping credentials out of your code.

```yaml
catalog:
  dremio:
    uri: https://catalog.dremio.cloud/api/iceberg
    type: rest
    # Your Dremio Cloud Project Name
    warehouse: <PROJECT_NAME>
    # Credential Vending (Enables direct writes without cloud keys)
    header.X-Iceberg-Access-Delegation: vended-credentials
    # OAuth2 Token Configuration
    oauth2-server-uri: https://login.dremio.cloud/oauth/token
    token: <YOUR_PAT_HERE>
```

## Runtime Configuration (Inline)

Alternatively, you can pass the configuration dictionary directly to `load_catalog` within your script. This allows you to fetching credentials from a secret manager (like AWS Secrets Manager) at runtime.

```python
from pyiceberg.catalog import load_catalog

# Retrieve token securely (e.g., from env or secret manager)
import os
dremio_token = os.environ.get("DREMIO_PAT")

catalog = load_catalog(
    "dremio",
    **{
        "uri": "https://catalog.dremio.cloud/api/iceberg",
        "type": "rest",
        "warehouse": "marketing_project", # Your Project Name
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "oauth2-server-uri": "https://login.dremio.cloud/oauth/token",
        "token": dremio_token
    }
)
```

## Usage in Python

Once configured, you can load the catalog by name and perform table operations.

```python
from pyiceberg.catalog import load_catalog
import pyarrow as pa

# Load the catalog defined in ~/.pyiceberg.yaml
catalog = load_catalog("dremio")

# 1. Create a Namespace
catalog.create_namespace("marketing")

# 2. Define a Schema
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType

schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "category", StringType(), required=False)
)

# 3. Create a Table
table = catalog.create_table(
    identifier="marketing.campaigns",
    schema=schema
)

# 4. Append Data (Using PyArrow)
df = pa.Table.from_pylist([
    {"id": 1, "name": "Summer Sale", "category": "Retail"},
    {"id": 2, "name": "Winter Clearance", "category": "Retail"}
], schema=schema.as_arrow())

table.append(df)

print(f"Successfully wrote {len(df)} records to marketing.campaigns")
```

## Key Configuration Parameters

| Parameter | Value | Description |
| :--- | :--- | :--- |
| `uri` | `https://catalog.dremio.cloud/api/iceberg` | The Dremio REST Catalog endpoint. |
| `header.X-Iceberg-Access-Delegation` | `vended-credentials` | **Critical**: Tells Dremio to vend temporary Write keys for S3/Azure/GCS. |
| `warehouse` | Project Name | Routes the request to your specific Dremio project. |
