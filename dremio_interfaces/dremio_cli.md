# Dremio CLI

`dremio-cli` is a powerful command-line tool and Python library for administering Dremio. It allows you to manage catalog entities (Spaces, Folders, PDS, VDS) via CLI commands or scripts.

**GitHub**: [developer-advocacy-dremio/dremio-cli](https://github.com/developer-advocacy-dremio/dremio-cli)

## Installation
```bash
pip install dremio-cli
```

## Setup
Create a config file or use env vars.
```bash
export DREMIO_HOST="api.dremio.cloud" # or localhost
export DREMIO_PORT="443"
export DREMIO_SSL="true"
export DREMIO_USER="dremio"
export DREMIO_PASSWORD="<PAT>"
```

## CLI Usage
```bash
# Execute SQL
dremio-cli sql execute "SELECT count(*) FROM sys.options"

# List Catalog Items
dremio-cli catalog list --path "marketing"
```

## Python Library Usage (Admin Automation)

```python
from dremio_client.conf import load_config
from dremio_client.auth import auth
from dremio_client.model.endpoints import catalog

# Connect
config = load_config()
token = auth(config)

# Get Catalog Item
item = catalog.get_by_path(token, "marketing.campaigns")
print(item)
```

## Use Case
Best for **DevOps, CI/CD pipelines**, and **Administration** tasks (creating views, managing permissions).
