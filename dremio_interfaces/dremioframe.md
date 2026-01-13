# DremioFrame

`dremioframe` provides a Pandas-like DataFrame API that pushes operations down to Dremio as SQL. It allows you to write Pythonic code while leveraging Dremio's query engine.

## Installation
```bash
pip install dremioframe
```

## Example

```python
from dremioframe.ctx import DremioContext
import os

# Connect
ctx = DremioContext(
    "dremio.cloud", 
    os.environ.get("DREMIO_PAT"), 
    os.environ.get("DREMIO_PROJECT_ID")
)

# Create a DataFrame from a Dremio dataset
df = ctx.tbl("marketing.campaigns")

# Operations (Lazy - compiled to SQL)
result = (
    df.select("id", "name", "category")
      .filter(df["category"] == "Retail")
      .collect() # Executes query and returns Pandas DataFrame
)

print(result)
```

## Use Case
Best for **Data Scientists** who prefer DataFrame syntax (select, filter, mutate) over writing raw SQL strings, but want the execution to happen in Dremio.
