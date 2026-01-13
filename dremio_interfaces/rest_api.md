# Dremio REST API (Python)

The Dremio REST API allows you to submit SQL queries and manage catalog entities programmatically. This is useful for lightweight orchestrations or when you can't install heavy drivers (JDBC/ODBC).

## Prerequisites
- `requests` library (`pip install requests`)
- Dremio PAT (Personal Access Token)

## Example: Submitting a SQL Query

The `/api/v3/sql` endpoint submits a query job. You must then poll the job status until completion to retrieve results.

```python
import requests
import time
import os
import json

# Configuration
DREMIO_URL = "https://api.dremio.cloud" # or http://localhost:9047 for software
TOKEN = os.environ.get("DREMIO_PAT")
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

def query_dremio(sql):
    # 1. Submit Query
    payload = {"sql": sql}
    response = requests.post(f"{DREMIO_URL}/api/v3/sql", headers=HEADERS, json=payload)
    response.raise_for_status()
    job_id = response.json()["id"]
    print(f"Job Submitted: {job_id}")

    # 2. Poll for Completion
    while True:
        job_status = requests.get(f"{DREMIO_URL}/api/v3/job/{job_id}", headers=HEADERS).json()
        state = job_status["jobState"]
        if state == "COMPLETED":
            break
        elif state in ["FAILED", "CANCELED"]:
            raise Exception(f"Job Failed: {job_status.get('errorMessage')}")
        time.sleep(1)

    # 3. Fetch Results
    # Dremio paginates results (default 100). Fetch the first batch.
    results = requests.get(f"{DREMIO_URL}/api/v3/job/{job_id}/results", headers=HEADERS).json()
    return results

# Usage
try:
    data = query_dremio("SELECT * FROM sys.options LIMIT 5")
    print(json.dumps(data, indent=2))
except Exception as e:
    print(f"Error: {e}")
```

## Pros/Cons
*   **Pros**: No drivers needed, works in any environment with HTTP access.
*   **Cons**: Slower than Flight/JDBC for large datasets; requires handling pagination and async job polling manually.
