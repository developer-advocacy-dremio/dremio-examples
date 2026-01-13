# Apache Arrow Flight (Python)

Arrow Flight is the highest-performance method for retrieving data from Dremio. It streams data in binary Arrow format, eliminating serialization overhead (unlike JDBC/ODBC).

## Prerequisites
- `pyarrow` (`pip install pyarrow`)
- Dremio PAT

## Example: High-Speed Data Retrieval

```python
from pyarrow import flight
import os

# Configuration
# Dremio Cloud: data.dremio.cloud:443
# Dremio Software: localhost:32010
HOST = "data.dremio.cloud"
PORT = 443
TOKEN = os.environ.get("DREMIO_PAT")

# Authentication
# For Dremio Cloud, use basic auth with token as password (username ignored)
# Or use Bearer token headers depending on the client implementation
headers = [
    (b"authorization", f"Bearer {TOKEN}".encode("utf-8"))
]

def flight_query(sql_query):
    # 1. Connect to Flight Client
    # Enable TLS for Cloud
    location = flight.Location.for_grpc_tls(HOST, PORT)
    client = flight.FlightClient(location)

    # 2. Authenticate
    # Dremio Flight often uses a specific auth flow, but passing Bearer headers works for many operations
    options = flight.FlightCallOptions(headers=headers)

    # 3. Get Flight Info (Plan the query)
    flight_info = client.get_flight_info(
        flight.FlightDescriptor.for_command(sql_query),
        options=options
    )

    # 4. Stream Results
    # A query might return multiple endpoints (streams). Read them all.
    reader = client.do_get(flight_info.endpoints[0].ticket, options=options)
    return reader.read_all() # Returns a PyArrow Table

# Usage
try:
    arrow_table = flight_query("SELECT * FROM Samples.\"samples.dremio.com\".\"NYC-taxi-trips\" LIMIT 10")
    
    # Convert to Pandas for analysis
    df = arrow_table.to_pandas()
    print(df.head())
except Exception as e:
    print(f"Error: {e}")
```

## Pros/Cons
*   **Pros**: Extremely fast (parallel stream), zero-copy to Pandas/Numpy.
*   **Cons**: Requires gRPC port access (443/32010), stricter network requirements than REST.
