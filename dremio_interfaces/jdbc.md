# JDBC/ODBC (Python)

Connecting via JDBC allows you to use standard SQL drivers. In Python, this is often done using `jaydebeapi`.

## Prerequisites
- JDK 1.8+ installed
- [Dremio JDBC Driver](https://www.dremio.com/drivers/) (`.jar` file)
- `jaydebeapi` (`pip install JayDeBeAPI`)
- `jpype1` (usually installed with jaydebeapi)

## Example: Connecting via JDBC

```python
import jaydebeapi
import os

# Configuration
DRIVER_PATH = "/path/to/dremio-jdbc-driver.jar"
JDBC_URL = "jdbc:dremio:direct=sql.dremio.cloud:443;ssl=true;" # Dremio Cloud
USER = "dremio" # Ignored for PAT auth usually, or username
PASSWORD = os.environ.get("DREMIO_PAT")

def query_jdbc(sql):
    conn = jaydebeapi.connect(
        "com.dremio.jdbc.Driver",
        JDBC_URL,
        {"user": USER, "password": PASSWORD},
        DRIVER_PATH,
    )
    
    curs = conn.cursor()
    curs.execute(sql)
    results = curs.fetchall()
    
    curs.close()
    conn.close()
    return results

# Usage
data = query_jdbc("SELECT * FROM sys.options LIMIT 5")
for row in data:
    print(row)
```

## Pros/Cons
*   **Pros**: Standard SQL compatibility, works with tools that only speak JDBC.
*   **Cons**: Creating the JVM bridge (JPype) can be finicky; serialization is slower than Arrow Flight.
