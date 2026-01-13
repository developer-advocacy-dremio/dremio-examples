# Connecting Go to Dremio Catalog

The [Apache Iceberg Go](https://github.com/apache/iceberg-go) library (currently in development) supports the REST Catalog specification.

## Installation

```bash
go get github.com/apache/iceberg-go/v2
```

## Example Code

```go
package main

import (
	"context"
	"fmt"
	"github.com/apache/iceberg-go/v2/catalog"
)

func main() {
	// 1. Define Properties
	props := map[string]string{
		"uri":                                "https://catalog.dremio.cloud/api/iceberg",
		"warehouse":                          "<PROJECT_NAME>",
		"credential":                         "<YOUR_PAT>",
		"header.X-Iceberg-Access-Delegation": "vended-credentials",
	}

	// 2. Load the REST Catalog
	cat, err := catalog.NewRestCatalog("dremio", props)
	if err != nil {
		panic(err)
	}

	// 3. List Namespaces
	namespaces, err := cat.ListNamespaces(context.Background(), nil)
	if err != nil {
		panic(err)
	}

	fmt.Println("Namespaces:", namespaces)
}
```

## Notes
- The usage of `credential` map key maps to the OAuth2 Bearer token in the REST client.
- Ensure you handle the context (`ctx`) properly for production timeouts.
