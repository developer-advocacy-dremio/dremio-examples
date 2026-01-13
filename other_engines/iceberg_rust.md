# Connecting Rust to Dremio Catalog

The [iceberg-rust](https://crates.io/crates/iceberg) ecosystem allows Rust applications to interact with Dremio's REST Catalog.

## Dependencies (`Cargo.toml`)

```toml
[dependencies]
iceberg = "0.2.0"
iceberg-catalog-rest = "0.2.0"
tokio = { version = "1", features = ["full"] }
```

## Example Code

```rust
use iceberg::Catalog;
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure the Catalog
    let mut props = HashMap::new();
    props.insert("uri".to_string(), "https://catalog.dremio.cloud/api/iceberg".to_string());
    props.insert("warehouse".to_string(), "<PROJECT_NAME>".to_string());
    props.insert("credential".to_string(), "<YOUR_PAT>".to_string());
    props.insert("header.X-Iceberg-Access-Delegation".to_string(), "vended-credentials".to_string());

    let config = RestCatalogConfig::builder()
        .uri("https://catalog.dremio.cloud/api/iceberg")
        .props(props)
        .build();

    // 2. Initialize the Catalog
    let catalog = RestCatalog::new(config);

    // 3. List Namespaces
    let namespaces = catalog.list_namespaces(None).await?;
    println!("Namespaces: {:?}", namespaces);

    Ok(())
}
```

## Key Properties
- `uri`: The Dremio Cloud REST API endpoint.
- `credential`: Your Personal Access Token (PAT).
- `header.X-Iceberg-Access-Delegation`: Enables credential vending for direct S3 access.
