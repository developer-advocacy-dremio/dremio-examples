# Connecting Native Java API to Dremio Catalog

You can use the official Apache Iceberg Java API without Spark or Flink to interact with the Dremio Catalog programmatically. This is useful for building custom applications or microservices.

## Dependencies (`pom.xml`)

```xml
<dependencies>
    <!-- Core Iceberg -->
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-core</artifactId>
        <version>1.5.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-rest-client</artifactId>
        <version>1.5.0</version>
    </dependency>
    <!-- AWS Bundle for S3 (Required for Dremio Cloud) -->
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-aws-bundle</artifactId>
        <version>1.5.0</version>
    </dependency>
</dependencies>
```

## Example Code

```java
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.hadoop.conf.Configuration;
import java.util.HashMap;
import java.util.Map;

public class DremioCatalogExample {
    public static void main(String[] args) {
        
        // 1. Configure Properties
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, "https://catalog.dremio.cloud/api/iceberg");
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "<PROJECT_NAME>"); // Your Project
        properties.put("credential", "<YOUR_PAT>"); // Dremio PAT
        
        // Critical for writing data (Cred Vending)
        properties.put("header.X-Iceberg-Access-Delegation", "vended-credentials");
        
        // FileIO Config (using bundled AWS SDK)
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");

        // 2. Initialize Catalog
        Catalog catalog = CatalogUtil.loadCatalog(
            "org.apache.iceberg.rest.RESTCatalog",
            "dremio",
            properties,
            new Configuration()
        );

        // 3. Perform Operations
        System.out.println("Namespaces: " + catalog.listNamespaces());
        
        if (catalog.tableExists(TableIdentifier.of("marketing", "campaigns"))) {
            System.out.println("Table exists!");
        }
    }
}
```

## Explanation
- **CatalogUtil**: The standard factory for loading catalogs dynamically.
- **S3FileIO**: Dremio Cloud stores tables in S3 (or Azure/GCS). Even if you are just reading metadata, the client often needs the FileIO implementation available on the classpath.
- **credential**: This property is automatically mapped to the Authorization header by the REST client.
