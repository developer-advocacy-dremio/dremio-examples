# Dremio Best Practices: Views, Reflections & Governance

This guide outlines architectural patterns for designing scalable Semantic Layers, optimizing Reflections for performance vs. cost, and maintaining high-quality metadata.

## 1. View Design (The Semantic Layer)

Dremio views decouple the **Physical Data** (S3, Iceberg, SQL Server) from the **Logical Business Logic**. We recommend a **Medallion Architecture** organized by functional areas (Spaces).

### Bronze Layer (Raw / Staging)
- **Purpose**: Direct 1:1 representation of source tables.
- **Transformations**: Minimal. Casting types (String -> Date), renaming technical columns to friendly names.
- **Anti-Pattern**: DO NOT perform joins or heavy aggregations here.
- **Example**: `Bronze.Orders` (Casts `order_ts` string to Timestamp).

### Silver Layer (Clean / Enriched)
- **Purpose**: The "Source of Truth" for business entities.
- **Best Practice: Sub-Layering**: Split Silver into two distinct levels to maximize reusability (`DRY - Don't Repeat Yourself`).
    
    **Level 1: Silver Clean (Technical Standardization)**
    - **Goal**: Create a pristine version of the source data that is *business-agnostic*.
    - **Actions**: 
        - Deduplication (`DISTINCT`, `QUALIFY ROW_NUMBER()`).
        - Standardization (e.g., mapping `NY`, `N.Y.`, `New York` -> `NY`).
        - Data Quality fixes (handling `NULL`s, casting types).
    - **Why**: This view can be reused by *all* domains (Finance, Marketing, Ops) without imposing business rules they might disagree on.
    - **Example**: `Silver.Technical.Clean_Orders`

    **Level 2: Silver Business (Domain Logic)**
    - **Goal**: Apply specific business rules and join dimensions.
    - **Actions**:
        - Filtering (e.g., `WHERE status != 'Cancelled'` - defining what counts as a valid order).
        - Calculations (e.g., `NetRevenue = Amount - Tax`).
        - Joins (enriching with Customers/Products).
    - **Why**: Different teams can build different "Silver Business" views on top of the *same* "Silver Clean" view. If the source schema changes, you fix it once in "Silver Clean", and everyone benefits.
    - **Example**: `Silver.Finance.Valid_Revenue_Orders` (Built on `Silver.Technical.Clean_Orders`).

- **Acceleration**: Place **Raw Reflections** on "Silver Business" views to accelerate broad ad-hoc exploration.

### Gold Layer (Presentation / Consumption)
- **Purpose**: Report-specific datasets ready for BI (Tableau, PowerBI).
- **Transformations**: Aggregations, business-specific logic, dimensional modeling (Star Schema).
- **Best Practice**: Create **Aggregate Reflections** here if dashboards are slow.
- **Example**: `Gold.Monthly_Revenue_By_Region`.

### Modularity & Reusability
- **Stack Views**: Build Gold views on top of Silver views. If business logic changes (e.g., definition of "Active Customer"), fix it once in Silver, and all Gold views update automatically.
- **Avoid Custom SQL** in BI Tools: Point tools to Dremio Views. This ensures logic lives in Dremio (version controlled, governing) rather than scattered across `.tbwbx` files.

---

## 2. Reflection Design (Acceleration)

Reflections are transparent materializations. The goal is to maximize **Query Coverage** while minimizing **Storage/Refresh Cost**.

### Broad vs. Specific Strategy
1.  **Broad Acceleration (Silver Layer)**:
    - **Type**: Raw Reflection.
    - **Goal**: Accelerate *any* ad-hoc query that needs this data.
    - **Selection**: Select all commonly used columns.
    - **Optimization**: Partition by low-cardinality time fields (e.g., `OrderDate`). Sort by high-cardinality filter fields (e.g., `CustomerID`).

2.  **Specific Acceleration (Gold Layer)**:
    - **Type**: Aggregate Reflection.
    - **Goal**: Accelerate specifically slow BI dashboards.
    - **Selection**: Explicit Dimensions (Group Bys) and Measures (Sums, Counts).
    - **Trade-off**: Highly efficient but only works for queries matching the aggregation level.

### Optimization Tips
- **Sorting**: Sorting a reflection by `User_ID` makes filtering `WHERE User_ID = '123'` massive faster (Data Skipping).
- **Partitioning**: Only partition if you have massive data (>100GB). Over-partitioning creates small files (bad).
- **Incremental Refresh**: Always enable for large tables based on a monotonic key (`TransactionID`, `UpdatedAt`).

---

## 3. Wiki Documentation Standards

A well-documented dataset increases trust and reduces "How do I use this?" questions.

### Recommended Wiki Structure
```markdown
# Dataset Name (e.g., Customer 360)

**Owner**: @DataEngineeringTeam / @SalesOps
**Refresh Schedule**: Daily at 2 AM UTC
**Source**: Salesforce (Accounts), Postgres (Transactions)

## Description
This dataset contains the unified view of all active customers, including their lifetime value and latest contract status.

## Key Fields
- `LTV`: Calculated as sum of all closed-won opportunities.
- `ChurnRisk`: ML score (0-1) indicating likelihood to leave.

## Usage Notes
- Filter by `IsActive = true` for current reporting.
- Do not use `Legacy_ID` for new joins.
```

---

## 4. Tagging Strategies

Tags help organize, discover, and govern datasets across different Spaces.

### Taxonomy Recommendation

| Category | Format | Examples | Purpose |
| :--- | :--- | :--- | :--- |
| **Domain** | `domain:<name>` | `domain:finance`, `domain:marketing` | Logical grouping beyond Spaces. |
| **Status** | `status:<state>` | `status:certified`, `status:beta`, `status:deprecated` | Indicates reliability. |
| **Sensitivity** | `pii:<level>` | `pii:none`, `pii:sensitive`, `pii:masked` | Flags data governance needs. |
| **Source** | `source:<system>` | `source:salesforce`, `source:s3` | Lineage hints. |
| **Workload** | `workload:<type>`| `workload:etl`, `workload:reporting` | Describes intended use. |

### Application
- **Admins**: Automate checking for `pii` tags to apply Security Policies.
- **Analysts**: Search for `status:certified` to find trusted data.
